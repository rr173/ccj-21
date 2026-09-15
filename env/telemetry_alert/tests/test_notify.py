"""通知投递：持久化待发队列、退避重试、重复确认幂等、
未确认重投、已确认不重发。"""
import unittest

from helpers import ServiceTestCase


class TestNotify(ServiceTestCase):
    def boot_alert(self):
        """制造一个开启的告警及其 opened 通知（w0 命中并被 w1 的水位线封存）。"""
        self.make_rule(consecutive_hits=1, recovery_count=1)
        self.svc.register_device("d1", "g1")
        self.ingest_ok([self.ev(value=90, t=30), self.ev(value=10, t=60)])
        notifs = self.svc.list_notifications()
        self.assertEqual(len(notifs), 1)
        self.assertEqual(notifs[0]["type"], "opened")
        self.assertEqual(notifs[0]["status"], "pending")
        return notifs[0]

    def test_notification_persisted_in_outbox(self):
        n = self.boot_alert()
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual(got["status"], "pending")
        self.assertEqual(got["attempts"], 0)
        self.assertIsNotNone(got["next_attempt_at"])

    def test_pump_success_then_ack(self):
        n = self.boot_alert()
        out = self.svc.pump_notifications()
        self.assertEqual(out["pumped"], 1)
        self.assertEqual(len(self.sender.sent), 1)
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual(got["status"], "sent")
        self.assertEqual(got["attempts"], 1)
        # 确认后完成
        acked = self.svc.ack_notification(n["notification_id"], "tok-1")
        self.assertEqual(acked["status"], "confirmed")
        self.assertEqual(acked["ack_token"], "tok-1")
        self.assertIsNotNone(acked["confirmed_at"])

    def test_duplicate_ack_is_idempotent(self):
        n = self.boot_alert()
        self.svc.pump_notifications()
        first = self.svc.ack_notification(n["notification_id"], "tok-1")
        again = self.svc.ack_notification(n["notification_id"], "tok-2")
        third = self.svc.ack_notification(n["notification_id"])
        # 重复确认不报错、不改变首次完成结果
        self.assertEqual(again["status"], "confirmed")
        self.assertEqual(again["ack_token"], "tok-1")
        self.assertEqual(again["confirmed_at"], first["confirmed_at"])
        self.assertEqual(third["confirmed_at"], first["confirmed_at"])
        # 尝试记录仍只有一次发送
        attempts = self.svc.list_notification_attempts(n["notification_id"])
        self.assertEqual(len(attempts), 1)

    def test_failed_send_retries_with_backoff(self):
        n = self.boot_alert()
        self.sender.fail_next(2)
        self.svc.pump_notifications()
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual(got["status"], "pending")
        self.assertEqual(got["attempts"], 1)
        self.assertAlmostEqual(got["next_attempt_at"], self.clock.t + 1.0)
        # 退避期内不重试
        self.assertEqual(self.svc.pump_notifications()["pumped"], 0)
        # 第二次失败后退避加倍
        self.clock.advance(1.0)
        self.svc.pump_notifications()
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual(got["attempts"], 2)
        self.assertAlmostEqual(got["next_attempt_at"], self.clock.t + 2.0)
        # 第三次成功
        self.clock.advance(2.0)
        self.svc.pump_notifications()
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual(got["status"], "sent")
        self.assertEqual(got["attempts"], 3)
        attempts = self.svc.list_notification_attempts(n["notification_id"])
        self.assertEqual([a["result"] for a in attempts],
                         ["failure", "failure", "success"])
        self.assertEqual([a["attempt_no"] for a in attempts], [1, 2, 3])

    def test_unconfirmed_notification_is_resent_after_timeout(self):
        n = self.boot_alert()
        self.svc.pump_notifications()          # sent，等待确认
        self.assertEqual(len(self.sender.sent), 1)
        self.clock.advance(29.0)
        self.assertEqual(self.svc.pump_notifications()["pumped"], 0)
        self.clock.advance(1.0)                # 到达 ack_timeout
        self.assertEqual(self.svc.pump_notifications()["pumped"], 1)
        self.assertEqual(len(self.sender.sent), 2)   # 重投（接收方按 id 去重）
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual(got["attempts"], 2)

    def test_confirmed_notification_never_resent(self):
        n = self.boot_alert()
        self.svc.pump_notifications()
        self.svc.ack_notification(n["notification_id"], "tok")
        self.clock.advance(10000.0)
        self.assertEqual(self.svc.pump_notifications()["pumped"], 0)
        self.assertEqual(len(self.sender.sent), 1)

    def test_closed_alert_enqueues_closed_notification(self):
        self.make_rule(consecutive_hits=1, recovery_count=1)
        self.svc.register_device("d1", "g1")
        # w0 命中开告警；w1 未命中关告警；w2 的事件把水位线推过 w1
        self.ingest_ok([self.ev(value=90, t=30), self.ev(value=10, t=90),
                        self.ev(value=10, t=150)])
        types = sorted(n["type"] for n in self.svc.list_notifications())
        self.assertEqual(types, ["closed", "opened"])

    def test_opened_and_closed_notifications_enqueued_once(self):
        """同一周期的 opened/closed 通知幂等：重放不会重复入队。"""
        self.make_rule(consecutive_hits=1, recovery_count=1)
        self.svc.register_device("d1", "g1")
        self.ingest_ok([self.ev(value=90, t=30), self.ev(value=10, t=90),
                        self.ev(value=10, t=150)])
        self.assertEqual(len(self.svc.list_notifications()), 2)
        # 再封存一个未命中窗口触发重放，告警结论不变，通知不重复
        self.ingest_ok(self.ev(value=5, t=200))
        self.assertEqual(len(self.svc.list_notifications()), 2)


if __name__ == "__main__":
    unittest.main()
