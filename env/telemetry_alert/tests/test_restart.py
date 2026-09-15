"""重启恢复：未封存窗口、未关闭告警、未完成通知在重启后继续；
不重复聚合、不重复开告警、不重发已确认通知。"""
import unittest

from helpers import ServiceTestCase


class TestRestart(ServiceTestCase):
    def boot(self, **rule):
        params = dict(consecutive_hits=2, recovery_count=1,
                      allowed_lateness_sec=300)
        params.update(rule)
        self.make_rule(**params)
        self.svc.register_device("d1", "g1")

    def open_alert(self):
        """w0、w1 命中，w2 的水位线封存它们 -> 开启一个告警周期。"""
        self.ingest_ok([self.ev(value=90, t=30), self.ev(value=95, t=90),
                        self.ev(value=91, t=150)])

    def test_unsealed_window_survives_restart(self):
        self.boot()
        self.ingest_ok(self.ev(value=90, t=10))     # 窗口 [0,60) 未封存
        self.reopen()
        # 重启后继续向同一窗口写入，聚合不重复、不丢失
        self.ingest_ok(self.ev(value=70, t=20))
        w = self.window(start=0.0)
        self.assertEqual((w["event_count"], w["agg_value"]), (2, 80.0))
        # 重启后重复事件仍被去重
        self.ingest_ok(self.ev(value=70, t=20, event_id="e2"))
        dup = self.ingest_ok(self.ev(value=70, t=20, event_id="e2"))[0]
        self.assertEqual(dup["status"], "duplicate")
        self.assertEqual(self.window(start=0.0)["event_count"], 3)

    def test_open_alert_continues_after_restart(self):
        self.boot()
        # w0、w1 命中；w2 命中（其水位线封存 w0、w1）-> 开告警
        self.ingest_ok([self.ev(value=90, t=30), self.ev(value=95, t=90),
                        self.ev(value=91, t=150)])
        alert_id = self.alerts()[0]["alert_id"]
        self.reopen()
        # w3 未命中（其水位线封存 w2 -> 第三次命中）：更新同一周期，不开新告警
        self.ingest_ok(self.ev(value=10, t=210))
        alerts = self.alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_id"], alert_id)
        self.assertEqual(alerts[0]["hit_count"], 3)
        # w4 未命中（其水位线封存 w3 -> 达到恢复条件）：告警在重启后正常关闭
        self.ingest_ok(self.ev(value=10, t=270))
        self.assertEqual(self.alerts()[0]["status"], "closed")

    def test_pending_notification_pumped_after_restart(self):
        self.boot()
        self.open_alert()
        n = self.svc.list_notifications()[0]
        self.assertEqual(n["status"], "pending")
        self.reopen()
        out = self.svc.pump_notifications()
        self.assertEqual(out["pumped"], 1)
        self.assertEqual(self.svc.get_notification(n["notification_id"])["status"],
                         "sent")

    def test_failed_notification_keeps_retry_state_after_restart(self):
        self.boot()
        self.open_alert()
        n = self.svc.list_notifications()[0]
        self.sender.fail_next(1)
        self.svc.pump_notifications()
        self.assertEqual(self.svc.get_notification(n["notification_id"])["attempts"], 1)
        self.reopen()
        # 退避状态持久化：到期前不重试，到期后继续
        self.assertEqual(self.svc.pump_notifications()["pumped"], 0)
        self.clock.advance(1.0)
        self.assertEqual(self.svc.pump_notifications()["pumped"], 1)
        got = self.svc.get_notification(n["notification_id"])
        self.assertEqual((got["status"], got["attempts"]), ("sent", 2))

    def test_confirmed_notification_not_resent_after_restart(self):
        self.boot()
        self.open_alert()
        n = self.svc.list_notifications()[0]
        self.svc.pump_notifications()
        self.svc.ack_notification(n["notification_id"], "tok")
        sent_before = len(self.sender.sent)
        self.reopen()
        self.clock.advance(10000.0)
        self.assertEqual(self.svc.pump_notifications()["pumped"], 0)
        self.assertEqual(len(self.sender.sent), sent_before)

    def test_unconfirmed_sent_notification_resent_after_restart(self):
        self.boot()
        self.open_alert()
        n = self.svc.list_notifications()[0]
        self.svc.pump_notifications()      # sent 但未确认时服务停止
        self.reopen()
        self.clock.advance(30.0)           # 超过 ack_timeout
        self.assertEqual(self.svc.pump_notifications()["pumped"], 1)
        self.assertEqual(self.svc.get_notification(n["notification_id"])["attempts"], 2)

    def test_no_duplicate_aggregation_after_restart(self):
        self.boot()
        evs = [self.ev(value=10, t=5), self.ev(value=20, t=15),
               self.ev(value=30, t=25)]
        self.ingest_ok(evs)
        self.reopen()
        # 原样重放同一批（例如客户端重试），不得重复计数
        res = self.ingest_ok([dict(e) for e in evs])
        self.assertTrue(all(r["status"] == "duplicate" for r in res))
        w = self.window(start=0.0)
        self.assertEqual((w["event_count"], w["agg_value"]), (3, 20.0))

    def test_watermark_and_quarantine_survive_restart(self):
        self.boot(allowed_lateness_sec=10)
        self.ingest_ok([self.ev(value=90, t=10), self.ev(value=1, t=200)])
        self.reopen()
        res = self.ingest_ok(self.ev(value=1, t=20))[0]
        self.assertEqual(res["status"], "quarantined")
        self.assertEqual(self.window(start=0.0)["agg_value"], 90.0)


if __name__ == "__main__":
    unittest.main()
