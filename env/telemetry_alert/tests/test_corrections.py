"""迟到修正：窗口重算留痕、告警结论改变时保留修正前后原因、
已发通知不删除。"""
import unittest

from helpers import ServiceTestCase


class TestCorrections(ServiceTestCase):
    def boot(self, **rule):
        params = dict(aggregation="avg", operator="gt", threshold=80.0,
                      consecutive_hits=2, recovery_count=1,
                      allowed_lateness_sec=300)
        params.update(rule)
        self.make_rule(**params)
        self.svc.register_device("d1", "g1")

    def test_late_event_recorrects_sealed_window(self):
        self.boot()
        # 窗口 [0,60) 一个事件 90 -> 命中；t=200 推进水位线封存多个窗口
        self.ingest_ok([self.ev(value=90, t=10), self.ev(value=10, t=200)])
        w = self.window(start=0.0)
        self.assertEqual((w["sealed"], w["hit"], w["agg_value"]), (1, 1, 90.0))
        # 迟到事件 t=20（水位线 200 < 60+300，仍在迟到期内）
        self.ingest_ok(self.ev(value=30, t=20))
        w = self.window(start=0.0)
        self.assertEqual(w["agg_value"], 60.0)   # (90+30)/2
        self.assertEqual(w["hit"], 0)
        corr = self.svc.list_window_corrections("d1")
        self.assertEqual(len(corr), 1)
        c = corr[0]
        self.assertEqual((c["old_agg"], c["new_agg"]), (90.0, 60.0))
        self.assertEqual((c["old_hit"], c["new_hit"]), (1, 0))
        self.assertEqual(c["rule_version"], 1)

    def test_correction_invalidates_alert_and_keeps_history(self):
        self.boot()
        # 窗口0、1 连续命中 -> 开告警；窗口2 未命中 -> 关闭
        self.ingest_ok([
            self.ev(value=90, t=10), self.ev(value=95, t=70),
            self.ev(value=10, t=130), self.ev(value=10, t=300),
        ])
        a = self.alerts()[0]
        self.assertEqual(a["status"], "closed")
        opened_notif = self.svc.list_notifications(alert_id=a["alert_id"])
        self.assertEqual({n["type"] for n in opened_notif}, {"opened", "closed"})

        # 迟到事件把窗口0 的均值拉下阈值：告警不再成立
        self.ingest_ok(self.ev(value=10, t=20))
        a = self.alerts()[0]
        self.assertEqual(a["status"], "invalidated")

        corr = self.svc.list_alert_corrections(a["alert_id"])
        self.assertEqual(len(corr), 1)
        self.assertEqual(corr[0]["correction_type"], "invalidated")
        self.assertIn("consecutive hits", corr[0]["reason_before"])
        self.assertIn("late-data correction", corr[0]["reason_after"])

        # 已发出的通知记录不删除，并新增一条 corrected 通知
        notifs = self.svc.list_notifications(alert_id=a["alert_id"])
        self.assertEqual({n["type"] for n in notifs},
                         {"opened", "closed", "corrected"})

    def test_correction_opens_retroactive_alert(self):
        self.boot()
        # 窗口0 未命中、窗口1 命中 -> 不连续，无告警
        self.ingest_ok([
            self.ev(value=10, t=10), self.ev(value=90, t=70),
            self.ev(value=10, t=300),
        ])
        self.assertEqual(self.alerts(), [])
        # 迟到事件让窗口0 也命中（avg(10,200)=105 > 80）-> 追溯开启告警（开启于窗口1）
        self.ingest_ok(self.ev(value=200, t=20))
        alerts = self.alerts()
        self.assertEqual(len(alerts), 1)
        a = alerts[0]
        self.assertEqual(a["status"], "open")
        self.assertEqual(a["opened_at"], 60.0)
        corr = self.svc.list_alert_corrections(a["alert_id"])
        self.assertEqual(corr[0]["correction_type"], "opened")
        self.assertEqual(corr[0]["reason_before"], "no alert period")
        self.assertIn("consecutive hits", corr[0]["reason_after"])
        notifs = self.svc.list_notifications(alert_id=a["alert_id"])
        self.assertEqual([n["type"] for n in notifs], ["opened"])

    def test_correction_reopens_closed_alert(self):
        self.boot()
        # 连续命中开窗 -> 窗口2 未命中关闭
        self.ingest_ok([
            self.ev(value=90, t=10), self.ev(value=95, t=70),
            self.ev(value=10, t=130), self.ev(value=10, t=300),
        ])
        a = self.alerts()[0]
        self.assertEqual(a["status"], "closed")
        self.assertEqual(a["closed_at"], 120.0)
        # 迟到事件让窗口2 变成命中：恢复条件不再满足，告警应重新打开
        self.ingest_ok(self.ev(value=200, t=140))
        a = self.alerts()[0]
        self.assertEqual(a["status"], "open")
        self.assertIsNone(a["closed_at"])
        corr = self.svc.list_alert_corrections(a["alert_id"])
        self.assertEqual(len(corr), 1)
        self.assertEqual(corr[0]["correction_type"], "updated")
        self.assertIn("recovered", corr[0]["reason_before"])
        self.assertIn("reopened", corr[0]["reason_after"])
        # closed 通知已发出不删除；补发 corrected 通知说明结论变化
        notifs = self.svc.list_notifications(alert_id=a["alert_id"])
        self.assertEqual({n["type"] for n in notifs},
                         {"opened", "closed", "corrected"})

    def test_correction_does_not_touch_quarantined_window(self):
        self.boot(allowed_lateness_sec=10)
        self.ingest_ok([self.ev(value=90, t=10), self.ev(value=1, t=200)])
        w = self.window(start=0.0)
        self.assertEqual((w["sealed"], w["agg_value"]), (1, 90.0))
        # 水位线 200 >= 60+10：超过迟到期，只能隔离，不能改动封存结果
        res = self.ingest_ok(self.ev(value=1, t=20))[0]
        self.assertEqual(res["status"], "quarantined")
        w = self.window(start=0.0)
        self.assertEqual(w["agg_value"], 90.0)
        self.assertEqual(self.svc.list_window_corrections("d1"), [])

    def test_correction_records_trigger_event(self):
        self.boot()
        self.ingest_ok([self.ev(value=90, t=10), self.ev(value=1, t=200)])
        self.ingest_ok(self.ev(value=30, t=20, event_id="late-1"))
        corr = self.svc.list_window_corrections("d1")
        self.assertEqual(corr[0]["trigger_event_id"], "late-1")

    def test_replay_is_idempotent(self):
        """重复触发重放（如重启后）不产生重复告警或重复通知。"""
        self.boot()
        self.ingest_ok([
            self.ev(value=90, t=10), self.ev(value=95, t=70),
            self.ev(value=10, t=300),
        ])
        before_alerts = self.alerts()
        before_notifs = self.svc.list_notifications()
        # 再发一个落在未封存窗口的普通事件，触发同一链的重放
        self.ingest_ok(self.ev(value=5, t=310))
        self.assertEqual(self.alerts(), before_alerts)
        self.assertEqual(self.svc.list_notifications(), before_notifs)


if __name__ == "__main__":
    unittest.main()
