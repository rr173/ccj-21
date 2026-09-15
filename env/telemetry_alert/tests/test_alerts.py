"""告警生命周期：连续命中只开一个周期、后续命中更新同一周期、
恢复关闭、静默期、缺口（无数据窗口）处理。

注意：窗口只有在水位线越过其终点后才封存评估，因此每个用例最后
都会再发一个事件把水位线推过待评估窗口。
"""
import unittest

from helpers import ServiceTestCase


class TestAlerts(ServiceTestCase):
    def boot(self, **rule):
        self.make_rule(**rule)
        self.svc.register_device("d1", "g1")

    def w(self, i, value):
        """窗口 i（60s 一个）中点的事件。"""
        return self.ev(value=value, t=30 + 60 * i)

    def test_single_alert_period_for_consecutive_hits(self):
        self.boot(consecutive_hits=2)          # 阈值 gt 80, recovery=1
        # w0,w1,w2 命中；w3 的事件把水位线推过 w0..w2 使其封存
        self.ingest_ok([self.w(0, 90), self.w(1, 95), self.w(2, 91), self.w(3, 10)])
        alerts = self.alerts()
        self.assertEqual(len(alerts), 1)       # 只开启一个告警周期
        a = alerts[0]
        self.assertEqual(a["status"], "open")
        self.assertEqual(a["opened_at"], 60.0)  # 第 2 个连续命中窗口
        self.assertEqual(a["hit_count"], 3)     # 后续命中更新同一周期
        self.assertEqual(a["last_hit_window"], 120.0)
        opened = [n for n in self.svc.list_notifications() if n["type"] == "opened"]
        self.assertEqual(len(opened), 1)        # 只入队一条 opened 通知

    def test_no_alert_when_hits_not_consecutive(self):
        self.boot(consecutive_hits=2)
        # 命中-未命中-命中，不连续
        self.ingest_ok([self.w(0, 90), self.w(1, 10), self.w(2, 95), self.w(3, 5)])
        self.assertEqual(self.alerts(), [])

    def test_recovery_closes_alert(self):
        self.boot(consecutive_hits=2, recovery_count=2)
        # w0,w1 命中开告警；w2,w3 连续未命中达到恢复条件
        self.ingest_ok([self.w(0, 90), self.w(1, 95), self.w(2, 10),
                        self.w(3, 20), self.w(4, 1)])
        alerts = self.alerts()
        self.assertEqual(len(alerts), 1)
        a = alerts[0]
        self.assertEqual(a["status"], "closed")
        self.assertEqual(a["closed_at"], 180.0)  # 第 2 个未命中窗口起点
        self.assertIn("recovered", a["close_reason"])
        closed = [n for n in self.svc.list_notifications() if n["type"] == "closed"]
        self.assertEqual(len(closed), 1)

    def test_reopen_after_recovery_creates_new_period(self):
        self.boot(consecutive_hits=2, recovery_count=1)
        # 开@60 -> w2 未命中关@120 -> w3,w4 再次连续命中开新周期@240
        self.ingest_ok([self.w(0, 90), self.w(1, 95), self.w(2, 10),
                        self.w(3, 88), self.w(4, 99), self.w(5, 1)])
        alerts = self.alerts()
        self.assertEqual(len(alerts), 2)
        self.assertEqual(alerts[0]["status"], "closed")
        self.assertEqual(alerts[1]["status"], "open")
        self.assertEqual(alerts[1]["opened_at"], 240.0)

    def test_silence_period_suppresses_reopen(self):
        self.boot(consecutive_hits=1, recovery_count=1, silence_sec=120)
        # 开@0 -> w1 未命中关@60，静默到 60+120=180
        # w2(120) 命中但在静默期内 -> 抑制；w3(180) 命中，静默结束 -> 开@180
        self.ingest_ok([self.w(0, 90), self.w(1, 10), self.w(2, 95),
                        self.w(3, 96), self.w(4, 97), self.w(5, 1)])
        alerts = self.alerts()
        self.assertEqual(len(alerts), 2)
        self.assertEqual(alerts[0]["opened_at"], 0.0)
        self.assertEqual(alerts[0]["status"], "closed")
        self.assertEqual(alerts[1]["opened_at"], 180.0)
        self.assertEqual(alerts[1]["status"], "open")

    def test_gap_windows_break_consecutive_hits(self):
        self.boot(consecutive_hits=2)
        # w0 命中，w1 无数据（缺口），w2 命中 -> 缺口重置连续计数
        self.ingest_ok([self.w(0, 90), self.w(2, 95), self.w(3, 1)])
        self.assertEqual(self.alerts(), [])

    def test_gap_recovers_open_alert(self):
        self.boot(consecutive_hits=1, recovery_count=2)
        # w0,w1 命中开告警；w2,w3 无数据；w4 的事件封存前面窗口，
        # 缺口按非命中计，2 个缺口窗口达到恢复条件 -> 关@180
        self.ingest_ok([self.w(0, 90), self.w(1, 95), self.w(4, 5), self.w(5, 1)])
        a = self.alerts()[0]
        self.assertEqual(a["status"], "closed")
        self.assertEqual(a["closed_at"], 180.0)  # 第 2 个非命中（缺口）窗口起点

    def test_alert_open_reason_and_close_reason_recorded(self):
        self.boot(consecutive_hits=2, recovery_count=1)
        self.ingest_ok([self.w(0, 90), self.w(1, 95), self.w(2, 10), self.w(3, 1)])
        a = self.alerts()[0]
        self.assertIn("consecutive hits", a["open_reason"])
        self.assertIn("cpu", a["open_reason"])
        self.assertIn("recovered", a["close_reason"])

    def test_hit_evaluation_uses_window_pinned_rule_version(self):
        """规则更新后，旧窗口仍按旧阈值判定，新窗口按新阈值。"""
        rid = self.make_rule(threshold=80.0)["rule_id"]
        self.svc.register_device("d1", "g1")
        self.ingest_ok(self.ev(value=70.0, t=100.0))   # 窗口 [60,120)，v1
        self.svc.update_rule(rid, effective_from=180.0, threshold=50.0)
        self.ingest_ok([
            self.ev(value=70.0, t=130.0),   # 窗口 [120,180)，仍属 v1（<180）
            self.ev(value=60.0, t=200.0),   # 窗口 [180,240)，v2
            self.ev(value=1.0, t=260.0),    # 推水位线封存前面窗口
        ])
        w1 = self.window(start=120.0)
        w2 = self.window(start=180.0)
        self.assertEqual((w1["rule_version"], w1["hit"]), (1, 0))  # v1: 70>80 否
        self.assertEqual((w2["rule_version"], w2["hit"]), (2, 1))  # v2: 60>50 是

    def test_alerts_isolated_per_device(self):
        self.boot(consecutive_hits=2)
        self.svc.register_device("d2", "g1")
        self.ingest_ok([
            self.ev(device="d1", value=90, t=30),
            self.ev(device="d2", value=95, t=30),
            self.ev(device="d2", value=96, t=90),
            self.ev(device="d2", value=1, t=150),   # 封存 d2 的 w0,w1
        ])
        self.assertEqual(self.alerts("d1"), [])       # d1 只有一次命中
        self.assertEqual(len(self.alerts("d2")), 1)   # d2 连续两次命中


if __name__ == "__main__":
    unittest.main()
