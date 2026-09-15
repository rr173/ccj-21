"""摄入语义：唯一事件编号去重、乱序入窗、聚合方式、水位线、迟到与隔离。"""
import unittest

from helpers import ServiceTestCase
from telemetry.service import (
    EVENT_ACCEPTED,
    EVENT_DUPLICATE,
    EVENT_NO_RULE,
    EVENT_QUARANTINED,
    RuleError,
)


class TestIngestion(ServiceTestCase):
    def test_batch_ingest_and_window_assignment(self):
        self.make_rule()
        self.svc.register_device("d1", "g1")
        res = self.ingest_ok([
            self.ev(value=10, t=5),
            self.ev(value=20, t=30),
            self.ev(value=40, t=65),
        ])
        self.assertEqual([r["status"] for r in res], [EVENT_ACCEPTED] * 3)
        w0 = self.window(start=0.0)
        w1 = self.window(start=60.0)
        self.assertEqual((w0["agg_value"], w0["event_count"]), (15.0, 2))
        self.assertEqual((w1["agg_value"], w1["event_count"]), (40.0, 1))

    def test_duplicate_event_id_counted_once(self):
        self.make_rule()
        self.svc.register_device("d1", "g1")
        e = self.ev(value=10, t=5, event_id="dup-1")
        r1 = self.ingest_ok([e, dict(e)])[0]
        r2 = self.ingest_ok([dict(e)])[0]
        self.assertEqual(r1["status"], EVENT_ACCEPTED)
        self.assertEqual(r2["status"], EVENT_DUPLICATE)
        self.assertEqual(r2["original_status"], EVENT_ACCEPTED)
        # 同批内重复也不重复计数
        self.assertEqual(self.window(start=0.0)["event_count"], 1)
        self.assertEqual(self.svc.get_event("dup-1")["status"], EVENT_ACCEPTED)

    def test_out_of_order_event_enters_correct_window(self):
        self.make_rule()
        self.svc.register_device("d1", "g1")
        # 先发 t=35 再发 t=5，两者同属窗口 [0,60)，水位线 35 未封存窗口
        self.ingest_ok([self.ev(value=100, t=35), self.ev(value=1, t=5)])
        w = self.window(start=0.0)
        self.assertEqual((w["event_count"], w["agg_value"]), (2, 50.5))
        self.assertEqual(w["sealed"], 0)

    def test_aggregations(self):
        cases = {
            "sum": 30.0, "avg": 10.0, "min": 5.0, "max": 15.0,
            "count": 3.0, "last": 15.0,
        }
        for agg, expected in cases.items():
            svc = self.make_service()
            svc.create_rule(group_id="g1", metric=f"m_{agg}", window_size_sec=60,
                            aggregation=agg, operator="gt", threshold=0,
                            consecutive_hits=1)
            svc.register_device("d1", "g1")
            svc.ingest([
                self.ev(metric=f"m_{agg}", value=5, t=1),
                self.ev(metric=f"m_{agg}", value=10, t=2),
                self.ev(metric=f"m_{agg}", value=15, t=3),
            ])
            w = svc.list_windows("d1", f"m_{agg}")[0]
            self.assertEqual(w["agg_value"], expected, f"aggregation {agg}")
            svc.close()

    def test_no_rule_status(self):
        self.svc.register_device("d1", "g1")  # 未创建任何规则
        res = self.ingest_ok(self.ev(value=1, t=10))[0]
        self.assertEqual(res["status"], EVENT_NO_RULE)
        self.assertEqual(self.svc.get_event(res["event_id"])["status"],
                         EVENT_NO_RULE)

    def test_event_before_rule_effective_from_is_no_rule(self):
        self.make_rule(effective_from=1000.0)
        self.svc.register_device("d1", "g1")
        res = self.ingest_ok(self.ev(value=1, t=10))[0]
        self.assertEqual(res["status"], EVENT_NO_RULE)

    def test_window_seals_when_watermark_passes_end(self):
        self.make_rule()
        self.svc.register_device("d1", "g1")
        self.ingest_ok([self.ev(value=10, t=5), self.ev(value=1, t=60)])
        w = self.window(start=0.0)
        self.assertEqual(w["sealed"], 1)
        self.assertIsNotNone(w["hit"])

    def test_late_event_within_lateness_recorrects_window(self):
        self.make_rule(allowed_lateness_sec=100)
        self.svc.register_device("d1", "g1")
        # t=100 使水位线=100，窗口 [0,60) 封存（封板点 60+100=160）
        self.ingest_ok([self.ev(value=10, t=5), self.ev(value=1, t=100)])
        self.assertEqual(self.window(start=0.0)["sealed"], 1)
        # 迟到事件 t=20 仍在迟到期内：重算窗口并留下修正记录
        res = self.ingest_ok(self.ev(value=50, t=20))[0]
        self.assertEqual(res["status"], EVENT_ACCEPTED)
        w = self.window(start=0.0)
        self.assertEqual((w["event_count"], w["agg_value"]), (2, 30.0))
        corr = self.svc.list_window_corrections("d1")
        self.assertEqual(len(corr), 1)
        self.assertEqual((corr[0]["old_agg"], corr[0]["new_agg"]), (10.0, 30.0))
        self.assertEqual(corr[0]["trigger_event_id"], res["event_id"])

    def test_event_beyond_lateness_is_quarantined(self):
        self.make_rule(allowed_lateness_sec=10)
        self.svc.register_device("d1", "g1")
        # 水位线=100，窗口 [0,60) 封板点=70，t=20 的事件超过迟到期
        self.ingest_ok([self.ev(value=10, t=5), self.ev(value=1, t=100)])
        res = self.ingest_ok(self.ev(value=999, t=20))[0]
        self.assertEqual(res["status"], EVENT_QUARANTINED)
        self.assertIn("lateness", res["reason"])
        # 已封存结果不被改动
        w = self.window(start=0.0)
        self.assertEqual((w["event_count"], w["agg_value"]), (1, 10.0))
        # 隔离结果可查询
        quar = self.svc.list_events(status=EVENT_QUARANTINED)
        self.assertEqual(len(quar), 1)
        self.assertEqual(quar[0]["event_id"], res["event_id"])
        # 隔离事件不产生窗口修正记录
        self.assertEqual(self.svc.list_window_corrections("d1"), [])

    def test_quarantine_boundary_is_inclusive(self):
        self.make_rule(allowed_lateness_sec=10)
        self.svc.register_device("d1", "g1")
        self.ingest_ok([self.ev(value=1, t=70)])  # 水位线=70，封板点恰好=70
        res = self.ingest_ok(self.ev(value=1, t=5))[0]
        self.assertEqual(res["status"], EVENT_QUARANTINED)

    def test_late_event_for_never_seen_window_is_accepted_once(self):
        """水位线已过但窗口从未有数据：直接封存评估，不产生修正记录。"""
        self.make_rule(allowed_lateness_sec=100)
        self.svc.register_device("d1", "g1")
        self.ingest_ok(self.ev(value=1, t=150))  # 水位线=150
        res = self.ingest_ok(self.ev(value=7, t=65))[0]  # 窗口 [60,120) 首次有数据
        self.assertEqual(res["status"], EVENT_ACCEPTED)
        w = self.window(start=60.0)
        self.assertEqual((w["sealed"], w["agg_value"]), (1, 7.0))
        self.assertEqual(self.svc.list_window_corrections("d1"), [])

    def test_invalid_events_rejected_atomically(self):
        self.make_rule()
        self.svc.register_device("d1", "g1")
        with self.assertRaises(RuleError):
            self.ingest_ok([self.ev(value=1, t=1), {"event_id": "bad"}])
        self.assertEqual(self.svc.list_events(), [])  # 整批未写入

    def test_unregistered_device_defaults_and_is_auditable(self):
        self.make_rule()  # 规则在 g1 组
        res = self.ingest_ok(self.ev(device="ghost", value=1, t=1))[0]
        self.assertEqual(res["status"], EVENT_NO_RULE)  # ghost 落入 default 组
        self.assertEqual(self.svc.get_event(res["event_id"])["device_id"], "ghost")


if __name__ == "__main__":
    unittest.main()
