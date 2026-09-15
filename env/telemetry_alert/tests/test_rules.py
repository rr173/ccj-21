"""规则版本化：生效时间、窗口绑定规则版本、更新后旧窗口不被重新解释。"""
import unittest

from helpers import ServiceTestCase
from telemetry.service import RuleError, NotFoundError


class TestRules(ServiceTestCase):
    def test_create_and_get_versions(self):
        rule = self.make_rule()
        rid = rule["rule_id"]
        self.assertEqual(len(rule["versions"]), 1)
        v1 = rule["versions"][0]
        self.assertEqual(v1["version"], 1)
        self.assertIsNone(v1["effective_to"])
        got = self.svc.get_rule(rid)
        self.assertEqual(got["versions"][0]["metric"], "cpu")

    def test_update_creates_new_version_and_closes_old_range(self):
        rid = self.make_rule()["rule_id"]
        updated = self.svc.update_rule(rid, effective_from=180.0, threshold=50.0)
        self.assertEqual(len(updated["versions"]), 2)
        v1, v2 = updated["versions"]
        self.assertEqual(v1["effective_to"], 180.0)
        self.assertEqual(v2["effective_from"], 180.0)
        self.assertIsNone(v2["effective_to"])
        self.assertEqual(v2["threshold"], 50.0)
        # 未修改的字段继承上一版本
        self.assertEqual(v2["consecutive_hits"], v1["consecutive_hits"])

    def test_update_rejects_unknown_field_and_bad_effective_from(self):
        rid = self.make_rule()["rule_id"]
        with self.assertRaises(RuleError):
            self.svc.update_rule(rid, effective_from=100.0, group_id="g2")
        with self.assertRaises(RuleError):
            self.svc.update_rule(rid, effective_from=-5.0)
        with self.assertRaises(NotFoundError):
            self.svc.update_rule("no-such-rule", effective_from=0.0)

    def test_duplicate_group_metric_rule_rejected(self):
        self.make_rule()
        with self.assertRaises(RuleError):
            self.make_rule()

    def test_invalid_rule_params(self):
        with self.assertRaises(RuleError):
            self.make_rule(aggregation="median")
        with self.assertRaises(RuleError):
            self.make_rule(operator="contains")
        with self.assertRaises(RuleError):
            self.make_rule(window_size_sec=0)
        with self.assertRaises(RuleError):
            self.make_rule(consecutive_hits=0)

    def test_windows_pinned_to_rule_version(self):
        """规则更新后：旧窗口保持 v1，新窗口按生效时间使用 v2。"""
        rid = self.make_rule(threshold=80.0, allowed_lateness_sec=300)["rule_id"]
        self.svc.register_device("d1", "g1")
        # t=100 -> 窗口 [60,120)，规则 v1
        self.ingest_ok(self.ev(value=90.0, t=100.0))
        # 更新规则：180 起阈值 50
        self.svc.update_rule(rid, effective_from=180.0, threshold=50.0)
        # t=200 -> 窗口 [180,240)，规则 v2；t=110 迟到但仍在迟到期内，
        # 落在已封存的旧窗口 [60,120)，触发修正且仍按 v1 判定；
        # t=250 推进水位线封存窗口 [180,240)
        self.ingest_ok([
            self.ev(value=60.0, t=200.0),
            self.ev(value=70.0, t=110.0),
            self.ev(value=1.0, t=250.0),
        ])
        windows = {w["window_start"]: w for w in self.svc.list_windows("d1", "cpu")}
        self.assertEqual(windows[60.0]["rule_version"], 1)
        self.assertEqual(windows[180.0]["rule_version"], 2)
        # 旧窗口 [60,120) 聚合了 90 和 70，均值 80，按 v1 阈值 80(gt) 不命中；
        # 若被 v2 重新解释(阈值 50)则会命中 —— 验证没有被重新解释
        self.assertEqual(windows[60.0]["agg_value"], 80.0)
        self.assertEqual(windows[60.0]["hit"], 0)
        self.assertEqual(windows[60.0]["sealed"], 1)
        # 新窗口按 v2 阈值 50 命中
        self.assertEqual(windows[180.0]["hit"], 1)

    def test_rule_version_selected_by_event_time(self):
        rid = self.make_rule(threshold=80.0)["rule_id"]
        self.svc.update_rule(rid, effective_from=120.0, threshold=10.0)
        self.svc.register_device("d1", "g1")
        r1 = self.ingest_ok(self.ev(value=50.0, t=60.0))[0]
        r2 = self.ingest_ok(self.ev(value=50.0, t=130.0))[0]
        self.assertEqual(r1["rule_version"], 1)
        self.assertEqual(r2["rule_version"], 2)

    def test_event_before_any_rule_gets_no_rule(self):
        self.make_rule(effective_from=1000.0)
        self.svc.register_device("d1", "g1")
        res = self.ingest_ok(self.ev(value=1.0, t=10.0))[0]
        self.assertEqual(res["status"], "no_rule")
        ev = self.svc.get_event(res["event_id"])
        self.assertEqual(ev["status"], "no_rule")


if __name__ == "__main__":
    unittest.main()
