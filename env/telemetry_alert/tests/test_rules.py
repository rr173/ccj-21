"""规则版本化：生效时间、窗口绑定规则版本、更新后旧窗口不被重新解释。

特别覆盖：新版本在窗口中途生效、或窗口长度变化导致新旧窗口起点相同时，
生效后的事件必须按自身事件时间进入新版本窗口（同起点新旧窗口并存），
已归属旧版本的事件/窗口/修正/告警始终沿用旧版本。
"""
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

    def test_event_uses_effective_version_when_version_takes_effect_inside_window(self):
        """版本在窗口中途生效：生效后的事件进入新版本窗口，旧版本窗口
        保留切换前数据；迟到事件仍修正旧版本窗口。"""
        rid = self.make_rule(threshold=80.0, consecutive_hits=1,
                             allowed_lateness_sec=300)["rule_id"]
        self.svc.register_device("d1", "g1")
        # t=10 -> v1 窗口 [0,60)
        self.ingest_ok(self.ev(value=90.0, t=10.0))
        # v2 在 90 生效，落在同一长度窗口 [60,120) 的中途
        self.svc.update_rule(rid, effective_from=90.0, threshold=50.0)
        # t=100 必须命中 v2 窗口 [60,120)；t=200 推进水位线封存两窗口
        self.ingest_ok([
            self.ev(value=60.0, t=100.0),
            self.ev(value=1.0, t=200.0),
        ])
        windows = {
            (w["window_start"], w["rule_version"]): w
            for w in self.svc.list_windows("d1", "cpu")
        }
        self.assertEqual(set(windows), {(0.0, 1), (60.0, 2), (180.0, 2)})
        self.assertEqual(windows[(60.0, 2)]["event_count"], 1)
        self.assertEqual(windows[(60.0, 2)]["agg_value"], 60.0)
        self.assertEqual(windows[(60.0, 2)]["hit"], 1)   # v2 阈值 50
        # 生效后的事件返回其绑定版本，不被旧版本窗口吞掉
        bound = {
            e["event_time"]: e["rule_version"]
            for e in self.svc.list_events("d1")
        }
        self.assertEqual(bound[100.0], 2)
        # 迟到事件 t=20 只能修正它自己版本(v1)的窗口，聚合值/结论按 v1
        self.ingest_ok(self.ev(value=30.0, t=20.0))
        windows = {
            (w["window_start"], w["rule_version"]): w
            for w in self.svc.list_windows("d1", "cpu")
        }
        v1, v2 = windows[(0.0, 1)], windows[(60.0, 2)]
        self.assertEqual(v1["event_count"], 2)
        self.assertEqual(v1["agg_value"], 60.0)          # (90+30)/2
        self.assertEqual(v1["hit"], 0)                   # v1 阈值 80，不命中
        self.assertEqual(v2["event_count"], 1)
        self.assertEqual(v2["agg_value"], 60.0)
        self.assertEqual(v2["hit"], 1)                   # v2 结论不受影响
        late = [e for e in self.svc.list_events("d1")
                if e["event_time"] == 20.0][0]
        self.assertEqual((late["rule_id"], late["rule_version"]),
                         (rid, 1))
        corr = self.svc.list_window_corrections("d1")
        self.assertEqual(len(corr), 1)
        self.assertEqual((corr[0]["window_start"], corr[0]["rule_version"]),
                         (0.0, 1))
        self.assertEqual((corr[0]["old_agg"], corr[0]["new_agg"]),
                         (90.0, 60.0))
        self.assertEqual((corr[0]["old_hit"], corr[0]["new_hit"]), (1, 0))

        # 告警判定：v1 旧结论被修正推翻（invalidated 保留），v2 命中开新周期
        alerts = {a["opened_at"]: a for a in self.svc.list_alerts(device_id="d1")}
        self.assertEqual(alerts[0.0]["status"], "invalidated")
        self.assertEqual(alerts[60.0]["status"], "open")
        acorr = self.svc.list_alert_corrections(alerts[0.0]["alert_id"])
        self.assertEqual(acorr[0]["correction_type"], "invalidated")

    def test_same_start_windows_coexist_when_window_size_changes(self):
        """窗口长度变化且新旧窗口起点相同：新版本事件进入新版本窗口，
        旧窗口不能吞掉新版本事件；迟到修正仍落在旧版本窗口。"""
        rid = self.make_rule(window_size_sec=60, threshold=80.0,
                             consecutive_hits=1, allowed_lateness_sec=300)["rule_id"]
        self.svc.register_device("d1", "g1")
        # t=30 -> v1 [0,60)
        self.ingest_ok(self.ev(value=90.0, t=30.0))
        # v2 自 100 生效、窗口长度 120：v2 的 [0,120) 与 v1 的 [0,60) 起点相同
        self.svc.update_rule(rid, effective_from=100.0,
                             window_size_sec=120, threshold=50.0)
        # t=110 -> v2 [0,120)；t=130 -> v2 [120,240)；t=300 封存
        self.ingest_ok([
            self.ev(value=60.0, t=110.0),
            self.ev(value=60.0, t=130.0),
            self.ev(value=1.0, t=300.0),
        ])
        windows = {
            (w["window_start"], w["rule_version"]): w
            for w in self.svc.list_windows("d1", "cpu")
        }
        # 同一起点 0.0 上新旧两个窗口必须并存
        self.assertIn((0.0, 1), windows)
        self.assertIn((0.0, 2), windows)
        self.assertEqual(windows[(0.0, 2)]["window_end"], 120.0)
        self.assertEqual(windows[(0.0, 2)]["event_count"], 1)
        self.assertEqual(windows[(0.0, 2)]["agg_value"], 60.0)
        self.assertEqual(windows[(120.0, 2)]["event_count"], 1)

        # 迟到 t=40 仍按 v1 修正旧窗口，不被同起点的 v2 窗口吞掉
        self.ingest_ok(self.ev(value=30.0, t=40.0))
        windows = {
            (w["window_start"], w["rule_version"]): w
            for w in self.svc.list_windows("d1", "cpu")
        }
        v1, v2 = windows[(0.0, 1)], windows[(0.0, 2)]
        self.assertEqual(v1["event_count"], 2)
        self.assertEqual(v1["agg_value"], 60.0)
        self.assertEqual(v1["hit"], 0)              # v1 阈值 80
        self.assertEqual(v2["event_count"], 1)
        self.assertEqual(v2["agg_value"], 60.0)     # v2 不混入迟到事件
        self.assertEqual(v2["hit"], 1)              # v2 阈值 50
        corr = self.svc.list_window_corrections("d1")
        self.assertEqual(len(corr), 1)
        self.assertEqual((corr[0]["window_start"], corr[0]["rule_version"]),
                         (0.0, 1))

    def test_lateness_checked_with_events_bound_version(self):
        """允许迟到期按事件绑定版本各自判定。"""
        rid = self.make_rule(allowed_lateness_sec=0)["rule_id"]
        self.svc.register_device("d1", "g1")
        self.ingest_ok([
            self.ev(value=90.0, t=10.0),
            self.ev(value=1.0, t=200.0),
        ])
        # v1 窗口 [0,60) 在水位线 200 时已封板
        self.svc.update_rule(rid, effective_from=100.0,
                             allowed_lateness_sec=1000.0)
        # 生效前的事件 t=20 用 v1（零迟到），必须隔离
        old = self.ingest_ok(self.ev(value=1.0, t=20.0))[0]
        self.assertEqual(old["status"], "quarantined")
        self.assertEqual(old["rule_version"] if "rule_version" in old else None,
                         None)
        # 生效后的事件 t=110 用 v2 的迟到设置，正常接收
        new = self.ingest_ok(self.ev(value=1.0, t=110.0))[0]
        self.assertEqual(new["status"], "accepted")
        self.assertEqual(new["rule_version"], 2)


if __name__ == "__main__":
    unittest.main()
