"""temporal-topo 行为测试 (stdlib unittest).

运行: python3 -m unittest discover -s tests -v   (在 temporal_topo/ 目录下)
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from topo import derive as derive_mod
from topo import failure as failure_mod
from topo import ingest, jobs, ops
from topo.store import Store


class TopoCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="topo-test-")
        self.store = Store(os.path.join(self.dir, "t.db"))

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    # ---- 小工具 ----
    def node(self, nid, trust=5, room=None, domain=None, exit_=False):
        ingest.register_node(self.store, nid, room=room, domain=domain,
                             trust=trust, is_exit=exit_ or None)

    def obs(self, observer, seq, t, neighbor, medium="radio", quality=0.9, ttl=1000):
        return ingest.ingest_observation(self.store, observer, seq, t,
                                         neighbor, medium, quality, ttl)

    def derive(self, force=False):
        return derive_mod.derive(self.store, trigger="test", force=force)

    def edge_at(self, t, a, b, medium="radio"):
        """当前视图在 t 时刻某条边的行 (或 None)."""
        a, b = sorted((a, b))
        return self.store.q1(
            "SELECT ce.* FROM current_edges ce JOIN epochs e "
            "ON ce.epoch_start=e.epoch_start "
            "WHERE e.epoch_start<=? AND e.epoch_end>? AND ce.ep_a=? AND ce.ep_b=? "
            "AND ce.medium=?",
            (t, t, a, b, medium))


class TestSequenceChannel(TopoCase):
    def test_seq_must_advance_and_dedup(self):
        r1 = self.obs("n1", 1, 100, "n2")
        self.assertEqual(r1["status"], "accepted")
        # 相同序号相同内容: 只采信一次
        r2 = self.obs("n1", 1, 100, "n2")
        self.assertEqual(r2["status"], "duplicate")
        # 相同序号不同内容: 保留首次证词
        r3 = self.obs("n1", 1, 100, "n3")
        self.assertEqual(r3["status"], "seq_conflict")
        # 序号倒退
        self.obs("n1", 5, 200, "n2")
        r4 = self.obs("n1", 3, 300, "n2")
        self.assertEqual(r4["status"], "regressed")
        # 只有一条被采信
        rows = self.store.q("SELECT * FROM observations WHERE observer='n1'")
        self.assertEqual(len(rows), 2)  # seq1 + seq5
        reasons = [r["reason"] for r in ops.rejects(self.store)]
        self.assertEqual(reasons, ["duplicate", "seq_conflict", "regressed"])

    def test_out_of_order_arrival_lands_in_correct_interval(self):
        # 到达顺序: t=900 先到, t=100 后到 (序号仍前进)
        self.obs("n1", 1, 900, "n2", ttl=100)
        self.obs("n1", 2, 100, "n2", ttl=100)
        self.derive()
        # t=150 的历史区间里应能看到第二条证词产生的边
        edge = self.edge_at(150, "n1", "n2")
        self.assertIsNotNone(edge)
        obs = self.store.q1("SELECT * FROM observations WHERE obs_id=?",
                            (edge["winner_obs"],))
        self.assertEqual(obs["sample_time"], 100)
        # t=950 的区间来自第一条证词
        edge2 = self.edge_at(950, "n1", "n2")
        self.assertEqual(
            self.store.q1("SELECT sample_time FROM observations WHERE obs_id=?",
                          (edge2["winner_obs"],))["sample_time"], 900)


class TestAdjudication(TopoCase):
    def test_trust_beats_freshness(self):
        self.node("n1", trust=9)
        self.node("n2", trust=2)
        # n1(高可信) 旧样本报 up; n2(低可信) 新样本报 down -> 高可信胜
        self.obs("n1", 1, 100, "n2", quality=0.9)
        self.obs("n2", 1, 200, "n1", quality=0.0)
        self.derive()
        edge = self.edge_at(300, "n1", "n2")
        self.assertEqual(edge["state"], "up")
        self.assertEqual(edge["decision"], "conflict-state")
        self.assertIn("trust=9", edge["rationale"])
        # 双方证词都留存
        self.assertIsNotNone(edge["obs_a"])
        self.assertIsNotNone(edge["obs_b"])

    def test_freshness_breaks_equal_trust(self):
        self.obs("n1", 1, 100, "n2", quality=0.9)   # 旧: up
        self.obs("n2", 1, 500, "n1", quality=0.0)   # 新: down
        self.derive()
        self.assertEqual(self.edge_at(600, "n1", "n2")["state"], "down")

    def test_medium_down_wins(self):
        # 以太网: 任一端报 down 即判 down, 即使对方更可信更新鲜
        self.node("n1", trust=9)
        self.obs("n1", 1, 500, "n2", medium="ethernet", quality=0.95)
        self.obs("n2", 1, 100, "n1", medium="ethernet", quality=0.0)
        self.derive()
        edge = self.edge_at(600, "n1", "n2", "ethernet")
        self.assertEqual(edge["state"], "down")
        self.assertIn("down_wins", edge["rationale"])

    def test_confirmation_required_medium(self):
        # 以太网需要两端证词: 单侧 -> unconfirmed, 不可用
        self.obs("n1", 1, 100, "n2", medium="ethernet", quality=0.9)
        self.derive()
        edge = self.edge_at(200, "n1", "n2", "ethernet")
        self.assertEqual(edge["state"], "unconfirmed")
        self.assertEqual(edge["usable"], 0)
        # radio 单侧即可确认
        self.obs("n3", 1, 100, "n4", medium="radio", quality=0.9)
        self.derive()
        self.assertEqual(self.edge_at(200, "n3", "n4")["state"], "up")

    def test_quality_conflict_recorded(self):
        self.obs("n1", 1, 100, "n2", quality=0.95)
        self.obs("n2", 1, 100, "n1", quality=0.40)
        self.derive()
        edge = self.edge_at(200, "n1", "n2")
        self.assertEqual(edge["decision"], "conflict-quality")
        conflicts = ops.conflicts_at(self.store, 200)
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(len(conflicts[0]["testimonies"]), 2)  # 双方证词留存


class TestLeaseSeparation(TopoCase):
    def test_lease_is_not_adjacency(self):
        self.node("n1"); self.node("n2")
        ingest.add_lease(self.store, "n1", 0, 10000)
        ingest.add_lease(self.store, "n2", 0, 10000)
        self.derive(force=True)
        # 两节点都"在线", 但没有任何邻接边 -> 各自孤立, 无路径
        st = ops.status_at(self.store, 500)
        self.assertEqual(sorted(st["online_by_lease"]), ["n1", "n2"])
        self.assertEqual(len(st["components"]), 2)
        res = ops.path_at(self.store, "n1", "n2", 500)
        self.assertFalse(res["ok"])


class TestFlapDamping(TopoCase):
    def _series(self, qualities, step=100):
        for i, q in enumerate(qualities, start=1):
            self.obs("n1", i, i * step, "n2", quality=q, ttl=step)

    def test_brief_flap_suppressed(self):
        # 好,坏,好: 短暂抖动只计入证据, 不翻转
        self._series([0.9, 0.9, 0.9, 0.1, 0.9, 0.9])
        self.derive()
        edge = self.edge_at(450, "n1", "n2")  # 坏样本之后的区间
        self.assertEqual(edge["usable"], 1)
        bad_epoch = self.edge_at(400, "n1", "n2")
        self.assertIn("flap suppressed", bad_epoch["damp_note"] or "")

    def test_sustained_drop_flips_down_then_recovers(self):
        # 连续 3 个坏样本越过门限 -> 不可用; 之后连续 2 个好样本恢复
        self._series([0.9, 0.9, 0.1, 0.1, 0.1, 0.9, 0.9, 0.9])
        self.derive()
        self.assertEqual(self.edge_at(550, "n1", "n2")["usable"], 0)  # 第3个坏样本后
        self.assertEqual(self.edge_at(650, "n1", "n2")["usable"], 0)  # 第1个好样本: 恢复待定
        self.assertEqual(self.edge_at(750, "n1", "n2")["usable"], 1)  # 第2个好样本: 恢复


class TestRevisionsAndSeal(TopoCase):
    def test_late_testimony_creates_new_revision_with_diff(self):
        self.obs("n1", 1, 100, "n2", ttl=2000)
        self.derive()
        rev1 = ops.revision_at(self.store, 500)["rev_id"]
        # 迟到证词: 采样时刻落在未封存历史, 改变历史视图
        self.obs("n2", 1, 300, "n3", ttl=2000)
        self.derive()
        rev2 = ops.revision_at(self.store, 500)["rev_id"]
        self.assertGreater(rev2, rev1)
        diffs = [d for d in self.store.q("SELECT * FROM revision_diffs WHERE rev_id=?",
                                         (rev2,))]
        kinds = {d["change"] for d in diffs}
        self.assertIn("added", kinds)  # n2-n3 新增
        # 旧修订留档仍可查
        self.assertTrue(ops.diff_revisions(self.store, rev1, rev2))

    def test_sealed_history_goes_to_sidecar(self):
        self.obs("n1", 1, 100, "n2", ttl=2000)
        self.derive()
        rev_before = ops.revision_at(self.store, 500)["rev_id"]
        derive_mod.seal(self.store, 1000)
        # 早于封存界限的证词 -> 旁路, 不改写既有修订
        r = self.obs("n1", 2, 500, "n3", ttl=100)
        self.assertEqual(r["status"], "sidecar")
        self.assertIn("seal boundary", r["reason"])
        self.derive()
        self.assertEqual(ops.revision_at(self.store, 500)["rev_id"], rev_before)
        self.assertTrue(ops.revision_at(self.store, 500)["sealed"])
        sc = ops.sidecar(self.store)
        self.assertEqual(len(sc), 1)
        self.assertEqual(sc[0]["sample_time"], 500)
        # 封存之后的证词正常受理并产生新修订
        self.obs("n1", 3, 1500, "n3", ttl=500)
        self.derive()
        self.assertGreater(ops.revision_at(self.store, 1600)["rev_id"], rev_before)


class TestCrashRecovery(TopoCase):
    def test_derive_resumes_from_checkpoint(self):
        for i in range(1, 4):
            self.obs("n1", i, i * 100, "n2", ttl=1000)
        self.store.crash_after = "damp"
        with self.assertRaises(jobs.CrashError):
            self.derive()
        # 崩溃后: 没有发布任何修订 (publish 未完成)
        self.assertIsNone(self.store.q1("SELECT MAX(rev_id) r FROM revisions")["r"])
        self.store.crash_after = None
        resumed = jobs.recover(self.store)
        self.assertEqual(len(resumed), 1)
        # 续跑完成: 边与路径索引同属一个修订, 无半成品
        rev = self.store.q1("SELECT MAX(rev_id) r FROM revisions")["r"]
        self.assertIsNotNone(rev)
        self.assertTrue(self.store.q("SELECT * FROM current_edges"))
        self.assertTrue(self.store.q("SELECT * FROM paths WHERE rev_id=?", (rev,)))

    def test_mid_publish_crash_leaves_no_partial_state(self):
        # 链式拓扑 n1-n2-n3, 让分量/路径/割点索引都有内容
        self.obs("n1", 1, 100, "n2", ttl=1000)
        self.obs("n2", 1, 100, "n3", ttl=1000)
        self.store.crash_mid_publish = True
        with self.assertRaises(jobs.CrashError):
            self.derive()
        # publish 事务整体回滚: 无修订、无边、无路径索引
        self.assertIsNone(self.store.q1("SELECT MAX(rev_id) r FROM revisions")["r"])
        self.assertFalse(self.store.q("SELECT * FROM current_edges"))
        self.assertFalse(self.store.q("SELECT * FROM components"))
        self.store.crash_mid_publish = False
        jobs.recover(self.store)
        rev = self.store.q1("SELECT MAX(rev_id) r FROM revisions")["r"]
        self.assertIsNotNone(rev)
        # 边、分量、路径、割点都属于同一修订
        for table in ("components", "paths", "articulation"):
            rows = self.store.q(f"SELECT DISTINCT rev_id FROM {table}")
            self.assertEqual({r["rev_id"] for r in rows}, {rev})

    def test_ingest_file_resumes_from_line_checkpoint(self):
        path = os.path.join(self.dir, "obs.jsonl")
        with open(path, "w") as f:
            for i in range(1, 8):
                f.write(json.dumps({"observer": "n1", "seq": i, "sample_time": i * 100,
                                    "neighbor": "n2", "medium": "radio",
                                    "quality": 0.9, "ttl": 1000}) + "\n")
        self.store.crash_after = "ingest-line-4"
        with self.assertRaises(jobs.CrashError):
            ingest.ingest_file(self.store, path)
        self.assertEqual(
            self.store.q1("SELECT COUNT(*) n FROM observations")["n"], 4)
        self.store.crash_after = None
        resumed = jobs.recover(self.store)
        self.assertEqual(len(resumed), 1)
        # 全部 7 条受理, 且没有重复 (每行幂等)
        self.assertEqual(
            self.store.q1("SELECT COUNT(*) n FROM observations")["n"], 7)
        self.assertFalse(ops.rejects(self.store))


class TestPathSelection(TopoCase):
    def test_lower_cost_wins_and_tie_break_is_fixed(self):
        # n1 - n2 - n4 与 n1 - n3 - n4 两条等价路径 (同介质同质量)
        for a, b in (("n1", "n2"), ("n2", "n4"), ("n1", "n3"), ("n3", "n4")):
            seq = self.store.q1("SELECT COALESCE(MAX(seq),0) s FROM observations "
                                "WHERE observer=?", (a,))["s"]
            self.obs(a, seq + 1, 100, b, quality=0.9, ttl=10000)
        self.derive()
        res = ops.path_at(self.store, "n1", "n4", 500)
        self.assertTrue(res["ok"])
        # 固定规则: 字典序最小节点序列 n1-n2-n4
        self.assertEqual(res["path"], ["n1", "n2", "n4"])
        self.assertTrue(res["tie_broken"])
        # 提高 n3-n4 质量 -> 代价更低, 稳定切换 (n3 的序号继续前进)
        self.obs("n3", 2, 200, "n4", quality=1.0, ttl=10000)
        self.derive()
        res2 = ops.path_at(self.store, "n1", "n4", 500)
        self.assertEqual(res2["path"], ["n1", "n3", "n4"])
        self.assertLess(res2["cost_micro"], res["cost_micro"])

    def test_path_explanation_traces_to_observations(self):
        self.obs("n1", 1, 100, "n2", quality=0.9, ttl=10000)
        self.obs("n2", 1, 100, "n3", quality=0.8, ttl=10000)
        self.derive()
        res = ops.path_at(self.store, "n1", "n3", 500)
        self.assertTrue(res["ok"])
        self.assertEqual(len(res["hops"]), 2)
        for hop in res["hops"]:
            self.assertIn("testimony", hop)
            self.assertIn("observer", hop["testimony"])
        # 沿边追溯
        trace = ops.edge_trace(self.store, "n1", "n2")
        self.assertTrue(trace)
        self.assertEqual(trace[0]["testimonies"][0]["observer"], "n1")


class TestFailureDomain(TopoCase):
    def _chain(self):
        # n1 - n2 - n3(exit) ; n2 是割点
        self.node("n3", exit_=True)
        self.obs("n1", 1, 100, "n2", ttl=10000)
        self.obs("n2", 1, 100, "n3", ttl=10000)
        self.derive()

    def test_articulation_points(self):
        self._chain()
        self.assertEqual(ops.cutpoints_at(self.store, 500), ["n2"])

    def test_failure_domain_members_and_lost_exits(self):
        self._chain()
        fd = failure_mod.failure_domain(self.store, "n2", 500)
        affected = {m["member"]: m for m in fd["members"]
                    if m["lost_peers"] > 0 or m["lost_exits"]}
        self.assertIn("n1", affected)
        self.assertEqual(affected["n1"]["lost_exits"], ["n3"])
        # n2 不是割点之外的普通节点失联影响小
        fd2 = failure_mod.failure_domain(self.store, "n1", 500)
        self.assertFalse([m for m in fd2["members"]
                          if m["lost_peers"] > 0 or m["lost_exits"]])

    def test_failure_domain_job_recovers(self):
        self._chain()
        self.store.crash_after = "snapshot"
        with self.assertRaises(jobs.CrashError):
            failure_mod.failure_domain(self.store, "n2", 500)
        self.store.crash_after = None
        resumed = jobs.recover(self.store)
        self.assertEqual(len(resumed), 1)
        rows = self.store.q("SELECT * FROM failure_domains")
        self.assertTrue(rows)


if __name__ == "__main__":
    unittest.main()
