"""命令派发代次绑定、接管/轮换时的命令处置、按版本对账。"""
from __future__ import annotations

from lease import config
from lease.store import LeaseError

from helpers import LeaseTestCase


def versions(commands):
    return {c["version"]: c for c in commands}


class CommandDispatchTests(LeaseTestCase):
    def test_issue_idempotent_and_monotonic_versions(self):
        self.provision()
        c1 = self.db.issue_command("dev1", {"a": 1}, idem_key="c1")
        c1b = self.db.issue_command("dev1", {"a": 1}, idem_key="c1")
        self.assertEqual(c1["command_id"], c1b["command_id"])
        self.assertTrue(c1b.get("duplicate"))
        c2 = self.db.issue_command("dev1", {"a": 2})
        self.assertEqual(c1["version"], 1)
        self.assertEqual(c2["version"], 2)

    def test_poll_dispatches_lowest_queued_and_binds_generation(self):
        self.provision()
        self.open_session(connection_no="c1")
        self.db.issue_command("dev1", {"v": 1})
        self.db.issue_command("dev1", {"v": 2})
        p1 = self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))
        self.assertEqual(p1["command"]["version"], 1)
        self.assertEqual(p1["command"]["dispatch_generation"], 1)
        # 在第一条 SENT 时不跳发第二条
        p_again = self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))
        self.assertEqual(p_again["command"]["version"], 1)
        self.assertTrue(p_again.get("duplicate_dispatch"))
        self.db.ack("dev1", "c1", 1, self._secret("dev1", 1), version=1)
        p2 = self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))
        self.assertEqual(p2["command"]["version"], 2)

    def test_duplicate_ack_idempotent(self):
        self.provision()
        self.open_session()
        self.db.issue_command("dev1", {"v": 1})
        self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))
        a1 = self.db.ack("dev1", "c1", 1, self._secret("dev1", 1),
                         version=1, code="APPLIED", idem_key="a1")
        self.assertFalse(a1["duplicate_ack"])
        a2 = self.db.ack("dev1", "c1", 1, self._secret("dev1", 1),
                         version=1)
        self.assertTrue(a2["duplicate_ack"])

    def test_ack_generation_mismatch_after_new_dispatch(self):
        # 命令重投到新一代后，旧会话若延迟 ACK 到达必须被拒绝
        self.provision()
        self.open_session(connection_no="c1")
        cmd = self.db.issue_command("dev1", {"v": 1})
        self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))  # gen1 SENT
        # 接管 -> 在途转 RECONCILING
        self.db.takeover("dev1")
        self.open_session(connection_no="c2")  # gen2
        # 新会话对账：设备没执行 -> 回 QUEUED -> gen2 重新派发 -> ACK
        r = self.db.reconcile("dev1", "c2", 1, self._secret("dev1", 1),
                              [{"version": cmd["version"], "done": False}])
        self.assertEqual(r["results"][0]["outcome"], "RETRY_TO_NEW_SESSION")
        self.db.poll("dev1", "c2", 1, self._secret("dev1", 1))
        self.db.ack("dev1", "c2", 1, self._secret("dev1", 1),
                    version=cmd["version"])
        # 旧会话延迟 ACK：在鉴权阶段即被拒（SESSION_SUPERSEDED）
        with self.assertRaises(LeaseError) as ctx:
            self.db.ack("dev1", "c1", 1, self._secret("dev1", 1),
                        version=cmd["version"])
        self.assertEqual(ctx.exception.code, "SESSION_SUPERSEDED")


class SessionSwitchCommandTests(LeaseTestCase):
    def _setup_three_commands(self):
        self.provision()
        self.open_session(connection_no="c1", lease=1000)
        acked = self.db.issue_command("dev1", {"op": "acked"})
        inflight = self.db.issue_command("dev1", {"op": "inflight"})
        queued = self.db.issue_command("dev1", {"op": "queued"})
        sec = self._secret("dev1", 1)
        self.db.poll("dev1", "c1", 1, sec)           # acked SENT
        self.db.ack("dev1", "c1", 1, sec, version=acked["version"])
        self.db.poll("dev1", "c1", 1, sec)           # inflight SENT
        # queued 保持 QUEUED
        return sec, acked, inflight, queued

    def test_concurrent_online_partitions_commands(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        s2 = self.open_session(connection_no="c2", lease=1000)
        cmds = versions(self.db.list_commands("dev1"))
        self.assertEqual(cmds[acked["version"]]["state"], config.CMD_ACKED)
        self.assertEqual(cmds[inflight["version"]]["state"],
                         config.CMD_RECONCILING)
        self.assertEqual(cmds[queued["version"]]["state"],
                         config.CMD_QUEUED_UNKNOWN)

        # 在对账完成前，旧版本结果未知会阻塞新命令的派发（不跳版本）
        blocked = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(blocked["command"]["version"], inflight["version"])
        self.assertTrue(blocked.get("await_reconcile"))

        # 设备一次性对账：在途 v2 已执行；未发送 v3 未执行 -> 回 QUEUED 派发
        r = self.db.reconcile("dev1", "c2", 1, sec, [
            {"version": inflight["version"], "done": True,
             "result": "APPLIED"},
            {"version": queued["version"], "done": False},
        ], idem_key="rc-both")
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[inflight["version"]], "RECONCILE_DONE")
        self.assertEqual(outcomes[queued["version"]], "RETRY_TO_NEW_SESSION")
        cmds = versions(self.db.list_commands("dev1"))
        self.assertEqual(cmds[inflight["version"]]["state"], config.CMD_ACKED)
        self.assertEqual(cmds[inflight["version"]]["ack_source"],
                         "RECONCILE_DONE")
        p = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p["command"]["version"], queued["version"])
        timeline = self.db.command_timeline(inflight["command_id"])
        types = [e["event_type"] for e in timeline["events"]]
        self.assertEqual(types,
                         ["CREATED", "DISPATCHED", "MARKED_RECONCILING",
                          "RECONCILE_DONE"])

    def test_reconcile_not_done_redispatches_to_new_session(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        self.open_session(connection_no="c2", lease=1000)
        r = self.db.reconcile("dev1", "c2", 1, sec, [
            {"version": inflight["version"], "done": False},
            {"version": queued["version"], "done": False},
        ])
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[inflight["version"]],
                         "RETRY_TO_NEW_SESSION")
        self.assertEqual(outcomes[queued["version"]],
                         "RETRY_TO_NEW_SESSION")
        # 低版本先派
        p1 = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p1["command"]["version"], inflight["version"])
        self.assertEqual(p1["session_generation"], 2)
        self.db.ack("dev1", "c2", 1, sec, version=inflight["version"])
        p2 = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p2["command"]["version"], queued["version"])
        # v2 两次派发尝试分别绑定两代会话
        tl = self.db.command_timeline(inflight["command_id"])
        gens = [a["generation"] for a in tl["attempts"]]
        self.assertEqual(gens, [1, 2])

    def test_ack_on_reconciling_command_rejected_until_reconcile(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        self.open_session(connection_no="c2", lease=1000)
        with self.assertRaises(LeaseError) as ctx:
            self.db.ack("dev1", "c2", 1, sec, version=inflight["version"])
        self.assertEqual(ctx.exception.code, "COMMAND_AWAIT_RECONCILE")
        self.assertIn("COMMAND_AWAIT_RECONCILE", self.rejected_reasons())

    def test_reconcile_is_idempotent(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        self.open_session(connection_no="c2", lease=1000)
        args = ("dev1", "c2", 1, sec,
                [{"version": inflight["version"], "done": True}], "rc-1")
        r1 = self.db.reconcile(*args)
        r2 = self.db.reconcile(*args)
        self.assertTrue(r2.get("idempotent_replay"))
        self.assertEqual(r1["results"], r2["results"])
        # 不带幂等键的重复对账：已完成的回 ALREADY_DONE，无副作用
        r3 = self.db.reconcile(
            "dev1", "c2", 1, sec,
            [{"version": inflight["version"], "done": True}])
        self.assertEqual(r3["results"][0]["outcome"], "ALREADY_DONE")

    def test_queued_commands_survive_takeover_rotation_and_expiry(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        # 租约到期
        self.clk.advance(1001)
        self.db.sweep()
        cmds = versions(self.db.list_commands("dev1"))
        self.assertEqual(cmds[queued["version"]]["state"],
                         config.CMD_QUEUED_UNKNOWN)
        self.assertEqual(cmds[inflight["version"]]["state"],
                         config.CMD_RECONCILING)
        self.assertEqual(cmds[acked["version"]]["state"], config.CMD_ACKED)
        # 新连接上来：先对账表态，未发送命令才派发
        s2 = self.open_session(connection_no="c2", lease=1000)
        blocked = self.db.poll("dev1", "c2", 1, sec)
        self.assertTrue(blocked.get("await_reconcile"))
        self.db.reconcile("dev1", "c2", 1, sec, [
            {"version": inflight["version"], "done": True},
            {"version": queued["version"], "done": False},
        ])
        p = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p["command"]["version"], queued["version"])
        self.assertEqual(p["session_generation"], s2["generation"])

    def test_rotation_grace_then_reconcile_flow(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        rot = self.db.start_rotation("dev1", 50, "rot")
        # 宽限期到期旧会话被杀
        self.clk.advance(51)
        self.db.sweep()
        # 新凭证上线（gen2）
        s2 = self.open_session(connection_no="c2", cred_version=2,
                               secret=rot["new_credential_secret"], lease=1000)
        self.assertEqual(s2["generation"], 2)
        # 对账表态：在途 v2 已执行；从未发送的 v3 未执行 -> 回 QUEUED 派发
        r = self.db.reconcile(
            "dev1", "c2", 2, rot["new_credential_secret"], [
                {"version": inflight["version"], "done": True},
                {"version": queued["version"], "done": False},
            ])
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[inflight["version"]], "RECONCILE_DONE")
        p = self.db.poll("dev1", "c2", 2, rot["new_credential_secret"])
        self.assertEqual(p["command"]["version"], queued["version"])
        self.db.ack("dev1", "c2", 2, rot["new_credential_secret"],
                    version=queued["version"])
        # v1 已确认，绝不重发
        p2 = self.db.poll("dev1", "c2", 2, rot["new_credential_secret"])
        self.assertIsNone(p2["command"])
