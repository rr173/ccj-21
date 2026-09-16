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
        # 从未下达的命令不冻结、不要求核实：原样留在队列由新通道领取
        self.assertEqual(cmds[queued["version"]]["state"], config.CMD_QUEUED)

        # 在在途命令对账完成前，低版本结果未知会阻塞新命令的派发（不跳版本）
        blocked = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(blocked["command"]["version"], inflight["version"])
        self.assertTrue(blocked.get("await_reconcile"))

        # 设备只对账在途 v2（已执行）；从未发送的 v3 无需出现在对账里
        r = self.db.reconcile("dev1", "c2", 1, sec, [
            {"version": inflight["version"], "done": True,
             "result": "APPLIED"},
        ], idem_key="rc-both")
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[inflight["version"]], "RECONCILE_DONE")
        cmds = versions(self.db.list_commands("dev1"))
        self.assertEqual(cmds[inflight["version"]]["state"], config.CMD_ACKED)
        self.assertEqual(cmds[inflight["version"]]["ack_source"],
                         "RECONCILE_DONE")
        # v3 立即由新通道领取并派发（产生绑定 gen2 的 attempt）
        p = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p["command"]["version"], queued["version"])
        self.assertEqual(p["session_generation"], s2["generation"])
        timeline = self.db.command_timeline(inflight["command_id"])
        types = [e["event_type"] for e in timeline["events"]]
        self.assertEqual(types,
                         ["CREATED", "DISPATCHED", "MARKED_RECONCILING",
                          "RECONCILE_DONE"])

    def test_reconcile_not_done_redispatches_to_new_session(self):
        sec, acked, inflight, queued = self._setup_three_commands()
        self.open_session(connection_no="c2", lease=1000)
        # 只需对已下达、结果未知的在途命令对账；从未发送的 v3 即使出现在
        # 对账批次里也只回 STILL_QUEUED（它从未离开队列，无副作用）
        r = self.db.reconcile("dev1", "c2", 1, sec, [
            {"version": inflight["version"], "done": False},
            {"version": queued["version"], "done": False},
        ])
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[inflight["version"]],
                         "RETRY_TO_NEW_SESSION")
        self.assertEqual(outcomes[queued["version"]], "STILL_QUEUED")
        # 低版本先派
        p1 = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p1["command"]["version"], inflight["version"])
        self.assertEqual(p1["session_generation"], 2)
        self.db.ack("dev1", "c2", 1, sec, version=inflight["version"])
        # 从未下达的 v3 无需核实，直接由新通道领取
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
        # 从未发送：原样留队，不冻结待核实
        self.assertEqual(cmds[queued["version"]]["state"], config.CMD_QUEUED)
        self.assertEqual(cmds[inflight["version"]]["state"],
                         config.CMD_RECONCILING)
        self.assertEqual(cmds[acked["version"]]["state"], config.CMD_ACKED)
        # 新连接上来：在途命令先对账；未发送命令等在途核实后直接派发
        s2 = self.open_session(connection_no="c2", lease=1000)
        blocked = self.db.poll("dev1", "c2", 1, sec)
        self.assertTrue(blocked.get("await_reconcile"))
        self.db.reconcile("dev1", "c2", 1, sec, [
            {"version": inflight["version"], "done": True},
        ])
        p = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p["command"]["version"], queued["version"])
        self.assertEqual(p["session_generation"], s2["generation"])
        # 从未下达的命令只在新通道留下唯一一次派发尝试
        tl = self.db.command_timeline(queued["command_id"])
        self.assertEqual([a["generation"] for a in tl["attempts"]], [2])

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
        # 对账表态：在途 v2 已执行；从未发送的 v3 即使入参带了也只是 STILL_QUEUED
        r = self.db.reconcile(
            "dev1", "c2", 2, rot["new_credential_secret"], [
                {"version": inflight["version"], "done": True},
                {"version": queued["version"], "done": False},
            ])
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[inflight["version"]], "RECONCILE_DONE")
        self.assertEqual(outcomes[queued["version"]], "STILL_QUEUED")
        p = self.db.poll("dev1", "c2", 2, rot["new_credential_secret"])
        self.assertEqual(p["command"]["version"], queued["version"])
        self.db.ack("dev1", "c2", 2, rot["new_credential_secret"],
                    version=queued["version"])
        # v1 已确认，绝不重发
        p2 = self.db.poll("dev1", "c2", 2, rot["new_credential_secret"])
        self.assertIsNone(p2["command"])


class NeverDispatchedRetentionTests(LeaseTestCase):
    """从未下达（QUEUED）的命令在通道切换时必须原样留队，新通道直接领取。"""

    def _two_commands_one_sent_one_queued(self):
        self.provision()
        self.open_session(connection_no="c1", lease=1000)
        sec = self._secret("dev1", 1)
        sent = self.db.issue_command("dev1", {"op": "sent"})
        self.db.poll("dev1", "c1", 1, sec)          # SENT，结果未知
        queued = self.db.issue_command("dev1", {"op": "never-sent"})
        return sec, sent, queued

    def test_takeover_keeps_never_dispatched_queued_and_delivers_directly(self):
        sec, sent, queued = self._two_commands_one_sent_one_queued()
        self.db.takeover("dev1")
        s2 = self.open_session(connection_no="c2", lease=1000)
        cmds = versions(self.db.list_commands("dev1"))
        self.assertEqual(cmds[sent["version"]]["state"],
                         config.CMD_RECONCILING)
        self.assertEqual(cmds[queued["version"]]["state"], config.CMD_QUEUED)

        # 低版本在途未核实：新通道第一拍被提示对账，不能跳版本
        blocked = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(blocked["command"]["version"], sent["version"])
        self.assertTrue(blocked.get("await_reconcile"))

        # 只需核实真正下达过的 v1；v2 从未下达，无需出现在对账里
        r = self.db.reconcile("dev1", "c2", 1, sec,
                              [{"version": sent["version"], "done": False}])
        self.assertEqual(r["results"][0]["outcome"], "RETRY_TO_NEW_SESSION")

        # v1 重投并确认后，v2 立刻由新通道领取，没有任何待核实标记
        p1 = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p1["command"]["version"], sent["version"])
        self.assertEqual(p1["session_generation"], s2["generation"])
        self.db.ack("dev1", "c2", 1, sec, version=sent["version"])
        p2 = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p2["command"]["version"], queued["version"])
        self.assertFalse(p2.get("await_reconcile"))
        self.assertFalse(p2.get("duplicate_dispatch"))
        # 从未下达的命令只有新通道这一次派发尝试
        tl = self.db.command_timeline(queued["command_id"])
        self.assertEqual([a["generation"] for a in tl["attempts"]],
                         [s2["generation"]])
        types = [e["event_type"] for e in tl["events"]]
        self.assertEqual(types, ["CREATED", "DISPATCHED"])

    def test_only_queued_command_after_takeover_delivers_without_reconcile(self):
        self.provision()
        self.open_session(connection_no="c1", lease=1000)
        sec = self._secret("dev1", 1)
        cmd = self.db.issue_command("dev1", {"op": "only-queued"})
        self.db.takeover("dev1")
        self.open_session(connection_no="c2", lease=1000)
        # 没有任何在途命令需要核实：第一拍直接派发
        p = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p["command"]["version"], cmd["version"])
        self.assertFalse(p.get("await_reconcile"))

    def test_legacy_queued_unknown_migrated_back_to_queued_on_restart(self):
        sec, sent, queued = self._two_commands_one_sent_one_queued()
        # 手工模拟旧版本留下的冻结状态
        self.db._conn.execute(
            "UPDATE commands SET state=? WHERE command_id=?",
            (config.CMD_QUEUED_UNKNOWN, queued["command_id"]))
        self.reopen()
        cmds = versions(self.db.list_commands("dev1"))
        self.assertEqual(cmds[queued["version"]]["state"], config.CMD_QUEUED)
        self.db.online("dev1", "c2", 1, sec, 1000)
        # 在途 v1 仍需核实；迁移回来的 v2 无需核实
        self.db.reconcile("dev1", "c2", 1, sec,
                          [{"version": sent["version"], "done": True}])
        p = self.db.poll("dev1", "c2", 1, sec)
        self.assertEqual(p["command"]["version"], queued["version"])
        tl = self.db.command_timeline(queued["command_id"])
        self.assertIn("RELEASED_TO_QUEUE",
                      [e["event_type"] for e in tl["events"]])


class StaleChannelReplayTests(LeaseTestCase):
    """通道切换后，旧通道携带原幂等键的重放必须在鉴权阶段被拦截留痕。"""

    def _old_channel_succeeded_once(self):
        self.provision()
        sec = self._secret("dev1", 1)
        self.open_session(connection_no="c1", lease=1000)
        return sec

    def _switch(self):
        self.db.takeover("dev1")
        self.open_session(connection_no="c2", lease=1000)

    def test_old_channel_report_replay_rejected_with_reason(self):
        sec = self._old_channel_succeeded_once()
        r1 = self.db.report_state("dev1", "c1", 1, sec, 1, {"x": 1},
                                  idem_key="rep-1")
        self.assertTrue(r1["accepted"])
        self._switch()
        # 旧通道重发同一条实况（同幂等键）：不得回放成功
        with self.assertRaises(LeaseError) as ctx:
            self.db.report_state("dev1", "c1", 1, sec, 1, {"x": 1},
                                 idem_key="rep-1")
        self.assertEqual(ctx.exception.code, "SESSION_SUPERSEDED")
        rej = self.db.list_rejected("dev1", kind="REPORT")
        self.assertEqual(rej[0]["reason"], "SESSION_SUPERSEDED")
        # 报告态没有被旧通道重放覆盖
        reported = self.db.device_read_model("dev1")["reported_state"]
        self.assertEqual(reported["generation"], 1)

    def test_old_channel_ack_replay_rejected_with_reason(self):
        sec = self._old_channel_succeeded_once()
        cmd = self.db.issue_command("dev1", {"op": 1})
        self.db.poll("dev1", "c1", 1, sec)
        a1 = self.db.ack("dev1", "c1", 1, sec, version=cmd["version"],
                         idem_key="ack-1")
        self.assertEqual(a1["state"], config.CMD_ACKED)
        self._switch()
        with self.assertRaises(LeaseError) as ctx:
            self.db.ack("dev1", "c1", 1, sec, version=cmd["version"],
                        idem_key="ack-1")
        self.assertEqual(ctx.exception.code, "SESSION_SUPERSEDED")
        reasons = self.rejected_reasons()
        self.assertIn("SESSION_SUPERSEDED", reasons)

    def test_old_channel_reconcile_replay_rejected(self):
        sec = self._old_channel_succeeded_once()
        cmd = self.db.issue_command("dev1", {"op": 1})
        self.db.poll("dev1", "c1", 1, sec)
        # 旧通道活跃时先成功对账一次（幂等账本留下成功响应）
        r = self.db.reconcile("dev1", "c1", 1, sec,
                              [{"version": cmd["version"], "done": True}],
                              idem_key="rec-1")
        self.assertEqual(r["results"][0]["outcome"], "RECONCILE_DONE")
        self._switch()
        # 切换后旧通道重放同一次对账：不得回放成功
        with self.assertRaises(LeaseError) as ctx:
            self.db.reconcile("dev1", "c1", 1, sec,
                              [{"version": cmd["version"], "done": True}],
                              idem_key="rec-1")
        self.assertEqual(ctx.exception.code, "SESSION_SUPERSEDED")
        self.assertIn("RECONCILE",
                      {r["kind"] for r in self.db.list_rejected("dev1")})

    def test_expired_channel_renew_replay_rejected_after_switch(self):
        self.provision()
        sec = self._secret("dev1", 1)
        self.open_session(connection_no="c1", lease=10)
        r1 = self.db.renew("dev1", "c1", 1, sec, 10, idem_key="ren-1")
        self.assertEqual(r1["state"], config.SE_ACTIVE)
        self.clk.advance(11)
        self.db.sweep()
        # 租约已过期：即便带着成功过的幂等键也不能续期
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, sec, 10, idem_key="ren-1")
        self.assertEqual(ctx.exception.code, "SESSION_EXPIRED")
        rej = self.db.list_rejected("dev1", kind="RENEW")
        self.assertEqual(rej[0]["reason"], "SESSION_EXPIRED")

    def test_current_channel_idempotent_replay_still_works(self):
        # 鉴权前置不能破坏正常重试：当前可写会话的同键重放仍然幂等
        sec = self._old_channel_succeeded_once()
        a1 = self.db.report_state("dev1", "c1", 1, sec, 1, {"x": 1},
                                  idem_key="rep-ok")
        a2 = self.db.report_state("dev1", "c1", 1, sec, 1, {"x": 1},
                                  idem_key="rep-ok")
        self.assertTrue(a2.get("idempotent_replay"))
        self.assertEqual(a1, {k: v for k, v in a2.items()
                              if k != "idempotent_replay"})
