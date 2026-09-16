"""服务重启恢复：租约、轮换进度、当前会话、在途对账命令、幂等账本。"""
from __future__ import annotations

from lease import config
from lease.store import LeaseError

from helpers import LeaseTestCase


class RestartTests(LeaseTestCase):
    def test_active_lease_restored_and_can_renew(self):
        self.provision()
        self.open_session(lease=100)
        self.clk.advance(40)
        self.reopen()
        rm = self.db.device_read_model("dev1")
        self.assertTrue(rm["online"])
        self.assertIsNotNone(rm["current_session"])
        rn = self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 100)
        self.assertEqual(rn["state"], config.SE_ACTIVE)

    def test_expired_session_not_resurrected_after_restart(self):
        self.provision()
        self.open_session(lease=10)
        self.clk.advance(20)
        self.reopen()
        # 重启后第一拍清扫：过期会话结束，不被复活
        self.db.sweep()
        self.assertFalse(self.db.device_read_model("dev1")["online"])
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 10)
        self.assertEqual(ctx.exception.code, "SESSION_EXPIRED")
        with self.assertRaises(LeaseError) as ctx:
            self.open_session(connection_no="c1")
        self.assertEqual(ctx.exception.code, "DEAD_CONNECTION_REUSED")
        # 新连接得到新一代
        s = self.open_session(connection_no="c2")
        self.assertEqual(s["generation"], 2)

    def test_rotation_progress_restored_and_grace_fires(self):
        self.provision()
        rot_secret = None
        self.open_session(connection_no="c1", lease=10000)
        rot = self.db.start_rotation("dev1", 50, "rot")
        rot_secret = rot["new_credential_secret"]
        self.db.close()
        self.clk.advance(60)
        self.reopen()
        rots = self.db.list_rotations("dev1")
        # 尚未 sweep：仍 ROTATING；凭证状态也恢复
        self.assertEqual(rots[0]["state"], config.ROT_ROTATING)
        self.db.sweep()
        rots = self.db.list_rotations("dev1")
        self.assertEqual(rots[0]["state"], config.ROT_OLD_REVOKED)
        statuses = {c["version"]: c["status"]
                    for c in self.db.list_credentials("dev1")}
        self.assertEqual(statuses[1], config.CRED_REVOKED)
        self.assertEqual(statuses[2], config.CRED_ACTIVE)
        # 新凭证连接后轮换完成
        s = self.open_session(connection_no="c2", cred_version=2,
                              secret=rot_secret, lease=100)
        self.assertEqual(s["generation"], 2)
        self.assertEqual(
            self.db.list_rotations("dev1")[0]["state"],
            config.ROT_COMPLETED)

    def test_current_session_and_reconciling_commands_restored(self):
        self.provision()
        self.open_session(connection_no="c1", lease=1000)
        c1 = self.db.issue_command("dev1", {"a": 1})
        self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))
        c2 = self.db.issue_command("dev1", {"b": 2})
        self.db.takeover("dev1", idem_key="tk")
        self.open_session(connection_no="c2", lease=1000)
        self.reopen()
        rm = self.db.device_read_model("dev1")
        self.assertTrue(rm["online"])
        self.assertEqual(rm["current_session"]["connection_no"], "c2")
        self.assertEqual(rm["current_generation"], 2)
        cmds = {c["version"]: c for c in self.db.list_commands("dev1")}
        self.assertEqual(cmds[c1["version"]]["state"],
                         config.CMD_RECONCILING)
        self.assertEqual(cmds[c2["version"]]["state"],
                         config.CMD_QUEUED_UNKNOWN)
        # 接管记录恢复为已完成
        tk = self.db.list_takeovers("dev1")[0]
        self.assertEqual(tk["state"], config.TK_COMPLETED)
        # 对账在重启后仍可完成；c2 未执行 -> 回 QUEUED
        r = self.db.reconcile("dev1", "c2", 1, self._secret("dev1", 1), [
            {"version": c1["version"], "done": True},
            {"version": c2["version"], "done": False},
        ])
        outcomes = {x["version"]: x["outcome"] for x in r["results"]}
        self.assertEqual(outcomes[c1["version"]], "RECONCILE_DONE")
        self.assertEqual(outcomes[c2["version"]], "RETRY_TO_NEW_SESSION")
        p = self.db.poll("dev1", "c2", 1, self._secret("dev1", 1))
        self.assertEqual(p["command"]["version"], c2["version"])

    def test_idempotency_ledger_survives_restart(self):
        self.provision()
        self.open_session(idem_key="on-1")
        self.db.issue_command("dev1", {"x": 1}, idem_key="cmd-1")
        self.reopen()
        # 同一上线幂等键：返回原会话而不是再开一代
        s = self.open_session(connection_no="c1", idem_key="on-1")
        self.assertTrue(s.get("idempotent_replay"))
        self.assertEqual(s["generation"], 1)
        c = self.db.issue_command("dev1", {"x": 1}, idem_key="cmd-1")
        self.assertTrue(c.get("duplicate"))
        self.assertEqual(c["version"], 1)

    def test_rejected_history_survives_restart(self):
        self.provision()
        self.open_session(connection_no="c1")
        self.open_session(connection_no="c2")
        with self.assertRaises(LeaseError):
            self.db.report_state("dev1", "c1", 1,
                                 self._secret("dev1", 1), 1, {"x": 1})
        self.reopen()
        reasons = self.rejected_reasons()
        self.assertIn("SESSION_SUPERSEDED", reasons)
        rejected = self.db.list_rejected("dev1", kind="REPORT")
        self.assertTrue(rejected)
        self.assertEqual(rejected[0]["kind"], "REPORT")

    def test_generation_never_goes_backwards_after_restart(self):
        self.provision()
        self.open_session(connection_no="c1")
        self.open_session(connection_no="c2")
        self.db.takeover("dev1")
        self.open_session(connection_no="c3")  # gen 3
        self.reopen()
        s = self.open_session(connection_no="c4")
        self.assertEqual(s["generation"], 4)
