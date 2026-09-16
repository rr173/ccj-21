"""凭证轮换：轮换期旧凭证受限、宽限期、管理员撤销、迁移完成。"""
from __future__ import annotations

from lease import config
from lease.store import LeaseError

from helpers import LeaseTestCase


class RotationTests(LeaseTestCase):
    def _start(self, grace=100.0, idem_key="rot-1"):
        rot = self.db.start_rotation("dev1", grace, idem_key)
        return rot

    def test_rotation_creates_new_active_and_old_rotating(self):
        self.provision()
        self.open_session()
        rot = self._start()
        self.assertEqual(rot["old_version"], 1)
        self.assertEqual(rot["new_version"], 2)
        self.assertEqual(rot["state"], config.ROT_ROTATING)
        statuses = {c["version"]: c["status"]
                    for c in self.db.list_credentials("dev1")}
        self.assertEqual(statuses, {1: config.CRED_ROTATING,
                                    2: config.CRED_ACTIVE})

    def test_rotation_is_idempotent(self):
        self.provision()
        r1 = self._start()
        r2 = self._start()
        self.assertTrue(r2.get("duplicate"))
        self.assertEqual(r1["rotation_id"], r2["rotation_id"])
        r3 = self.db.start_rotation("dev1", 100.0, "rot-1")
        self.assertTrue(r3.get("idempotent_replay"))

    def test_old_credential_new_connection_rejected_during_rotation(self):
        self.provision()
        self.open_session(connection_no="c1")
        rot = self._start()
        with self.assertRaises(LeaseError) as ctx:
            self.open_session(connection_no="cX", cred_version=1,
                              secret=self._secret("dev1", 1))
        self.assert_rejected(ctx, "OLD_CREDENTIAL_NEW_CONNECTION")
        # 但新凭证新连接允许
        s = self.open_session(connection_no="c2", cred_version=2,
                              secret=rot["new_credential_secret"])
        self.assertEqual(s["generation"], 2)

    def test_old_credential_can_only_renew_existing_session(self):
        self.provision()
        self.open_session(connection_no="c1", lease=30)
        self._start()
        # 续旧会话：明确放行
        rn = self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 30)
        self.assertTrue(rn["renewed_with_old_credential"])
        # 旧会话的 poll / 上报 / ACK 在轮换期内照常（它仍然可写）
        self.db.report_state("dev1", "c1", 1, self._secret("dev1", 1),
                             1, {"ok": True})
        # 开新连接不行
        with self.assertRaises(LeaseError):
            self.open_session(connection_no="c9", cred_version=1,
                              secret=self._secret("dev1", 1))

    def test_migration_completes_rotation_and_revokes_old_early(self):
        self.provision()
        self.open_session(connection_no="c1", lease=100)
        rot = self._start(grace=100)
        self.clk.advance(10)
        s2 = self.open_session(connection_no="c2", cred_version=2,
                               secret=rot["new_credential_secret"], lease=100)
        self.assertEqual(s2["rotation_completed"]["rotation_id"],
                         rot["rotation_id"])
        statuses = {c["version"]: c["status"]
                    for c in self.db.list_credentials("dev1")}
        self.assertEqual(statuses[1], config.CRED_REVOKED)
        rots = self.db.list_rotations("dev1")
        self.assertEqual(rots[0]["state"], config.ROT_COMPLETED)
        # 迁移后旧凭证续期立即被拒
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 30)
        self.assert_rejected(ctx, "CREDENTIAL_REVOKED")

    def test_grace_expiry_revokes_old_credential_kills_session_keeps_commands(self):
        self.provision()
        self.open_session(connection_no="c1", lease=1000)
        cmd = self.db.issue_command("dev1", {"op": "reboot"})
        self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))  # SENT
        queued = self.db.issue_command("dev1", {"op": "tune"})     # QUEUED
        self._start(grace=50)
        self.clk.advance(51)
        out = self.db.sweep()
        self.assertEqual(out["grace_expirations"], 1)
        # 旧会话失效
        with self.assertRaises(LeaseError) as ctx:
            self.db.report_state("dev1", "c1", 1,
                                 self._secret("dev1", 1), 1, {"a": 1})
        self.assert_rejected(ctx, "CREDENTIAL_REVOKED")
        # 命令不丢：在途转对账，未发送的原样留队由新通道直接领取
        cmds = {c["version"]: c for c in self.db.list_commands("dev1")}
        self.assertEqual(cmds[cmd["version"]]["state"],
                         config.CMD_RECONCILING)
        self.assertEqual(cmds[queued["version"]]["state"],
                         config.CMD_QUEUED)
        rots = self.db.list_rotations("dev1")
        self.assertEqual(rots[0]["state"], config.ROT_OLD_REVOKED)

    def test_admin_revoke_immediate(self):
        self.provision()
        self.open_session(connection_no="c1", lease=1000)
        rot = self._start(grace=10000)
        rv = self.db.revoke_old_credential("dev1", idem_key="rv-1")
        self.assertEqual(rv["state"], config.ROT_OLD_REVOKED)
        # 幂等：再次撤销返回同一状态
        rv2 = self.db.revoke_old_credential("dev1", idem_key="rv-1")
        self.assertTrue(rv2.get("idempotent_replay"))
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 10)
        self.assert_rejected(ctx, "CREDENTIAL_REVOKED")
        # 宽限期再到期不重复处理
        self.clk.advance(20000)
        self.assertEqual(self.db.sweep()["grace_expirations"], 0)

    def test_revoke_without_rotation_conflicts(self):
        self.provision()
        with self.assertRaises(LeaseError) as ctx:
            self.db.revoke_old_credential("dev1")
        self.assert_rejected(ctx, "NO_ROTATION_IN_PROGRESS")

    def test_new_credential_survives_and_second_rotation_chains(self):
        self.provision()
        self.open_session()
        rot1 = self._start()
        s2 = self.open_session(connection_no="c2", cred_version=2,
                               secret=rot1["new_credential_secret"])
        self.assertEqual(s2["generation"], 2)
        rot2 = self.db.start_rotation("dev1", 100, "rot-2")
        self.assertEqual(rot2["old_version"], 2)
        self.assertEqual(rot2["new_version"], 3)
        # 此时 v2 只能续期，v3 才能开新连接
        with self.assertRaises(LeaseError):
            self.open_session(connection_no="cX", cred_version=2,
                              secret=rot1["new_credential_secret"])
        s3 = self.open_session(connection_no="c3", cred_version=3,
                               secret=rot2["new_credential_secret"])
        self.assertEqual(s3["generation"], 3)
        statuses = {c["version"]: c["status"]
                    for c in self.db.list_credentials("dev1")}
        self.assertEqual(statuses, {1: config.CRED_REVOKED,
                                    2: config.CRED_REVOKED,
                                    3: config.CRED_ACTIVE})
