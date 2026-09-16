"""会话上线、租约续期、并发上线/接管与单调代次。"""
from __future__ import annotations

import threading

from lease import config
from lease.store import LeaseError

from helpers import LeaseTestCase


class OnlineTests(LeaseTestCase):
    def test_first_online_generation_one_and_writable(self):
        self.provision()
        s = self.open_session()
        self.assertEqual(s["generation"], 1)
        self.assertEqual(s["state"], config.SE_ACTIVE)
        self.assertTrue(self.db.device_read_model("dev1")["online"])

    def test_bad_credential_rejected(self):
        self.provision()
        with self.assertRaises(LeaseError) as ctx:
            self.db.online("dev1", "c1", 1, "wrong-secret")
        self.assert_rejected(ctx, "BAD_CREDENTIAL")
        with self.assertRaises(LeaseError) as ctx:
            self.db.online("dev1", "c1", 9, self._secret("dev1", 1))
        self.assert_rejected(ctx, "BAD_CREDENTIAL")

    def test_duplicate_online_same_connection_is_idempotent(self):
        self.provision()
        s1 = self.open_session(idem_key="on-1")
        s2 = self.open_session(idem_key="on-1")
        self.assertEqual(s1["session_id"], s2["session_id"])
        self.assertTrue(s2.get("idempotent_replay"))
        # 不带幂等键但连接编号相同且会话活跃：同样返回同一会话
        s3 = self.open_session()
        self.assertEqual(s3["session_id"], s1["session_id"])
        self.assertTrue(s3["duplicate"])

    def test_idempotency_fingerprint_conflict(self):
        self.provision()
        self.open_session(idem_key="k")
        with self.assertRaises(LeaseError) as ctx:
            self.db.online("dev1", "c1", 1, self._secret("dev1", 1),
                           lease_seconds=120, idem_key="k")
        self.assert_rejected(ctx, "IDEMPOTENCY_CONFLICT")

    def test_dead_connection_number_never_revived(self):
        self.provision()
        self.open_session()
        self.open_session(connection_no="c2")  # 并发上线顶替 c1
        with self.assertRaises(LeaseError) as ctx:
            self.open_session(connection_no="c1")
        self.assert_rejected(ctx, "DEAD_CONNECTION_REUSED")
        self.assertIn("DEAD_CONNECTION_REUSED", self.rejected_reasons())

    def test_dead_connection_cannot_revive_via_idempotency_replay(self):
        # 旧通道先用幂等键成功上线；被顶替后拿同一个键重放也必须拦截
        self.provision()
        self.open_session(connection_no="c1", idem_key="on-c1")
        self.open_session(connection_no="c2")
        with self.assertRaises(LeaseError) as ctx:
            self.open_session(connection_no="c1", idem_key="on-c1")
        self.assert_rejected(ctx, "DEAD_CONNECTION_REUSED")
        rejected = self.db.list_rejected("dev1", kind="ONLINE")
        self.assertTrue(any(r["reason"] == "DEAD_CONNECTION_REUSED"
                            for r in rejected))

    def test_concurrent_online_generates_new_generation(self):
        self.provision()
        s1 = self.open_session(connection_no="c1")
        s2 = self.open_session(connection_no="c2")
        self.assertEqual(s2["generation"], 2)
        self.assertTrue(s2.get("concurrent_online"))
        old = next(s for s in self.db.list_sessions("dev1")
                   if s["session_id"] == s1["session_id"])
        self.assertEqual(old["state"], config.SE_SUPERSEDED)
        self.assertEqual(old["superseded_by_session_id"], s2["session_id"])

    def test_only_one_writable_session(self):
        self.provision()
        self.open_session(connection_no="c1")
        self.open_session(connection_no="c2")
        self.open_session(connection_no="c3")
        active = [s for s in self.db.list_sessions("dev1")
                  if s["state"] == config.SE_ACTIVE]
        self.assertEqual(len(active), 1)
        self.assertEqual(self.current_generation(), 3)

    def test_generation_strictly_monotonic_under_threads(self):
        self.provision()
        generations = []

        def connect(i):
            s = self.db.online("dev1", f"t{i}", 1,
                               self._secret("dev1", 1), 60)
            generations.append(s["generation"])

        threads = [threading.Thread(target=connect, args=(i,))
                   for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(generations), list(range(1, 21)))
        active = [s for s in self.db.list_sessions("dev1")
                  if s["state"] == config.SE_ACTIVE]
        self.assertEqual(len(active), 1)


class LeaseTests(LeaseTestCase):
    def test_renew_extends_deadline(self):
        self.provision()
        s = self.open_session(lease=30)
        self.clk.advance(10)
        r = self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 30)
        self.assertAlmostEqual(r["lease_expires_at"], self.clk.t + 30)
        self.assertGreater(r["lease_expires_at"], s["lease_expires_at"])

    def test_renew_unknown_connection_rejected(self):
        self.provision()
        self.open_session()
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "ghost", 1, self._secret("dev1", 1))
        self.assert_rejected(ctx, "UNKNOWN_CONNECTION")

    def test_renew_after_expiry_rejected_and_session_dead(self):
        self.provision()
        self.open_session(lease=10)
        self.clk.advance(11)
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, self._secret("dev1", 1))
        self.assert_rejected(ctx, "SESSION_EXPIRED")
        # 后续再续仍然拒绝，复活不了
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, self._secret("dev1", 1))
        self.assert_rejected(ctx, "SESSION_EXPIRED")
        self.assertFalse(self.db.device_read_model("dev1")["online"])

    def test_renew_superseded_session_rejected(self):
        self.provision()
        self.open_session(connection_no="c1")
        self.open_session(connection_no="c2")
        with self.assertRaises(LeaseError) as ctx:
            self.db.renew("dev1", "c1", 1, self._secret("dev1", 1))
        self.assert_rejected(ctx, "SESSION_SUPERSEDED")

    def test_renew_is_idempotent(self):
        self.provision()
        self.open_session(lease=30)
        self.clk.advance(5)
        r1 = self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 30,
                           idem_key="rn-1")
        self.clk.advance(5)
        r2 = self.db.renew("dev1", "c1", 1, self._secret("dev1", 1), 30,
                           idem_key="rn-1")
        self.assertTrue(r2.get("idempotent_replay"))
        self.assertEqual(r1["lease_expires_at"], r2["lease_expires_at"])

    def test_poll_counts_as_heartbeat(self):
        self.provision()
        self.open_session(lease=10)
        self.clk.advance(9)
        self.db.poll("dev1", "c1", 1, self._secret("dev1", 1),
                     lease_seconds=10)
        self.clk.advance(5)
        # 若 poll 没有续期，此刻租约已过期
        self.assertTrue(self.db.device_read_model("dev1")["online"])


class StaleMessageTests(LeaseTestCase):
    def _old_and_new(self):
        self.provision()
        self.open_session(connection_no="c1")
        self.open_session(connection_no="c2")

    def test_old_session_report_rejected(self):
        self._old_and_new()
        with self.assertRaises(LeaseError) as ctx:
            self.db.report_state("dev1", "c1", 1, self._secret("dev1", 1),
                                 1, {"a": 1})
        self.assert_rejected(ctx, "SESSION_SUPERSEDED")
        self.assertIsNone(
            self.db.device_read_model("dev1")["reported_state"])

    def test_new_session_report_accepted_and_old_cannot_overwrite(self):
        self._old_and_new()
        self.db.report_state("dev1", "c2", 1, self._secret("dev1", 1),
                             1, {"gen": 2})
        with self.assertRaises(LeaseError):
            self.db.report_state("dev1", "c1", 1, self._secret("dev1", 1),
                                 2, {"gen": 1})
        reported = self.db.device_read_model("dev1")["reported_state"]
        self.assertEqual(reported["state"], {"gen": 2})
        self.assertEqual(reported["generation"], 2)

    def test_stale_and_duplicate_state_versions(self):
        self.provision()
        self.open_session()
        self.db.report_state("dev1", "c1", 1, self._secret("dev1", 1),
                             5, {"v": 5})
        with self.assertRaises(LeaseError) as ctx:
            self.db.report_state("dev1", "c1", 1, self._secret("dev1", 1),
                                 4, {"v": 4})
        self.assert_rejected(ctx, "STALE_STATE_VERSION")
        with self.assertRaises(LeaseError) as ctx:
            self.db.report_state("dev1", "c1", 1, self._secret("dev1", 1),
                                 5, {"v": 5})
        self.assert_rejected(ctx, "DUPLICATE_STATE_VERSION")

    def test_old_session_ack_rejected(self):
        self._old_and_new()
        cmd = self.db.issue_command("dev1", {"x": 1})
        # 新会话领取
        p = self.db.poll("dev1", "c2", 1, self._secret("dev1", 1))
        self.assertEqual(p["command"]["version"], cmd["version"])
        with self.assertRaises(LeaseError) as ctx:
            self.db.ack("dev1", "c1", 1, self._secret("dev1", 1),
                        command_id=cmd["command_id"])
        self.assert_rejected(ctx, "SESSION_SUPERSEDED")
        # 命令仍是 SENT（旧确认没有任何副作用）
        got = next(c for c in self.db.list_commands("dev1")
                   if c["command_id"] == cmd["command_id"])
        self.assertEqual(got["state"], config.CMD_SENT)


class TakeoverTests(LeaseTestCase):
    def test_takeover_kills_writable_session_and_reconnect_advances(self):
        self.provision()
        s1 = self.open_session(connection_no="c1")
        tk = self.db.takeover("dev1", reason="incident", idem_key="tk-1")
        self.assertEqual(tk["state"], config.TK_PENDING)
        self.assertEqual(tk["old_session_id"], s1["session_id"])
        self.assertFalse(self.db.device_read_model("dev1")["online"])
        # 重复接管：幂等返回同一条待重连记录
        tk2 = self.db.takeover("dev1", reason="incident", idem_key="tk-1")
        self.assertTrue(tk2.get("idempotent_replay"))
        # 旧连接消息被拒
        with self.assertRaises(LeaseError) as ctx:
            self.db.poll("dev1", "c1", 1, self._secret("dev1", 1))
        self.assert_rejected(ctx, "SESSION_SUPERSEDED")
        # 设备重连：代次 +1，接管完成并回填
        s2 = self.open_session(connection_no="c2")
        self.assertEqual(s2["generation"], 2)
        done = self.db.list_takeovers("dev1")[0]
        self.assertEqual(done["state"], config.TK_COMPLETED)
        self.assertEqual(done["new_session_id"], s2["session_id"])
        self.assertTrue(self.db.device_read_model("dev1")["online"])

    def test_takeover_when_offline_completes_on_next_online(self):
        self.provision()
        tk = self.db.takeover("dev1")
        self.assertIsNone(tk["old_session_id"])
        s = self.open_session(connection_no="c1")
        self.assertEqual(s["generation"], 1)
        done = self.db.list_takeovers("dev1")[0]
        self.assertEqual(done["state"], config.TK_COMPLETED)
        self.assertEqual(done["new_session_id"], s["session_id"])
