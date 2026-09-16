"""真实 HTTP 端到端：起服务线程，走完整控制面/设备面流程。"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from lease import config
from lease.api import build_server
from lease.store import Store


def _req(method: str, url: str, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("X-Admin-Token", token)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class ApiE2ETest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._orig = (config.DB_PATH, config.HTTP_PORT,
                     config.SWEEP_INTERVAL_SECONDS, config.ADMIN_TOKEN)
        config.DB_PATH = ":memory:"
        # ThreadingHTTPServer + 内存库：同进程线程共享连接，可行
        fd, cls.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(cls.path)
        cls.db = Store(cls.path)
        cls.server = build_server(cls.db, host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.port}"
        cls.admin = "admin-secret"
        config.ADMIN_TOKEN = cls.admin

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.db.close()
        for suffix in ("", "-wal", "-shm"):
            p = cls.path + suffix
            if os.path.exists(p):
                os.unlink(p)
        (config.DB_PATH, config.HTTP_PORT,
         config.SWEEP_INTERVAL_SECONDS, config.ADMIN_TOKEN) = cls._orig

    def call(self, method, path, body=None, device_auth=None, admin=True):
        if device_auth:
            body = dict(body or {})
            body.update(device_auth)
        return _req(method, self.base + path, body,
                    self.admin if admin else None)

    # ------------------------------------------------------------------
    def test_full_lifecycle(self):
        st, reg = self.call("POST", "/v1/devices",
                            {"device_id": "lamp-1", "name": "Lamp"})
        self.assertEqual(st, 200)
        self.assertTrue(reg["created"])
        v1, secret1 = reg["credential_version"], reg["credential_secret"]
        auth1 = {"device_id": "lamp-1", "connection_no": "conn-a",
                 "credential_version": v1, "credential_secret": secret1}

        # 控制面需要管理员令牌
        st, err = _req("GET", self.base + "/v1/devices/lamp-1")
        self.assertEqual(st, 401)

        # 设备上线
        st, s1 = self.call("POST", "/devices/online",
                           {"lease_seconds": 30}, device_auth=auth1,
                           admin=False)
        self.assertEqual(st, 200)
        self.assertEqual(s1["generation"], 1)

        # 控制端下发两条命令（幂等键重复一次）
        st, c1 = self.call("POST", "/v1/devices/lamp-1/commands",
                           {"payload": {"led": "on"},
                            "idempotency_key": "cmd-1"})
        self.assertEqual(st, 200)
        st, c1b = self.call("POST", "/v1/devices/lamp-1/commands",
                            {"payload": {"led": "on"},
                             "idempotency_key": "cmd-1"})
        self.assertEqual(c1b["command_id"], c1["command_id"])
        self.assertTrue(c1b.get("duplicate"))
        self.call("POST", "/v1/devices/lamp-1/commands",
                  {"payload": {"brightness": 50}})

        # 领取 + 确认
        st, p = self.call("POST", "/devices/poll", {}, device_auth=auth1,
                          admin=False)
        self.assertEqual(p["command"]["version"], 1)
        st, ack = self.call("POST", "/devices/ack",
                            {"version": 1, "code": "APPLIED"},
                            device_auth=auth1, admin=False)
        self.assertEqual(ack["state"], "ACKED")
        # 重复 ACK
        st, ack2 = self.call("POST", "/devices/ack", {"version": 1},
                             device_auth=auth1, admin=False)
        self.assertTrue(ack2["duplicate_ack"])

        # 状态上报
        st, rep = self.call("POST", "/devices/reported",
                            {"state_version": 1, "state": {"led": "on"}},
                            device_auth=auth1, admin=False)
        self.assertTrue(rep["accepted"])

        # 并发上线（新连接，同样凭证）
        auth1b = dict(auth1, connection_no="conn-b")
        st, s2 = self.call("POST", "/devices/online",
                           {"lease_seconds": 30}, device_auth=auth1b,
                           admin=False)
        self.assertEqual(s2["generation"], 2)
        self.assertTrue(s2["concurrent_online"])
        # 旧连接上报被拒
        st, err = self.call("POST", "/devices/reported",
                            {"state_version": 2, "state": {"led": "off"}},
                            device_auth=auth1, admin=False)
        self.assertEqual(st, 410)
        self.assertEqual(err["error"], "SESSION_SUPERSEDED")

        # 新通道把 v2 领走（已下达、结果未知）
        st, p1b = self.call("POST", "/devices/poll", {},
                            device_auth=auth1b, admin=False)
        self.assertEqual(st, 200)
        self.assertEqual(p1b["command"]["version"], 2)
        # 再下发一条、任何通道都尚未领取的命令（v3）
        st, c3 = self.call("POST", "/v1/devices/lamp-1/commands",
                           {"payload": {"color": "blue"}})
        self.assertEqual(st, 200)

        # v2 已被新会话领走：会话切换时在途 -> RECONCILING
        st, tl = self.call("GET", f"/v1/commands/{c1['command_id']}"
                           "/timeline", admin=True)
        # c1 已 ACK，检查时间线
        types = [e["event_type"] for e in tl["events"]]
        self.assertEqual(types[0], "CREATED")

        # 接管
        st, tk = self.call("POST", "/v1/devices/lamp-1/takeover",
                           {"reason": "security", "idempotency_key": "tk-1"})
        self.assertEqual(tk["state"], "PENDING_RECONNECT")
        st, tkb = self.call("POST", "/v1/devices/lamp-1/takeover",
                            {"reason": "security", "idempotency_key": "tk-1"})
        self.assertTrue(tkb.get("idempotent_replay"))
        # 旧连接 poll 被拒
        st, err = self.call("POST", "/devices/poll", {},
                            device_auth=auth1b, admin=False)
        self.assertEqual(err["error"], "SESSION_SUPERSEDED")

        # 轮换凭证（宽限 2 秒，由后台清扫到期）
        st, rot = self.call("POST", "/v1/devices/lamp-1/rotations",
                            {"grace_seconds": 2, "idempotency_key": "rot-1"})
        self.assertEqual(rot["old_version"], 1)
        self.assertEqual(rot["new_version"], 2)
        secret2 = rot["new_credential_secret"]
        # 旧凭证不能开新连接
        st, err = self.call("POST", "/devices/online",
                            {"lease_seconds": 30},
                            device_auth={"device_id": "lamp-1",
                                         "connection_no": "conn-c",
                                         "credential_version": 1,
                                         "credential_secret": secret1},
                            admin=False)
        self.assertEqual(err["error"], "OLD_CREDENTIAL_NEW_CONNECTION")

        # 新凭证上线：接管完成 + 轮换完成（旧凭证提前撤销）
        auth2 = {"device_id": "lamp-1", "connection_no": "conn-d",
                 "credential_version": 2, "credential_secret": secret2}
        st, s3 = self.call("POST", "/devices/online",
                           {"lease_seconds": 30}, device_auth=auth2,
                           admin=False)
        self.assertEqual(s3["generation"], 3)
        self.assertIsNotNone(s3.get("rotation_completed"))

        # v2 命令（brightness）接管时已在途（SENT）：先对账核实
        st, rec = self.call("POST", "/devices/reconcile",
                            {"entries": [{"version": 2, "done": False}]},
                            device_auth=auth2, admin=False)
        self.assertEqual(rec["results"][0]["outcome"],
                         "RETRY_TO_NEW_SESSION")
        # 现在 poll 能领到 v2（已确认的 v1 绝不重发）
        st, p2 = self.call("POST", "/devices/poll", {}, device_auth=auth2,
                           admin=False)
        self.assertEqual(p2["command"]["version"], 2)
        st, ack3 = self.call("POST", "/devices/ack", {"version": 2},
                             device_auth=auth2, admin=False)
        self.assertEqual(ack3["state"], "ACKED")
        # v3 从未下达给任何终端：接管/换通道不要求核实，新通道直接领取
        st, p3 = self.call("POST", "/devices/poll", {}, device_auth=auth2,
                           admin=False)
        self.assertEqual(p3["command"]["version"], 3)
        self.assertEqual(p3["command"]["payload"], {"color": "blue"})

        # 查询面
        st, rm = self.call("GET", "/v1/devices/lamp-1", admin=True)
        self.assertTrue(rm["online"])
        self.assertEqual(rm["current_session"]["generation"], 3)
        st, sess = self.call("GET", "/v1/devices/lamp-1/sessions")
        self.assertEqual(len(sess["sessions"]), 3)
        st, creds = self.call("GET", "/v1/devices/lamp-1/credentials")
        self.assertEqual({c["status"] for c in creds["credentials"]},
                         {"REVOKED", "ACTIVE"})
        st, tks = self.call("GET", "/v1/devices/lamp-1/takeovers")
        self.assertEqual(tks["takeovers"][0]["state"], "COMPLETED")
        st, rots = self.call("GET", "/v1/devices/lamp-1/rotations")
        self.assertEqual(rots["rotations"][0]["state"], "COMPLETED")
        st, rej = self.call("GET",
                            "/v1/rejected-messages?device_id=lamp-1")
        reasons = {r["reason"] for r in rej["rejected"]}
        self.assertIn("SESSION_SUPERSEDED", reasons)
        self.assertIn("OLD_CREDENTIAL_NEW_CONNECTION", reasons)
        st, evs = self.call("GET", "/v1/events?device_id=lamp-1&limit=200")
        self.assertTrue(evs["events"])

    def test_grace_expiry_via_background_sweeper(self):
        st, reg = self.call("POST", "/v1/devices",
                            {"device_id": "fan-9"})
        secret1 = reg["credential_secret"]
        auth1 = {"device_id": "fan-9", "connection_no": "f1",
                 "credential_version": 1, "credential_secret": secret1}
        self.call("POST", "/devices/online", {"lease_seconds": 3600},
                  device_auth=auth1, admin=False)
        self.call("POST", "/v1/devices/fan-9/rotations",
                  {"grace_seconds": 1})
        # 等待后台 sweep
        deadline = time.time() + 5
        while time.time() < deadline:
            st, rots = self.call("GET", "/v1/devices/fan-9/rotations")
            if rots["rotations"][0]["state"] == "OLD_REVOKED":
                break
            time.sleep(0.2)
        self.assertEqual(rots["rotations"][0]["state"], "OLD_REVOKED")
        st, err = self.call("POST", "/devices/reported",
                            {"state_version": 1, "state": {"rpm": 100}},
                            device_auth=auth1, admin=False)
        self.assertEqual(err["error"], "CREDENTIAL_REVOKED")


if __name__ == "__main__":
    unittest.main()
