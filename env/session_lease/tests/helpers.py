"""测试公共工具：可控时钟 + 临时文件 Store + 便捷断言。"""
from __future__ import annotations

import os
import tempfile
import unittest

from lease.store import LeaseError, Store


class FakeClock:
    def __init__(self, start: float = 1_000_000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class LeaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.clk = FakeClock()
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.db = Store(self.path, clock=self.clk)

    def tearDown(self) -> None:
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            p = self.path + suffix
            if os.path.exists(p):
                os.unlink(p)

    def reopen(self) -> Store:
        """模拟服务重启：关闭再以同一数据库文件打开（不复活任何连接）。"""
        self.db.close()
        self.db = Store(self.path, clock=self.clk)
        return self.db

    # ---- 构造快捷方式 ----
    def provision(self, device_id: str = "dev1"):
        reg = self.db.register_device(device_id, device_id)
        return reg["credential_version"], reg["credential_secret"]

    def open_session(self, device_id="dev1", connection_no="c1",
                     cred_version=1, secret=None, lease=60.0,
                     idem_key=None):
        if secret is None:
            secret = self._secret(device_id, cred_version)
        return self.db.online(device_id, connection_no, cred_version,
                              secret, lease, idem_key)

    def _secret(self, device_id: str, version: int) -> str:
        row = self.db._row(
            "SELECT secret FROM credentials WHERE device_id=? AND version=?",
            (device_id, version))
        return row["secret"]

    def assert_rejected(self, ctx, reason: str) -> None:
        self.assertEqual(ctx.exception.code, reason)

    def rejected_reasons(self, device_id="dev1"):
        return {r["reason"] for r in self.db.list_rejected(device_id)}

    def current_generation(self, device_id="dev1") -> int:
        return self.db.device_read_model(device_id)["current_generation"]
