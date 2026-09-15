"""测试辅助：可控时钟、可失败的发送器、通用 TestCase。"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telemetry.service import TelemetryService  # noqa: E402


class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = float(t)

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class SenderStub:
    """记录成功投递；fail_next(n) 让接下来 n 次发送抛异常。"""

    def __init__(self):
        self.sent = []
        self._fail = 0

    def fail_next(self, n=1):
        self._fail += n

    def __call__(self, notification):
        if self._fail > 0:
            self._fail -= 1
            raise RuntimeError("simulated send failure")
        self.sent.append(notification)


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.sender = SenderStub()
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "test.db")
        self.svc = self.make_service()

    def tearDown(self):
        self.svc.close()
        self._tmp.cleanup()

    def make_service(self, **kw):
        kw.setdefault("clock", self.clock)
        kw.setdefault("sender", self.sender)
        kw.setdefault("ack_timeout_sec", 30.0)
        kw.setdefault("retry_base_sec", 1.0)
        return TelemetryService(self.db_path, **kw)

    def reopen(self, **kw):
        """模拟服务重启：关闭后用同一数据库文件重新打开。"""
        self.svc.close()
        self.svc = self.make_service(**kw)
        return self.svc

    # ---------------------------------------------------------- 数据工厂
    _seq = [0]

    @classmethod
    def ev(cls, device="d1", metric="cpu", value=1.0, t=0.0, event_id=None):
        cls._seq[0] += 1
        return {
            "event_id": event_id or f"e{cls._seq[0]}",
            "device_id": device,
            "metric": metric,
            "value": value,
            "event_time": float(t),
        }

    def make_rule(self, **kw):
        params = dict(
            group_id="g1",
            metric="cpu",
            window_size_sec=60,
            aggregation="avg",
            operator="gt",
            threshold=80.0,
            consecutive_hits=2,
            recovery_count=1,
            silence_sec=0.0,
            allowed_lateness_sec=0.0,
            effective_from=0.0,
        )
        params.update(kw)
        return self.svc.create_rule(**params)

    def ingest_ok(self, events):
        if isinstance(events, dict):
            events = [events]
        return self.svc.ingest(events)

    def window(self, device="d1", metric="cpu", start=0.0):
        for w in self.svc.list_windows(device, metric):
            if w["window_start"] == start:
                return w
        return None

    def alerts(self, device="d1"):
        return self.svc.list_alerts(device_id=device)
