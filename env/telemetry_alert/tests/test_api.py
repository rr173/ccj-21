"""控制端 HTTP API 集成测试。"""
import json
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer

from helpers import ServiceTestCase
from telemetry.api import ApiHandler


class TestApi(ServiceTestCase):
    def setUp(self):
        super().setUp()
        handler = type("H", (ApiHandler,), {"service": self.svc})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def call(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode())

    def test_end_to_end_over_http(self):
        # 建规则、注册设备
        code, rule = self.call("POST", "/rules", {
            "group_id": "g1", "metric": "cpu", "window_size_sec": 60,
            "aggregation": "avg", "operator": "gt", "threshold": 80,
            "consecutive_hits": 2, "recovery_count": 1,
        })
        self.assertEqual(code, 201)
        rid = rule["rule_id"]
        self.assertEqual(self.call("POST", "/devices",
                                   {"device_id": "d1", "group_id": "g1"})[0], 201)
        # 上报事件（第三个事件推进水位线，封存前两个命中窗口）
        code, res = self.call("POST", "/ingest", {"events": [
            self.ev(value=90, t=30, event_id="api-e1"),
            self.ev(value=95, t=90, event_id="api-e2"),
            self.ev(value=92, t=150, event_id="api-e3"),
        ]})
        self.assertEqual(code, 200)
        self.assertTrue(all(r["status"] == "accepted" for r in res["results"]))
        # 告警产生
        code, alerts = self.call("GET", "/alerts?device_id=d1")
        self.assertEqual(len(alerts), 1)
        # 窗口与规则版本可查
        code, windows = self.call("GET", "/windows?device_id=d1&metric=cpu")
        self.assertEqual({w["rule_version"] for w in windows}, {1})
        # 通知泵送并确认
        self.assertEqual(self.call("POST", "/notifications/pump")[0], 200)
        code, notifs = self.call("GET", "/notifications?status=sent")
        self.assertEqual(len(notifs), 1)
        nid = notifs[0]["notification_id"]
        self.assertEqual(self.call("POST", f"/notifications/{nid}/ack",
                                   {"ack_token": "t1"})[0], 200)
        # 重复确认幂等
        code, again = self.call("POST", f"/notifications/{nid}/ack",
                                {"ack_token": "t2"})
        self.assertEqual((code, again["status"], again["ack_token"]),
                         (200, "confirmed", "t1"))
        # 事件结果可查
        code, ev = self.call("GET", "/events/api-e1")
        self.assertEqual(ev["status"], "accepted")
        # 规则更新
        code, updated = self.call("POST", f"/rules/{rid}/versions",
                                  {"effective_from": 1000, "threshold": 50})
        self.assertEqual(code, 201)
        self.assertEqual(len(updated["versions"]), 2)

    def test_error_mapping(self):
        self.assertEqual(self.call("GET", "/rules/nope")[0], 404)
        self.assertEqual(self.call("GET", "/events/nope")[0], 404)
        code, err = self.call("POST", "/rules", {"group_id": "g1"})
        self.assertEqual(code, 400)
        self.assertIn("error", err)
        self.assertEqual(self.call("GET", "/nothing-here")[0], 404)


if __name__ == "__main__":
    unittest.main()
