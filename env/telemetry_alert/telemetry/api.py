"""控制端 HTTP API（仅标准库实现）。

运行:  python -m telemetry --db telemetry.db --port 8080
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .service import TelemetryService, RuleError, NotFoundError


def _float_or_none(v):
    return None if v is None else float(v)


class ApiHandler(BaseHTTPRequestHandler):
    service: TelemetryService = None  # 由 serve() 注入

    # ---------------------------------------------------------- 基础工具
    def log_message(self, fmt, *args):  # 静默默认访问日志
        pass

    def _send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise RuleError("request body must be valid JSON") from None

    def _query(self):
        return {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}

    # ---------------------------------------------------------- 路由
    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method):
        path = urlparse(self.path).path.rstrip("/") or "/"
        q = self._query()
        svc = self.service
        try:
            body = self._read_json() if method == "POST" else {}

            if method == "GET" and path == "/health":
                return self._send_json(200, {"status": "ok"})

            if method == "POST" and path == "/rules":
                return self._send_json(201, svc.create_rule(**body))

            m = re.fullmatch(r"/rules/([0-9a-zA-Z_-]+)/versions", path)
            if m and method == "POST":
                return self._send_json(201, svc.update_rule(m.group(1), **body))

            m = re.fullmatch(r"/rules/([0-9a-zA-Z_-]+)", path)
            if m and method == "GET":
                return self._send_json(200, svc.get_rule(m.group(1)))
            if m and method == "POST":
                return self._send_json(201, svc.update_rule(m.group(1), **body))

            if method == "GET" and path == "/rules":
                return self._send_json(
                    200, svc.list_rules(q.get("group_id"), q.get("metric"))
                )

            if method == "POST" and path == "/devices":
                return self._send_json(
                    201, svc.register_device(body["device_id"], body["group_id"])
                )

            if method == "POST" and path == "/ingest":
                events = body if isinstance(body, list) else body.get("events")
                return self._send_json(200, {"results": svc.ingest(events)})

            m = re.fullmatch(r"/events/([^/]+)", path)
            if m and method == "GET":
                return self._send_json(200, svc.get_event(m.group(1)))
            if method == "GET" and path == "/events":
                return self._send_json(
                    200, svc.list_events(q.get("device_id"), q.get("status"))
                )

            if method == "GET" and path == "/windows":
                return self._send_json(
                    200, svc.list_windows(q.get("device_id"), q.get("metric"))
                )

            if method == "GET" and path == "/alerts":
                return self._send_json(
                    200,
                    svc.list_alerts(
                        q.get("device_id"), q.get("rule_id"), q.get("status")
                    ),
                )

            if method == "GET" and path == "/corrections/windows":
                return self._send_json(
                    200, svc.list_window_corrections(q.get("device_id"))
                )
            if method == "GET" and path == "/corrections/alerts":
                return self._send_json(
                    200, svc.list_alert_corrections(q.get("alert_id"))
                )

            if method == "GET" and path == "/quarantine":
                return self._send_json(
                    200, svc.list_events(q.get("device_id"), "quarantined")
                )

            if method == "GET" and path == "/notifications":
                return self._send_json(200, svc.list_notifications(q.get("status")))

            m = re.fullmatch(r"/notifications/([0-9a-zA-Z_-]+)/attempts", path)
            if m and method == "GET":
                return self._send_json(200, svc.list_notification_attempts(m.group(1)))

            m = re.fullmatch(r"/notifications/([0-9a-zA-Z_-]+)/ack", path)
            if m and method == "POST":
                return self._send_json(
                    200,
                    svc.ack_notification(m.group(1), body.get("ack_token")),
                )

            m = re.fullmatch(r"/notifications/([0-9a-zA-Z_-]+)", path)
            if m and method == "GET":
                return self._send_json(200, svc.get_notification(m.group(1)))

            if method == "POST" and path == "/notifications/pump":
                return self._send_json(200, svc.pump_notifications())

            return self._send_json(404, {"error": f"no route: {method} {path}"})
        except RuleError as exc:
            return self._send_json(400, {"error": str(exc)})
        except NotFoundError as exc:
            return self._send_json(404, {"error": str(exc)})
        except (KeyError, TypeError) as exc:
            return self._send_json(400, {"error": f"bad request: {exc}"})
        except Exception as exc:  # noqa: BLE001
            return self._send_json(500, {"error": f"internal: {exc}"})


def serve(db_path="telemetry.db", host="127.0.0.1", port=8080, sender=None):
    service = TelemetryService(db_path, sender=sender)
    handler = type("BoundApiHandler", (ApiHandler,), {"service": service})
    server = ThreadingHTTPServer((host, port), handler)
    print(f"telemetry service listening on http://{host}:{port} (db={db_path})")
    try:
        server.serve_forever()
    finally:
        service.close()
