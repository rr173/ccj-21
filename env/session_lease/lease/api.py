"""HTTP 服务：控制面（管理员令牌）与设备面（凭证鉴权）同进程暴露。

仅用标准库 http.server；写操作全部进 Store 的串行事务。
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse

from . import config
from .store import LeaseError, Store

Route = tuple[str, str, Callable, bool]  # method, pattern, handler, needs_admin


def _json_response(handler: BaseHTTPRequestHandler, status: int,
                   obj: Any) -> None:
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    store: Store
    routes: list[Route]

    def log_message(self, fmt: str, *args) -> None:  # 静默
        return

    # ---- 请求解析 ----
    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LeaseError(400, "BAD_JSON", "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise LeaseError(400, "BAD_JSON", "请求体必须是 JSON 对象")
        return data

    def _match(self, method: str, path: str):
        for m, pattern, fn, needs_admin in self.routes:
            if m != method:
                continue
            parts = pattern.strip("/").split("/")
            target = path.strip("/").split("/")
            if len(parts) != len(target):
                continue
            kwargs = {}
            ok = True
            for p, t in zip(parts, target):
                if p.startswith("{") and p.endswith("}"):
                    kwargs[p[1:-1]] = t
                elif p != t:
                    ok = False
                    break
            if ok:
                return fn, kwargs, needs_admin
        return None

    def _dispatch(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            matched = self._match(method, parsed.path)
            if matched is None:
                raise LeaseError(404, "NOT_FOUND", "路径不存在")
            fn, kwargs, needs_admin = matched
            query = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            body = self._read_json() if method in ("POST", "PUT") else {}
            if needs_admin:
                token = self.headers.get("X-Admin-Token", "")
                if token != config.ADMIN_TOKEN:
                    raise LeaseError(401, "BAD_ADMIN_TOKEN",
                                     "控制面接口需要 X-Admin-Token")
            result = fn(body=body, query=query, **kwargs)
            _json_response(self, result.pop("_status", 200), result)
        except LeaseError as e:
            _json_response(self, e.http_status,
                           {"error": e.code, "message": e.message,
                            **({"detail": e.detail} if e.detail else {})})
        except Exception as e:  # noqa: BLE001 - 兜底，避免连接挂死
            _json_response(self, 500,
                           {"error": "INTERNAL", "message": str(e)})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")


def build_server(store: Store,
                 host: str = config.HTTP_HOST,
                 port: int = config.HTTP_PORT) -> ThreadingHTTPServer:
    # ---------------- 控制面 ----------------
    def register(body, query):
        if not body.get("device_id"):
            raise LeaseError(400, "MISSING_DEVICE_ID", "需要 device_id")
        return store.register_device(body["device_id"], body.get("name"),
                                     body.get("idempotency_key"))

    def device_detail(body, query, device_id):
        return store.device_read_model(device_id)

    def sessions(body, query, device_id):
        return {"sessions": store.list_sessions(
            device_id, int(query.get("limit", 100)))}

    def credentials(body, query, device_id):
        return {"credentials": store.list_credentials(device_id)}

    def rotations(body, query, device_id=None):
        if device_id is None:
            return {"rotations": store.list_rotations(
                None, int(query.get("limit", 100)))}
        return {"rotations": store.list_rotations(
            device_id, int(query.get("limit", 100)))}

    def takeovers(body, query, device_id):
        return {"takeovers": store.list_takeovers(
            device_id, int(query.get("limit", 100)))}

    def issue(body, query, device_id):
        if "payload" not in body:
            raise LeaseError(400, "MISSING_PAYLOAD", "需要 payload")
        return store.issue_command(device_id, body["payload"],
                                   body.get("idempotency_key"))

    def commands(body, query, device_id):
        return {"commands": store.list_commands(
            device_id, int(query.get("limit", 100)))}

    def timeline(body, query, command_id):
        return store.command_timeline(command_id)

    def do_takeover(body, query, device_id):
        return store.takeover(device_id, body.get("reason"),
                              body.get("idempotency_key"))

    def do_rotate(body, query, device_id):
        return store.start_rotation(device_id, body.get("grace_seconds"),
                                    body.get("idempotency_key"))

    def do_revoke(body, query, device_id):
        return store.revoke_old_credential(device_id,
                                           body.get("idempotency_key"))

    def rejected(body, query):
        return {"rejected": store.list_rejected(
            query.get("device_id"), query.get("kind"),
            int(query.get("limit", 100)), int(query.get("offset", 0)))}

    def events(body, query):
        return {"events": store.list_events(
            query.get("device_id"),
            int(query.get("limit", 100)), int(query.get("offset", 0)))}

    def sweep(body, query):
        return store.sweep()

    # ---------------- 设备面 ----------------
    def _dev_auth(body):
        for key in ("device_id", "connection_no", "credential_version"):
            if body.get(key) in (None, ""):
                raise LeaseError(400, f"MISSING_{key.upper()}",
                                 f"需要 {key}")
        return (body["device_id"], str(body["connection_no"]),
                int(body["credential_version"]),
                body.get("credential_secret", body.get("secret", "")))

    def dev_online(body, query):
        d, cn, cv, sec = _dev_auth(body)
        return store.online(d, cn, cv, sec, body.get("lease_seconds"),
                            body.get("idempotency_key"))

    def dev_renew(body, query):
        d, cn, cv, sec = _dev_auth(body)
        return store.renew(d, cn, cv, sec, body.get("lease_seconds"),
                           body.get("idempotency_key"))

    def dev_poll(body, query):
        d, cn, cv, sec = _dev_auth(body)
        return store.poll(d, cn, cv, sec, body.get("lease_seconds"))

    def dev_report(body, query):
        d, cn, cv, sec = _dev_auth(body)
        if "state_version" not in body or "state" not in body:
            raise LeaseError(400, "MISSING_STATE",
                             "需要 state_version 与 state")
        return store.report_state(d, cn, cv, sec,
                                  int(body["state_version"]), body["state"],
                                  body.get("idempotency_key"))

    def dev_ack(body, query):
        d, cn, cv, sec = _dev_auth(body)
        if body.get("command_id") is None and body.get("version") is None:
            raise LeaseError(400, "MISSING_COMMAND_REF",
                             "需要 command_id 或 version")
        return store.ack(d, cn, cv, sec, body.get("command_id"),
                         body.get("version"), body.get("code"),
                         body.get("result"), body.get("idempotency_key"))

    def dev_reconcile(body, query):
        d, cn, cv, sec = _dev_auth(body)
        entries = body.get("entries")
        if not isinstance(entries, list) or not entries:
            raise LeaseError(400, "MISSING_ENTRIES",
                             "需要非空 entries: [{version,done,result?}]")
        return store.reconcile(d, cn, cv, sec, entries,
                               body.get("idempotency_key"))

    routes: list[Route] = [
        # 控制面（X-Admin-Token）
        ("POST", "/v1/devices", register, True),
        ("GET", "/v1/devices/{device_id}", device_detail, True),
        ("GET", "/v1/devices/{device_id}/sessions", sessions, True),
        ("GET", "/v1/devices/{device_id}/credentials", credentials, True),
        ("GET", "/v1/devices/{device_id}/rotations", rotations, True),
        ("GET", "/v1/rotations", rotations, True),
        ("GET", "/v1/devices/{device_id}/takeovers", takeovers, True),
        ("POST", "/v1/devices/{device_id}/commands", issue, True),
        ("GET", "/v1/devices/{device_id}/commands", commands, True),
        ("GET", "/v1/commands/{command_id}/timeline", timeline, True),
        ("POST", "/v1/devices/{device_id}/takeover", do_takeover, True),
        ("POST", "/v1/devices/{device_id}/rotations", do_rotate, True),
        ("POST", "/v1/devices/{device_id}/revoke-old", do_revoke, True),
        ("GET", "/v1/rejected-messages", rejected, True),
        ("GET", "/v1/events", events, True),
        ("POST", "/v1/sweep", sweep, True),
        # 设备面（凭证在请求体中）
        ("POST", "/devices/online", dev_online, False),
        ("POST", "/devices/renew", dev_renew, False),
        ("POST", "/devices/poll", dev_poll, False),
        ("POST", "/devices/reported", dev_report, False),
        ("POST", "/devices/ack", dev_ack, False),
        ("POST", "/devices/reconcile", dev_reconcile, False),
    ]

    handler = Handler
    handler.store = store
    handler.routes = routes
    server = ThreadingHTTPServer((host, port), handler)

    def sweeper() -> None:
        while not server._stop_event.is_set():  # type: ignore[attr-defined]
            try:
                store.sweep()
            except Exception:  # noqa: BLE001
                pass
            server._stop_event.wait(config.SWEEP_INTERVAL_SECONDS)  # type: ignore[attr-defined]

    server._stop_event = threading.Event()  # type: ignore[attr-defined]
    server._sweeper = threading.Thread(  # type: ignore[attr-defined]
        target=sweeper, daemon=True)
    server._sweeper.start()  # type: ignore[attr-defined]

    original_shutdown = server.shutdown

    def shutdown_with_sweeper() -> None:
        server._stop_event.set()  # type: ignore[attr-defined]
        original_shutdown()

    server.shutdown = shutdown_with_sweeper  # type: ignore[method-assign]
    return server
