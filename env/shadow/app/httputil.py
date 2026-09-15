"""零依赖 HTTP/JSON 小工具（基于 http.server）。"""
from __future__ import annotations

import json
import logging
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional

log = logging.getLogger("shadow")


class JsonApp(BaseHTTPRequestHandler):
    """路由式 Handler：子类需设置 routes 与鉴权。"""

    server_version = "ShadowHTTP/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        log.info("%s - %s", self.address_string(), fmt % args)

    # ---- 响应 ----
    def json_response(self, code: int, obj: Any) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json(self) -> Optional[dict]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {"_": data}
        except json.JSONDecodeError:
            return None

    # ---- 路由分发 ----
    def _dispatch(self, method: str) -> None:
        path = self.path.split("?", 1)[0]
        routes = self.routes(method)  # type: ignore[attr-defined]
        for pattern, handler, opts in routes:
            m = re.fullmatch(pattern, path)
            if m:
                if opts.get("internal") and not self._check_internal():
                    return
                if opts.get("device_auth") and not self._auth_device():
                    return
                body = None
                if method in ("POST", "PUT", "PATCH"):
                    body = self.read_json()
                    if body is None:
                        self.json_response(400, {"error": "invalid JSON"})
                        return
                try:
                    groups = m.groups()
                    if groups:
                        handler(*groups, body=body)
                    else:
                        handler(body=body)
                except KeyError as e:
                    self.json_response(404, {"error": str(e)})
                except Exception as e:  # 服务端兜底
                    log.exception("handler error")
                    self.json_response(500, {"error": str(e)})
                return
        self.json_response(404, {"error": "not found", "path": path})

    def _check_internal(self) -> bool:
        from . import config
        token = self.headers.get("X-Internal-Token", "")
        if token != config.INTERNAL_TOKEN:
            self.json_response(401, {"error": "invalid internal token"})
            return False
        return True

    def _auth_device(self) -> bool:
        # 设备认证在 ingress 层具体处理（需要 token -> device 解析）
        raise NotImplementedError

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_PUT(self) -> None:
        self._dispatch("PUT")


def serve(host: str, port: int, handler_cls) -> ThreadingHTTPServer:
    httpd = ThreadingHTTPServer((host, port), handler_cls)
    log.info("listening on %s:%d (%s)", host, port, handler_cls.__name__)
    return httpd
