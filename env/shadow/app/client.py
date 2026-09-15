"""ingress / dispatcher 调用 core 内部 API 的小客户端。"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Optional

from . import config


class CoreError(RuntimeError):
    def __init__(self, status: int, body: Any):
        super().__init__(f"core {status}: {body}")
        self.status = status
        self.body = body


def call(method: str, path: str, payload: Optional[dict] = None,
         timeout: float = 10.0) -> dict:
    url = config.CORE_URL.rstrip("/") + path
    data = None
    headers = {"X-Internal-Token": config.INTERNAL_TOKEN}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
        except Exception:
            body = None
        raise CoreError(e.code, body)
