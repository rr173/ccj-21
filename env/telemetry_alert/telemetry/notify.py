"""通知发送器：可插拔的投递通道。

TelemetryService 只依赖一个可调用对象 sender(notification_dict)，
发送失败时抛出异常即可进入退避重试。这里提供几种常用实现。
"""
from __future__ import annotations

import json
import urllib.request


class HttpWebhookSender:
    """把通知以 JSON POST 到 webhook；非 2xx 视为失败。"""

    def __init__(self, url, timeout_sec=5.0, headers=None):
        self.url = url
        self.timeout_sec = timeout_sec
        self.headers = {"Content-Type": "application/json"}
        if headers:
            self.headers.update(headers)

    def __call__(self, notification):
        body = notification["payload"]
        if isinstance(body, str):
            body = body.encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, headers=self.headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
            if resp.status >= 300:
                raise RuntimeError(f"webhook returned HTTP {resp.status}")


class FileSender:
    """把通知追加写入 JSONL 文件（演示/联调用）。"""

    def __init__(self, path):
        self.path = path

    def __call__(self, notification):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(notification, ensure_ascii=False) + "\n")


class CallableSender:
    """包装任意可调用对象。"""

    def __init__(self, fn):
        self.fn = fn

    def __call__(self, notification):
        return self.fn(notification)
