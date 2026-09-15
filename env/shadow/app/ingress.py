"""ingress 服务：设备接入网关（独立部署）。

设备协议（HTTP，无需长连接库）：
  POST /devices/commands/poll   header: X-Device-Token
        long-poll：心跳 + 拉取下一条按版本排序的命令；无命令时挂起后返回空
  POST /devices/ack             确认命令（version 或 command_id），重复确认安全
  POST /devices/reported        上报报告态（旧版本/重复版本会被 core 拒绝）

设计要点：
- 接入与存储解耦：本服务无状态、可水平扩展，所有状态都在 core/数据库，
  重启后设备重新 poll 即恢复，已排队命令不会丢。
- poll 时先心跳（上线），再向 core 原子领取下一条命令，因此重新上线后
  严格按版本顺序补发。
- 发送给设备失败时（断连）回调 core 的 failed 接口，由 core 决定
  指数退避重试 / 超次 FAILED / 过期 EXPIRED。
"""
from __future__ import annotations

import logging
import socket
import time
from http.server import ThreadingHTTPServer
from threading import Event

from . import client, config, httputil

log = logging.getLogger("shadow.ingress")


class IngressHandler(httputil.JsonApp):
    # 关闭事件，测试时可注入
    stop_event: Event = Event()

    def _auth_device(self) -> bool:
        token = self.headers.get("X-Device-Token", "")
        try:
            res = client.call("POST", "/internal/resolve", {"token": token},
                              timeout=5)
        except client.CoreError as e:
            self.json_response(e.status or 401,
                               e.body or {"error": "auth failed"})
            return False
        self.device_id = res["device_id"]
        return True

    # ---------- 健康检查 ----------
    def h_healthz(self, body=None):
        self.json_response(200, {"ok": True, "service": "ingress"})

    # ---------- 设备接口 ----------
    def _client_alive(self) -> bool:
        """在向 core 领取命令前确认设备连接仍然挂着（RST 后不可写）。"""
        try:
            self.connection.setblocking(False)
            try:
                peek = self.connection.recv(1, socket.MSG_PEEK)
                if peek == b"":
                    return False
            except BlockingIOError:
                return True  # 对端没发数据，连接正常
            finally:
                self.connection.setblocking(True)
            return True
        except OSError:
            return False

    def h_poll(self, body):
        device_id = self.device_id
        try:
            hb = client.call(
                "POST", f"/internal/devices/{device_id}/heartbeat", {})
        except client.CoreError as e:
            self.json_response(502, {"error": "core error",
                                     "detail": e.body})
            return

        def _claim():
            return client.call("POST", "/internal/commands/claim",
                               {"device_id": device_id}).get("command")

        try:
            cmd = _claim()
        except client.CoreError as e:
            self.json_response(502, {"error": "core error",
                                     "detail": e.body})
            return

        deadline = time.time() + config.DEVICE_POLL_WAIT
        while cmd is None and time.time() < deadline \
                and not self.stop_event.is_set():
            time.sleep(1.0)
            if not self._client_alive():
                # 设备已断线：不要再领取/派发，直接结束线程
                log.info("device %s gone during long-poll", device_id)
                return
            try:
                client.call("POST",
                            f"/internal/devices/{device_id}/heartbeat", {})
                cmd = _claim()
            except client.CoreError:
                break

        became_online = hb.get("became_online", False)
        if cmd is None:
            self.json_response(204 if not became_online else 200,
                               {"device_id": device_id, "command": None,
                                "online": True})
            return

        if not self._client_alive():
            # 命令已领取但设备连接已断：立刻上报派发失败，触发退避重试
            log.warning("device %s disconnected before delivery cmd=%s",
                        device_id, cmd["id"])
            try:
                client.call("POST",
                            f"/internal/commands/{cmd['id']}/failed",
                            {"error": "DEVICE_GONE_BEFORE_DELIVERY"})
            except client.CoreError:
                log.exception("report delivery-failed error")
            return

        payload = {"device_id": device_id, "command": cmd}
        delivered = self._deliver(200, payload)
        if not delivered:
            log.warning("delivery failed to device %s cmd=%s, report to core",
                        device_id, cmd["id"])
            try:
                client.call("POST",
                            f"/internal/commands/{cmd['id']}/failed",
                            {"error": "DEVICE_CONNECTION_CLOSED"})
            except client.CoreError:
                log.exception("report delivery-failed error")

    def h_ack(self, body):
        device_id = self.device_id
        try:
            res = client.call("POST",
                              f"/internal/devices/{device_id}/ack", body)
        except client.CoreError as e:
            self.json_response(e.status or 502,
                               e.body or {"error": "core error"})
            return
        # 重复确认返回 200 + duplicate:true，设备可安全重发
        self.json_response(200, res)

    def h_reported(self, body):
        device_id = self.device_id
        try:
            res = client.call(
                "POST", f"/internal/devices/{device_id}/reported", body)
        except client.CoreError as e:
            if e.status == 409:
                # 旧版本/重复版本被拒绝：原样转达拒绝原因
                self.json_response(409, e.body or {})
                return
            self.json_response(502, {"error": "core error",
                                     "detail": e.body})
            return
        self.json_response(200, res)

    def _deliver(self, code: int, obj) -> bool:
        """向设备写响应，连接断开返回 False。"""
        import json
        raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type",
                             "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, TimeoutError,
                OSError) as e:
            log.info("device socket write error: %s", e)
            return False

    def routes(self, method: str):
        D = {"device_auth": True}
        return [
            (r"/healthz", self.h_healthz, {}),
            (r"/devices/commands/poll", self.h_poll, D),
            (r"/devices/ack", self.h_ack, D),
            (r"/devices/reported", self.h_reported, D),
        ]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    httpd = ThreadingHTTPServer((config.INGRESS_HTTP_HOST,
                                 config.INGRESS_HTTP_PORT), IngressHandler)
    log.info("ingress listening on %s:%d -> core %s",
             config.INGRESS_HTTP_HOST, config.INGRESS_HTTP_PORT,
             config.CORE_URL)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
