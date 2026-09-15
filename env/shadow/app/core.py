"""core 服务：设备影子核心 —— 控制面 API + 内部状态机 API。

职责独立：状态存储、版本规则、命令生成/确认全部在此完成并落库；
ingress（设备接入）与 dispatcher（派发/维护）都只是它的客户端，
可各自独立部署、独立重启，状态不依赖进程内存。
"""
from __future__ import annotations

import logging
import os
from functools import partial

from . import config, httputil
from .store import Store

log = logging.getLogger("shadow.core")


def make_handler(store: Store):
    class CoreHandler(httputil.JsonApp):
        # ---------- 健康检查 ----------
        def h_healthz(self, body=None):
            self.json_response(200, {"ok": True, "service": "core"})

        # ---------- 控制面：设备 ----------
        def h_register(self, body):
            if not body.get("id"):
                self.json_response(400, {"error": "id required"})
                return
            res = store.provision_device(body["id"], body.get("name", ""),
                                         body.get("token"))
            self.json_response(201 if res.get("created") else 200, res)

        def h_list_devices(self, body=None):
            self.json_response(200, {"devices": store.list_devices()})

        def h_get_shadow(self, device_id, body=None):
            view = store.shadow_read_model(device_id)
            if view is None:
                self.json_response(404, {"error": "device not found"})
                return
            self.json_response(200, view)

        # ---------- 控制面：期望态 ----------
        def h_put_desired(self, device_id, body):
            if "state" not in body or not isinstance(body["state"], dict):
                self.json_response(400,
                                   {"error": "state (object) required"})
                return
            idem = body.get("idempotency_key")
            res = store.set_desired(device_id, body["state"],
                                    body.get("expected_version"), idem)
            if res.get("status") == "CONFLICT":
                self.json_response(409, res)
            else:
                self.json_response(200, res)

        # ---------- 控制面：追溯查询 ----------
        def h_list_commands(self, device_id, body=None):
            if store.get_device_by_id(device_id) is None:
                self.json_response(404, {"error": "device not found"})
                return
            self.json_response(200,
                               {"commands": store.list_commands(device_id)})

        def h_attempts(self, command_id, body=None):
            self.json_response(200,
                               {"attempts": store.list_attempts(command_id)})

        def h_events(self, body=None):
            from urllib.parse import parse_qs, urlparse
            q = parse_qs(urlparse(self.path).query)
            dev = q.get("device_id", [None])[0]
            limit = int(q.get("limit", ["100"])[0])
            offset = int(q.get("offset", ["0"])[0])
            self.json_response(
                200, {"events": store.list_events(dev, limit, offset)})

        # ---------- 内部 API（ingress / dispatcher 调用） ----------
        def i_resolve(self, body):
            token = body.get("token") or ""
            row = store.get_device_by_token(token)
            if row is None:
                self.json_response(401, {"error": "invalid device token"})
                return
            self.json_response(200, {"device_id": row["id"],
                                     "name": row["name"]})

        def i_heartbeat(self, device_id, body):
            if store.get_device_by_id(device_id) is None:
                self.json_response(404, {"error": "device not found"})
                return
            self.json_response(200, store.touch(device_id))

        def i_claim(self, body):
            device_id = body.get("device_id")
            if not device_id:
                self.json_response(400, {"error": "device_id required"})
                return
            cmd = store.claim_next_command(device_id)
            self.json_response(200, {"command": cmd})

        def i_failed(self, command_id, body):
            res = store.delivery_failed(command_id,
                                        body.get("error", "unknown"))
            self.json_response(200, res)

        def i_reported(self, device_id, body):
            try:
                version = int(body.get("version"))
            except (TypeError, ValueError):
                self.json_response(400, {"error": "version (int) required"})
                return
            state = body.get("state", {})
            if not isinstance(state, dict):
                self.json_response(400, {"error": "state must be object"})
                return
            try:
                res = store.report_state(device_id, version, state)
            except KeyError:
                self.json_response(404, {"error": "device not found"})
                return
            self.json_response(200 if res["accepted"] else 409, res)

        def i_ack(self, device_id, body):
            res = store.ack_by_device(
                device_id, body.get("version"), body.get("command_id"),
                body.get("code", "OK"), body.get("message", ""))
            code = 200 if res.get("status") in (
                "ACKED", "DUPLICATE_ACK") else 404
            self.json_response(code, res)

        def i_tick(self, body):
            self.json_response(200, store.tick())

        def i_sweep(self, body):
            self.json_response(200, {"changes": store.sweep_offline()})

        def routes(self, method: str):
            I = {"internal": True}
            return [
                (r"/healthz", self.h_healthz, {}),
                (r"/v1/devices", self.h_register, {}) if method == "POST"
                else (r"/v1/devices", self.h_list_devices, {}),
                (r"/v1/events", self.h_events, {}),
                (r"/v1/devices/([A-Za-z0-9_.:-]+)", self.h_get_shadow, {}),
                (r"/v1/devices/([A-Za-z0-9_.:-]+)/desired",
                 self.h_put_desired, {}),
                (r"/v1/devices/([A-Za-z0-9_.:-]+)/commands",
                 self.h_list_commands, {}),
                (r"/v1/commands/([A-Za-z0-9_.:-]+)/attempts",
                 self.h_attempts, {}),
                # internal
                (r"/internal/tick", self.i_tick, I),
                (r"/internal/sweep-offline", self.i_sweep, I),
                (r"/internal/resolve", self.i_resolve, I),
                (r"/internal/commands/claim", self.i_claim, I),
                (r"/internal/commands/([A-Za-z0-9_.:-]+)/failed",
                 self.i_failed, I),
                (r"/internal/devices/([A-Za-z0-9_.:-]+)/heartbeat",
                 self.i_heartbeat, I),
                (r"/internal/devices/([A-Za-z0-9_.:-]+)/reported",
                 self.i_reported, I),
                (r"/internal/devices/([A-Za-z0-9_.:-]+)/ack",
                 self.i_ack, I),
            ]

    return CoreHandler


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    store = Store(config.DB_PATH)
    handler = make_handler(store)
    httpd = httputil.serve(config.CORE_HTTP_HOST, config.CORE_HTTP_PORT,
                           handler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        store.close()


if __name__ == "__main__":
    main()
