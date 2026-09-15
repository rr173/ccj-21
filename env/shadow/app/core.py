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

        # ---------- 控制面：设备组 / 分批发布 ----------
        def h_create_group(self, body):
            if not body.get("id"):
                self.json_response(400, {"error": "id required"})
                return
            try:
                res = store.upsert_group(
                    body["id"], body.get("name", ""),
                    int(body.get("priority", 100)),
                    body.get("device_ids"))
            except KeyError as e:
                self.json_response(404, {"error": str(e)})
                return
            except (TypeError, ValueError) as e:
                self.json_response(400, {"error": str(e)})
                return
            self.json_response(201 if res.get("created") else 200, res)

        def h_list_groups(self, body=None):
            self.json_response(200, {"groups": store.list_groups()})

        def h_get_group(self, group_id, body=None):
            group = store.get_group(group_id)
            if group is None:
                self.json_response(404, {"error": "group not found"})
                return
            self.json_response(200, group)

        def h_create_release(self, body):
            if not body.get("group_id"):
                self.json_response(400, {"error": "group_id required"})
                return
            if not isinstance(body.get("target_state"), dict):
                self.json_response(400,
                                   {"error": "target_state (object) required"})
                return
            try:
                res = store.create_release(
                    body["group_id"], body["target_state"],
                    batch_percent=float(body.get("batch_percent", 20.0)),
                    confirm_threshold=float(
                        body.get("confirm_threshold", 100.0)),
                    drift_threshold=float(body.get("drift_threshold", 0.0)),
                    batch_deadline_seconds=float(
                        body.get("batch_deadline_seconds", 300.0)),
                    created_by=body.get("created_by", ""),
                    idem_key=body.get("idempotency_key"))
            except KeyError as e:
                self.json_response(404, {"error": str(e)})
                return
            except (TypeError, ValueError) as e:
                self.json_response(400, {"error": str(e)})
                return
            self.json_response(201, res)

        def h_list_releases(self, body=None):
            from urllib.parse import parse_qs, urlparse
            status = parse_qs(urlparse(self.path).query).get(
                "status", [None])[0]
            self.json_response(
                200, {"releases": store.list_releases(status)})

        def h_get_release(self, release_id, body=None):
            view = store.release_read_model(release_id)
            if view is None:
                self.json_response(404, {"error": "release not found"})
                return
            self.json_response(200, view)

        def h_release_devices(self, release_id, body=None):
            if store.release_read_model(release_id) is None:
                self.json_response(404, {"error": "release not found"})
                return
            from urllib.parse import parse_qs, urlparse
            q = parse_qs(urlparse(self.path).query)
            batch_no = q.get("batch_no", [None])[0]
            status = q.get("status", [None])[0]
            devices = store.list_release_devices(
                release_id,
                int(batch_no) if batch_no is not None else None, status)
            self.json_response(200, {"devices": devices})

        def h_release_conflicts(self, release_id, body=None):
            if store.release_read_model(release_id) is None:
                self.json_response(404, {"error": "release not found"})
                return
            self.json_response(
                200, {"release_id": release_id,
                      "conflicts": store.release_conflicts(release_id)})

        def h_release_events(self, release_id, body=None):
            if store.release_read_model(release_id) is None:
                self.json_response(404, {"error": "release not found"})
                return
            from urllib.parse import parse_qs, urlparse
            q = parse_qs(urlparse(self.path).query)
            limit = int(q.get("limit", ["500"])[0])
            offset = int(q.get("offset", ["0"])[0])
            self.json_response(
                200, {"events": store.list_release_events(
                    release_id, limit, offset)})

        def _release_action(self, release_id, body, method):
            if store.release_read_model(release_id) is None:
                self.json_response(404, {"error": "release not found"})
                return
            body = body or {}
            try:
                res = method(release_id, body.get("idempotency_key")) \
                    if method != store.pause_release else \
                    store.pause_release(
                        release_id, body.get("reason", "ADMIN_PAUSED"),
                        body.get("idempotency_key"))
            except (TypeError, ValueError) as e:
                self.json_response(400, {"error": str(e)})
                return
            self.json_response(200, res)

        def h_pause_release(self, release_id, body):
            self._release_action(release_id, body, store.pause_release)

        def h_continue_release(self, release_id, body):
            self._release_action(release_id, body, store.continue_release)

        def h_skip_release(self, release_id, body):
            self._release_action(release_id, body,
                                 store.skip_failed_devices)

        def h_rollback_release(self, release_id, body):
            self._release_action(release_id, body, store.rollback_release)

        def h_release_tick(self, body):
            self.json_response(200, store.tick_releases())

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

        def i_release_tick(self, body):
            self.json_response(200, store.tick_releases())

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
                (r"/v1/groups", self.h_create_group, {})
                if method == "POST"
                else (r"/v1/groups", self.h_list_groups, {}),
                (r"/v1/groups/([A-Za-z0-9_.:-]+)", self.h_get_group, {}),
                (r"/v1/releases", self.h_create_release, {})
                if method == "POST"
                else (r"/v1/releases", self.h_list_releases, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)",
                 self.h_get_release, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/devices",
                 self.h_release_devices, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/conflicts",
                 self.h_release_conflicts, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/events",
                 self.h_release_events, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/pause",
                 self.h_pause_release, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/continue",
                 self.h_continue_release, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/skip-failed",
                 self.h_skip_release, {}),
                (r"/v1/releases/([A-Za-z0-9_.:-]+)/rollback",
                 self.h_rollback_release, {}),
                # internal
                (r"/internal/tick", self.i_tick, I),
                (r"/internal/release-tick", self.i_release_tick, I),
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
