"""分批发布状态机。

所有方法都在 Store 的写锁和 SQLite 事务内执行。发布只会生成新版本的逐设备
命令；继续/回滚产生的也是新 desired version，绝不更新或覆盖历史命令。
"""
from __future__ import annotations

import json
import uuid
from typing import Any, Optional

from . import config, model


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


class ReleaseStoreMixin:
    # ---------------- 设备组 ----------------
    def upsert_group(self, group_id: str, name: str = "",
                     priority: int = 100,
                     device_ids: Optional[list[str]] = None) -> dict:
        with self._wlock:
            now = self.clock()
            row = self._row("SELECT id FROM device_groups WHERE id=?",
                            (group_id,))
            with self._conn:
                if row:
                    self._conn.execute(
                        "UPDATE device_groups SET name=?, priority=?,"
                        " updated_at=? WHERE id=?",
                        (name, priority, now, group_id))
                    if device_ids is not None:
                        self._conn.execute(
                            "DELETE FROM group_members WHERE group_id=?",
                            (group_id,))
                        self._add_members(group_id, device_ids, now)
                    created = False
                else:
                    self._conn.execute(
                        "INSERT INTO device_groups(id,name,priority,"
                        "created_at,updated_at) VALUES(?,?,?,?,?)",
                        (group_id, name, priority, now, now))
                    if device_ids is not None:
                        self._add_members(group_id, device_ids, now)
                    created = True
            return self.get_group(group_id) | {"created": created}

    def _add_members(self, group_id: str, device_ids: list[str],
                     now: float) -> None:
        for device_id in dict.fromkeys(device_ids):
            if self.get_device_by_id(device_id) is None:
                raise KeyError(f"device not found: {device_id}")
            self._conn.execute(
                "INSERT OR IGNORE INTO group_members(group_id,device_id,"
                "added_at) VALUES(?,?,?)", (group_id, device_id, now))

    def list_groups(self) -> list[dict]:
        rows = self._all(
            "SELECT g.*, COUNT(gm.device_id) AS device_count"
            " FROM device_groups g LEFT JOIN group_members gm"
            " ON gm.group_id=g.id GROUP BY g.id ORDER BY g.priority,g.id")
        return [dict(r) for r in rows]

    def get_group(self, group_id: str) -> Optional[dict]:
        row = self._row(
            "SELECT * FROM device_groups WHERE id=?", (group_id,))
        if row is None:
            return None
        devices = [r["device_id"] for r in self._all(
            "SELECT device_id FROM group_members WHERE group_id=?"
            " ORDER BY device_id", (group_id,))]
        return {**dict(row), "devices": devices}

    # ---------------- 创建发布 ----------------
    def create_release(self, group_id: str, target_state: dict, *,
                       batch_percent: float = 20.0,
                       confirm_threshold: float = 100.0,
                       drift_threshold: float = 0.0,
                       batch_deadline_seconds: float = 300.0,
                       created_by: str = "",
                       idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            if idem_key:
                hit = self._row(
                    "SELECT response FROM idempotent_requests"
                    " WHERE idem_key=?", (idem_key,))
                if hit:
                    return {"replayed": True, **json.loads(hit["response"])}
            group = self.get_group(group_id)
            if group is None:
                raise KeyError(f"group not found: {group_id}")
            if not isinstance(target_state, dict):
                raise ValueError("target_state must be object")
            if not 0 < batch_percent <= 100:
                raise ValueError("batch_percent must be in (0,100]")
            if not 0 < confirm_threshold <= 100:
                raise ValueError("confirm_threshold must be in (0,100]")
            if not 0 <= drift_threshold <= 1:
                raise ValueError("drift_threshold must be in [0,1]")
            if batch_deadline_seconds <= 0:
                raise ValueError("batch_deadline_seconds must be > 0")
            device_ids = sorted(group["devices"])
            if not device_ids:
                raise ValueError("group has no devices")
            batches = model.split_batches(device_ids, batch_percent)
            release_id = new_id("rel")
            with self._conn:
                self._conn.execute(
                    "INSERT INTO releases(id,group_id,target_state,"
                    "batch_percent,confirm_threshold,drift_threshold,"
                    "batch_deadline_seconds,status,current_batch,gate_reason,"
                    "created_at,created_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (release_id, group_id, _json(target_state),
                     batch_percent, confirm_threshold, drift_threshold,
                     batch_deadline_seconds, config.RL_PENDING, 0,
                     "WAITING_CONFLICT", now, created_by))
                ordinal = 0
                for batch_no, members in enumerate(batches, start=1):
                    self._conn.execute(
                        "INSERT INTO release_batches(release_id,batch_no,"
                        "status,device_count,gate_reason)"
                        " VALUES(?,?,?,?,?)",
                        (release_id, batch_no, "WAITING", len(members), ""))
                    for device_id in members:
                        shadow = self._row(
                            "SELECT desired_version, desired FROM shadows"
                            " WHERE device_id=?", (device_id,))
                        self._conn.execute(
                            "INSERT INTO release_devices(release_id,device_id,"
                            "batch_no,ordinal,baseline_version,baseline_state,"
                            "status,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                            (release_id, device_id, batch_no, ordinal,
                             shadow["desired_version"], shadow["desired"],
                             config.RD_WAITING, now))
                        ordinal += 1
                self._record_op_unlocked(release_id, "CREATE", idem_key,
                                         "COMPLETED", {"devices": len(device_ids),
                                                       "batches": len(batches)},
                                         now)
                self.event(None, "RELEASE_CREATED",
                           {"release_id": release_id, "group_id": group_id,
                            "target_state": target_state,
                            "batch_percent": batch_percent,
                            "confirm_threshold": confirm_threshold,
                            "drift_threshold": drift_threshold,
                            "batch_deadline_seconds": batch_deadline_seconds,
                            "devices": device_ids},
                           at=now, release_id=release_id)
            result = {"status": config.RL_PENDING, "release_id": release_id,
                      "group_id": group_id, "device_count": len(device_ids),
                      "batch_count": len(batches)}
            if idem_key:
                self._conn.execute(
                    "INSERT OR IGNORE INTO idempotent_requests"
                    "(idem_key,response,created_at) VALUES(?,?,?)",
                    (idem_key, _json(result), now))
            return result

    def _record_op_unlocked(self, release_id: str, operation: str,
                            idem_key: Optional[str], status: str,
                            detail: dict, now: float) -> None:
        self._conn.execute(
            "INSERT INTO release_operations(release_id,operation,"
            "idempotency_key,status,detail,created_at)"
            " VALUES(?,?,?,?,?,?)",
            (release_id, operation, idem_key, status, _json(detail), now))

    # ---------------- 调度与门禁 ----------------
    def holding_release_for_device(self, device_id: str) -> Optional[dict]:
        """返回仍占用设备的活动/暂停/回滚发布（用于阻止普通期望命令）。"""
        row = self._row(
            "SELECT r.* FROM releases r JOIN release_devices rd"
            " ON rd.release_id=r.id WHERE rd.device_id=?"
            " AND ((r.status=? AND rd.status IN (?,?,?,?,?,?)) OR"
            " (r.status=? AND rd.status IN (?,?)) OR"
            " (r.status=? AND rd.status IN (?,?,?,?,?,?)) OR"
            " (r.status=? AND rd.status IN (?,?)))"
            " ORDER BY r.created_at LIMIT 1",
            (device_id,
             config.RL_ACTIVE, config.RD_ACTIVE, config.RD_MATCHED,
             config.RD_REJECTED, config.RD_FAILED, config.RD_EXPIRED,
             config.RD_DRIFT,
             config.RL_ROLLING_BACK, config.RD_ROLLBACK_ACTIVE,
             config.RD_ROLLBACK_FAILED,
             config.RL_PAUSED, config.RD_ACTIVE, config.RD_MATCHED,
             config.RD_REJECTED, config.RD_FAILED, config.RD_EXPIRED,
             config.RD_DRIFT,
             config.RL_PAUSED, config.RD_ROLLBACK_ACTIVE,
             config.RD_ROLLBACK_FAILED))
        return dict(row) if row else None

    def active_release_for_device(self, device_id: str) -> Optional[dict]:
        row = self._row(
            "SELECT r.* FROM releases r JOIN release_devices rd"
            " ON rd.release_id=r.id WHERE rd.device_id=?"
            " AND ((r.status=? AND rd.status IN (?,?,?,?,?,?)) OR"
            " (r.status=? AND rd.status=?))"
            " ORDER BY r.created_at LIMIT 1",
            (device_id, config.RL_ACTIVE, config.RD_ACTIVE,
             config.RD_MATCHED, config.RD_REJECTED, config.RD_FAILED,
             config.RD_EXPIRED, config.RD_DRIFT,
             config.RL_ROLLING_BACK, config.RD_ROLLBACK_ACTIVE))
        return dict(row) if row else None

    def _unresolved_devices_unlocked(self, release_id: str,
                                     statuses: tuple[str, ...] = (
                                         config.RD_ACTIVE,
                                         config.RD_ROLLBACK_ACTIVE)) -> set[str]:
        rows = self._all(
            "SELECT device_id FROM release_devices WHERE release_id=?"
            f" AND status IN ({','.join('?' for _ in statuses)})",
            (release_id, *statuses))
        return {r["device_id"] for r in rows}

    def _release_locks(self, statuses: tuple[str, ...]) -> dict[str, dict]:
        rows = self._all(
            "SELECT r.id, r.group_id, r.created_at, g.priority, r.status,"
            " r.phase, rd.device_id, rd.status AS device_status"
            " FROM releases r JOIN device_groups g ON g.id=r.group_id"
            " JOIN release_devices rd ON rd.release_id=r.id"
            f" WHERE r.status IN ({','.join('?' for _ in statuses)})",
            statuses)
        forward_held = {config.RD_ACTIVE, config.RD_MATCHED,
                        config.RD_REJECTED, config.RD_FAILED,
                        config.RD_EXPIRED, config.RD_DRIFT}
        rollback_held = {config.RD_ROLLBACK_ACTIVE,
                         config.RD_ROLLBACK_FAILED}
        locks: dict[str, dict] = {}
        for r in rows:
            holds = (rollback_held if r["status"] == config.RL_ROLLING_BACK
                     else forward_held)
            if r["device_status"] in holds:
                locks.setdefault(r["device_id"], dict(r))
        return locks

    def _refresh_release_for_command_unlocked(self, cmd, now: float) -> None:
        if not cmd["release_id"]:
            return
        release = self._release(cmd["release_id"])
        if release is None or release["status"] not in (
                config.RL_ACTIVE, config.RL_ROLLING_BACK):
            return
        item = self._release_device(cmd["release_id"], cmd["device_id"])
        if item is None:
            return
        self._refresh_item_unlocked(item, release, now)
        self._evaluate_release_unlocked(cmd["release_id"], now)

    def report_release_progress(self, device_id: str, now: float) -> None:
        """设备报告后尝试推进其命中的活动发布。"""
        with self._wlock:
            release = self.active_release_for_device(device_id)
            if release is not None:
                with self._conn:
                    item = self._release_device(release["id"], device_id)
                    if item is not None:
                        self._refresh_item_unlocked(item, release, now)
                        self._evaluate_release_unlocked(release["id"], now)

    def _pending_candidates_unlocked(self) -> list[dict]:
        rows = self._all(
            "SELECT r.*, g.priority FROM releases r"
            " JOIN device_groups g ON g.id=r.group_id"
            " WHERE r.status=? ORDER BY g.priority ASC, r.created_at ASC,"
            " r.id ASC", (config.RL_PENDING,))
        return [dict(r) for r in rows]

    def tick_releases(self) -> dict:
        with self._wlock:
            return self._tick_releases_unlocked(self.clock())

    def _tick_releases_unlocked(self, now: float) -> dict:
        activated: list[str] = []
        paused: list[dict] = []
        completed: list[str] = []
        with self._conn:
            for release in self._all(
                    "SELECT id FROM releases WHERE status IN (?,?)"
                    " ORDER BY created_at",
                    (config.RL_ACTIVE, config.RL_ROLLING_BACK)):
                outcome = self._evaluate_release_unlocked(
                    release["id"], now)
                paused.extend(outcome["paused"])
                if outcome["completed"]:
                    completed.append(release["id"])

            held = self._release_locks((
                config.RL_ACTIVE, config.RL_PAUSED,
                config.RL_ROLLING_BACK))
            candidates = self._pending_candidates_unlocked()
            for release in candidates:
                devices = {r["device_id"] for r in self._all(
                    "SELECT device_id FROM release_devices"
                    " WHERE release_id=?", (release["id"],))}
                blockers: list[dict] = []
                for device_id in sorted(devices):
                    holder = held.get(device_id)
                    if holder and holder["id"] != release["id"]:
                        blockers.append({
                            "device_id": device_id,
                            "blocked_by_release_id": holder["id"],
                            "blocked_by_group_id": holder["group_id"],
                            "blocked_by_status": holder["status"],
                            "reason": "GROUP_PRIORITY_AND_CREATION_ORDER"})
                if blockers:
                    self._update_release_unlocked(
                        release["id"], gate_reason="BLOCKED_BY_RELEASE")
                    continue
                rid = self._start_release_unlocked(release["id"], now)
                activated.append(rid)
                for device_id in devices:
                    held[device_id] = {"id": rid,
                                       "group_id": release["group_id"]}
        return {"at": now, "activated": activated, "paused": paused,
                "completed": completed}

    def _start_release_unlocked(self, release_id: str, now: float) -> str:
        release = self._release(release_id)
        members = self._batch_devices(release_id, 1)
        deadline = now + release["batch_deadline_seconds"]
        self._conn.execute(
            "UPDATE releases SET status=?, current_batch=1, started_at=?,"
            " gate_reason=?, pause_reason='', paused_at=NULL WHERE id=?",
            (config.RL_ACTIVE, now, "BATCH_ACTIVE", release_id))
        self._set_batch_unlocked(release_id, 1, "ACTIVE", now, deadline,
                                 "BATCH_ACTIVE")
        for device_id in members:
            enq = self._enqueue_desired_unlocked(
                device_id, json.loads(release["target_state"]), now,
                release_id=release_id, phase="FORWARD")
            self._set_device_unlocked(
                release_id, device_id, config.RD_ACTIVE,
                command_column="forward_command_id", command_id=enq["id"],
                error="", at=now, extra={"version": enq["version"],
                                         "batch_no": 1})
            self.event(device_id, "RELEASE_DEVICE_BATCH_STARTED",
                       {"release_id": release_id, "batch_no": 1,
                        "command_id": enq["id"], "version": enq["version"],
                        "deadline_at": deadline},
                       command_id=enq["id"], at=now, release_id=release_id)
        self._release_event_unlocked(
            release_id, "RELEASE_STARTED",
            {"batch_no": 1, "device_count": len(members),
             "deadline_at": deadline}, now)
        return release_id

    def _refresh_item_unlocked(self, item, release, now: float) -> tuple[
            str, Optional[dict]]:
        """根据命令和报告态刷新单设备门禁，返回 (当前状态, 暂停事件)。"""
        status = item["status"]
        # MATCHED 不是终点：设备后续上报漂移时要能立即重新门禁。
        if status not in (config.RD_ACTIVE, config.RD_ROLLBACK_ACTIVE,
                          config.RD_MATCHED):
            return status, None
        phase = ("ROLLBACK" if status == config.RD_ROLLBACK_ACTIVE
                 else "FORWARD")
        command_col = ("rollback_command_id" if phase == "ROLLBACK"
                       else "forward_command_id")
        cmd_id = item[command_col]
        cmd = self._row("SELECT * FROM commands WHERE id=?", (cmd_id,))
        phase = "ROLLBACK" if status == config.RD_ROLLBACK_ACTIVE else "FORWARD"
        target_raw = (item["baseline_state"]
                      if phase == "ROLLBACK" else release["target_state"])
        target = json.loads(target_raw)
        next_status: Optional[str] = None
        error = item["last_error"]

        if cmd is None:
            next_status = (config.RD_ROLLBACK_FAILED if phase == "ROLLBACK"
                           else config.RD_FAILED)
            error = "COMMAND_NOT_FOUND"
        elif cmd["status"] == config.ST_FAILED:
            next_status = (config.RD_ROLLBACK_FAILED if phase == "ROLLBACK"
                           else config.RD_FAILED)
            error = cmd["last_error"] or "MAX_DELIVERY_ATTEMPTS"
        elif cmd["status"] == config.ST_EXPIRED:
            next_status = config.RD_EXPIRED if phase == "FORWARD" \
                else config.RD_ROLLBACK_FAILED
            error = "COMMAND_EXPIRED"
        elif cmd["status"] == config.ST_ACKED and \
                cmd["ack_code"] not in config.ACK_SUCCESS_CODES:
            next_status = config.RD_REJECTED if phase == "FORWARD" \
                else config.RD_ROLLBACK_FAILED
            error = f"DEVICE_REJECTED:{cmd['ack_code']}"
        elif cmd["status"] == config.ST_ACKED:
            shadow = self._row(
                "SELECT reported, reported_updated_at FROM shadows"
                " WHERE device_id=?", (item["device_id"],))
            # ACK 只表示设备接受命令；门禁必须等 ACK 后新报告。不能把旧报告
            # 与新目标不同误判为漂移，那只是 ACKED_NOT_REPORTED。
            if shadow["reported_updated_at"] is None or \
                    shadow["reported_updated_at"] < cmd["acked_at"]:
                next_status = None
            else:
                reported = model.parse_json(shadow["reported"])
                if model.state_matches(target, reported,
                                       release["drift_threshold"]):
                    next_status = (config.RD_ROLLED_BACK
                                   if phase == "ROLLBACK" else config.RD_MATCHED)
                elif model.drift_ratio(target, reported) > \
                        release["drift_threshold"]:
                    # ACK 后曾匹配但后来漂移也会落到这里；当前报告已超阈值。
                    next_status = config.RD_DRIFT if phase == "FORWARD" \
                        else config.RD_ROLLBACK_FAILED
                    error = "STATE_DRIFT"

        if next_status and next_status != status:
            self._set_device_unlocked(
                release["id"], item["device_id"], next_status,
                command_column=command_col, command_id=cmd_id,
                error=error, at=now,
                extra={"version": cmd["version"] if cmd else None,
                       "batch_no": item["batch_no"], "phase": phase})
            event_type = ("RELEASE_DEVICE_MATCHED"
                          if next_status in (config.RD_MATCHED,
                                             config.RD_ROLLED_BACK)
                          else "RELEASE_DEVICE_FAILED")
            self.event(item["device_id"], event_type,
                       {"release_id": release["id"],
                        "batch_no": item["batch_no"], "status": next_status,
                        "command_id": cmd_id, "phase": phase, "error": error},
                       command_id=cmd_id, at=now, release_id=release["id"])
        failure = next_status in (config.RD_REJECTED, config.RD_FAILED,
                                  config.RD_EXPIRED, config.RD_DRIFT,
                                  config.RD_ROLLBACK_FAILED)
        pause = {"release_id": release["id"], "device_id": item["device_id"],
                 "reason": next_status, "batch_no": item["batch_no"],
                 "phase": phase} if failure else None
        return next_status or status, pause

    def _evaluate_release_unlocked(self, release_id: str, now: float) -> dict:
        release = self._release(release_id)
        items = self._all(
            "SELECT * FROM release_devices WHERE release_id=?",
            (release_id,))
        paused: list[dict] = []
        for item in items:
            _, pause = self._refresh_item_unlocked(item, release, now)
            if pause:
                paused.append(pause)
        # _refresh_item_unlocked 更新数据库；重新读取以避免本事务内本地快照过旧。
        items = self._all(
            "SELECT * FROM release_devices WHERE release_id=?",
            (release_id,))

        rollback = release["status"] == config.RL_ROLLING_BACK
        active_status = (config.RD_ROLLBACK_ACTIVE if rollback
                         else config.RD_ACTIVE)
        success_status = config.RD_ROLLED_BACK if rollback \
            else config.RD_MATCHED
        active = [r for r in items
                  if r["status"] in (config.RD_ACTIVE,
                                     config.RD_ROLLBACK_ACTIVE)]
        failures = [dict(r) for r in self._all(
            "SELECT * FROM release_devices WHERE release_id=? AND status IN"
            " (?,?,?,?,?)",
            (release_id, config.RD_REJECTED, config.RD_FAILED,
             config.RD_EXPIRED, config.RD_DRIFT,
             config.RD_ROLLBACK_FAILED))]

        if failures:
            reason = failures[0]["status"]
            detail = {"reason": reason, "failures": [
                {"device_id": r["device_id"], "status": r["status"],
                 "batch_no": r["batch_no"], "error": r["last_error"]}
                for r in failures]}
            self._pause_release_unlocked(
                release_id, reason, detail, now,
                status=config.RL_PAUSED,
                batch_status="GATED" if not rollback else "GATED")
            return {"paused": detail["failures"], "completed": False}

        if not active and rollback:
            self._complete_rollback_batch_unlocked(release_id, now)
            return {"paused": [], "completed": False}

        if rollback:
            batch = self._row(
                "SELECT * FROM release_batches WHERE release_id=? AND batch_no=?",
                (release_id, release["current_batch"]))
            if batch is not None and now >= batch["deadline_at"]:
                detail = {"reason": "ROLLBACK_DEADLINE_EXCEEDED",
                          "batch_no": release["current_batch"],
                          "active_devices": [r["device_id"] for r in active]}
                self._pause_release_unlocked(
                    release_id, "ROLLBACK_DEADLINE_EXCEEDED", detail, now,
                    status=config.RL_PAUSED, batch_status="GATED")
                return {"paused": [detail], "completed": False}
            # 回滚不允许带着未收敛设备结束；等待 ACK/报告或失败门禁。
            self._update_release_unlocked(release_id,
                                          gate_reason="ROLLBACK_IN_PROGRESS")
            return {"paused": [], "completed": False}

        current = release["current_batch"]
        current_items = [r for r in items if r["batch_no"] == current]
        if not active and not rollback:
            if all(r["status"] in (config.RD_MATCHED, config.RD_SKIPPED)
                   for r in current_items) and \
                    any(r["status"] == config.RD_MATCHED
                        for r in current_items):
                self._set_batch_unlocked(
                    release_id, current, "COMPLETED", now,
                    None, "GATE_PASSED")
                if current < self._batch_count(release_id):
                    self._start_batch_unlocked(
                        release_id, current + 1, now)
                    return {"paused": [], "completed": False}
            self._finish_release_unlocked(
                release_id, config.RL_COMPLETED, now, False)
            return {"paused": [], "completed": True}

        batch = self._row(
            "SELECT * FROM release_batches WHERE release_id=? AND batch_no=?",
            (release_id, current))
        batch_items = [r for r in items if r["batch_no"] == current]
        matched = [r for r in batch_items
                   if r["status"] == config.RD_MATCHED]
        eligible = [r for r in batch_items
                    if r["status"] not in (config.RD_SKIPPED,
                                           config.RD_WAITING)]
        matched_ratio = 100.0 * len(matched) / max(1, len(eligible))
        threshold_met = len(eligible) > 0 and matched_ratio + 1e-9 >= \
            release["confirm_threshold"]

        if threshold_met and any(r["status"] == config.RD_ACTIVE
                                 for r in batch_items):
            # 已达到确认率但尾部设备仍在途：保留锁和命令，等待其自然收敛。
            self._update_release_unlocked(
                release_id, gate_reason="WAITING_TAIL_AFTER_GATE")
            return {"paused": [], "completed": False}

        # 管理员跳过后 GATED 批次重新满足门禁时，也要继续推进。
        if threshold_met:
            next_batch = current + 1
            self._set_batch_unlocked(release_id, current, "COMPLETED", now,
                                     batch["deadline_at"], "GATE_PASSED")
            if next_batch <= self._batch_count(release_id):
                self._start_batch_unlocked(release_id, next_batch, now)
            else:
                self._update_release_unlocked(
                    release_id, gate_reason="WAITING_TAIL_AFTER_GATE")
        elif batch["deadline_at"] is not None and now >= batch["deadline_at"]:
            detail = {"reason": "BATCH_DEADLINE_EXCEEDED",
                      "batch_no": current,
                      "matched": len(matched),
                      "eligible": len(eligible),
                      "confirm_rate": matched_ratio,
                      "required": release["confirm_threshold"]}
            self._pause_release_unlocked(
                release_id, "BATCH_DEADLINE_EXCEEDED", detail, now,
                status=config.RL_PAUSED, batch_status="GATED")
            return {"paused": [detail], "completed": False}
        else:
            self._update_release_unlocked(
                release_id,
                gate_reason="WAITING_CONFIRMATIONS"
                if batch["status"] == "ACTIVE" else batch["gate_reason"])
        return {"paused": [], "completed": False}

    def _start_batch_unlocked(self, release_id: str, batch_no: int,
                              now: float) -> None:
        release = self._release(release_id)
        members = self._batch_devices(release_id, batch_no)
        deadline = now + release["batch_deadline_seconds"]
        self._conn.execute(
            "UPDATE releases SET current_batch=?, gate_reason=?,"
            " pause_reason='', paused_at=NULL WHERE id=?",
            (batch_no, "BATCH_ACTIVE", release_id))
        self._set_batch_unlocked(release_id, batch_no, "ACTIVE", now,
                                 deadline, "BATCH_ACTIVE")
        target = json.loads(release["target_state"])
        for device_id in members:
            enq = self._enqueue_desired_unlocked(
                device_id, target, now, release_id=release_id,
                phase="FORWARD")
            self._set_device_unlocked(
                release_id, device_id, config.RD_ACTIVE,
                command_column="forward_command_id", command_id=enq["id"],
                error="", at=now, extra={"version": enq["version"],
                                         "batch_no": batch_no})
            self.event(device_id, "RELEASE_DEVICE_BATCH_STARTED",
                       {"release_id": release_id, "batch_no": batch_no,
                        "command_id": enq["id"], "version": enq["version"],
                        "deadline_at": deadline},
                       command_id=enq["id"], at=now, release_id=release_id)
        self._release_event_unlocked(
            release_id, "RELEASE_BATCH_STARTED",
            {"batch_no": batch_no, "device_count": len(members),
             "deadline_at": deadline}, now)

    # ---------------- 管理操作（全部幂等） ----------------
    def pause_release(self, release_id: str,
                      reason: str = "ADMIN_PAUSED",
                      idem_key: Optional[str] = None) -> dict:
        return self._idempotent_release_op(
            release_id, "PAUSE", idem_key, reason,
            {config.RL_ACTIVE, config.RL_ROLLING_BACK, config.RL_PENDING},
            self._pause_admin)

    def continue_release(self, release_id: str,
                         idem_key: Optional[str] = None) -> dict:
        return self._idempotent_release_op(
            release_id, "CONTINUE", idem_key, None,
            {config.RL_PAUSED}, self._continue_release)

    def skip_failed_devices(self, release_id: str,
                            idem_key: Optional[str] = None) -> dict:
        return self._idempotent_release_op(
            release_id, "SKIP_FAILED", idem_key, None,
            {config.RL_PAUSED}, self._skip_failed)

    def rollback_release(self, release_id: str,
                         idem_key: Optional[str] = None) -> dict:
        return self._idempotent_release_op(
            release_id, "ROLLBACK", idem_key, None,
            {config.RL_PAUSED, config.RL_ACTIVE}, self._start_rollback)

    def _idempotent_release_op(self, release_id: str, operation: str,
                               idem_key: Optional[str], reason: Optional[str],
                               allowed: set[str], fn) -> dict:
        with self._wlock:
            now = self.clock()
            if self._release(release_id) is None:
                raise KeyError(f"release not found: {release_id}")
            if idem_key:
                hit = self._row(
                    "SELECT response FROM idempotent_requests WHERE idem_key=?",
                    (idem_key,))
                if hit:
                    return {"replayed": True, **json.loads(hit["response"])}
            with self._conn:
                result = fn(release_id, now, reason)
            if idem_key:
                self._conn.execute(
                    "INSERT OR IGNORE INTO idempotent_requests"
                    "(idem_key,response,created_at) VALUES(?,?,?)",
                    (idem_key, _json(result), now))
            return result

    def _pause_admin(self, release_id: str, now: float,
                     reason: Optional[str]) -> dict:
        release = self._release(release_id)
        if release["status"] == config.RL_PAUSED:
            return {"status": config.RL_PAUSED, "release_id": release_id,
                    "idempotent": True, "reason": release["pause_reason"]}
        if release["status"] not in (config.RL_ACTIVE,
                                     config.RL_ROLLING_BACK,
                                     config.RL_PENDING):
            return {"status": release["status"], "release_id": release_id,
                    "idempotent": True}
        reason = reason or "ADMIN_PAUSED"
        detail = {"reason": reason}
        self._pause_release_unlocked(
            release_id, reason, detail, now,
            status=config.RL_PAUSED,
            batch_status="GATED" if release["current_batch"] else "WAITING")
        return {"status": config.RL_PAUSED, "release_id": release_id,
                "reason": reason}

    def _continue_release(self, release_id: str, now: float,
                          _reason=None) -> dict:
        release = self._release(release_id)
        if release["status"] != config.RL_PAUSED:
            return {"status": release["status"], "release_id": release_id,
                    "idempotent": True, "regenerated_devices": []}
        rollback = release["phase"] == "ROLLBACK"
        status = config.RL_ROLLING_BACK if rollback else config.RL_ACTIVE
        item_status = (config.RD_ROLLBACK_ACTIVE if rollback
                       else config.RD_ACTIVE)
        # 已处于目标状态的重复请求（服务重启或客户端重试）直接重放，不生成版本。
        # PAUSED 才会走到这里，因此恢复时需要新的命令版本。
        rows = self._all(
            "SELECT * FROM release_devices WHERE release_id=? AND status=?",
            (release_id, config.RD_ACTIVE if not rollback
             else config.RD_ROLLBACK_ACTIVE))
        # 设备拒绝/派发失败/过期/漂移一定为失败设备生成新版本。批次超时只是
        # 时间门禁，仍在正常 ACTIVE 的设备继续使用原命令，避免重复下发。
        if rollback:
            target_statuses = (config.RD_ROLLBACK_FAILED,)
        else:
            target_statuses = (config.RD_REJECTED, config.RD_FAILED,
                               config.RD_EXPIRED, config.RD_DRIFT)
        pending = self._all(
            "SELECT * FROM release_devices WHERE release_id=?"
            f" AND status IN ({','.join('?' for _ in target_statuses)})"
            " AND batch_no=?",
            (release_id, *target_statuses, release["current_batch"]))
        regenerated: list[str] = []
        deadline = now + release["batch_deadline_seconds"]
        for item in pending:
            state = (json.loads(item["baseline_state"]) if rollback
                     else json.loads(release["target_state"]))
            enq = self._enqueue_desired_unlocked(
                item["device_id"], state, now, release_id=release_id,
                phase="ROLLBACK" if rollback else "FORWARD")
            self._set_device_unlocked(
                release_id, item["device_id"], item_status,
                command_column=("rollback_command_id" if rollback
                                else "forward_command_id"),
                command_id=enq["id"], error="", at=now,
                extra={"version": enq["version"],
                       "batch_no": item["batch_no"], "retried": True,
                       "phase": "ROLLBACK" if rollback else "FORWARD"})
            regenerated.append(item["device_id"])
            self.event(item["device_id"],
                       "RELEASE_DEVICE_COMMAND_VERSION_CREATED",
                       {"release_id": release_id,
                        "command_id": enq["id"], "version": enq["version"],
                        "phase": "ROLLBACK" if rollback else "FORWARD",
                        "operation": "CONTINUE"},
                       command_id=enq["id"], at=now, release_id=release_id)
        self._conn.execute(
            "UPDATE releases SET status=?, paused_at=NULL, pause_reason='',"
            " gate_reason=? WHERE id=?",
            (status, "ROLLBACK_IN_PROGRESS" if rollback else "BATCH_ACTIVE",
             release_id))
        current = release["current_batch"] or 1
        batch = self._row(
            "SELECT * FROM release_batches WHERE release_id=? AND batch_no=?",
            (release_id, current))
        if batch is not None:
            self._set_batch_unlocked(
                release_id, current,
                "ACTIVE" if batch["status"] != "COMPLETED"
                else batch["status"], now, deadline if batch["status"] != "COMPLETED"
                else batch["deadline_at"],
                "BATCH_ACTIVE" if batch["status"] != "COMPLETED"
                else batch["gate_reason"])
        self._record_op_unlocked(release_id, "CONTINUE", None, "COMPLETED",
                                 {"regenerated": regenerated}, now)
        self._release_event_unlocked(
            release_id, "RELEASE_CONTINUED",
            {"regenerated_devices": regenerated,
             "phase": release["phase"]}, now)
        return {"status": status, "release_id": release_id,
                "regenerated_devices": regenerated}

    def _skip_failed(self, release_id: str, now: float,
                     _reason=None) -> dict:
        release = self._release(release_id)
        if release["status"] == config.RL_ACTIVE:
            # 第一次 skip 成功后通常已推进到 ACTIVE；重试请求是无副作用 no-op。
            return {"status": config.RL_ACTIVE, "release_id": release_id,
                    "skipped_devices": [], "idempotent": True}
        if release["status"] != config.RL_PAUSED:
            return {"status": release["status"], "release_id": release_id,
                    "idempotent": True, "skipped_devices": []}
        rows = self._all(
            "SELECT * FROM release_devices WHERE release_id=? AND status IN"
            " (?,?,?,?)",
            (release_id, config.RD_REJECTED, config.RD_FAILED,
             config.RD_EXPIRED, config.RD_DRIFT))
        skipped: list[str] = []
        for r in rows:
            self._set_device_unlocked(
                release_id, r["device_id"], config.RD_SKIPPED,
                command_column="forward_command_id",
                command_id=r["forward_command_id"],
                error=r["last_error"], at=now,
                extra={"batch_no": r["batch_no"], "previous_status": r["status"]})
            skipped.append(r["device_id"])
            self.event(r["device_id"], "RELEASE_DEVICE_SKIPPED",
                       {"release_id": release_id,
                        "batch_no": r["batch_no"],
                        "previous_status": r["status"],
                        "command_id": r["forward_command_id"]},
                       command_id=r["forward_command_id"], at=now,
                       release_id=release_id)
        self._record_op_unlocked(release_id, "SKIP_FAILED", None,
                                 "COMPLETED" if skipped else "NOOP",
                                 {"skipped": skipped}, now)
        if skipped:
            self._release_event_unlocked(
                release_id, "RELEASE_FAILED_DEVICES_SKIPPED",
                {"devices": skipped}, now)
        # 立即重新评估：失败项被跳过后，批次可能已过门禁。
        self._conn.execute(
            "UPDATE releases SET status=?, paused_at=NULL, pause_reason='',"
            " gate_reason=? WHERE id=?",
            (config.RL_ACTIVE, "BATCH_ACTIVE", release_id))
        outcome = self._evaluate_release_unlocked(release_id, now)
        return {"status": self._release(release_id)["status"],
                "release_id": release_id, "skipped_devices": skipped,
                "idempotent": not skipped,
                "outcome": {"paused": outcome["paused"],
                            "completed": outcome["completed"]}}

    def _start_rollback(self, release_id: str, now: float,
                        _reason=None) -> dict:
        release = self._release(release_id)
        if release["status"] == config.RL_ROLLING_BACK:
            return {"status": config.RL_ROLLING_BACK, "release_id": release_id,
                    "idempotent": True}
        if release["status"] == config.RL_ROLLED_BACK:
            return {"status": config.RL_ROLLED_BACK, "release_id": release_id,
                    "idempotent": True}
        started = [dict(r) for r in self._all(
            "SELECT * FROM release_devices WHERE release_id=?"
            " AND status != ?", (release_id, config.RD_WAITING))]
        rollback_items = [r for r in started
                          if r["status"] != config.RD_SKIPPED]
        if not rollback_items:
            self._finish_release_unlocked(release_id,
                                          config.RL_ROLLED_BACK, now, True)
            return {"status": config.RL_ROLLED_BACK, "release_id": release_id,
                    "rolled_back_devices": []}
        highest_batch = max(r["batch_no"] for r in rollback_items)
        current_batch = self._batch_devices(
            release_id, release["current_batch"], include_skipped=False)
        first_batch_ids = set(current_batch) if release["status"] == \
            config.RL_ACTIVE else set(
                self._batch_devices(release_id, highest_batch,
                                    include_skipped=False))
        deadline = now + release["batch_deadline_seconds"]
        rollback_batch = release["current_batch"] \
            if release["status"] == config.RL_ACTIVE else highest_batch
        self._conn.execute(
            "UPDATE releases SET status=?, phase='ROLLBACK', current_batch=?,"
            " paused_at=NULL, pause_reason='', gate_reason=? WHERE id=?",
            (config.RL_ROLLING_BACK, rollback_batch,
             "ROLLBACK_IN_PROGRESS", release_id))
        self._set_batch_unlocked(release_id, rollback_batch, "ACTIVE", now,
                                 deadline, "ROLLBACK_IN_PROGRESS")
        rolled: list[str] = []
        for device_id in sorted(first_batch_ids):
            item = self._release_device(release_id, device_id)
            enq = self._enqueue_desired_unlocked(
                device_id, json.loads(item["baseline_state"]), now,
                release_id=release_id, phase="ROLLBACK")
            self._set_device_unlocked(
                release_id, device_id, config.RD_ROLLBACK_ACTIVE,
                command_column="rollback_command_id", command_id=enq["id"],
                error="", at=now,
                extra={"version": enq["version"],
                       "batch_no": rollback_batch,
                       "previous_status": item["status"]})
            rolled.append(device_id)
            self.event(device_id, "RELEASE_DEVICE_ROLLBACK_COMMAND_CREATED",
                       {"release_id": release_id,
                        "batch_no": rollback_batch, "command_id": enq["id"],
                        "version": enq["version"],
                        "baseline_version": item["baseline_version"]},
                       command_id=enq["id"], at=now, release_id=release_id)
        self._record_op_unlocked(release_id, "ROLLBACK", None, "STARTED",
                                 {"devices": rolled,
                                  "batch_no": rollback_batch}, now)
        self._release_event_unlocked(
            release_id, "RELEASE_ROLLBACK_STARTED",
            {"devices": rolled, "batch_no": rollback_batch,
             "deadline_at": deadline}, now)
        return {"status": config.RL_ROLLING_BACK, "release_id": release_id,
                "batch_no": rollback_batch, "rolled_back_devices": rolled}
    # ---------------- 内部辅助 ----------------
    def _release(self, release_id: str):
        return self._row("SELECT * FROM releases WHERE id=?", (release_id,))

    def _release_device(self, release_id: str, device_id: str):
        return self._row(
            "SELECT * FROM release_devices WHERE release_id=? AND device_id=?",
            (release_id, device_id))

    def _batch_count(self, release_id: str) -> int:
        return self._row(
            "SELECT COUNT(*) AS n FROM release_batches WHERE release_id=?",
            (release_id,))["n"]

    def _batch_devices(self, release_id: str, batch_no: int,
                       include_skipped: bool = True) -> list[str]:
        sql = ("SELECT device_id FROM release_devices WHERE release_id=?"
               " AND batch_no=?")
        if not include_skipped:
            sql += " AND status != ?"
            rows = self._all(
                sql + " ORDER BY ordinal",
                (release_id, batch_no, config.RD_SKIPPED))
        else:
            rows = self._all(sql + " ORDER BY ordinal",
                             (release_id, batch_no))
        return [r["device_id"] for r in rows]

    def _set_batch_unlocked(self, release_id: str, batch_no: int,
                            status: str, started_at: Optional[float],
                            deadline_at: Optional[float],
                            reason: str) -> None:
        if status == "ACTIVE":
            self._conn.execute(
                "UPDATE release_batches SET status=?, started_at=?,"
                " deadline_at=?, gate_reason=? WHERE release_id=? AND batch_no=?",
                (status, started_at, deadline_at, reason,
                 release_id, batch_no))
        else:
            self._conn.execute(
                "UPDATE release_batches SET status=?, finished_at=?,"
                " gate_reason=? WHERE release_id=? AND batch_no=?",
                (status, started_at, reason, release_id, batch_no))

    def _update_release_unlocked(self, release_id: str, **fields) -> None:
        if not fields:
            return
        assignments = ",".join(f"{k}=?" for k in fields)
        self._conn.execute(
            f"UPDATE releases SET {assignments} WHERE id=?",
            (*fields.values(), release_id))

    def _set_device_unlocked(self, release_id: str, device_id: str,
                             status: str, command_column: str,
                             command_id: Optional[str], error: str,
                             at: float, extra: dict) -> None:
        if command_column not in ("forward_command_id",
                                  "rollback_command_id"):
            raise ValueError("invalid command column")
        self._conn.execute(
            f"UPDATE release_devices SET status=?,{command_column}=?,"
            " last_error=?,updated_at=? WHERE release_id=? AND device_id=?",
            (status, command_id, error, at, release_id, device_id))

    def _release_event_unlocked(self, release_id: str, event_type: str,
                                detail: dict, now: float) -> None:
        self.event(None, event_type, {"release_id": release_id, **detail},
                   at=now, release_id=release_id)

    def _pause_release_unlocked(self, release_id: str, reason: str,
                                detail: dict, now: float, *,
                                status: str = config.RL_PAUSED,
                                batch_status: str = "GATED") -> None:
        release = self._release(release_id)
        self._update_release_unlocked(
            release_id, status=status, paused_at=now, pause_reason=reason,
            gate_reason=reason)
        if release["current_batch"]:
            self._set_batch_unlocked(release_id, release["current_batch"],
                                     batch_status, now, None, reason)
        self._record_op_unlocked(release_id, "AUTO_PAUSE", None, status,
                                 detail, now)
        self._release_event_unlocked(release_id, "RELEASE_PAUSED", detail, now)

    def _finish_release_unlocked(self, release_id: str, status: str,
                                 now: float, rollback: bool) -> None:
        release = self._release(release_id)
        rollback = rollback or release["phase"] == "ROLLBACK"
        status = config.RL_ROLLED_BACK if rollback else config.RL_COMPLETED
        self._update_release_unlocked(
            release_id, status=status, completed_at=now, paused_at=None,
            pause_reason="",
            gate_reason="ROLLED_BACK" if rollback else "COMPLETED")
        current = self._release(release_id)["current_batch"]
        if current:
            self._set_batch_unlocked(
                release_id, current,
                "COMPLETED" if status == config.RL_COMPLETED else "COMPLETED",
                now, None, "ROLLED_BACK" if rollback else "COMPLETED")
        self._release_event_unlocked(
            release_id,
            "RELEASE_ROLLED_BACK" if rollback else "RELEASE_COMPLETED",
            {"status": status}, now)

    def _complete_rollback_batch_unlocked(self, release_id: str,
                                          now: float) -> None:
        release = self._release(release_id)
        current = release["current_batch"]
        self._set_batch_unlocked(release_id, current, "COMPLETED", now, None,
                                 "ROLLED_BACK")
        lower = self._all(
            "SELECT batch_no, COUNT(*) AS n FROM release_devices"
            " WHERE release_id=? AND batch_no < ? AND status NOT IN (?,?,?)"
            " GROUP BY batch_no ORDER BY batch_no DESC LIMIT 1",
            (release_id, current, config.RD_WAITING, config.RD_SKIPPED,
             config.RD_MATCHED))
        if not lower:
            # 兜底：所有非跳过设备都已 ROLLED_BACK。
            for r in self._all(
                    "SELECT DISTINCT batch_no FROM release_devices"
                    " WHERE release_id=?", (release_id,)):
                self._set_batch_unlocked(release_id, r["batch_no"],
                                         "COMPLETED", now, None,
                                         "ROLLED_BACK")
            self._finish_release_unlocked(release_id,
                                          config.RL_ROLLED_BACK, now, True)
            return
        next_batch = lower[0]["batch_no"]
        deadline = now + release["batch_deadline_seconds"]
        self._update_release_unlocked(release_id, current_batch=next_batch,
                                      gate_reason="ROLLBACK_IN_PROGRESS")
        self._set_batch_unlocked(release_id, next_batch, "ACTIVE", now,
                                 deadline, "ROLLBACK_IN_PROGRESS")
        members = [r for r in self._all(
            "SELECT * FROM release_devices WHERE release_id=? AND batch_no=?"
            " AND status NOT IN (?,?) ORDER BY ordinal",
            (release_id, next_batch, config.RD_WAITING,
             config.RD_SKIPPED))]
        devices: list[str] = []
        for item in members:
            enq = self._enqueue_desired_unlocked(
                item["device_id"], json.loads(item["baseline_state"]), now,
                release_id=release_id, phase="ROLLBACK")
            self._set_device_unlocked(
                release_id, item["device_id"], config.RD_ROLLBACK_ACTIVE,
                "rollback_command_id", enq["id"], "", now,
                {"version": enq["version"], "batch_no": next_batch,
                 "previous_status": item["status"]})
            devices.append(item["device_id"])
            self.event(item["device_id"],
                       "RELEASE_DEVICE_ROLLBACK_COMMAND_CREATED",
                       {"release_id": release_id, "batch_no": next_batch,
                        "command_id": enq["id"], "version": enq["version"]},
                       command_id=enq["id"], at=now, release_id=release_id)
        self._release_event_unlocked(
            release_id, "RELEASE_ROLLBACK_BATCH_STARTED",
            {"batch_no": next_batch, "devices": devices,
             "deadline_at": deadline}, now)

    # ---------------- 查询读模型 ----------------
    def list_releases(self, status: Optional[str] = None) -> list[dict]:
        if status:
            rows = self._all(
                "SELECT r.*, g.name AS group_name, g.priority FROM releases r"
                " JOIN device_groups g ON g.id=r.group_id"
                " WHERE r.status=? ORDER BY g.priority,r.created_at",
                (status,))
        else:
            rows = self._all(
                "SELECT r.*, g.name AS group_name, g.priority FROM releases r"
                " JOIN device_groups g ON g.id=r.group_id"
                " ORDER BY g.priority,r.created_at")
        return [self._release_view(dict(r)) for r in rows]

    def release_read_model(self, release_id: str) -> Optional[dict]:
        row = self._row(
            "SELECT r.*, g.name AS group_name, g.priority FROM releases r"
            " JOIN device_groups g ON g.id=r.group_id WHERE r.id=?",
            (release_id,))
        if row is None:
            return None
        return self._release_view(dict(row))

    def _release_view(self, r: dict) -> dict:
        release_id = r["id"]
        rows = self._all(
            "SELECT batch_no,status,COUNT(*) AS n FROM release_devices"
            " WHERE release_id=? GROUP BY batch_no,status ORDER BY batch_no",
            (release_id,))
        status_by_batch: dict[int, dict[str, int]] = {}
        for x in rows:
            status_by_batch.setdefault(x["batch_no"], {})[x["status"]] = x["n"]
        counts = {s: 0 for s in (
            config.RD_WAITING, config.RD_ACTIVE, config.RD_MATCHED,
            config.RD_SKIPPED, config.RD_REJECTED, config.RD_FAILED,
            config.RD_EXPIRED, config.RD_DRIFT, config.RD_ROLLBACK_ACTIVE,
            config.RD_ROLLED_BACK, config.RD_ROLLBACK_FAILED)}
        for batch in status_by_batch.values():
            for k, v in batch.items():
                counts[k] = counts.get(k, 0) + v
        unresolved = counts[config.RD_ACTIVE] + counts[config.RD_ROLLBACK_ACTIVE]
        gate = None
        if r["current_batch"]:
            b = status_by_batch.get(r["current_batch"], {})
            eligible = sum(v for k, v in b.items()
                           if k not in (config.RD_SKIPPED,
                                        config.RD_WAITING))
            matched = b.get(config.RD_MATCHED, 0) + b.get(
                config.RD_ROLLED_BACK, 0)
            gate = {"batch_no": r["current_batch"],
                    "reason": r["gate_reason"],
                    "pause_reason": r["pause_reason"],
                    "matched": matched,
                    "active": b.get(config.RD_ACTIVE, 0)
                    + b.get(config.RD_ROLLBACK_ACTIVE, 0),
                    "failed": sum(b.get(k, 0) for k in (
                        config.RD_REJECTED, config.RD_FAILED,
                        config.RD_EXPIRED, config.RD_DRIFT,
                        config.RD_ROLLBACK_FAILED)),
                    "skipped": b.get(config.RD_SKIPPED, 0),
                    "confirm_rate": (100.0 * matched / eligible
                                     if eligible else None),
                    "required_confirm_rate": r["confirm_threshold"]}
        return {**r, "target_state": model.parse_json(r["target_state"]),
                "counts": counts, "unresolved_devices": unresolved,
                "current_gate": gate,
                "conflicts": self.release_conflicts(release_id),
                "batches": self.list_release_batches(release_id)}

    def list_release_batches(self, release_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM release_batches WHERE release_id=?"
            " ORDER BY batch_no", (release_id,))
        out = []
        for r in rows:
            d = dict(r)
            stats = {x["status"]: x["n"] for x in self._all(
                "SELECT status,COUNT(*) AS n FROM release_devices"
                " WHERE release_id=? AND batch_no=? GROUP BY status",
                (release_id, r["batch_no"]))}
            d["status_counts"] = stats
            out.append(d)
        return out

    def list_release_devices(self, release_id: str,
                             batch_no: Optional[int] = None,
                             status: Optional[str] = None) -> list[dict]:
        sql = ("SELECT rd.*, c.version AS command_version,"
                " c.status AS command_status, c.ack_code, c.ack_message,"
                " c.last_attempt_at, c.expires_at AS command_expires_at,"
                " rc.version AS rollback_version,"
                " rc.status AS rollback_command_status"
                " FROM release_devices rd"
                " LEFT JOIN commands c ON c.id=rd.forward_command_id"
                " LEFT JOIN commands rc ON rc.id=rd.rollback_command_id"
                " WHERE rd.release_id=?")
        args: list[Any] = [release_id]
        if batch_no is not None:
            sql += " AND rd.batch_no=?"
            args.append(batch_no)
        if status:
            sql += " AND rd.status=?"
            args.append(status)
        sql += " ORDER BY rd.batch_no,rd.ordinal"
        return [dict(r) for r in self._all(sql, args)]

    def release_conflicts(self, release_id: str) -> list[dict]:
        release = self._release(release_id)
        if release is None:
            return []
        mine = {r["device_id"] for r in self._all(
            "SELECT device_id FROM release_devices WHERE release_id=?",
            (release_id,))}
        held = self._release_locks((
            config.RL_ACTIVE, config.RL_PAUSED, config.RL_ROLLING_BACK))
        blockers: list[dict] = []
        for device_id in sorted(mine):
            h = held.get(device_id)
            if h and h["id"] != release_id:
                blockers.append({"device_id": device_id,
                                 "blocked_by_release_id": h["id"],
                                 "blocked_by_group_id": h["group_id"],
                                 "blocked_by_status": h["status"],
                                 "reason": "GROUP_PRIORITY_AND_CREATION_ORDER"})
        # 自己作为阻塞方时也展示反向关系。
        blocking: list[dict] = []
        if release["status"] in (config.RL_ACTIVE, config.RL_PAUSED,
                                 config.RL_ROLLING_BACK):
            unresolved = self._unresolved_devices_unlocked(release_id)
            others = self._all(
                "SELECT r.id, r.group_id, r.status, rd.device_id"
                " FROM releases r JOIN release_devices rd"
                " ON rd.release_id=r.id WHERE r.id != ? AND r.status=?"
                " ORDER BY r.created_at", (release_id, config.RL_PENDING))
            for r in others:
                if r["device_id"] in unresolved:
                    blocking.append({"release_id": r["id"],
                                     "group_id": r["group_id"],
                                     "device_id": r["device_id"]})
        result = blockers
        if blocking:
            result.append({"blocking": blocking})
        return result

    def list_release_events(self, release_id: str, limit: int = 500,
                            offset: int = 0) -> list[dict]:
        command_ids = [r["id"] for r in self._all(
            "SELECT forward_command_id AS id FROM release_devices"
            " WHERE release_id=? AND forward_command_id IS NOT NULL"
            " UNION SELECT rollback_command_id AS id FROM release_devices"
            " WHERE release_id=? AND rollback_command_id IS NOT NULL",
            (release_id, release_id))]
        where = "(events.release_id=? OR events.command_id IN ("
        args: list[Any] = [release_id]
        if command_ids:
            where += ",".join("?" for _ in command_ids)
            args.extend(command_ids)
        where += "))" if command_ids else "))"
        rows = self._all(
            f"SELECT events.id,at,device_id,command_id,release_id,event_type,detail"
            f" FROM events WHERE {where} ORDER BY events.id ASC"
            " LIMIT ? OFFSET ?",
            (*args, limit, offset))
        return [dict(r) for r in rows]
# __APPEND_HELPERS__