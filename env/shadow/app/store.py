"""SQLite 存储层：全部状态机规则都在事务内完成。

写事务由进程内一把锁串行化（core 是唯一直接写库的进程，ingress/dispatcher
都通过 core 的内部 HTTP API 间接触发，因此无跨进程写竞争）。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from typing import Any, Optional

from . import config, model
from .release_store import ReleaseStoreMixin
from .schema import SCHEMA


def _now() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


class Store(ReleaseStoreMixin):
    def __init__(self, db_path: str = config.DB_PATH, clock=_now):
        self.db_path = db_path
        self.clock = clock
        self._wlock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.execute("PRAGMA busy_timeout=5000;")
        self._conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        """为旧版 SQLite 数据库补齐后加字段（新建库无需 ALTER）。"""
        columns = {
            r["name"] for r in
            self._conn.execute("PRAGMA table_info(commands)")}
        if "release_id" not in columns:
            self._conn.execute(
                "ALTER TABLE commands ADD COLUMN release_id TEXT")
        if "release_phase" not in columns:
            self._conn.execute(
                "ALTER TABLE commands ADD COLUMN release_phase TEXT"
                " NOT NULL DEFAULT ''")
        event_columns = {
            r["name"] for r in
            self._conn.execute("PRAGMA table_info(events)")}
        if "release_id" not in event_columns:
            self._conn.execute("ALTER TABLE events ADD COLUMN release_id TEXT")

    def close(self) -> None:
        self._conn.close()

    # ---------------- 通用 ----------------
    def event(self, device_id: Optional[str], event_type: str,
              detail: Optional[dict] = None, command_id: Optional[str] = None,
              at: Optional[float] = None,
              release_id: Optional[str] = None) -> None:
        self._conn.execute(
            "INSERT INTO events(at, device_id, command_id, release_id,"
            " event_type, detail) VALUES(?,?,?,?,?,?)",
            (at or self.clock(), device_id, command_id, release_id,
             event_type, _json(detail or {})))

    def _row(self, sql: str, args=()):
        return self._conn.execute(sql, args).fetchone()

    def _all(self, sql: str, args=()):
        return self._conn.execute(sql, args).fetchall()

    # ---------------- 设备 ----------------
    def provision_device(self, device_id: str, name: str,
                         token: Optional[str] = None) -> dict:
        with self._wlock:
            token = token or ("tok_" + uuid.uuid4().hex)
            now = self.clock()
            exists = self._row("SELECT id FROM devices WHERE id=?",
                               (device_id,))
            if exists:
                row = self._row(
                    "SELECT d.id, d.name, d.token, s.online, s.last_seen_at"
                    " FROM devices d JOIN shadows s ON s.device_id=d.id"
                    " WHERE d.id=?", (device_id,))
                return {"created": False, "id": row["id"], "name": row["name"],
                        "token": row["token"], "online": bool(row["online"])}
            cur = self._conn.execute(
                "INSERT INTO devices(id,name,token,registered_at)"
                " VALUES(?,?,?,?)", (device_id, name, token, now))
            cur.execute(
                "INSERT INTO shadows(device_id) VALUES(?)", (device_id,))
            self.event(device_id, "DEVICE_REGISTERED",
                       {"name": name, "token": token}, at=now)
            return {"created": True, "id": device_id, "name": name,
                    "token": token, "online": False}

    def get_device_by_id(self, device_id: str):
        return self._row(
            "SELECT d.*, s.online, s.last_seen_at FROM devices d"
            " JOIN shadows s ON s.device_id=d.id WHERE d.id=?",
            (device_id,))

    def get_device_by_token(self, token: str):
        return self._row(
            "SELECT d.*, s.online, s.last_seen_at FROM devices d"
            " JOIN shadows s ON s.device_id=d.id WHERE d.token=?",
            (token,))

    def touch(self, device_id: str) -> dict:
        """设备心跳（每次 long-poll 都会打到这里）。返回在线状态变化。"""
        with self._wlock:
            now = self.clock()
            row = self._row("SELECT online, last_seen_at FROM shadows"
                            " WHERE device_id=?", (device_id,))
            became_online = row is not None and row["online"] == 0
            self._conn.execute(
                "UPDATE shadows SET last_seen_at=?, online=1,"
                " prolonged_notified=0,"
                " online_changed_at=CASE WHEN online=0 THEN ? ELSE"
                " online_changed_at END WHERE device_id=?",
                (now, now, device_id))
            if became_online:
                self.event(device_id, "DEVICE_ONLINE",
                           {"last_seen_at": row["last_seen_at"]}, at=now)
            return {"online": True, "became_online": bool(became_online)}

    def sweep_offline(self) -> list[dict]:
        """心跳超时 -> 离线；长时间离线 -> 再发一次预警事件。"""
        now = self.clock()
        results: list[dict] = []
        with self._wlock:
            stale = self._all(
                "SELECT device_id, last_seen_at, online_changed_at"
                " FROM shadows WHERE online=1 AND"
                " (last_seen_at IS NULL OR last_seen_at < ?)",
                (now - config.OFFLINE_AFTER_SECONDS,))
            for r in stale:
                self._conn.execute(
                    "UPDATE shadows SET online=0, online_changed_at=?"
                    " WHERE device_id=?", (now, r["device_id"]))
                detail = {"reason": "HEARTBEAT_TIMEOUT",
                          "offline_after": config.OFFLINE_AFTER_SECONDS,
                          "last_seen_at": r["last_seen_at"]}
                self.event(r["device_id"], "DEVICE_OFFLINE", detail, at=now)
                results.append({"device_id": r["device_id"],
                                "event": "DEVICE_OFFLINE", **detail})

            prolonged = self._all(
                "SELECT device_id, last_seen_at FROM shadows"
                " WHERE online=0 AND prolonged_notified=0"
                " AND last_seen_at IS NOT NULL AND last_seen_at < ?",
                (now - config.PROLONGED_OFFLINE_SECONDS,))
            for r in prolonged:
                self._conn.execute(
                    "UPDATE shadows SET prolonged_notified=1"
                    " WHERE device_id=?", (r["device_id"],))
                detail = {"offline_for": now - r["last_seen_at"],
                          "threshold": config.PROLONGED_OFFLINE_SECONDS}
                self.event(r["device_id"], "DEVICE_OFFLINE_PROLONGED",
                           detail, at=now)
                results.append({"device_id": r["device_id"],
                                "event": "DEVICE_OFFLINE_PROLONGED", **detail})
        return results

    # ---------------- 期望态 ----------------
    def _enqueue_desired_unlocked(self, device_id: str, desired: dict,
                                  now: float, release_id: Optional[str] = None,
                                  phase: str = "") -> dict:
        """生成新的设备期望版本和逐设备命令；不改写任何历史命令。"""
        cur_ver = self._row(
            "SELECT desired_version FROM shadows WHERE device_id=?",
            (device_id,))["desired_version"]
        new_ver = cur_ver + 1
        cmd_id = new_id("cmd")
        expires_at = now + config.COMMAND_TTL_SECONDS
        self._conn.execute(
            "UPDATE shadows SET desired=?, desired_version=?,"
            " desired_updated_at=? WHERE device_id=?",
            (_json(desired), new_ver, now, device_id))
        self._conn.execute(
            "INSERT INTO commands(id,device_id,version,desired,status,"
            "created_at,expires_at,release_id,release_phase)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (cmd_id, device_id, new_ver, _json(desired),
             config.ST_QUEUED, now, expires_at, release_id, phase))
        old = self._all(
            "SELECT id, version FROM commands WHERE device_id=?"
            " AND version < ? AND status IN (?,?)",
            (device_id, new_ver, config.ST_QUEUED, config.ST_RETRYING))
        for r in old:
            self._conn.execute(
                "UPDATE commands SET status=? WHERE id=?",
                (config.ST_SUPERSEDED, r["id"]))
            self.event(device_id, "COMMAND_SUPERSEDED",
                       {"command_id": r["id"], "version": r["version"],
                        "superseded_by_version": new_ver,
                        "release_id": release_id},
                       command_id=r["id"], at=now, release_id=release_id)
        self.event(device_id, "DESIRED_UPDATED",
                   {"version": new_ver, "desired": desired,
                    "release_id": release_id, "phase": phase},
                   at=now, release_id=release_id)
        self.event(device_id, "COMMAND_ENQUEUED",
                   {"command_id": cmd_id, "version": new_ver,
                    "expires_at": expires_at, "release_id": release_id,
                    "phase": phase},
                   command_id=cmd_id, at=now, release_id=release_id)
        return {"id": cmd_id, "version": new_ver, "expires_at": expires_at}

    def set_desired(self, device_id: str, desired: dict,
                    expected_version: Optional[int] = None,
                    idem_key: Optional[str] = None) -> dict:
        """控制端设置期望态：版本 +1，为该版本生成一条命令。

        仍在 QUEUED/RETRYING（从未真正送达设备）的旧版本命令标记 SUPERSEDED；
        已经 SENT 的命令保留，避免设备已在执行却被服务端抹掉。
        """
        with self._wlock:
            now = self.clock()
            if idem_key:
                hit = self._row(
                    "SELECT response FROM idempotent_requests WHERE idem_key=?",
                    (idem_key,))
                if hit:
                    return {"replayed": True,
                            **json.loads(hit["response"])}

            sh = self.get_device_by_id(device_id)
            if sh is None:
                raise KeyError(f"device not found: {device_id}")
            cur_ver = self._row(
                "SELECT desired_version FROM shadows WHERE device_id=?",
                (device_id,))["desired_version"]
            if expected_version is not None and expected_version != cur_ver:
                return {"status": "CONFLICT",
                        "current_version": cur_ver,
                        "expected_version": expected_version}

            new_ver = cur_ver + 1
            with self._conn:  # 单一事务
                enq = self._enqueue_desired_unlocked(
                    device_id, desired, now)
            cmd_id = enq["id"]
            new_ver = enq["version"]
            expires_at = enq["expires_at"]

            online = bool(self._row(
                "SELECT online FROM shadows WHERE device_id=?",
                (device_id,))["online"])
            result = {"status": "OK", "version": new_ver,
                      "command_id": cmd_id, "online": online,
                      "expires_at": expires_at}
            if idem_key:
                self._conn.execute(
                    "INSERT OR IGNORE INTO idempotent_requests"
                    "(idem_key,response,created_at) VALUES(?,?,?)",
                    (idem_key, _json(result), now))
            return result

    # ---------------- 报告态 ----------------
    def report_state(self, device_id: str, version: int,
                     state: dict) -> dict:
        """设备上报报告态。

        报告态文档有独立的单调版本号（设备自管理），与期望/命令版本无关：
        乱序/重复保护为 version <= 当前 reported_version 一律拒绝
        （重复报文/旧报文不能覆盖新状态）。
        命令确认由设备显式 ACK（command_id 或命令版本）完成，不做隐式确认。
        """
        with self._wlock:
            now = self.clock()
            row = self._row(
                "SELECT reported_version, reported, reported_updated_at"
                " FROM shadows WHERE device_id=?", (device_id,))
            if row is None:
                raise KeyError(f"device not found: {device_id}")
            cur_ver = row["reported_version"]
            active_release = self.active_release_for_device(device_id)
            if version <= cur_ver:
                detail = {"received_version": version,
                          "current_version": cur_ver,
                          "reason": ("DUPLICATE_VERSION" if version == cur_ver
                                     else "STALE_VERSION")}
                self.event(device_id, "REPORT_REJECTED", detail, at=now)
                return {"accepted": False, "current_version": cur_ver,
                        "reason": detail["reason"]}

            with self._conn:
                self._conn.execute(
                    "UPDATE shadows SET reported=?, reported_version=?,"
                    " reported_updated_at=? WHERE device_id=?",
                    (_json(state), version, now, device_id))
                self.event(device_id, "REPORT_ACCEPTED",
                           {"version": version, "state": state}, at=now,
                           release_id=active_release["id"]
                           if active_release else None)
                self.report_release_progress(device_id, now)
            return {"accepted": True, "version": version}

    # ---------------- 命令派发 ----------------
    def _command_due(self, r, now: float) -> bool:
        if r["status"] == config.ST_QUEUED:
            return True
        if r["status"] == config.ST_RETRYING:
            wait = model.next_backoff_delay(r["attempts"])
            return (r["last_attempt_at"] or 0) + wait <= now
        return False

    def claim_next_command(self, device_id: str) -> Optional[dict]:
        """取该设备下一条可派发命令（严格按版本顺序），状态推进到 SENT。

        - 先清理：过期 -> EXPIRED；SENT 超过 ACK 超时 -> 回到 RETRYING
        - 候选：version 最小的 QUEUED / 已到退避时间的 RETRYING
        """
        with self._wlock:
            now = self.clock()
            # 过期清理（只看本设备）
            self._expire_unlocked(device_id, now)
            self._ack_timeout_unlocked(device_id, now)

            # 旧版本仍在途(SENT)时不能先派发新版本，避免旧命令超时重发乱序。
            # 旧版本处于 RETRYING 时，下面的 ORDER BY version 会先取到它；
            # FAILED/EXPIRED/SUPERSEDED/ACKED 都是终结态，不构成阻塞。
            # 严格版本顺序：只考虑最低版本的未终结命令；旧版本在途/等待退避时
            # 不跳过它去发新版本（否则旧命令稍后重发会造成乱序）。
            rows = self._all(
                "SELECT * FROM commands WHERE device_id=?"
                " AND status IN (?,?,?)"
                " ORDER BY version ASC",
                (device_id, config.ST_QUEUED, config.ST_RETRYING,
                 config.ST_SENT))
            dispatchable = []
            active_release = self.holding_release_for_device(device_id)
            for r in rows:
                if r["release_id"]:
                    item = self._row(
                        "SELECT status FROM release_devices"
                        " WHERE release_id=? AND device_id=?",
                        (r["release_id"], device_id))
                    if item is None:
                        continue
                    expected = (config.RD_ROLLBACK_ACTIVE
                                if r["release_phase"] == "ROLLBACK"
                                else config.RD_ACTIVE)
                    if item["status"] == expected:
                        dispatchable.append(r)
                elif active_release is None:
                    # 设备正被发布批次占有时，普通控制命令等待发布锁释放。
                    dispatchable.append(r)
            rows = dispatchable
            # 最低版本若已 SENT（在途），整体不可派发；若是 RETRYING 未到
            # 退避时间，也必须继续等待。
            cand = None
            if rows:
                first = rows[0]
                if self._command_due(first, now):
                    cand = first
            if cand is None:
                return None
            if now >= cand["expires_at"]:
                self._set_expired(cand, now)
                return None

            attempt_no = cand["attempts"] + 1
            with self._conn:
                self._conn.execute(
                    "INSERT INTO command_attempts(command_id,attempt_no,"
                    "started_at,outcome) VALUES(?,?,?,'PENDING')",
                    (cand["id"], attempt_no, now))
                self._conn.execute(
                    "UPDATE commands SET status=?, attempts=?,"
                    " claimed_at=COALESCE(claimed_at,?), delivered_at=?,"
                    " last_attempt_at=? WHERE id=?",
                    (config.ST_SENT, attempt_no, now, now, now, cand["id"]))
                self.event(device_id, "COMMAND_SENT",
                           {"command_id": cand["id"],
                            "version": cand["version"],
                            "attempt_no": attempt_no},
                           command_id=cand["id"], at=now)
            return {"id": cand["id"], "device_id": device_id,
                    "version": cand["version"],
                    "desired": model.parse_json(cand["desired"]),
                    "attempt_no": attempt_no,
                    "expires_at": cand["expires_at"]}

    def _ack_unlocked(self, command_id: str, code: str,
                      message: str, now: float) -> dict:
        """幂等确认：同一条命令重复 ACK 返回 duplicate=True，不产生副作用。"""
        cmd = self._row("SELECT * FROM commands WHERE id=?", (command_id,))
        if cmd is None:
            return {"status": "UNKNOWN_COMMAND"}
        if cmd["status"] == config.ST_ACKED:
            self.event(cmd["device_id"], "ACK_DUPLICATE",
                       {"command_id": command_id,
                        "version": cmd["version"], "code": code},
                       command_id=command_id, at=now)
            return {"status": "DUPLICATE_ACK", "command_id": command_id,
                    "version": cmd["version"], "duplicate": True,
                    "first_ack_code": cmd["ack_code"]}

        with self._conn:
            self._conn.execute(
                "UPDATE command_attempts SET finished_at=?, outcome='ACKED'"
                " WHERE command_id=? AND outcome='PENDING'",
                (now, command_id))
            self._conn.execute(
                "UPDATE commands SET status=?, acked_at=?, ack_code=?,"
                " ack_message=? WHERE id=?",
                (config.ST_ACKED, now, code, message, command_id))
            self.event(cmd["device_id"], "COMMAND_ACKED",
                       {"command_id": command_id, "version": cmd["version"],
                        "code": code, "message": message,
                        "duplicate": False,
                        "release_id": cmd["release_id"],
                        "phase": cmd["release_phase"]},
                       command_id=command_id, at=now,
                       release_id=cmd["release_id"])
            self._refresh_release_for_command_unlocked(cmd, now)
        return {"status": "ACKED", "command_id": command_id,
                "version": cmd["version"], "duplicate": False}

    def ack_by_device(self, device_id: str, version: Optional[int],
                      command_id: Optional[str], code: str,
                      message: str) -> dict:
        if command_id:
            cmd = self._row("SELECT * FROM commands WHERE id=? AND device_id=?",
                            (command_id, device_id))
        elif version is not None:
            cmd = self._row(
                "SELECT * FROM commands WHERE device_id=? AND version=?",
                (device_id, version))
        else:
            return {"status": "BAD_REQUEST",
                    "error": "command_id or version required"}
        if cmd is None:
            return {"status": "UNKNOWN_COMMAND"}
        with self._wlock:
            return self._ack_unlocked(cmd["id"], code, message, self.clock())

    def delivery_failed(self, command_id: str, error: str) -> dict:
        """ingress 上报一次派发失败：记录尝试，按重试预算决定 RETRYING/FAILED。"""
        with self._wlock:
            now = self.clock()
            cmd = self._row("SELECT * FROM commands WHERE id=?",
                            (command_id,))
            if cmd is None:
                return {"status": "UNKNOWN_COMMAND"}
            if cmd["status"] in model.TERMINAL_STATUSES:
                return {"status": cmd["status"], "command_id": command_id}

            attempts = cmd["attempts"]
            next_wait = model.next_backoff_delay(attempts)
            give_up = attempts >= config.MAX_DELIVERY_ATTEMPTS or \
                now >= cmd["expires_at"]
            with self._conn:
                self._conn.execute(
                    "UPDATE command_attempts SET finished_at=?, outcome='FAILED',"
                    " error=? WHERE command_id=? AND outcome='PENDING'",
                    (now, error, command_id))
                if give_up:
                    reason = ("EXPIRED" if now >= cmd["expires_at"]
                              else "MAX_ATTEMPTS")
                    new_status = (config.ST_EXPIRED
                                  if reason == "EXPIRED" else config.ST_FAILED)
                    self._conn.execute(
                        "UPDATE commands SET status=?, last_error=? WHERE id=?",
                        (new_status, error, command_id))
                    self.event(cmd["device_id"], "COMMAND_FAILED",
                              {"command_id": command_id,
                               "version": cmd["version"], "error": error,
                               "attempts": attempts, "reason": reason,
                               "release_id": cmd["release_id"],
                               "phase": cmd["release_phase"]},
                              command_id=command_id, at=now,
                              release_id=cmd["release_id"])
                    self._refresh_release_for_command_unlocked(cmd, now)
                    return {"status": new_status, "command_id": command_id,
                            "retry": False, "reason": reason}
                self._conn.execute(
                    "UPDATE commands SET status=?, last_error=?,"
                    " last_attempt_at=? WHERE id=?",
                    (config.ST_RETRYING, error, now, command_id))
                self.event(cmd["device_id"], "COMMAND_RETRY",
                          {"command_id": command_id,
                           "version": cmd["version"], "error": error,
                           "attempts": attempts,
                           "next_retry_in": next_wait},
                          command_id=command_id, at=now)
            return {"status": config.ST_RETRYING, "command_id": command_id,
                    "retry": True, "next_retry_in": next_wait,
                    "attempts": attempts}

    # ---------------- 定时维护（dispatcher 周期调用） ----------------
    def _set_expired(self, cmd, now: float) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE commands SET status=? WHERE id=?",
                (config.ST_EXPIRED, cmd["id"]))
            self._conn.execute(
                "UPDATE command_attempts SET finished_at=?, outcome='FAILED',"
                " error='EXPIRED' WHERE command_id=? AND outcome='PENDING'",
                (now, cmd["id"]))
            self.event(cmd["device_id"], "COMMAND_EXPIRED",
                       {"command_id": cmd["id"], "version": cmd["version"],
                        "expires_at": cmd["expires_at"],
                        "release_id": cmd["release_id"],
                        "phase": cmd["release_phase"]},
                       command_id=cmd["id"], at=now,
                       release_id=cmd["release_id"])
            self._refresh_release_for_command_unlocked(cmd, now)

    def _expire_unlocked(self, device_id: Optional[str], now: float) -> int:
        args: list[Any] = [now]
        where = "expires_at <= ? AND status NOT IN (?,?,?,?)"
        params: list[Any] = [now, config.ST_ACKED, config.ST_FAILED,
                             config.ST_EXPIRED, config.ST_SUPERSEDED]
        if device_id:
            where += " AND device_id=?"
            params.append(device_id)
        rows = self._all(
            f"SELECT * FROM commands WHERE {where}", params)
        for r in rows:
            self._set_expired(r, now)
        return len(rows)

    def _ack_timeout_unlocked(self, device_id: Optional[str],
                              now: float) -> int:
        """SENT 后迟迟收不到 ACK：关闭当前尝试，回到 RETRYING 等待重发。"""
        params: list[Any] = [now - config.ACK_TIMEOUT_SECONDS]
        where = ("status=? AND last_attempt_at IS NOT NULL"
                 " AND last_attempt_at < ?")
        params = [config.ST_SENT, now - config.ACK_TIMEOUT_SECONDS]
        if device_id:
            where += " AND device_id=?"
            params.append(device_id)
        rows = self._all(f"SELECT * FROM commands WHERE {where}", params)
        for r in rows:
            wait = model.next_backoff_delay(r["attempts"])
            with self._conn:
                self._conn.execute(
                    "UPDATE command_attempts SET finished_at=?,"
                    " outcome='FAILED', error='ACK_TIMEOUT' WHERE command_id=?"
                    " AND outcome='PENDING'", (now, r["id"]))
                self._conn.execute(
                    "UPDATE commands SET status=?, last_error='ACK_TIMEOUT'"
                    " WHERE id=?", (config.ST_RETRYING, r["id"]))
                self.event(r["device_id"], "COMMAND_ACK_TIMEOUT",
                           {"command_id": r["id"], "version": r["version"],
                            "attempts": r["attempts"],
                            "next_retry_in": wait},
                           command_id=r["id"], at=now)
                self.event(r["device_id"], "COMMAND_RETRY",
                           {"command_id": r["id"], "version": r["version"],
                            "error": "ACK_TIMEOUT",
                            "attempts": r["attempts"],
                            "next_retry_in": wait},
                           command_id=r["id"], at=now)
        return len(rows)

    def tick(self) -> dict:
        """全局定时维护：过期、ACK 超时。"""
        with self._wlock:
            now = self.clock()
            expired = self._expire_unlocked(None, now)
            timeouts = self._ack_timeout_unlocked(None, now)
            release_result = self._tick_releases_unlocked(now)
        return {"at": now, "expired": expired,
                "ack_timeouts": timeouts, "releases": release_result}

    # ---------------- 查询读模型 ----------------
    def shadow_read_model(self, device_id: str) -> Optional[dict]:
        sh = self._row(
            "SELECT * FROM shadows WHERE device_id=?", (device_id,))
        if sh is None:
            return None
        dev = self._row("SELECT name, token, registered_at FROM devices"
                        " WHERE id=?", (device_id,))
        now = self.clock()
        content_match = model.states_equal(sh["desired"], sh["reported"])
        ever_reported = sh["reported_updated_at"] is not None
        # 「未决命令」= 最高期望版本中尚未终结的命令；
        # 若最新版本命令已 ACKED，说明设备确认过但报告内容还没匹配
        # （ACKED_NOT_REPORTED）；FAILED/EXPIRED 保留作为差异原因。
        nxt = None
        if not (content_match and ever_reported):
            # 最新期望版本那条命令最能代表「设备追上当前目标」的进度
            latest = self._row(
                "SELECT * FROM commands WHERE device_id=? ORDER BY version"
                " DESC LIMIT 1", (device_id,))
            if latest is not None and latest["status"] not in (
                    config.ST_SUPERSEDED, config.ST_ACKED):
                nxt = latest
            elif latest is not None and latest["status"] == config.ST_ACKED:
                nxt = latest  # ACKED_NOT_REPORTED（内容尚未匹配）
        last_ack = self._row(
            "SELECT * FROM commands WHERE device_id=? AND status=?"
            " ORDER BY version DESC LIMIT 1",
            (device_id, config.ST_ACKED))

        open_status = nxt["status"] if nxt else None
        in_sync, reason = model.classify(
            desired_version=sh["desired_version"],
            reported_version=sh["reported_version"],
            desired_json=sh["desired"], reported_json=sh["reported"],
            online=bool(sh["online"]),
            open_command_status=open_status,
            ever_reported=ever_reported,
            acked_at=nxt["acked_at"] if nxt else None,
            reported_updated_at=sh["reported_updated_at"])

        next_pending = None
        if nxt and nxt["status"] != config.ST_ACKED:
            wait = model.next_backoff_delay(nxt["attempts"]) if \
                nxt["status"] == config.ST_RETRYING else 0.0
            next_pending = {
                "command_id": nxt["id"], "version": nxt["version"],
                "status": nxt["status"], "attempts": nxt["attempts"],
                "expires_at": nxt["expires_at"],
                "next_eligible_at": ((nxt["last_attempt_at"] or 0) + wait
                                     if wait else None),
                "last_error": nxt["last_error"]}

        last_ack_view = None
        if last_ack:
            last_ack_view = {
                "command_id": last_ack["id"],
                "version": last_ack["version"],
                "acked_at": last_ack["acked_at"],
                "code": last_ack["ack_code"],
                "message": last_ack["ack_message"],
                "attempts": last_ack["attempts"]}

        offline_for = None
        if not sh["online"] and sh["last_seen_at"]:
            offline_for = now - sh["last_seen_at"]

        return {
            "device_id": device_id, "name": dev["name"],
            "online": bool(sh["online"]),
            "last_seen_at": sh["last_seen_at"],
            "offline_for_seconds": offline_for,
            "desired": {"version": sh["desired_version"],
                        "state": model.parse_json(sh["desired"]),
                        "updated_at": sh["desired_updated_at"]},
            "reported": {"version": sh["reported_version"],
                         "state": model.parse_json(sh["reported"]),
                         "updated_at": sh["reported_updated_at"]},
            "sync": {"in_sync": in_sync, "reason": reason},
            "next_pending_command": next_pending,
            "last_acknowledged": last_ack_view}

    def list_devices(self) -> list[dict]:
        rows = self._all(
            "SELECT s.device_id, d.name, s.online, s.last_seen_at,"
            " s.desired_version, s.reported_version FROM shadows s"
            " JOIN devices d ON d.id=s.device_id"
            " ORDER BY s.device_id")
        return [dict(r) for r in rows]

    def list_commands(self, device_id: str, limit: int = 50) -> list[dict]:
        rows = self._all(
            "SELECT id, device_id, version, status, attempts, created_at,"
            " expires_at, claimed_at, delivered_at, acked_at,"
            " last_attempt_at, last_error, ack_code, ack_message"
            " FROM commands WHERE device_id=? ORDER BY version DESC LIMIT ?",
            (device_id, limit))
        return [dict(r) for r in rows]

    def list_attempts(self, command_id: str) -> list[dict]:
        rows = self._all(
            "SELECT attempt_no, started_at, finished_at, outcome, error"
            " FROM command_attempts WHERE command_id=? ORDER BY attempt_no",
            (command_id,))
        return [dict(r) for r in rows]

    def list_events(self, device_id: Optional[str] = None,
                    limit: int = 100, offset: int = 0) -> list[dict]:
        if device_id:
            rows = self._all(
                "SELECT id,at,device_id,command_id,event_type,detail"
                " FROM events WHERE device_id=? ORDER BY id DESC"
                " LIMIT ? OFFSET ?", (device_id, limit, offset))
        else:
            rows = self._all(
                "SELECT id,at,device_id,command_id,event_type,detail"
                " FROM events ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset))
        return [dict(r) for r in rows]
