"""会话租约状态机：所有规则在 SQLite 事务内完成，进程内一把锁串行化写。

关键不变量
==========
* 同一设备至多一个 ACTIVE（可写）会话；新连接上线/接管都产生严格单调递增的
  会话代次（devices.current_generation），关闭过的连接编号永远不能重开。
* 旧会话（非当前代次、或已结束）的状态上报、命令确认、续期一律拒绝并落
  rejected_messages，绝不覆盖新会话产生的状态。
* 命令 SENT 绑定派发代次；会话结束（被接管/租约过期/凭证撤销）时，
  已确认的不动、未发送（QUEUED）原样留给新会话、在途（SENT）转 RECONCILING，
  必须由新会话按命令版本对账后才能 ACKED 或回 QUEUED 重投。
* 轮换期间旧凭证 ROTATING 只允许给它自己的现存会话续期；新连接必须用 ACTIVE
  新凭证。宽限期到期或管理员撤销后旧会话 REVOKED/失效，但命令不丢。
"""
from __future__ import annotations

import json
import secrets as _secrets
import sqlite3
import threading
import time
import uuid
from typing import Any, Optional

from . import config
from .schema import SCHEMA


def _now() -> float:
    return time.time()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def new_secret() -> str:
    return "cred_" + _secrets.token_hex(16)


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _fingerprint(obj: dict) -> str:
    """幂等请求的请求体指纹（规范化 JSON 后比较）。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


class LeaseError(Exception):
    def __init__(self, http_status: int, code: str, message: str,
                 detail: Optional[dict] = None):
        super().__init__(message)
        self.http_status = http_status
        self.code = code
        self.message = message
        self.detail = detail or {}


def _clamp_lease(value: Optional[float]) -> float:
    if value is None:
        return config.DEFAULT_LEASE_SECONDS
    return min(config.MAX_LEASE_SECONDS,
               max(config.MIN_LEASE_SECONDS, float(value)))


class Store:
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

    def close(self) -> None:
        self._conn.close()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _row(self, sql: str, args=()):
        return self._conn.execute(sql, args).fetchone()

    def _all(self, sql: str, args=()):
        return self._conn.execute(sql, args).fetchall()

    def event(self, event_type: str, device_id: Optional[str] = None,
              session_id: Optional[str] = None, detail: Optional[dict] = None,
              at: Optional[float] = None) -> None:
        self._conn.execute(
            "INSERT INTO events(at,device_id,session_id,event_type,detail)"
            " VALUES(?,?,?,?,?)",
            (at or self.clock(), device_id, session_id, event_type,
             _json(detail or {})))

    def command_event(self, command_id: str, device_id: str, event_type: str,
                      generation: Optional[int] = None,
                      session_id: Optional[str] = None,
                      detail: Optional[dict] = None,
                      at: Optional[float] = None) -> None:
        self._conn.execute(
            "INSERT INTO command_events(at,command_id,device_id,event_type,"
            " generation,session_id,detail) VALUES(?,?,?,?,?,?,?)",
            (at or self.clock(), command_id, device_id, event_type,
             generation, session_id, _json(detail or {})))

    def reject(self, kind: str, reason: str, device_id: Optional[str] = None,
               session_id: Optional[str] = None,
               generation: Optional[int] = None,
               connection_no: Optional[str] = None,
               detail: Optional[dict] = None, at: Optional[float] = None) -> None:
        self._conn.execute(
            "INSERT INTO rejected_messages(at,device_id,session_id,generation,"
            " connection_no,kind,reason,detail) VALUES(?,?,?,?,?,?,?,?)",
            (at or self.clock(), device_id, session_id, generation,
             connection_no, kind, reason, _json(detail or {})))

    def _idem_get(self, scope: str, key: str):
        return self._row(
            "SELECT * FROM idempotency WHERE scope=? AND idem_key=?",
            (scope, key))

    def _idem_put(self, scope: str, key: str, fingerprint: str,
                  response: dict) -> None:
        self._conn.execute(
            "INSERT INTO idempotency(scope,idem_key,fingerprint,created_at,"
            " response) VALUES(?,?,?,?,?)",
            (scope, key, fingerprint, self.clock(), _json(response)))

    def _idem_replay(self, row, response: dict) -> dict:
        stored = json.loads(row["response"])
        stored["idempotent_replay"] = True
        # 带 duplicate 字段的读模型，重放一律按重复语义标记
        if "duplicate" in stored:
            stored["duplicate"] = True
        return stored

    def _idem_guard(self, scope: str, key: Optional[str],
                    fingerprint_obj: dict) -> Optional[dict]:
        """返回已存响应表示幂等重放；指纹冲突抛 409；否则 None。"""
        if not key:
            return None
        row = self._idem_get(scope, key)
        if row is None:
            return None
        fp = _fingerprint(fingerprint_obj)
        if row["fingerprint"] != fp:
            raise LeaseError(409, "IDEMPOTENCY_CONFLICT",
                             "同一个幂等键对应了不同的请求内容",
                             {"scope": scope, "idem_key": key})
        return self._idem_replay(row, json.loads(row["response"]))

    # ------------------------------------------------------------------
    # 设备注册
    # ------------------------------------------------------------------
    def register_device(self, device_id: str, name: Optional[str] = None,
                        idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            fp = {"device_id": device_id, "name": name or ""}
            replay = self._idem_guard("register", idem_key, fp)
            if replay:
                return replay
            now = self.clock()
            existing = self._row("SELECT * FROM devices WHERE device_id=?",
                                 (device_id,))
            if existing:
                cred = self._row(
                    "SELECT * FROM credentials WHERE device_id=? AND version=1",
                    (device_id,))
                resp = {"created": False, "device_id": device_id,
                        "credential_version": 1,
                        "credential_secret": cred["secret"]}
            else:
                self._conn.execute(
                    "INSERT INTO devices(device_id,name,registered_at)"
                    " VALUES(?,?,?)", (device_id, name or device_id, now))
                secret = new_secret()
                self._conn.execute(
                    "INSERT INTO credentials(device_id,version,secret,status,"
                    " created_at) VALUES(?,?,?,?,?)",
                    (device_id, 1, secret, config.CRED_ACTIVE, now))
                self.event("DEVICE_REGISTERED", device_id,
                           detail={"name": name or device_id}, at=now)
                resp = {"created": True, "device_id": device_id,
                        "credential_version": 1,
                        "credential_secret": secret}
            if idem_key:
                self._idem_put("register", idem_key, _fingerprint(fp), resp)
            return resp

    def _device(self, device_id: str):
        row = self._row("SELECT * FROM devices WHERE device_id=?",
                        (device_id,))
        if row is None:
            raise LeaseError(404, "UNKNOWN_DEVICE", "设备不存在",
                             {"device_id": device_id})
        return row

    def _next_generation(self, device_id: str) -> int:
        """会话代次严格单调：取设备表与会话表的最大值 +1。"""
        row = self._row(
            "SELECT MAX(g) AS m FROM ("
            " SELECT current_generation AS g FROM devices"
            " WHERE device_id=? UNION ALL"
            " SELECT COALESCE(MAX(generation),0) AS g FROM sessions"
            " WHERE device_id=?)", (device_id, device_id))
        return int(row["m"] or 0) + 1

    def _check_credential(self, device_id: str, version: int,
                          secret: str):
        cred = self._row(
            "SELECT * FROM credentials WHERE device_id=? AND version=?",
            (device_id, version))
        if cred is None or not _secrets.compare_digest(cred["secret"],
                                                       secret or ""):
            raise LeaseError(401, "BAD_CREDENTIAL",
                             "凭证版本不存在或密钥不匹配",
                             {"device_id": device_id,
                              "credential_version": version})
        return cred

    # ------------------------------------------------------------------
    # 会话结束的共享副作用：迁移在途命令、清指针、审计
    # ------------------------------------------------------------------
    def _end_session(self, session_row, state: str, reason: str,
                     at: float, new_session_id: Optional[str] = None) -> None:
        self._conn.execute(
            "UPDATE sessions SET state=?, ended_at=?, end_reason=?,"
            " superseded_by_session_id=COALESCE(?, superseded_by_session_id)"
            " WHERE session_id=?",
            (state, at, reason, new_session_id, session_row["session_id"]))
        # 在途（SENT，结果未知）命令转对账；已 ACKED 的绝不重发；
        # 从未发送（QUEUED）的转 QUEUED_UNKNOWN 等新会话对账表态，
        # 不直接重投（设备可能已经在本地执行过）。
        sent = self._all(
            "SELECT * FROM commands WHERE device_id=? AND state=?",
            (session_row["device_id"], config.CMD_SENT))
        for cmd in sent:
            last_attempt = self._row(
                "SELECT session_id FROM command_attempts WHERE command_id=?"
                " ORDER BY attempt_no DESC LIMIT 1", (cmd["command_id"],))
            self._conn.execute(
                "UPDATE commands SET state=? WHERE command_id=?",
                (config.CMD_RECONCILING, cmd["command_id"]))
            self.command_event(
                cmd["command_id"], cmd["device_id"], "MARKED_RECONCILING",
                generation=cmd["dispatch_generation"],
                session_id=last_attempt["session_id"] if last_attempt else None,
                detail={"reason": reason,
                        "lost_generation": cmd["dispatch_generation"]}, at=at)
        unsent = self._all(
            "SELECT * FROM commands WHERE device_id=? AND state=?",
            (session_row["device_id"], config.CMD_QUEUED))
        for cmd in unsent:
            self._conn.execute(
                "UPDATE commands SET state=? WHERE command_id=?",
                (config.CMD_QUEUED_UNKNOWN, cmd["command_id"]))
            self.command_event(
                cmd["command_id"], cmd["device_id"], "HELD_PENDING_RECONCILE",
                session_id=session_row["session_id"],
                detail={"reason": reason}, at=at)
        # 清掉设备的当前可写会话指针（若指向它）
        self._conn.execute(
            "UPDATE devices SET current_session_id=NULL"
            " WHERE device_id=? AND current_session_id=?",
            (session_row["device_id"], session_row["session_id"]))
        self.event(f"SESSION_{state}", session_row["device_id"],
                   session_row["session_id"],
                   detail={"generation": session_row["generation"],
                           "reason": reason,
                           "reconciling_commands": len(sent)}, at=at)

    def _expire_if_due(self, session_row, now: float):
        """租约已到期则按 EXPIRED 结束并返回新 row（None）。"""
        if (session_row is not None
                and session_row["state"] == config.SE_ACTIVE
                and session_row["lease_expires_at"] <= now):
            self._end_session(session_row, config.SE_EXPIRED,
                              "LEASE_EXPIRED", now)
            return None
        return session_row

    # ------------------------------------------------------------------
    # 设备上线
    # ------------------------------------------------------------------
    def online(self, device_id: str, connection_no: str,
               cred_version: int, secret: str,
               lease_seconds: Optional[float] = None,
               idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id, "connection_no": connection_no,
                      "credential_version": cred_version,
                      "lease_seconds": lease_seconds}
            replay = self._idem_guard(config.IDEM_ONLINE, idem_key, fp_obj)
            if replay:
                return replay

            device = self._device(device_id)
            cred = self._check_credential(device_id, cred_version, secret)
            if cred["status"] == config.CRED_REVOKED:
                self.reject("ONLINE", "CREDENTIAL_REVOKED", device_id,
                            connection_no=connection_no,
                            detail={"credential_version": cred_version}, at=now)
                raise LeaseError(403, "CREDENTIAL_REVOKED",
                                 "凭证已被撤销，不能建立连接",
                                 {"credential_version": cred_version})
            if cred["status"] == config.CRED_ROTATING:
                # 轮换期：旧凭证只能给已有会话续期，不能开新连接
                self.reject("ONLINE", "OLD_CREDENTIAL_NEW_CONNECTION",
                            device_id, connection_no=connection_no,
                            detail={"credential_version": cred_version}, at=now)
                raise LeaseError(
                    403, "OLD_CREDENTIAL_NEW_CONNECTION",
                    "轮换期间旧凭证只允许已有会话续期，新连接必须使用新凭证",
                    {"credential_version": cred_version})

            # 同一连接编号：关闭过的永不复活；仍 ACTIVE 的视为重复上线
            existing = self._row(
                "SELECT * FROM sessions WHERE device_id=? AND connection_no=?",
                (device_id, connection_no))
            if existing is not None:
                if existing["state"] != config.SE_ACTIVE:
                    self.reject("ONLINE", "DEAD_CONNECTION_REUSED", device_id,
                                existing["session_id"], existing["generation"],
                                connection_no,
                                detail={"state": existing["state"],
                                        "end_reason": existing["end_reason"]},
                                at=now)
                    raise LeaseError(
                        409, "DEAD_CONNECTION_REUSED",
                        "该连接编号对应的会话已结束，连接编号不能复用",
                        {"state": existing["state"]})
                if existing["credential_version"] != cred_version:
                    # 活跃连接编号配了别的凭证：拒绝，防止同号顶替
                    self.reject("ONLINE", "CREDENTIAL_MISMATCH", device_id,
                                existing["session_id"], existing["generation"],
                                connection_no,
                                detail={"session_credential_version":
                                        existing["credential_version"],
                                        "presented": cred_version}, at=now)
                    raise LeaseError(409, "CREDENTIAL_MISMATCH",
                                     "连接编号已绑定其它凭证版本")
                resp = self._session_view(existing, duplicate=True)
                if idem_key:
                    self._idem_put(config.IDEM_ONLINE, idem_key,
                                   _fingerprint(fp_obj), resp)
                return resp

            lease = _clamp_lease(lease_seconds)

            # 并发上线：当前可写会话被新一代顶替；先处理租约到期。
            # 代次始终从库里现取（入口快照可能已被本事务前的接管推进）。
            current = self._row(
                "SELECT * FROM sessions WHERE session_id=?",
                (device["current_session_id"],)) \
                if device["current_session_id"] else None
            current = self._expire_if_due(current, now)
            session_id = new_id("sess")
            if current is not None and current["state"] == config.SE_ACTIVE:
                self._end_session(current, config.SE_SUPERSEDED,
                                  "CONCURRENT_ONLINE", now,
                                  new_session_id=session_id)
            new_generation = self._next_generation(device_id)

            self._conn.execute(
                "INSERT INTO sessions(session_id,device_id,generation,"
                " connection_no,credential_version,state,opened_at,"
                " lease_seconds,lease_expires_at,last_renewed_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (session_id, device_id, new_generation, connection_no,
                 cred_version, config.SE_ACTIVE, now, lease, now + lease, now))
            self._conn.execute(
                "UPDATE devices SET current_session_id=?, current_generation=?",
                (session_id, new_generation))
            self.event("SESSION_OPENED", device_id, session_id,
                       detail={"generation": new_generation,
                               "connection_no": connection_no,
                               "credential_version": cred_version,
                               "lease_seconds": lease,
                               "concurrent": current is not None}, at=now)

            # 有待完成接管：本次新开的会话完成它。若接管之后、本次上线之前
            # 设备已自行开过新会话（接管已被隐式满足），记录保持挂账不回填。
            pending_tk = self._row(
                "SELECT * FROM takeovers WHERE device_id=? AND state=?",
                (device_id, config.TK_PENDING))
            if pending_tk is not None and self._row(
                    "SELECT 1 FROM sessions WHERE device_id=?"
                    " AND opened_at > ? AND opened_at < ?",
                    (device_id, pending_tk["created_at"], now)) is not None:
                pending_tk = None
            if pending_tk is not None:
                self._conn.execute(
                    "UPDATE takeovers SET state=?, new_session_id=?,"
                    " new_generation=?, completed_at=? WHERE takeover_id=?",
                    (config.TK_COMPLETED, session_id, new_generation, now,
                     pending_tk["takeover_id"]))
                self._conn.execute(
                    "UPDATE sessions SET takeover_id=? WHERE session_id=?",
                    (pending_tk["takeover_id"], session_id))
                self.event("TAKEOVER_COMPLETED", device_id, session_id,
                           detail={"takeover_id": pending_tk["takeover_id"],
                                   "old_generation":
                                   pending_tk["old_generation"],
                                   "new_generation": new_generation}, at=now)

            # 轮换迁移：用新凭证上线 -> 旧凭证撤销、轮换完成
            rotation = self._complete_rotation_if_needed(
                device_id, cred_version, session_id, now)

            resp = self._session_view(
                self._row("SELECT * FROM sessions WHERE session_id=?",
                          (session_id,)),
                extra={"concurrent_online": current is not None,
                       "rotation_completed": rotation})
            if idem_key:
                self._idem_put(config.IDEM_ONLINE, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    def _complete_rotation_if_needed(self, device_id: str, cred_version: int,
                                     session_id: str, now: float) -> Optional[dict]:
        rot = self._row(
            "SELECT * FROM rotations WHERE device_id=? AND new_version=?"
            " AND state != ? ORDER BY started_at DESC LIMIT 1",
            (device_id, cred_version, config.ROT_COMPLETED))
        if rot is None:
            return None
        if rot["state"] == config.ROT_ROTATING:
            # 宽限期未到设备就迁完了：旧凭证立即撤销
            self._revoke_old_credential(rot, "SUPERSEDED_MIGRATION", now)
        self._conn.execute(
            "UPDATE rotations SET state=?, new_session_id=?, completed_at=?"
            " WHERE rotation_id=?",
            (config.ROT_COMPLETED, session_id, now, rot["rotation_id"]))
        self.event("ROTATION_COMPLETED", device_id, session_id,
                   detail={"rotation_id": rot["rotation_id"],
                           "old_version": rot["old_version"],
                           "new_version": rot["new_version"]}, at=now)
        return {"rotation_id": rot["rotation_id"]}

    # ------------------------------------------------------------------
    # 设备消息鉴权：定位「仍可写」的会话
    # ------------------------------------------------------------------
    def _writable_session(self, device_id: str, connection_no: str,
                          cred_version: int, secret: str, kind: str,
                          at: Optional[float] = None):
        at = at or self.clock()
        device = self._device(device_id)
        session = self._row(
            "SELECT * FROM sessions WHERE device_id=? AND connection_no=?",
            (device_id, connection_no))
        if session is None:
            self.reject(kind, "UNKNOWN_CONNECTION", device_id,
                        connection_no=connection_no,
                        detail={"credential_version": cred_version}, at=at)
            raise LeaseError(404, "UNKNOWN_CONNECTION",
                             "连接编号没有对应的会话")
        # 先验证凭证本身
        cred = self._check_credential(device_id, cred_version, secret)
        if cred["status"] == config.CRED_REVOKED:
            self.reject(kind, "CREDENTIAL_REVOKED", device_id,
                        session["session_id"], session["generation"],
                        connection_no,
                        detail={"credential_version": cred_version}, at=at)
            raise LeaseError(403, "CREDENTIAL_REVOKED", "凭证已被撤销")
        if session["credential_version"] != cred_version:
            self.reject(kind, "CREDENTIAL_MISMATCH", device_id,
                        session["session_id"], session["generation"],
                        connection_no,
                        detail={"session_credential_version":
                                session["credential_version"],
                                "presented": cred_version}, at=at)
            raise LeaseError(403, "CREDENTIAL_MISMATCH",
                             "该连接不属于所提交的凭证版本")
        # 租约到期：结束旧会话（幂等），随后拒绝
        session = self._expire_if_due(session, at)
        if session is None:
            self.reject(kind, "SESSION_EXPIRED", device_id,
                        connection_no=connection_no,
                        generation=None,
                        detail={"lease_expired_before": at}, at=at)
            raise LeaseError(410, "SESSION_EXPIRED",
                             "会话租约已到期，消息被拒绝；请重新上线")
        if session["state"] != config.SE_ACTIVE:
            reason = {
                config.SE_SUPERSEDED: "SESSION_SUPERSEDED",
                config.SE_REVOKED: "SESSION_REVOKED",
                config.SE_EXPIRED: "SESSION_EXPIRED",
            }.get(session["state"], "SESSION_NOT_WRITABLE")
            self.reject(kind, reason, device_id, session["session_id"],
                        session["generation"], connection_no,
                        detail={"state": session["state"],
                                "current_generation":
                                device["current_generation"]}, at=at)
            raise LeaseError(
                410, reason,
                "旧连接已不是当前可写会话，消息被拒绝且未产生任何副作用",
                {"session_generation": session["generation"],
                 "current_generation": device["current_generation"]})
        if device["current_session_id"] != session["session_id"]:
            # 理论不会发生（ACTIVE 却不在指针上），防御性拒绝
            self.reject(kind, "NOT_CURRENT_SESSION", device_id,
                        session["session_id"], session["generation"],
                        connection_no, at=at)
            raise LeaseError(410, "NOT_CURRENT_SESSION",
                             "该会话不是设备当前会话")
        return device, session, cred

    # ------------------------------------------------------------------
    # 租约续期（轮换期旧凭证只允许走这里）
    # ------------------------------------------------------------------
    def renew(self, device_id: str, connection_no: str, cred_version: int,
              secret: str, lease_seconds: Optional[float] = None,
              idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id, "connection_no": connection_no,
                      "credential_version": cred_version,
                      "lease_seconds": lease_seconds}
            replay = self._idem_guard(config.IDEM_RENEW, idem_key, fp_obj)
            if replay:
                return replay

            device = self._device(device_id)
            session = self._row(
                "SELECT * FROM sessions WHERE device_id=? AND connection_no=?",
                (device_id, connection_no))
            if session is None:
                self.reject("RENEW", "UNKNOWN_CONNECTION", device_id,
                            connection_no=connection_no, at=now)
                raise LeaseError(404, "UNKNOWN_CONNECTION",
                                 "连接编号没有对应的会话")
            cred = self._check_credential(device_id, cred_version, secret)
            if cred["status"] == config.CRED_REVOKED:
                self.reject("RENEW", "CREDENTIAL_REVOKED", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"credential_version": cred_version}, at=now)
                raise LeaseError(403, "CREDENTIAL_REVOKED",
                                 "旧凭证已撤销，旧会话不能续期")
            if session["credential_version"] != cred_version:
                self.reject("RENEW", "CREDENTIAL_MISMATCH", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"session_credential_version":
                                    session["credential_version"],
                                    "presented": cred_version}, at=now)
                raise LeaseError(403, "CREDENTIAL_MISMATCH",
                                 "凭证版本与会话不匹配")
            if session["state"] != config.SE_ACTIVE:
                reason = {
                    config.SE_SUPERSEDED: "SESSION_SUPERSEDED",
                    config.SE_REVOKED: "SESSION_REVOKED",
                    config.SE_EXPIRED: "SESSION_EXPIRED",
                }[session["state"]]
                self.reject("RENEW", reason, device_id, session["session_id"],
                            session["generation"], connection_no,
                            detail={"state": session["state"]}, at=now)
                raise LeaseError(410, reason, "已结束的会话不能续期")
            if session["lease_expires_at"] <= now:
                # 到期瞬间：先结束再拒绝，绝不延长
                self._expire_if_due(session, now)
                self.reject("RENEW", "SESSION_EXPIRED", device_id,
                            session["session_id"], session["generation"],
                            connection_no, at=now)
                raise LeaseError(410, "SESSION_EXPIRED",
                                 "租约已到期，不能续期；请重新上线")
            # ROTATING 旧凭证：明确允许续它自己的现存会话（唯一放行的旧凭证操作）
            lease = _clamp_lease(lease_seconds)
            new_expiry = now + lease
            self._conn.execute(
                "UPDATE sessions SET lease_expires_at=?, last_renewed_at=?,"
                " lease_seconds=? WHERE session_id=?",
                (new_expiry, now, lease, session["session_id"]))
            self.event("SESSION_RENEWED", device_id, session["session_id"],
                       detail={"generation": session["generation"],
                               "credential_version": cred_version,
                               "credential_status": cred["status"],
                               "lease_expires_at": new_expiry}, at=now)
            resp = self._session_view(
                self._row("SELECT * FROM sessions WHERE session_id=?",
                          (session["session_id"],)),
                extra={"renewed_with_old_credential":
                       cred["status"] == config.CRED_ROTATING})
            if idem_key:
                self._idem_put(config.IDEM_RENEW, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    # ------------------------------------------------------------------
    # 心跳领命令
    # ------------------------------------------------------------------
    def poll(self, device_id: str, connection_no: str, cred_version: int,
             secret: str, lease_seconds: Optional[float] = None) -> dict:
        with self._wlock:
            now = self.clock()
            device, session, cred = self._writable_session(
                device_id, connection_no, cred_version, secret, "POLL", now)
            # poll 同时充当心跳：延长租约
            lease = _clamp_lease(lease_seconds)
            self._conn.execute(
                "UPDATE sessions SET lease_expires_at=?, last_renewed_at=?"
                " WHERE session_id=?",
                (now + lease, now, session["session_id"]))

            # 严格按命令版本顺序处理：最低版本未决命令决定 poll 返回什么。
            lowest = self._row(
                "SELECT * FROM commands WHERE device_id=? AND state IN (?,?,?,?)"
                " ORDER BY version ASC LIMIT 1",
                (device_id, config.CMD_QUEUED, config.CMD_SENT,
                 config.CMD_RECONCILING, config.CMD_QUEUED_UNKNOWN))
            queued = lowest if lowest is not None and \
                lowest["state"] == config.CMD_QUEUED else None
            if lowest is not None and lowest["state"] in (
                    config.CMD_SENT, config.CMD_RECONCILING,
                    config.CMD_QUEUED_UNKNOWN):
                # 低版本结果未知：幂等重发在途命令/提示对账，绝不跳发新版本。
                # QUEUED_UNKNOWN 不是派发（不产生 attempt），状态保持不变。
                return {"command": self._command_view(lowest),
                        "duplicate_dispatch":
                        lowest["state"] == config.CMD_SENT,
                        "await_reconcile": lowest["state"] in (
                            config.CMD_RECONCILING,
                            config.CMD_QUEUED_UNKNOWN),
                        "session_generation": session["generation"],
                        "lease_expires_at": now + lease}
            if queued is not None:
                attempt_no = self._next_attempt_no(queued["command_id"])
                self._conn.execute(
                    "UPDATE commands SET state=?, sent_at=?,"
                    " dispatch_generation=? WHERE command_id=?",
                    (config.CMD_SENT, now, session["generation"],
                     queued["command_id"]))
                self._conn.execute(
                    "INSERT INTO command_attempts(command_id,attempt_no,"
                    " session_id,generation,dispatched_at)"
                    " VALUES(?,?,?,?,?)",
                    (queued["command_id"], attempt_no, session["session_id"],
                     session["generation"], now))
                self.command_event(
                    queued["command_id"], device_id, "DISPATCHED",
                    generation=session["generation"],
                    session_id=session["session_id"],
                    detail={"version": queued["version"],
                            "attempt_no": attempt_no}, at=now)
                return {"command": self._command_view(
                    self._row("SELECT * FROM commands WHERE command_id=?",
                              (queued["command_id"],))),
                    "session_generation": session["generation"],
                    "lease_expires_at": now + lease}

            # 没有 QUEUED：若本会话有一条在途（重复 poll/响应丢失），幂等返回它
            inflight = self._row(
                "SELECT * FROM commands WHERE device_id=? AND state=?"
                " AND dispatch_generation=? ORDER BY version ASC LIMIT 1",
                (device_id, config.CMD_SENT, session["generation"]))
            if inflight is not None:
                return {"command": self._command_view(inflight),
                        "duplicate_dispatch": True,
                        "session_generation": session["generation"],
                        "lease_expires_at": now + lease}
            return {"command": None,
                    "session_generation": session["generation"],
                    "lease_expires_at": now + lease}

    def _next_attempt_no(self, command_id: str) -> int:
        row = self._row(
            "SELECT COALESCE(MAX(attempt_no),0)+1 AS n FROM command_attempts"
            " WHERE command_id=?", (command_id,))
        return int(row["n"])

    # ------------------------------------------------------------------
    # 状态上报（旧会话不能覆盖新会话状态）
    # ------------------------------------------------------------------
    def report_state(self, device_id: str, connection_no: str,
                     cred_version: int, secret: str, state_version: int,
                     state: Any, idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id, "connection_no": connection_no,
                      "credential_version": cred_version,
                      "state_version": state_version, "state": state}
            replay = self._idem_guard(config.IDEM_REPORT, idem_key, fp_obj)
            if replay:
                return replay
            device, session, _ = self._writable_session(
                device_id, connection_no, cred_version, secret, "REPORT", now)

            prev = self._row(
                "SELECT * FROM reported_states WHERE device_id=?",
                (device_id,))
            prev_version = int(prev["state_version"]) if prev else 0
            if state_version < prev_version:
                self.reject("REPORT", "STALE_STATE_VERSION", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"submitted": state_version,
                                    "current": prev_version,
                                    "submitted_generation":
                                    session["generation"]}, at=now)
                raise LeaseError(
                    409, "STALE_STATE_VERSION",
                    "状态版本比当前版本旧，拒绝且不覆盖新会话状态",
                    {"submitted_version": state_version,
                     "current_version": prev_version})
            if state_version == prev_version:
                self.reject("REPORT", "DUPLICATE_STATE_VERSION", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"state_version": state_version}, at=now)
                raise LeaseError(409, "DUPLICATE_STATE_VERSION",
                                 "相同状态版本重复上报",
                                 {"state_version": state_version})
            self._conn.execute(
                "INSERT INTO reported_states(device_id,state_version,doc,"
                " updated_at,generation,session_id) VALUES(?,?,?,?,?,?)"
                " ON CONFLICT(device_id) DO UPDATE SET state_version=excluded"
                ".state_version, doc=excluded.doc, updated_at=excluded"
                ".updated_at, generation=excluded.generation,"
                " session_id=excluded.session_id",
                (device_id, state_version, _json(state), now,
                 session["generation"], session["session_id"]))
            self.event("STATE_REPORTED", device_id, session["session_id"],
                       detail={"state_version": state_version,
                               "generation": session["generation"]}, at=now)
            resp = {"accepted": True, "state_version": state_version,
                    "session_generation": session["generation"]}
            if idem_key:
                self._idem_put(config.IDEM_REPORT, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    # ------------------------------------------------------------------
    # 命令确认（必须由收到它的那一代会话确认）
    # ------------------------------------------------------------------
    def ack(self, device_id: str, connection_no: str, cred_version: int,
            secret: str, command_id: Optional[str] = None,
            version: Optional[int] = None, code: Optional[str] = None,
            result: Optional[str] = None,
            idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id, "connection_no": connection_no,
                      "credential_version": cred_version,
                      "command_id": command_id, "version": version,
                      "code": code, "result": result}
            replay = self._idem_guard(config.IDEM_ACK, idem_key, fp_obj)
            if replay:
                return replay
            device, session, _ = self._writable_session(
                device_id, connection_no, cred_version, secret, "ACK", now)

            cmd = self._find_command(device_id, command_id, version)
            if cmd["state"] == config.CMD_ACKED:
                # 能走到这里的必定是当前可写会话（旧会话在鉴权阶段已拒绝），
                # 已确认命令的重复确认一律幂等返回，绝不重发任何副作用
                return {"command_id": cmd["command_id"],
                        "version": cmd["version"], "state": config.CMD_ACKED,
                        "duplicate_ack": True,
                        "session_generation": session["generation"]}
            if cmd["state"] in (config.CMD_RECONCILING,
                                config.CMD_QUEUED_UNKNOWN):
                self.reject("ACK", "COMMAND_AWAIT_RECONCILE", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"command_id": cmd["command_id"],
                                    "command_version": cmd["version"],
                                    "command_state": cmd["state"],
                                    "dispatch_generation":
                                    cmd["dispatch_generation"]}, at=now)
                raise LeaseError(
                    409, "COMMAND_AWAIT_RECONCILE",
                    "命令在会话切换时结果未知，必须先按命令版本对账，不能直接确认")
            if cmd["state"] == config.CMD_QUEUED:
                self.reject("ACK", "COMMAND_NOT_DISPATCHED", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"command_id": cmd["command_id"],
                                    "command_version": cmd["version"]}, at=now)
                raise LeaseError(409, "COMMAND_NOT_DISPATCHED",
                                 "命令尚未派发给本会话，不能确认")
            # SENT：必须是派发给当前这一代会话的
            if cmd["dispatch_generation"] != session["generation"]:
                self.reject("ACK", "GENERATION_MISMATCH", device_id,
                            session["session_id"], session["generation"],
                            connection_no,
                            detail={"command_id": cmd["command_id"],
                                    "command_version": cmd["version"],
                                    "dispatch_generation":
                                    cmd["dispatch_generation"],
                                    "session_generation":
                                    session["generation"]}, at=now)
                raise LeaseError(410, "GENERATION_MISMATCH",
                                 "命令属于另一代会话，本会话不能确认")
            self._conn.execute(
                "UPDATE commands SET state=?, acked_at=?, ack_code=?,"
                " ack_result=?, ack_source=? WHERE command_id=?",
                (config.CMD_ACKED, now, code or "OK", result, "ACK",
                 cmd["command_id"]))
            self.command_event(
                cmd["command_id"], device_id, "ACKED",
                generation=session["generation"],
                session_id=session["session_id"],
                detail={"version": cmd["version"], "code": code or "OK",
                        "result": result, "source": "ACK"}, at=now)
            resp = {"command_id": cmd["command_id"], "version": cmd["version"],
                    "state": config.CMD_ACKED, "duplicate_ack": False,
                    "session_generation": session["generation"]}
            if idem_key:
                self._idem_put(config.IDEM_ACK, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    def _find_command(self, device_id: str, command_id: Optional[str],
                      version: Optional[int]):
        if command_id:
            cmd = self._row(
                "SELECT * FROM commands WHERE command_id=? AND device_id=?",
                (command_id, device_id))
        elif version is not None:
            cmd = self._row(
                "SELECT * FROM commands WHERE device_id=? AND version=?",
                (device_id, version))
        else:
            raise LeaseError(400, "MISSING_COMMAND_REF",
                             "必须提供 command_id 或 version")
        if cmd is None:
            raise LeaseError(404, "COMMAND_NOT_FOUND", "命令不存在")
        return cmd

    # ------------------------------------------------------------------
    # 对账：新会话按命令版本裁定在途/重投命令
    # ------------------------------------------------------------------
    def reconcile(self, device_id: str, connection_no: str,
                  cred_version: int, secret: str,
                  entries: list[dict], idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            norm = sorted(
                ({"version": int(e["version"]),
                  "done": bool(e["done"]),
                  "result": e.get("result")} for e in entries),
                key=lambda e: e["version"])
            fp_obj = {"device_id": device_id, "connection_no": connection_no,
                      "credential_version": cred_version, "entries": norm}
            replay = self._idem_guard(config.IDEM_RECONCILE, idem_key, fp_obj)
            if replay:
                return replay
            device, session, _ = self._writable_session(
                device_id, connection_no, cred_version, secret,
                "RECONCILE", now)

            results = []
            done_count = retry_count = 0
            for e in norm:
                cmd = self._row(
                    "SELECT * FROM commands WHERE device_id=? AND version=?",
                    (device_id, e["version"]))
                if cmd is None:
                    raise LeaseError(404, "COMMAND_NOT_FOUND",
                                     f"命令版本 {e['version']} 不存在",
                                     {"version": e["version"]})
                outcome = self._reconcile_one(cmd, e, session, now)
                results.append(outcome)
                if outcome["outcome"] == "RECONCILE_DONE":
                    done_count += 1
                elif outcome["outcome"] == "RETRY_TO_NEW_SESSION":
                    retry_count += 1
            resp = {"session_generation": session["generation"],
                    "results": results,
                    "reconciled_done": done_count,
                    "retried_to_new_session": retry_count}
            if idem_key:
                self._idem_put(config.IDEM_RECONCILE, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    def _reconcile_one(self, cmd, entry: dict, session, now: float) -> dict:
        cid, ver = cmd["command_id"], cmd["version"]
        if cmd["state"] == config.CMD_ACKED:
            # 幂等：已经对账/确认完成的，重放只回现状
            return {"version": ver, "command_id": cid,
                    "outcome": "ALREADY_DONE", "state": config.CMD_ACKED,
                    "ack_source": cmd["ack_source"]}
        if cmd["state"] == config.CMD_RECONCILING:
            if entry["done"]:
                self._conn.execute(
                    "UPDATE commands SET state=?, acked_at=?, ack_code=?,"
                    " ack_result=?, ack_source=? WHERE command_id=?",
                    (config.CMD_ACKED, now,
                     entry.get("result") or "RECONCILED_DONE",
                     entry.get("result"), "RECONCILE_DONE", cid))
                self.command_event(
                    cid, cmd["device_id"], "RECONCILE_DONE",
                    generation=session["generation"],
                    session_id=session["session_id"],
                    detail={"version": ver,
                            "lost_generation": cmd["dispatch_generation"],
                            "result": entry.get("result")}, at=now)
                return {"version": ver, "command_id": cid,
                        "outcome": "RECONCILE_DONE", "state": config.CMD_ACKED}
            # 设备确认没执行/没执行完：回到队列，交给当前会话重投
            self._conn.execute(
                "UPDATE commands SET state=? WHERE command_id=?",
                (config.CMD_QUEUED, cid))
            self.command_event(
                cid, cmd["device_id"], "RECONCILE_RETRY",
                generation=session["generation"],
                session_id=session["session_id"],
                detail={"version": ver,
                        "lost_generation": cmd["dispatch_generation"]}, at=now)
            return {"version": ver, "command_id": cid,
                    "outcome": "RETRY_TO_NEW_SESSION",
                    "state": config.CMD_QUEUED}
        if cmd["state"] == config.CMD_QUEUED:
            return {"version": ver, "command_id": cid,
                    "outcome": "STILL_QUEUED", "state": config.CMD_QUEUED}
        if cmd["state"] == config.CMD_QUEUED_UNKNOWN:
            # 会话切换前从未发送给设备：设备对账表态
            if entry["done"]:
                # 设备声称未发送却已完成：保守视为完成（设备本地执行过）
                self._conn.execute(
                    "UPDATE commands SET state=?, acked_at=?, ack_code=?,"
                    " ack_result=?, ack_source=? WHERE command_id=?",
                    (config.CMD_ACKED, now,
                     entry.get("result") or "RECONCILED_DONE",
                     entry.get("result"), "RECONCILE_DONE", cid))
                self.command_event(
                    cid, cmd["device_id"], "RECONCILE_DONE",
                    generation=session["generation"],
                    session_id=session["session_id"],
                    detail={"version": ver, "was_unsent": True,
                            "result": entry.get("result")}, at=now)
                return {"version": ver, "command_id": cid,
                        "outcome": "RECONCILE_DONE", "state": config.CMD_ACKED}
            self._conn.execute(
                "UPDATE commands SET state=? WHERE command_id=?",
                (config.CMD_QUEUED, cid))
            self.command_event(
                cid, cmd["device_id"], "RECONCILE_RETRY",
                generation=session["generation"],
                session_id=session["session_id"],
                detail={"version": ver, "was_unsent": True}, at=now)
            return {"version": ver, "command_id": cid,
                    "outcome": "RETRY_TO_NEW_SESSION",
                    "state": config.CMD_QUEUED}
        # SENT：当前代次在途（对账与派发竞态）
        if cmd["dispatch_generation"] == session["generation"]:
            if entry["done"]:
                self._conn.execute(
                    "UPDATE commands SET state=?, acked_at=?, ack_code=?,"
                    " ack_result=?, ack_source=? WHERE command_id=?",
                    (config.CMD_ACKED, now,
                     entry.get("result") or "RECONCILED_DONE",
                     entry.get("result"), "RECONCILE_DONE", cid))
                self.command_event(
                    cid, cmd["device_id"], "RECONCILE_DONE",
                    generation=session["generation"],
                    session_id=session["session_id"],
                    detail={"version": ver, "in_flight": True}, at=now)
                return {"version": ver, "command_id": cid,
                        "outcome": "RECONCILE_DONE", "state": config.CMD_ACKED}
            return {"version": ver, "command_id": cid, "outcome": "IN_FLIGHT",
                    "state": config.CMD_SENT}
        # 派发给别的代次却仍是 SENT（理论上会话结束时已转 RECONCILING）
        self._conn.execute(
            "UPDATE commands SET state=? WHERE command_id=?",
            (config.CMD_RECONCILING, cid))
        return {"version": ver, "command_id": cid,
                "outcome": "DEFERRED_RECONCILE",
                "state": config.CMD_RECONCILING}

    # ------------------------------------------------------------------
    # 控制端：下发命令（幂等）
    # ------------------------------------------------------------------
    def issue_command(self, device_id: str, payload: Any,
                      idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id, "payload": payload}
            replay = self._idem_guard(config.IDEM_COMMAND, idem_key, fp_obj)
            if replay:
                return replay
            device = self._device(device_id)
            # 同一幂等键重放（无键时下面的唯一索引天然保证不重复）
            if idem_key:
                dup = self._row(
                    "SELECT * FROM commands WHERE device_id=? AND idem_key=?",
                    (device_id, idem_key))
                if dup is not None:
                    return self._command_view(dup, duplicate=True)
            version = device["next_command_version"] + 1
            cid = new_id("cmd")
            self._conn.execute(
                "UPDATE devices SET next_command_version=? WHERE device_id=?",
                (version, device_id))
            self._conn.execute(
                "INSERT INTO commands(command_id,device_id,version,payload,"
                " state,created_at,idem_key) VALUES(?,?,?,?,?,?,?)",
                (cid, device_id, version, _json(payload), config.CMD_QUEUED,
                 now, idem_key))
            self.command_event(cid, device_id, "CREATED",
                               detail={"version": version,
                                       "idem_key": idem_key}, at=now)
            resp = self._command_view(
                self._row("SELECT * FROM commands WHERE command_id=?", (cid,)))
            if idem_key:
                self._idem_put(config.IDEM_COMMAND, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    # ------------------------------------------------------------------
    # 控制端：接管
    # ------------------------------------------------------------------
    def takeover(self, device_id: str, reason: Optional[str] = None,
                 idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id, "reason": reason or "ADMIN"}
            replay = self._idem_guard(config.IDEM_TAKEOVER, idem_key, fp_obj)
            if replay:
                return replay

            device = self._device(device_id)
            pending = self._row(
                "SELECT * FROM takeovers WHERE device_id=? AND state=?",
                (device_id, config.TK_PENDING))
            if pending is not None:
                # 接管本身幂等：已有待重连接管，直接返回
                resp = self._takeover_view(pending, duplicate=True)
                if idem_key:
                    self._idem_put(config.IDEM_TAKEOVER, idem_key,
                                   _fingerprint(fp_obj), resp)
                return resp

            tk_id = new_id("tk")
            current = self._row(
                "SELECT * FROM sessions WHERE session_id=?",
                (device["current_session_id"],)) \
                if device["current_session_id"] else None
            current = self._expire_if_due(current, now)
            old_sid = old_gen = None
            if current is not None and current["state"] == config.SE_ACTIVE:
                old_sid = current["session_id"]
                old_gen = current["generation"]
                self._end_session(current, config.SE_SUPERSEDED,
                                  "ADMIN_TAKEOVER", now)
            self._conn.execute(
                "INSERT INTO takeovers(takeover_id,device_id,old_session_id,"
                " old_generation,reason,state,created_at,idem_key)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (tk_id, device_id, old_sid, old_gen, reason or "ADMIN",
                 config.TK_PENDING, now, idem_key or tk_id))
            self.event("TAKEOVER_REQUESTED", device_id, old_sid,
                       detail={"takeover_id": tk_id,
                               "old_generation": old_gen,
                               "reason": reason or "ADMIN"}, at=now)
            resp = self._takeover_view(
                self._row("SELECT * FROM takeovers WHERE takeover_id=?",
                          (tk_id,)))
            if idem_key:
                self._idem_put(config.IDEM_TAKEOVER, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    # ------------------------------------------------------------------
    # 控制端：凭证轮换
    # ------------------------------------------------------------------
    def start_rotation(self, device_id: str,
                       grace_seconds: Optional[float] = None,
                       idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            grace = grace_seconds if grace_seconds is not None \
                else config.DEFAULT_GRACE_SECONDS
            grace = max(config.MIN_GRACE_SECONDS, float(grace))
            fp_obj = {"device_id": device_id, "grace_seconds": grace}
            replay = self._idem_guard(config.IDEM_ROTATE, idem_key, fp_obj)
            if replay:
                return replay

            device = self._device(device_id)
            open_rot = self._row(
                "SELECT * FROM rotations WHERE device_id=? AND state != ?"
                " ORDER BY started_at DESC LIMIT 1",
                (device_id, config.ROT_COMPLETED))
            if open_rot is not None:
                # 进行中的轮换重复发起：幂等返回，并带上当时签发的新密钥。
                new_cred = self._row(
                    "SELECT secret FROM credentials WHERE device_id=?"
                    " AND version=?",
                    (device_id, open_rot["new_version"]))
                return self._rotation_view(
                    open_rot, duplicate=True,
                    extra={"new_credential_secret": new_cred["secret"]})

            old = self._row(
                "SELECT * FROM credentials WHERE device_id=? AND status=?",
                (device_id, config.CRED_ACTIVE))
            if old is None:
                raise LeaseError(409, "NO_ACTIVE_CREDENTIAL",
                                 "设备没有 ACTIVE 凭证，无法轮换")
            new_version = int(old["version"]) + 1
            fresh_secret = new_secret()
            rot_id = new_id("rot")
            self._conn.execute(
                "INSERT INTO credentials(device_id,version,secret,status,"
                " created_at,rotation_id) VALUES(?,?,?,?,?,?)",
                (device_id, new_version, fresh_secret, config.CRED_ACTIVE, now,
                 rot_id))
            self._conn.execute(
                "UPDATE credentials SET status=?, rotation_id=?,"
                " revoke_reason=NULL WHERE device_id=? AND version=?",
                (config.CRED_ROTATING, rot_id, device_id, old["version"]))
            self._conn.execute(
                "INSERT INTO rotations(rotation_id,device_id,old_version,"
                " new_version,state,started_at,grace_deadline)"
                " VALUES(?,?,?,?,?,?,?)",
                (rot_id, device_id, old["version"], new_version,
                 config.ROT_ROTATING, now, now + grace))
            self.event("ROTATION_STARTED", device_id,
                       detail={"rotation_id": rot_id,
                               "old_version": old["version"],
                               "new_version": new_version,
                               "grace_deadline": now + grace}, at=now)
            resp = self._rotation_view(
                self._row("SELECT * FROM rotations WHERE rotation_id=?",
                          (rot_id,)),
                extra={"new_credential_secret": fresh_secret})
            if idem_key:
                self._idem_put(config.IDEM_ROTATE, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    def revoke_old_credential(self, device_id: str,
                              idem_key: Optional[str] = None) -> dict:
        with self._wlock:
            now = self.clock()
            fp_obj = {"device_id": device_id}
            replay = self._idem_guard(config.IDEM_REVOKE, idem_key, fp_obj)
            if replay:
                return replay
            device = self._device(device_id)
            rot = self._row(
                "SELECT * FROM rotations WHERE device_id=? AND state != ?"
                " ORDER BY started_at DESC LIMIT 1",
                (device_id, config.ROT_COMPLETED))
            if rot is None:
                raise LeaseError(409, "NO_ROTATION_IN_PROGRESS",
                                 "该设备没有进行中的轮换")
            if rot["state"] != config.ROT_ROTATING:
                # 已因宽限期到期/重复撤销而撤销：幂等返回
                return self._rotation_view(rot, duplicate=True)
            self._revoke_old_credential(rot, "ADMIN_REVOKED", now)
            self._conn.execute(
                "UPDATE rotations SET state=? WHERE rotation_id=?",
                (config.ROT_OLD_REVOKED, rot["rotation_id"]))
            self.event("ROTATION_OLD_REVOKED", device_id,
                       detail={"rotation_id": rot["rotation_id"],
                               "reason": "ADMIN_REVOKED"}, at=now)
            resp = self._rotation_view(
                self._row("SELECT * FROM rotations WHERE rotation_id=?",
                          (rot["rotation_id"],)))
            if idem_key:
                self._idem_put(config.IDEM_REVOKE, idem_key,
                               _fingerprint(fp_obj), resp)
            return resp

    def _revoke_old_credential(self, rot, reason: str, now: float) -> None:
        """旧凭证置 REVOKED；仍在用它的可写会话立即失效，在途命令转对账。"""
        self._conn.execute(
            "UPDATE credentials SET status=?, revoked_at=?, revoke_reason=?"
            " WHERE device_id=? AND version=?",
            (config.CRED_REVOKED, now, reason, rot["device_id"],
             rot["old_version"]))
        sessions = self._all(
            "SELECT * FROM sessions WHERE device_id=? AND credential_version=?"
            " AND state=?",
            (rot["device_id"], rot["old_version"], config.SE_ACTIVE))
        lost = 0
        for s in sessions:
            self._end_session(s, config.SE_REVOKED,
                              "CREDENTIAL_REVOKED_" + reason, now)
            lost += 1
        self.event("CREDENTIAL_REVOKED", rot["device_id"],
                   detail={"rotation_id": rot["rotation_id"],
                           "old_version": rot["old_version"],
                           "reason": reason, "sessions_killed": lost}, at=now)

    # ------------------------------------------------------------------
    # 后台清扫：租约到期 + 轮换宽限期到期
    # ------------------------------------------------------------------
    def sweep(self) -> dict:
        with self._wlock:
            now = self.clock()
            expired = 0
            for s in self._all(
                    "SELECT * FROM sessions WHERE state=? AND lease_expires_at"
                    " <= ?", (config.SE_ACTIVE, now)):
                self._end_session(s, config.SE_EXPIRED, "LEASE_EXPIRED", now)
                expired += 1
            grace_hit = 0
            for rot in self._all(
                    "SELECT * FROM rotations WHERE state=?"
                    " AND grace_deadline <= ?",
                    (config.ROT_ROTATING, now)):
                self._revoke_old_credential(rot, "GRACE_EXPIRED", now)
                self._conn.execute(
                    "UPDATE rotations SET state=? WHERE rotation_id=?",
                    (config.ROT_OLD_REVOKED, rot["rotation_id"]))
                self.event("ROTATION_GRACE_EXPIRED", rot["device_id"],
                           detail={"rotation_id": rot["rotation_id"]}, at=now)
                grace_hit += 1
            return {"at": now, "sessions_expired": expired,
                    "grace_expirations": grace_hit}

    # ------------------------------------------------------------------
    # 读模型 / 审计查询
    # ------------------------------------------------------------------
    def _session_view(self, row, duplicate: bool = False,
                      extra: Optional[dict] = None) -> dict:
        out = {
            "session_id": row["session_id"],
            "device_id": row["device_id"],
            "generation": row["generation"],
            "connection_no": row["connection_no"],
            "credential_version": row["credential_version"],
            "state": row["state"],
            "opened_at": row["opened_at"],
            "lease_seconds": row["lease_seconds"],
            "lease_expires_at": row["lease_expires_at"],
            "last_renewed_at": row["last_renewed_at"],
            "ended_at": row["ended_at"],
            "end_reason": row["end_reason"],
            "superseded_by_session_id": row["superseded_by_session_id"],
            "takeover_id": row["takeover_id"],
            "duplicate": duplicate,
        }
        if extra:
            out.update(extra)
        return out

    def _rotation_view(self, row, duplicate: bool = False,
                       extra: Optional[dict] = None) -> dict:
        out = {
            "rotation_id": row["rotation_id"],
            "device_id": row["device_id"],
            "old_version": row["old_version"],
            "new_version": row["new_version"],
            "state": row["state"],
            "started_at": row["started_at"],
            "grace_deadline": row["grace_deadline"],
            "revoked_at": row["revoked_at"],
            "revoke_reason": row["revoke_reason"],
            "new_session_id": row["new_session_id"],
            "completed_at": row["completed_at"],
            "duplicate": duplicate,
        }
        if extra:
            out.update(extra)
        return out

    def _takeover_view(self, row, duplicate: bool = False) -> dict:
        return {
            "takeover_id": row["takeover_id"],
            "device_id": row["device_id"],
            "old_session_id": row["old_session_id"],
            "old_generation": row["old_generation"],
            "new_session_id": row["new_session_id"],
            "new_generation": row["new_generation"],
            "reason": row["reason"],
            "state": row["state"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "duplicate": duplicate,
        }

    def _command_view(self, row, duplicate: bool = False) -> dict:
        return {
            "command_id": row["command_id"],
            "device_id": row["device_id"],
            "version": row["version"],
            "payload": json.loads(row["payload"]),
            "state": row["state"],
            "created_at": row["created_at"],
            "sent_at": row["sent_at"],
            "acked_at": row["acked_at"],
            "dispatch_generation": row["dispatch_generation"],
            "ack_code": row["ack_code"],
            "ack_result": row["ack_result"],
            "ack_source": row["ack_source"],
            "duplicate": duplicate,
        }

    def device_read_model(self, device_id: str) -> dict:
        device = self._device(device_id)
        current = None
        if device["current_session_id"]:
            current = self._session_view(self._row(
                "SELECT * FROM sessions WHERE session_id=?",
                (device["current_session_id"],)))
        rotation = self._row(
            "SELECT * FROM rotations WHERE device_id=? AND state != ?"
            " ORDER BY started_at DESC LIMIT 1",
            (device_id, config.ROT_COMPLETED))
        reported = self._row(
            "SELECT * FROM reported_states WHERE device_id=?", (device_id,))
        counts = {}
        for r in self._all(
                "SELECT state, COUNT(*) AS n FROM commands WHERE device_id=?"
                " GROUP BY state", (device_id,)):
            counts[r["state"]] = r["n"]
        return {
            "device_id": device_id,
            "name": device["name"],
            "current_generation": device["current_generation"],
            "next_command_version": device["next_command_version"],
            "current_session": current,
            "online": current is not None and current["state"] == config.SE_ACTIVE,
            "open_rotation": self._rotation_view(rotation) if rotation else None,
            "reported_state": None if reported is None else {
                "state_version": reported["state_version"],
                "state": json.loads(reported["doc"]),
                "updated_at": reported["updated_at"],
                "generation": reported["generation"],
                "session_id": reported["session_id"],
            },
            "command_counts": counts,
        }

    def list_sessions(self, device_id: str, limit: int = 100) -> list[dict]:
        self._device(device_id)
        rows = self._all(
            "SELECT * FROM sessions WHERE device_id=? ORDER BY generation DESC"
            " LIMIT ?", (device_id, limit))
        return [self._session_view(r) for r in rows]

    def list_credentials(self, device_id: str) -> list[dict]:
        self._device(device_id)
        return [dict(r) for r in self._all(
            "SELECT device_id,version,status,created_at,revoked_at,"
            "revoke_reason,rotation_id FROM credentials"
            " WHERE device_id=? ORDER BY version", (device_id,))]

    def list_rotations(self, device_id: Optional[str] = None,
                       limit: int = 100) -> list[dict]:
        if device_id:
            rows = self._all(
                "SELECT * FROM rotations WHERE device_id=?"
                " ORDER BY started_at DESC LIMIT ?", (device_id, limit))
        else:
            rows = self._all(
                "SELECT * FROM rotations ORDER BY started_at DESC LIMIT ?",
                (limit,))
        return [self._rotation_view(r) for r in rows]

    def list_takeovers(self, device_id: str, limit: int = 100) -> list[dict]:
        self._device(device_id)
        rows = self._all(
            "SELECT * FROM takeovers WHERE device_id=?"
            " ORDER BY created_at DESC LIMIT ?", (device_id, limit))
        return [self._takeover_view(r) for r in rows]

    def list_commands(self, device_id: str, limit: int = 100) -> list[dict]:
        self._device(device_id)
        rows = self._all(
            "SELECT * FROM commands WHERE device_id=? ORDER BY version DESC"
            " LIMIT ?", (device_id, limit))
        return [self._command_view(r) for r in rows]

    def command_timeline(self, command_id: str) -> dict:
        cmd = self._row("SELECT * FROM commands WHERE command_id=?",
                        (command_id,))
        if cmd is None:
            raise LeaseError(404, "COMMAND_NOT_FOUND", "命令不存在")
        attempts = [dict(r) for r in self._all(
            "SELECT * FROM command_attempts WHERE command_id=?"
            " ORDER BY attempt_no", (command_id,))]
        events = [
            {"at": r["at"], "event_type": r["event_type"],
             "generation": r["generation"], "session_id": r["session_id"],
             "detail": json.loads(r["detail"])}
            for r in self._all(
                "SELECT * FROM command_events WHERE command_id=?"
                " ORDER BY id", (command_id,))]
        return {"command": self._command_view(cmd),
                "attempts": attempts, "events": events}

    def list_rejected(self, device_id: Optional[str] = None,
                      kind: Optional[str] = None,
                      limit: int = 100, offset: int = 0) -> list[dict]:
        sql = "SELECT * FROM rejected_messages WHERE 1=1"
        args: list = []
        if device_id:
            sql += " AND device_id=?"
            args.append(device_id)
        if kind:
            sql += " AND kind=?"
            args.append(kind)
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        args.extend([limit, offset])
        out = []
        for r in self._all(sql, args):
            d = dict(r)
            d["detail"] = json.loads(r["detail"])
            out.append(d)
        return out

    def list_events(self, device_id: Optional[str] = None,
                    limit: int = 100, offset: int = 0) -> list[dict]:
        sql = "SELECT * FROM events WHERE 1=1"
        args: list = []
        if device_id:
            sql += " AND device_id=?"
            args.append(device_id)
        sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
        args.extend([limit, offset])
        out = []
        for r in self._all(sql, args):
            d = dict(r)
            d["detail"] = json.loads(r["detail"])
            out.append(d)
        return out
