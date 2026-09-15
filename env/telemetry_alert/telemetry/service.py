"""设备遥测规则与告警核心服务。

语义约定
--------
* 时间一律使用事件时间（设备时间）。每条 (device, metric) 流维护水位线
  watermark = 已接受事件的最大事件时间。
* 滚动窗口 [start, end)。watermark >= window_end 时窗口封存并评估命中；
  watermark >= window_end + allowed_lateness 后窗口封板，之后到达的
  事件只能进入隔离列表，不能改动已封存结果。
* 每个事件按自身事件时间命中当时生效的规则版本：窗口按
  (device, metric, rule_id, rule_version, window_start) 归属。
  新版本在窗口中途生效时，生效后的事件进入新版本窗口，同一起点的新旧
  版本窗口可以并存；已归属旧版本的窗口与事件永远按旧版本解释
  （查询、迟到修正、告警判定均沿用窗口/事件绑定的版本）。
* 告警周期由"已封存窗口序列"确定性重放得出；迟到修正改变窗口结论后
  重新重放并与库中周期对齐，修正前后原因写入 alert_corrections，
  已发出的通知记录永不删除。
"""
from __future__ import annotations

import json
import math
import time
import uuid
from itertools import groupby

from .storage import Database

# ---------------------------------------------------------------- 常量

OPERATORS = {
    "gt": lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
    "eq": lambda a, b: a == b,
    "neq": lambda a, b: a != b,
}
AGGREGATIONS = ("sum", "avg", "min", "max", "count", "last")

EVENT_ACCEPTED = "accepted"
EVENT_DUPLICATE = "duplicate"
EVENT_QUARANTINED = "quarantined"
EVENT_NO_RULE = "no_rule"

NOTIF_OPENED = "opened"
NOTIF_CLOSED = "closed"
NOTIF_CORRECTED = "corrected"

RULE_MUTABLE_FIELDS = (
    "window_size_sec",
    "aggregation",
    "operator",
    "threshold",
    "consecutive_hits",
    "recovery_count",
    "silence_sec",
    "allowed_lateness_sec",
)


class RuleError(ValueError):
    """规则或事件参数非法。"""


class NotFoundError(KeyError):
    """引用的实体不存在。"""


def _uuid() -> str:
    return uuid.uuid4().hex


def _window_start_of(t: float, size: float) -> float:
    # 1e-9 抵消浮点误差，保证边界值落在预期窗口
    return math.floor(t / size + 1e-9) * size


def compute_periods(windows):
    """由已封存窗口序列（按 window_start 升序）确定性推导告警周期。

    windows: [{"window_start", "window_size", "hit", "agg_value",
               "params": {consecutive_hits, recovery_count, silence_sec,
                          operator, threshold, metric}}]
    返回: [{"opened_at", "closed_at", "status", "hit_count",
            "last_hit_window", "open_reason", "close_reason"}]

    缺失的窗口（设备未上报）按非命中处理：打断连续命中，并计入恢复条件。
    """
    periods = []
    consec = 0
    non_hits = 0
    open_p = None
    silence_until = None
    prev = None

    for w in windows:
        p = w["params"]
        if prev is not None:
            size = prev["window_size"]
            gap = int(round((w["window_start"] - (prev["window_start"] + size)) / size))
            # 规则版本切换时并存窗口可能在时间轴上重叠（gap 为负）：
            # 重叠部分没有缺失窗口，直接继续；只有正缺口才补非命中窗口
            gap = max(gap, 0)
            if gap > 0:
                # 逐个逻辑经过缺失窗口；告警关闭后剩余缺口不再影响状态
                k = 0
                while k < gap:
                    k += 1
                    consec = 0
                    non_hits += 1
                    miss_start = prev["window_start"] + size * k
                    pp = prev["params"]
                    if open_p is not None and non_hits >= pp["recovery_count"]:
                        open_p["closed_at"] = miss_start
                        open_p["close_reason"] = (
                            f"recovered: {non_hits} consecutive non-hit windows"
                        )
                        open_p["status"] = "closed"
                        silence_until = miss_start + pp["silence_sec"]
                        open_p = None
                    if open_p is None:
                        break

        if w["hit"]:
            consec += 1
            non_hits = 0
            if open_p is not None:
                open_p["hit_count"] += 1
                open_p["last_hit_window"] = w["window_start"]
            elif consec >= p["consecutive_hits"] and (
                silence_until is None or w["window_start"] >= silence_until
            ):
                open_p = {
                    "opened_at": w["window_start"],
                    "closed_at": None,
                    "status": "open",
                    "hit_count": consec,
                    "last_hit_window": w["window_start"],
                    "open_reason": (
                        f"{consec} consecutive hits: {p['metric']} "
                        f"{p['operator']} {p['threshold']} (agg={w['agg_value']})"
                    ),
                    "close_reason": None,
                }
                periods.append(open_p)
        else:
            consec = 0
            non_hits += 1
            if open_p is not None and non_hits >= p["recovery_count"]:
                open_p["closed_at"] = w["window_start"]
                open_p["close_reason"] = (
                    f"recovered: {non_hits} consecutive non-hit windows"
                )
                open_p["status"] = "closed"
                silence_until = w["window_start"] + p["silence_sec"]
                open_p = None
        prev = w

    return periods


# ---------------------------------------------------------------- 服务

class TelemetryService:
    def __init__(
        self,
        db_path=":memory:",
        *,
        clock=None,
        sender=None,
        ack_timeout_sec=30.0,
        retry_base_sec=1.0,
        retry_max_sec=300.0,
    ):
        self.db = Database(db_path)
        self.clock = clock or time.time
        # sender(notification_dict)；抛异常视为投递失败，进入退避重试
        self.sender = sender or (lambda notification: None)
        self.ack_timeout_sec = float(ack_timeout_sec)
        self.retry_base_sec = float(retry_base_sec)
        self.retry_max_sec = float(retry_max_sec)

    def close(self):
        self.db.close()

    # ------------------------------------------------------------ 规则

    def create_rule(
        self,
        *,
        group_id,
        metric,
        window_size_sec,
        aggregation,
        operator,
        threshold,
        consecutive_hits,
        recovery_count=1,
        silence_sec=0.0,
        allowed_lateness_sec=0.0,
        effective_from=0.0,
        rule_id=None,
    ):
        params = dict(
            window_size_sec=float(window_size_sec),
            aggregation=aggregation,
            operator=operator,
            threshold=float(threshold),
            consecutive_hits=int(consecutive_hits),
            recovery_count=int(recovery_count),
            silence_sec=float(silence_sec),
            allowed_lateness_sec=float(allowed_lateness_sec),
        )
        self._validate_rule_params(params)
        now = self.clock()
        rule_id = rule_id or _uuid()
        with self.db.tx() as conn:
            dup = conn.execute(
                "SELECT 1 FROM rules WHERE group_id=? AND metric=?",
                (group_id, metric),
            ).fetchone()
            if dup:
                raise RuleError(
                    f"rule already exists for group={group_id} metric={metric}; "
                    "use update_rule to create a new version"
                )
            conn.execute(
                """INSERT INTO rules(rule_id, version, group_id, metric,
                   window_size_sec, aggregation, operator, threshold,
                   consecutive_hits, recovery_count, silence_sec,
                   allowed_lateness_sec, effective_from, effective_to, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rule_id, 1, group_id, metric, params["window_size_sec"],
                    params["aggregation"], params["operator"], params["threshold"],
                    params["consecutive_hits"], params["recovery_count"],
                    params["silence_sec"], params["allowed_lateness_sec"],
                    float(effective_from), None, now,
                ),
            )
        return self.get_rule(rule_id)

    def update_rule(self, rule_id, *, effective_from, **changes):
        """创建规则新版本。旧版本生效区间在 effective_from 处截止；
        已归属旧版本的窗口保持原解释不变。"""
        unknown = set(changes) - set(RULE_MUTABLE_FIELDS)
        if unknown:
            raise RuleError(f"cannot update fields: {sorted(unknown)}")
        now = self.clock()
        with self.db.tx() as conn:
            cur = conn.execute(
                "SELECT * FROM rules WHERE rule_id=? ORDER BY version DESC LIMIT 1",
                (rule_id,),
            ).fetchone()
            if cur is None:
                raise NotFoundError(f"rule {rule_id} not found")
            effective_from = float(effective_from)
            if effective_from < cur["effective_from"]:
                raise RuleError(
                    "effective_from must be >= current version effective_from "
                    f"({cur['effective_from']})"
                )
            params = {k: cur[k] for k in RULE_MUTABLE_FIELDS}
            params.update({k: v for k, v in changes.items() if v is not None})
            params["window_size_sec"] = float(params["window_size_sec"])
            params["threshold"] = float(params["threshold"])
            params["consecutive_hits"] = int(params["consecutive_hits"])
            params["recovery_count"] = int(params["recovery_count"])
            params["silence_sec"] = float(params["silence_sec"])
            params["allowed_lateness_sec"] = float(params["allowed_lateness_sec"])
            self._validate_rule_params(params)
            conn.execute(
                "UPDATE rules SET effective_to=? WHERE rule_id=? AND version=?",
                (effective_from, rule_id, cur["version"]),
            )
            conn.execute(
                """INSERT INTO rules(rule_id, version, group_id, metric,
                   window_size_sec, aggregation, operator, threshold,
                   consecutive_hits, recovery_count, silence_sec,
                   allowed_lateness_sec, effective_from, effective_to, created_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rule_id, cur["version"] + 1, cur["group_id"], cur["metric"],
                    params["window_size_sec"], params["aggregation"],
                    params["operator"], params["threshold"],
                    params["consecutive_hits"], params["recovery_count"],
                    params["silence_sec"], params["allowed_lateness_sec"],
                    effective_from, None, now,
                ),
            )
        return self.get_rule(rule_id)

    @staticmethod
    def _validate_rule_params(p):
        if p["aggregation"] not in AGGREGATIONS:
            raise RuleError(f"aggregation must be one of {AGGREGATIONS}")
        if p["operator"] not in OPERATORS:
            raise RuleError(f"operator must be one of {sorted(OPERATORS)}")
        if p["window_size_sec"] <= 0:
            raise RuleError("window_size_sec must be > 0")
        if p["consecutive_hits"] < 1:
            raise RuleError("consecutive_hits must be >= 1")
        if p["recovery_count"] < 1:
            raise RuleError("recovery_count must be >= 1")
        if p["silence_sec"] < 0:
            raise RuleError("silence_sec must be >= 0")
        if p["allowed_lateness_sec"] < 0:
            raise RuleError("allowed_lateness_sec must be >= 0")

    def get_rule(self, rule_id):
        rows = self.db.query(
            "SELECT * FROM rules WHERE rule_id=? ORDER BY version", (rule_id,)
        )
        if not rows:
            raise NotFoundError(f"rule {rule_id} not found")
        return {"rule_id": rule_id, "versions": [dict(r) for r in rows]}

    def list_rules(self, group_id=None, metric=None):
        sql, args = "SELECT * FROM rules WHERE 1=1", []
        if group_id is not None:
            sql += " AND group_id=?"
            args.append(group_id)
        if metric is not None:
            sql += " AND metric=?"
            args.append(metric)
        sql += " ORDER BY group_id, metric, rule_id, version"
        return [dict(r) for r in self.db.query(sql, args)]

    def _rule_for(self, conn, group_id, metric, t):
        """事件时间 t 生效的规则版本。"""
        return conn.execute(
            """SELECT * FROM rules
               WHERE group_id=? AND metric=? AND effective_from<=?
                 AND (effective_to IS NULL OR effective_to>?)
               ORDER BY version DESC LIMIT 1""",
            (group_id, metric, t, t),
        ).fetchone()

    @staticmethod
    def _rule_by_version(conn, rule_id, version):
        return conn.execute(
            "SELECT * FROM rules WHERE rule_id=? AND version=?",
            (rule_id, version),
        ).fetchone()

    # ------------------------------------------------------------ 设备

    def register_device(self, device_id, group_id):
        now = self.clock()
        with self.db.tx() as conn:
            conn.execute(
                """INSERT INTO devices(device_id, group_id, created_at) VALUES(?,?,?)
                   ON CONFLICT(device_id) DO UPDATE SET group_id=excluded.group_id""",
                (device_id, group_id, now),
            )
        return {"device_id": device_id, "group_id": group_id}

    # ------------------------------------------------------------ 摄入

    def ingest(self, events):
        """批量摄入事件。整批在一个事务中处理；返回每个事件的处置结果。"""
        if isinstance(events, dict):
            events = [events]
        if not isinstance(events, list) or not events:
            raise RuleError("events must be a non-empty list")
        for e in events:
            self._validate_event(e)

        now = self.clock()
        results = []
        with self.db.tx() as conn:
            streams = set()       # 需要推进水位线/封存窗口的 (device, metric)
            chains = set()        # 需要重放告警的 (device, metric, rule_id)
            corrections = set()   # 其中由迟到修正触发的链
            for e in events:
                results.append(
                    self._process_event(conn, e, now, streams, chains, corrections)
                )
            for device_id, metric in streams:
                self._seal_due_windows(conn, device_id, metric, now, chains)
            for chain in chains:
                trigger = "correction" if chain in corrections else "seal"
                self._replay_alerts(conn, chain, trigger, now)
        return results

    @staticmethod
    def _validate_event(e):
        required = ("event_id", "device_id", "metric", "value", "event_time")
        missing = [k for k in required if k not in e]
        if missing:
            raise RuleError(f"event missing fields: {missing}")
        try:
            float(e["value"])
            float(e["event_time"])
        except (TypeError, ValueError):
            raise RuleError("value and event_time must be numeric") from None
        if float(e["event_time"]) < 0:
            raise RuleError("event_time must be >= 0")

    def _process_event(self, conn, e, now, streams, chains, corrections):
        event_id = e["event_id"]
        device_id = e["device_id"]
        metric = e["metric"]
        value = float(e["value"])
        event_time = float(e["event_time"])

        # 1) 幂等去重：同一 event_id 只计数一次
        existing = conn.execute(
            "SELECT status FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        if existing is not None:
            return {
                "event_id": event_id,
                "status": EVENT_DUPLICATE,
                "original_status": existing["status"],
            }

        # 2) 设备归属（未注册设备自动归入 default 组，便于审计）
        device = conn.execute(
            "SELECT group_id FROM devices WHERE device_id=?", (device_id,)
        ).fetchone()
        if device is None:
            group_id = "default"
            conn.execute(
                "INSERT INTO devices(device_id, group_id, created_at) VALUES(?,?,?)",
                (device_id, group_id, now),
            )
        else:
            group_id = device["group_id"]

        # 3) 事件按自身事件时间命中当时生效的规则版本
        rule = self._rule_for(conn, group_id, metric, event_time)
        if rule is None:
            conn.execute(
                """INSERT INTO events(event_id, device_id, metric, value, event_time,
                   received_at, status, reason, window_start, rule_id, rule_version)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (event_id, device_id, metric, value, event_time, now,
                 EVENT_NO_RULE, "no effective rule for event time", None, None, None),
            )
            return {"event_id": event_id, "status": EVENT_NO_RULE,
                    "reason": "no effective rule for event time"}

        # 窗口身份包含事件命中的规则版本：版本在窗口中途生效、或窗口长度
        # 变化导致新旧窗口起点相同时，生效后的事件进入自己版本的窗口，
        # 不会被同起点的旧窗口吞掉
        ws = _window_start_of(event_time, rule["window_size_sec"])
        win = self._get_window(
            conn, device_id, metric, ws, rule["rule_id"], rule["version"]
        )
        if win is not None:
            # 窗口已存在：沿用窗口绑定的规则版本，不被后续规则更新重新解释
            rule = self._rule_by_version(conn, win["rule_id"], win["rule_version"])
            ws, we = win["window_start"], win["window_end"]
        else:
            we = ws + rule["window_size_sec"]

        # 4) 超过允许迟到期：只能隔离，不能改动已封存结果（用事件绑定版本
        #    自身的 allowed_lateness 判定）
        wm = self._watermark(conn, device_id, metric)
        if wm >= we + rule["allowed_lateness_sec"]:
            reason = (
                f"beyond allowed lateness: watermark={wm}, window_end={we}, "
                f"allowed_lateness={rule['allowed_lateness_sec']}"
            )
            conn.execute(
                """INSERT INTO events(event_id, device_id, metric, value, event_time,
                   received_at, status, reason, window_start, rule_id, rule_version)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (event_id, device_id, metric, value, event_time, now,
                 EVENT_QUARANTINED, reason, ws, rule["rule_id"], rule["version"]),
            )
            return {"event_id": event_id, "status": EVENT_QUARANTINED,
                    "reason": reason, "window_start": ws}

        # 5) 接受：落库（记住命中的规则版本）、推进水位线、进入窗口
        conn.execute(
            """INSERT INTO events(event_id, device_id, metric, value, event_time,
               received_at, status, reason, window_start, rule_id, rule_version)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (event_id, device_id, metric, value, event_time, now,
             EVENT_ACCEPTED, None, ws, rule["rule_id"], rule["version"]),
        )
        streams.add((device_id, metric))
        conn.execute(
            """INSERT INTO watermarks(device_id, metric, max_event_time) VALUES(?,?,?)
               ON CONFLICT(device_id, metric) DO UPDATE
               SET max_event_time=MAX(max_event_time, excluded.max_event_time)""",
            (device_id, metric, event_time),
        )

        chain = (device_id, metric, rule["rule_id"])
        if win is None:
            sealed = 1 if wm >= we else 0
            conn.execute(
                """INSERT INTO windows(device_id, metric, window_start, window_end,
                   rule_id, rule_version, sealed, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (device_id, metric, ws, we, rule["rule_id"], rule["version"],
                 sealed, now, now),
            )
            self._recalc_window(conn, device_id, metric, ws, rule, now)
            if sealed:
                # 水位线已过但窗口从未有数据：直接封存评估（无旧结论，不算修正）
                self._seal_window(conn, device_id, metric, ws, rule, now)
                chains.add(chain)
        elif not win["sealed"]:
            self._recalc_window(conn, device_id, metric, ws, rule, now)
        else:
            # 6) 水位线之前、仍在允许迟到期内：事件已落库，重算其绑定版本
            #    的窗口并留下修正记录
            old_agg, old_hit = win["agg_value"], win["hit"]
            new_agg, _ = self._recalc_window(conn, device_id, metric, ws, rule, now)
            new_hit = self._hit(rule, new_agg)
            if new_agg != old_agg or new_hit != bool(old_hit or 0):
                conn.execute(
                    """UPDATE windows SET hit=?, updated_at=?
                       WHERE device_id=? AND metric=? AND window_start=?
                         AND rule_id=? AND rule_version=?""",
                    (1 if new_hit else 0, now, device_id, metric, ws,
                     rule["rule_id"], rule["version"]),
                )
                conn.execute(
                    """INSERT INTO window_corrections(device_id, metric, window_start,
                       rule_id, rule_version, old_agg, new_agg, old_hit, new_hit,
                       trigger_event_id, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (device_id, metric, ws, rule["rule_id"], rule["version"],
                     old_agg, new_agg, old_hit, 1 if new_hit else 0,
                     event_id, now),
                )
                chains.add(chain)
                corrections.add(chain)

        return {
            "event_id": event_id,
            "status": EVENT_ACCEPTED,
            "window_start": ws,
            "rule_id": rule["rule_id"],
            "rule_version": rule["version"],
        }

    @staticmethod
    def _watermark(conn, device_id, metric):
        row = conn.execute(
            "SELECT max_event_time FROM watermarks WHERE device_id=? AND metric=?",
            (device_id, metric),
        ).fetchone()
        return row["max_event_time"] if row else float("-inf")

    @staticmethod
    def _get_window(conn, device_id, metric, ws, rule_id, rule_version):
        """按完整窗口身份（含规则版本）取窗口。"""
        return conn.execute(
            """SELECT * FROM windows
               WHERE device_id=? AND metric=? AND window_start=?
                 AND rule_id=? AND rule_version=?""",
            (device_id, metric, ws, rule_id, rule_version),
        ).fetchone()

    def _recalc_window(self, conn, device_id, metric, ws, rule, now):
        """从已接受事件重算窗口聚合值（幂等；迟到修正也走这里）。

        只统计事件时间命中了窗口所绑定规则版本的事件——同起点的新旧版本
        窗口并存时，各版本只聚合自己版本的事件。
        """
        rows = conn.execute(
            """SELECT value, event_time, event_id FROM events
               WHERE device_id=? AND metric=? AND window_start=? AND status='accepted'
                 AND rule_id=? AND rule_version=?""",
            (device_id, metric, ws, rule["rule_id"], rule["version"]),
        ).fetchall()
        agg = rule["aggregation"]
        if not rows:
            agg_value = None
        elif agg == "count":
            agg_value = float(len(rows))
        elif agg == "sum":
            agg_value = sum(r["value"] for r in rows)
        elif agg == "avg":
            agg_value = sum(r["value"] for r in rows) / len(rows)
        elif agg == "min":
            agg_value = min(r["value"] for r in rows)
        elif agg == "max":
            agg_value = max(r["value"] for r in rows)
        elif agg == "last":
            last = max(rows, key=lambda r: (r["event_time"], r["event_id"]))
            agg_value = last["value"]
        else:  # pragma: no cover - 规则校验已拦截
            raise RuleError(f"unknown aggregation {agg}")
        conn.execute(
            """UPDATE windows SET agg_value=?, event_count=?, updated_at=?
               WHERE device_id=? AND metric=? AND window_start=?
                 AND rule_id=? AND rule_version=?""",
            (agg_value, len(rows), now, device_id, metric, ws,
             rule["rule_id"], rule["version"]),
        )
        return agg_value, len(rows)

    @staticmethod
    def _hit(rule, agg_value):
        if agg_value is None:
            return False
        return bool(OPERATORS[rule["operator"]](agg_value, rule["threshold"]))

    def _seal_window(self, conn, device_id, metric, ws, rule, now):
        agg = conn.execute(
            "SELECT agg_value FROM windows WHERE device_id=? AND metric=? "
            "AND window_start=? AND rule_id=? AND rule_version=?",
            (device_id, metric, ws, rule["rule_id"], rule["version"]),
        ).fetchone()["agg_value"]
        conn.execute(
            """UPDATE windows SET sealed=1, sealed_at=?, hit=?, updated_at=?
               WHERE device_id=? AND metric=? AND window_start=?
                 AND rule_id=? AND rule_version=?""",
            (now, 1 if self._hit(rule, agg) else 0, now, device_id, metric, ws,
             rule["rule_id"], rule["version"]),
        )

    def _seal_due_windows(self, conn, device_id, metric, now, chains):
        wm = self._watermark(conn, device_id, metric)
        rows = conn.execute(
            """SELECT * FROM windows WHERE device_id=? AND metric=? AND sealed=0
               AND window_end<=?
               ORDER BY rule_id, rule_version, window_start""",
            (device_id, metric, wm),
        ).fetchall()
        for win in rows:
            rule = self._rule_by_version(conn, win["rule_id"], win["rule_version"])
            self._recalc_window(conn, device_id, metric, win["window_start"], rule, now)
            self._seal_window(conn, device_id, metric, win["window_start"], rule, now)
            chains.add((device_id, metric, win["rule_id"]))

    # ------------------------------------------------------------ 告警

    def _replay_alerts(self, conn, chain, trigger, now):
        device_id, metric, rule_id = chain
        rows = conn.execute(
            """SELECT w.window_start, w.window_end, w.agg_value, w.hit,
                      w.rule_version,
                      r.consecutive_hits, r.recovery_count, r.silence_sec,
                      r.operator, r.threshold, r.metric AS rule_metric
               FROM windows w
               JOIN rules r ON r.rule_id=w.rule_id AND r.version=w.rule_version
               WHERE w.device_id=? AND w.metric=? AND w.rule_id=? AND w.sealed=1
               ORDER BY w.rule_version, w.window_start""",
            (device_id, metric, rule_id),
        ).fetchall()
        # 每个规则版本独立重放：版本在窗口中途生效时并存窗口各算各的，
        # 连续命中/恢复/静默都只采用本版本参数，不跨版本累计
        computed = []
        for version, group in groupby(rows, key=lambda r: r["rule_version"]):
            windows = [
                {
                    "window_start": r["window_start"],
                    "window_size": r["window_end"] - r["window_start"],
                    "hit": bool(r["hit"]),
                    "agg_value": r["agg_value"],
                    "params": {
                        "consecutive_hits": r["consecutive_hits"],
                        "recovery_count": r["recovery_count"],
                        "silence_sec": r["silence_sec"],
                        "operator": r["operator"],
                        "threshold": r["threshold"],
                        "metric": r["rule_metric"],
                    },
                }
                for r in group
            ]
            for p in compute_periods(windows):
                p["rule_version"] = version
                computed.append(p)
        self._apply_periods(conn, chain, computed, trigger, now)

    def _apply_periods(self, conn, chain, computed, trigger, now):
        """把重放出的告警周期与库中周期对齐：相同的保持，新增的开启，
        多余的作废；修正触发时记录修正前后原因并发出 corrected 通知。"""
        device_id, metric, rule_id = chain
        stored = conn.execute(
            "SELECT * FROM alert_periods WHERE device_id=? AND rule_id=? "
            "ORDER BY rule_version, opened_at, rowid",
            (device_id, rule_id),
        ).fetchall()
        # 同一开窗时间、不同规则版本的周期是各自独立的告警
        by_open = {(p["opened_at"], p["rule_version"]): p for p in stored}
        matched = set()

        for c in computed:
            key = (c["opened_at"], c["rule_version"])
            s = by_open.get(key)
            if s is None:
                alert_id = _uuid()
                conn.execute(
                    """INSERT INTO alert_periods(alert_id, device_id, metric, rule_id,
                       rule_version, status, opened_at, closed_at, open_reason,
                       close_reason, hit_count, last_hit_window, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (alert_id, device_id, metric, rule_id, c["rule_version"],
                     c["status"], c["opened_at"], c["closed_at"], c["open_reason"],
                     c["close_reason"], c["hit_count"], c["last_hit_window"],
                     now, now),
                )
                if trigger == "correction":
                    self._alert_correction(
                        conn, alert_id, "opened", "no alert period",
                        c["open_reason"], now,
                    )
                self._enqueue(conn, alert_id, device_id, metric, rule_id,
                              NOTIF_OPENED, c, trigger, now)
                if c["closed_at"] is not None:
                    self._enqueue(conn, alert_id, device_id, metric, rule_id,
                                  NOTIF_CLOSED, c, trigger, now)
                matched.add(alert_id)
                continue

            matched.add(s["alert_id"])
            alert_id = s["alert_id"]
            if s["status"] == "invalidated":
                # 修正后又恢复成立：复用原周期，保留全部历史
                conn.execute(
                    """UPDATE alert_periods SET status=?, closed_at=?, close_reason=?,
                       hit_count=?, last_hit_window=?, updated_at=? WHERE alert_id=?""",
                    (c["status"], c["closed_at"], c["close_reason"], c["hit_count"],
                     c["last_hit_window"], now, alert_id),
                )
                if trigger == "correction":
                    self._alert_correction(
                        conn, alert_id, "revived",
                        f"invalidated (was: {s['open_reason']})",
                        c["open_reason"], now,
                    )
                    self._enqueue_corrected(conn, alert_id, device_id, metric,
                                            rule_id, "revived", c, trigger, now)
                continue

            old_closed, new_closed = s["closed_at"], c["closed_at"]
            close_changed = (old_closed is None) != (new_closed is None) or (
                old_closed is not None
                and new_closed is not None
                and old_closed != new_closed
            )
            if close_changed:
                if trigger == "correction":
                    before = s["close_reason"] or f"open since window {s['opened_at']}"
                    after = c["close_reason"] or "reopened by late-data correction"
                    ctype = "closed" if (new_closed is not None
                                         and old_closed is None) else "updated"
                    self._alert_correction(conn, alert_id, ctype, before, after, now)
                    self._enqueue_corrected(conn, alert_id, device_id, metric,
                                            rule_id, ctype, c, trigger, now,
                                            before=before, after=after)
                elif new_closed is not None and old_closed is None:
                    self._enqueue(conn, alert_id, device_id, metric, rule_id,
                                  NOTIF_CLOSED, c, trigger, now)
                conn.execute(
                    """UPDATE alert_periods SET status=?, closed_at=?, close_reason=?,
                       updated_at=? WHERE alert_id=?""",
                    (c["status"], new_closed, c["close_reason"], now, alert_id),
                )
            # 运行计数静默刷新（后续命中只更新同一周期，不产生新通知）
            if s["hit_count"] != c["hit_count"] or \
                    s["last_hit_window"] != c["last_hit_window"]:
                conn.execute(
                    """UPDATE alert_periods SET hit_count=?, last_hit_window=?,
                       updated_at=? WHERE alert_id=?""",
                    (c["hit_count"], c["last_hit_window"], now, alert_id),
                )

        for s in stored:
            if s["alert_id"] in matched or s["status"] == "invalidated":
                continue
            # 迟到修正后不再成立的周期：作废但保留，通知记录不删除
            conn.execute(
                "UPDATE alert_periods SET status='invalidated', updated_at=? "
                "WHERE alert_id=?",
                (now, s["alert_id"]),
            )
            if trigger == "correction":
                self._alert_correction(
                    conn, s["alert_id"], "invalidated", s["open_reason"],
                    "alert conclusion removed by late-data correction", now,
                )
                self._enqueue_corrected(
                    conn, s["alert_id"], device_id, metric, rule_id, "invalidated",
                    {
                        "opened_at": s["opened_at"],
                        "closed_at": s["closed_at"],
                        "hit_count": s["hit_count"],
                        "open_reason": s["open_reason"],
                        "close_reason": "invalidated by late-data correction",
                        "last_hit_window": s["last_hit_window"],
                        "rule_version": s["rule_version"],
                    },
                    trigger, now,
                    before=s["open_reason"],
                    after="alert conclusion removed by late-data correction",
                )

    @staticmethod
    def _alert_correction(conn, alert_id, ctype, before, after, now):
        conn.execute(
            """INSERT INTO alert_corrections(alert_id, correction_type,
               reason_before, reason_after, created_at) VALUES(?,?,?,?,?)""",
            (alert_id, ctype, before, after, now),
        )

    def _enqueue(self, conn, alert_id, device_id, metric, rule_id, ntype,
                 period, trigger, now, extra=None):
        """通知进入持久化待发队列。opened/closed 对同一周期幂等（唯一索引）。"""
        payload = {
            "alert_id": alert_id,
            "device_id": device_id,
            "rule_id": rule_id,
            "rule_version": period.get("rule_version"),
            "metric": metric,
            "type": ntype,
            "trigger": trigger,
            "opened_at": period.get("opened_at"),
            "closed_at": period.get("closed_at"),
            "hit_count": period.get("hit_count"),
            "open_reason": period.get("open_reason"),
            "close_reason": period.get("close_reason"),
        }
        if extra:
            payload.update(extra)
        conn.execute(
            """INSERT OR IGNORE INTO notifications(notification_id, alert_id,
               device_id, type, payload, status, attempts, next_attempt_at,
               created_at) VALUES(?,?,?,?,?,'pending',0,?,?)""",
            (_uuid(), alert_id, device_id, ntype, json.dumps(payload), now, now),
        )

    def _enqueue_corrected(self, conn, alert_id, device_id, metric, rule_id,
                           ctype, period, trigger, now, before=None, after=None):
        self._enqueue(
            conn, alert_id, device_id, metric, rule_id, NOTIF_CORRECTED, period,
            trigger, now,
            extra={
                "correction_type": ctype,
                "reason_before": before if before is not None
                else period.get("open_reason"),
                "reason_after": after if after is not None
                else (period.get("close_reason") or period.get("open_reason")),
            },
        )

    # ------------------------------------------------------------ 通知投递

    def pump_notifications(self, limit=100):
        """投递到期的通知；失败按指数退避重试；sent 未确认的会在
        ack_timeout 后重投（接收方按 notification_id 去重）。"""
        now = self.clock()
        due = self.db.query(
            """SELECT * FROM notifications
               WHERE status IN ('pending','sent') AND next_attempt_at IS NOT NULL
                 AND next_attempt_at<=?
               ORDER BY created_at LIMIT ?""",
            (now, limit),
        )
        outcomes = []
        for n in due:
            attempt_no = n["attempts"] + 1
            error = None
            try:
                self.sender(dict(n))
                result = "success"
            except Exception as exc:  # noqa: BLE001 - 任何发送失败都进入退避
                result, error = "failure", str(exc)
            with self.db.tx() as conn:
                conn.execute(
                    """INSERT INTO notification_attempts(notification_id, attempt_no,
                       attempted_at, result, error) VALUES(?,?,?,?,?)""",
                    (n["notification_id"], attempt_no, now, result, error),
                )
                if result == "success":
                    if n["status"] == "pending":
                        conn.execute(
                            """UPDATE notifications SET status='sent', attempts=?,
                               sent_at=?, next_attempt_at=? WHERE notification_id=?""",
                            (attempt_no, now, now + self.ack_timeout_sec,
                             n["notification_id"]),
                        )
                    else:
                        conn.execute(
                            """UPDATE notifications SET attempts=?, next_attempt_at=?
                               WHERE notification_id=?""",
                            (attempt_no, now + self.ack_timeout_sec,
                             n["notification_id"]),
                        )
                else:
                    backoff = min(
                        self.retry_max_sec,
                        self.retry_base_sec * (2 ** (attempt_no - 1)),
                    )
                    conn.execute(
                        """UPDATE notifications SET attempts=?, next_attempt_at=?
                           WHERE notification_id=?""",
                        (attempt_no, now + backoff, n["notification_id"]),
                    )
            outcomes.append({
                "notification_id": n["notification_id"],
                "result": result,
                "error": error,
            })
        return {"pumped": len(outcomes), "outcomes": outcomes}

    def ack_notification(self, notification_id, ack_token=None):
        """接收方确认。重复确认幂等：已完成的不做任何事、不报错。"""
        now = self.clock()
        with self.db.tx() as conn:
            n = conn.execute(
                "SELECT * FROM notifications WHERE notification_id=?",
                (notification_id,),
            ).fetchone()
            if n is None:
                raise NotFoundError(f"notification {notification_id} not found")
            if n["status"] == "confirmed":
                return dict(n)
            conn.execute(
                """UPDATE notifications SET status='confirmed', confirmed_at=?,
                   ack_token=?, next_attempt_at=NULL WHERE notification_id=?""",
                (now, ack_token, notification_id),
            )
        return self.get_notification(notification_id)

    # ------------------------------------------------------------ 查询

    def get_event(self, event_id):
        row = self.db.one("SELECT * FROM events WHERE event_id=?", (event_id,))
        if row is None:
            raise NotFoundError(f"event {event_id} not found")
        return dict(row)

    def list_events(self, device_id=None, status=None):
        sql, args = "SELECT * FROM events WHERE 1=1", []
        if device_id is not None:
            sql += " AND device_id=?"
            args.append(device_id)
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY received_at, event_id"
        return [dict(r) for r in self.db.query(sql, args)]

    def list_windows(self, device_id=None, metric=None):
        sql, args = "SELECT * FROM windows WHERE 1=1", []
        if device_id is not None:
            sql += " AND device_id=?"
            args.append(device_id)
        if metric is not None:
            sql += " AND metric=?"
            args.append(metric)
        sql += " ORDER BY device_id, metric, rule_id, rule_version, window_start"
        return [dict(r) for r in self.db.query(sql, args)]

    def list_alerts(self, device_id=None, rule_id=None, status=None):
        sql, args = "SELECT * FROM alert_periods WHERE 1=1", []
        if device_id is not None:
            sql += " AND device_id=?"
            args.append(device_id)
        if rule_id is not None:
            sql += " AND rule_id=?"
            args.append(rule_id)
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY rule_id, rule_version, opened_at, alert_id"
        return [dict(r) for r in self.db.query(sql, args)]

    def list_window_corrections(self, device_id=None, metric=None):
        sql, args = "SELECT * FROM window_corrections WHERE 1=1", []
        if device_id is not None:
            sql += " AND device_id=?"
            args.append(device_id)
        if metric is not None:
            sql += " AND metric=?"
            args.append(metric)
        sql += " ORDER BY id"
        return [dict(r) for r in self.db.query(sql, args)]

    def list_alert_corrections(self, alert_id=None):
        sql, args = "SELECT * FROM alert_corrections WHERE 1=1", []
        if alert_id is not None:
            sql += " AND alert_id=?"
            args.append(alert_id)
        sql += " ORDER BY id"
        return [dict(r) for r in self.db.query(sql, args)]

    def get_notification(self, notification_id):
        row = self.db.one(
            "SELECT * FROM notifications WHERE notification_id=?",
            (notification_id,),
        )
        if row is None:
            raise NotFoundError(f"notification {notification_id} not found")
        return dict(row)

    def list_notifications(self, status=None, alert_id=None):
        sql, args = "SELECT * FROM notifications WHERE 1=1", []
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        if alert_id is not None:
            sql += " AND alert_id=?"
            args.append(alert_id)
        sql += " ORDER BY created_at, notification_id"
        return [dict(r) for r in self.db.query(sql, args)]

    def list_notification_attempts(self, notification_id):
        return [
            dict(r)
            for r in self.db.query(
                "SELECT * FROM notification_attempts WHERE notification_id=? "
                "ORDER BY attempt_no",
                (notification_id,),
            )
        ]

    def get_watermark(self, device_id, metric):
        row = self.db.one(
            "SELECT * FROM watermarks WHERE device_id=? AND metric=?",
            (device_id, metric),
        )
        return dict(row) if row else None
