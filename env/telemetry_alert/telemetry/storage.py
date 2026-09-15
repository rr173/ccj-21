"""SQLite 持久化层：schema 定义与事务助手。

所有核心状态（规则版本、事件、水位线、窗口、修正记录、告警周期、
通知待发队列与投递尝试）都保存在 SQLite 中，服务重启后可直接恢复。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS rules (
    rule_id              TEXT NOT NULL,
    version              INTEGER NOT NULL,
    group_id             TEXT NOT NULL,
    metric               TEXT NOT NULL,
    window_size_sec      REAL NOT NULL,
    aggregation          TEXT NOT NULL,
    operator             TEXT NOT NULL,
    threshold            REAL NOT NULL,
    consecutive_hits     INTEGER NOT NULL,
    recovery_count       INTEGER NOT NULL,
    silence_sec          REAL NOT NULL DEFAULT 0,
    allowed_lateness_sec REAL NOT NULL DEFAULT 0,
    effective_from       REAL NOT NULL,
    effective_to         REAL,
    created_at           REAL NOT NULL,
    PRIMARY KEY (rule_id, version)
);
CREATE INDEX IF NOT EXISTS ix_rules_group_metric ON rules(group_id, metric);

CREATE TABLE IF NOT EXISTS devices (
    device_id  TEXT PRIMARY KEY,
    group_id   TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    device_id   TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value       REAL NOT NULL,
    event_time  REAL NOT NULL,
    received_at REAL NOT NULL,
    status      TEXT NOT NULL,          -- accepted | quarantined | no_rule
    reason      TEXT,
    window_start REAL
);
CREATE INDEX IF NOT EXISTS ix_events_window
    ON events(device_id, metric, window_start) WHERE status = 'accepted';
CREATE INDEX IF NOT EXISTS ix_events_status ON events(status);

CREATE TABLE IF NOT EXISTS watermarks (
    device_id      TEXT NOT NULL,
    metric         TEXT NOT NULL,
    max_event_time REAL NOT NULL,
    PRIMARY KEY (device_id, metric)
);

CREATE TABLE IF NOT EXISTS windows (
    device_id    TEXT NOT NULL,
    metric       TEXT NOT NULL,
    window_start REAL NOT NULL,
    window_end   REAL NOT NULL,
    rule_id      TEXT NOT NULL,
    rule_version INTEGER NOT NULL,
    agg_value    REAL,
    event_count  INTEGER NOT NULL DEFAULT 0,
    hit          INTEGER,               -- NULL = 未评估(未封存); 0/1 = 封存后的结论
    sealed       INTEGER NOT NULL DEFAULT 0,
    sealed_at    REAL,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    PRIMARY KEY (device_id, metric, window_start)
);

CREATE TABLE IF NOT EXISTS window_corrections (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id        TEXT NOT NULL,
    metric           TEXT NOT NULL,
    window_start     REAL NOT NULL,
    rule_id          TEXT NOT NULL,
    rule_version     INTEGER NOT NULL,
    old_agg          REAL,
    new_agg          REAL,
    old_hit          INTEGER,
    new_hit          INTEGER,
    trigger_event_id TEXT NOT NULL,
    created_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_wcorr ON window_corrections(device_id, metric);

CREATE TABLE IF NOT EXISTS alert_periods (
    alert_id        TEXT PRIMARY KEY,
    device_id       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    rule_id         TEXT NOT NULL,
    status          TEXT NOT NULL,      -- open | closed | invalidated
    opened_at       REAL NOT NULL,      -- 触发告警的窗口起点(事件时间)
    closed_at       REAL,
    open_reason     TEXT NOT NULL,
    close_reason    TEXT,
    hit_count       INTEGER NOT NULL,
    last_hit_window REAL,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_alerts_device ON alert_periods(device_id, rule_id);

CREATE TABLE IF NOT EXISTS alert_corrections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id        TEXT NOT NULL,
    correction_type TEXT NOT NULL,      -- opened | closed | updated | invalidated | revived
    reason_before   TEXT NOT NULL,
    reason_after    TEXT NOT NULL,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_acorr ON alert_corrections(alert_id);

CREATE TABLE IF NOT EXISTS notifications (
    notification_id TEXT PRIMARY KEY,
    alert_id        TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    type            TEXT NOT NULL,      -- opened | closed | corrected
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,      -- pending | sent | confirmed
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL,
    ack_token       TEXT,
    created_at      REAL NOT NULL,
    sent_at         REAL,
    confirmed_at    REAL
);
-- 同一告警周期的 opened/closed 通知只入队一次，保证重启/重放不重复投递
CREATE UNIQUE INDEX IF NOT EXISTS ux_notifications_once
    ON notifications(alert_id, type) WHERE type IN ('opened', 'closed');
CREATE INDEX IF NOT EXISTS ix_notifications_status ON notifications(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS notification_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    notification_id TEXT NOT NULL,
    attempt_no      INTEGER NOT NULL,
    attempted_at    REAL NOT NULL,
    result          TEXT NOT NULL,      -- success | failure
    error           TEXT
);
CREATE INDEX IF NOT EXISTS ix_attempts ON notification_attempts(notification_id);
"""


class Database:
    """对 sqlite3 连接的轻量封装：行字典化 + 显式事务。"""

    def __init__(self, path: str = ":memory:"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        with self._lock:
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    @contextmanager
    def tx(self):
        """事务上下文：正常结束提交，异常回滚。"""
        with self._lock:
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def query(self, sql, args=()):
        with self._lock:
            return self.conn.execute(sql, args).fetchall()

    def one(self, sql, args=()):
        rows = self.query(sql, args)
        return rows[0] if rows else None

    def close(self):
        with self._lock:
            self.conn.close()
