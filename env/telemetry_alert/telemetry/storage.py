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
    event_id     TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL,
    metric       TEXT NOT NULL,
    value        REAL NOT NULL,
    event_time   REAL NOT NULL,
    received_at  REAL NOT NULL,
    status       TEXT NOT NULL,          -- accepted | quarantined | no_rule
    reason       TEXT,
    window_start REAL,
    rule_id      TEXT,                   -- 事件按自身事件时间命中的规则版本
    rule_version INTEGER
);
CREATE INDEX IF NOT EXISTS ix_events_window
    ON events(device_id, metric, window_start, rule_id, rule_version)
    WHERE status = 'accepted';
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
    -- 同一规则版本下窗口起点唯一；不同规则版本即使起点相同也是各自的窗口，
    -- 因为版本在窗口中途生效时，新旧版本窗口会并存
    PRIMARY KEY (device_id, metric, rule_id, rule_version, window_start)
);
CREATE INDEX IF NOT EXISTS ix_windows_open
    ON windows(device_id, metric, sealed, window_end);

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
    rule_version    INTEGER NOT NULL DEFAULT 1,  -- 告警判定所绑定的规则版本
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
CREATE INDEX IF NOT EXISTS ix_alerts_version
    ON alert_periods(device_id, rule_id, rule_version, opened_at);

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
            self._migrate(self.conn)
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    @staticmethod
    def _migrate(conn):
        """旧版本库结构升级：事件补记命中的规则版本；窗口主键加入规则版本，
        使版本在窗口中途生效时同起点的新旧窗口可以并存。"""
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "events" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
            if "rule_id" not in cols:
                conn.execute("ALTER TABLE events ADD COLUMN rule_id TEXT")
            if "rule_version" not in cols:
                conn.execute("ALTER TABLE events ADD COLUMN rule_version INTEGER")
            # 旧索引只按 window_start：删掉后由 SCHEMA 按新列重建
            conn.execute("DROP INDEX IF EXISTS ix_events_window")
        if "windows" in tables:
            ddl = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='windows'"
            ).fetchone()[0] or ""
            if "rule_id, rule_version, window_start" not in ddl:
                # 旧主键 (device_id, metric, window_start) 无法容纳同起点、
                # 不同规则版本的并存窗口：重建表并搬移既有窗口
                conn.execute("ALTER TABLE windows RENAME TO windows_legacy_v1")
                conn.execute(
                    """CREATE TABLE windows (
                        device_id    TEXT NOT NULL,
                        metric       TEXT NOT NULL,
                        window_start REAL NOT NULL,
                        window_end   REAL NOT NULL,
                        rule_id      TEXT NOT NULL,
                        rule_version INTEGER NOT NULL,
                        agg_value    REAL,
                        event_count  INTEGER NOT NULL DEFAULT 0,
                        hit          INTEGER,
                        sealed       INTEGER NOT NULL DEFAULT 0,
                        sealed_at    REAL,
                        created_at   REAL NOT NULL,
                        updated_at   REAL NOT NULL,
                        PRIMARY KEY (device_id, metric, rule_id, rule_version,
                                     window_start)
                    )"""
                )
                conn.execute(
                    """INSERT INTO windows(device_id, metric, window_start,
                           window_end, rule_id, rule_version, agg_value,
                           event_count, hit, sealed, sealed_at, created_at,
                           updated_at)
                       SELECT device_id, metric, window_start, window_end,
                              rule_id, rule_version, agg_value, event_count,
                              hit, sealed, sealed_at, created_at, updated_at
                       FROM windows_legacy_v1"""
                )
                conn.execute("DROP TABLE windows_legacy_v1")

        if "alert_periods" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(alert_periods)")}
            if "rule_version" not in cols:
                # 旧告警周期没有版本列：按开窗窗口起点回填其绑定版本；
                # 回填不到（窗口已不在）时退回该规则的最新版本
                conn.execute("ALTER TABLE alert_periods ADD COLUMN rule_version INTEGER")
                conn.execute(
                    """UPDATE alert_periods
                       SET rule_version = COALESCE((
                               SELECT w.rule_version FROM windows w
                               WHERE w.device_id = alert_periods.device_id
                                 AND w.metric = alert_periods.metric
                                 AND w.rule_id = alert_periods.rule_id
                                 AND w.window_start = alert_periods.opened_at
                           ), (
                               SELECT MAX(r.version) FROM rules r
                               WHERE r.rule_id = alert_periods.rule_id
                           ), 1)"""
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS ix_alerts_version "
                    "ON alert_periods(device_id, rule_id, rule_version, opened_at)"
                )
        # 旧事件没有记录命中的规则版本：accepted 事件按其归属的旧窗口回填；
        # 隔离事件按事件时间落在当时生效的版本回填，保证修正/聚合不丢事件
        if "events" in tables:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
            if "rule_id" in cols:
                unbound = conn.execute(
                    "SELECT COUNT(1) AS n FROM events WHERE rule_id IS NULL"
                ).fetchone()["n"]
                if unbound:
                    conn.execute(
                        """UPDATE events
                           SET rule_id = (
                                   SELECT w.rule_id FROM windows w
                                   WHERE w.device_id = events.device_id
                                     AND w.metric = events.metric
                                     AND w.window_start = events.window_start
                               ),
                               rule_version = (
                                   SELECT w.rule_version FROM windows w
                                   WHERE w.device_id = events.device_id
                                     AND w.metric = events.metric
                                     AND w.window_start = events.window_start
                               )
                           WHERE rule_id IS NULL AND status = 'accepted'"""
                    )
                    conn.execute(
                        """UPDATE events
                           SET rule_id = (
                                   SELECT r.rule_id FROM rules r
                                   WHERE r.group_id = (
                                           SELECT d.group_id FROM devices d
                                           WHERE d.device_id = events.device_id
                                       )
                                     AND r.metric = events.metric
                                     AND r.effective_from <= events.event_time
                                     AND (r.effective_to IS NULL
                                          OR r.effective_to > events.event_time)
                                   ORDER BY r.version DESC LIMIT 1
                               ),
                               rule_version = (
                                   SELECT r.version FROM rules r
                                   WHERE r.group_id = (
                                           SELECT d.group_id FROM devices d
                                           WHERE d.device_id = events.device_id
                                       )
                                     AND r.metric = events.metric
                                     AND r.effective_from <= events.event_time
                                     AND (r.effective_to IS NULL
                                          OR r.effective_to > events.event_time)
                                   ORDER BY r.version DESC LIMIT 1
                               )
                           WHERE rule_id IS NULL AND status = 'quarantined'"""
                    )

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
