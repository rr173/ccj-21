"""SQLite 建表与索引（启动时幂等执行）。

所有可追溯状态都落库：
- devices / shadows            设备主档与当前影子（期望态 / 报告态）
- commands                     每台设备按期望版本生成的命令
- command_attempts             每次派发尝试（失败重试的完整轨迹）
- events                       审计事件流（重启不丢、全程可追溯）
- idempotent_requests          控制端幂等键去重
"""
from __future__ import annotations

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS devices (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL DEFAULT '',
    token         TEXT NOT NULL UNIQUE,
    registered_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS shadows (
    device_id        TEXT PRIMARY KEY REFERENCES devices(id),
    -- 期望态：控制端设置的目标
    desired          TEXT NOT NULL DEFAULT '{}',     -- JSON 对象
    desired_version  INTEGER NOT NULL DEFAULT 0,
    desired_updated_at REAL,
    -- 报告态：设备最近一次上报（仅接受新版本，旧版本被拒绝）
    reported         TEXT NOT NULL DEFAULT '{}',
    reported_version INTEGER NOT NULL DEFAULT 0,
    reported_updated_at REAL,
    -- 在线状态（最近一次心跳由 ingress 上报给 core）
    online           INTEGER NOT NULL DEFAULT 0,
    last_seen_at     REAL,
    online_changed_at REAL,
    prolonged_notified INTEGER NOT NULL DEFAULT 0
);

-- 一条命令对应一次「期望态版本推进」；同一设备版本号严格递增
CREATE TABLE IF NOT EXISTS commands (
    id              TEXT PRIMARY KEY,
    device_id       TEXT NOT NULL REFERENCES devices(id),
    version         INTEGER NOT NULL,
    desired         TEXT NOT NULL,                 -- 该版本的期望快照
    status          TEXT NOT NULL,
    created_at      REAL NOT NULL,
    expires_at      REAL NOT NULL,
    claimed_at      REAL,
    delivered_at    REAL,
    acked_at        REAL,
    last_attempt_at REAL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT NOT NULL DEFAULT '',
    ack_code        TEXT NOT NULL DEFAULT '',
    ack_message     TEXT NOT NULL DEFAULT '',
    UNIQUE(device_id, version)
);

CREATE INDEX IF NOT EXISTS idx_commands_dispatch
    ON commands(device_id, status, version);

CREATE TABLE IF NOT EXISTS command_attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    command_id  TEXT NOT NULL REFERENCES commands(id),
    attempt_no  INTEGER NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL,
    outcome     TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING/FAILED/ACKED
    error       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS idempotent_requests (
    idem_key    TEXT PRIMARY KEY,
    response    TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    at          REAL NOT NULL,
    device_id   TEXT,
    command_id  TEXT,
    event_type  TEXT NOT NULL,
    detail      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_device_time
    ON events(device_id, id);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(id);
"""
