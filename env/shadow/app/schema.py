"""SQLite 建表与索引（启动时幂等执行）。

所有可追溯状态都落库：
- devices / shadows            设备主档与当前影子（期望态 / 报告态）
- commands                     每台设备按期望版本生成的命令
- command_attempts             每次派发尝试（失败重试的完整轨迹）
- events                       审计事件流（重启不丢、全程可追溯）
- idempotent_requests          控制端幂等键去重
- device_groups / group_members 设备组及发布时锁定的成员关系
- releases / release_batches / release_devices 分批发布、批次门禁和设备明细
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
    release_id      TEXT,
    release_phase   TEXT NOT NULL DEFAULT '',           -- FORWARD/ROLLBACK
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

CREATE TABLE IF NOT EXISTS device_groups (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL DEFAULT '',
    priority     INTEGER NOT NULL DEFAULT 100,  -- 数字越小优先级越高
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS group_members (
    group_id     TEXT NOT NULL REFERENCES device_groups(id),
    device_id    TEXT NOT NULL REFERENCES devices(id),
    added_at     REAL NOT NULL,
    PRIMARY KEY(group_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_group_members_device
    ON group_members(device_id);

CREATE TABLE IF NOT EXISTS releases (
    id                    TEXT PRIMARY KEY,
    group_id              TEXT NOT NULL REFERENCES device_groups(id),
    target_state          TEXT NOT NULL,
    batch_percent         REAL NOT NULL,
    confirm_threshold     REAL NOT NULL,
    drift_threshold       REAL NOT NULL DEFAULT 0,
    batch_deadline_seconds REAL NOT NULL,
    status                TEXT NOT NULL,
    -- PENDING/ACTIVE/PAUSED/COMPLETED/ROLLING_BACK/ROLLED_BACK/CANCELLED
    current_batch         INTEGER NOT NULL DEFAULT 0,
    gate_reason           TEXT NOT NULL DEFAULT 'WAITING_CONFLICT',
    pause_reason          TEXT NOT NULL DEFAULT '',
    phase                 TEXT NOT NULL DEFAULT 'FORWARD',
    created_at            REAL NOT NULL,
    created_by            TEXT NOT NULL DEFAULT '',
    started_at            REAL,
    paused_at             REAL,
    completed_at          REAL
);

CREATE INDEX IF NOT EXISTS idx_releases_scheduling
    ON releases(status, created_at);

CREATE TABLE IF NOT EXISTS release_devices (
    release_id       TEXT NOT NULL REFERENCES releases(id),
    device_id        TEXT NOT NULL REFERENCES devices(id),
    batch_no         INTEGER NOT NULL,
    ordinal          INTEGER NOT NULL,
    baseline_version INTEGER NOT NULL,
    baseline_state   TEXT NOT NULL,
    status           TEXT NOT NULL,
    -- WAITING/ACTIVE/MATCHED/SKIPPED/REJECTED/FAILED/EXPIRED/DRIFT/
    -- ROLLBACK_ACTIVE/ROLLED_BACK/ROLLBACK_FAILED
    forward_command_id TEXT,
    rollback_command_id TEXT,
    last_error       TEXT NOT NULL DEFAULT '',
    updated_at       REAL NOT NULL,
    PRIMARY KEY(release_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_release_devices_device
    ON release_devices(device_id, status);
CREATE INDEX IF NOT EXISTS idx_release_devices_batch
    ON release_devices(release_id, batch_no, status);

CREATE TABLE IF NOT EXISTS release_batches (
    release_id       TEXT NOT NULL REFERENCES releases(id),
    batch_no         INTEGER NOT NULL,
    status           TEXT NOT NULL,
    -- WAITING/ACTIVE/GATED/COMPLETED/SKIPPED
    device_count     INTEGER NOT NULL,
    started_at       REAL,
    deadline_at      REAL,
    finished_at      REAL,
    gate_reason      TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(release_id, batch_no)
);

CREATE TABLE IF NOT EXISTS release_operations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id      TEXT NOT NULL REFERENCES releases(id),
    operation       TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    status          TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '{}',
    created_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_ops
    ON release_operations(release_id, id);

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
    release_id  TEXT,
    event_type  TEXT NOT NULL,
    detail      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_device_time
    ON events(device_id, id);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(id);
"""
