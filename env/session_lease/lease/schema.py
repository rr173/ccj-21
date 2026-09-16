"""会话租约 / 凭证轮换 / 命令对账系统的 SQLite schema。

所有关键状态（当前会话与代次、租约截止时间、凭证与轮换进度、
接管记录、被拒绝的旧会话消息、命令在会话切换前后的派发与对账过程）
都持久化在 SQLite 中，服务重启后直接恢复，不依赖内存状态。
"""

SCHEMA = """
-- 设备：记录当前可写会话指针、单调会话代次上限、单调命令版本号
CREATE TABLE IF NOT EXISTS devices (
    device_id             TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    current_session_id    TEXT,
    current_generation    INTEGER NOT NULL DEFAULT 0,
    next_command_version  INTEGER NOT NULL DEFAULT 0,
    registered_at         REAL NOT NULL
);

-- 凭证：同一设备可有多个版本；轮换期间旧版本 ROTATING（只允许续旧会话），
-- 宽限期到期/管理员撤销后 REVOKED；新版本始终 ACTIVE，新连接必须用它
CREATE TABLE IF NOT EXISTS credentials (
    device_id      TEXT NOT NULL,
    version        INTEGER NOT NULL,
    secret         TEXT NOT NULL,
    status         TEXT NOT NULL,          -- ACTIVE | ROTATING | REVOKED
    created_at     REAL NOT NULL,
    revoked_at     REAL,
    revoke_reason  TEXT,                   -- GRACE_EXPIRED | ADMIN_REVOKED | SUPERSEDED_MIGRATION
    rotation_id    TEXT,
    PRIMARY KEY (device_id, version)
);

-- 凭证轮换进度（重启后原样恢复）
CREATE TABLE IF NOT EXISTS rotations (
    rotation_id       TEXT PRIMARY KEY,
    device_id         TEXT NOT NULL,
    old_version       INTEGER NOT NULL,
    new_version       INTEGER NOT NULL,
    state             TEXT NOT NULL,       -- ROTATING | OLD_REVOKED | COMPLETED
    started_at        REAL NOT NULL,
    grace_deadline    REAL NOT NULL,
    revoked_at        REAL,
    revoke_reason     TEXT,
    new_session_id    TEXT,
    completed_at      REAL
);
CREATE INDEX IF NOT EXISTS ix_rotations_device ON rotations(device_id, started_at);

-- 会话：同一设备 (device_id, generation) 单调唯一；连接编号用于幂等重放，
-- 关闭过的连接编号永远不能再开（失效连接不复活）
CREATE TABLE IF NOT EXISTS sessions (
    session_id                TEXT PRIMARY KEY,
    device_id                 TEXT NOT NULL,
    generation                INTEGER NOT NULL,
    connection_no             TEXT NOT NULL,
    credential_version        INTEGER NOT NULL,
    state                     TEXT NOT NULL,  -- ACTIVE | SUPERSEDED | EXPIRED | REVOKED
    opened_at                 REAL NOT NULL,
    lease_seconds             REAL NOT NULL,
    lease_expires_at          REAL NOT NULL,
    last_renewed_at           REAL NOT NULL,
    ended_at                  REAL,
    end_reason                TEXT,
    superseded_by_session_id  TEXT,
    takeover_id               TEXT,
    UNIQUE (device_id, generation),
    UNIQUE (device_id, connection_no)
);
CREATE INDEX IF NOT EXISTS ix_sessions_device ON sessions(device_id, opened_at);

-- 接管记录：控制端发起时旧会话失效、等待设备用新连接重连；
-- 重连产生新一代会话后回填 new_session_id
CREATE TABLE IF NOT EXISTS takeovers (
    takeover_id    TEXT PRIMARY KEY,
    device_id      TEXT NOT NULL,
    old_session_id TEXT,
    old_generation INTEGER,
    new_session_id TEXT,
    new_generation INTEGER,
    reason         TEXT NOT NULL,
    state          TEXT NOT NULL,           -- PENDING_RECONNECT | COMPLETED
    created_at     REAL NOT NULL,
    completed_at   REAL,
    idem_key       TEXT NOT NULL,
    UNIQUE (device_id, idem_key)
);
CREATE INDEX IF NOT EXISTS ix_takeovers_device ON takeovers(device_id, created_at);

-- 命令：设备内版本单调；每条命令的当前派发代次记录在案
-- QUEUED -> SENT -> ACKED（终态）；会话失效时 SENT -> RECONCILING，
-- 对账判定设备已执行 -> ACKED(RECONCILE_DONE)，未执行 -> 回到 QUEUED 重投
CREATE TABLE IF NOT EXISTS commands (
    command_id                   TEXT PRIMARY KEY,
    device_id                    TEXT NOT NULL,
    version                      INTEGER NOT NULL,
    payload                      TEXT NOT NULL,
    state                        TEXT NOT NULL,
    created_at                   REAL NOT NULL,
    sent_at                      REAL,
    acked_at                     REAL,
    dispatch_generation          INTEGER,
    ack_code                     TEXT,
    ack_result                   TEXT,
    ack_source                   TEXT,       -- ACK | RECONCILE_DONE
    idem_key                     TEXT,
    UNIQUE (device_id, version),
    UNIQUE (device_id, idem_key)
);
CREATE INDEX IF NOT EXISTS ix_commands_device ON commands(device_id, version);

-- 每次派发尝试（代次/会话绑定，跨会话切换可追溯）
CREATE TABLE IF NOT EXISTS command_attempts (
    command_id     TEXT NOT NULL,
    attempt_no     INTEGER NOT NULL,
    session_id     TEXT NOT NULL,
    generation     INTEGER NOT NULL,
    dispatched_at  REAL NOT NULL,
    PRIMARY KEY (command_id, attempt_no)
);

-- 命令时间线：创建/派发/确认/在途丢失/对账完成或重投，全部留痕
CREATE TABLE IF NOT EXISTS command_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    at          REAL NOT NULL,
    command_id  TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    generation  INTEGER,
    session_id  TEXT,
    detail      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_command_events_cmd ON command_events(command_id, id);

-- 设备报告态：带设备自管理版本与产生它的会话代次（旧会话不能覆盖新会话状态）
CREATE TABLE IF NOT EXISTS reported_states (
    device_id     TEXT PRIMARY KEY,
    state_version INTEGER NOT NULL,
    doc           TEXT NOT NULL,
    updated_at    REAL NOT NULL,
    generation    INTEGER NOT NULL,
    session_id    TEXT NOT NULL
);

-- 被拒绝的旧会话消息（状态上报/命令确认/续期/上线/对账，全部带原因）
CREATE TABLE IF NOT EXISTS rejected_messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    at            REAL NOT NULL,
    device_id     TEXT,
    session_id    TEXT,
    generation    INTEGER,
    connection_no TEXT,
    kind          TEXT NOT NULL,
    reason        TEXT NOT NULL,
    detail        TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_rejected_device ON rejected_messages(device_id, id);

-- 幂等请求账本：上线/续期/接管/轮换/撤销/下发命令/对账
CREATE TABLE IF NOT EXISTS idempotency (
    scope        TEXT NOT NULL,
    idem_key     TEXT NOT NULL,
    fingerprint  TEXT NOT NULL,
    created_at   REAL NOT NULL,
    response     TEXT NOT NULL,
    PRIMARY KEY (scope, idem_key)
);

-- 全局审计事件流（会话/凭证/接管生命周期）
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    at         REAL NOT NULL,
    device_id  TEXT,
    session_id TEXT,
    event_type TEXT NOT NULL,
    detail     TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_events_device ON events(device_id, id);
"""
