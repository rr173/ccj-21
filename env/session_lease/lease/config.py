"""配置（全部可用环境变量覆盖）。"""
from __future__ import annotations

import os


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


DB_PATH = os.environ.get("LEASE_DB_PATH", "/data/lease.db")
HTTP_HOST = os.environ.get("LEASE_HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("LEASE_HTTP_PORT", "8080"))
ADMIN_TOKEN = os.environ.get("LEASE_ADMIN_TOKEN", "change-me")
SWEEP_INTERVAL_SECONDS = _env_float("LEASE_SWEEP_INTERVAL_SECONDS", 1.0)

# 设备申请续期时允许的租约上下限（秒）
DEFAULT_LEASE_SECONDS = _env_float("LEASE_DEFAULT_LEASE_SECONDS", 60.0)
MIN_LEASE_SECONDS = _env_float("LEASE_MIN_LEASE_SECONDS", 5.0)
MAX_LEASE_SECONDS = _env_float("LEASE_MAX_LEASE_SECONDS", 3600.0)

# 轮换宽限期默认长度（旧凭证只可续旧会话）
DEFAULT_GRACE_SECONDS = _env_float("LEASE_DEFAULT_GRACE_SECONDS", 300.0)
MIN_GRACE_SECONDS = _env_float("LEASE_MIN_GRACE_SECONDS", 1.0)

# 会话/凭证状态
SE_ACTIVE = "ACTIVE"
SE_SUPERSEDED = "SUPERSEDED"
SE_EXPIRED = "EXPIRED"
SE_REVOKED = "REVOKED"
SESSION_ENDED_STATES = {SE_SUPERSEDED, SE_EXPIRED, SE_REVOKED}

CRED_ACTIVE = "ACTIVE"
CRED_ROTATING = "ROTATING"
CRED_REVOKED = "REVOKED"

ROT_ROTATING = "ROTATING"
ROT_OLD_REVOKED = "OLD_REVOKED"
ROT_COMPLETED = "COMPLETED"

# 命令状态
CMD_QUEUED = "QUEUED"                    # 从未发送 / 对账确认未执行，可派发
CMD_QUEUED_UNKNOWN = "QUEUED_UNKNOWN"    # 会话切换前从未发送，等待新会话对账表态
CMD_SENT = "SENT"
CMD_ACKED = "ACKED"
CMD_RECONCILING = "RECONCILING"
# 尚未确认终态、需要对账参与才能决定去留的状态
CMD_AWAIT_RECONCILE_STATES = (CMD_RECONCILING, CMD_QUEUED_UNKNOWN)

# 接管记录状态
TK_PENDING = "PENDING_RECONNECT"
TK_COMPLETED = "COMPLETED"

# 幂等作用域
IDEM_ONLINE = "online"
IDEM_RENEW = "renew"
IDEM_REPORT = "report"
IDEM_ACK = "ack"
IDEM_RECONCILE = "reconcile"
IDEM_ROTATE = "rotate"
IDEM_REVOKE = "revoke"
IDEM_TAKEOVER = "takeover"
IDEM_COMMAND = "command"
