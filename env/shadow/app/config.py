"""集中配置：所有参数均可通过环境变量覆盖（见 docker-compose.yml）。"""
from __future__ import annotations

import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


# 存储
DB_PATH = os.environ.get("SHADOW_DB_PATH", "/data/shadow.db")
# 内部服务间共享密钥
INTERNAL_TOKEN = os.environ.get("SHADOW_INTERNAL_TOKEN", "internal-dev-token")

# core 控制面
CORE_HTTP_HOST = os.environ.get("CORE_HOST", "0.0.0.0")
CORE_HTTP_PORT = _int("CORE_PORT", 8080)

# ingress 设备接入
INGRESS_HTTP_HOST = os.environ.get("INGRESS_HOST", "0.0.0.0")
INGRESS_HTTP_PORT = _int("INGRESS_PORT", 8081)
CORE_URL = os.environ.get("CORE_URL", "http://core:8080")
# 设备 long-poll 最长挂起秒数
DEVICE_POLL_WAIT = _int("DEVICE_POLL_WAIT", 25)

# dispatcher 派发器
DISPATCHER_POLL_INTERVAL = _float("DISPATCHER_POLL_INTERVAL", 1.0)
# 有设备刚上线时立即补发的宽限时间（秒），避免错过 poll 窗口
DISPATCHER_TICK_BUDGET = _float("DISPATCHER_TICK_BUDGET", 1.0)

# 业务策略
OFFLINE_AFTER_SECONDS = _int("OFFLINE_AFTER_SECONDS", 30)
PROLONGED_OFFLINE_SECONDS = _int("PROLONGED_OFFLINE_SECONDS", 300)
# 命令派发后等待 ACK 的时间，超时重试
ACK_TIMEOUT_SECONDS = _int("ACK_TIMEOUT_SECONDS", 30)
# 命令整体有效期（基于期望版本产生时刻），过期不再发送
COMMAND_TTL_SECONDS = _int("COMMAND_TTL_SECONDS", 900)
# 派发失败最大尝试次数（超过进入 FAILED）
MAX_DELIVERY_ATTEMPTS = _int("MAX_DELIVERY_ATTEMPTS", 5)
# 指数退避基数：base * 2^(attempt-1)
RETRY_BACKOFF_BASE = _float("RETRY_BACKOFF_BASE", 2.0)
RETRY_BACKOFF_MAX = _float("RETRY_BACKOFF_MAX", 60.0)

# 命令状态
ST_QUEUED = "QUEUED"
ST_SENT = "SENT"
ST_ACKED = "ACKED"
ST_RETRYING = "RETRYING"
ST_FAILED = "FAILED"
ST_EXPIRED = "EXPIRED"
ST_SUPERSEDED = "SUPERSEDED"

# 分批发布状态
RL_PENDING = "PENDING"
RL_ACTIVE = "ACTIVE"
RL_PAUSED = "PAUSED"
RL_COMPLETED = "COMPLETED"
RL_ROLLING_BACK = "ROLLING_BACK"
RL_ROLLED_BACK = "ROLLED_BACK"
RL_CANCELLED = "CANCELLED"

RD_WAITING = "WAITING"
RD_ACTIVE = "ACTIVE"
RD_MATCHED = "MATCHED"
RD_SKIPPED = "SKIPPED"
RD_REJECTED = "REJECTED"
RD_FAILED = "FAILED"
RD_EXPIRED = "EXPIRED"
RD_DRIFT = "DRIFT"
RD_ROLLBACK_ACTIVE = "ROLLBACK_ACTIVE"
RD_ROLLED_BACK = "ROLLED_BACK"
RD_ROLLBACK_FAILED = "ROLLBACK_FAILED"

UNRESOLVED_DEVICE_STATUSES = (RD_ACTIVE, RD_ROLLBACK_ACTIVE)
TERMINAL_DEVICE_FAILURE_STATUSES = (
    RD_REJECTED, RD_FAILED, RD_EXPIRED, RD_DRIFT)

ACK_SUCCESS_CODES = {"", "OK", "APPLIED", "ACCEPTED", "SUCCESS"}
