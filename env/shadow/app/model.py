"""领域纯逻辑：不一致原因分类、退避计算、状态判定。

版本模型（经典设备影子语义）
============================
* desired_version：控制端每修改一次期望态 +1；每条命令绑定一个期望版本。
* reported_version：报告态文档的单调版本号，由设备自管理（每次上报 +1），
  与期望版本相互独立，不做数值比较。乱序/重复按 reported_version 拒绝。
* 设备是否「追上」期望，用期望态与报告态的内容是否相等判断，
  再结合当前未决命令的状态给出精确原因。

「不一致原因」必须能明确区分，而不是只给一个 true/false：

   IN_SYNC                 期望与报告内容一致（设备已追上目标）
   CONTENT_MISMATCH        没有未决命令但报告内容仍与期望不同
                           （执行偏差 / 局部应用 / 设备自发改变）
   PENDING_DELIVERY        报告还没追上，下一条命令排队中（设备在线，尚未补发）
   IN_FLIGHT               命令已下发但还没收到确认
   RETRYING                命令正在失败退避重试
   FAILED_DISPATCH         命令重试次数耗尽，派发失败
   EXPIRED                 命令超过 TTL 过期
   SUPERSEDED              旧版本命令在送达前被更新期望版本取代
   ACKED_NOT_REPORTED      设备已确认命令，但报告内容还不匹配期望
   DEVICE_OFFLINE          命令排队中且设备当前离线
   NEVER_DESIRED           还没有任何期望态
   NEVER_REPORTED          控制端已设期望，但设备从未上报过任何状态
"""
from __future__ import annotations

import json
from typing import Any, Optional

from . import config

# ---- 命令生命周期 ----
# QUEUED -> SENT -> ACKED
# SENT/RETRYING 在派发失败或 ACK 超时时: RETRYING(退避后重试) -> FAILED
# 未送达命令在新期望产生时 -> SUPERSEDED
# 命令超过 expires_at -> EXPIRED
TERMINAL_STATUSES = {config.ST_ACKED, config.ST_FAILED,
                     config.ST_EXPIRED, config.ST_SUPERSEDED}


def parse_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None


def canonical(obj: Any) -> Any:
    """把 JSON 对象规整成可比较的形式（按键排序、忽略空白差异）。"""
    if isinstance(obj, dict):
        return {k: canonical(obj[k]) for k in sorted(obj)}
    if isinstance(obj, list):
        return [canonical(v) for v in obj]
    return obj


def states_equal(a_json: str, b_json: str) -> bool:
    a, b = parse_json(a_json), parse_json(b_json)
    return canonical(a) == canonical(b)


def flatten_state(obj: Any, prefix: str = "") -> dict[str, Any]:
    """把 JSON 状态展开成可比较的叶子路径。

    列表按位置作为路径的一部分；缺失字段和值变化都计入漂移。
    """
    out: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key in sorted(obj):
            path = f"{prefix}.{key}" if prefix else str(key)
            out.update(flatten_state(obj[key], path))
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            out.update(flatten_state(value, f"{prefix}[{i}]"))
    else:
        out[prefix or "$"] = obj
    return out


def drift_ratio(target: Any, reported: Any) -> float:
    """返回报告相对发布目标的叶子字段漂移比例（0 表示完全匹配）。"""
    a = flatten_state(canonical(target))
    b = flatten_state(canonical(reported))
    paths = set(a) | set(b)
    if not paths:
        return 0.0
    diff = sum(1 for path in paths if a.get(path) != b.get(path))
    return diff / len(paths)


def state_matches(target: Any, reported: Any, drift_threshold: float) -> bool:
    """报告是否满足发布门禁；threshold 是允许的最大叶子漂移比例。"""
    if canonical(target) == canonical(reported):
        return True
    return drift_threshold > 0 and drift_ratio(target, reported) <= drift_threshold


def split_batches(device_ids: list[str], percent: float) -> list[list[str]]:
    """按固定比例切分创建时锁定的设备快照。"""
    if percent <= 0:
        raise ValueError("batch_percent must be > 0")
    ordered = sorted(device_ids)
    total = len(ordered)
    if total == 0:
        return []
    batches: list[list[str]] = []
    taken = 0
    while taken < total:
        if percent >= 100 or not batches:
            size = max(1, round(total * min(percent, 100.0) / 100.0)) if percent < 100 else total
        else:
            size = round(total * min(percent, 100.0) / 100.0)
        size = min(size, total - taken)
        # 最后一批兜底；极小设备数/比例下每批至少一台。
        if not batches and percent < 100:
            size = max(1, size)
        if size <= 0:
            size = 1
        batches.append(ordered[taken:taken + size])
        taken += size
        if percent >= 100:
            break
    return batches


def next_backoff_delay(attempt_no: int,
                       base: float = config.RETRY_BACKOFF_BASE,
                       cap: float = config.RETRY_BACKOFF_MAX) -> float:
    """第 attempt_no 次失败后，下一次重试的退避秒数（指数退避）。"""
    if attempt_no <= 0:
        return 0.0
    return min(cap, base * (2 ** (attempt_no - 1)))


def classify(
    *,
    desired_version: int,
    reported_version: int,
    desired_json: str,
    reported_json: str,
    online: bool,
    open_command_status: Optional[str],
    ever_reported: bool,
    acked_at: Optional[float] = None,
    reported_updated_at: Optional[float] = None,
) -> tuple[bool, str]:
    """根据影子与当前未决命令状态判定 (是否一致, 原因码)。

    open_command_status：内容尚未一致时，最高的一条命令状态；没有则 None。
    desired/reported 版本号各自独立，不比较大小。
    """
    if desired_version == 0:
        return True, "NEVER_DESIRED"

    if ever_reported and states_equal(desired_json, reported_json):
        return True, "IN_SYNC"

    # 内容不一致：设备还没（完全）追上期望
    if not ever_reported:
        return False, "NEVER_REPORTED"

    if open_command_status is None:
        return False, "CONTENT_MISMATCH"

    # 最新命令已 ACK 但内容不匹配：
    # 报告比确认更新 -> 设备确认后又漂移（CONTENT_MISMATCH）；
    # 否则 -> 已确认但尚未补报匹配内容（ACKED_NOT_REPORTED）。
    if open_command_status == config.ST_ACKED:
        if acked_at is not None and reported_updated_at is not None \
                and reported_updated_at >= acked_at:
            return False, "CONTENT_MISMATCH"
        return False, "ACKED_NOT_REPORTED"

    mapping = {
        config.ST_QUEUED:
            "DEVICE_OFFLINE" if not online else "PENDING_DELIVERY",
        config.ST_SENT: "IN_FLIGHT",
        config.ST_RETRYING: "RETRYING",
        config.ST_FAILED: "FAILED_DISPATCH",
        config.ST_EXPIRED: "EXPIRED",
        config.ST_SUPERSEDED: "SUPERSEDED",
    }
    return False, mapping.get(open_command_status, "PENDING_DELIVERY")
