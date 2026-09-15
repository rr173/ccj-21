"""dispatcher 服务：命令派发维护循环（独立部署、无状态、可重启）。

周期性调用 core：
  POST /internal/tick            命令 TTL 过期；SENT 后 ACK 超时 -> 退回重试
  POST /internal/sweep-offline   心跳超时标记离线；长时间离线产生预警事件

实际的「按版本取命令并发送」由设备重新上线后的 poll 驱动（见 ingress），
所以 dispatcher 重启不丢命令：所有排队、退避、过期状态都已持久化。
"""
from __future__ import annotations

import logging
import time

from . import client, config

log = logging.getLogger("shadow.dispatcher")


def run(stop=None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    log.info("dispatcher started: tick=%.1fs offline=%ss prolonged=%ss",
             config.DISPATCHER_POLL_INTERVAL,
             config.OFFLINE_AFTER_SECONDS,
             config.PROLONGED_OFFLINE_SECONDS)
    while stop is None or not stop.is_set():
        try:
            tick = client.call("POST", "/internal/tick", {}, timeout=10)
            if tick.get("expired") or tick.get("ack_timeouts"):
                log.info("tick: expired=%s ack_timeouts=%s",
                         tick.get("expired"), tick.get("ack_timeouts"))
            sweep = client.call("POST", "/internal/sweep-offline", {},
                                timeout=10)
            for ch in sweep.get("changes", []):
                log.info("presence: %s %s %s", ch["device_id"],
                         ch["event"],
                         ch.get("reason") or
                         f"offline_for={ch.get('offline_for'):.0f}s"
                         if ch.get("offline_for") else "")
        except Exception:
            log.exception("dispatcher cycle failed; retry next interval")
        time.sleep(config.DISPATCHER_POLL_INTERVAL)


if __name__ == "__main__":
    run()
