"""分批发布状态机测试：临时 SQLite + 虚拟时钟。"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config
from app.store import Store


class Clock:
    def __init__(self):
        self.t = 2_000_000.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def store():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    s = Store(tmp.name, clock=Clock())
    for i in range(1, 6):
        s.provision_device(f"d{i}", f"device {i}", token=f"tok{i}")
        s.touch(f"d{i}")
    s.upsert_group("g-all", "all", priority=50,
                   device_ids=[f"d{i}" for i in range(1, 6)])
    s.upsert_group("g-low", "low", priority=100,
                   device_ids=["d1", "d2"])
    return s


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print("ok:", msg)


def apply_device(s, release_id, device_id, target, report_version,
                 code="APPLIED"):
    cmd = s.claim_next_command(device_id)
    check(cmd is not None, f"{device_id} receives release command")
    s.ack_by_device(device_id, cmd["version"], None, code, "")
    if code == "APPLIED":
        accepted = s.report_state(device_id, report_version, target)
        check(accepted["accepted"], f"{device_id} matching report accepted")
    return cmd


def main():
    s = store()

    # 1. 固定快照、比例切分；首个发布立即启动第一批。
    r = s.create_release(
        "g-all", {"mode": "v2"}, batch_percent=40,
        batch_deadline_seconds=60, confirm_threshold=50)
    rid = r["release_id"]
    tick = s.tick_releases()
    check(rid in tick["activated"], "first release activated")
    batch1 = s.list_release_devices(rid, batch_no=1)
    check(len(batch1) == 2 and {d["device_id"] for d in batch1} == {"d1", "d2"},
          "first batch is fixed snapshot subset by 40%")

    # 2. 后创建但同组设备命中时，按组优先级和创建顺序阻塞，并给出 blocker。
    low = s.create_release("g-low", {"mode": "low"}, batch_percent=100,
                           batch_deadline_seconds=60)
    lid = low["release_id"]
    s.tick_releases()
    blocked = s.release_read_model(lid)
    check(blocked["status"] == config.RL_PENDING
          and blocked["gate_reason"] == "BLOCKED_BY_RELEASE",
          "later release waits")
    check(blocked["conflicts"][0]["blocked_by_release_id"] == rid,
          "waiting release shows blocking release and device")

    # 3. 达到确认率后进入下一批；尾部未完成设备仍保留锁。
    apply_device(s, rid, "d1", {"mode": "v2"}, 1)
    s.tick_releases()
    view = s.release_read_model(rid)
    check(view["current_batch"] == 1
          and view["current_gate"]["reason"] == "WAITING_TAIL_AFTER_GATE",
          "50% gate reached but tail remains in flight")
    apply_device(s, rid, "d2", {"mode": "v2"}, 1)
    s.tick_releases()
    view = s.release_read_model(rid)
    check(view["current_batch"] == 2
          and len(s.list_release_devices(rid, batch_no=2)) == 2,
          "whole batch confirmed then next batch starts")

    # 4. 设备拒绝自动暂停。
    apply_device(s, rid, "d3", {"mode": "v2"}, 1, code="REJECTED")
    s.tick_releases()
    view = s.release_read_model(rid)
    check(view["status"] == config.RL_PAUSED
          and view["pause_reason"] == config.RD_REJECTED,
          "device rejection auto-pauses release")

    # 暂停、跳过、继续幂等；继续生成新版本，不覆盖历史。
    pause1 = s.pause_release(rid)
    pause2 = s.pause_release(rid, idem_key="pause-1")
    pause3 = s.pause_release(rid, idem_key="pause-1")
    check(pause1["idempotent"] and pause3.get("replayed"),
          "pause is idempotent")
    before_versions = {d["command_version"]
                       for d in s.list_release_devices(rid, batch_no=2)}
    cont1 = s.continue_release(rid)
    cont2 = s.continue_release(rid)
    check(cont1["regenerated_devices"] == ["d3"]
          and cont2.get("idempotent") is True
          and cont2["regenerated_devices"] == [],
          "continue retries failed device and is idempotent")
    after = [d for d in s.list_release_devices(rid, batch_no=2)
             if d["device_id"] == "d3"][0]
    check(after["command_version"] not in before_versions,
          "continue creates a new per-device command version")

    # 5. 跳过失败设备后，批次继续推进。
    check(s.release_read_model(rid)["status"] == config.RL_ACTIVE,
          "continue moves release back to active")
    # 让新命令处于重试失败终态。
    cmd = s.claim_next_command("d3")
    import time
    for _ in range(config.MAX_DELIVERY_ATTEMPTS):
        s.delivery_failed(cmd["id"], "reset")
        s.clock.advance(120)
        s.claim_next_command("d3")
    s.tick_releases()
    skip1 = s.skip_failed_devices(rid)
    skip2 = s.skip_failed_devices(rid)
    check("d3" in skip1["skipped_devices"]
          and skip2.get("idempotent") is True,
          "skip failed devices is idempotent")
    s.continue_release(rid)
    apply_device(s, rid, "d4", {"mode": "v2"}, 1)
    s.tick_releases()
    check(s.release_read_model(rid)["current_batch"] == 3,
          "remaining device confirms and skipping failure advances final batch")

    # 完成剩余设备。
    apply_device(s, rid, "d5", {"mode": "v2"}, 1)
    s.tick_releases()
    view = s.release_read_model(rid)
    check(view["status"] == config.RL_COMPLETED
          and view["counts"][config.RD_SKIPPED] == 1,
          "release completes with skipped failure retained")

    # 6. 原发布完成后，被阻塞发布可以启动。
    s.tick_releases()
    check(s.release_read_model(lid)["status"] == config.RL_ACTIVE,
          "previously blocked release starts after lock release")

    # 7. 超时自动暂停。
    timeout = s.create_release("g-low", {"mode": "timeout"},
                               batch_percent=100,
                               batch_deadline_seconds=30)
    tid = timeout["release_id"]
    blocked = s.release_read_model(tid)
    check(blocked["status"] == config.RL_PENDING,
          "new release blocked by earlier lower-priority active release")
    rb = s.rollback_release(lid)
    check(rb["status"] == config.RL_ROLLING_BACK and rb["batch_no"] == 1,
          "rollback starts")
    rb2 = s.rollback_release(lid, idem_key="rb1")
    rb3 = s.rollback_release(lid, idem_key="rb1")
    check(rb3.get("replayed"), "rollback start is idempotent")
    for dev, ver in (("d1", 2), ("d2", 2)):
        cmd = s.claim_next_command(dev)
        s.ack_by_device(dev, cmd["version"], None, "APPLIED", "")
        s.report_state(dev, ver, {"mode": "v2"})
    s._evaluate_release_unlocked(lid, s.clock())
    check(s.release_read_model(lid)["status"] == config.RL_ROLLED_BACK,
          "rollback completes to fixed per-device snapshots")
    check(s.shadow_read_model("d1")["desired"]["version"] >= 3 and
          s.shadow_read_model("d1")["desired"]["state"] == {"mode": "v2"},
          "rollback appends versions and restores fixed snapshot")

    # 8. 状态漂移超过阈值自动暂停。
    s.tick_releases()
    check(s.release_read_model(tid)["status"] == config.RL_ACTIVE,
          "timeout release starts after rollback releases locks")
    cmd = s.claim_next_command("d1")
    s.ack_by_device("d1", cmd["version"], None, "APPLIED", "")
    s.report_state("d1", 2, {"mode": "different"})
    s.tick_releases()
    view = s.release_read_model(tid)
    check(view["status"] == config.RL_PAUSED
          and view["pause_reason"] == config.RD_DRIFT,
          "state drift beyond threshold auto-pauses")

    # 9. 重启后从原批次继续，不重复推进/回滚。
    path = s.db_path
    now = s.clock()
    s.close()
    s = Store(path, clock=lambda: now)
    view = s.release_read_model(tid)
    check(view["status"] == config.RL_PAUSED and view["current_batch"] == 1,
          "paused batch position survives restart")
    commands_before = len(s.list_commands("d1"))
    s.tick_releases()
    check(len(s.list_commands("d1")) == commands_before,
          "restart/tick does not duplicate commands or advance paused release")
    events = s.list_release_events(tid)
    check(any(e["event_type"] == "RELEASE_PAUSED" for e in events)
          and any(e["event_type"] == "RELEASE_DEVICE_BATCH_STARTED"
                  for e in events),
          "complete release timeline is queryable")
    s.close()
    print("\nALL RELEASE TESTS PASSED")


if __name__ == "__main__":
    main()
