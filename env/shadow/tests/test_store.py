"""Store 状态机集成测试：临时 SQLite + 虚拟时钟。

覆盖：版本化命令、离线排队/按版本补发、旧命令取代、报告乱序/重复拒绝、
重复确认幂等、派发失败退避重试/超次失败、TTL 过期、ACK 超时、
离线检测、幂等键、重启恢复、读模型原因分类。
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import config, model
from app.store import Store


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def fresh_store():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    clk = Clock()
    # 虚拟时钟远大于默认 TTL，测试中显式调大，过期用例单独设置
    config.COMMAND_TTL_SECONDS = 200000
    s = Store(tmp.name, clock=clk)
    return s, tmp.name, clk


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print("ok:", msg)


def main():
    s, path, clk = fresh_store()
    s.provision_device("dev1", "lamp", token="secret-token")

    check(s.get_device_by_token("secret-token")["id"] == "dev1",
          "token resolves device")
    check(s.get_device_by_token("bad") is None, "bad token rejected")

    # ---------- 1. 期望态版本与命令生成 ----------
    r1 = s.set_desired("dev1", {"led": "on"})
    check(r1["status"] == "OK" and r1["version"] == 1, "first desired v1")
    r2 = s.set_desired("dev1", {"led": "on", "brightness": 50})
    check(r2["version"] == 2, "second desired v2")
    cmds = s.list_commands("dev1")
    versions = sorted(c["version"] for c in cmds)
    check(versions == [1, 2], "one command per desired version")

    # 期望并发控制 expected_version
    conflict = s.set_desired("dev1", {"x": 1}, expected_version=1)
    check(conflict["status"] == "CONFLICT"
          and conflict["current_version"] == 2,
          "expected_version optimistic conflict")

    # ---------- 2. 幂等键 ----------
    idem1 = s.set_desired("dev1", {"a": 1}, idem_key="key-123")
    idem1b = s.set_desired("dev1", {"a": 999}, idem_key="key-123")
    check(idem1b.get("replayed") and idem1b["version"] == idem1["version"],
          "idempotency key replays same result")

    # ---------- 3. 离线排队：v1/v2 仍 QUEUED，新期望取代旧的未送达命令 ----------
    r3 = s.set_desired("dev1", {"led": "off"})  # v4
    check(r3["version"] == 4, "version now 4")
    statuses = {c["version"]: c["status"]
                for c in s.list_commands("dev1")}
    check(statuses[1] == config.ST_SUPERSEDED
          and statuses[2] == config.ST_SUPERSEDED
          and statuses[3] == config.ST_SUPERSEDED,
          "undelivered QUEUED commands superseded by newer version")

    # ---------- 4. 上线后严格按版本顺序领取 ----------
    s.touch("dev1")
    c = s.claim_next_command("dev1")
    check(c is not None and c["version"] == 4,
          "claim returns oldest deliverable version first")
    check(s.claim_next_command("dev1") is None, "no more due commands")

    # ---------- 5. 重复确认幂等 ----------
    ack1 = s.ack_by_device("dev1", 4, None, "APPLIED", "ok")
    check(ack1["status"] == "ACKED" and ack1["duplicate"] is False,
          "first ack accepted")
    ack2 = s.ack_by_device("dev1", 4, None, "APPLIED", "ok again")
    check(ack2["status"] == "DUPLICATE_ACK" and ack2["duplicate"] is True,
          "duplicate ack detected, command not executed twice")
    ack_by_id = s.ack_by_device("dev1", None, ack1["command_id"], "X", "")
    check(ack_by_id["status"] == "DUPLICATE_ACK",
          "ack by command_id also idempotent")
    # 设备上报与期望内容一致的报告态（报告文档版本独立递增）
    rep = s.report_state("dev1", 1, {"led": "off"})
    check(rep["accepted"] and "implicit_ack" not in rep,
          "state report accepted independently (no implicit ack)")

    # ---------- 6. 报告态乱序/重复拒绝 ----------
    stale = s.report_state("dev1", 1, {"led": "stale"})
    check(not stale["accepted"] and stale["reason"] == "DUPLICATE_VERSION",
          "equal report version rejected (DUPLICATE_VERSION)")
    older = s.report_state("dev1", 0, {"led": "stale"})
    check(not older["accepted"] and older["reason"] == "STALE_VERSION",
          "older report version rejected (STALE_VERSION)")
    fresh = s.report_state("dev1", 2, {"led": "bright"})
    check(fresh["accepted"], "newer report version accepted")

    # 期望 v5：命令正常生成；报告内容落后于期望但报告文档版本可以是任意值
    s.set_desired("dev1", {"led": "warm"})  # v5
    c5 = s.claim_next_command("dev1")
    check(c5["version"] == 5, "claim v5")
    s.ack_by_device("dev1", 5, None, "APPLIED", "")
    # 设备确认后立即上报了不匹配内容（确认之后漂移）-> CONTENT_MISMATCH
    r_bad = s.report_state("dev1", 3, {"led": "warm-x"})
    view = s.shadow_read_model("dev1")
    check(r_bad["accepted"]
          and view["sync"]["reason"] == "CONTENT_MISMATCH",
          "post-ack drift -> CONTENT_MISMATCH")
    s.report_state("dev1", 4, {"led": "warm"})
    check(s.shadow_read_model("dev1")["sync"]["reason"] == "IN_SYNC",
          "matching content -> IN_SYNC regardless of version numbers")

    # ---------- 7. 派发失败 -> 指数退避重试 -> 超次 FAILED ----------
    s.set_desired("dev1", {"color": "red"})  # v6
    cmd = s.claim_next_command("dev1")
    check(cmd is not None and cmd["version"] == 6
          and cmd["attempt_no"] == 1, "claim v6 #1")
    f1 = s.delivery_failed(cmd["id"], "conn reset")
    check(f1["retry"] and f1["status"] == config.ST_RETRYING
          and f1["next_retry_in"] == 2.0,
          "failure #1 -> RETRYING with 2s backoff")
    check(s.claim_next_command("dev1") is None,
          "retrying command not claimed before backoff elapses")
    clk.advance(2.1)
    cmd2 = s.claim_next_command("dev1")
    check(cmd2 is not None and cmd2["attempt_no"] == 2,
          "retry after backoff, attempt #2")
    for i in range(2, config.MAX_DELIVERY_ATTEMPTS + 1):
        fr = s.delivery_failed(cmd["id"], f"err{i}")
        if i < config.MAX_DELIVERY_ATTEMPTS:
            clk.advance(model.next_backoff_delay(i) + 0.1)
            s.claim_next_command("dev1")
        else:
            check(fr["status"] == config.ST_FAILED and not fr["retry"],
                  "exhausted attempts -> FAILED")
    check(len(s.list_attempts(cmd["id"])) == config.MAX_DELIVERY_ATTEMPTS,
          "every delivery attempt persisted for traceability")
    view = s.shadow_read_model("dev1")
    check(view["sync"]["reason"] == "FAILED_DISPATCH",
          "read model reports FAILED_DISPATCH")

    # 顺序保证：旧版本 SENT 在途时，新版本 QUEUED 不能先发
    s.provision_device("dev3", "motor", token="tok3")
    s.touch("dev3")
    s.set_desired("dev3", {"pos": 1})  # v1
    c3a = s.claim_next_command("dev3")
    s.set_desired("dev3", {"pos": 2})  # v2，v1 仍 SENT
    check(s.claim_next_command("dev3") is None,
          "newer QUEUED blocked while older command still SENT")
    clk.advance(config.ACK_TIMEOUT_SECONDS + 1)
    s.tick()  # v1 -> RETRYING
    c3b = s.claim_next_command("dev3")
    check(c3b["version"] == 1 and c3b["attempt_no"] == 2,
          "older version redelivered first after timeout")
    s.ack_by_device("dev3", 1, None, "OK", "")
    s.report_state("dev3", 1, {"pos": 1})
    c3c = s.claim_next_command("dev3")
    check(c3c["version"] == 2, "then v2 delivered after v1 settled")

    # ---------- 8. ACK 超时：SENT 后无确认 -> 退回 RETRYING ----------
    s.set_desired("dev1", {"color": "blue"})  # v7
    c7 = s.claim_next_command("dev1")
    check(s.tick()["ack_timeouts"] == 0, "no timeout right after send")
    clk.advance(config.ACK_TIMEOUT_SECONDS + 1)
    check(s.tick()["ack_timeouts"] >= 1,
          "sent command times out waiting ack")
    st7 = {c["version"]: c["status"]
           for c in s.list_commands("dev1")}[7]
    check(st7 == config.ST_RETRYING, "timed-out command back to RETRYING")

    # ---------- 9. TTL 过期 ----------
    s.provision_device("dev2", "fan", token="tok2")
    s.touch("dev2")
    config.COMMAND_TTL_SECONDS = 60
    s.set_desired("dev2", {"speed": 3})                       # v1 送达
    c_d2 = s.claim_next_command("dev2")
    s.ack_by_device("dev2", 1, None, "OK", "")
    s.report_state("dev2", 1, {"speed": 3})
    s.set_desired("dev2", {"speed": 9})                       # v2 不领取
    clk.advance(config.COMMAND_TTL_SECONDS + 1)
    check(s.tick()["expired"] >= 1, "commands past TTL expire")
    row = [c for c in s.list_commands("dev2") if c["version"] == 2][0]
    check(row["status"] == config.ST_EXPIRED, "stale queued cmd EXPIRED")
    config.COMMAND_TTL_SECONDS = 200000

    # ---------- 10. 离线检测 / 长时间离线 ----------
    s.touch("dev2")
    check(s.get_device_by_id("dev2")["online"] == 1, "heartbeat -> online")
    clk.advance(config.OFFLINE_AFTER_SECONDS + 1)
    changes = s.sweep_offline()
    ev = [c for c in changes if c["device_id"] == "dev2"
          and c["event"] == "DEVICE_OFFLINE"]
    check(len(ev) == 1 and ev[0]["reason"] == "HEARTBEAT_TIMEOUT",
          "missed heartbeat -> offline with traceable reason")
    clk.advance(config.PROLONGED_OFFLINE_SECONDS)
    changes2 = s.sweep_offline()
    check(any(c["event"] == "DEVICE_OFFLINE_PROLONGED"
              for c in changes2 if c["device_id"] == "dev2"),
          "prolonged offline alert once")
    s.touch("dev2")
    clk.advance(config.OFFLINE_AFTER_SECONDS + config.PROLONGED_OFFLINE_SECONDS
                + 1)
    changes3 = s.sweep_offline()
    check(any(c["event"] == "DEVICE_OFFLINE_PROLONGED"
              for c in changes3 if c["device_id"] == "dev2"),
          "prolonged alert can fire again after reconnect cycle")

    # ---------- 11. 读模型：下一条待发 + 最后确认 + 原因 ----------
    view = s.shadow_read_model("dev1")
    check(view["last_acknowledged"] is not None
          and view["last_acknowledged"]["version"] == 5,
          "read model exposes last acknowledged command")
    v2view = s.shadow_read_model("dev2")
    check(v2view["sync"]["reason"] == "EXPIRED"
          and v2view["next_pending_command"]["status"]
          == config.ST_EXPIRED,
          "mismatch reason EXPIRED distinguishable")

    # ---------- 12. 重启恢复 ----------
    s.close()
    s2 = Store(path, clock=clk)
    v = s2.shadow_read_model("dev1")
    check(v["desired"]["version"] == 7,
          "desired shadow survives service restart")
    cmds_after = {c["version"]: c["status"]
                  for c in s2.list_commands("dev1")}
    check(cmds_after[6] == config.ST_FAILED
          and cmds_after[4] == config.ST_ACKED,
          "command lifecycle state survives restart")
    events = s2.list_events(limit=1000)
    types = {e["event_type"] for e in events}
    for needed in ["DESIRED_UPDATED", "COMMAND_ENQUEUED", "COMMAND_SENT",
                   "COMMAND_ACKED", "ACK_DUPLICATE", "REPORT_REJECTED",
                   "COMMAND_RETRY", "COMMAND_FAILED", "COMMAND_EXPIRED",
                   "COMMAND_ACK_TIMEOUT", "COMMAND_SUPERSEDED",
                   "DEVICE_OFFLINE", "DEVICE_ONLINE",
                   "DEVICE_OFFLINE_PROLONGED"]:
        check(needed in types, f"audit event recorded: {needed}")

    # 重启后派发可继续：dev2 设 v3，上线领取
    s2.set_desired("dev2", {"speed": 5})
    s2.touch("dev2")
    cc = s2.claim_next_command("dev2")
    check(cc is not None and cc["version"] == 3,
          "dispatching resumes after restart, version order intact")
    s2.close()

    print("\nALL STORE TESTS PASSED")


if __name__ == "__main__":
    main()
