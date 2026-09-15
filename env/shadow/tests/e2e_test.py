"""端到端测试：真实启动 core / ingress / dispatcher 三个独立进程。

覆盖：注册鉴权、离线排队、上线按版本补发、重复确认幂等、乱序报告拒绝、
派发连接失败退避重试至 FAILED、ACK 超时重发、TTL 过期、服务重启恢复、
控制面读模型（下一条待发 / 最后确认 / 不一致原因）。

报告态版本由设备端独立计数（与期望/命令版本无关），模拟真实影子语义。
"""
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CORE_PORT = 18080
INGRESS_PORT = 18081
CORE = f"http://127.0.0.1:{CORE_PORT}"
INGRESS = f"http://127.0.0.1:{INGRESS_PORT}"

BASE_ENV = {
    "CORE_PORT": str(CORE_PORT),
    "INGRESS_PORT": str(INGRESS_PORT),
    "CORE_HOST": "127.0.0.1",
    "INGRESS_HOST": "127.0.0.1",
    "CORE_URL": CORE,
    "SHADOW_INTERNAL_TOKEN": "e2e-token",
    "DEVICE_POLL_WAIT": "3",
    "DISPATCHER_POLL_INTERVAL": "0.3",
    "OFFLINE_AFTER_SECONDS": "2",
    "PROLONGED_OFFLINE_SECONDS": "4",
    "ACK_TIMEOUT_SECONDS": "3",
    "COMMAND_TTL_SECONDS": "12",
    "MAX_DELIVERY_ATTEMPTS": "3",
    "RETRY_BACKOFF_BASE": "1",
    "RETRY_BACKOFF_MAX": "8",
    "PYTHONUNBUFFERED": "1",
    "PATH": os.environ.get("PATH", ""),
}

procs = []
logdir = None


def start(service, dbpath):
    env = dict(BASE_ENV)
    env["SHADOW_DB_PATH"] = dbpath
    logf = open(os.path.join(logdir, f"{service}.log"), "w")
    p = subprocess.Popen(
        [sys.executable, "-m", f"app.{service}"],
        cwd=ROOT, env=env, stdout=logf, stderr=subprocess.STDOUT)
    procs.append((p, logf))
    return p


def wait_http(url, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                r.read()
                return True
        except Exception:
            time.sleep(0.2)
    return False


def http(method, url, payload=None, token=None, timeout=35):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Device-Token"] = token
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return r.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print("ok:", msg)


def register(dev_id):
    st, body = http("POST", CORE + "/v1/devices",
                    {"id": dev_id, "name": dev_id})
    check(st in (200, 201), f"{dev_id} registered")
    return body["token"]


def shadow(dev_id):
    return http("GET", f"{CORE}/v1/devices/{dev_id}")[1]


def set_desired(dev_id, state, **extra):
    return http("PUT", f"{CORE}/v1/devices/{dev_id}/desired",
                {"state": state, **extra})


def commands(dev_id):
    return http("GET", f"{CORE}/v1/devices/{dev_id}/commands")[1]["commands"]


def poll(token, timeout=8):
    return http("POST", INGRESS + "/devices/commands/poll", {},
                token=token, timeout=timeout)


def ack(token, **kw):
    return http("POST", INGRESS + "/devices/ack", kw, token=token, timeout=10)


def report(token, version, state):
    return http("POST", INGRESS + "/devices/reported",
                {"version": version, "state": state},
                token=token, timeout=10)


def poll_until_command(token, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st, body = poll(token, timeout=8)
        if st == 200 and body.get("command"):
            return body["command"]
        time.sleep(0.2)
    raise AssertionError("command not received in time")


def drop_once(token):
    """发 poll 请求后 RST 连接，迫使 ingress 记录一次派发失败。"""
    s = socket.create_connection(("127.0.0.1", INGRESS_PORT), timeout=5)
    payload = b"{}"
    s.sendall(
        b"POST /devices/commands/poll HTTP/1.1\r\nHost: dev\r\n"
        b"Content-Type: application/json\r\n"
        + f"X-Device-Token: {token}\r\n".encode()
        + f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
        .encode() + payload)
    time.sleep(1.5)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                 b"\x01\x00\x00\x00\x00\x00\x00\x00")
    s.close()


def stop_all(which=None):
    global procs
    targets = procs if which is None else \
        [(p, lf) for (p, lf) in procs if p in which]
    for p, lf in targets:
        p.send_signal(signal.SIGTERM)
    for p, lf in targets:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
        lf.close()
    stopped = {id(p) for (p, _) in targets}
    procs = [(p, lf) for (p, lf) in procs if id(p) not in stopped]


def main():
    global logdir, procs
    logdir = tempfile.mkdtemp(prefix="shadow-e2e-")
    datadir = tempfile.mkdtemp(prefix="shadow-data-")
    dbpath = os.path.join(datadir, "shadow.db")
    print("logs:", logdir, "db:", dbpath)

    core = start("core", dbpath)
    start("ingress", dbpath)
    start("dispatcher", dbpath)
    check(wait_http(CORE + "/healthz"), "core up")
    check(wait_http(INGRESS + "/healthz"), "ingress up")

    # ---- 鉴权：坏 token 被拒 ----
    st, _ = http("POST", INGRESS + "/devices/commands/poll", {},
                 token="wrong", timeout=5)
    check(st == 401, "ingress rejects bad device token")

    # ---- 场景 1：离线排队 + 上线按版本补发 + 正常收敛 ----
    # 设备端自己维护报告文档版本计数器 rv
    t1 = register("lamp")
    rv = 0
    st, _ = set_desired("lamp", {"led": "on", "brightness": 80})
    check(st == 200, "desired set while device offline -> command queued")
    time.sleep(2.5)  # dispatcher 判定离线
    v = shadow("lamp")
    check(v["online"] is False and v["sync"]["reason"] == "NEVER_REPORTED",
          "offline device reason NEVER_REPORTED")
    check(v["next_pending_command"]
          and v["next_pending_command"]["status"] == "QUEUED",
          "control plane sees next queued command")

    cmd = poll_until_command(t1)   # 设备上线（第一次 poll）
    check(cmd["version"] == 1 and cmd["desired"]["led"] == "on",
          "reconnect: queued command delivered in version order")
    st, a1 = ack(t1, command_id=cmd["id"], version=1, code="APPLIED")
    check(st == 200 and a1["status"] == "ACKED", "first ack accepted")

    # ---- 场景 2：重复确认不能执行两次 ----
    st, a2 = ack(t1, command_id=cmd["id"], version=1, code="APPLIED")
    check(st == 200 and a2.get("duplicate") is True,
          "duplicate ack is idempotent (not executed twice)")

    rv += 1
    st, r1 = report(t1, rv, {"led": "on", "brightness": 80})
    check(st == 200 and r1["accepted"], "reported doc v1 accepted")
    v = shadow("lamp")
    check(v["sync"]["in_sync"] and v["sync"]["reason"] == "IN_SYNC",
          "shadow IN_SYNC after report content matches")
    check(v["last_acknowledged"]["version"] == 1
          and v["last_acknowledged"]["code"] == "APPLIED",
          "control plane sees last acknowledgement")
    check(v["next_pending_command"] is None, "no pending command when synced")

    # ---- 场景 3：乱序 / 重复报告被拒（报告版本独立计数） ----
    # rv 当前为 1（上次收敛上报）；再发一版同内容，用于制造重复/乱序
    rv += 1
    st, fresh = report(t1, rv, {"led": "on", "brightness": 80})
    check(fresh["accepted"], "spontaneous report accepted")
    st, stale = report(t1, rv - 1, {"led": "old"})
    check(st == 409 and stale["reason"] == "STALE_VERSION",
          "out-of-order (older) report rejected")
    st, dup = report(t1, rv, {"led": "warm", "brightness": 80})
    check(st == 409 and dup["reason"] == "DUPLICATE_VERSION",
          "duplicate same-version report rejected")
    # 报告内容与期望完全一致 => IN_SYNC（设备自发上报同内容）
    check(shadow("lamp")["sync"]["reason"] == "IN_SYNC",
          "equal content -> IN_SYNC")

    # 设备再自发改变成与期望不同的内容，且尚无新命令 -> CONTENT_MISMATCH
    rv += 1
    report(t1, rv, {"led": "warm"})
    v = shadow("lamp")
    check(v["sync"]["reason"] == "CONTENT_MISMATCH",
          "device drift with no open command -> CONTENT_MISMATCH")

    # 下发期望 v2：差异原因切换为 PENDING_DELIVERY，命令送达后 IN_FLIGHT
    set_desired("lamp", {"led": "blue"})
    v = shadow("lamp")
    check(v["sync"]["reason"] == "DEVICE_OFFLINE"
          or v["sync"]["reason"] == "PENDING_DELIVERY",
          "new desired while behind -> queued reason")
    cmd2 = poll_until_command(t1)
    check(cmd2["version"] == 2, "desired v2 command delivered")
    ack(t1, command_id=cmd2["id"], version=2)
    v = shadow("lamp")
    check(v["sync"]["reason"] == "ACKED_NOT_REPORTED",
          "acked but no matching report yet -> ACKED_NOT_REPORTED")
    rv += 1
    report(t1, rv, {"led": "red"})  # 确认之后设备漂移
    v = shadow("lamp")
    check(v["sync"]["reason"] == "CONTENT_MISMATCH",
          "post-ack drift -> CONTENT_MISMATCH")
    # 设备随后纠正上报，内容收敛
    rv += 1
    report(t1, rv, {"led": "blue"})
    check(shadow("lamp")["sync"]["reason"] == "IN_SYNC",
          "corrected report -> IN_SYNC")

    # ---- 场景 4：在线继续按版本顺序派发 ----
    set_desired("lamp", {"led": "c4"})  # v3
    c3 = poll_until_command(t1)
    check(c3["version"] == 3, "v3 delivered while online")
    ack(t1, command_id=c3["id"], version=3)
    rv += 1
    report(t1, rv, {"led": "c4"})

    # ---- 场景 5：ACK 超时后重发，第二次确认成功 ----
    set_desired("lamp", {"led": "noack"})  # v4
    c4 = poll_until_command(t1)
    check(c4["version"] == 4 and c4["attempt_no"] == 1, "v4 sent #1")
    deadline = time.time() + 12
    while time.time() < deadline:
        if {c["version"]: c["status"]
                for c in commands("lamp")}.get(4) == "RETRYING":
            break
        time.sleep(0.3)
    check({c["version"]: c["status"]
           for c in commands("lamp")}.get(4) == "RETRYING",
          "no-ack -> command returns to RETRYING after timeout")
    c4b = poll_until_command(t1)
    check(c4b["id"] == c4["id"] and c4b["attempt_no"] == 2,
          "same command redelivered as attempt #2 after ACK timeout")
    ack(t1, command_id=c4b["id"], version=4)
    rv += 1
    report(t1, rv, {"led": "noack"})
    check(shadow("lamp")["sync"]["reason"] == "IN_SYNC",
          "recovered after retry+ack")

    # ---- 场景 6：派发连接失败 -> 退避重试 -> 超次 FAILED ----
    t6 = register("fan")
    poll(t6)  # 上线建立基线
    report(t6, 1, {"speed": 0})
    time.sleep(2.5)  # 等 dispatcher 判离线，确保命令进入排队而非提前领取
    set_desired("fan", {"speed": 5})   # 命令 v1：QUEUED
    # 设备每次重连 poll，ingress 领取命令后写响应时连接已 RST
    for _ in range(8):
        if {c["version"]: c["status"]
                for c in commands("fan")}.get(1) in (
                "FAILED", "EXPIRED"):
            break
        drop_once(t6)
        time.sleep(2.2)  # > 退避(1s,2s)，确保下次 poll 命令到期
    final = {c["version"]: c["status"]
             for c in commands("fan")}.get(1)
    check(final in ("FAILED", "EXPIRED"),
          "delivery failures exhaust retries or TTL")
    failed_cmd = [c for c in commands("fan") if c["version"] == 1][0]
    check(failed_cmd["attempts"] >= 1 and failed_cmd["last_error"],
          "failed command records attempt count and last error")
    attempts = http("GET", CORE + f"/v1/commands/{failed_cmd['id']}/attempts")
    check(len(attempts[1]["attempts"]) == failed_cmd["attempts"]
          and all(a["outcome"] == "FAILED"
                  for a in attempts[1]["attempts"]),
          "every failed delivery attempt is traceable")
    v = shadow("fan")
    check(v["sync"]["reason"] in ("FAILED_DISPATCH", "EXPIRED")
          and v["next_pending_command"]["status"] in ("FAILED", "EXPIRED"),
          "mismatch reason FAILED_DISPATCH/EXPIRED distinguishable")

    # ---- 场景 7：命令 TTL 过期 ----
    t7 = register("lock")
    poll(t7)
    report(t7, 1, {"locked": False})  # 基线上报
    set_desired("lock", {"locked": True})  # v1 命令，设备不再 poll
    deadline = time.time() + 16
    while time.time() < deadline:
        if {c["version"]: c["status"]
                for c in commands("lock")}.get(1) == "EXPIRED":
            break
        time.sleep(0.5)
    check({c["version"]: c["status"]
           for c in commands("lock")}.get(1) == "EXPIRED",
          "unacked command past TTL -> EXPIRED")
    check(shadow("lock")["sync"]["reason"] == "EXPIRED",
          "mismatch reason EXPIRED distinguishable")

    # ---- 场景 8：审计事件（重试轨迹必须完整） ----
    ev = http("GET", CORE + "/v1/events?device_id=fan&limit=50")[1]["events"]
    kinds = {e["event_type"] for e in ev}
    check({"COMMAND_SENT", "COMMAND_RETRY"} <= kinds,
          "audit trail contains sent/retry events")
    check("COMMAND_FAILED" in kinds or "COMMAND_EXPIRED" in kinds,
          "audit trail records terminal failure reason")

    # ---- 场景 9：core 重启后状态完整保留，补发继续 ----
    t9 = register("blind")
    rv9 = 0
    set_desired("blind", {"pos": "open"})
    stop_all(which=[core])
    time.sleep(1)
    start("core", dbpath)
    check(wait_http(CORE + "/healthz"), "core restarted")
    v = shadow("blind")
    check(v["desired"]["version"] == 1
          and v["next_pending_command"]["status"] == "QUEUED",
          "queued command survives core restart")
    cmd = poll_until_command(t9)
    check(cmd["version"] == 1 and cmd["desired"]["pos"] == "open",
          "delivery resumes after core restart")
    ack(t9, command_id=cmd["id"], version=1)
    rv9 += 1
    report(t9, rv9, {"pos": "open"})
    check(shadow("blind")["sync"]["reason"] == "IN_SYNC",
          "converges after restart")

    # ---- 场景 10：ingress 重启不影响设备恢复 ----
    ing = next(p for (p, _) in procs if p.args[2] == "app.ingress")
    stop_all(which=[ing])
    time.sleep(1)
    start("ingress", dbpath)
    check(wait_http(INGRESS + "/healthz"), "ingress restarted")
    set_desired("blind", {"pos": "closed"})
    cmd = poll_until_command(t9)
    check(cmd["version"] == 2, "delivery works after ingress restart")
    ack(t9, command_id=cmd["id"], version=2)
    rv9 += 1
    report(t9, rv9, {"pos": "closed"})

    # ---- 场景 11：设备离线排队，重连后按版本补发 ----
    time.sleep(2.5)  # 等 dispatcher 判离线
    set_desired("blind", {"pos": "tilt"})
    cmd = poll_until_command(t9)
    check(cmd["version"] == 3, "offline-queued v3 delivered on reconnect")
    ack(t9, command_id=cmd["id"], version=3)
    rv9 += 1
    report(t9, rv9, {"pos": "tilt"})
    check(shadow("blind")["sync"]["reason"] == "IN_SYNC",
          "converges after reconnect catchup")

    # ---- 场景 12：期望乐观锁（expected_version） ----
    st, body = set_desired("blind", {"pos": "x"}, expected_version=1)
    check(st == 409 and body["current_version"] == 3,
          "expected_version conflict rejected with current version")
    st, body = set_desired("blind", {"pos": "x"}, expected_version=3)
    check(st == 200 and body["version"] == 4,
          "expected_version match proceeds")

    stop_all()
    print("\nALL E2E TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\nE2E FAILED — service logs in", logdir)
        for name in ("core", "ingress", "dispatcher"):
            p = os.path.join(logdir, f"{name}.log")
            if os.path.exists(p):
                print(f"\n===== {name}.log (tail) =====")
                print("".join(open(p).readlines()[-30:]))
        stop_all()
        raise
