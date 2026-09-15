"""设备模拟器：连接 ingress，演示各种场景。

通用参数：
  --ingress-url   ingress 地址（默认 http://localhost:8081）
  --token         设备令牌（注册时返回）
  --scenario      场景：
                    normal      正常收命令 -> 确认 -> 上报匹配版本报告态
                    noack       收到命令但不确认（验证 ACK 超时->重试）
                    dupack      每条命令确认两次（验证重复确认幂等）
                    drop        在服务器下发命令时断开连接（验证派发失败重试）
                    outoforder  故意先上报旧版本再上报新版本（验证乱序拒绝）

也可被控制脚本按步骤驱动（配合 scripts/demo.sh）。
"""
from __future__ import annotations

import argparse
import json
import socket
import time
import urllib.error
import urllib.request


def req(url: str, token: str, payload: dict, timeout: float = 35.0,
        raw_drop: bool = False):
    data = json.dumps(payload).encode("utf-8")
    r = urllib.request.Request(url, data=data, method="POST", headers={
        "Content-Type": "application/json",
        "X-Device-Token": token})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")
    except (urllib.error.URLError, socket.timeout, ConnectionResetError,
            BrokenPipeError, OSError) as e:
        return "IOERR", {"error": str(e)}


def post(base: str, token: str, path: str, payload: dict,
         timeout: float = 35.0):
    return req(base + path, token, payload, timeout)


def loop(base: str, token: str, scenario: str, max_cmds: int = 100,
         stop_after: float = 600) -> None:
    deadline = time.time() + stop_after
    handled = 0
    report_doc_version = 0  # 设备自己的报告文档版本（与期望/命令版本独立）
    print(f"[sim] scenario={scenario} connecting {base}")
    while handled < max_cmds and time.time() < deadline:
        if scenario == "drop":
            # 用底层 socket 建立连接、发完请求立刻 RST，模拟设备掉线
            status, body = _drop_request(base, token)
            print("[sim] drop poll ->", status, body)
            time.sleep(3)
            continue

        status, body = post(base, token, "/devices/commands/poll", {},
                            timeout=32)
        if status == 204 or (isinstance(body, dict)
                             and not body.get("command")):
            print("[sim] poll: no command")
            continue
        if status != 200:
            print("[sim] poll error:", status, body)
            time.sleep(2)
            continue

        cmd = body["command"]
        v = cmd["version"]
        print(f"[sim] RECEIVED cmd v{v} attempt={cmd['attempt_no']} "
              f"desired={cmd['desired']}")

        if scenario == "noack":
            print(f"[sim] deliberately NOT acking v{v}; waiting timeout")
            handled += 1
            continue

        # ACK（dupack 场景确认两次）
        times = 2 if scenario == "dupack" else 1
        for i in range(times):
            st, ack = post(base, token, "/devices/ack",
                           {"command_id": cmd["id"], "version": v,
                            "code": "APPLIED",
                            "message": f"applied v{v}"}, timeout=10)
            print(f"[sim] ACK v{v} #{i+1} -> {st} {ack}")

        if scenario == "outoforder":
            # 故意乱序：先报一个旧文档版本（被拒绝），再报新版本
            report_doc_version += 1
            cur_rv = report_doc_version
            st, rej = post(base, token, "/devices/reported",
                           {"version": cur_rv - 1,
                            "state": {"led": "old"}}, timeout=10)
            print(f"[sim] report stale doc v{cur_rv-1} -> {st} {rej}")
            st, ok = post(base, token, "/devices/reported",
                          {"version": cur_rv,
                           "state": dict(cmd["desired"])}, timeout=10)
            print(f"[sim] report fresh doc v{cur_rv} -> {st} {ok}")
        else:
            report_doc_version += 1
            st, rep = post(base, token, "/devices/reported",
                           {"version": report_doc_version,
                            "state": dict(cmd["desired"])}, timeout=10)
            print(f"[sim] report doc v{report_doc_version} -> {st} {rep}")
        handled += 1
    print(f"[sim] done, handled {handled} commands")


def _drop_request(base: str, token: str):
    """建立 TCP 连接发完 HTTP 请求后立即 close(RST)，服务器写响应时失败。"""
    assert base.startswith("http://")
    host_port, _, _ = base[len("http://"):].partition("/")
    host, _, port = host_port.partition(":")
    port = int(port or 80)
    payload = json.dumps({}).encode()
    raw = (f"POST /devices/commands/poll HTTP/1.1\r\nHost: {host}\r\n"
           f"Content-Type: application/json\r\n"
           f"X-Device-Token: {token}\r\n"
           f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n")
    try:
        s = socket.create_connection((host, port), timeout=10)
        s.sendall(raw.encode() + payload)
        # 等服务器领取命令（heartbeat+claim 很快）
        time.sleep(2.0)
        # 设置 SO_LINGER 产生 RST
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                     b"\x01\x00\x00\x00\x00\x00\x00\x00")
        s.close()
        return "DROPPED", {"note": "connection reset on purpose"}
    except OSError as e:
        return "IOERR", {"error": str(e)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingress-url", default="http://localhost:8081")
    ap.add_argument("--token", required=True)
    ap.add_argument("--scenario",
                    choices=["normal", "noack", "dupack", "drop",
                             "outoforder"], default="normal")
    ap.add_argument("--max-cmds", type=int, default=100)
    ap.add_argument("--stop-after", type=float, default=600)
    args = ap.parse_args()
    loop(args.ingress_url, args.token, args.scenario,
         max_cmds=args.max_cmds, stop_after=args.stop_after)


if __name__ == "__main__":
    main()
