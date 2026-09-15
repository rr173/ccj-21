# IoT 设备影子服务（Device Shadow）

三个可独立部署、独立重启的服务 + 一份持久化状态（SQLite 卷）：

```
        控制端 (controller)                 设备 (devices)
              │ HTTP                            │ HTTP long-poll
              ▼                                 ▼
   ┌─────────────────────┐           ┌──────────────────────┐
   │ core   :8080        │◄──────────│ ingress :8081        │ 设备接入网关
   │ 影子状态机 / 存储    │  内部API   │ (无状态, 可多副本)    │
   │ 版本规则/命令生成    │           └──────────────────────┘
   └─────────▲───────────┘
             │ 周期调用 tick / sweep-offline
   ┌─────────┴───────────┐
   │ dispatcher         │ TTL 过期 / ACK 超时重试 / 离线扫描（无状态）
   └─────────────────────┘
             │
   /data/shadow.db (SQLite WAL, Docker volume)
```

* **core**：唯一写库者。期望/报告双版本、命令生成与确认、幂等、重试预算、审计事件。
* **ingress**：设备鉴权（设备令牌）+ long-poll 拉命令 + ACK + 报告上报。
  无状态，设备断线重连后行为完全由 core 中的持久状态决定。
* **dispatcher**：周期维护循环。命令 TTL 过期、SENT 后 ACK 超时退回重试、
  心跳超时标离线、长时间离线预警。

全部代码只用 Python 3.11 标准库，镜像构建不需要联网装依赖。

## 核心语义

### 版本模型

| 版本 | 归属 | 规则 |
|---|---|---|
| `desired_version` | 期望态 | 控制端每次修改 +1；**每条命令绑定一个期望版本**，同设备 `(device, version)` 唯一 |
| `reported_version` | 报告态 | 设备自管理的文档版本，每次上报 +1，与期望版本**相互独立、不比数值** |

设备是否追上目标，用「报告态内容 == 期望态内容」判断（JSON 按键序规范化后比较）。

### 命令生命周期

```
QUEUED ──设备 poll 领取──▶ SENT ──ACK──▶ ACKED（终态）
  │                          │
  │ 新期望产生且本命令从未送达  │ ACK 超时 / 派发写失败
  ▼                          ▼
SUPERSEDED（终态）         RETRYING ──指数退避到期 poll──▶ SENT（attempt+1）
                             │
                  尝试 ≥ MAX_DELIVERY_ATTEMPTS ─▶ FAILED（终态）
QUEUED/RETRYING/SENT 超过 expires_at ─▶ EXPIRED（终态）
```

* **离线排队 / 按版本补发**：设备不在线时命令保持 QUEUED；重新上线 poll 时，
  严格取最低版本的可派发命令。旧版本仍在途（SENT）或等待退避时不会跳发新版本。
* **重复确认安全**：ACK 按 `command_id` 或版本号定位，已 ACK 的命令再次确认
  返回 `200 {"status":"DUPLICATE_ACK","duplicate":true}`，不产生任何副作用。
* **报告乱序/重复**：`reported_version <= 当前版本` 一律 HTTP 409 拒绝，
  原因区分 `STALE_VERSION`（旧版本）与 `DUPLICATE_VERSION`（同版本重复）。
* **取代策略**：新期望产生时，从未送达（QUEUED/RETRYING）的旧版本命令置
  SUPERSEDED；已经 SENT 的命令保留，避免设备已在执行却被服务端抹掉。
* **派发失败重试**：ingress 领取命令后若写响应时设备已断连，回调
  `/failed`；指数退避 `2^(n-1) * RETRY_BACKOFF_BASE`（上限可配），
  超预算 FAILED。每次尝试都在 `command_attempts` 留痕。

### 不一致原因（控制端查询影子可直接看到）

| reason | 含义 |
|---|---|
| `IN_SYNC` | 报告内容与期望一致 |
| `NEVER_DESIRED` | 还没有期望态 |
| `NEVER_REPORTED` | 设了期望但设备从未上报 |
| `DEVICE_OFFLINE` | 命令排队中且设备离线 |
| `PENDING_DELIVERY` | 设备在线，命令排队待补发 |
| `IN_FLIGHT` | 命令已下发，等待 ACK |
| `RETRYING` | 派发失败/ACK 超时，退避重试中 |
| `ACKED_NOT_REPORTED` | 已确认命令，但还没上报匹配内容 |
| `CONTENT_MISMATCH` | 无未决命令但内容不同（设备执行偏差/自发漂移） |
| `FAILED_DISPATCH` | 重试次数耗尽 |
| `EXPIRED` | 命令超过 TTL |
| `SUPERSEDED` | 旧版本命令被新期望取代 |

### 可追溯状态（全部落库，服务重启不丢）

* `commands`：版本、状态、尝试次数、最近错误、创建/派发/确认时间、过期时间。
* `command_attempts`：每次派发尝试的开始/结束时间、结果、错误。
* `events`：完整审计事件流（期望修改、命令入队/发送/确认/重试/失败/过期/
  取代、重复 ACK、报告接受/拒绝、设备上线/离线/长时间离线）。

## HTTP API

### 控制面（core，默认 :8080）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/devices` | `{id,name?}` 注册设备，返回设备 `token` |
| GET | `/v1/devices` | 设备列表 |
| GET | `/v1/devices/{id}` | **影子读模型**（见下） |
| PUT | `/v1/devices/{id}/desired` | `{state, expected_version?, idempotency_key?}` |
| GET | `/v1/devices/{id}/commands` | 该设备命令（版本倒序） |
| GET | `/v1/commands/{cmdId}/attempts` | 每次派发尝试 |
| GET | `/v1/events?device_id=&limit=&offset=` | 审计事件 |

`GET /v1/devices/{id}` 关键字段：

```json
{
  "online": false,
  "offline_for_seconds": 132.4,
  "desired":  {"version": 3, "state": {"led": "on"}},
  "reported": {"version": 7, "state": {"led": "off"}},
  "sync": {"in_sync": false, "reason": "DEVICE_OFFLINE"},
  "next_pending_command": {
    "command_id": "cmd_...", "version": 3, "status": "QUEUED",
    "attempts": 0, "expires_at": 1789..., "next_eligible_at": null,
    "last_error": ""
  },
  "last_acknowledged": {
    "command_id": "cmd_...", "version": 2, "acked_at": 1789...,
    "code": "APPLIED", "attempts": 1
  }
}
```

### 设备面（ingress，默认 :8081；请求头 `X-Device-Token`）

| 方法 | 路径 | Body |
|---|---|---|
| POST | `/devices/commands/poll` | `{}` long-poll，心跳 + 领下一条命令 |
| POST | `/devices/ack` | `{command_id}` 或 `{version}`，`code/message` 可选 |
| POST | `/devices/reported` | `{version, state}`，旧版本返回 409 |

## Docker 部署

```bash
docker compose up -d --build
# core:     http://localhost:8080
# ingress:  http://localhost:8081
# 数据卷:    shadow-data -> /data/shadow.db (WAL)
```

可调环境变量（见 `docker-compose.yml`，均有默认值）：

| 变量 | 默认 | 含义 |
|---|---|---|
| `OFFLINE_AFTER_SECONDS` | 30 | 心跳超时判离线 |
| `PROLONGED_OFFLINE_SECONDS` | 300 | 长时间离线预警（每轮离线周期告警一次） |
| `ACK_TIMEOUT_SECONDS` | 30 | SENT 后等 ACK 超时 |
| `COMMAND_TTL_SECONDS` | 900 | 命令整体有效期 |
| `MAX_DELIVERY_ATTEMPTS` | 5 | 最大派发尝试 |
| `RETRY_BACKOFF_BASE/MAX` | 2/60 | 指数退避基数/上限（秒） |
| `DEVICE_POLL_WAIT` | 25 | 设备 long-poll 挂起时长 |
| `SHADOW_INTERNAL_TOKEN` | change-me | 服务间内部令牌（**生产必改**） |

## 本地开发 / 测试（无需 Docker）

```bash
pip 零依赖，直接：
./scripts/dev_local.sh                 # 三进程本地启动，数据在 ./data

# 控制端演示（注册 + 离线下发 + 查询影子）
./scripts/demo_control.sh demo-lamp

# 设备模拟器（另开终端，令牌来自注册响应）
python3 run_sim.py --ingress-url http://localhost:8081 \
    --token <TOKEN> --scenario normal     # 正常收发
                              # dupack   每条命令确认两次
                              # outoforder 故意乱序上报
                              # drop     派发时 RST（失败重试）
                              # noack    不确认（ACK 超时重发）

# 测试
python3 tests/test_model.py   # 纯逻辑：分类/退避/JSON 比较
python3 tests/test_store.py   # SQLite 状态机（虚拟时钟，含重启恢复）
python3 tests/e2e_test.py     # 真实三进程 HTTP 端到端
```

## 生产化备注

* core 是单写者、SQLite WAL；高可用场景可把 `store.py` 替换为
  PostgreSQL/CockroachDB（接口不变），ingress/dispatcher 已可水平扩展。
* 当前设备协议是 HTTP long-poll；同样的 core 内部 API
  （resolve/heartbeat/claim/ack/reported/failed）可平移到 MQTT/gRPC 网关。
* 设备令牌目前在注册时生成、明文存库；接入真实体系时应换为哈希存储 +
  注册流程签发。
