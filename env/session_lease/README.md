# 设备会话租约 / 凭证轮换 / 命令对账系统

设备接入网关的会话与凭证控制面，单服务 + 一份持久化状态（SQLite WAL）：

```
        控制端 (X-Admin-Token)                设备 (凭证版本 + 密钥)
                  │ HTTP                              │ HTTP
                  ▼                                   ▼
        ┌─────────────────────────────────────────────────┐
        │ session-lease  :8080                             │
        │  会话代次状态机 / 租约 / 凭证轮换 / 命令对账       │
        │  所有写操作在 SQLite 事务内串行，后台 sweep 清扫   │
        └───────────────────────┬─────────────────────────┘
                                │
                    /data/lease.db (SQLite WAL)
```

全部代码只用 Python 3.11 标准库；`python3 -m lease` 即可启动。

## 核心不变量

* **同一设备至多一个可写会话**。设备上线提交 `connection_no`（连接编号）
  与 `credential_version`（凭证版本 + 密钥）；并发上线、接管重连都产生
  **严格单调递增的会话代次（generation）**。代次在 SQLite 中按
  `max(设备表代次, 会话表最大代次)+1` 生成，多线程并发也不重复、不倒退。
* **关闭过的连接编号永不复活**：连接编号在设备内唯一；其会话 SUPERSEDED /
  EXPIRED / REVOKED 后再用同一编号上线，拒绝 `DEAD_CONNECTION_REUSED`。
* **旧连接的消息一律拒绝且留痕**：被顶替、过期、撤销的会话提交状态上报、
  命令确认、续期、对账时，在鉴权阶段就被拒绝，原因写入 `rejected_messages`
  （控制端可按设备/类型查询），**绝不覆盖新会话产生的状态**。
* **租约持久化**：每次上线/续期/poll 心跳都把 `lease_expires_at` 落库；
  后台 sweep（或下一次消息）到点把会话置 EXPIRED。**到期之后的续期被拒绝**，
  只能重新上线拿新代次。
* **服务重启不复活失效连接**：重启后从库中恢复当前会话指针、租约截止时间、
  轮换进度与结果未知的命令；已结束的会话仍然是死的。

## 凭证轮换

```
ACTIVE ──发起轮换──▶ ROTATING ──宽限期到期 / 管理员撤销──▶ REVOKED
  │                    │
  └ 新版本始终 ACTIVE   └ 旧凭证只允许给「它自己的现存会话」续期
                          （旧凭证开新连接一律 OLD_CREDENTIAL_NEW_CONNECTION）
```

* `POST .../rotations` 创建新版本凭证并返回新密钥；旧版本进入 ROTATING，
  宽限截止时间落库，重启后继续计时。
* 轮换期间：**新连接必须用新凭证**；旧凭证唯一被放行的操作是给已存在的旧会话
  续期（续期响应中 `renewed_with_old_credential=true`）。
* 设备用新凭证上线后轮换提前 **COMPLETED**，旧凭证立即 REVOKED
  （`SUPERSEDED_MIGRATION`），仍持有旧凭证的会话立刻失效。
* 宽限期到期：sweep 撤销旧凭证（`GRACE_EXPIRED`），杀掉所有仍在用旧凭证的
  可写会话；管理员也可随时 `.../revoke-old`（`ADMIN_REVOKED`）立即撤销。
* 轮换、撤销、上线、续期全部幂等。

## 命令在会话切换时的处置

命令按设备维护单调 `version`，每次派发绑定**当前会话代次**
（`dispatch_generation`），每次派发在 `command_attempts` 留痕。

```
                    ┌─ 设备 ACK ─▶ ACKED（终态，接管后绝不重发）
QUEUED ─poll派发─▶ SENT
                    └─ 会话失效（接管/并发上线/租约过期/凭证撤销）
                              ─▶ RECONCILING（结果未知，冻结）

QUEUED（从未发送）─ 会话失效 ─▶ QUEUED_UNKNOWN（冻结，等新会话对账表态）
```

接管 / 并发上线 / 租约到期 / 凭证撤销发生时：

| 切换前状态 | 处置 |
|---|---|
| ACKED（已确认） | 原封不动，**绝不重发** |
| QUEUED（从未发送） | 转 `QUEUED_UNKNOWN`，等新会话按版本对账 |
| SENT（已发送，结果未知） | 转 `RECONCILING`，必须对账后才能决定 |

新会话对结果未知的命令**按命令版本对账**（`/devices/reconcile`，批量）：

* `done=true`  → `ACKED`，`ack_source=RECONCILE_DONE`（设备已执行，不重发）；
* `done=false` → 回 `QUEUED`，随后 poll 派发给新会话（`attempts` 里留下
  两代/两个会话的派发记录）；
* 已完成命令重复对账返回 `ALREADY_DONE`，普通 QUEUED 返回 `STILL_QUEUED`，
  当前代次仍在途的返回 `IN_FLIGHT`——**对账整体幂等**。

在最低版本的命令结果未知期间，poll **严格按版本补发**：在途命令幂等重发
（`duplicate_dispatch`，不新增 attempt），对账中的命令返回
`await_reconcile` 提示，不跳发新版本。直接对 RECONCILING / QUEUED_UNKNOWN
命令发 ACK 会被拒绝（`COMMAND_AWAIT_RECONCILE`），只能走对账。

每条命令的完整过程（创建/派发/确认/转对账/对账完成或重投，含代次与会话）
都可经 `GET /v1/commands/{id}/timeline` 查到。

## 接管

* `POST /v1/devices/{id}/takeover`：当前可写会话立即 SUPERSEDED
  （`ADMIN_TAKEOVER`），在途命令转对账，生成 `PENDING_RECONNECT` 接管记录；
* 设备用**新连接编号**重新上线时获得新一代会话，接管记录回填
  `new_session_id/new_generation` 并 COMPLETED；
* 重复接管返回同一条待重连记录（幂等键保护 + 同设备待重连去重）。

## HTTP API

### 控制面（请求头 `X-Admin-Token`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/devices` | 注册设备，返回 v1 凭证版本与密钥 |
| GET | `/v1/devices/{id}` | 当前会话/代次/轮换/报告态/命令计数 |
| GET | `/v1/devices/{id}/sessions` | 当前与历史会话（代次倒序） |
| GET | `/v1/devices/{id}/credentials` | 各凭证版本状态（ACTIVE/ROTATING/REVOKED） |
| POST | `/v1/devices/{id}/rotations` | 发起轮换 `{grace_seconds?, idempotency_key?}`，返回新密钥 |
| GET | `/v1/devices/{id}/rotations` · `/v1/rotations` | 轮换记录 |
| POST | `/v1/devices/{id}/revoke-old` | 管理员立即撤销轮换中的旧凭证 |
| POST | `/v1/devices/{id}/takeover` | 发起接管 `{reason?, idempotency_key?}` |
| GET | `/v1/devices/{id}/takeovers` | 接管记录（旧/新会话与代次） |
| POST | `/v1/devices/{id}/commands` | 下发命令 `{payload, idempotency_key?}` |
| GET | `/v1/devices/{id}/commands` | 命令列表（版本倒序） |
| GET | `/v1/commands/{commandId}/timeline` | 派发尝试 + 完整事件时间线 |
| GET | `/v1/rejected-messages?device_id=&kind=&limit=&offset=` | 被拒绝的旧会话消息 |
| GET | `/v1/events?device_id=&limit=&offset=` | 全局审计事件 |
| POST | `/v1/sweep` | 手动触发一次清扫（测试/运维用） |

### 设备面（凭证放请求体；`device_id/connection_no/credential_version/credential_secret`）

| 方法 | 路径 | Body / 说明 |
|---|---|---|
| POST | `/devices/online` | 上线，`{lease_seconds?}`，返回 generation 与租约截止时间 |
| POST | `/devices/renew` | 租约续期；轮换期旧凭证只在此被放行 |
| POST | `/devices/poll` | 心跳领命令（兼做心跳续期），返回最低版本可派发命令 |
| POST | `/devices/reported` | `{state_version, state}`；旧/重复版本 409 拒绝 |
| POST | `/devices/ack` | `{command_id}` 或 `{version}`，重复 ACK 幂等 |
| POST | `/devices/reconcile` | `{entries:[{version, done, result?}]}` 按版本对账 |

所有写接口接受可选 `idempotency_key`：同键同请求体返回首次结果
（`idempotent_replay=true`）；同键不同请求体返回 409
`IDEMPOTENCY_CONFLICT`。设备消息另带一层自然幂等：活跃连接编号重复上线、
活跃会话重复 poll 同一条在途命令、已确认命令重复 ACK、完成后的重复对账，
都返回重复标记而无副作用。

### 拒绝原因码（`rejected_messages.reason` / 错误体 `error`）

| code | 含义 |
|---|---|
| `BAD_CREDENTIAL` | 凭证版本不存在或密钥不匹配 |
| `CREDENTIAL_REVOKED` | 凭证已撤销（宽限到期/管理员撤销/迁移完成） |
| `OLD_CREDENTIAL_NEW_CONNECTION` | 轮换期用旧凭证开新连接 |
| `CREDENTIAL_MISMATCH` | 连接编号绑定的凭证版本与提交的不一致 |
| `UNKNOWN_CONNECTION` | 连接编号没有对应会话 |
| `DEAD_CONNECTION_REUSED` | 已结束会话的连接编号被复用 |
| `SESSION_SUPERSEDED` / `SESSION_EXPIRED` / `SESSION_REVOKED` | 旧连接不是当前可写会话 |
| `STALE_STATE_VERSION` / `DUPLICATE_STATE_VERSION` | 报告版本旧 / 重复 |
| `GENERATION_MISMATCH` | 命令属于另一代会话，本会话不能确认 |
| `COMMAND_AWAIT_RECONCILE` | 命令结果未知，必须先按版本对账 |
| `COMMAND_NOT_DISPATCHED` | 命令尚未派发，不能确认 |
| `IDEMPOTENCY_CONFLICT` | 幂等键对应了不同请求体 |

## 运行

```bash
python3 -m lease --db lease.db --port 8080
# 环境变量：LEASE_ADMIN_TOKEN（生产必改）、LEASE_DEFAULT_LEASE_SECONDS、
#          LEASE_MAX_LEASE_SECONDS、LEASE_DEFAULT_GRACE_SECONDS、
#          LEASE_SWEEP_INTERVAL_SECONDS
```

端到端流程示例：

```bash
TOK="$LEASE_ADMIN_TOKEN"; B=http://localhost:8080
curl -s -XPOST $B/v1/devices -H "X-Admin-Token: $TOK" \
  -d '{"device_id":"lamp-1"}'                       # -> 凭证 v1 + 密钥
curl -s -XPOST $B/devices/online -d '{
  "device_id":"lamp-1","connection_no":"c1",
  "credential_version":1,"credential_secret":"...",
  "lease_seconds":60}'
curl -s -XPOST $B/v1/devices/lamp-1/rotations \
  -H "X-Admin-Token: $TOK" -d '{"grace_seconds":300}'  # -> 凭证 v2 + 新密钥
```

## 测试

```bash
python3 -m unittest discover -s tests
# 48 个用例：
#   test_sessions.py  上线/续期/并发上线/接管/单调代次（含 20 线程并发）
#   test_rotation.py  轮换、旧凭证受限、宽限到期、管理员撤销、连续轮换
#   test_commands.py  代次绑定、会话切换三态处置、按版本对账、严格补发
#   test_restart.py   重启恢复租约/轮换/会话/对账命令/幂等账本，不复活死连接
#   test_api.py       真实 HTTP 端到端（含后台 sweep 宽限到期）
```

## 持久化表

`sessions`（会话代次/租约/结束原因/顶替关系/接管关联）、`devices`
（当前会话指针、代次上限、命令版本号）、`credentials`、`rotations`、
`takeovers`、`commands`、`command_attempts`、`command_events`、
`reported_states`、`rejected_messages`、`idempotency`、`events`。

## 生产化备注

* 单进程写者 + SQLite WAL；水平扩展时把 `store.py` 换成 Postgres/CockroachDB
  即可（代次生成用行锁/序列，接口不变）。
* 凭证密钥当前明文落库仅为演示；接入真实 KMS/HSM 时应改为哈希存储并在
  轮换签发环节加密分发。
* 设备面为简化演示把凭证放在请求体中；接入网关部署时应改到 mTLS 或
  签名请求头，并在网关终结 TLS。
