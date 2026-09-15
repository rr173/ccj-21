# 设备遥测规则与告警模块

控制端为设备组定义带生效时间的指标规则；设备批量上报带唯一编号与设备时间的指标；
系统按事件时间窗口聚合、判定连续命中并管理告警周期，通过持久化待发队列投递通知。
全部状态落 SQLite，重启后无缝恢复。

## 运行

```bash
# 启动控制端服务（仅依赖 Python 3.11 标准库）
python3 -m telemetry --db telemetry.db --port 8080 [--webhook URL]

# 运行测试（56 个用例）
python3 -m unittest discover -s tests
```

## 核心语义

### 规则与版本
- 规则按 `(group_id, metric)` 唯一定义：事件时间窗口、聚合方式
  （sum/avg/min/max/count/last）、比较符与阈值、连续命中次数、恢复条件
  （连续未命中窗口数）、静默期、允许迟到时间、生效时间。
- `POST /rules/{id}/versions` 产生新版本：旧版本生效区间在 `effective_from`
  处截止，新版本自此生效。**窗口在创建时绑定当时生效的规则版本**，
  之后规则更新不会重新解释已归属旧版本的窗口（修正也沿用窗口绑定版本）。

### 摄入、水位线与迟到
- 事件必须携带唯一 `event_id` 与设备时间 `event_time`；同一 `event_id`
  只计数一次（重复上报返回 `duplicate` 及原处置结果）。
- 乱序事件按事件时间进入正确窗口。每条 (device, metric) 流维护水位线
  `watermark = max(已接受事件时间)`；`watermark >= window_end` 时窗口封存并评估。
- 水位线之前但仍在允许迟到期内（`watermark < window_end + allowed_lateness`）
  的事件：重算窗口聚合值并写入 `window_corrections` 修正记录（修正前后聚合值、
  命中结论、触发事件）。
- 超过迟到期的事件只能进入隔离列表（`status=quarantined` 带原因），
  **不能改动已封存结果**。

### 告警生命周期
- 告警周期由已封存窗口序列**确定性重放**得出：连续命中达到阈值只开启一个周期，
  后续命中更新同一周期（`hit_count`、`last_hit_window`），满足恢复条件后关闭；
  静默期内抑制重新开启。无数据的缺口窗口按非命中处理（打断连续命中、计入恢复）。
- 迟到修正改变告警结论时：重放并与库中周期对齐——多余的周期作废（`invalidated`
  但保留）、缺失的周期追溯开启、关闭结论被推翻的重新打开；`alert_corrections`
  记录每次修正的**前后原因**，已发出的通知记录永不删除。

### 通知投递
- 通知先写入持久化待发队列（`notifications`），由 `pump` 投递；
  同一周期的 opened/closed 通知幂等入队（唯一索引），重启/重放不重复。
- 失败按指数退避重试（`next_attempt_at`），每次尝试写入 `notification_attempts`。
- 投递成功等待接收方确认；超时未确认会重投（接收方按 `notification_id` 去重）。
- **重复确认幂等**：已 confirmed 的通知再次确认直接返回原结果，不重复完成、不报错。

### 重启恢复
- 所有状态（未封存窗口、水位线、未关闭告警、待发/未确认通知、退避状态）
  持久化在 SQLite。重启后：未封存窗口继续聚合、未关闭告警继续评估、
  未完成通知继续投递——不重复聚合（事件去重）、不重复开告警（周期按
  `opened_at` 对齐）、不重发已确认通知。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/rules` | 创建规则（含生效时间） |
| POST | `/rules/{id}/versions` | 创建规则新版本 |
| GET | `/rules/{id}`、`/rules` | 查询规则及版本 |
| POST | `/devices` | 注册设备到设备组 |
| POST | `/ingest` | 批量上报事件，返回每个事件的处置结果 |
| GET | `/events/{id}`、`/events?device_id=&status=` | 事件接收/隔离/无规则结果 |
| GET | `/windows?device_id=&metric=` | 窗口聚合值、命中结论、采用的规则版本 |
| GET | `/alerts?device_id=&rule_id=&status=` | 告警周期（open/closed/invalidated） |
| GET | `/corrections/windows`、`/corrections/alerts` | 窗口/告警迟到修正记录 |
| GET | `/quarantine` | 隔离列表 |
| GET | `/notifications`、`/notifications/{id}`、`/notifications/{id}/attempts` | 通知状态与每次投递尝试 |
| POST | `/notifications/pump` | 触发一轮投递 |
| POST | `/notifications/{id}/ack` | 接收方确认（幂等，可带 ack_token） |

## 代码结构

```
telemetry/
  storage.py   # SQLite schema 与事务助手
  service.py   # 核心域逻辑：规则版本、摄入/去重/乱序/迟到/隔离、
               # 窗口聚合与封存、告警重放、修正对齐、通知队列与退避
  notify.py    # 可插拔投递通道（webhook / 文件 / 自定义可调用）
  api.py       # 控制端 HTTP API（标准库 http.server）
  __main__.py  # 服务入口
tests/         # 56 个用例：规则版本、摄入语义、告警生命周期、
               # 迟到修正、通知投递、重启恢复、HTTP 端到端
```
