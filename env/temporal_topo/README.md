# temporal-topo · 可追溯的时态网络拓扑

现场节点通过**邻接观测**维护可追溯的时态网络拓扑。纯 Python 3.11 标准库 +
SQLite (WAL)，无第三方依赖。

```
  观测摄取                拓扑推导 (分阶段作业)                运维查询
 ┌──────────┐   受理    ┌──────────────────────────┐   原子发布   ┌───────────────┐
 │ 邻接观测  │ ───────▶ │ adjudicate → damp → publish │ ────────▶ │ 修订/区间/边   │
 │ (证词)    │  序号通道 │  裁定冲突    阻尼防抖      │           │ 路径索引/差异  │
 └──────────┘          └──────────────────────────┘           │ 旁路/驳回档案  │
 ┌──────────┐   只表示可达, 绝不产生邻接边                     │ 故障域留档     │
 │ 在线租约  │ ───────────────────────────────────────────▶  │ (并列展示)     │
 └──────────┘                                                └───────────────┘
```

## 快速开始

```bash
cd temporal_topo
python3 -m unittest discover -s tests -v   # 20 个行为测试
python3 demo.py                            # 端到端演示 (生成 demo.db)
python3 -m topo --db demo.db status --at 5000
python3 -m topo --db demo.db path n1 n5 --at 5000
python3 -m topo --db demo.db conflicts --at 5000
python3 -m topo --db demo.db edge n2 n3 --medium radio
```

## 需求到实现的映射

### 观测受理 (`topo/ingest.py`)

每份观测 = {观察者, 本地序号, 采样时刻, 邻居, 介质, 链路质量, 存活期限}。

* **序号只能前进**: `observer_state.last_seq` 单调递增；`seq < last_seq` 拒绝
  并留痕 (`rejects.reason='regressed'`)。
* **相同序号只采信一次**: `observations UNIQUE(observer, seq)` + 内容哈希；
  同号同文 → `duplicate`，同号异文 → `seq_conflict`（保留首次证词），
  都写入 `rejects` 供审计。
* **到达先后 ≠ 采样先后**: 受理只检查序号通道；推导阶段按 `sample_time`
  切分历史区间 (`adjudicate.compute_epochs`)，与到达顺序无关。
* **每次受理是一个事务**（序号前进 + 证词落库），崩溃重试幂等。

### 冲突裁定 (`topo/adjudicate.py`)

链路两端证词不一致时按 **来源可信级别 → 新鲜度 → 介质规则** 裁定：

* 可信级别高者优先；并列时采样时刻新者优先（`trust, sample_time, seq`）。
* 介质规则 (`topo/models.py: MEDIA`)：
  * `confirm: both`（fiber/ethernet）需两端证词，单侧 → `unconfirmed` 不可用；
    `confirm: single`（radio/wifi）接受单侧证词；
  * `down_policy: down_wins`（ethernet）任一端报 down 即判 down；
    `trust` 则按可信/新鲜度裁定。
* 质量差超过容忍度记 `conflict-quality`；up/down 不一致记 `conflict-state`。
* **双方证词都留存**（`obs_a`/`obs_b` 指向原始观测），裁定依据写入
  `rationale`，运维界面 `conflicts` / `edge` 可展示。

### 租约 ≠ 邻接

在线租约 (`leases`) 只表示节点可达；邻接证词才表示相连。推导只消费
`status='accepted'` 的观测；`status` 总览把 `online_by_lease` 与连通分量
并列展示，无邻接的在线节点标为 `lease_only` 孤立分量。测试
`test_lease_is_not_adjacency` 锁死该边界。

### 抖动门限 (`topo/damping.py`)

* 质量样本沿时间轴逐区间喂入阻尼器：连续 3 个坏样本（低于介质门限）才
  置不可用，连续 2 个好样本才恢复（`DAMP_DOWN_AFTER/UP_AFTER`）。
* 不足门限的短暂抖动只写入 `damp_note` 证据（`flap suppressed ...`），
  拓扑不翻转。
* 显式 down（quality=0）是邻接断言而非质量样本，由裁定层处理。

### 修订、迟到证词与封存 (`topo/derive.py`)

* 每次推导产生**不可改写的新修订**：`revisions` + `revision_edges`（留档）
  + `revision_diffs`（新增/撤销/改判）+ 路径索引，全部在一个事务内发布。
* **迟到证词**（采样时刻落在未封存历史）改变对应区间的裁定，下一次推导
  生成新修订；`revision_at(t)` 对同一历史时刻返回更新的修订号。
* **封存** (`seal`)：界限处切开跨界区间，之前的历史冻结；之后到达的更早
  证词在摄取层直接分流到旁路档案（`status='sidecar'` + 原因），
  绝不改写既有修订。

### 崩溃恢复 (`topo/jobs.py`)

* 摄取、推导、故障域计算都是**分阶段作业**，每阶段完成后提交持久检查点；
  `recover()` 在重启后续跑所有 `running` 作业，跳过已完成阶段。
* 推导的 `publish` 阶段是**单个事务**：修订、当前视图、区间映射、差异、
  路径索引（连通分量/最短路/割点）要么全部落库要么全部回滚——
  不会留下只更新边却没更新路径索引的半成品。
* 批量摄取按行提交检查点，崩溃后从下一行续跑，已受理行幂等去重。
* 测试用 `crash_after` / `crash_mid_publish` 钩子模拟崩溃验证。

### 路径与故障域 (`topo/graph.py`)

* 代价 = `介质基础代价 × (2 - 质量)`（微单位整数，精确比较）；同对节点的
  平行介质边先取代价最低者——**路径相同但质量不同时稳定选代价更低者**。
* Dijkstra 堆键为 `(总代价, 节点序列)`：**等价路径按节点名字典序这一固定
  规则决胜**，`tie_broken` 标记写入路径索引并在路径解释中说明。
* 割点用 DFS low-link；故障域 = 移除节点前后同分量成员的连通性变化
  （丢失的上联出口、失联的对端数），按机房/交换域归档展示。

### 运维界面 (`topo/ops.py` + `topo/cli.py`)

| 命令 | 展示 |
|---|---|
| `status --at T` | T 时刻采用的修订号、封存界限、在线视图与连通分量 |
| `components --at T` | 连通分量（含出口、lease_only 标记） |
| `path A B --at T` | 最短可用路径 + 逐跳代价/证词/决胜说明 |
| `cutpoints --at T` | 单点割点 |
| `failure-domain X --at T` | X 失联的受影响成员、丢失出口（可崩溃续跑） |
| `conflicts [--at T]` | 证词冲突、双方证词与裁定依据 |
| `edge A B [--medium M]` | 沿边追到原始观测（逐区间裁定 + 证词原文） |
| `revisions` / `diff R1 R2` | 修订列表（差异统计）/ 任意两修订差异 |
| `sidecar` / `rejects` | 旁路原因 / 驳回留痕 |
| `recover` | 崩溃后续跑未完成作业 |

## 目录结构

```
topo/
  models.py      介质规则、门限常量、代价函数
  store.py       SQLite(WAL) schema、事务、检查点
  jobs.py        分阶段作业运行器 + recover()
  ingest.py      登记/租约/观测受理/批量摄取作业
  adjudicate.py  历史区间切分 + 冲突裁定
  damping.py     抖动阻尼器
  derive.py      推导作业（adjudicate→damp→publish）与封存
  graph.py       分量/最短路/割点/故障域算法
  failure.py     故障域计算作业
  ops.py         运维查询层
  cli.py         命令行界面 (python3 -m topo)
tests/test_topo.py   20 个行为测试
demo.py              端到端演示
```

## 核心不变量

1. 同一观察者的已受理序号严格递增；`(observer, seq)` 至多一条证词。
2. 封存界限只前进；`sample_time < boundary` 的证词永远进旁路，不改写历史。
3. 租约绝不产生邻接边；路径只走 `usable=1` 的裁定边。
4. 一次发布的修订内，边、区间映射、差异、路径索引同属一个 `rev_id`
   （单事务），崩溃后要么不存在要么完整。
5. 一切选择（证词、代价、决胜）都有确定规则，同一输入重放结果一致。
