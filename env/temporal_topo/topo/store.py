"""SQLite 持久层 (WAL).

崩溃一致性约定:
  * 每条观测的受理 (observer_state 前进 + observations 落行) 是一个事务;
  * 拓扑推导的"发布"阶段 (revisions + current_edges + epochs + 路径索引 +
    修订差异) 是**一个**事务 —— 绝不留下只更新边却没更新路径索引的半成品;
  * 作业分阶段推进, 每阶段完成后提交持久检查点 (checkpoints 表), 崩溃后
    recover() 从最后一个已提交检查点续跑.
"""

import json
import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- 运维登记: 机房 / 交换域 / 节点 / 上联出口
CREATE TABLE IF NOT EXISTS rooms(
  name TEXT PRIMARY KEY,
  note TEXT
);
CREATE TABLE IF NOT EXISTS domains(
  name TEXT PRIMARY KEY,
  room TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS nodes(
  node_id TEXT PRIMARY KEY,
  room TEXT,
  domain TEXT,
  trust INTEGER NOT NULL DEFAULT 5,
  is_exit INTEGER NOT NULL DEFAULT 0,
  exit_name TEXT
);

-- 在线租约: 只表示节点可达, 绝不参与邻接推导
CREATE TABLE IF NOT EXISTS leases(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  node_id TEXT NOT NULL,
  starts INTEGER NOT NULL,
  ends INTEGER NOT NULL,
  source TEXT
);

-- 观察者序号通道
CREATE TABLE IF NOT EXISTS observer_state(
  observer_id TEXT PRIMARY KEY,
  last_seq INTEGER NOT NULL
);

-- 原始观测 (受理的证词 + 旁路档案); (observer, seq) 唯一保证同序号只采信一次
CREATE TABLE IF NOT EXISTS observations(
  obs_id INTEGER PRIMARY KEY AUTOINCREMENT,
  observer TEXT NOT NULL,
  seq INTEGER NOT NULL,
  sample_time INTEGER NOT NULL,
  neighbor TEXT NOT NULL,
  medium TEXT NOT NULL,
  quality REAL NOT NULL,
  ttl INTEGER NOT NULL,
  arrival INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  status TEXT NOT NULL,            -- accepted | sidecar
  reason TEXT,
  UNIQUE(observer, seq)
);

-- 被拒观测留痕 (序号倒退 / 重复 / 同号不同文 / 非法介质)
CREATE TABLE IF NOT EXISTS rejects(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  observer TEXT NOT NULL,
  seq INTEGER NOT NULL,
  arrival INTEGER NOT NULL,
  reason TEXT NOT NULL,
  payload_json TEXT
);

-- 作业与持久检查点
CREATE TABLE IF NOT EXISTS jobs(
  job_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  status TEXT NOT NULL,            -- running | done
  payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoints(
  job_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  data_json TEXT,
  PRIMARY KEY(job_id, stage)
);

-- 推导作业的暂存区 (按 job_id 隔离, 崩溃后可重放)
CREATE TABLE IF NOT EXISTS staging_epochs(
  job_id TEXT NOT NULL,
  epoch_start INTEGER NOT NULL,
  epoch_end INTEGER NOT NULL,
  PRIMARY KEY(job_id, epoch_start)
);
CREATE TABLE IF NOT EXISTS staging_edges(
  job_id TEXT NOT NULL,
  epoch_start INTEGER NOT NULL,
  epoch_end INTEGER NOT NULL,
  ep_a TEXT NOT NULL,
  ep_b TEXT NOT NULL,
  medium TEXT NOT NULL,
  quality REAL NOT NULL,
  state TEXT NOT NULL,             -- up | down | unconfirmed
  usable INTEGER NOT NULL DEFAULT 0,
  obs_a INTEGER, obs_b INTEGER, winner_obs INTEGER,
  decision TEXT NOT NULL,
  rationale TEXT NOT NULL,
  damp_note TEXT,
  PRIMARY KEY(job_id, epoch_start, ep_a, ep_b, medium)
);

-- 拓扑修订: 每次推导产生一个不可改写的新修订
CREATE TABLE IF NOT EXISTS revisions(
  rev_id INTEGER PRIMARY KEY AUTOINCREMENT,
  trigger TEXT NOT NULL,
  note TEXT,
  job_id TEXT
);

-- 历史区间 -> 当前采用的修订 (封存后 rev_id 冻结)
CREATE TABLE IF NOT EXISTS epochs(
  epoch_start INTEGER PRIMARY KEY,
  epoch_end INTEGER NOT NULL,
  rev_id INTEGER NOT NULL,
  sealed INTEGER NOT NULL DEFAULT 0
);

-- 当前视图 (封存部分永不改写)
CREATE TABLE IF NOT EXISTS current_edges(
  epoch_start INTEGER NOT NULL,
  epoch_end INTEGER NOT NULL,
  ep_a TEXT NOT NULL,
  ep_b TEXT NOT NULL,
  medium TEXT NOT NULL,
  quality REAL NOT NULL,
  state TEXT NOT NULL,
  usable INTEGER NOT NULL,
  obs_a INTEGER, obs_b INTEGER, winner_obs INTEGER,
  decision TEXT NOT NULL,
  rationale TEXT NOT NULL,
  damp_note TEXT,
  PRIMARY KEY(epoch_start, ep_a, ep_b, medium)
);

-- 每个修订的完整边集 (历史留档, 供追溯与差异)
CREATE TABLE IF NOT EXISTS revision_edges(
  rev_id INTEGER NOT NULL,
  epoch_start INTEGER NOT NULL,
  epoch_end INTEGER NOT NULL,
  ep_a TEXT NOT NULL, ep_b TEXT NOT NULL, medium TEXT NOT NULL,
  quality REAL NOT NULL, state TEXT NOT NULL, usable INTEGER NOT NULL,
  obs_a INTEGER, obs_b INTEGER, winner_obs INTEGER,
  decision TEXT NOT NULL, rationale TEXT NOT NULL, damp_note TEXT,
  PRIMARY KEY(rev_id, epoch_start, ep_a, ep_b, medium)
);

-- 修订差异: 新增 / 撤销 / 改判
CREATE TABLE IF NOT EXISTS revision_diffs(
  rev_id INTEGER NOT NULL,
  change TEXT NOT NULL,            -- added | withdrawn | readjudicated
  epoch_start INTEGER NOT NULL,
  ep_a TEXT NOT NULL, ep_b TEXT NOT NULL, medium TEXT NOT NULL,
  detail_json TEXT NOT NULL
);

-- 路径索引 (与所属修订同事务提交)
CREATE TABLE IF NOT EXISTS components(
  rev_id INTEGER NOT NULL,
  epoch_start INTEGER NOT NULL,
  node TEXT NOT NULL,
  component TEXT NOT NULL,
  PRIMARY KEY(rev_id, epoch_start, node)
);
CREATE TABLE IF NOT EXISTS paths(
  rev_id INTEGER NOT NULL,
  epoch_start INTEGER NOT NULL,
  src TEXT NOT NULL, dst TEXT NOT NULL,
  cost_micro INTEGER NOT NULL,
  path_json TEXT NOT NULL,
  hops_json TEXT NOT NULL,
  tie_broken INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(rev_id, epoch_start, src, dst)
);
CREATE TABLE IF NOT EXISTS articulation(
  rev_id INTEGER NOT NULL,
  epoch_start INTEGER NOT NULL,
  node TEXT NOT NULL,
  PRIMARY KEY(rev_id, epoch_start, node)
);

-- 故障域计算结果 (按作业留档)
CREATE TABLE IF NOT EXISTS failure_domains(
  job_id TEXT NOT NULL,
  rev_id INTEGER NOT NULL,
  epoch_start INTEGER NOT NULL,
  failed_node TEXT NOT NULL,
  member TEXT NOT NULL,
  room TEXT, domain TEXT,
  before_component TEXT, after_component TEXT,
  lost_exits_json TEXT NOT NULL,
  lost_peers INTEGER NOT NULL,
  PRIMARY KEY(job_id, member)
);
"""


class Store:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, isolation_level=None)  # 手动事务
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        # 测试/演示用的崩溃注入钩子
        self.crash_after = None        # 阶段名: 该阶段检查点提交后模拟崩溃
        self.crash_mid_publish = False  # 在 publish 事务中间模拟崩溃

    def close(self):
        self.conn.close()

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    # ---- 小工具 ----
    def meta(self, key, default=None):
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key, value):
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def next_counter(self, key) -> int:
        """单调计数器 (到达序号 / 作业序号), 调用方须在事务内使用."""
        cur = self.meta(key, "0")
        nxt = int(cur) + 1
        self.set_meta(key, nxt)
        return nxt

    def q(self, sql, args=()):
        return self.conn.execute(sql, args).fetchall()

    def q1(self, sql, args=()):
        return self.conn.execute(sql, args).fetchone()

    def seal_boundary(self):
        v = self.meta("seal_boundary")
        return int(v) if v is not None else None

    # ---- 作业检查点 ----
    def checkpoint_get(self, job_id, stage):
        row = self.q1("SELECT data_json FROM checkpoints WHERE job_id=? AND stage=?",
                      (job_id, stage))
        return json.loads(row["data_json"]) if row and row["data_json"] else None

    def checkpoint_done(self, job_id, stage) -> bool:
        data = self.checkpoint_get(job_id, stage)
        return bool(data and data.get("done"))

    def checkpoint_put(self, job_id, stage, data):
        self.conn.execute(
            "INSERT INTO checkpoints(job_id,stage,data_json) VALUES(?,?,?) "
            "ON CONFLICT(job_id,stage) DO UPDATE SET data_json=excluded.data_json",
            (job_id, stage, json.dumps(data)),
        )
