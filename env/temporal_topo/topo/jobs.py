"""分阶段作业运行器: 持久检查点 + 崩溃续跑.

每个作业由若干阶段组成; 阶段函数自己负责把产出写入暂存区/正式表
(产出写入与检查点提交未必同一事务, 因此阶段函数必须幂等: 重跑时先清理
自己 job_id 下的旧产出再重写)。运行器在每个阶段完成后提交检查点
{done: true}; 崩溃后 recover() 找到 status='running' 的作业, 跳过已完成
阶段, 从第一个未完成阶段续跑。

"发布"类阶段 (derive/publish, failure-domain/compute-publish) 内部是单个
SQLite 事务: 要么边、路径索引、修订差异全部落库, 要么全部回滚, 绝不留下
只更新边却没更新路径索引的半成品。
"""

import json


class CrashError(Exception):
    """模拟进程崩溃 (测试/演示用)."""


JOB_TYPES = {}


def register(job_type, stages_fn):
    """stages_fn(store, job_id, payload) -> [(stage_name, callable), ...]"""
    JOB_TYPES[job_type] = stages_fn


def _new_job_id(store, job_type):
    n = int(store.meta("job_seq", "0")) + 1
    store.set_meta("job_seq", n)
    return f"{job_type}-{n}"


def run_job(store, job_type, payload, job_id=None):
    """运行或续跑一个作业, 返回 job_id."""
    if job_type not in JOB_TYPES:
        raise ValueError(f"unknown job type: {job_type}")
    if job_id is None:
        job_id = _new_job_id(store, job_type)
        with store.tx() as c:
            c.execute(
                "INSERT INTO jobs(job_id,type,status,payload_json) VALUES(?,?,?,?)",
                (job_id, job_type, "running", json.dumps(payload)),
            )
    stages = JOB_TYPES[job_type](store, job_id, payload)
    for name, fn in stages:
        if store.checkpoint_done(job_id, name):
            continue  # 该阶段崩溃前已完成, 跳过
        fn()
        with store.tx() as c:
            c.execute(
                "INSERT INTO checkpoints(job_id,stage,data_json) VALUES(?,?,?) "
                "ON CONFLICT(job_id,stage) DO UPDATE SET data_json=excluded.data_json",
                (job_id, name, json.dumps({"done": True})),
            )
        if store.crash_after == name:
            raise CrashError(f"simulated crash after stage '{name}' of {job_id}")
    with store.tx() as c:
        c.execute("UPDATE jobs SET status='done' WHERE job_id=?", (job_id,))
    return job_id


def recover(store):
    """进程重启后的恢复入口: 续跑所有未完成作业, 返回续跑的 job_id 列表.

    崩溃时处于事务中间的阶段已被 SQLite 回滚, 这里只需按检查点续跑;
    阶段函数幂等, 重跑不会产生重复效果。
    """
    resumed = []
    rows = store.q("SELECT job_id, type, payload_json FROM jobs WHERE status='running' "
                   "ORDER BY job_id")
    for row in rows:
        run_job(store, row["type"], json.loads(row["payload_json"]), job_id=row["job_id"])
        resumed.append(row["job_id"])
    return resumed
