"""故障域计算作业: 指定节点在某时刻失联时的受影响范围.

分两阶段: snapshot (把该时刻的可用拓扑快照进暂存区) -> compute-publish
(单事务计算并落库)。崩溃后 recover() 从检查点续跑; 结果按 job_id 留档,
运维界面可查询故障域成员、丢失的上联出口与受影响机房/交换域。
"""

import json

from . import graph, jobs, models


def _fd_stages(store, job_id, payload):
    return [
        ("snapshot", lambda: _stage_snapshot(store, job_id, payload)),
        ("compute-publish", lambda: _stage_compute(store, job_id, payload)),
    ]


jobs.register("failure-domain", _fd_stages)


def _resolve_epoch(store, t):
    row = store.q1("SELECT epoch_start, epoch_end, rev_id, sealed FROM epochs "
                   "WHERE epoch_start<=? AND epoch_end>? ORDER BY epoch_start DESC "
                   "LIMIT 1", (t, t))
    return row


def _stage_snapshot(store, job_id, payload):
    t = payload["at"]
    ep = _resolve_epoch(store, t)
    with store.tx() as c:
        c.execute("DELETE FROM staging_edges WHERE job_id=?", (job_id,))
        c.execute("DELETE FROM staging_epochs WHERE job_id=?", (job_id,))
        if not ep:
            return
        rows = store.q("SELECT * FROM current_edges WHERE epoch_start=? AND usable=1",
                       (ep["epoch_start"],))
        c.execute("INSERT INTO staging_epochs(job_id,epoch_start,epoch_end) "
                  "VALUES(?,?,?)", (job_id, ep["epoch_start"], ep["epoch_end"]))
        c.executemany(
            "INSERT INTO staging_edges(job_id,epoch_start,epoch_end,ep_a,ep_b,medium,"
            " quality,state,usable,decision,rationale) VALUES(?,?,?,?,?,?,?,?,1,?,?)",
            [(job_id, ep["epoch_start"], ep["epoch_end"], r["ep_a"], r["ep_b"],
              r["medium"], r["quality"], r["state"], r["decision"], r["rationale"])
             for r in rows])
        store.checkpoint_put(job_id, "snapshot-meta",
                             {"rev_id": ep["rev_id"], "epoch_start": ep["epoch_start"],
                              "epoch_end": ep["epoch_end"]})


def _stage_compute(store, job_id, payload):
    failed = payload["node"]
    meta = store.checkpoint_get(job_id, "snapshot-meta")
    nodes = sorted(r["node_id"] for r in store.q("SELECT node_id FROM nodes"))
    node_info = {r["node_id"]: dict(r) for r in store.q("SELECT * FROM nodes")}
    exits = sorted(r["node_id"] for r in
                   store.q("SELECT node_id FROM nodes WHERE is_exit=1"))
    with store.tx() as c:
        c.execute("DELETE FROM failure_domains WHERE job_id=?", (job_id,))
        if not meta:
            store.checkpoint_put(job_id, "result",
                                 {"error": f"no topology epoch covers t={payload['at']}"})
            return
        edge_rows = store.q("SELECT * FROM staging_edges WHERE job_id=?", (job_id,))
        adj = graph.build_adj(nodes, edge_rows)
        members = graph.failure_domain(nodes, adj, failed, exits, node_info)
        for m in members:
            c.execute(
                "INSERT INTO failure_domains(job_id,rev_id,epoch_start,failed_node,"
                " member,room,domain,before_component,after_component,"
                " lost_exits_json,lost_peers) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (job_id, meta["rev_id"], meta["epoch_start"], failed, m["member"],
                 m["room"], m["domain"], m["before_component"], m["after_component"],
                 json.dumps(m["lost_exits"]), m["lost_peers"]))
        affected = [m for m in members if m["lost_exits"] or m["lost_peers"] > 0]
        store.checkpoint_put(job_id, "result",
                             {"rev_id": meta["rev_id"],
                              "epoch_start": meta["epoch_start"],
                              "epoch_end": meta["epoch_end"],
                              "failed": failed,
                              "affected": len(affected),
                              "members": len(members)})
        c.execute("DELETE FROM staging_edges WHERE job_id=?", (job_id,))
        c.execute("DELETE FROM staging_epochs WHERE job_id=?", (job_id,))


def failure_domain(store, node, at):
    """运行故障域作业并返回结构化结果."""
    job_id = jobs.run_job(store, "failure-domain", {"node": node, "at": at})
    result = store.checkpoint_get(job_id, "result") or {}
    rows = [dict(r) for r in store.q(
        "SELECT * FROM failure_domains WHERE job_id=? ORDER BY member", (job_id,))]
    for r in rows:  # 运维视图: 解析丢失出口列表
        r["lost_exits"] = json.loads(r.pop("lost_exits_json"))
    return {"job_id": job_id, "result": result, "members": rows}
