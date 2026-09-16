"""拓扑推导作业: adjudicate -> damp -> publish, 以及历史封存.

publish 阶段是单个事务, 一次性落库: 新修订号、当前视图、区间->修订映射、
修订差异 (新增/撤销/改判)、路径索引 (连通分量/最短路/割点)。崩溃即整体
回滚, recover() 从检查点续跑, 绝不留下边与路径索引不一致的半成品。

迟到证词 (sample_time 落在未封存历史) 会改变对应区间的裁定, 推导产生
新修订并在 revision_diffs 列明变化; 早于封存界限的证词在摄取层就被
分流到旁路档案, 不会到达这里。
"""

import json

from . import adjudicate, damping, graph, jobs, models


def _publish_stages(store, job_id, payload):
    return [
        ("adjudicate", lambda: adjudicate.stage_adjudicate(store, job_id)),
        ("damp", lambda: damping.stage_damp(store, job_id)),
        ("publish", lambda: stage_publish(store, job_id, payload)),
    ]


jobs.register("derive", _publish_stages)


def derive(store, trigger="manual", note=None, force=False):
    """有未消化证词时运行一次推导作业, 返回 (job_id, rev_id|None)."""
    if not force and store.meta("dirty") != "1":
        return None, None
    before = store.q1("SELECT MAX(rev_id) AS r FROM revisions")["r"]
    job_id = jobs.run_job(store, "derive", {"trigger": trigger, "note": note})
    after = store.q1("SELECT MAX(rev_id) AS r FROM revisions")["r"]
    return job_id, (after if after != before else None)


def stage_publish(store, job_id, payload):
    """阶段3: 原子发布新修订. 全部写操作在一个事务里."""
    boundary = store.seal_boundary()
    lo = boundary if boundary is not None else -(10 ** 18)

    epochs = store.q("SELECT epoch_start, epoch_end FROM staging_epochs "
                     "WHERE job_id=? AND epoch_start>=? ORDER BY epoch_start",
                     (job_id, lo))
    edges = store.q("SELECT * FROM staging_edges WHERE job_id=? AND epoch_start>=? "
                    "ORDER BY epoch_start, ep_a, ep_b, medium", (job_id, lo))
    nodes = sorted(r["node_id"] for r in store.q("SELECT node_id FROM nodes"))

    with store.tx() as c:
        cur = c.execute("INSERT INTO revisions(trigger,note,job_id) VALUES(?,?,?)",
                        (payload.get("trigger", "manual"),
                         payload.get("note"), job_id))
        rev_id = cur.lastrowid

        # --- 修订差异: 新视图 vs 当前视图 (未封存部分) ---
        old = {(r["epoch_start"], r["ep_a"], r["ep_b"], r["medium"]): r
               for r in c.execute(
                   "SELECT * FROM current_edges WHERE epoch_start>=?", (lo,))}
        new = {(r["epoch_start"], r["ep_a"], r["ep_b"], r["medium"]): r
               for r in edges}
        diffs = []
        for k in sorted(new.keys() - old.keys()):
            diffs.append((rev_id, "added", k, json.dumps({"after": _brief(new[k])})))
        for k in sorted(old.keys() - new.keys()):
            diffs.append((rev_id, "withdrawn", k, json.dumps({"before": _brief(old[k])})))
        for k in sorted(new.keys() & old.keys()):
            if _material(old[k]) != _material(new[k]):
                diffs.append((rev_id, "readjudicated", k,
                              json.dumps({"before": _brief(old[k]),
                                          "after": _brief(new[k])})))

        # --- 当前视图与区间映射 (只动未封存部分; 封存行永不改写) ---
        c.execute("DELETE FROM current_edges WHERE epoch_start>=?", (lo,))
        c.execute("DELETE FROM epochs WHERE epoch_start>=?", (lo,))
        c.executemany(
            "INSERT INTO current_edges(epoch_start,epoch_end,ep_a,ep_b,medium,quality,"
            " state,usable,obs_a,obs_b,winner_obs,decision,rationale,damp_note) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(r["epoch_start"], r["epoch_end"], r["ep_a"], r["ep_b"], r["medium"],
              r["quality"], r["state"], r["usable"], r["obs_a"], r["obs_b"],
              r["winner_obs"], r["decision"], r["rationale"], r["damp_note"])
             for r in edges])
        c.executemany("INSERT INTO epochs(epoch_start,epoch_end,rev_id,sealed) "
                      "VALUES(?,?,?,0)",
                      [(e["epoch_start"], e["epoch_end"], rev_id) for e in epochs])
        c.executemany(
            "INSERT INTO revision_edges(rev_id,epoch_start,epoch_end,ep_a,ep_b,medium,"
            " quality,state,usable,obs_a,obs_b,winner_obs,decision,rationale,damp_note) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(rev_id, r["epoch_start"], r["epoch_end"], r["ep_a"], r["ep_b"],
              r["medium"], r["quality"], r["state"], r["usable"], r["obs_a"],
              r["obs_b"], r["winner_obs"], r["decision"], r["rationale"],
              r["damp_note"]) for r in edges])
        c.executemany(
            "INSERT INTO revision_diffs(rev_id,change,epoch_start,ep_a,ep_b,medium,"
            " detail_json) VALUES(?,?,?,?,?,?,?)",
            [(r, ch, k[0], k[1], k[2], k[3], detail) for (r, ch, k, detail) in diffs])

        if store.crash_mid_publish:
            raise jobs.CrashError(f"simulated crash mid-publish of rev {rev_id}")

        # --- 路径索引: 与边同一事务, 保证无半成品 ---
        by_epoch = {}
        for r in edges:
            if r["usable"]:
                by_epoch.setdefault(r["epoch_start"], []).append(r)
        comp_rows, path_rows, art_rows = [], [], []
        for e in epochs:
            s = e["epoch_start"]
            adj = graph.build_adj(nodes, by_epoch.get(s, []))
            for node, cid in graph.components(nodes, adj).items():
                comp_rows.append((rev_id, s, node, cid))
            for src in nodes:
                best, hops = graph.dijkstra(adj, src)
                counts = graph.min_path_counts(adj, src, best)
                for dst, (cost, path) in best.items():
                    if dst == src:
                        continue
                    path_rows.append((rev_id, s, src, dst, cost,
                                      json.dumps(list(path)),
                                      json.dumps(hops[dst]),
                                      1 if counts.get(dst, 0) > 1 else 0))
            for node in sorted(graph.articulation_points(nodes, adj)):
                art_rows.append((rev_id, s, node))
        c.executemany("INSERT INTO components(rev_id,epoch_start,node,component) "
                      "VALUES(?,?,?,?)", comp_rows)
        c.executemany("INSERT INTO paths(rev_id,epoch_start,src,dst,cost_micro,"
                      " path_json,hops_json,tie_broken) VALUES(?,?,?,?,?,?,?,?)",
                      path_rows)
        c.executemany("INSERT INTO articulation(rev_id,epoch_start,node) "
                      "VALUES(?,?,?)", art_rows)

        store.set_meta("dirty", "0")
        # 暂存区随发布清理 (同事务)
        c.execute("DELETE FROM staging_edges WHERE job_id=?", (job_id,))
        c.execute("DELETE FROM staging_epochs WHERE job_id=?", (job_id,))


def _material(r):
    """参与"改判"判定的实质字段."""
    return (r["state"], r["usable"], round(r["quality"], 6),
            r["winner_obs"], r["decision"])


def _brief(r):
    return {"state": r["state"], "usable": r["usable"],
            "quality": r["quality"], "winner_obs": r["winner_obs"],
            "decision": r["decision"], "epoch_end": r["epoch_end"]}


def seal(store, boundary_time):
    """封存 boundary_time 之前的历史: 当前视图冻结, 之后到的更早证词进旁路."""
    cur = store.seal_boundary()
    if cur is not None and boundary_time <= cur:
        raise ValueError(f"seal boundary must advance (current {cur})")
    with store.tx() as c:
        # 跨界限的区间在界限处切开, 前半封存
        row = c.execute("SELECT * FROM epochs WHERE epoch_start<? AND epoch_end>?",
                        (boundary_time, boundary_time)).fetchone()
        if row:
            s, e, rev = row["epoch_start"], row["epoch_end"], row["rev_id"]
            c.execute("UPDATE epochs SET epoch_end=? WHERE epoch_start=?",
                      (boundary_time, s))
            c.execute("INSERT INTO epochs(epoch_start,epoch_end,rev_id,sealed) "
                      "VALUES(?,?,?,0)", (boundary_time, e, rev))
            half = c.execute("SELECT * FROM current_edges WHERE epoch_start=?",
                             (s,)).fetchall()
            c.execute("UPDATE current_edges SET epoch_end=? WHERE epoch_start=?",
                      (boundary_time, s))
            c.executemany(
                "INSERT INTO current_edges(epoch_start,epoch_end,ep_a,ep_b,medium,"
                " quality,state,usable,obs_a,obs_b,winner_obs,decision,rationale,"
                " damp_note) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(boundary_time, e, r["ep_a"], r["ep_b"], r["medium"], r["quality"],
                  r["state"], r["usable"], r["obs_a"], r["obs_b"], r["winner_obs"],
                  r["decision"], r["rationale"], r["damp_note"]) for r in half])
            for table, cols in (("components", "node,component"),
                                ("paths", "src,dst,cost_micro,path_json,hops_json,"
                                          "tie_broken"),
                                ("articulation", "node")):
                rows = c.execute(
                    f"SELECT * FROM {table} WHERE rev_id=? AND epoch_start=?",
                    (rev, s)).fetchall()
                for r in rows:
                    vals = [rev, boundary_time] + [r[col] for col in cols.split(",")]
                    c.execute(f"INSERT OR IGNORE INTO {table}"
                              f"(rev_id,epoch_start,{cols}) "
                              f"VALUES({','.join('?' * (2 + len(vals[2:])))})", vals)
        c.execute("UPDATE epochs SET sealed=1 WHERE epoch_end<=?", (boundary_time,))
        store.set_meta("seal_boundary", boundary_time)
