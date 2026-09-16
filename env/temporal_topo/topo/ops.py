"""运维查询层: 指定时刻的修订号、连通分量、路径解释、割点、证词冲突、
故障域成员、旁路原因、修订差异, 以及沿任意边追到原始观测。

注意分层: 租约 (online) 只表示节点可达; 邻接证词 (usable 边) 才表示相连。
两处视图并列展示, 互不代替。
"""

import json

from . import ingest, models


# ---------------------------------------------------------------- 时刻 -> 修订

def revision_at(store, t):
    """指定时刻采用的修订号 (封存区间返回冻结时的修订)."""
    row = store.q1("SELECT epoch_start, epoch_end, rev_id, sealed FROM epochs "
                   "WHERE epoch_start<=? AND epoch_end>?", (t, t))
    if row:
        return {"time": t, "rev_id": row["rev_id"],
                "epoch": [row["epoch_start"], row["epoch_end"]],
                "sealed": bool(row["sealed"])}
    latest = store.q1("SELECT MAX(rev_id) AS r FROM revisions")
    return {"time": t, "rev_id": latest["r"] if latest else None,
            "epoch": None, "sealed": False,
            "note": "no topology epoch covers this time"}


def _epoch_of(store, t):
    row = revision_at(store, t)
    if not row["epoch"]:
        return None
    return row


# ---------------------------------------------------------------- 状态总览

def status_at(store, t):
    ep = _epoch_of(store, t)
    online = ingest.online_nodes(store, t)
    out = {"time": t, "revision": ep["rev_id"] if ep else None,
           "sealed": ep["sealed"] if ep else None,
           "seal_boundary": store.seal_boundary(),
           "online_by_lease": online, "components": []}
    if not ep:
        # 该时刻没有任何邻接区间: 所有登记节点各自孤立 (租约不产生邻接)
        exits = {r["node_id"] for r in store.q("SELECT node_id FROM nodes WHERE is_exit=1")}
        for r in store.q("SELECT node_id FROM nodes ORDER BY node_id"):
            n = r["node_id"]
            out["components"].append({
                "component": n, "members": [n],
                "exits": [n] if n in exits else [],
                "lease_only": [n] if n in online else []})
        return out
    rows = store.q("SELECT node, component FROM components WHERE rev_id=? AND "
                   "epoch_start=? ORDER BY component, node",
                   (ep["rev_id"], ep["epoch"][0]))
    comps = {}
    for r in rows:
        comps.setdefault(r["component"], []).append(r["node"])
    exits = {r["node_id"] for r in store.q("SELECT node_id FROM nodes WHERE is_exit=1")}
    for cid in sorted(comps):
        members = comps[cid]
        out["components"].append({
            "component": cid, "members": members,
            "exits": sorted(exits & set(members)),
            "lease_only": [n for n in members if n in online and
                           not _has_edge(store, ep, n)],
        })
    return out


def _has_edge(store, ep, node):
    row = store.q1("SELECT 1 FROM current_edges WHERE epoch_start=? AND usable=1 "
                   "AND (ep_a=? OR ep_b=?) LIMIT 1",
                   (ep["epoch"][0], node, node))
    return row is not None


def components_at(store, t):
    return status_at(store, t)["components"]


# ---------------------------------------------------------------- 路径与解释

def path_at(store, src, dst, t):
    """最短可用路径 + 完整解释 (采用的修订、逐跳代价、决胜规则、原始观测)."""
    ep = _epoch_of(store, t)
    if not ep:
        return {"ok": False, "reason": f"no topology epoch covers t={t}"}
    rev, es = ep["rev_id"], ep["epoch"][0]
    row = store.q1("SELECT * FROM paths WHERE rev_id=? AND epoch_start=? "
                   "AND src=? AND dst=?", (rev, es, src, dst))
    if not row:
        c1 = store.q1("SELECT component FROM components WHERE rev_id=? AND "
                      "epoch_start=? AND node=?", (rev, es, src))
        c2 = store.q1("SELECT component FROM components WHERE rev_id=? AND "
                      "epoch_start=? AND node=?", (rev, es, dst))
        return {"ok": False, "reason": "unreachable",
                "detail": f"{src} in component {c1['component'] if c1 else '?'}, "
                          f"{dst} in component {c2['component'] if c2 else '?'}",
                "revision": rev, "epoch": ep["epoch"]}
    hops = json.loads(row["hops_json"])
    for h in hops:
        a, b = sorted((h["from"], h["to"]))
        edge = store.q1("SELECT * FROM current_edges WHERE epoch_start=? AND "
                        "ep_a=? AND ep_b=? AND medium=?",
                        (es, a, b, h["medium"]))
        if edge:
            h["winner_obs"] = edge["winner_obs"]
            h["decision"] = edge["decision"]
            obs = store.q1("SELECT obs_id,observer,seq,sample_time,quality,ttl "
                           "FROM observations WHERE obs_id=?",
                           (edge["winner_obs"],))
            if obs:
                h["testimony"] = dict(obs)
    return {"ok": True, "revision": rev, "epoch": ep["epoch"],
            "sealed": ep["sealed"],
            "path": json.loads(row["path_json"]),
            "cost_micro": row["cost_micro"],
            "cost": models.cost_str(row["cost_micro"]),
            "tie_broken": bool(row["tie_broken"]),
            "tie_rule": "equal-cost paths resolved by lexicographically smallest "
                        "node sequence" if row["tie_broken"] else None,
            "hops": hops}


def cutpoints_at(store, t):
    ep = _epoch_of(store, t)
    if not ep:
        return []
    rows = store.q("SELECT node FROM articulation WHERE rev_id=? AND epoch_start=? "
                   "ORDER BY node", (ep["rev_id"], ep["epoch"][0]))
    return [r["node"] for r in rows]


# ---------------------------------------------------------------- 冲突与追溯

def conflicts_at(store, t=None):
    """证词冲突列表 (含双方证词与裁定依据); 不给时刻则列出当前视图全部."""
    sql = ("SELECT c.* FROM current_edges c WHERE c.decision LIKE 'conflict%'")
    args = []
    if t is not None:
        ep = _epoch_of(store, t)
        if not ep:
            return []
        sql += " AND c.epoch_start=?"
        args.append(ep["epoch"][0])
    sql += " ORDER BY c.epoch_start, c.ep_a, c.ep_b"
    out = []
    for r in store.q(sql, args):
        d = dict(r)
        d["testimonies"] = _obs_pair(store, r["obs_a"], r["obs_b"])
        out.append(d)
    return out


def _obs_pair(store, obs_a, obs_b):
    pair = []
    for oid in (obs_a, obs_b):
        if oid is None:
            continue
        row = store.q1("SELECT obs_id,observer,seq,sample_time,neighbor,medium,"
                       "quality,ttl,status FROM observations WHERE obs_id=?", (oid,))
        if row:
            pair.append(dict(row))
    return pair


def edge_trace(store, a, b, medium=None):
    """沿一条边追到原始观测: 当前视图各区间裁定 + 双方证词原文."""
    a, b = sorted((a, b))
    sql = ("SELECT * FROM current_edges WHERE ep_a=? AND ep_b=?")
    args = [a, b]
    if medium:
        sql += " AND medium=?"
        args.append(medium)
    sql += " ORDER BY epoch_start"
    out = []
    for r in store.q(sql, args):
        d = dict(r)
        d["testimonies"] = _obs_pair(store, r["obs_a"], r["obs_b"])
        out.append(d)
    return out


# ---------------------------------------------------------------- 档案与差异

def sidecar(store):
    """旁路档案: 早于封存界限的证词及原因."""
    return [dict(r) for r in store.q(
        "SELECT * FROM observations WHERE status=? ORDER BY obs_id",
        (models.ST_SIDECAR,))]


def rejects(store):
    return [dict(r) for r in store.q("SELECT * FROM rejects ORDER BY id")]


def revisions(store):
    out = []
    for r in store.q("SELECT * FROM revisions ORDER BY rev_id"):
        counts = {c: 0 for c in ("added", "withdrawn", "readjudicated")}
        for d in store.q("SELECT change, COUNT(*) AS n FROM revision_diffs "
                         "WHERE rev_id=? GROUP BY change", (r["rev_id"],)):
            counts[d["change"]] = d["n"]
        out.append({"rev_id": r["rev_id"], "trigger": r["trigger"],
                    "note": r["note"], "job_id": r["job_id"], "diff": counts})
    return out


def diff_revisions(store, r1, r2):
    """两个修订间的差异 (基于各自留档的边集直接比对)."""
    def rows_of(rev):
        return {(r["epoch_start"], r["ep_a"], r["ep_b"], r["medium"]): dict(r)
                for r in store.q("SELECT * FROM revision_edges WHERE rev_id=?", (rev,))}
    a, b = rows_of(r1), rows_of(r2)
    def mat(r):
        return (r["state"], r["usable"], round(r["quality"], 6),
                r["winner_obs"], r["decision"])
    out = []
    for k in sorted(b.keys() - a.keys()):
        out.append({"change": "added", "key": k, "after": b[k]})
    for k in sorted(a.keys() - b.keys()):
        out.append({"change": "withdrawn", "key": k, "before": a[k]})
    for k in sorted(a.keys() & b.keys()):
        if mat(a[k]) != mat(b[k]):
            out.append({"change": "readjudicated", "key": k,
                        "before": a[k], "after": b[k]})
    return out
