"""裁定器: 把受理的证词按采样时刻切成历史区间, 对每个区间内的每条链路
按 来源可信级别 -> 新鲜度 -> 介质规则 的顺序裁定有效边。

双方证词都留存 (obs_a / obs_b), 裁定依据写入 rationale; 冲突 (状态冲突 /
质量冲突) 以 decision='conflict-*' 标记, 供运维界面展示。
"""

from collections import defaultdict

from . import models

_NEG = -(10 ** 18)


def _asserts_up(obs_quality):
    """证词断言: quality>0 表示观测到邻接 (链路存在), 数值是质量样本;
    quality==0 表示显式断言链路 down. 质量是否可用由阻尼器按门限判定,
    低质量样本不会在裁定层直接翻转边。"""
    return obs_quality > 0.0


def _cred_key(obs, trusts):
    """裁定优先级: 可信级别高者优先; 并列时采样时刻新者优先; 再并列按序号."""
    return (trusts.get(obs["observer"], 5), obs["sample_time"], obs["seq"], obs["obs_id"])


def adjudicate_pair(a, b, medium, ta, tb, trusts):
    """对链路 {a,b} 在某区间内的两端证词 (ta: a 的证词, tb: b 的证词, 可缺)
    裁定出有效边状态. 返回 staging_edges 行字典."""
    rule = models.MEDIA[medium]
    base = {"ep_a": a, "ep_b": b, "medium": medium,
            "obs_a": ta["obs_id"] if ta else None,
            "obs_b": tb["obs_id"] if tb else None,
            "winner_obs": None, "quality": 0.0, "state": models.EDGE_DOWN,
            "decision": "", "rationale": ""}

    if ta and tb:
        up_a, up_b = _asserts_up(ta["quality"]), _asserts_up(tb["quality"])
        winner, loser = (ta, tb) if _cred_key(ta, trusts) >= _cred_key(tb, trusts) else (tb, ta)
        cred = (f"obs#{winner['obs_id']}(trust={trusts.get(winner['observer'], 5)},"
                f" t={winner['sample_time']}) over obs#{loser['obs_id']}"
                f"(trust={trusts.get(loser['observer'], 5)}, t={loser['sample_time']})")
        if up_a and up_b:
            base.update(state=models.EDGE_UP, quality=winner["quality"],
                        winner_obs=winner["obs_id"])
            if abs(ta["quality"] - tb["quality"]) > models.QUALITY_CONFLICT_TOLERANCE:
                base.update(decision="conflict-quality",
                            rationale=f"both ends report link up but quality differs "
                                      f"({ta['quality']:.2f} vs {tb['quality']:.2f}); "
                                      f"adopted {cred}")
            else:
                base.update(decision="corroborated",
                            rationale=f"both ends corroborate; quality from {cred}")
        elif up_a != up_b:
            if rule["down_policy"] == "down_wins":
                down_obs = ta if not up_a else tb
                up_obs = tb if not up_a else ta
                base.update(state=models.EDGE_DOWN, quality=0.0,
                            winner_obs=down_obs["obs_id"],
                            decision="conflict-state",
                            rationale=f"state conflict (up vs down); medium '{medium}' "
                                      f"rule down_wins: obs#{down_obs['obs_id']} "
                                      f"(t={down_obs['sample_time']}) is authoritative "
                                      f"over obs#{up_obs['obs_id']}")
            else:
                base.update(state=models.EDGE_UP if _asserts_up(winner["quality"])
                            else models.EDGE_DOWN,
                            quality=winner["quality"] if _asserts_up(winner["quality"]) else 0.0,
                            winner_obs=winner["obs_id"],
                            decision="conflict-state",
                            rationale=f"state conflict (up vs down) resolved by "
                                      f"trust/freshness: adopted {cred}")
        else:
            base.update(state=models.EDGE_DOWN, quality=0.0,
                        winner_obs=winner["obs_id"], decision="both-down",
                        rationale=f"both ends report link down; later/more-trusted "
                                  f"testimony is {cred}")
    else:
        t = ta or tb
        side = a if ta else b
        base.update(obs_a=t["obs_id"] if ta else None,
                    obs_b=t["obs_id"] if tb else None,
                    winner_obs=t["obs_id"], quality=t["quality"])
        if rule["confirm"] == "single":
            up = _asserts_up(t["quality"])
            base.update(state=models.EDGE_UP if up else models.EDGE_DOWN,
                        quality=t["quality"] if up else 0.0,
                        decision="single-sided",
                        rationale=f"medium '{medium}' accepts single-sided testimony "
                                  f"from {side} (obs#{t['obs_id']})")
        else:
            base.update(state=models.EDGE_UNCONFIRMED, quality=t["quality"],
                        decision="unconfirmed",
                        rationale=f"medium '{medium}' requires testimony from both ends; "
                                  f"only {side} testified (obs#{t['obs_id']})")
    return base


def compute_epochs(observations, boundary):
    """按所有证词区间端点 + 封存界限切分历史区间 (与到达顺序无关)."""
    points = set()
    for o in observations:
        points.add(o["sample_time"])
        points.add(o["sample_time"] + o["ttl"])
    if boundary is not None:
        points.add(boundary)
    pts = sorted(points)
    return [(pts[i], pts[i + 1]) for i in range(len(pts) - 1)]


def stage_adjudicate(store, job_id):
    """推导作业阶段1: 重放全部受理证词, 产出每个历史区间的裁定结果到暂存区."""
    obs = store.q("SELECT * FROM observations WHERE status=? ORDER BY obs_id",
                  (models.ST_ACCEPTED,))
    boundary = store.seal_boundary()
    trusts = {r["node_id"]: r["trust"] for r in store.q("SELECT node_id,trust FROM nodes")}

    streams = defaultdict(list)
    pair_media = set()
    for o in obs:
        a, b = sorted((o["observer"], o["neighbor"]))
        streams[(o["observer"], o["neighbor"], o["medium"])].append(o)
        pair_media.add((a, b, o["medium"]))

    def best_covering(observer, neighbor, medium, s, e):
        best = None
        for o in streams.get((observer, neighbor, medium), ()):
            if o["sample_time"] <= s and o["sample_time"] + o["ttl"] >= e:
                if best is None or (o["sample_time"], o["seq"], o["obs_id"]) >= \
                                   (best["sample_time"], best["seq"], best["obs_id"]):
                    best = o
        return best

    epochs = compute_epochs(obs, boundary)
    rows = []
    for (s, e) in epochs:
        for (a, b, m) in sorted(pair_media):
            ta = best_covering(a, b, m, s, e)
            tb = best_covering(b, a, m, s, e)
            if ta is None and tb is None:
                continue
            row = adjudicate_pair(a, b, m, ta, tb, trusts)
            row.update(epoch_start=s, epoch_end=e)
            rows.append(row)

    with store.tx() as c:  # 幂等: 先清后写
        c.execute("DELETE FROM staging_epochs WHERE job_id=?", (job_id,))
        c.execute("DELETE FROM staging_edges WHERE job_id=?", (job_id,))
        c.executemany("INSERT INTO staging_epochs(job_id,epoch_start,epoch_end) "
                      "VALUES(?,?,?)", [(job_id, s, e) for (s, e) in epochs])
        c.executemany(
            "INSERT INTO staging_edges(job_id,epoch_start,epoch_end,ep_a,ep_b,medium,"
            " quality,state,usable,obs_a,obs_b,winner_obs,decision,rationale) "
            "VALUES(?,?,?,?,?,?,?,?,0,?,?,?,?,?)",
            [(job_id, r["epoch_start"], r["epoch_end"], r["ep_a"], r["ep_b"],
              r["medium"], r["quality"], r["state"], r["obs_a"], r["obs_b"],
              r["winner_obs"], r["decision"], r["rationale"]) for r in rows])
