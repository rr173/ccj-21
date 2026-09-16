"""摄取层: 运维登记、在线租约、邻接观测受理.

受理规则 (在单个事务内完成, 崩溃后可安全重试 —— 幂等去重):
  * 同一观察者的序号只能前进: seq < last_seq 拒绝并留痕 (regressed);
  * 相同序号再次到达只采信一次: 内容相同记 duplicate, 内容不同记 seq_conflict,
    均不改变已受理证词;
  * 到达先后不同于采样先后时, 证词仍按 sample_time 归入正确的历史区间
    (推导阶段按采样时刻切分历史区间, 与到达顺序无关);
  * sample_time 早于封存界限的证词进入旁路档案 (status=sidecar), 序号通道
    照常前进, 但绝不改写已封存的修订.

在线租约只登记节点可达性, 与邻接证词完全分离, 不参与拓扑推导。
"""

import hashlib
import json

from . import jobs, models


# ---------------------------------------------------------------- 运维登记

def register_room(store, name, note=None):
    with store.tx() as c:
        c.execute("INSERT INTO rooms(name,note) VALUES(?,?) "
                  "ON CONFLICT(name) DO UPDATE SET note=COALESCE(excluded.note, rooms.note)",
                  (name, note))


def register_domain(store, name, room=None, note=None):
    with store.tx() as c:
        c.execute("INSERT INTO domains(name,room,note) VALUES(?,?,?) "
                  "ON CONFLICT(name) DO UPDATE SET room=COALESCE(excluded.room, domains.room), "
                  "note=COALESCE(excluded.note, domains.note)",
                  (name, room, note))


def register_node(store, node_id, room=None, domain=None, trust=None,
                  is_exit=None, exit_name=None):
    with store.tx() as c:
        c.execute(
            "INSERT INTO nodes(node_id,room,domain,trust,is_exit,exit_name) "
            "VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(node_id) DO UPDATE SET "
            " room=COALESCE(?, nodes.room), domain=COALESCE(?, nodes.domain), "
            " trust=COALESCE(?, nodes.trust), is_exit=COALESCE(?, nodes.is_exit), "
            " exit_name=COALESCE(?, nodes.exit_name)",
            (node_id, room, domain, trust if trust is not None else 5,
             int(is_exit or 0), exit_name,
             room, domain, trust, None if is_exit is None else int(is_exit), exit_name),
        )


def ensure_node(store, conn, node_id):
    conn.execute("INSERT OR IGNORE INTO nodes(node_id) VALUES(?)", (node_id,))


def add_lease(store, node_id, starts, ends, source="operator"):
    """登记在线租约 —— 只表示节点可达, 不产生任何邻接边."""
    with store.tx() as c:
        ensure_node(store, c, node_id)
        c.execute("INSERT INTO leases(node_id,starts,ends,source) VALUES(?,?,?,?)",
                  (node_id, starts, ends, source))


def online_nodes(store, t):
    """t 时刻持有有效租约的节点 (可达性视图, 与邻接视图并列展示)."""
    rows = store.q("SELECT DISTINCT node_id FROM leases WHERE starts<=? AND ends>?",
                   (t, t))
    return sorted(r["node_id"] for r in rows)


# ---------------------------------------------------------------- 观测受理

def _hash_payload(observer, seq, sample_time, neighbor, medium, quality, ttl):
    canon = json.dumps([observer, seq, sample_time, neighbor, medium,
                        round(float(quality), 6), ttl], separators=(",", ":"))
    return hashlib.sha1(canon.encode()).hexdigest()


def ingest_observation(store, observer, seq, sample_time, neighbor,
                       medium, quality, ttl):
    """受理一条邻接观测, 返回 {status, reason, arrival}.

    status: accepted | sidecar | duplicate | seq_conflict | regressed | bad_medium
    """
    payload = dict(observer=observer, seq=seq, sample_time=sample_time,
                   neighbor=neighbor, medium=medium, quality=quality, ttl=ttl)
    with store.tx() as c:
        arrival = store.next_counter("arrival_seq")
        if medium not in models.MEDIA:
            _reject(c, observer, seq, arrival, "bad_medium", payload)
            return {"status": "bad_medium", "reason": f"unknown medium '{medium}'",
                    "arrival": arrival}

        ensure_node(store, c, observer)
        ensure_node(store, c, neighbor)

        content_hash = _hash_payload(observer, seq, sample_time, neighbor,
                                     medium, quality, ttl)
        st = c.execute("SELECT last_seq FROM observer_state WHERE observer_id=?",
                       (observer,)).fetchone()
        last_seq = st["last_seq"] if st else None

        if last_seq is not None and seq < last_seq:
            _reject(c, observer, seq, arrival, "regressed", payload)
            return {"status": "regressed",
                    "reason": f"seq {seq} < last accepted seq {last_seq}; "
                              f"observer sequence must advance",
                    "arrival": arrival}

        if last_seq is not None and seq == last_seq:
            prev = c.execute(
                "SELECT content_hash FROM observations WHERE observer=? AND seq=?",
                (observer, seq)).fetchone()
            if prev and prev["content_hash"] == content_hash:
                _reject(c, observer, seq, arrival, "duplicate", payload)
                return {"status": "duplicate",
                        "reason": f"seq {seq} already accepted with identical content; "
                                  f"accepted only once",
                        "arrival": arrival}
            _reject(c, observer, seq, arrival, "seq_conflict", payload)
            return {"status": "seq_conflict",
                    "reason": f"seq {seq} already accepted with different content; "
                              f"first testimony retained",
                    "arrival": arrival}

        # seq 前进: 受理. 早于封存界限的证词进入旁路档案, 不改写既有修订.
        boundary = store.seal_boundary()
        status, reason = models.ST_ACCEPTED, None
        if boundary is not None and sample_time < boundary:
            status = models.ST_SIDECAR
            reason = (f"sample_time {sample_time} precedes seal boundary {boundary}; "
                      f"archived to sidecar, sealed revisions left untouched")

        c.execute(
            "INSERT INTO observations(observer,seq,sample_time,neighbor,medium,"
            " quality,ttl,arrival,content_hash,status,reason) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (observer, seq, sample_time, neighbor, medium, float(quality), ttl,
             arrival, content_hash, status, reason))
        c.execute(
            "INSERT INTO observer_state(observer_id,last_seq) VALUES(?,?) "
            "ON CONFLICT(observer_id) DO UPDATE SET last_seq=excluded.last_seq",
            (observer, seq))
        if status == models.ST_ACCEPTED:
            store.set_meta("dirty", "1")
        return {"status": status, "reason": reason, "arrival": arrival}


def _reject(conn, observer, seq, arrival, reason, payload):
    conn.execute(
        "INSERT INTO rejects(observer,seq,arrival,reason,payload_json) VALUES(?,?,?,?,?)",
        (observer, seq, arrival, reason, json.dumps(payload)))


# ---------------------------------------------------------------- 批量摄取作业

def _ingest_file_stages(store, job_id, payload):
    path = payload["path"]

    def stage_ingest():
        ck = store.checkpoint_get(job_id, "ingest") or {}
        next_line = ck.get("next_line", 0)
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i < next_line:
                    continue
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    ingest_observation(store, **rec)
                # 每行一个持久检查点: 崩溃后从下一行续跑, 已受理行幂等去重
                with store.tx() as c:
                    store.checkpoint_put(job_id, "ingest",
                                         {"next_line": i + 1, "done": False})
                if store.crash_after == f"ingest-line-{i + 1}":
                    raise jobs.CrashError(
                        f"simulated crash after ingesting line {i + 1} of {path}")

    return [("ingest", stage_ingest)]


jobs.register("ingest-file", _ingest_file_stages)


def ingest_file(store, path):
    return jobs.run_job(store, "ingest-file", {"path": path})
