#!/usr/bin/env python3
"""端到端演示: 从观测摄取到时态拓扑运维的完整故事.

运行: python3 demo.py   (在 temporal_topo/ 目录下, 生成 demo.db)
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from topo import derive as derive_mod
from topo import failure as failure_mod
from topo import ingest, jobs, ops
from topo.store import Store

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "demo.db")
for suffix in ("", "-wal", "-shm"):
    if os.path.exists(DB + suffix):
        os.remove(DB + suffix)

store = Store(DB)


def h(title):
    print(f"\n{'=' * 72}\n## {title}\n{'=' * 72}")


def pp(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------- 1. 运维登记
h("1. 登记机房 / 交换域 / 节点 / 上联出口 / 在线租约")
ingest.register_room(store, "room-A")
ingest.register_room(store, "room-B")
ingest.register_domain(store, "sw-1", room="room-A")
ingest.register_domain(store, "sw-2", room="room-B")
ingest.register_node(store, "n1", room="room-A", domain="sw-1", trust=8)
ingest.register_node(store, "n2", room="room-A", domain="sw-1", trust=6)
ingest.register_node(store, "n3", room="room-A", domain="sw-1", trust=4)
ingest.register_node(store, "n4", room="room-B", domain="sw-2", trust=6)
ingest.register_node(store, "n5", room="room-B", domain="sw-2", trust=8,
                     is_exit=True, exit_name="uplink-1")
for n in ("n1", "n2", "n3", "n4", "n5"):
    ingest.add_lease(store, n, 0, 100000, source="dhcp")
print("5 nodes registered (n5 = uplink exit), leases [0,100000) — 租约只表示可达")

# ---------------------------------------------------------------- 2. 邻接观测
h("2. 邻接观测摄取 -> 自动推导拓扑修订")
obs = ingest.ingest_observation
print(obs(store, "n1", 1, 100, "n2", "ethernet", 0.95, 50000))
print(obs(store, "n2", 1, 100, "n1", "ethernet", 0.93, 50000))
print(obs(store, "n2", 2, 100, "n3", "radio", 0.80, 50000))
print(obs(store, "n3", 1, 100, "n4", "wifi", 0.70, 50000))
print(obs(store, "n4", 1, 100, "n3", "wifi", 0.72, 50000))
print(obs(store, "n4", 2, 100, "n5", "fiber", 0.99, 50000))
print(obs(store, "n5", 1, 100, "n4", "fiber", 0.99, 50000))
print(obs(store, "n2", 3, 100, "n4", "radio", 0.50, 50000))  # 低质量备用链路
job_id, rev = derive_mod.derive(store, trigger="initial observations")
print(f"-> revision {rev} (job {job_id})")

h("2b. 同一时刻的连通分量与在线视图 (租约 != 邻接)")
st = ops.status_at(store, 1000)
pp({"revision": st["revision"], "online_by_lease": st["online_by_lease"],
    "components": st["components"]})

# ---------------------------------------------------------------- 3. 证词冲突
h("3. 两端证词冲突: 按可信级别 / 新鲜度 / 介质规则裁定, 双方证词留存")
# n2(trust=6) 报 n2-n3 radio up 0.8; n3(trust=4) 报 down -> 高可信胜
print(obs(store, "n3", 2, 200, "n2", "radio", 0.0, 50000))
derive_mod.derive(store, trigger="conflicting testimony")
for c in ops.conflicts_at(store, 1000):
    pp({"edge": f"{c['ep_a']}-{c['ep_b']}:{c['medium']}", "epoch": [c["epoch_start"], c["epoch_end"]],
        "decision": c["decision"], "state": c["state"],
        "rationale": c["rationale"],
        "testimonies": c["testimonies"]})

# ---------------------------------------------------------------- 4. 抖动阻尼
h("4. 抖动门限: 短暂抖动只计入证据, 持续劣化才翻转")
for i, q in enumerate([0.1, 0.9, 0.1, 0.1, 0.1], start=4):
    print(obs(store, "n2", i, 1000 + (i - 4) * 100, "n4", "radio", q, 200))
derive_mod.derive(store, trigger="quality samples")
for t in (1050, 1150, 1250, 1350):
    e = ops.edge_trace(store, "n2", "n4", "radio")
    row = [r for r in e if r["epoch_start"] <= t < r["epoch_end"]]
    if row:
        r = row[0]
        print(f"t={t}: usable={r['usable']} quality={r['quality']} note={r['damp_note']}")

# ---------------------------------------------------------------- 5. 路径解释
h("5. 最短可用路径与解释 (代价 / 决胜规则 / 逐跳原始观测)")
res = ops.path_at(store, "n1", "n5", 5000)
pp({"revision": res["revision"], "path": res["path"], "cost": res["cost"],
    "tie_broken": res["tie_broken"],
    "hops": [{"edge": f"{h_['from']}-{h_['to']}:{h_['medium']}",
              "quality": h_["quality"],
              "testimony": h_.get("testimony")} for h_ in res["hops"]]})

# ---------------------------------------------------------------- 6. 割点与故障域
h("6. 单点割点与节点失联的受影响范围")
print("cutpoints@5000:", ops.cutpoints_at(store, 5000))
fd = failure_mod.failure_domain(store, "n4", 5000)
pp({"failed": "n4", "job": fd["job_id"],
    "affected": [{"member": m["member"], "room": m["room"],
                  "lost_exits": m["lost_exits"], "lost_peers": m["lost_peers"]}
                 for m in fd["members"] if m["lost_exits"] or m["lost_peers"] > 0]})

# ---------------------------------------------------------------- 7. 迟到证词
h("7. 迟到证词改写未封存历史 -> 新修订 + 差异 (新增/撤销/改判)")
before = ops.revision_at(store, 300)["rev_id"]
# 采样时刻 250 的证词现在才到 (序号前进, 历史区间正确归位)
print(obs(store, "n1", 2, 250, "n3", "radio", 0.85, 50000))
derive_mod.derive(store, trigger="late testimony")
after = ops.revision_at(store, 300)["rev_id"]
print(f"revision at t=300: {before} -> {after}")
pp(ops.diff_revisions(store, before, after))

# ---------------------------------------------------------------- 8. 封存与旁路
h("8. 封存历史: 早于界限的证词进入旁路档案, 不改写既有修订")
derive_mod.seal(store, 1000)
late = obs(store, "n1", 3, 500, "n5", "radio", 0.9, 100)
pp(late)
print("revision at t=500 (封存后不变):", ops.revision_at(store, 500))
pp([{"observer": s["observer"], "seq": s["seq"], "sample_time": s["sample_time"],
     "reason": s["reason"]} for s in ops.sidecar(store)])

# ---------------------------------------------------------------- 9. 崩溃恢复
h("9. 崩溃恢复: 推导作业从持久检查点续跑, 不留半成品")
print(obs(store, "n1", 4, 2000, "n5", "wifi", 0.6, 50000))
store.crash_after = "damp"          # 在 damp 阶段后模拟进程崩溃
try:
    derive_mod.derive(store, trigger="crash demo")
except jobs.CrashError as e:
    print("CRASH:", e)
print("revisions visible after crash:",
      [r["rev_id"] for r in ops.revisions(store)])
store.crash_after = None
print("recover ->", jobs.recover(store))
revs = ops.revisions(store)
print("revisions after recovery:", [r["rev_id"] for r in revs])
pp(revs[-1])

h("10. 运维界面汇总")
print("revisions:", [(r["rev_id"], r["diff"]) for r in ops.revisions(store)])
print("sidecar:", len(ops.sidecar(store)), "rejects:", len(ops.rejects(store)))
store.close()
print("\nDONE. 数据库:", DB)
