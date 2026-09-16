"""运维命令行界面.

用法: python3 -m topo --db topo.db <command> ...

  登记:   room add / domain add / node add / exit add / lease add
  摄取:   observe / ingest-file / derive / seal
  查询:   status / components / path / cutpoints / failure-domain
          conflicts / edge / revisions / diff / sidecar / rejects
  恢复:   recover
"""

import argparse
import json
import sys

from . import derive as derive_mod
from . import failure as failure_mod
from . import ingest, jobs, models, ops
from .store import Store


def _pp(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=False))


def main(argv=None):
    p = argparse.ArgumentParser(prog="topo", description="temporal topology control plane")
    p.add_argument("--db", default="topo.db", help="SQLite database path")
    sub = p.add_subparsers(dest="cmd", required=True)

    def sp(name, *args, **kw):
        return sub.add_parser(name, *args, **kw)

    r = sp("room"); r.add_argument("name"); r.add_argument("--note")
    d = sp("domain"); d.add_argument("name"); d.add_argument("--room"); d.add_argument("--note")
    n = sp("node"); n.add_argument("node_id"); n.add_argument("--room"); n.add_argument("--domain")
    n.add_argument("--trust", type=int); n.add_argument("--exit", action="store_true")
    n.add_argument("--exit-name")
    l = sp("lease"); l.add_argument("node_id"); l.add_argument("--start", type=int, required=True)
    l.add_argument("--end", type=int, required=True); l.add_argument("--source", default="operator")

    o = sp("observe")
    o.add_argument("observer"); o.add_argument("--seq", type=int, required=True)
    o.add_argument("--time", type=int, required=True); o.add_argument("--neighbor", required=True)
    o.add_argument("--medium", required=True, choices=sorted(models.MEDIA))
    o.add_argument("--quality", type=float, required=True)
    o.add_argument("--ttl", type=int, required=True)
    o.add_argument("--no-derive", action="store_true")

    f = sp("ingest-file"); f.add_argument("path"); f.add_argument("--no-derive", action="store_true")
    dr = sp("derive"); dr.add_argument("--note")
    s = sp("seal"); s.add_argument("time", type=int)

    q = sp("status"); q.add_argument("--at", type=int, required=True)
    c = sp("components"); c.add_argument("--at", type=int, required=True)
    pa = sp("path"); pa.add_argument("src"); pa.add_argument("dst"); pa.add_argument("--at", type=int, required=True)
    cu = sp("cutpoints"); cu.add_argument("--at", type=int, required=True)
    fd = sp("failure-domain"); fd.add_argument("node"); fd.add_argument("--at", type=int, required=True)
    co = sp("conflicts"); co.add_argument("--at", type=int)
    e = sp("edge"); e.add_argument("a"); e.add_argument("b"); e.add_argument("--medium")
    sp("revisions")
    di = sp("diff"); di.add_argument("r1", type=int); di.add_argument("r2", type=int)
    sp("sidecar")
    sp("rejects")
    sp("recover")

    args = p.parse_args(argv)
    store = Store(args.db)
    try:
        return _dispatch(store, args)
    finally:
        store.close()


def _dispatch(store, args):
    cmd = args.cmd
    if cmd == "room":
        ingest.register_room(store, args.name, args.note); print(f"room {args.name} registered")
    elif cmd == "domain":
        ingest.register_domain(store, args.name, args.room, args.note)
        print(f"domain {args.name} registered")
    elif cmd == "node":
        ingest.register_node(store, args.node_id, room=args.room, domain=args.domain,
                             trust=args.trust, is_exit=args.exit or None,
                             exit_name=args.exit_name)
        print(f"node {args.node_id} registered")
    elif cmd == "lease":
        ingest.add_lease(store, args.node_id, args.start, args.end, args.source)
        print(f"lease {args.node_id} [{args.start},{args.end}) registered "
              f"(reachability only, not adjacency)")
    elif cmd == "observe":
        res = ingest.ingest_observation(store, args.observer, args.seq, args.time,
                                        args.neighbor, args.medium, args.quality, args.ttl)
        _pp(res)
        if res["status"] == "accepted" and not args.no_derive:
            _print_derive(store, trigger=f"observation {args.observer}#{args.seq}")
    elif cmd == "ingest-file":
        job_id = ingest.ingest_file(store, args.path)
        print(f"ingest-file job {job_id} done")
        if not args.no_derive:
            _print_derive(store, trigger=f"ingest-file {args.path}")
    elif cmd == "derive":
        _print_derive(store, trigger="manual", note=args.note, force=True)
    elif cmd == "seal":
        derive_mod.seal(store, args.time)
        print(f"history sealed at t={args.time}; earlier testimony now goes to sidecar")
    elif cmd == "status":
        _pp(ops.status_at(store, args.at))
    elif cmd == "components":
        _pp(ops.components_at(store, args.at))
    elif cmd == "path":
        _pp(ops.path_at(store, args.src, args.dst, args.at))
    elif cmd == "cutpoints":
        _pp({"time": args.at, "articulation_points": ops.cutpoints_at(store, args.at)})
    elif cmd == "failure-domain":
        _pp(failure_mod.failure_domain(store, args.node, args.at))
    elif cmd == "conflicts":
        _pp(ops.conflicts_at(store, args.at))
    elif cmd == "edge":
        _pp(ops.edge_trace(store, args.a, args.b, args.medium))
    elif cmd == "revisions":
        _pp(ops.revisions(store))
    elif cmd == "diff":
        _pp(ops.diff_revisions(store, args.r1, args.r2))
    elif cmd == "sidecar":
        _pp(ops.sidecar(store))
    elif cmd == "rejects":
        _pp(ops.rejects(store))
    elif cmd == "recover":
        resumed = jobs.recover(store)
        print(f"recovered {len(resumed)} job(s): {', '.join(resumed) or 'none'}")
    return 0


def _print_derive(store, trigger, note=None, force=False):
    job_id, rev = derive_mod.derive(store, trigger=trigger, note=note, force=force)
    if rev:
        print(f"derived revision {rev} (job {job_id})")
    else:
        print("no new testimony to derive" if not force else "derivation produced no revision")


if __name__ == "__main__":
    sys.exit(main())
