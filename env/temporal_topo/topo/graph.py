"""图算法: 连通分量 / 最短可用路径 / 割点 / 故障域.

路径选择规则 (全部确定可复现):
  * 同一对节点间的多条介质边, 先取代价最低者 (代价随质量单调下降,
    所以"路径相同但质量不同"时稳定选代价更低者); 代价并列取介质名小者;
  * Dijkstra 的堆键为 (总代价, 节点序列元组), 等价路径按节点名字典序
    这一固定规则决胜, tie_broken 标记写进路径索引供运维解释。
"""

import heapq

from . import models


def build_adj(nodes, edge_rows):
    """由可用边行构建邻接表; 平行介质边折叠为 (代价, 介质名) 最优的一条."""
    best = {}
    for r in edge_rows:
        u, v = r["ep_a"], r["ep_b"]
        cost = models.edge_cost_micro(r["medium"], r["quality"])
        key = (u, v)
        if key not in best or (cost, r["medium"]) < (best[key][0], best[key][1]):
            best[key] = (cost, r["medium"], r["quality"])
    adj = {n: [] for n in nodes}
    for (u, v), (cost, medium, quality) in best.items():
        adj[u].append((v, cost, medium, quality))
        adj[v].append((u, cost, medium, quality))
    for n in adj:
        adj[n].sort(key=lambda t: (t[1], t[0], t[2]))
    return adj


def components(nodes, adj):
    """连通分量; 分量 id 取成员中最小节点名, 保证确定."""
    comp = {}
    for start in sorted(nodes):
        if start in comp:
            continue
        cid = start
        stack = [start]
        comp[start] = cid
        while stack:
            u = stack.pop()
            for (v, _c, _m, _q) in adj.get(u, ()):
                if v not in comp:
                    comp[v] = cid
                    stack.append(v)
    return comp


def dijkstra(adj, src):
    """单源最短路径. 返回 (best, hops):
    best[node] = (cost_micro, path_tuple); hops[node] = 逐跳边明细列表."""
    best = {src: (0, (src,))}
    hops = {src: []}
    pq = [(0, (src,), src)]
    while pq:
        cost, path, u = heapq.heappop(pq)
        if best.get(u) != (cost, path):
            continue
        for (v, c, medium, quality) in adj.get(u, ()):
            nc, npath = cost + c, path + (v,)
            if v not in best or (nc, npath) < best[v]:
                best[v] = (nc, npath)
                hops[v] = hops[u] + [{"from": u, "to": v, "medium": medium,
                                      "quality": quality, "cost_micro": c}]
                heapq.heappush(pq, (nc, npath, v))
    return best, hops


def min_path_counts(adj, src, best):
    """每个节点的最小代价路径条数 (封顶计到 2), 用于标记等价路径决胜."""
    count = {src: 1}
    for u in sorted(best, key=lambda n: (best[n][0], n)):
        cu = best[u][0]
        for (v, c, _m, _q) in adj.get(u, ()):
            if v in best and best[v][0] == cu + c:
                count[v] = min(2, count.get(v, 0) + count.get(u, 0))
    return count


def articulation_points(nodes, adj):
    """单点割点: 移除后使连通分量数增加的节点."""
    index, low, arts = {}, {}, set()
    timer = [0]

    def dfs(u, parent):
        index[u] = low[u] = timer[0]
        timer[0] += 1
        children = 0
        for (v, _c, _m, _q) in sorted(adj.get(u, ()), key=lambda t: t[0]):
            if v == parent:
                continue
            if v in index:
                low[u] = min(low[u], index[v])
            else:
                children += 1
                dfs(v, u)
                low[u] = min(low[u], low[v])
                if parent is not None and low[v] >= index[u]:
                    arts.add(u)
        if parent is None and children > 1:
            arts.add(u)

    for n in sorted(nodes):
        if n not in index:
            dfs(n, None)
    return arts


def failure_domain(nodes, adj, failed, exits, node_info):
    """节点失联的受影响范围: 失联前同分量成员在移除后的连通性变化.

    返回成员列表: {member, before_component, after_component,
    lost_exits, lost_peers}; lost_* 非空即受影响。
    """
    before = components(nodes, adj)
    if failed not in before:
        return []
    rest = [n for n in nodes if n != failed]
    adj2 = {n: [e for e in adj.get(n, ()) if e[0] != failed] for n in rest}
    after = components(rest, adj2)

    members = []
    for m in sorted(rest):
        if before.get(m) != before[failed]:
            continue  # 失联前就不连通, 不受影响
        before_peers = {n for n in nodes if before.get(n) == before[m]} - {m, failed}
        after_peers = {n for n in rest if after.get(n) == after[m]} - {m}
        lost_exits = sorted(x for x in exits
                            if x != failed and x in before_peers
                            and x not in after_peers)
        members.append({
            "member": m,
            "room": node_info.get(m, {}).get("room"),
            "domain": node_info.get(m, {}).get("domain"),
            "before_component": before[m],
            "after_component": after.get(m),
            "lost_exits": lost_exits,
            "lost_peers": len(before_peers - after_peers),
        })
    return members
