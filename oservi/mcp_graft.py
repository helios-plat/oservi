"""3O 范式内化 — Graft MCP 工具暴露 (oservi).

机制固化: 检索逻辑全部经由注入的 MarkdownGraphStore 完成。
三个 MCP 接口: graft_find_code / graft_trace_calls / graft_repo_map。
"""

from __future__ import annotations

from typing import Any

from obase.graph_storage import GraphNode, MarkdownGraphStore


class GraftMcpTools:
    """Graft 专属 MCP 工具集 (store 依赖注入, 引擎零业务)。"""

    def __init__(self, store: MarkdownGraphStore):
        self.store = store

    def graft_find_code(self, query: str) -> list[dict[str, Any]]:
        """根据语义及依赖查找关联的 Markdown 节点 (关键词打分)。"""
        q_words = set(w.lower() for w in query.split() if len(w) > 2)
        scored: list[tuple[int, GraphNode]] = []
        for node in self.store.load_all():
            haystack = f"{node.summary} {node.crux} {node.node_id}".lower()
            score = sum(1 for w in q_words if w in haystack)
            if score > 0:
                scored.append((score, node))
        scored.sort(key=lambda t: t[0], reverse=True)
        return [n.to_dict() for _, n in scored[:10]]

    def graft_trace_calls(self, symbol: str, direction: str = "downstream") -> dict[str, Any]:
        """沿 depends_on 边追溯上游调用者 (upstream) 或下游爆炸半径 (downstream)。"""
        nodes = self.store.load_all()
        by_id = {n.node_id: n for n in nodes}
        target = by_id.get(symbol)
        if target is None:
            return {"symbol": symbol, "direction": direction, "trace": []}

        if direction == "upstream":
            # 谁依赖我 → 上游调用者
            trace = [
                {"node": n.node_id, "relation": r.type}
                for n in nodes
                for r in n.relations
                if r.target == symbol
            ]
        else:
            # 我依赖谁 → 下游爆炸半径
            trace = [
                {"node": r.target, "relation": r.type} for r in target.relations
            ]
        return {"symbol": symbol, "direction": direction, "trace": trace}

    def graft_repo_map(self) -> dict[str, Any]:
        """输出顶层系统的全貌结构图 (节点 + 边)。"""
        nodes = self.store.load_all()
        return {
            "node_count": len(nodes),
            "nodes": [n.node_id for n in nodes],
            "edges": [
                {"source": n.node_id, "target": r.target, "type": r.type}
                for n in nodes
                for r in n.relations
            ],
        }
