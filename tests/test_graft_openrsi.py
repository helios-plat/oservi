"""3O 范式内化 — oservi 服务测试 (Graft MCP / 记忆 API / SFT 导出)."""

from __future__ import annotations

import json

from obase import MemoryBank
from obase.graph_storage import GraphNode, GraphRelation, MarkdownGraphStore

from oservi import GraftMcpTools, MemoryApi, build_sft_records, export_trajectories


def _make_store(tmp_path) -> MarkdownGraphStore:
    store = MarkdownGraphStore(tmp_path / ".veya" / "graph")
    store.save_node(
        GraphNode(
            node_id="auth_service",
            file_path="src/auth.py",
            content_hash="h1",
            summary="JWT authentication and token refresh",
            crux="if expired: refresh",
            relations=[GraphRelation("db_pool"), GraphRelation("user_controller", "part_of")],
        )
    )
    store.save_node(
        GraphNode(
            node_id="db_pool",
            file_path="src/db.py",
            content_hash="h2",
            summary="MySQL connection pool management",
            crux="pool.acquire()",
        )
    )
    return store


def test_graft_find_code(tmp_path):
    tools = GraftMcpTools(_make_store(tmp_path))
    hits = tools.graft_find_code("JWT token authentication")
    assert hits and hits[0]["node_id"] == "auth_service"


def test_graft_trace_calls(tmp_path):
    tools = GraftMcpTools(_make_store(tmp_path))
    upstream = tools.graft_trace_calls("db_pool", direction="upstream")
    assert any(t["node"] == "auth_service" for t in upstream["trace"])
    downstream = tools.graft_trace_calls("auth_service", direction="downstream")
    assert any(t["node"] == "db_pool" for t in downstream["trace"])


def test_graft_repo_map(tmp_path):
    tools = GraftMcpTools(_make_store(tmp_path))
    m = tools.graft_repo_map()
    assert m["node_count"] == 2
    assert any(e["source"] == "auth_service" and e["target"] == "db_pool" for e in m["edges"])


async def test_memory_api_crud(tmp_path):
    bank = MemoryBank(str(tmp_path / "mem.db"))
    api = MemoryApi(bank)
    await api.add(
        "MEM-9",
        context_description="cache stampede fix",
        failed_attempt={"action": "direct query", "reason": "thundering herd"},
        successful_strategy={"action": "single-flight", "reason": "dedupe concurrent"},
    )
    assert (await api.stats())["memory_count"] == 1
    hits = await api.search("cache stampede concurrent", top_k=3)
    assert hits[0]["memory_id"] == "MEM-9"
    assert len(await api.list_all()) == 1
    assert await api.delete("MEM-9") is True
    assert (await api.stats())["memory_count"] == 0


def test_sft_exporter(tmp_path):
    trajectories = [
        {
            "task_id": "T-1",
            "branch_id": "b1",
            "operator_used": "Evo_Debug",
            "input_code": "def f(): pass",
            "error_trace": "AssertionError at test.py:3",
            "output_code": "def f(): return 1",
        },
        {"task_id": "T-1", "branch_id": "b2", "operator_used": "sandbox_eval", "input_code": "x"},
    ]
    path = export_trajectories(trajectories, output_path=tmp_path / "dataset.jsonl")
    lines = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2

    records = build_sft_records(trajectories)
    assert len(records) == 1  # 仅 Evo_Debug 型轨迹构成 SFT 三元组
    assert "AssertionError" in records[0]["instruction"]
    assert records[0]["output"] == "def f(): return 1"


def test_sft_exporter_rejects_unknown_format(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="Unsupported export format"):
        export_trajectories([], output_path=tmp_path / "d.jsonl", format="parquet")
