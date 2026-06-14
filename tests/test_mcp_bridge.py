"""McpBridgeEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.mcp_bridge import McpBridgeEngine


# ===== Fake injectables =====

def fake_connect(*, server_url, **kwargs):
    return {"connected": True, "url": server_url}


async def async_connect(*, server_url, **kwargs):
    return {"connected": True, "url": server_url, "async": True}


def fake_call_fs(*, tool_name, tool_args):
    return {"tool": tool_name, "args": tool_args, "primitive": "fs"}


def fake_call_search(*, tool_name, tool_args):
    return {"tool": tool_name, "args": tool_args, "primitive": "search"}


async def async_call(*, tool_name, tool_args):
    return {"tool": tool_name, "async": True}


def failing_call(*, tool_name, tool_args):
    raise RuntimeError("MCP call failed")


def fake_registry(*, tool_name):
    # route search tools to primitive index 1
    if "search" in tool_name:
        return {"primitive_idx": 1}
    return {"primitive_idx": 0}


def _make_engine(**overrides):
    defaults = dict(
        connect=fake_connect,
        call=[fake_call_fs, fake_call_search],
        registry=fake_registry,
        trigger={"on_demand": True},
        config={},
        name="test-mcp-bridge",
    )
    defaults.update(overrides)
    return McpBridgeEngine(**defaults)


# ===== Tests =====

def test_mcp_bridge_registered():
    assert "mcp_bridge" in list_skeletons()


def test_injection_points_keys():
    pts = McpBridgeEngine.injection_points
    assert "connect" in pts
    assert "call" in pts
    assert "registry" in pts


def test_injection_point_connect():
    pt = McpBridgeEngine.injection_points["connect"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_call():
    pt = McpBridgeEngine.injection_points["call"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1..n"


def test_injection_point_registry():
    pt = McpBridgeEngine.injection_points["registry"]
    assert pt.kind == "layer4"
    assert pt.cardinality == "1"


def test_trigger_mode_on_demand():
    assert McpBridgeEngine.trigger_mode == "on_demand"


def test_instantiates():
    engine = _make_engine()
    assert engine.name == "test-mcp-bridge"
    assert len(engine.call_primitives) == 2


def test_single_call_stored_as_list():
    engine = _make_engine(call=fake_call_fs)
    assert len(engine.call_primitives) == 1


def test_run_marks_running():
    engine = _make_engine()
    engine.run()
    assert engine._running is True


def test_stop_marks_not_running():
    engine = _make_engine()
    engine.run()
    engine.stop()
    assert engine._running is False


def test_invoke_tool_routes_to_fs_primitive():
    engine = _make_engine()
    result = asyncio.run(engine.invoke_tool("read_file", {"path": "/tmp/x"}))
    assert result["status"] == "ok"
    assert result["result"]["primitive"] == "fs"


def test_invoke_tool_routes_to_search_primitive():
    engine = _make_engine()
    result = asyncio.run(engine.invoke_tool("search_files", {"query": "foo"}))
    assert result["status"] == "ok"
    assert result["result"]["primitive"] == "search"


def test_invoke_tool_returns_tool_name():
    engine = _make_engine()
    result = asyncio.run(engine.invoke_tool("read_file", {}))
    assert result["tool_name"] == "read_file"


def test_invoke_tool_async_call_primitive():
    engine = _make_engine(call=[async_call])
    result = asyncio.run(engine.invoke_tool("any_tool", {}))
    assert result["status"] == "ok"
    assert result["result"]["async"] is True


def test_invoke_tool_failing_call_returns_error():
    engine = _make_engine(call=[failing_call])
    result = asyncio.run(engine.invoke_tool("bad_tool", {}))
    assert result["status"] == "error"
    assert "error" in result


def test_call_count_increments():
    engine = _make_engine()
    asyncio.run(engine.invoke_tool("read_file", {}))
    asyncio.run(engine.invoke_tool("read_file", {}))
    assert engine._call_count == 2


def test_connect_server_sync():
    engine = _make_engine()
    result = asyncio.run(engine.connect_server("mcp://localhost:8080"))
    assert result["status"] == "connected"
    assert result["result"]["url"] == "mcp://localhost:8080"


def test_connect_server_async():
    engine = _make_engine(connect=async_connect)
    result = asyncio.run(engine.connect_server("mcp://async:9090"))
    assert result["status"] == "connected"
    assert result["result"]["async"] is True


def test_connect_server_error():
    def bad_connect(*, server_url, **kw):
        raise ConnectionError("refused")
    engine = _make_engine(connect=bad_connect)
    result = asyncio.run(engine.connect_server("mcp://bad"))
    assert result["status"] == "error"
    assert "error" in result


def test_health_reports():
    engine = _make_engine()
    engine.run()
    h = engine.health()
    assert h["status"] == "healthy"
    assert h["details"]["call_primitives_count"] == 2
    assert h["details"]["call_count"] == 0
