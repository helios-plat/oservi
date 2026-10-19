"""WAVE B gates (oservi): McpBridgeEngine skills expose/consume.

lifecycle through injected MCP client, no business persistence.
"""

from __future__ import annotations

import pytest

from oservi.engines.mcp_bridge import McpBridgeEngine


def fake_connect(*, server_url, **kwargs):
    return {"connected": True, "url": server_url}


def fake_call(*, tool_name, tool_args):
    return {"tool": tool_name}


def fake_registry(*, tool_name):
    return {"primitive_idx": 0}


class FakeSkillsClient:
    extensions = ["io.modelcontextprotocol/skills"]

    async def list_tools(self):
        return [{"name": "skill_summarize", "description": "summarize"}]

    async def call_tool(self, name, args):
        if name == "skill_list":
            return {"skills": [{"name": "skill_summarize", "version": "1.0"}]}
        return {"ran": name, "args": args}


def _make_engine(**overrides):
    defaults = dict(
        connect=fake_connect,
        call=fake_call,
        registry=fake_registry,
        trigger={"on_demand": True},
        config={},
        name="test-mcp-skills",
    )
    defaults.update(overrides)
    return McpBridgeEngine(**defaults)


def test_client_injection_optional():
    engine = _make_engine()
    assert engine.client is None
    assert "client" in McpBridgeEngine.injection_points


@pytest.mark.asyncio()
async def test_expose_remote_skills():
    engine = _make_engine(client=FakeSkillsClient())
    out = await engine.expose_remote_skills()
    assert out["status"] == "ok"
    assert out["exposed"] == ["skill_summarize"]
    assert out["negotiation"]["supported"] is True


@pytest.mark.asyncio()
async def test_consume_remote_skill():
    engine = _make_engine(client=FakeSkillsClient())
    out = await engine.consume_remote_skill("skill_summarize", {"text": "hi"})
    assert out["status"] == "ok"
    assert out["result"] == {"ran": "skill_summarize", "args": {"text": "hi"}}


@pytest.mark.asyncio()
async def test_no_client_honest_error():
    engine = _make_engine()
    with pytest.raises(RuntimeError):
        await engine.expose_remote_skills()


@pytest.mark.asyncio()
async def test_no_business_persistence():
    engine = _make_engine(client=FakeSkillsClient())
    await engine.expose_remote_skills()
    # 引擎不得持有 skill 内容库:只有路由计数等运行态
    assert not hasattr(engine, "skill_store")
    assert not hasattr(engine, "skills_db")
    assert engine.health()["details"]["call_count"] == 0
