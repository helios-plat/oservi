"""End-to-end MCP SDK protocol tests for the Gloom 3O server."""

from __future__ import annotations

from contextlib import asynccontextmanager
import json
from typing import Any

import pytest

from oservi.mcp_helios.server import MCP_TOOL_NAMES, create_gloom3o_mcp_server


@asynccontextmanager
async def _connected_client(server: Any):
    try:
        from mcp.client._memory import InMemoryTransport
    except ImportError:
        from mcp.shared.memory import create_connected_server_and_client_session

        async with create_connected_server_and_client_session(server) as session:
            yield session
    else:
        from mcp.client.session import ClientSession

        async with InMemoryTransport(server) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield session


def _input_schema(tool: Any) -> dict[str, Any]:
    return getattr(tool, "input_schema", None) or getattr(tool, "inputSchema")


def _is_error(result: Any) -> bool:
    return bool(getattr(result, "is_error", getattr(result, "isError", False)))


def _structured_content(result: Any) -> dict[str, Any]:
    structured = getattr(result, "structured_content", None) or getattr(
        result, "structuredContent", None
    )
    if structured is not None:
        return structured
    return json.loads(result.content[0].text)


@pytest.mark.asyncio
async def test_real_mcp_tools_list_exposes_every_gloom3o_command() -> None:
    """Exercise initialize + tools/list through the official SDK client/server transport."""
    server = create_gloom3o_mcp_server()
    async with _connected_client(server) as session:
        result = await session.list_tools()

    tool_names = {tool.name for tool in result.tools}
    assert tool_names == set(MCP_TOOL_NAMES)
    assert "gloom_kelly" in tool_names
    assert "gloom_opx" in tool_names
    assert "gloom_help" in tool_names


@pytest.mark.asyncio
async def test_mcp_tool_list_contains_typed_argument_schemas() -> None:
    server = create_gloom3o_mcp_server()
    async with _connected_client(server) as session:
        result = await session.list_tools()

    by_name = {tool.name: tool for tool in result.tools}
    assert _input_schema(by_name["gloom_quote"])["required"] == ["symbol"]
    assert set(_input_schema(by_name["gloom_kelly"])["required"]) == {"p", "b"}
    assert _input_schema(by_name["gloom_risk"])["properties"]["returns"]["type"] == "array"


@pytest.mark.asyncio
async def test_mcp_tools_call_reaches_the_shared_help_dispatcher() -> None:
    server = create_gloom3o_mcp_server()
    async with _connected_client(server) as session:
        result = await session.call_tool("gloom_help", {})

    assert not _is_error(result)
    assert _structured_content(result)["commands"] == [
        "QUOTE", "DES", "GP", "OPX", "KELLY", "RISK", "PORT", "FUSION", "HELP"
    ]
