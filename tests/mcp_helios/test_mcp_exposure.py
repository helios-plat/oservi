"""Tests for MCP exposure service."""
from oservi.mcp_helios.mcp_exposure import list_exposed_helios_tools, invoke_exposed_tool

def test_mcp_exposure():
    tools = list_exposed_helios_tools()
    assert len(tools) >= 3

    res = invoke_exposed_tool("helios_kelly_compute", {"p": 0.6, "b": 2.0})
    assert "kelly_fraction" in res
