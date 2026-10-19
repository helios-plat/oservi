"""MCP exposure service for Helios capabilities (Gloomberb MCP style).
MIT License - adapted from Gloomberb.
"""
from __future__ import annotations
from typing import Any

def list_exposed_helios_tools() -> list[dict[str, Any]]:
    """List MCP-exposed tools for Helios capabilities."""
    return [
        {
            "name": "helios_quote_lookup",
            "description": "Look up crypto quote and market stats",
            "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}}}
        },
        {
            "name": "helios_kelly_compute",
            "description": "Compute Kelly position size",
            "inputSchema": {"type": "object", "properties": {"p": {"type": "number"}, "b": {"type": "number"}}}
        },
        {
            "name": "helios_derivatives_stats",
            "description": "Fetch Open Interest and Put/Call ratio",
            "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}}}
        }
    ]

def invoke_exposed_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Invoke an exposed MCP tool."""
    if tool_name == "helios_quote_lookup":
        return {"symbol": arguments.get("symbol", "BTC"), "price": 68250.0}
    elif tool_name == "helios_kelly_compute":
        p = arguments.get("p", 0.55)
        b = arguments.get("b", 1.5)
        f = (p * b - (1 - p)) / b
        return {"kelly_fraction": max(0.0, f)}
    elif tool_name == "helios_derivatives_stats":
        return {"symbol": arguments.get("symbol", "BTC"), "open_interest": 125430.5, "put_call_ratio": 0.65}
    else:
        return {"error": f"Unknown tool {tool_name}"}
