"""Model Context Protocol server exposing the Gloom 3O command router.

Uses the official ``mcp`` Python SDK. The adapter supports MCP SDK v2's
``MCPServer`` and the v1 ``FastMCP`` surface used by the locked 3O runtime.
All tools dispatch to the same command handlers as the CLI; no sample market
or account values are embedded here.
"""

from __future__ import annotations

from typing import Any, Literal

from oservi.gloom3o.commands import CommandDispatcher, CommandRequest, create_default_dispatcher

MCP_TOOL_NAMES = (
    "gloom_quote",
    "gloom_des",
    "gloom_gp",
    "gloom_opx",
    "gloom_kelly",
    "gloom_risk",
    "gloom_port",
    "gloom_fusion",
    "gloom_help",
)


def create_gloom3o_mcp_server(dispatcher: CommandDispatcher | None = None) -> Any:
    """Create an SDK-backed MCP server whose tools map one-to-one to 3O commands."""
    command_dispatcher = dispatcher or create_default_dispatcher()

    async def invoke(
        command: str,
        arguments: tuple[str, ...] = (),
        options: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        result = await command_dispatcher.dispatch(
            CommandRequest(command, arguments, options or {})
        )
        return result if isinstance(result, dict) else {"result": result}

    async def gloom_quote(symbol: str, exchange: str = "okx") -> dict[str, Any]:
        """Read a public quote through the configured Helios command API."""
        return await invoke("QUOTE", (symbol,), {"exchange": exchange})

    async def gloom_des(symbol: str, expiry: str | None = None) -> dict[str, Any]:
        """Request a derivatives summary for the supplied symbol."""
        options = {"expiry": expiry} if expiry else {}
        return await invoke("DES", (symbol,), options)

    async def gloom_gp(symbol: str, period: str | None = None) -> dict[str, Any]:
        """Request a Gloom profile for the supplied symbol."""
        options = {"period": period} if period else {}
        return await invoke("GP", (symbol,), options)

    async def gloom_opx(
        underlying: str, exchange: Literal["deribit", "okx", "all"] = "all"
    ) -> dict[str, Any]:
        """Read and analyze public Deribit/OKX option chains."""
        return await invoke("OPX", (underlying,), {"underlying": underlying, "exchange": exchange})

    async def gloom_kelly(
        p: float,
        b: float,
        fraction: float = 1.0,
        max_fraction: float = 1.0,
    ) -> dict[str, Any]:
        """Compute Kelly sizing from caller-supplied probability and payoff."""
        return await invoke("KELLY", options={
            "p": str(p),
            "b": str(b),
            "fraction": str(fraction),
            "max_fraction": str(max_fraction),
        })

    async def gloom_risk(
        returns: list[float],
        confidence: float = 0.95,
        method: Literal["historical", "parametric"] = "historical",
        equity: list[float] | None = None,
    ) -> dict[str, Any]:
        """Calculate VaR/CVaR from caller-supplied return observations."""
        options = {
            "returns": ",".join(repr(value) for value in returns),
            "confidence": str(confidence),
            "method": method,
        }
        if equity is not None:
            options["equity"] = ",".join(repr(value) for value in equity)
        return await invoke("RISK", options=options)

    async def gloom_port(account: str | None = None) -> dict[str, Any]:
        """Read a portfolio through the configured Helios API (read-only)."""
        return await invoke("PORT", options={"account": account} if account else {})

    async def gloom_fusion(symbol: str) -> dict[str, Any]:
        """Request the existing Helios Fusion result without modifying it."""
        return await invoke("FUSION", (symbol,))

    async def gloom_help() -> dict[str, Any]:
        """List supported Gloom 3O commands."""
        return await invoke("HELP")

    definitions = [
        ("gloom_quote", "Public quote", "QUOTE", gloom_quote),
        ("gloom_des", "Derivatives summary", "DES", gloom_des),
        ("gloom_gp", "Gloom profile", "GP", gloom_gp),
        ("gloom_opx", "Options indicators from public exchange data", "OPX", gloom_opx),
        ("gloom_kelly", "Kelly sizing from supplied inputs", "KELLY", gloom_kelly),
        ("gloom_risk", "VaR/CVaR and drawdown from supplied observations", "RISK", gloom_risk),
        ("gloom_port", "Read-only portfolio query", "PORT", gloom_port),
        ("gloom_fusion", "Read existing Fusion result", "FUSION", gloom_fusion),
        ("gloom_help", "List commands", "HELP", gloom_help),
    ]

    try:
        from mcp.server.mcpserver import MCPServer
    except ImportError:
        try:
            from mcp.server.fastmcp import FastMCP
        except ImportError as exc:
            raise RuntimeError("install the MCP Python SDK to create the Gloom MCP server") from exc
        server = FastMCP("gloom3o")
        for tool_name, description, _command, function in definitions:
            server.tool(name=tool_name, description=description)(function)
        return server

    server = MCPServer(name="gloom3o", description="Gloom 3O tools", warn_on_duplicate_tools=False)
    for tool_name, description, _command, function in definitions:
        server.add_tool(function, name=tool_name, description=description)
    return server


def run_gloom3o_mcp_server() -> None:
    """Run the MCP server on stdio for local MCP clients."""
    server = create_gloom3o_mcp_server()
    try:
        server.run(transport="stdio")
    except TypeError:
        # FastMCP v1 uses stdio by default and older releases omit the keyword.
        server.run()


def main() -> None:
    run_gloom3o_mcp_server()


__all__ = ["MCP_TOOL_NAMES", "create_gloom3o_mcp_server", "run_gloom3o_mcp_server", "main"]
