"""Versioned MCP registry engine skeleton."""

from __future__ import annotations

from typing import Any, cast

from obase.tool_governance import MCPServerSpec
from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


class MCPRegistryEngine(EngineSkeleton):
    """Manage versioned MCP registrations through the injected obase registry."""

    injection_points: dict[str, Injection] = {  # noqa: RUF012
        "registry": Injection("obase", "1", "McpClientRegistry-compatible registry"),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self, *, registry: Any, trigger: dict[str, Any], config: dict[str, Any], name: str
    ) -> None:
        self.name = name
        self.registry = registry
        self.trigger = trigger
        self.config = config
        self._running = False

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    def register(self, spec: MCPServerSpec, *, client: Any | None = None) -> dict[str, Any]:
        if not self._running:
            return {"status": "failed", "error": "MCP registry engine is stopped"}
        if client is None:
            self.registry.register_spec(spec)
        else:
            self.registry.register_server(spec, client)
        return {"status": "registered", "server": spec.identity, "tools": len(spec.tools)}

    def invalidate(self, name: str) -> dict[str, Any]:
        if not self._running:
            return {"status": "failed", "error": "MCP registry engine is stopped"}
        return {
            "status": "invalidated" if self.registry.invalidate(name) else "missing",
            "server": name,
        }

    def list(self) -> list[MCPServerSpec]:
        return cast(list[MCPServerSpec], self.registry.list_specs())

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {"name": self.name, "registered": len(self.registry.list_specs())},
        }


register_skeleton("mcp_registry", MCPRegistryEngine)

__all__ = ["MCPRegistryEngine"]
