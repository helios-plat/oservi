"""MCP Bridge Engine Skeleton.

机制 (固化):
- on_demand 触发
- 路由 MCP tool 调用 (routing only, no connection pooling)
- connect oprim 建立连接
- call oprim 列表执行工具调用
- registry layer4 管理工具注册表

业务 (注入):
- connect: oprim (1) — MCP connect primitive
- call: oprim (1..n) — MCP call primitives
- registry: layer4 (1) — Tool registry

红线对照:
- 红线 2 (机制/业务分离): 连接/调用业务全靠注入
- 红线 3 (注入契约): 3 注入点声明
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class McpBridgeEngine(EngineSkeleton):
    """MCP Bridge 引擎骨架 (on_demand, routing only).

    Routes MCP tool calls to the appropriate call primitive.
    No connection pooling — each call routes through the injected
    call primitives registered in the tool registry.

    Example::

        engine = McpBridgeEngine(
            connect=oprim_mcp_connect,
            call=[oprim_mcp_call_fs, oprim_mcp_call_search],
            registry=layer4_tool_registry,
            trigger={"on_demand": True},
            config={},
            name="mcp-bridge",
        )
        result = asyncio.run(engine.invoke_tool("read_file", {"path": "/tmp/x"}))
    """

    injection_points: ClassVar[dict] = {
        "connect": Injection(
            kind="oprim",
            cardinality="1",
            description="MCP connect primitive",
        ),
        "call": Injection(
            kind="oprim",
            cardinality="1..n",
            description="MCP call primitives",
        ),
        "registry": Injection(
            kind="layer4",
            cardinality="1",
            description="Tool registry",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        connect: Callable[..., Any],
        call: list[Callable[..., Any]] | Callable[..., Any],
        registry: Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.connect = connect
        self.call_primitives: list[Callable[..., Any]] = call if isinstance(call, list) else [call]
        self.registry = registry
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._call_count = 0
        self._last_error: str | None = None

    # ===== 生命周期 =====

    def run(self) -> None:
        """on_demand 引擎: run() 标记就绪."""
        self._running = True
        logger.info(f"McpBridgeEngine '{self.name}' ready")

    def stop(self) -> None:
        self._running = False

    # ===== 核心 API =====

    async def invoke_tool(
        self,
        tool_name: str,
        tool_args: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Route an MCP tool call to the appropriate call primitive.

        1. Looks up tool_name in registry to find the right call primitive index.
        2. Calls connect if needed.
        3. Routes to the matching call primitive.

        Returns:
            {"status": "ok" | "error", "result": Any, "tool_name": str}
        """
        tool_args = tool_args or {}

        # 1. lookup in registry
        try:
            if inspect.iscoroutinefunction(self.registry):
                reg_result = await self.registry(tool_name=tool_name)
            else:
                raw = self.registry(tool_name=tool_name)
                if asyncio.iscoroutine(raw):
                    reg_result = await raw
                else:
                    reg_result = raw
            # registry returns index or callable reference
            primitive_idx = reg_result.get("primitive_idx", 0) if isinstance(reg_result, dict) else 0
        except Exception as e:
            logger.warning(f"McpBridgeEngine registry lookup failed for '{tool_name}': {e}")
            primitive_idx = 0

        # 2. select call primitive (route by index, fallback to 0)
        idx = int(primitive_idx) if primitive_idx is not None else 0
        idx = min(idx, len(self.call_primitives) - 1)
        call_fn = self.call_primitives[idx]

        # 3. invoke
        try:
            if inspect.iscoroutinefunction(call_fn):
                result = await call_fn(tool_name=tool_name, tool_args=tool_args)
            else:
                raw = call_fn(tool_name=tool_name, tool_args=tool_args)
                if asyncio.iscoroutine(raw):
                    result = await raw
                else:
                    result = raw
            self._call_count += 1
            return {"status": "ok", "result": result, "tool_name": tool_name}
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            logger.warning(f"McpBridgeEngine call failed for '{tool_name}': {e}")
            return {"status": "error", "result": None, "tool_name": tool_name, "error": str(e)}

    async def connect_server(self, server_url: str, **kwargs: Any) -> dict[str, Any]:
        """Establish MCP connection via connect primitive."""
        try:
            if inspect.iscoroutinefunction(self.connect):
                result = await self.connect(server_url=server_url, **kwargs)
            else:
                raw = self.connect(server_url=server_url, **kwargs)
                if asyncio.iscoroutine(raw):
                    result = await raw
                else:
                    result = raw
            return {"status": "connected", "result": result}
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            logger.warning(f"McpBridgeEngine connect failed: {e}")
            return {"status": "error", "error": str(e)}

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "call_count": self._call_count,
                "call_primitives_count": len(self.call_primitives),
                "last_error": self._last_error,
            },
        }


register_skeleton("mcp_bridge", McpBridgeEngine)
