"""ResilientAgenticLoop — agent loop with breaker + optional folding.

Does not replace AgenticLoopEngine. Does not register Coordinator tools.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from obase.loop_breaker import init_breaker, reset_breaker
from obase.token_counter import token_counter

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


class ResilientAgenticLoop(EngineSkeleton):
    injection_points: ClassVar[dict] = {
        "llm_caller": Injection(kind="oprim", cardinality="1", description="LLM call"),
        "tools": Injection(kind="oprim", cardinality="1..n", description="jailed tools"),
        "health_monitor": Injection(kind="omodul", cardinality="1", description="loop breaker"),
        "context_compactor": Injection(
            kind="omodul", cardinality="0..1", description="optional folding"
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        llm_caller: Callable[..., Any],
        tools: list[Callable[..., Any]],
        health_monitor: Callable[..., Any],
        context_compactor: Callable[..., Any] | None = None,
        trigger: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        name: str = "resilient-loop",
        output_dir: Path | str | None = None,
    ) -> None:
        self.name = name
        self.llm_caller = llm_caller
        self.tools = {getattr(t, "__name__", str(i)): t for i, t in enumerate(tools)}
        self.health_monitor = health_monitor
        self.context_compactor = context_compactor
        self.trigger = trigger or {"on_demand": True}
        self.config = config or {}
        self.output_dir = Path(output_dir) if output_dir else Path(".")
        self._running = False

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def run_loop(self, initial_prompt: str) -> dict[str, Any]:
        token = init_breaker()
        self._running = True
        messages: list[dict[str, Any]] = [{"role": "user", "content": initial_prompt}]
        last_error = False
        try:
            while self._running:
                messages = await self._maybe_compact(messages)
                response = await self._call(self.llm_caller, messages=messages)
                tool, final = _parse_llm(response)
                if tool is None:
                    return {
                        "status": "completed",
                        "messages": messages,
                        "final": final,
                    }

                monitor = await self._call(
                    self.health_monitor,
                    config=self.config.get("monitor") or {},
                    input_data={
                        "tool_name": tool["name"],
                        "arguments": tool["args"],
                        "is_error": last_error,
                    },
                    output_dir=self.output_dir,
                )
                if not isinstance(monitor, dict) or monitor.get("status") == "failed":
                    return {
                        "status": "failed",
                        "messages": messages,
                        "error": (monitor or {}).get("error"),
                    }
                action = (monitor.get("findings") or {}).get("action") or "continue"
                if action == "halt":
                    return {
                        "status": "halted_by_breaker",
                        "messages": messages,
                        "monitor": monitor,
                    }
                if action == "intervene":
                    prompt = (monitor.get("findings") or {}).get("intervention_prompt") or ""
                    messages.append({"role": "system", "content": prompt})
                    last_error = False
                    continue

                observation, last_error = await self._run_tool(tool["name"], tool["args"])
                messages.append(
                    {
                        "role": "assistant",
                        "content": f"tool {tool['name']}",
                        "tool_call": tool,
                    }
                )
                messages.append({"role": "tool", "content": str(observation)})
            return {"status": "stopped", "messages": messages}
        finally:
            self._running = False
            reset_breaker(token)

    async def _maybe_compact(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if self.context_compactor is None:
            return messages
        threshold = int(self.config.get("token_threshold", 80_000))
        tokens = sum(token_counter(str(item.get("content") or "")) for item in messages)
        if tokens <= threshold:
            return messages
        result = await self._call(
            self.context_compactor,
            config=self.config.get("compactor") or {},
            input_data={"messages": messages},
            output_dir=self.output_dir,
        )
        if isinstance(result, dict) and result.get("status") == "completed":
            folded = (result.get("findings") or {}).get("compacted_messages")
            if isinstance(folded, list):
                return folded
        return messages

    async def _run_tool(self, name: str, args: dict[str, Any]) -> tuple[Any, bool]:
        tool = self.tools.get(name)
        if tool is None:
            return f"ERROR: tool {name!r} not found", True
        try:
            result = await self._call(tool, **args)
            return result, False
        except Exception as exc:  # noqa: BLE001 — tool failure becomes is_error
            return f"ERROR: {type(exc).__name__}: {exc}", True

    async def _call(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        if fn is None:
            return None
        # omodul transactions want typed config; skeleton may pass None
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        result = fn(**kwargs)
        if inspect.isawaitable(result):
            return await result
        return result


def _parse_llm(response: Any) -> tuple[dict[str, Any] | None, Any]:
    if not isinstance(response, dict):
        return None, response
    if response.get("stop_reason") == "tool_use" or response.get("tool_call"):
        call = response.get("tool_call") or {}
        return {
            "name": call.get("name") or "",
            "args": call.get("args") or call.get("arguments") or {},
        }, None
    if response.get("tool_name"):
        return {
            "name": response["tool_name"],
            "args": response.get("tool_args") or response.get("arguments") or {},
        }, None
    if response.get("final_answer") is not None:
        return None, response["final_answer"]
    return None, response.get("content") or response.get("text") or ""


register_skeleton("resilient_agentic_loop", ResilientAgenticLoop)
