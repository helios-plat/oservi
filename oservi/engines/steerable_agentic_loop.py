"""Steerable Agentic Loop Engine Skeleton — 打断式 Agentic 循环.

机制 (固化):
- ReAct 循环（thought → action → observe），同 agentic_loop
- **steer 检查点**：每轮行动前检查注入的 steer_channel（HITL 信号总线）：
  - interrupted 置位 → 立即终止并回传 interrupt_reason
  - 存在待决审批 → 经 approval_gate 等待人工决策（approved 继续 / rejected 停止）
  - 人工指令（steer instruction）→ 经 steer_fuser 融合进下一轮 prompt
- 依赖倒置：llm_caller / tools / steer_channel / approval_gate / steer_fuser
  全部注入，不 import 任何具体 3O 元素

红线对照:
- 红线 2 (机制/业务分离): LLM/工具/HITL 业务全靠注入
- 红线 3 (注入契约): llm_caller=oprim(1) / tools=oprim(1..n) /
  steer_channel=obase(0..1) / approval_gate=oskill(0..1) / steer_fuser=oskill(0..1)
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


class SteerableAgenticLoopEngine(EngineSkeleton):
    """打断式 Agentic 循环引擎骨架.

    Example::

        engine = SteerableAgenticLoopEngine(
            llm_caller=obase_react_llm,
            tools={"bash": bash_tool, "read": read_tool},
            steer_channel=hitl_bus,          # obase.hitl_signal_bus
            approval_gate=approval_fn,       # async (scope, summary) -> decision dict
            steer_fuser=steer_fn,            # (instruction, original_text) -> str
            trigger={"on_demand": True},
            config={"max_steps": 20, "steer_check_every": 1},
            name="steerable-agent",
        )
    """

    injection_points: ClassVar[dict] = {
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM ReAct 推理调用器",
        ),
        "tools": Injection(
            kind="oprim",
            cardinality="1..n",
            description="可用工具表（name -> callable）",
        ),
        "steer_channel": Injection(
            kind="obase",
            cardinality="0..1",
            description="HITL 打断信号总线（hitl_signal_bus 兼容）",
        ),
        "approval_gate": Injection(
            kind="oskill",
            cardinality="0..1",
            description="人工审批门：async (scope, summary) -> decision dict",
        ),
        "steer_fuser": Injection(
            kind="oskill",
            cardinality="0..1",
            description="干预指令融合算子 (instruction, text) -> text",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        llm_caller: Callable[..., Any],
        tools: dict[str, Callable[..., Any]],
        steer_channel: Any | None = None,
        approval_gate: Callable[..., Any] | None = None,
        steer_fuser: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.llm_caller = llm_caller
        self.tools = dict(tools)
        self.steer_channel = steer_channel
        self.approval_gate = approval_gate
        self.steer_fuser = steer_fuser
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    # ------------------------------------------------------------------
    # 打断检查
    # ------------------------------------------------------------------

    def _channel_interrupted(self) -> bool:
        """steer_channel 是否发出打断信号（鸭子类型：.interrupted 或 .is_set）。"""
        channel = self.steer_channel
        if channel is None:
            return False
        if hasattr(channel, "interrupted"):
            return bool(channel.interrupted)
        if hasattr(channel, "is_set"):
            try:
                return bool(channel.is_set())
            except Exception:  # pragma: no cover
                return False
        return False

    async def _steer_check(self, step_no: int) -> dict[str, Any] | None:
        """每轮检查点：返回干预指令 dict 或 None。

        Returns:
            {"kind": "interrupt", "reason": str}
            {"kind": "approval", "decision": dict, "approved": bool}
            {"kind": "instruction", "instruction": str}
        """
        if self._stop_event.is_set():
            return {"kind": "interrupt", "reason": "engine stop requested"}
        if self._channel_interrupted():
            reason = ""
            try:
                reason = getattr(self.steer_channel, "interrupt_reason", "")
            except Exception:  # pragma: no cover
                pass
            return {"kind": "interrupt", "reason": reason or "user interrupt"}

        if self.approval_gate is not None and step_no > 0:
            try:
                decision = await self.approval_gate(
                    scope=self.name, summary=f"step {step_no}"
                )
                outcome = decision.get("outcome") if isinstance(decision, dict) else None
                if outcome == "rejected":
                    return {"kind": "approval", "decision": decision, "approved": False}
                if outcome == "approved":
                    return {"kind": "approval", "decision": decision, "approved": True}
            except Exception as exc:  # pragma: no cover - 审批门异常不致命
                logger.warning("steer check approval_gate failed: %s", exc)

        return None

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    async def run_turn(
        self,
        task: dict[str, Any],
        *,
        history: list[dict[str, Any]] | None = None,
        on_tool_call: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """单次任务执行：ReAct 循环 + 每轮 steer 检查点。

        Returns:
            {"status": "completed" | "interrupted" | "rejected" | "error",
             "answer": str | None, "steps": int, "reason": str | None,
             "tool_calls": [dict]}
        """
        max_steps = int(self.config.get("max_steps", 20))
        history = list(history or [])
        tool_calls: list[dict[str, Any]] = []

        for step_no in range(max_steps):
            steer = await self._steer_check(step_no)
            if steer is not None:
                if steer["kind"] == "interrupt":
                    return {
                        "status": "interrupted",
                        "answer": None,
                        "steps": step_no,
                        "reason": steer.get("reason", "interrupted"),
                        "tool_calls": tool_calls,
                    }
                if steer["kind"] == "approval" and not steer["approved"]:
                    return {
                        "status": "rejected",
                        "answer": None,
                        "steps": step_no,
                        "reason": "approval rejected by human",
                        "tool_calls": tool_calls,
                    }
                # approved → 继续

            try:
                response = await self.llm_caller(
                    messages=history,
                    tools=[{"name": n} for n in self.tools],
                    max_tokens=int(self.config.get("max_tokens", 2048)),
                )
            except Exception as exc:
                self._last_error = f"llm: {type(exc).__name__}: {exc}"
                return {
                    "status": "error",
                    "answer": None,
                    "steps": step_no,
                    "reason": self._last_error,
                    "tool_calls": tool_calls,
                }

            text = _resp_text(response)
            final = response.get("final", False) or _looks_final(text)
            if final:
                return {
                    "status": "completed",
                    "answer": text,
                    "steps": step_no + 1,
                    "reason": None,
                    "tool_calls": tool_calls,
                }

            tool_name, tool_args = _parse_action(text)
            if tool_name is None or tool_name not in self.tools:
                history.append({"role": "assistant", "content": text})
                history.append(
                    {
                        "role": "user",
                        "content": f"unknown tool {tool_name!r}; available: {sorted(self.tools)}",
                    }
                )
                continue

            try:
                result = self.tools[tool_name](**tool_args)
                if inspect.isawaitable(result):
                    result = await result
            except Exception as exc:
                result = {"error": f"{type(exc).__name__}: {exc}"}
            tool_calls.append({"tool": tool_name, "args": tool_args, "result": result})
            if on_tool_call:
                try:
                    on_tool_call(tool_calls[-1])
                except Exception:  # pragma: no cover
                    pass
            history.append({"role": "assistant", "content": text})
            history.append({"role": "user", "content": f"[tool {tool_name}] {result}"})

        return {
            "status": "interrupted",  # 达到 max_steps：按截断处理
            "answer": None,
            "steps": max_steps,
            "reason": f"max_steps={max_steps} reached",
            "tool_calls": tool_calls,
        }

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_task_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """on_demand 入口：执行单任务（Task 队列语义由调用方持有）。"""
        input_data = input_data or {}
        result = await self.run_turn(input_data, on_tool_call=on_task_done)
        if on_task_done:
            try:
                on_task_done(result)
            except Exception:  # pragma: no cover
                pass
        return result

    def stop(self) -> None:
        """请求停止：下一轮 steer 检查点生效。"""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "tools": len(self.tools),
                "has_steer_channel": self.steer_channel is not None,
                "has_approval_gate": self.approval_gate is not None,
                "has_steer_fuser": self.steer_fuser is not None,
                "last_error": self._last_error,
            },
        }


# ---------------------------------------------------------------------------
# 响应解析（纯算法辅助）
# ---------------------------------------------------------------------------


def _resp_text(response: dict[str, Any]) -> str:
    content = response.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content)


def _looks_final(text: str) -> bool:
    lowered = text.strip().lower()
    return lowered.startswith("final:") or lowered.startswith("answer:") or lowered.startswith("结论:")


def _parse_action(text: str) -> tuple[str | None, dict[str, Any]]:
    """从 ReAct 输出解析 tool 调用：`tool: name\nargs: key=value ...` 或 JSON。"""
    import json
    import re

    for line in text.splitlines():
        m = re.match(r"\s*tool:\s*(\S+)", line)
        if m:
            name = m.group(1)
            args: dict[str, Any] = {}
            args_text = "\n".join(
                line for line in text.splitlines() if not re.match(r"\s*tool:", line)
            ).strip()
            if args_text.startswith("{"):
                try:
                    parsed = json.loads(args_text)
                    if isinstance(parsed, dict):
                        args = parsed
                except json.JSONDecodeError:
                    pass
            else:
                for kv in re.findall(r"(\w+)=(\S+)", args_text):
                    args[kv[0]] = _coerce(kv[1])
            return name, args
    return None, {}


def _coerce(value: str) -> Any:
    if value in ("true", "True"):
        return True
    if value in ("false", "False"):
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


register_skeleton("steerable_agentic_loop", SteerableAgenticLoopEngine)
