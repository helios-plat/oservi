"""
oservice: agentic_loop
=======================
hicode 的核心引擎骨架。Build 模式 / Subagent 内嵌循环的顶层编排者。

五红线自检（§8.2）
------------------
✅ 红线 1  反复出现的服务形态：agentic loop 是工业成熟模式（Claude Code / opencode 均用此结构）
✅ 红线 2  机制与业务分离：引擎固化"怎么跑"（调度/循环/状态机）；业务靠注入 callable
✅ 红线 3  注入点契约声明：每注入点含 kind + cardinality（见 injection_points）
✅ 红线 4  无状态骨架：引擎持运行时状态（running/tick_count），不持业务状态/不写主库持久化
✅ 红线 5  依赖倒置：骨架不硬编码 import 具体 oprim/oskill/omodul；具体实现装配时注入

注入点一览
----------
llm_caller         oprim   1         主 LLM 调用（支持 streaming）
tools              layer4  1..n      工具适配器（带 JSON schema 的 callable）
hook_dispatch      layer4  0..1      25 个生命周期点的 hook 路由器
permission_gate    layer4  0..1      PreToolUse 权限决策（接 match_permission_rule）
skill_selector     oskill  0..1      渐进式 skill 加载决策
todo_tracker       layer4  0..1      TodoWrite 状态维护
thinking_controller oskill 0..1      escalate_thinking_budget
compactor          omodul  0..1      上下文接近上限时压缩
changeset_applier  omodul  0..1      file edit 落盘（接 apply_changeset）
subagent_runner    omodul  0..1      子 agent 任务（接 run_subagent）

TriggerMode: on_demand（非阻塞，外部 await session()）
对应 §8.4：runner 返回即引擎空闲，下次调用开启新会话。
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


# ---------------------------------------------------------------------------
# InjectionKind / Injection 契约（§8.3）
# ---------------------------------------------------------------------------

InjectionKind = Literal["oprim", "oskill", "omodul", "obase", "layer4"]


@dataclass
class Injection:
    kind: InjectionKind
    cardinality: str   # "1" | "0..1" | "1..n" | "0..n"
    description: str


# ---------------------------------------------------------------------------
# EngineSkeleton 基类（§8.6 标准模式）
# ---------------------------------------------------------------------------

class ManifestValidationError(Exception):
    pass


class EngineSkeleton:
    """所有 oservice 引擎的基类。"""
    injection_points: dict[str, Injection] = {}

    def __init__(self, *, trigger: dict, config: dict | None = None):
        self.trigger = trigger
        self.config = config or {}
        self._running = False
        self._tick_count = 0
        self._injected: dict[str, Any] = {}

    def assemble(self, **kwargs) -> "EngineSkeleton":
        """
        装配注入点。检查 required（cardinality="1" 或 "1..n"）是否满足。
        返回 self 以支持链式调用。
        """
        self._injected = kwargs

        for name, inj in self.injection_points.items():
            required = not inj.cardinality.startswith("0")
            provided = name in kwargs and kwargs[name] is not None
            if required and not provided:
                raise ManifestValidationError(
                    f"required injection '{name}' ({inj.kind}, {inj.cardinality}) not provided"
                )

        return self

    def health(self) -> dict:
        return {
            "status": "healthy" if self._running else "stopped",
            "tick_count": self._tick_count,
            "injected": list(self._injected.keys()),
        }

    def run(self) -> None:
        """on_demand 模式：run() 直接返回（外部 await session()）。"""
        self._running = True


# ---------------------------------------------------------------------------
# ToolSpec：Layer 4 tool-adapter 契约
# ---------------------------------------------------------------------------

@dataclass
class ToolSpec:
    """Layer 4 tool-adapter 包装。name + JSON schema + 可调用实现。"""
    name: str
    description: str
    input_schema: dict          # JSON Schema object
    callable: Callable          # async (input_dict) -> Any
    readonly: bool = False      # plan 模式只允许 readonly=True 工具


# ---------------------------------------------------------------------------
# 会话状态（运行时，不持久化业务态）
# ---------------------------------------------------------------------------

@dataclass
class SessionState:
    """引擎运行时状态（不是业务态）。业务 session 数据在 Layer 4 + obase.persistence。"""
    session_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    messages: list[dict] = field(default_factory=list)
    todos: list[dict] = field(default_factory=list)
    iteration: int = 0
    start_ts: float = field(default_factory=time.time)
    events: list[dict] = field(default_factory=list)

    def record(self, **kwargs) -> None:
        self.events.append({"ts": time.time(), "iter": self.iteration, **kwargs})


# ---------------------------------------------------------------------------
# AgenticLoop 引擎
# ---------------------------------------------------------------------------

class AgenticLoop(EngineSkeleton):
    """
    oservice: agentic_loop
    Build 模式核心循环引擎（on_demand）。
    """

    injection_points = {
        "llm_caller": Injection(
            kind="oprim", cardinality="1",
            description="主 LLM 调用，callable(messages, tools, max_tokens, thinking_budget) -> dict"
        ),
        "tools": Injection(
            kind="layer4", cardinality="1..n",
            description="工具适配器列表（ToolSpec），每个带 JSON schema + async callable"
        ),
        "hook_dispatch": Injection(
            kind="layer4", cardinality="0..1",
            description="Hook 路由器：async (event, payload) -> {decision, modified_payload}"
        ),
        "permission_gate": Injection(
            kind="layer4", cardinality="0..1",
            description="权限决策：(tool_call) -> 'allow'|'deny'|'ask'"
        ),
        "skill_selector": Injection(
            kind="oskill", cardinality="0..1",
            description="渐进式 skill 选择：(task, context) -> list[SkillContext]"
        ),
        "todo_tracker": Injection(
            kind="layer4", cardinality="0..1",
            description="Todo 状态维护：(todos, update) -> list[dict]"
        ),
        "thinking_controller": Injection(
            kind="oskill", cardinality="0..1",
            description="扩展思考 token 预算：(prompt) -> int"
        ),
        "compactor": Injection(
            kind="omodul", cardinality="0..1",
            description="上下文压缩事务：(config, input, output_dir) -> dict"
        ),
        "changeset_applier": Injection(
            kind="omodul", cardinality="0..1",
            description="编辑落盘事务：(config, input, output_dir) -> dict"
        ),
        "subagent_runner": Injection(
            kind="omodul", cardinality="0..1",
            description="子 agent 调用事务：(config, input, output_dir) -> dict"
        ),
    }

    def __init__(
        self,
        *,
        max_iterations: int = 50,
        context_limit_tokens: int = 180_000,
        compaction_threshold: float = 0.85,
        budget_usd: float = 10.0,
        model: str = "claude-sonnet-4-6",
        mode: Literal["build", "plan"] = "build",
        output_dir: Path = Path("/tmp/hicode"),
        thinking_budget: int | None = None,
    ):
        super().__init__(trigger={"on_demand": True})
        self.max_iterations = max_iterations
        self.context_limit_tokens = context_limit_tokens
        self.compaction_threshold = compaction_threshold
        self.budget_usd = budget_usd
        self.model = model
        self.mode = mode
        self.output_dir = Path(output_dir)
        self.thinking_budget = thinking_budget

        # 运行时状态（不是业务态）
        self._total_cost: float = 0.0
        self._total_in_tok: int = 0
        self._total_out_tok: int = 0

    # ── 注입점 접근자 ─────────────────────────────────────────────────────

    @property
    def _llm(self) -> Callable:
        return self._injected["llm_caller"]

    @property
    def _tools(self) -> list[ToolSpec]:
        tools = self._injected["tools"]
        if self.mode == "plan":
            return [t for t in tools if t.readonly]
        return tools

    def _hook(self, event: str, payload: dict) -> "asyncio.coroutine":
        dispatch = self._injected.get("hook_dispatch")
        if dispatch:
            return dispatch(event, payload)
        return _noop_hook(event, payload)

    def _check_permission(self, tool_call: dict) -> str:
        gate = self._injected.get("permission_gate")
        if gate:
            return gate(tool_call)
        return "allow"

    def _get_thinking_budget(self, prompt: str) -> int | None:
        ctrl = self._injected.get("thinking_controller")
        if ctrl:
            return ctrl(prompt)
        return self.thinking_budget

    # ── 工具 schema 构建 ──────────────────────────────────────────────────

    def _build_tool_schemas(self) -> list[dict]:
        """把 ToolSpec 列表转成 LLM tools 参数格式。"""
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.input_schema,
            }
            for t in self._tools
        ]

    def _find_tool(self, name: str) -> ToolSpec | None:
        return next((t for t in self._tools if t.name == name), None)

    # ── context 大小估算 ─────────────────────────────────────────────────

    def _estimate_tokens(self, messages: list[dict]) -> int:
        """粗估 token 数（生产版接 obase.TokenCounter）。"""
        total_chars = sum(
            len(json.dumps(m, ensure_ascii=False)) for m in messages
        )
        return total_chars // 4  # ~4 chars/token

    def _needs_compaction(self, messages: list[dict]) -> bool:
        estimated = self._estimate_tokens(messages)
        return estimated > self.context_limit_tokens * self.compaction_threshold

    # ── 核心会话 loop ─────────────────────────────────────────────────────

    async def session(
        self,
        task: str,
        *,
        system_prompt: str = "",
        on_step: Callable[[dict], None] | None = None,
        on_token: Callable[[str], None] | None = None,
    ) -> dict:
        """
        执行一次完整的 agentic session（on_demand）。

        返回 {
            "result": str,           # LLM 最终输出
            "status": "completed" | "interrupted" | "budget_exceeded" | "failed",
            "iterations": int,
            "cost_usd": float,
            "session_id": str,
            "events": list[dict],    # 引擎事件日志
        }
        """
        self._running = True
        self._total_cost = 0.0
        self._total_in_tok = 0
        self._total_out_tok = 0

        state = SessionState()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # ── UserPromptSubmit hook ─────────────────────────────────────────
        hook_result = await self._hook("UserPromptSubmit", {"task": task})
        if hook_result.get("decision") == "block":
            return _error_result("blocked by UserPromptSubmit hook", state)

        task = hook_result.get("modified_payload", {}).get("task", task)

        # ── skill 渐进加载 ────────────────────────────────────────────────
        extra_context = ""
        selector = self._injected.get("skill_selector")
        if selector:
            skills = selector(task, extra_context)
            if skills:
                extra_context = "\n\n".join(
                    s.get("body", "") for s in skills if s.get("body")
                )
                state.record(event="skills_loaded", count=len(skills))

        # ── 初始系统 prompt ───────────────────────────────────────────────
        effective_system = _build_system(
            system_prompt, self.mode, self._tools, extra_context
        )

        # 把 system 注入为 messages[0]（部分 provider 不支持 system 参数时的兼容做法）
        state.messages = [
            {"role": "user", "content": task}
        ]

        tool_schemas = self._build_tool_schemas()
        final_text = ""
        status = "completed"

        if on_step:
            on_step({"event": "session_start", "session_id": state.session_id,
                     "mode": self.mode, "task_preview": task[:120]})

        # ── SessionStart hook ─────────────────────────────────────────────
        await self._hook("SessionStart", {"session_id": state.session_id})

        try:
            for iteration in range(self.max_iterations):
                state.iteration = iteration
                self._tick_count += 1

                # 上下文压缩检查
                if self._needs_compaction(state.messages):
                    state.messages = await self._maybe_compact(
                        state.messages, state, on_step
                    )

                # ── LLM 调用 ──────────────────────────────────────────────
                # 生产版：调 oprim.llm_complete(messages, caller=self._llm, ...)
                # llm_complete 的原子价值：消息校验 + token 截断 + 错误标准化 + usage 提取
                # （M1 Owner 裁决：llm_complete 不是 caller 的 alias，是规范化外壳）
                # 此处为简洁直接调 _llm（stub），生产版替换为 llm_complete 调用。
                thinking_budget = self._get_thinking_budget(task)
                state.record(event="llm_call", iter=iteration)

                response = await self._llm(
                    messages=state.messages,
                    tools=tool_schemas or None,
                    max_tokens=8192,
                    thinking_budget=thinking_budget,
                    system=effective_system,
                )

                # cost 累加（引擎持运行时 cost，不用 ContextVar——引擎是 layer 4 边界）
                usage = response.get("usage", {})
                in_tok = usage.get("input_tokens", 0)
                out_tok = usage.get("output_tokens", 0)
                self._total_in_tok += in_tok
                self._total_out_tok += out_tok
                call_cost = _estimate_cost(in_tok, out_tok)
                self._total_cost += call_cost

                if self._total_cost > self.budget_usd:
                    status = "budget_exceeded"
                    state.record(event="budget_exceeded",
                                 total_usd=self._total_cost, limit=self.budget_usd)
                    if on_step:
                        on_step({"event": "budget_exceeded",
                                 "total_usd": self._total_cost})
                    break

                content = response.get("content", [])
                stop_reason = response.get("stop_reason", "end_turn")

                # 收集文本 / thinking
                for block in content:
                    if block.get("type") == "text":
                        final_text = block["text"]
                        if on_token:
                            on_token(final_text)
                        state.record(event="text", preview=final_text[:80])
                    elif block.get("type") == "thinking":
                        state.record(event="thinking",
                                     preview=block.get("thinking", "")[:60])

                if stop_reason != "tool_use":
                    state.record(event="loop_done", reason=stop_reason, iter=iteration)
                    break

                # ── 工具调用处理 ──────────────────────────────────────────
                tool_uses = [b for b in content if b.get("type") == "tool_use"]
                tool_results_content = []

                for tool_use in tool_uses:
                    tr = await self._handle_tool_use(
                        tool_use, state, on_step
                    )
                    tool_results_content.append(tr)

                # 更新 messages
                state.messages.append({"role": "assistant", "content": content})
                state.messages.append({
                    "role": "user", "content": tool_results_content
                })

                if on_step:
                    on_step({
                        "event": "iteration_done",
                        "iteration": iteration,
                        "n_tools": len(tool_uses),
                        "cost_usd": round(self._total_cost, 6),
                    })

        except asyncio.CancelledError:
            status = "interrupted"
            state.record(event="cancelled")
            raise
        except Exception as exc:
            status = "failed"
            state.record(event="error", error=str(exc))
            final_text = f"[engine error: {exc}]"
        finally:
            # Stop hook
            await self._hook("Stop", {
                "session_id": state.session_id,
                "iterations": state.iteration,
                "status": status,
            })
            self._running = False

        if on_step:
            on_step({"event": "session_done", "status": status,
                     "iterations": state.iteration + 1,
                     "cost_usd": round(self._total_cost, 6)})

        return {
            "result": final_text,
            "status": status,
            "iterations": state.iteration + 1,
            "cost_usd": round(self._total_cost, 6),
            "in_tokens": self._total_in_tok,
            "out_tokens": self._total_out_tok,
            "session_id": state.session_id,
            "events": state.events,
        }

    # ── 工具调用处理 ──────────────────────────────────────────────────────

    async def _handle_tool_use(
        self,
        tool_use: dict,
        state: SessionState,
        on_step: Callable | None,
    ) -> dict:
        """
        单个工具调用的完整生命周期：
        PreToolUse hook → permission check → 执行 → PostToolUse hook
        返回 tool_result content block。
        """
        tool_name = tool_use.get("name", "")
        tool_input = tool_use.get("input", {})
        tool_use_id = tool_use.get("id", str(uuid.uuid4()))

        state.record(event="tool_call", tool=tool_name,
                     input_preview=str(tool_input)[:150])
        if on_step:
            on_step({"event": "tool_call", "tool": tool_name, "input": tool_input})

        # ── PreToolUse hook（主安全检查点）────────────────────────────────
        hook_result = await self._hook(
            "PreToolUse", {"tool": tool_name, "input": tool_input}
        )
        if hook_result.get("decision") == "block":
            state.record(event="hook_blocked", tool=tool_name)
            return _tool_result_block(
                tool_use_id, f"[blocked by PreToolUse hook: {tool_name}]"
            )

        # hook 可修改 input
        if "modified_payload" in hook_result:
            tool_input = hook_result["modified_payload"].get("input", tool_input)

        # ── 权限检查 ──────────────────────────────────────────────────────
        permission = self._check_permission({"name": tool_name, "input": tool_input})

        if permission == "deny":
            state.record(event="permission_denied", tool=tool_name)
            return _tool_result_block(
                tool_use_id,
                f"[permission denied: {tool_name} not allowed in {self.mode} mode]"
            )

        if permission == "ask":
            # PermissionRequest hook（Layer 4 gate 决定 approve/deny）
            pr_result = await self._hook(
                "PermissionRequest", {"tool": tool_name, "input": tool_input}
            )
            if pr_result.get("decision") != "allow":
                state.record(event="permission_ask_denied", tool=tool_name)
                return _tool_result_block(
                    tool_use_id, f"[permission denied after review: {tool_name}]"
                )

        # ── 查找工具 ──────────────────────────────────────────────────────
        tool_spec = self._find_tool(tool_name)
        if tool_spec is None:
            state.record(event="tool_not_found", tool=tool_name)
            return _tool_result_block(
                tool_use_id, f"[tool not found: {tool_name}]"
            )

        # ── 执行工具 ──────────────────────────────────────────────────────
        try:
            if inspect.iscoroutinefunction(tool_spec.callable):
                result = await tool_spec.callable(tool_input)
            else:
                result = await asyncio.to_thread(tool_spec.callable, tool_input)

            # Todo tracker
            if tool_name == "TodoWrite":
                tracker = self._injected.get("todo_tracker")
                if tracker:
                    state.todos = tracker(state.todos, tool_input)

            result_str = json.dumps(result) if isinstance(result, dict) else str(result)
            state.record(event="tool_result", tool=tool_name,
                         result_preview=result_str[:200])

        except Exception as exc:
            result_str = f"[tool error: {exc}]"
            state.record(event="tool_error", tool=tool_name, error=str(exc))

        # ── PostToolUse hook ──────────────────────────────────────────────
        await self._hook(
            "PostToolUse", {"tool": tool_name, "result": result_str[:500]}
        )

        if on_step:
            on_step({"event": "tool_result", "tool": tool_name,
                     "result_preview": result_str[:120]})

        return _tool_result_block(tool_use_id, result_str)

    # ── context 压缩 ──────────────────────────────────────────────────────

    async def _maybe_compact(
        self,
        messages: list[dict],
        state: SessionState,
        on_step: Callable | None,
    ) -> list[dict]:
        """
        上下文超出阈值时触发压缩（PreCompact hook → compactor omodul）。
        若 compactor 未注入，直接截断（最后 N 条）。
        """
        # PreCompact hook
        await self._hook("PreCompact", {"n_messages": len(messages)})

        compactor = self._injected.get("compactor")
        if compactor is None:
            # 简单截断：保留最近 20 条
            kept = messages[-20:]
            state.record(event="context_truncated",
                         before=len(messages), after=len(kept))
            if on_step:
                on_step({"event": "context_compacted", "strategy": "truncate",
                         "before": len(messages), "after": len(kept)})
            return kept

        # compactor 是 omodul：(config, input, output_dir) -> dict
        # 此处简化调用（生产版用真实 compact_conversation omodul）
        try:
            from types import SimpleNamespace
            compact_input = SimpleNamespace(messages=messages)
            compact_config = SimpleNamespace()
            result = compactor(compact_config, compact_input, self.output_dir)
            compacted = result.get("messages", messages[-20:])
            state.record(event="context_compacted",
                         before=len(messages), after=len(compacted))
            return compacted
        except Exception:
            return messages[-20:]


# ---------------------------------------------------------------------------
# 引擎注册（§8.6 register_skeleton）
# ---------------------------------------------------------------------------

_SKELETON_REGISTRY: dict[str, type] = {}


def register_skeleton(name: str, cls: type) -> None:
    _SKELETON_REGISTRY[name] = cls


def get_skeleton(name: str) -> type:
    if name not in _SKELETON_REGISTRY:
        raise KeyError(f"unknown skeleton: {name}")
    return _SKELETON_REGISTRY[name]


register_skeleton("agentic_loop", AgenticLoop)


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

async def _noop_hook(event: str, payload: dict) -> dict:
    return {"decision": "allow", "modified_payload": payload}


def _tool_result_block(tool_use_id: str, content: str) -> dict:
    return {
        "type": "tool_result",
        "tool_use_id": tool_use_id,
        "content": content,
    }


def _build_system(
    base: str,
    mode: str,
    tools: list[ToolSpec],
    extra_context: str,
) -> str:
    parts = [base] if base else ["You are hicode, an expert AI coding agent."]
    parts.append(f"Mode: {mode.upper()}.")
    if mode == "plan":
        parts.append("In PLAN mode: analyze and propose changes only. Do NOT write files or run commands.")
    if extra_context:
        parts.append(f"\n## Skills\n{extra_context}")
    return "\n\n".join(parts)


def _estimate_cost(in_tok: int, out_tok: int) -> float:
    """粗估成本（生产版接 obase.PricingTable）。Sonnet 4.6 价格。"""
    return in_tok * 3e-6 + out_tok * 15e-6


def _error_result(reason: str, state: SessionState) -> dict:
    return {
        "result": "",
        "status": "failed",
        "iterations": 0,
        "cost_usd": 0.0,
        "in_tokens": 0,
        "out_tokens": 0,
        "session_id": state.session_id,
        "events": [{"ts": time.time(), "event": "error", "reason": reason}],
    }
