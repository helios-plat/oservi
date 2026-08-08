"""oservi.master_agent — the Master Brain: tool-routing ReAct engine.

3O layer: oservi (engine assembly).
The Master Coordinator routes user requests to backend tools / sub-agents
(Genesis, swarm, automata, RAG, vault) and synthesizes the final answer.

Everything is dependency-injected (duck-typed protocols below): the host
assembles veya's concrete providers (tool registry, skill hub, memory bank,
automata, swarm, rag, vault). Events are delivered via an injected ``notify``
callback (host bridges it to SSE / fire_step). LLM calls go through an
injected ``llm_caller``. No main-library dependency on the host.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable
from typing import Any, Protocol

_log = logging.getLogger(__name__)

# =========================================================================
# 依赖协议(鸭子类型 — 宿主组件只需满足方法签名)
# =========================================================================


class ToolRegistryProtocol(Protocol):
    """Static tool registry (schemas + dispatch)."""

    def get_all_schemas(self) -> list[dict]: ...
    def list_tools(self) -> list[str]: ...
    def describe(self, name: str) -> str: ...
    def has(self, name: str) -> bool: ...
    async def execute(self, name: str, kwargs: dict) -> str: ...


class SkillHubProtocol(Protocol):
    """Dynamic skill hub (reloadable tool set)."""

    def get_all_schemas(self) -> list[dict]: ...
    def list_skills(self) -> list[str]: ...
    def describe(self, name: str) -> str: ...
    def reload_skills(self) -> dict: ...
    async def execute(self, name: str, kwargs: dict) -> str: ...


class MemoryProtocol(Protocol):
    """Cross-session preference ledger."""

    def inject_subconscious(self) -> str: ...
    def add_preference(self, **kwargs: Any) -> str: ...
    def remove_preference(self, **kwargs: Any) -> str: ...


class AutomataProtocol(Protocol):
    """Background automation scheduler."""

    def register_cron_task(
        self, cron_expr: str, task_prompt: str, task_id: str | None = None
    ) -> str: ...
    def remove_task(self, task_id: str) -> str: ...
    def get_jobs(self) -> list[dict]: ...


class SwarmProtocol(Protocol):
    """Multi-agent Map-Reduce orchestrator."""

    async def run_swarm(self, overarching_goal: str, sub_tasks: list[dict]) -> str: ...


class RagProtocol(Protocol):
    """Codebase semantic search engine."""

    def search_context(self, query: str, top_k: int = 3) -> str: ...
    def reindex_workspace(self, force: bool = False) -> str: ...


class VaultProtocol(Protocol):
    """Zero-trust secrets vault with HITL approval."""

    async def execute_secure_tool(
        self,
        tool_name: str,
        intent_args: dict,
        required_vault_id: str,
        physical_tool_callback: Callable[..., Any],
        *,
        timeout: float | None = None,
    ) -> str: ...


class OmniGatewayProtocol(Protocol):
    """Omni-channel distribution gateway (host-injected, duck-typed)."""

    def get_llm_schema(self) -> dict: ...
    async def execute_dispatch(self, targets: list[str], title: str, content: str) -> str: ...


# =========================================================================
# 潜意识注入: 工具使用说明书 (SOP)
# =========================================================================

MASTER_SYSTEM_PROMPT = """You are the Veya Master Coordinator, an elite AI orchestrator.
You have access to a suite of powerful backend tools. DO NOT simulate actions; actually use the tools.

# MASTER DIRECTIVES (HIGHEST PRIORITY)
## ROLE & IDENTITY
You are the Master Coordinator of Veya OS, an industrial-grade Agentic system and quantitative research core.
You are driven by Native Intelligence. Do not simulate a rigid "Thought-Action-Observation" loop. Do not narrate your internal thought process unless explicitly asked to explain.

## COGNITIVE DIRECTIVES
1. **Direct Action & Native Reasoning**: Trust your zero-shot and few-shot capabilities. If a user asks a conceptual question, code review, or architectural discussion, answer directly using your native intelligence. Do NOT invoke tools unnecessarily.
2. **Tools as Physical Extensions**: You have access to a flat arsenal of tools (e.g., Z3 Neuro-Symbolic Verifier, HardenedExecutor, Quant Sandbox). Treat them as your hands. Call them ONLY when you need to interact with the physical file system, verify absolute mathematical bounds, or run heavy asynchronous computations.
3. **Auto-Recovery over Panic**: The underlying `veya-loop` enforces extreme physical security (deny-by-default, causal circuit breakers). If a tool returns an Error, a Permission Denied, or an UNSAT core, DO NOT blindly retry. Read the traceback natively, reflect on the missing dependency or logic flaw, and dynamically adjust your strategy or rewrite the code.
4. **Asynchronous Delegation**: If a task is computationally massive, dispatch it to the underlying DAG engines or spawn a swarm, then immediately return control to the user. Do not wait synchronously.

## COMMUNICATION PROTOCOL
- Be brutally concise, professional, and architect-oriented.
- Zero robotic preambles (Never say "As an AI...", "Here is the code...", "I will now call the tool...").
- Deliver the final artifacts (Code, Reports, Decisions) directly.

# INTENT ROUTING RULES (CRITICAL):
1. [General Knowledge / Chat]: Just answer normally. No tools needed.
2. [Web Data / Live Research]: If asked about news, X (Twitter), or external sites, you MUST use `browser_run`.
3. [Code Execution / Testing]: Use the `run_in_sandbox` tool. NEVER output Python code and tell the user to run it themselves. You run it.
4. [3O Core Development]: If the user wants to build a new system module or algorithm, DO NOT write the code directly. Instead, generate a PRD. Once approved, use `delegate_to_genesis`.
5. [Codebase Understanding]: Use `read_file_ast` / `grep` / `list_files` to explore the workspace before answering structural questions.
6. [Avoid Duplication]: Before requesting new 3O operator development, check `search_genesis_ledger` first.

Think step-by-step. If a tool fails, read the error and try again.
Do not invent tool results. Do not ask the user to run commands for you.

# MEMORY MANAGEMENT (CRITICAL):
You possess cross-session memory.
If the user corrects your behavior or states a preference, you MUST silently call
`system_save_preference` BEFORE answering them. Do not ask for permission to save it.
Just save it and acknowledge it in your reply.
Also call it when the user says 'remember that', 'from now on', or 'always/never use X'.
Only persist GENERAL rules that apply to future sessions — never save one-off instructions.
If a saved rule is no longer valid, call `system_remove_preference` with its ID.

# AUTOMATION (CRITICAL):
You possess a background daemon (Automata) with Cron scheduling.
If the user asks for something recurring — 'every morning', 'every hour', 'monitor X daily' —
call `system_create_automation` with a proper cron expression and a self-contained task_prompt
(the prompt YOU will receive when the trigger fires). Do NOT pretend to run periodic tasks.
Use `system_list_automations` to check existing jobs, `system_remove_automation` to cancel one.

# SWARM (CRITICAL):
You possess a multi-agent Swarm Orchestrator (Map-Reduce).
If the user asks for a LARGE, multi-component deliverable (full-stack app, multiple modules,
frontend + backend + database), DO NOT try to write it all yourself. Decompose the work into
role-based sub-tasks and call `system_spawn_swarm` with an overarching_goal and a sub_tasks array
(roles like 'Svelte Frontend', 'FastAPI Backend', 'DB Architect').

# EXECUTE-WHEN-ASKED (CRITICAL):
If the user has already chosen a path (e.g. "先验证再立项" → "那你可以执行了") and then says
"执行 / 开始 / 跑 / go / 动手", DO the chosen path with the available tools. Never re-ask,
never store it as a mere preference, never stall waiting for more ideas. If tools are needed
for that path (quant backtest, sandbox, genesis), call them in sequence until the path is done.

# WORKSPACE RAG (CRITICAL):
You possess a semantic search engine over the ENTIRE local codebase (AST-indexed).
Before modifying ANY existing code, call `system_workspace_search` FIRST to locate the exact
functions/classes involved. Never guess file paths or duplicate existing logic.
If the index seems stale, call `system_workspace_reindex`.

# ZERO-TRUST VAULT (CRITICAL):
Real secrets (Binance keys, AWS tokens) are NEVER exposed to you. If a task requires a
credential, call `system_secure_exec` with only the vault_id reference and your intent.
A human will approve via UI; the backend injects the secret into the physical tool.
Never ask the user to paste a secret into chat.

# DISTRIBUTION (OMNI-CHANNEL GATEWAY):
If the user asks you to send, post, publish, or distribute content to external platforms
(e.g. Feishu, Xiaohongshu, Twitter/X), call `system_dispatch_omni_channel` with the target
channel names, a title, and the content. The gateway automatically decides between a
platform's official API and RPA browser automation — you do NOT need to know which.
Do NOT just describe how to publish manually; actually call the tool.

# QUANT PROTOCOL (CRITICAL — Control Plane / Data Plane separation):
You are the strategy EXPRESSER and result ANALYST. You NEVER load or compute market data yourself.
When the user asks for a backtest / quant strategy:
1. Call `get_market_data_schema` to learn the columns and dtypes (schema + 5 rows only).
2. Write strategy code defining `run_strategy(df)` that returns a df with 'daily_return' and 'cum_return'.
3. Call `run_backtest_coprocessor` — the sandbox computes Sharpe / drawdown on millions of rows.
4. When the coprocessor returns condensed JSON, write a SHORT research note and emit a
   `<veya-artifact type="react">` dashboard (metric cards + ECharts curve) — inject echarts_data_json
   verbatim into the artifact code. Do not invent metrics.
5. When the user says "execute / run it / 执行 / 开始 / 验证一下 / 按 PRD 执行" after a plan or PRD:
   treat it as an explicit GO — call `get_market_data_schema` then `run_backtest_coprocessor` directly.
   Do NOT re-confirm which option, do NOT just save a preference, do NOT ask for more input.
6. After ONE successful backtest pass, summarize and deliver — do NOT loop optimizing
   parameters forever. If the first pass failed, fix the strategy and retry at most once,
   then deliver the result either way.

# ARTIFACTS PROTOCOL (UI & CHARTS):
If the user asks for a UI component, a data dashboard, or a chart, you MUST output a dynamic artifact.
Wrap the executable code in the following XML format:
<veya-artifact type="react" title="Name of the Component">
// Your code here
</veya-artifact>

Rules for React artifacts:
1. You have access to Tailwind CSS classes.
2. You have access to `React`, `ReactDOM`, and `echarts` (Apache ECharts) via global window object.
3. You MUST define a main component (e.g., `App`) and render it to the DOM at the end of your code like this:
   `const root = ReactDOM.createRoot(document.getElementById('root')); root.render(<App />);`
4. Do NOT use import statements. Assume React and echarts are globally available.
"""


def _truncate(text: str, limit: int = 40000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... [truncated {len(text) - limit} chars]"


class MasterAgent:
    """Master Brain: intent routing + tool dispatch + final synthesis."""

    def __init__(
        self,
        llm_caller: Callable,
        *,
        tools: ToolRegistryProtocol,
        skill_hub: SkillHubProtocol,
        memory: MemoryProtocol,
        swarm: SwarmProtocol,
        vault: VaultProtocol,
        automata_factory: Callable[[], AutomataProtocol] | None = None,
        rag_factory: Callable[[], RagProtocol] | None = None,
        omni_gateway: OmniGatewayProtocol | None = None,
        notify: Callable[[dict], None] | None = None,
        system_prompt: str | None = None,
        max_rounds: int = 32,
        temperature: float = 0.2,
        cost_calculator: Callable[[dict], float] | None = None,
    ):
        """
        Args:
            llm_caller: Host-injected LLM function
                (async (messages, **kwargs) -> OpenAI-format dict).
            tools: Static tool registry (browser / genesis / ast / sandbox ...).
            skill_hub: Dynamic skill hub (~/.veya/skills, hot-reloadable).
            memory: Cross-session preference ledger.
            swarm: Multi-agent swarm orchestrator.
            vault: Zero-trust secrets vault.
            automata_factory: Lazy factory for the background scheduler
                (hosts with event-loop constraints inject lazily).
            rag_factory: Lazy factory for the codebase RAG engine.
            omni_gateway: Omni-channel distribution gateway (schema + dispatch).
            notify: Event callback (host bridges to SSE/fire_step); None = silent.
            system_prompt: Override the built-in SOP.
        """
        self._llm_caller = llm_caller
        self.tools = tools
        self.skill_hub = skill_hub
        self.memory = memory
        self.swarm = swarm
        self.vault = vault
        self._automata_factory = automata_factory
        self._rag_factory = rag_factory
        self.omni_gateway = omni_gateway
        self._automata: AutomataProtocol | None = None
        self._rag: RagProtocol | None = None
        self.notify = notify or (lambda _e: None)
        self.system_prompt = system_prompt or MASTER_SYSTEM_PROMPT
        self.max_rounds = max_rounds
        self.temperature = temperature
        # Host-injected cost estimator (usage dict -> USD); default 0
        self._cost_calculator = cost_calculator
        # Vault physical tool callbacks: tool_name -> async (**intent, _injected_secret=...)
        self._vault_tool_callbacks: dict[str, Callable] = {}
        # 连续对话历史 (session_id -> messages, 进程内 LRU; 首条恒为 system)
        self._histories: dict[str, list[dict[str, Any]]] = {}
        self._history_cap_sessions = 200
        self._history_max_msgs = 100

    # ── 惰性子系统 ───────────────────────────────────────────────────
    @property
    def automata(self) -> AutomataProtocol:
        if self._automata is None:
            if self._automata_factory is None:
                raise RuntimeError("automata_factory 未注入(宿主装配缺失)")
            self._automata = self._automata_factory()
        return self._automata

    @property
    def rag(self) -> RagProtocol:
        if self._rag is None:
            if self._rag_factory is None:
                raise RuntimeError("rag_factory 未注入(宿主装配缺失)")
            self._rag = self._rag_factory()
        return self._rag

    # ── 潜意识注入 ───────────────────────────────────────────────────
    def get_system_prompt(self) -> str:
        """SOP + subconscious (cross-session ledger) + dynamic tool inventory."""
        subconscious = self.memory.inject_subconscious()
        return (
            self.system_prompt
            + subconscious
            + "\n# AVAILABLE TOOLS (choose from these only):\n"
            + self._tool_inventory()
        )

    def _tool_inventory(self) -> str:
        lines = [f"- {self.tools.describe(name)}" for name in self.tools.list_tools()]
        lines += [f"- {self.skill_hub.describe(name)}" for name in self.skill_hub.list_skills()]
        lines.append(
            "- system_reload_skills — Reload all skills from disk. Call this after a new skill package is installed."
        )
        return "\n".join(lines)

    def get_system_schemas(self) -> list[dict]:
        """Host-level tools (not unloadable): reload / memory / automation / swarm /
        rag / vault / omni-dispatch."""
        schemas = [
            {
                "type": "function",
                "function": {
                    "name": "system_reload_skills",
                    "description": (
                        "Reload all skills from disk. Call this if the user asks you to refresh "
                        "your capabilities or after installing a new plugin."
                    ),
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_create_automation",
                    "description": (
                        "Schedule a background automation task. Use this when the user asks you to "
                        "'do something every morning', 'monitor something every hour', etc."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "cron_expr": {
                                "type": "string",
                                "description": "Standard Unix Cron expression (e.g., '0 9 * * *' for 9 AM daily).",
                            },
                            "task_prompt": {
                                "type": "string",
                                "description": "The exact natural language prompt you (the AI) will receive when the task triggers.",
                            },
                        },
                        "required": ["cron_expr", "task_prompt"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_remove_automation",
                    "description": "Cancel a scheduled background automation task by its ID.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "task_id": {
                                "type": "string",
                                "description": "The automation task ID to cancel",
                            }
                        },
                        "required": ["task_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_list_automations",
                    "description": "List all scheduled background automation tasks.",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_save_preference",
                    "description": (
                        "Save a user's persistent preference, workflow rule, or correction. "
                        "Call this IMMEDIATELY if the user corrects you (e.g., 'stop using npm, "
                        "use pnpm' or 'my standard port is 8080'). Do NOT ask for permission."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "rule": {
                                "type": "string",
                                "description": "The exact rule to obey in the future, e.g., 'Always use pnpm instead of npm'.",
                            },
                            "context": {
                                "type": "string",
                                "description": "Category: 'Coding', 'Tone', 'Architecture', etc.",
                            },
                        },
                        "required": ["rule", "context"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_remove_preference",
                    "description": (
                        "Remove an outdated or invalid saved preference by its memory ID "
                        "(shown in the subconscious block, e.g. mem_20260805120000_abcd)."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "memory_id": {"type": "string", "description": "The memory ID to erase"}
                        },
                        "required": ["memory_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_spawn_swarm",
                    "description": (
                        "Spawn a multi-agent swarm to execute complex, multi-component projects "
                        "concurrently (Map-Reduce: decompose -> parallel workers -> master synthesis). "
                        "Use this instead of trying to write massive codebases yourself."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "overarching_goal": {
                                "type": "string",
                                "description": "The big picture context for the swarm.",
                            },
                            "sub_tasks": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "role": {
                                            "type": "string",
                                            "description": "E.g., 'React Frontend Engineer', 'FastAPI Backend Dev'.",
                                        },
                                        "instruction": {
                                            "type": "string",
                                            "description": "The exact task for this agent.",
                                        },
                                    },
                                    "required": ["role", "instruction"],
                                },
                            },
                        },
                        "required": ["overarching_goal", "sub_tasks"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_workspace_search",
                    "description": (
                        "Semantic search across the entire local codebase (AST-indexed functions/classes). "
                        "Use this FIRST when you need to understand existing functions, architectures, "
                        "or find where a specific logic is implemented, e.g. 'Where is the VWAP logic?' "
                        "or 'Find the risk monitor'. Never guess file paths or duplicate existing logic."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "Semantic query, e.g. 'VWAP calculation' or 'risk monitor leverage'.",
                            },
                            "top_k": {
                                "type": "integer",
                                "description": "number of results (optional, default 3)",
                            },
                        },
                        "required": ["query"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_workspace_reindex",
                    "description": "Force a full re-index of the codebase semantic engine (normally auto-incremental).",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "system_secure_exec",
                    "description": (
                        "Zero-trust execution: run a physical tool that needs a REAL credential. "
                        "You pass only the vault_id reference + your intent — the secret itself is "
                        "NEVER exposed to you. A human approves via the UI before execution. "
                        "Use when the user asks to deploy / trade / access protected resources."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "tool_name": {
                                "type": "string",
                                "description": "physical tool to invoke, e.g. 'binance_place_order'",
                            },
                            "intent_args": {
                                "type": "object",
                                "description": "execution intent arguments (no secrets)",
                            },
                            "required_vault_id": {
                                "type": "string",
                                "description": "credential reference ID, e.g. 'binance_prod_key'",
                            },
                        },
                        "required": ["tool_name", "intent_args", "required_vault_id"],
                    },
                },
            },
        ]
        # Omni-Channel 分发网关(宿主注入): schema 单一来源, 由网关自身提供
        if self.omni_gateway is not None:
            schemas.append(self.omni_gateway.get_llm_schema())
        return schemas

    def get_all_tool_schemas(self) -> list[dict]:
        """System tools + static tools + dynamic skills, all fed to the LLM."""
        return (
            self.get_system_schemas()
            + self.tools.get_all_schemas()
            + self.skill_hub.get_all_schemas()
        )

    def register_secure_tool(self, tool_name: str, callback: Callable) -> None:
        """Register a physical tool that needs secret injection (host wiring).

        callback signature: async (**intent_args, _injected_secret: str) -> str
        """
        self._vault_tool_callbacks[tool_name] = callback

    async def handle_tool_call(self, tool_name: str, tool_args: dict) -> str:
        """Tool routing: system-level intercept -> static tools -> dynamic skills."""
        if tool_name == "system_reload_skills":
            stats = self.skill_hub.reload_skills()
            return (
                f"System skills reloaded successfully. Now tracking "
                f"{len(self.skill_hub.get_all_schemas())} dynamic skills "
                f"(loaded={stats['loaded']}, skipped={stats['skipped']})."
            )
        if tool_name == "system_save_preference":
            return self.memory.add_preference(**tool_args)
        if tool_name == "system_remove_preference":
            return self.memory.remove_preference(**tool_args)
        if tool_name == "system_create_automation":
            return self.automata.register_cron_task(**tool_args)
        if tool_name == "system_remove_automation":
            return self.automata.remove_task(**tool_args)
        if tool_name == "system_list_automations":
            jobs = self.automata.get_jobs()
            if not jobs:
                return "当前没有后台自动化任务。"
            return "\n".join(f"- {j['id']} | next: {j['next_run']}" for j in jobs)
        if tool_name == "system_spawn_swarm":
            # Swarm carries its own SSE notifications; the master suspends and waits
            result = await self.swarm.run_swarm(**tool_args)
            return f"Swarm Synthesis Complete:\n{result}"
        if tool_name == "system_workspace_search":
            return self.rag.search_context(**tool_args)
        if tool_name == "system_workspace_reindex":
            return self.rag.reindex_workspace(force=True)
        if tool_name == "system_secure_exec":
            physical = tool_args.pop("tool_name")
            vault_id = tool_args.pop("required_vault_id")
            intent_args = tool_args.pop("intent_args", {}) or {}
            callback = self._vault_tool_callbacks.get(physical)
            if callback is None:
                return f"❌ 金库未注册物理工具: {physical}"
            return await self.vault.execute_secure_tool(
                tool_name=physical,
                intent_args=intent_args,
                required_vault_id=vault_id,
                physical_tool_callback=callback,
            )
        if tool_name == "system_dispatch_omni_channel":
            if self.omni_gateway is None:
                return "❌ 分发网关未装配(宿主未注入 omni_gateway)"
            return await self.omni_gateway.execute_dispatch(
                targets=tool_args.get("targets") or [],
                title=tool_args.get("title") or "",
                content=tool_args.get("content") or "",
            )
        if self.tools.has(tool_name):
            return await self.tools.execute(tool_name, tool_args)
        return await self.skill_hub.execute(tool_name, tool_args)

    # ── 无缝组装 (ReAct 循环) ───────────────────────────────────────
    async def chat_stream(
        self,
        user_prompt: str,
        *,
        session_id: str | None = None,
        max_rounds: int | None = None,
        llm_kwargs: dict[str, Any] | None = None,
        long_task: Any | None = None,
    ) -> dict[str, Any]:
        """Master entry: assemble context -> model decides -> tools execute -> synthesize.

        ``llm_kwargs`` are merged into every LLM call (per-request provider /
        model / config override — e.g. the frontend's user-supplied API key).

        ``long_task``: optional long-task driver hook (duck typing, host-injected;
        default None = behavior identical to pre-integration). Provides
        ``pre_round()`` / ``post_round()``: every round reads projection + quota
        check before LLM, writes todo/evidence/quota after tool execution.
        """
        sid = session_id or str(uuid.uuid4())
        max_rounds = max_rounds or self.max_rounds

        # 连续对话: 复用 session 历史 (首条 system 常驻), 新会话则初始化
        messages = self._histories.get(sid)
        if messages is None:
            messages = [{"role": "system", "content": self.get_system_prompt()}]
            self._histories[sid] = messages
            if len(self._histories) > self._history_cap_sessions:
                del self._histories[next(iter(self._histories))]
        messages.append({"role": "user", "content": user_prompt})
        self.notify({"type": "master_start", "session_id": sid})

        round_count = 0
        total_cost = 0.0
        tool_trace: list[dict] = []

        while round_count < max_rounds:
            round_count += 1
            _log.info("[Master %s] routing round %d/%d", sid, round_count, max_rounds)
            self.notify({"type": "master_round", "session_id": sid, "round": round_count})

            # ── 长程任务: 每轮读投影 + 配额检查 (可选钩子, 默认 None 零影响) ──
            if long_task is not None:
                try:
                    _lt_ctx = await long_task.pre_round()
                    if not _lt_ctx.quota_ok:
                        self.notify({"type": "master_paused", "session_id": sid, "reason": "quota"})
                        return {
                            "status": "paused_by_quota",
                            "final_answer": (
                                f"[long task paused: quota exhausted "
                                f"(remaining ${_lt_ctx.remaining_usd})]"
                            ),
                            "rounds": round_count - 1,
                            "tool_calls": tool_trace,
                            "cost_usd": round(total_cost, 6),
                            "session_id": sid,
                        }
                    # 首轮注入 next_action 提示 (后续轮次在历史里, 避免污染)
                    if round_count == 1 and _lt_ctx.prompt_suffix:
                        messages[-1] = {
                            **messages[-1],
                            "content": messages[-1]["content"] + _lt_ctx.prompt_suffix,
                        }
                except Exception as _lt_exc:  # noqa: BLE001 — 钩子失败转明确错误, 不崩循环
                    _log.error("[Master %s] long_task hook error: %s", sid, _lt_exc)
                    self.notify(
                        {
                            "type": "master_error",
                            "session_id": sid,
                            "error": f"long task hook error: {_lt_exc}",
                            "round": round_count,
                        }
                    )
                    return {
                        "status": "failed",
                        "error": f"long task hook error: {_lt_exc}",
                        "rounds": round_count - 1,
                        "tool_calls": tool_trace,
                        "cost_usd": round(total_cost, 6),
                        "session_id": sid,
                    }

            # 1. Feed the LLM the JSON schemas it can understand
            call_kwargs = {
                "tools": self.get_all_tool_schemas(),
                "temperature": self.temperature,
                "max_tokens": 8192,
            }
            if llm_kwargs:
                call_kwargs.update(llm_kwargs)
            try:
                response = await self._llm_caller(messages, **call_kwargs)
            except Exception as exc:  # noqa: BLE001 — LLM 网络/鉴权失败: 明确返回而非循环
                _log.error("[Master %s] LLM call failed: %s", sid, exc)
                self.notify(
                    {
                        "type": "master_error",
                        "session_id": sid,
                        "error": str(exc),
                        "round": round_count,
                    }
                )
                return {
                    "status": "failed",
                    "error": f"LLM call failed: {exc}",
                    "rounds": round_count,
                    "tool_calls": tool_trace,
                    "cost_usd": round(total_cost, 6),
                    "session_id": sid,
                }
            total_cost += self._cost_of(response)

            choice = (response.get("choices") or [{}])[0]
            message = choice.get("message") or {}
            content = message.get("content") or ""
            tool_calls = message.get("tool_calls") or []
            messages.append({"role": "assistant", "content": content, "tool_calls": tool_calls})
            if len(messages) > self._history_max_msgs:
                messages[:] = [messages[0]] + messages[-(self._history_max_msgs - 1) :]

            # 2. Model answers directly (no tool calls) -> done
            if not tool_calls:
                # 收尾兜底: 模型在工具执行后返回 'None'/空 (opencode 免费池多轮
                # 疲劳) → 输出工具执行摘要而非静默空, 用户至少看到真实进度。
                if not content.strip() or content.strip().lower() in ("none", "null"):
                    if tool_trace:
                        done = [t.get("tool", "") for t in tool_trace]
                        content = (
                            "本轮已完成工具执行: " + ", ".join(done)
                            + "; 但收尾总结生成失败 (模型返回无效响应), "
                            "以上为实际执行结果。"
                        )
                    else:
                        content = "模型未能生成有效回答 (返回无效响应), 请重试或换种说法。"
                self.notify({"type": "master_done", "session_id": sid, "round": round_count})
                return {
                    "status": "success",
                    "final_answer": content,
                    "rounds": round_count,
                    "tool_calls": tool_trace,
                    "cost_usd": round(total_cost, 6),
                    "session_id": sid,
                }

            # 3. Intercept and execute the real physical functions, feed back
            for tool_call in tool_calls:
                fn = tool_call.get("function") or {}
                tool_name = fn.get("name", "")
                raw_args = fn.get("arguments") or "{}"
                if isinstance(raw_args, str):
                    try:
                        tool_args = json.loads(raw_args)
                    except json.JSONDecodeError:
                        tool_args = {}
                else:
                    tool_args = raw_args
                tc_id = tool_call.get("id", f"call_{tool_name}")
                self.notify(
                    {
                        "type": "tool_call",
                        "session_id": sid,
                        "tool_name": tool_name,
                        "tool_args": tool_args,
                        "round": round_count,
                    }
                )
                try:
                    result = await self.handle_tool_call(tool_name, tool_args)
                    tool_trace.append({"tool": tool_name, "status": "success"})
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc_id,
                            "content": f"[Tool {tool_name} SUCCESS]\nResult:\n{_truncate(result)}",
                        }
                    )
                except Exception as exc:  # noqa: BLE001 — tool failure feeds reflection, not the user
                    tool_trace.append({"tool": tool_name, "status": "failed", "error": str(exc)})
                    _log.warning("[Master %s] tool %s failed: %s", sid, tool_name, exc)
                    self.notify(
                        {
                            "type": "tool_error",
                            "session_id": sid,
                            "tool_name": tool_name,
                            "error": str(exc),
                            "round": round_count,
                        }
                    )
                    # 4. Failure fed back -> model reflects and retries differently
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc_id,
                            "content": (
                                f"[Tool {tool_name} FAILED]\nError:\n{exc!s}\n\n"
                                f"请仔细分析上述报错, 思考哪里出了问题, 并尝试另一种方法。"
                            ),
                        }
                    )

            # ── 长程任务: 每轮工具执行后写 todo/evidence/配额 (可选钩子) ──
            if long_task is not None:
                try:
                    await long_task.post_round({"cost_usd": total_cost})
                except Exception as _lt_exc:  # noqa: BLE001 — 钩子失败转明确错误
                    _log.error("[Master %s] long_task post_round error: %s", sid, _lt_exc)
                    return {
                        "status": "failed",
                        "error": f"long task hook error: {_lt_exc}",
                        "rounds": round_count,
                        "tool_calls": tool_trace,
                        "cost_usd": round(total_cost, 6),
                        "session_id": sid,
                    }

        # 轮次护栏耗尽 (防物理死循环, 不限制智能): 返回模型最后产出, 不报 HITL。
        # 大模型全程自由调用工具/直答; 极端情况下预算用尽也把已有内容交还用户。
        last_content = ""
        for m in reversed(messages):
            if m.get("role") == "assistant" and m.get("content"):
                last_content = m["content"]
                break
        self.notify({"type": "master_rounds_exhausted", "session_id": sid, "rounds": round_count})
        if last_content:
            return {
                "status": "success",
                "final_answer": last_content,
                "rounds": round_count,
                "tool_calls": tool_trace,
                "cost_usd": round(total_cost, 6),
                "session_id": sid,
            }
        # 轮次用尽但执行过工具: 输出工具执行摘要 (不静默 failed) — 用户
        # 至少看到真实进度, 而非空白失败。与收尾轮 'None' 兜底同一语义。
        if tool_trace:
            done = [t.get("tool", "") for t in tool_trace]
            return {
                "status": "success",
                "final_answer": (
                    "轮次预算已用尽, 已执行 " + str(len(done)) + " 项工具: "
                    + ", ".join(done) + "; 任务仍在进行中 (模型未在轮次内收尾)。"
                ),
                "rounds": round_count,
                "tool_calls": tool_trace,
                "cost_usd": round(total_cost, 6),
                "session_id": sid,
            }
        return {
            "status": "failed",
            "error": "模型未在预算内产出回答 (LLM 未返回内容)。",
            "rounds": round_count,
            "tool_calls": tool_trace,
            "cost_usd": round(total_cost, 6),
            "session_id": sid,
            "last_messages": messages[-3:],
        }

    async def chat(self, user_prompt: str, **kwargs: Any) -> dict[str, Any]:
        """Lightweight single-turn chat (no tools)."""
        call_kwargs = {"temperature": self.temperature}
        llm_kwargs = kwargs.pop("llm_kwargs", None)
        if llm_kwargs:
            call_kwargs.update(llm_kwargs)
        # 轻量单轮同样注入主脑 system prompt — 否则模型自报本体人格
        # (如 "我是 DeepSeek"), veya 身份/能力上下文完全丢失。
        messages = [{"role": "system", "content": self.get_system_prompt()},
                    {"role": "user", "content": user_prompt}]
        response = await self._llm_caller(messages, **call_kwargs)
        content = ((response.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        return {"status": "success", "final_answer": content, "tool_calls": []}

    def _cost_of(self, response: dict) -> float:
        if self._cost_calculator is not None:
            try:
                return self._cost_calculator(response)
            except Exception:  # noqa: BLE001 — cost failure must not break the loop
                return 0.0
        return 0.0
