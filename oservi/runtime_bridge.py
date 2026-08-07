"""oservi.runtime_bridge — 三框架运行时统一桥 (3O 元素: 外部 agent 运行时适配)。

单一来源 (§1.4): 多框架运行时 (prime-agent / pi / agentscope) 的协议定义与
适配器实现是本元素; veya 主仓只做装配 (server/runtimes shim + Infra.init 注册)。

AgentRuntime 协议: init / dispatch / invoke / lifecycle / health。
注册: obase.agent_registry 的 runtime 类型 (由装配层注册)。

依赖: 仅 obase (event_bus 翻译) + stdlib —— 零 veya 反向依赖。
"""

from __future__ import annotations

import asyncio
import importlib
import os
import shutil
import time
from typing import Any, Protocol, runtime_checkable

from obase.agent_registry import AgentRegistry, RegistryConflictError

__all__ = [
    "ALL_RUNTIMES",
    "AgentRuntime",
    "AgentScopeBridgeRuntime",
    "PiBridgeRuntime",
    "PrimeAgentRuntime",
    "agentscope_bridge",
    "pi_bridge",
    "prime_agent_runtime",
    "register_runtime",
    "unavailable",
]


# =========================================================================
# 协议
# =========================================================================

@runtime_checkable
class AgentRuntime(Protocol):
    """统一运行时协议 — 上层 (编排/CLI/MCP) 零感知差异。

    治理 (权限/审计/脱敏) 由 veya hooks 统一包裹, 适配器内部不重复实现。
    """

    name: str
    kind: str = "runtime"

    async def init(self, config: dict | None = None) -> dict: ...
    async def dispatch(self, task: str, **kwargs: Any) -> dict: ...
    async def invoke(self, prompt: str, **kwargs: Any) -> dict: ...
    async def lifecycle(self, action: str) -> dict: ...
    async def health(self) -> dict: ...


def register_runtime(adapter: AgentRuntime,
                     registry: AgentRegistry | None = None) -> dict[str, Any]:
    """注册适配器到 agent_registry (runtime 类型, 幂等)。"""
    reg = registry or AgentRegistry()
    try:
        reg.register("runtime", adapter.name, adapter,
                     desc=getattr(adapter, "description", adapter.__class__.__doc__ or ""))
        return {"registered": adapter.name}
    except RegistryConflictError:
        return {"skipped": adapter.name}


def unavailable(adapter_name: str, reason: str) -> dict[str, Any]:
    """依赖缺失时的统一结构化返回 (不崩溃)。"""
    return {"ok": False, "runtime": adapter_name, "error": reason}


# =========================================================================
# L1 — prime-agent (RLM 内核, 可插拔)
# =========================================================================

_PRIME_HINT = (
    "prime-agent 未接入: 设置 PRIME_AGENT_MODULE=<python模块路径> "
    "(RLM harness 的 AgentRuntime 兼容实现), 或 pip 安装后重试"
)


def _resolve_prime_harness():
    mod_name = os.environ.get("PRIME_AGENT_MODULE", "")
    if mod_name:
        try:
            return importlib.import_module(mod_name)
        except ImportError:
            return None
    try:
        return importlib.import_module("prime_agent")
    except ImportError:
        return None


class PrimeAgentRuntime:
    """prime-agent 内核运行时适配器 (协议骨架, 可插拔)。"""

    name = "prime_agent_runtime"
    description = "prime-agent RLM 内核: 代码即交互/自我改写 (Continual Harness)"

    def __init__(self) -> None:
        self._harness = None
        self._started_at = 0.0

    async def init(self, config: dict | None = None) -> dict[str, Any]:
        self._harness = _resolve_prime_harness()
        if self._harness is None:
            return unavailable(self.name, _PRIME_HINT)
        self._started_at = time.time()
        return {"ok": True, "runtime": self.name, "version": "harness",
                "module": getattr(self._harness, "__name__", "?")}

    async def dispatch(self, task: str, **kwargs: Any) -> dict[str, Any]:
        if self._harness is None:
            return unavailable(self.name, _PRIME_HINT)
        try:
            run = getattr(self._harness, "run", None)
            result = run(task, **kwargs) if callable(run) else None
            if result is None:
                return unavailable(self.name, "harness 无 run() 入口")
            return {"ok": True, "runtime": self.name, "output": str(result)[:4000]}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "runtime": self.name, "error": str(e)[:2000]}

    async def invoke(self, prompt: str, **kwargs: Any) -> dict[str, Any]:
        return await self.dispatch(prompt, **kwargs)

    async def lifecycle(self, action: str) -> dict[str, Any]:
        if action in ("health", "status"):
            return await self.health()
        return {"ok": True, "runtime": self.name, "action": action,
                "note": "prime-agent v1 无状态 (进程内按需加载)"}

    async def health(self) -> dict[str, Any]:
        connected = _resolve_prime_harness() is not None
        return {"ok": connected, "runtime": self.name, "connected": connected,
                "uptime_s": time.time() - self._started_at if self._started_at else 0}


# =========================================================================
# L2 — pi (CLI 桥, TS/Bun → subprocess)
# =========================================================================

class PiBridgeRuntime:
    """pi (pi-coding-agent) CLI 桥: 极简/类型安全工具链, 统一多厂商 API。"""

    name = "pi_bridge"
    description = "pi (pi-coding-agent) CLI 桥: subprocess 执行, 无 shell 注入面"

    def __init__(self) -> None:
        self._bin: str | None = None
        self._version = ""

    def _find_bin(self) -> str | None:
        return shutil.which("pi")

    async def _run(self, args: list[str], timeout_s: float = 600.0) -> dict[str, Any]:
        assert self._bin is not None
        proc = await asyncio.create_subprocess_exec(
            self._bin, *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            return {"ok": False, "runtime": self.name, "error": f"pi 超时 ({timeout_s:.0f}s)"}
        out = stdout.decode(errors="replace")
        err = stderr.decode(errors="replace")
        if proc.returncode != 0:
            return {"ok": False, "runtime": self.name,
                    "error": err[-2000:] or f"exit={proc.returncode}", "output": out[-2000:]}
        return {"ok": True, "runtime": self.name, "output": out[-4000:]}

    async def init(self, config: dict | None = None) -> dict[str, Any]:
        self._bin = self._find_bin()
        if self._bin is None:
            return unavailable(self.name, "pi CLI 未安装 (npm i -g @pi-coding/pi 或官方安装脚本)")
        r = await self._run(["--version"], timeout_s=15)
        self._version = r.get("output", "").strip() or "unknown"
        return {"ok": True, "runtime": self.name, "version": self._version,
                "bin": self._bin}

    async def dispatch(self, task: str, **kwargs: Any) -> dict[str, Any]:
        if self._bin is None:
            return unavailable(self.name, "pi CLI 未初始化 (先 init)")
        args = ["-p", task]
        model = kwargs.get("model") or kwargs.get("model_name")
        if model:
            args += ["--model", str(model)]
        return await self._run(args, timeout_s=kwargs.get("timeout_s", 600.0))

    async def invoke(self, prompt: str, **kwargs: Any) -> dict[str, Any]:
        return await self.dispatch(prompt, **kwargs)

    async def lifecycle(self, action: str) -> dict[str, Any]:
        if action in ("health", "status"):
            return await self.health()
        return {"ok": True, "runtime": self.name, "action": action,
                "note": "pi bridge v1 按需子进程 (无常驻 daemon)"}

    async def health(self) -> dict[str, Any]:
        ok = self._find_bin() is not None
        return {"ok": ok, "runtime": self.name,
                "bin": self._find_bin(), "version": self._version or None}


# =========================================================================
# L3 — agentscope (平台编排桥, 事件翻译)
# =========================================================================

_ASCOPE_HINT = (
    "agentscope 未安装: pip install agentscope (2.x) "
    "—— Event Bus / 中间件 / MCP / Skill Hub 桥接需该包"
)

# agentscope 事件 → veya event_bus 主题 (PRD §5 事件映射表)
_EVENT_MAP: dict[str, str] = {
    "start": "agent.start",
    "message": "agent.message",
    "end": "agent.end",
    "error": "agent.error",
}


class AgentScopeBridgeRuntime:
    """agentscope 平台编排桥 (双向翻译, 可插拔)。"""

    name = "agentscope_bridge"
    description = "agentscope 平台桥: Event Bus 翻译 + 中间件↔hooks + MCP/Skill Hub"

    def __init__(self) -> None:
        self._ascope: Any = None
        self._started_at = 0.0

    def _load(self) -> Any | None:
        try:
            return importlib.import_module("agentscope")
        except ImportError:
            return None

    def _translate_event(self, ascope_event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        ev_type = (ascope_event.get("type") or "").lower()
        topic = _EVENT_MAP.get(ev_type, f"agent.{ev_type or 'unknown'}")
        return topic, {"source": "agentscope", **ascope_event}

    async def publish_to_veya(self, ascope_event: dict[str, Any]) -> dict[str, Any]:
        """翻译桥: agentscope 事件 → obase.event_bus。"""
        try:
            from obase.event_bus import event_bus

            topic, payload = self._translate_event(ascope_event)
            event_bus.publish(topic, payload)
            return {"ok": True, "topic": topic}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "error": f"event_bus 发布失败: {e}"}

    async def init(self, config: dict | None = None) -> dict[str, Any]:
        self._ascope = self._load()
        if self._ascope is None:
            return unavailable(self.name, _ASCOPE_HINT)
        self._started_at = time.time()
        version = getattr(self._ascope, "__version__", "unknown")
        return {"ok": True, "runtime": self.name, "version": version}

    async def dispatch(self, task: str, **kwargs: Any) -> dict[str, Any]:
        if self._ascope is None:
            return unavailable(self.name, _ASCOPE_HINT)
        try:
            agent_cls = getattr(self._ascope, "Agent", None)
            if agent_cls is None:
                return unavailable(self.name, "agentscope 无 Agent 入口 (版本差异)")
            agent = agent_cls(name=f"veya-{int(time.time())}")
            reply = agent(task)
            return {"ok": True, "runtime": self.name,
                    "output": str(getattr(reply, "content", reply))[:4000]}
        except Exception as e:  # noqa: BLE001
            return {"ok": False, "runtime": self.name, "error": str(e)[:2000]}

    async def invoke(self, prompt: str, **kwargs: Any) -> dict[str, Any]:
        return await self.dispatch(prompt, **kwargs)

    async def lifecycle(self, action: str) -> dict[str, Any]:
        if action in ("health", "status"):
            return await self.health()
        return {"ok": True, "runtime": self.name, "action": action,
                "note": "agentscope bridge v1 按需实例 (无平台常驻)"}

    async def health(self) -> dict[str, Any]:
        connected = self._load() is not None
        return {"ok": connected, "runtime": self.name, "connected": connected,
                "event_map": _EVENT_MAP}


# =========================================================================
# 装配导出
# =========================================================================

ALL_RUNTIMES: list[AgentRuntime] = [
    PrimeAgentRuntime(),      # L1 内核
    PiBridgeRuntime(),        # L2 工具链
    AgentScopeBridgeRuntime(),  # L3 平台
]

prime_agent_runtime = ALL_RUNTIMES[0]
pi_bridge = ALL_RUNTIMES[1]
agentscope_bridge = ALL_RUNTIMES[2]


def register_all_runtimes(registry: AgentRegistry | None = None) -> dict[str, Any]:
    """注册全部适配器到 agent_registry (runtime 类型, 幂等)。"""
    registered: list[str] = []
    skipped: list[str] = []
    for rt in ALL_RUNTIMES:
        out = register_runtime(rt, registry)
        (registered if "registered" in out else skipped).append(rt.name)
    return {"registered": registered, "skipped": skipped}
