"""Agentic Loop Engine Skeleton.

收敛自双实证:
- Aegis C2 AgenticLoop: LLM thought → tool select → tool call → observe → repeat
- Helios-Refactorer AutoAgent: plan → file read → edit → verify loop

共性 (固化为机制):
- 持续运行循环 (由 trigger 控制调度, 单次 task 由外部 submit 触发)
- LLM thought-action-observation 循环 (ReAct 模式)
- 工具调度: tools list 中按 LLM 选择的 tool_name 路由
- max_steps 上限防止无限循环
- knowledge_retrieval 可选 (RAG 上下文注入)
- 任务结果通过 on_task_done 回调输出

差异 (留 inject + config):
- llm_provider: ReAct 推理引擎 → 注入 obase
- tools: 可用工具集 → 注入 oprim 列表
- knowledge_retrieval: RAG 检索 → 注入 oskill (可选)

红线对照:
- 红线 1 (≥2 实证): Aegis C2 AgenticLoop + Helios-Refactorer AutoAgent ✅
- 红线 2 (机制/业务分离): LLM/工具业务靠注入, 骨架只做循环控制和路由
- 红线 3 (注入契约): llm_provider=obase(1) / tools=oprim(1..n) / knowledge_retrieval=oskill(0..1)
- 红线 4 (无状态骨架): 状态只在 AgenticLoopEngine 实例
- 红线 5 (不反向依赖): 本文件不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class AgenticLoopEngine(EngineSkeleton):
    """自主代理循环引擎骨架.

    机制 (骨架固化):
    - ReAct (Reason + Act) 循环: thought → action → observation → repeat
    - 工具路由: 按 LLM 输出的 tool_name 在 tools 列表中查找并调用
    - max_steps 上限 (config.max_steps, 默认 20)
    - 可选 RAG 上下文注入 (knowledge_retrieval)
    - 任务完成 / 超限 → on_task_done 回调

    业务 (注入填料):
    - llm_provider: ReAct 推理, 接收 (task, context, history) → 返回 {thought, action, tool_name, tool_args, final_answer}
    - tools: oprim 工具列表, 各工具 callable 接收 **kwargs → 返回 Any
    - knowledge_retrieval: oskill callable, 接收 (query) → 返回 context str

    Example::

        manifest = ServiceManifest(
            name="aegis-c2-agentic-loop",
            skeleton="agentic_loop",
            inject={
                "llm_provider": obase_react_llm,
                "tools": [docker_container_inspect, postgres_pool_status],
                "knowledge_retrieval": retrieve_runbook,
            },
            trigger={"on_interval": 0},
            config={"max_steps": 15, "timeout_seconds": 120},
        )
    """

    injection_points = {
        "llm_provider": Injection(
            kind="layer4",
            cardinality="1",
            description="ReAct LLM: (task, context, history) → {thought, action, tool_name, tool_args, final_answer}",
        ),
        "tools": Injection(
            kind="oprim",
            cardinality="1..n",
            description="可用工具集: oprim callable 列表, 各工具须有 __name__ 属性",
        ),
        "knowledge_retrieval": Injection(
            kind="layer4",
            cardinality="0..1",
            description="RAG 检索: (query: str) → context str (可选)",
        ),
    }

    def __init__(
        self,
        *,
        llm_provider: Callable[..., Any],
        tools: list[Callable[..., Any]],
        knowledge_retrieval: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_task_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.name = name
        self.llm_provider = llm_provider
        self.tools = {getattr(t, "__name__", str(i)): t for i, t in enumerate(tools)}
        self.knowledge_retrieval = knowledge_retrieval
        self.trigger = trigger
        self.config = config
        self.on_task_done = on_task_done

        # 运行期状态 (红线 4)
        self._running = False
        self._task_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._completed_count = 0
        self._last_error: str | None = None

    def submit_task(self, task: dict[str, Any]) -> None:
        """提交一个 agentic task (外部调用)."""
        self._task_queue.put_nowait(task)

    async def invoke(self, task: dict[str, Any]) -> dict[str, Any]:
        """单次同步调用（不走队列，直接执行，用于 request/response 场景）."""
        return await self._execute_task(task)

    def run(self) -> None:
        """启动持续运行循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"AgenticLoopEngine {self.name} already running")
        self._running = True
        logger.info(f"AgenticLoopEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"AgenticLoopEngine '{self.name}' stopped")

    def stop(self) -> None:
        self._running = False

    async def _run_loop(self) -> None:
        while self._running:
            try:
                task = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)
                result = await self._execute_task(task)
                self._completed_count += 1
                if self.on_task_done:
                    try:
                        self.on_task_done(result)
                    except Exception as e:
                        logger.warning(f"on_task_done callback failed: {e}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"AgenticLoopEngine '{self.name}' task failed")

    async def _execute_task(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行单次 agentic task (ReAct 循环)."""
        max_steps = self.config.get("max_steps", 20)
        history: list[dict[str, Any]] = []
        context = ""

        # RAG 上下文注入 (可选)
        if self.knowledge_retrieval:
            try:
                raw = self.knowledge_retrieval(query=task.get("goal", ""))
                if asyncio.iscoroutine(raw):
                    raw = await raw
                context = str(raw) if raw else ""
            except Exception as e:
                logger.warning(f"knowledge_retrieval failed: {e}")

        for step in range(max_steps):
            # LLM 推理
            llm_out = await self._call_llm(task, context, history)

            if llm_out.get("final_answer"):
                return {
                    "status": "completed",
                    "steps": step + 1,
                    "final_answer": llm_out["final_answer"],
                    "history": history,
                    "task": task,
                }

            # 工具调用
            tool_name = llm_out.get("tool_name", "")
            tool_args = llm_out.get("tool_args", {})
            observation = await self._call_tool(tool_name, tool_args)

            history.append(
                {
                    "step": step,
                    "thought": llm_out.get("thought", ""),
                    "action": llm_out.get("action", ""),
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                    "observation": observation,
                }
            )

        return {
            "status": "max_steps_reached",
            "steps": max_steps,
            "final_answer": None,
            "history": history,
            "task": task,
        }

    async def _call_llm(
        self,
        task: dict[str, Any],
        context: str,
        history: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """调 llm_provider 推理."""
        try:
            result = self.llm_provider(task=task, context=context, history=history)
            if asyncio.iscoroutine(result):
                result = await result
            return result if isinstance(result, dict) else {}
        except Exception as e:
            logger.warning(f"AgenticLoopEngine LLM call failed: {e}")
            return {"final_answer": f"LLM error: {e}"}

    async def _call_tool(self, tool_name: str, tool_args: dict[str, Any]) -> Any:
        """调用指定工具."""
        tool = self.tools.get(tool_name)
        if not tool:
            return f"ERROR: tool '{tool_name}' not found. Available: {list(self.tools.keys())}"
        try:
            result = tool(**tool_args)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as e:
            return f"ERROR: tool '{tool_name}' raised {type(e).__name__}: {e}"

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "queue_size": self._task_queue.qsize(),
                "completed_count": self._completed_count,
                "tools_available": list(self.tools.keys()),
                "has_knowledge_retrieval": self.knowledge_retrieval is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("agentic_loop", AgenticLoopEngine)
