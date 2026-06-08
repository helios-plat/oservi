"""Action Planner Engine Skeleton.

收敛自双实证:
- Aegis C2 ActionPlanner: 症状输入 → runbook 匹配 → 行动计划生成 → 步骤分发
- Tide v5 RemediationPlanner: 指标超阈 → playbook 匹配 → 分阶段行动

共性 (固化为机制):
- 持续运行循环 (由 trigger 控制; 计划请求由外部 submit 触发)
- RAG 上下文检索 (可选, rag 注入)
- LLM 行动计划生成: 输入 (symptom, context) → 行动步骤列表
- plugin_registry 路由: 按步骤 plugin_id 查注册表并 dispatch
- 最大重试次数 (config.max_retries)
- 执行结果汇报 on_plan_done

差异 (留 inject + config):
- llm_provider: 计划生成 LLM → 注入 obase
- plugin_registry: 行动插件注册表 → 注入 layer4
- rag: RAG 检索 → 注入 oskill (可选)

红线对照:
- 红线 1 (≥2 实证): Aegis C2 ActionPlanner + Tide v5 RemediationPlanner ✅
- 红线 2 (机制/业务分离): 计划业务靠 LLM 注入, 插件业务靠 registry 注入
- 红线 3 (注入契约): llm_provider=obase(1) / plugin_registry=layer4(1) / rag=oskill(0..1)
- 红线 4 (无状态骨架): 状态只在 ActionPlannerEngine 实例
- 红线 5 (不反向依赖): 本文件不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class ActionPlannerEngine(EngineSkeleton):
    """行动计划引擎骨架.

    机制 (骨架固化):
    - 接收 symptom 请求 (via submit_request)
    - 可选 RAG 检索 (rag 注入)
    - LLM 生成行动步骤列表 [{plugin_id, params, description}]
    - 按 plugin_registry 路由执行每个步骤
    - 失败步骤按 max_retries 重试
    - 结果通过 on_plan_done 回调

    业务 (注入填料):
    - llm_provider: 计划生成 LLM, 接收 (symptom, context) → [{plugin_id, params, description}]
    - plugin_registry: layer4 callable, 接收 (plugin_id) → Callable | None
    - rag: oskill callable, 接收 (query) → context str

    Example::

        manifest = ServiceManifest(
            name="aegis-c2-action-planner",
            skeleton="action_planner",
            inject={
                "llm_provider": obase_planner_llm,
                "plugin_registry": get_plugin,
                "rag": retrieve_runbook,
            },
            trigger={"on_interval": 0},
            config={"max_retries": 2, "step_timeout_seconds": 30},
        )
    """

    injection_points = {
        "llm_provider": Injection(
            kind="layer4",
            cardinality="1",
            description="计划生成 LLM: (symptom, context) → [{plugin_id, params, description}]",
        ),
        "plugin_registry": Injection(
            kind="layer4",
            cardinality="1",
            description="插件注册表: (plugin_id: str) → Callable | None",
        ),
        "rag": Injection(
            kind="layer4",
            cardinality="0..1",
            description="RAG 检索: (query: str) → context str (可选)",
        ),
    }

    def __init__(
        self,
        *,
        llm_provider: Callable[..., Any],
        plugin_registry: Callable[..., Any],
        rag: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_plan_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.name = name
        self.llm_provider = llm_provider
        self.plugin_registry = plugin_registry
        self.rag = rag
        self.trigger = trigger
        self.config = config
        self.on_plan_done = on_plan_done

        # 运行期状态 (红线 4)
        self._running = False
        self._request_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._plans_executed = 0
        self._last_error: str | None = None

    def submit_request(self, request: dict[str, Any]) -> None:
        """提交一个计划请求 (外部调用)."""
        self._request_queue.put_nowait(request)

    async def invoke(self, request: dict[str, Any]) -> dict[str, Any]:
        """单次同步调用（不走队列，直接执行）."""
        return await self._execute_plan(request)

    def run(self) -> None:
        """启动持续运行循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"ActionPlannerEngine {self.name} already running")
        self._running = True
        logger.info(f"ActionPlannerEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"ActionPlannerEngine '{self.name}' stopped")

    def stop(self) -> None:
        self._running = False

    async def _run_loop(self) -> None:
        while self._running:
            try:
                request = await asyncio.wait_for(self._request_queue.get(), timeout=1.0)
                result = await self._execute_plan(request)
                self._plans_executed += 1
                if self.on_plan_done:
                    try:
                        self.on_plan_done(result)
                    except Exception as e:
                        logger.warning(f"on_plan_done callback failed: {e}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"ActionPlannerEngine '{self.name}' plan failed")

    async def _execute_plan(self, request: dict[str, Any]) -> dict[str, Any]:
        """执行单次行动计划: RAG → LLM 生成 → 步骤执行."""
        symptom = request.get("symptom", "")
        context = ""

        # RAG 上下文检索
        if self.rag:
            try:
                raw = self.rag(query=symptom)
                if asyncio.iscoroutine(raw):
                    raw = await raw
                context = str(raw) if raw else ""
            except Exception as e:
                logger.warning(f"RAG retrieval failed: {e}")

        # LLM 生成行动步骤
        steps = await self._generate_plan(symptom, context)

        # 执行步骤
        step_results = []
        for step in steps:
            step_result = await self._execute_step(step)
            step_results.append(step_result)
            if step_result.get("status") == "failed" and self.config.get("abort_on_failure"):
                break

        success_count = sum(1 for s in step_results if s.get("status") == "ok")
        return {
            "status": "completed" if success_count == len(step_results) else "partial",
            "symptom": symptom,
            "steps_total": len(step_results),
            "steps_ok": success_count,
            "step_results": step_results,
            "request": request,
        }

    async def _generate_plan(self, symptom: str, context: str) -> list[dict[str, Any]]:
        """调 llm_provider 生成行动步骤."""
        try:
            result = self.llm_provider(symptom=symptom, context=context)
            if asyncio.iscoroutine(result):
                result = await result
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.warning(f"ActionPlannerEngine LLM plan generation failed: {e}")
            return []

    async def _execute_step(self, step: dict[str, Any]) -> dict[str, Any]:
        """通过 plugin_registry 执行单个步骤, 带重试."""
        plugin_id = step.get("plugin_id", "")
        params = step.get("params", {})
        max_retries = self.config.get("max_retries", 1)
        timeout = self.config.get("step_timeout_seconds", 30)

        plugin_fn = self.plugin_registry(plugin_id)
        if plugin_fn is None:
            return {
                "plugin_id": plugin_id,
                "status": "failed",
                "error": f"plugin '{plugin_id}' not found in registry",
            }

        for attempt in range(max_retries + 1):
            try:
                result = await asyncio.wait_for(
                    self._call_plugin(plugin_fn, params),
                    timeout=timeout,
                )
                return {
                    "plugin_id": plugin_id,
                    "status": "ok",
                    "result": result,
                    "attempt": attempt,
                }
            except asyncio.TimeoutError:
                error = f"timeout after {timeout}s"
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
            logger.warning(
                f"Step '{plugin_id}' attempt {attempt + 1}/{max_retries + 1} failed: {error}"
            )

        return {"plugin_id": plugin_id, "status": "failed", "error": error}

    async def _call_plugin(self, fn: Callable[..., Any], params: dict[str, Any]) -> Any:
        result = fn(**params)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "queue_size": self._request_queue.qsize(),
                "plans_executed": self._plans_executed,
                "has_rag": self.rag is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("action_planner", ActionPlannerEngine)
