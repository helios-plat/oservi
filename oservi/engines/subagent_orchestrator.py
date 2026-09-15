"""Subagent Orchestrator Engine Skeleton.

机制 (固化):
- on_demand 触发
- 调 subagent_runner omodul 执行子任务
- 可选 scheduler layer4 控制任务调度
- cost 通过返回值传播

业务 (注入):
- subagent_runner: omodul (1) — 执行子 agent 任务
- llm_caller: oprim (1) — LLM 调用
- scheduler: layer4 (0..1) — 可选任务调度器

红线对照:
- 红线 2 (机制/业务分离): 子任务业务全靠注入
- 红线 3 (注入契约): 3 注入点声明
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, ClassVar, Protocol, runtime_checkable

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class WorkspaceFleetPort(Protocol):
    """Injected workspace allocator; this engine never imports its provider."""

    async def allocate(self, spec: Any) -> Any: ...

    async def bind(self, workspace: Any, session_ref: Any, **kwargs: Any) -> Any: ...


@runtime_checkable
class PersistentAgentSessionPort(Protocol):
    """Injected session lifecycle facade for workspace composition."""

    async def create(self, spec: Any) -> Any: ...

    async def start(self) -> Any: ...


class SubagentOrchestratorEngine(EngineSkeleton):
    """子代理编排引擎骨架 (on_demand).

    Example::

        engine = SubagentOrchestratorEngine(
            subagent_runner=omodul_run_subagent,
            llm_caller=oprim_llm,
            scheduler=None,
            trigger={"on_demand": True},
            config={"max_parallel": 4},
            name="orchestrator",
        )
        result = asyncio.run(engine.orchestrate(tasks=[...]))
    """

    injection_points: ClassVar[dict[str, Injection]] = {
        "subagent_runner": Injection(
            kind="omodul",
            cardinality="1",
            description="run_subagent_task omodul",
        ),
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM call primitive",
        ),
        "scheduler": Injection(
            kind="layer4",
            cardinality="0..1",
            description="Optional task scheduler",
        ),
        "workspace_fleet": Injection(
            kind="layer4",
            cardinality="0..1",
            description="Optional isolated workspace allocator",
        ),
        "session_factory": Injection(
            kind="layer4",
            cardinality="0..1",
            description="Optional persistent agent session factory",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        subagent_runner: Callable[..., Any],
        llm_caller: Callable[..., Any],
        scheduler: Callable[..., Any] | None = None,
        workspace_fleet: WorkspaceFleetPort | None = None,
        session_factory: Any | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.subagent_runner = subagent_runner
        self.llm_caller = llm_caller
        self.scheduler = scheduler
        self.workspace_fleet = workspace_fleet
        self.session_factory = session_factory
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._task_count = 0
        self._total_cost_usd = 0.0
        self._last_error: str | None = None

    # ===== 生命周期 =====

    def run(self) -> None:
        """on_demand 引擎: run() 标记就绪, 不阻塞."""
        self._running = True
        logger.info(f"SubagentOrchestratorEngine '{self.name}' ready")

    def stop(self) -> None:
        self._running = False

    # ===== 核心 API =====

    async def orchestrate(
        self,
        tasks: list[dict[str, Any]],
        *,
        parallel: bool = False,
    ) -> dict[str, Any]:
        """Orchestrate a list of subagent tasks.

        Args:
            tasks: List of task dicts passed to subagent_runner.
            parallel: If True, run tasks concurrently (up to config.max_parallel).

        Returns:
            {"status": "completed", "results": [...], "total_cost_usd": float}
        """
        return await self._orchestrate_impl(tasks, parallel=parallel)

    async def orchestrate_workspace_candidates(
        self,
        tasks: Sequence[Mapping[str, Any]],
        *,
        workspace_specs: Sequence[Any] | Mapping[str, Any] | None = None,
        parallel: bool = True,
        start_sessions: bool = True,
    ) -> dict[str, Any]:
        """Fan out tasks into isolated workspaces and aggregate raw candidates.

        Allocation and session lifecycle are injected ports.  The method only
        attaches identifiers to task inputs and returns one result per
        candidate; it does not create a GoalRun, choose a winner, or mark a
        result accepted.
        """
        if self.workspace_fleet is None:
            raise RuntimeError("workspace_fleet injection is required")
        enriched: list[dict[str, Any]] = []
        bindings: list[dict[str, Any]] = []
        for index, original in enumerate(tasks):
            task = dict(original)
            task_id = str(task.get("id", ""))
            spec = self._workspace_spec_for(
                task_id,
                index,
                task,
                workspace_specs,
            )
            workspace = await self.workspace_fleet.allocate(spec)
            workspace_id = self._identifier(workspace, "workspace_id")
            session_ref = None
            if self.session_factory is not None:
                session_ref = await self._start_injected_session(
                    workspace_id,
                    task,
                    spec,
                    start=start_sessions,
                )
                if session_ref is not None:
                    scope = (
                        str(spec.get("scope", "default"))
                        if isinstance(spec, Mapping)
                        else str(task.get("scope", "default"))
                    )
                    await self.workspace_fleet.bind(workspace, session_ref, scope=scope)
            task["workspace_id"] = workspace_id
            task["workspace_ref"] = workspace_id
            if session_ref is not None:
                task["session_ref"] = session_ref
            enriched.append(task)
            bindings.append(
                {
                    "task_id": task_id,
                    "workspace_id": workspace_id,
                    "session_ref": session_ref,
                }
            )
        result = await self.orchestrate(enriched, parallel=parallel)
        by_task = {
            str(item.get("task_id", "")): item
            for item in result.get("results", [])
            if isinstance(item, Mapping)
        }
        candidates = [
            {
                **binding,
                "result": by_task.get(binding["task_id"]),
            }
            for binding in bindings
        ]
        return {
            **result,
            "candidates": candidates,
            "candidate_aggregation": "raw_results",
            "winner": None,
            "acceptance_authority": 0,
        }

    async def fan_out_workspaces(
        self,
        tasks: Sequence[Mapping[str, Any]],
        *,
        workspace_specs: Sequence[Any] | Mapping[str, Any] | None = None,
        parallel: bool = True,
    ) -> dict[str, Any]:
        """Readable alias for the workspace candidate composition."""
        return await self.orchestrate_workspace_candidates(
            tasks, workspace_specs=workspace_specs, parallel=parallel
        )

    @staticmethod
    def aggregate_candidates(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """Return stable raw candidate ordering; selection remains external."""
        ordered = sorted(
            (dict(item) for item in candidates),
            key=lambda item: (str(item.get("task_id", "")), str(item.get("workspace_id", ""))),
        )
        return {
            "candidates": ordered,
            "selection_required": bool(ordered),
            "winner": None,
            "acceptance_authority": 0,
        }

    @staticmethod
    def _workspace_spec_for(
        task_id: str,
        index: int,
        task: Mapping[str, Any],
        workspace_specs: Sequence[Any] | Mapping[str, Any] | None,
    ) -> Any:
        if isinstance(workspace_specs, Mapping):
            selected = workspace_specs.get(task_id)
            if selected is not None:
                return selected
        elif workspace_specs is not None and index < len(workspace_specs):
            return workspace_specs[index]
        selected = task.get("workspace_spec")
        if selected is not None:
            return selected
        return {
            "source_ref": str(task.get("source_ref", task.get("repo", task_id))),
            "workspace_type": str(task.get("workspace_type", "ephemeral")),
            "owner_id": str(task.get("owner_id", task_id)),
            "scope": str(task.get("scope", "default")),
        }

    async def _start_injected_session(
        self,
        workspace_id: str,
        task: Mapping[str, Any],
        workspace_spec: Any,
        *,
        start: bool,
    ) -> str | None:
        factory = self.session_factory
        session = factory
        session_spec = {
            "workspace_ref": workspace_id,
            "agent_type": str(task.get("agent_type", "terminal")),
            "scope": str(
                workspace_spec.get("scope", "default")
                if isinstance(workspace_spec, Mapping)
                else task.get("scope", "default")
            ),
        }
        if callable(factory) and not hasattr(factory, "create"):
            value = factory(session_spec)
            session = await value if inspect.isawaitable(value) else value
        if session is None:
            return None
        if hasattr(session, "create"):
            created = session.create(session_spec)
            await created if inspect.isawaitable(created) else created
        if start and hasattr(session, "start"):
            started = session.start()
            await started if inspect.isawaitable(started) else started
        if hasattr(session, "session_id"):
            return str(session.session_id)
        state = getattr(session, "_state", None)
        if state is not None and getattr(state, "session_id", None):
            return str(state.session_id)
        if isinstance(session, Mapping):
            value = session.get("session_id", session.get("id"))
            return str(value) if value else None
        return str(session)

    @staticmethod
    def _identifier(value: Any, key: str) -> str:
        if isinstance(value, Mapping):
            candidate = value.get(key, value.get("id"))
        else:
            candidate = getattr(value, key, getattr(value, "id", None))
        if candidate is None or not str(candidate):
            raise ValueError(f"allocated workspace has no {key}")
        return str(candidate)

    async def _orchestrate_impl(
        self, tasks: list[dict[str, Any]], *, parallel: bool
    ) -> dict[str, Any]:
        """DAG scheduler with immediate slot replenishment and typed fan-in."""
        if any(not isinstance(task, dict) or not task.get("id") for task in tasks):
            raise ValueError("every task requires a non-empty id")
        ids = [str(task["id"]) for task in tasks]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate task id")
        by_id = {str(task["id"]): task for task in tasks}
        dependencies = {task_id: list(by_id[task_id].get("depends_on", [])) for task_id in ids}
        if any(task_id in deps for task_id, deps in dependencies.items()):
            raise ValueError("self dependency")
        if any(dep not in by_id for deps in dependencies.values() for dep in deps):
            raise ValueError("missing dependency")
        self._validate_acyclic(dependencies)
        status = {task_id: "PENDING" for task_id in ids}
        outputs: dict[str, Any] = {}
        running: dict[str, asyncio.Task[Any]] = {}
        ready = [task_id for task_id in ids if not dependencies[task_id]]
        max_parallel = 1 if not parallel else max(1, int(self.config.get("max_parallel", 4)))
        backpressure_limit = self.config.get("backpressure_limit")
        backpressure_activated = False
        deadline = None
        if self.config.get("overall_timeout"):
            deadline = asyncio.get_running_loop().time() + float(self.config["overall_timeout"])

        async def admitted(task: dict[str, Any]) -> bool:
            policy = self.config.get("admission_control")
            if policy is None:
                return True
            value = policy(task=task, running=len(running), ready=len(ready))
            return bool(await value if inspect.isawaitable(value) else value)

        def mark_blocked() -> None:
            changed = True
            while changed:
                changed = False
                for task_id in ids:
                    if status[task_id] == "PENDING" and any(
                        status[dep]
                        in {"FAILED", "BLOCKED", "CANCELLED", "TIMED_OUT", "ADMISSION_REJECTED"}
                        for dep in dependencies[task_id]
                    ):
                        status[task_id] = "BLOCKED"
                        changed = True
                        if task_id in ready:
                            ready.remove(task_id)

        try:
            while ready or running or any(value == "PENDING" for value in status.values()):
                mark_blocked()
                if backpressure_limit is not None and len(ready) > int(backpressure_limit):
                    backpressure_activated = True
                for task_id in ids:
                    if (
                        status[task_id] == "PENDING"
                        and all(status[dep] == "COMPLETED" for dep in dependencies[task_id])
                        and task_id not in ready
                    ):
                        ready.append(task_id)
                while ready and len(running) < max_parallel:
                    task_id = ready.pop(0)
                    if not await admitted(by_id[task_id]):
                        status[task_id] = "ADMISSION_REJECTED"
                        outputs[task_id] = {"task_id": task_id, "status": "admission_rejected"}
                        continue
                    status[task_id] = "RUNNING"
                    timeout = by_id[task_id].get("timeout", self.config.get("task_timeout"))
                    coro = self._run_subagent(by_id[task_id])
                    running[task_id] = asyncio.create_task(
                        asyncio.wait_for(coro, float(timeout)) if timeout else coro
                    )
                if not running:
                    mark_blocked()
                    break
                wait_timeout = (
                    None
                    if deadline is None
                    else max(0.0, deadline - asyncio.get_running_loop().time())
                )
                done, _ = await asyncio.wait(
                    running.values(), timeout=wait_timeout, return_when=asyncio.FIRST_COMPLETED
                )
                if not done:
                    for task_id, child in running.items():
                        child.cancel()
                        status[task_id] = "TIMED_OUT"
                        outputs[task_id] = {"task_id": task_id, "status": "timed_out"}
                    await asyncio.gather(*running.values(), return_exceptions=True)
                    running.clear()
                    break
                for child in done:
                    task_id = next(key for key, value in running.items() if value is child)
                    del running[task_id]
                    try:
                        value = child.result()
                        outputs[task_id] = value
                        status[task_id] = (
                            "FAILED"
                            if isinstance(value, dict) and value.get("error")
                            else "COMPLETED"
                        )
                    except TimeoutError:
                        status[task_id] = "TIMED_OUT"
                        outputs[task_id] = {"task_id": task_id, "status": "timed_out"}
                    except asyncio.CancelledError:
                        status[task_id] = "CANCELLED"
                        raise
                    except type(Exception()) as exc:
                        status[task_id] = "FAILED"
                        outputs[task_id] = {
                            "task_id": task_id,
                            "status": "failed",
                            "error": str(exc),
                        }
            completed = [outputs[key] for key in ids if status[key] == "COMPLETED"]
            failed = [outputs[key] for key in ids if status[key] == "FAILED"]
            blocked = [
                {"task_id": key, "status": "blocked"} for key in ids if status[key] == "BLOCKED"
            ]
            cancelled = [
                {"task_id": key, "status": "cancelled"} for key in ids if status[key] == "CANCELLED"
            ]
            timed_out = [outputs[key] for key in ids if status[key] == "TIMED_OUT"]
            rejected = [outputs[key] for key in ids if status[key] == "ADMISSION_REJECTED"]
            total_cost = sum(
                float(item.get("cost_usd", 0.0))
                for item in outputs.values()
                if isinstance(item, dict)
            )
            self._total_cost_usd += total_cost
            self._task_count += len(tasks)
            return {
                "status": "completed",
                "results": [outputs[task_id] for task_id in ids if task_id in outputs],
                "completed": completed,
                "failed": failed + rejected,
                "blocked": blocked,
                "cancelled": cancelled,
                "timed_out": timed_out,
                "partial": bool(failed or blocked or cancelled or timed_out or rejected),
                "backpressure_activated": backpressure_activated,
                "total_cost_usd": total_cost,
            }
        finally:
            for child in running.values():
                child.cancel()
            if running:
                await asyncio.gather(*running.values(), return_exceptions=True)

    @staticmethod
    def _validate_acyclic(dependencies: dict[str, list[str]]) -> None:
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visiting:
                raise ValueError("dependency cycle")
            if node in visited:
                return
            visiting.add(node)
            for dep in dependencies[node]:
                visit(dep)
            visiting.remove(node)
            visited.add(node)

        for node in dependencies:
            visit(node)

    async def _run_subagent(self, task: dict[str, Any]) -> Any:
        """Invoke subagent_runner with iscoroutinefunction check."""
        try:
            if inspect.iscoroutinefunction(self.subagent_runner):
                result = await self.subagent_runner(task=task, config=self.config)
            else:
                raw = self.subagent_runner(task=task, config=self.config)
                if asyncio.iscoroutine(raw):
                    result = await raw
                else:
                    result = raw
            return result
        except (ValueError, TypeError, KeyError, AttributeError, RuntimeError, OSError) as e:
            self._last_error = f"{type(e).__name__}: {e}"
            logger.warning(f"SubagentOrchestratorEngine subagent_runner failed: {e}")
            return {"error": str(e)}

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "task_count": self._task_count,
                "total_cost_usd": self._total_cost_usd,
                "has_scheduler": self.scheduler is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("subagent_orchestrator", SubagentOrchestratorEngine)
