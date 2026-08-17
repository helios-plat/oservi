"""SpecDrivenGoalEngine — Spec Kit DAG + constitution-guarded leaves."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from obase.loop_breaker import init_breaker, reset_breaker
from obase.veya_workspace import TaskNode
from oservi.engines._base import EngineSkeleton, Injection, register_skeleton
from oskill.dag_compiler import pick_ready_task_ids


class SpecDrivenGoalEngine(EngineSkeleton):
    injection_points: ClassVar[dict] = {
        "leaf_executor": Injection(
            kind="oprim", cardinality="1", description="constitution-jailed leaf"
        ),
        "plan_phase": Injection(kind="omodul", cardinality="1"),
        "verify_phase": Injection(kind="omodul", cardinality="1"),
        "health_monitor": Injection(kind="omodul", cardinality="1"),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        leaf_executor: Callable[..., Any],
        plan_phase: Callable[..., Any],
        verify_phase: Callable[..., Any],
        health_monitor: Callable[..., Any],
        trigger: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        name: str = "spec-driven-goal",
        output_dir: Path | str | None = None,
    ) -> None:
        self.name = name
        self.leaf_executor = leaf_executor
        self.plan_phase = plan_phase
        self.verify_phase = verify_phase
        self.health_monitor = health_monitor
        self.trigger = trigger or {"on_demand": True}
        self.config = config or {}
        self.output_dir = Path(output_dir) if output_dir else Path(".")
        self._running = False

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def run_goal(self, project_root: Path | str, *, goal_id: str) -> dict[str, Any]:
        token = init_breaker()
        self._running = True
        try:
            plan = await self._call(
                self.plan_phase,
                config={
                    "goal_id": goal_id,
                    "project_root": str(project_root),
                    "max_leaf_tasks": self.config.get("max_leaf_tasks", 40),
                },
                input_data={},
                output_dir=self.output_dir,
            )
            if not isinstance(plan, dict) or plan.get("status") != "completed":
                return {"status": "blocked_invalid_spec", "plan": plan}
            findings = plan.get("findings") or {}
            nodes = [TaskNode.model_validate(t) for t in findings.get("tasks") or []]
            constitution = str(findings.get("constitution") or "")
            completed: set[str] = set()
            safety = 0
            while self._running and safety < 200:
                safety += 1
                ready = pick_ready_task_ids(nodes, completed_ids=completed)
                if not ready:
                    break
                node = next(n for n in nodes if n.id == ready[0])
                monitor = await self._call(
                    self.health_monitor,
                    config=self.config.get("monitor") or {},
                    input_data={
                        "tool_name": "leaf",
                        "arguments": {"id": node.id},
                        "execution_log": node.instruction,
                        "constitution": constitution,
                    },
                    output_dir=self.output_dir,
                )
                action = ((monitor or {}).get("findings") or {}).get("action") or "continue"
                if action == "halt":
                    return {"status": "halted_by_breaker", "completed": sorted(completed)}
                if action == "intervene":
                    node.retries += 1
                    node.status = "blocked"
                    return {
                        "status": "intervened",
                        "reason": (monitor.get("findings") or {}).get("intervention_prompt"),
                        "completed": sorted(completed),
                    }
                node.status = "running"
                leaf = await self._call(
                    self.leaf_executor,
                    project_root=project_root,
                    instruction=node.instruction,
                    constitution_text=constitution,
                    assignee=node.assignee,
                )
                log = str((leaf or {}).get("brief") or (leaf or {}).get("summary") or "")
                verify = await self._call(
                    self.verify_phase,
                    config={},
                    input_data={
                        "execution_log": log,
                        "acceptance": node.acceptance,
                        "leaf_status": (leaf or {}).get("status") or "",
                    },
                    output_dir=self.output_dir,
                )
                passed = ((verify or {}).get("findings") or {}).get("passed", True)
                if passed and (leaf or {}).get("status") != "blocked":
                    node.status = "completed"
                    completed.add(node.id)
                else:
                    node.retries += 1
                    node.status = "blocked"
                    return {
                        "status": "blocked",
                        "failed": node.id,
                        "completed": sorted(completed),
                    }
            status = "completed" if len(completed) == len(nodes) else "blocked"
            return {"status": status, "completed": sorted(completed), "tasks": len(nodes)}
        finally:
            self._running = False
            reset_breaker(token)

    async def _call(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        result = fn(**kwargs)
        if inspect.isawaitable(result):
            return await result
        return result


register_skeleton("spec_driven_goal", SpecDrivenGoalEngine)
