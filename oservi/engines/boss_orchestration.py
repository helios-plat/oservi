"""BossOrchestrationEngine — DAG dispatch + evidence verify. No coding."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from obase.loop_breaker import init_breaker, reset_breaker
from obase.veya_workspace import TaskNode
from oskill.dag_compiler import pick_ready_task_ids

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


class BossOrchestrationEngine(EngineSkeleton):
    """Contractor skeleton: snapshot → G0 intent → G1 plan → leaf → G2."""

    injection_points: ClassVar[dict] = {
        "inspector": Injection(
            kind="oprim", cardinality="1", description="capture project snapshot"
        ),
        "intent_phase": Injection(
            kind="omodul", cardinality="1", description="G0 intent triage"
        ),
        "plan_phase": Injection(kind="omodul", cardinality="1", description="G1 plan"),
        "verify_phase": Injection(
            kind="omodul", cardinality="1", description="G2 evidence verify"
        ),
        "leaf_executor": Injection(
            kind="oprim", cardinality="1", description="hicode/dsh leaf"
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        inspector: Callable[..., Any],
        intent_phase: Callable[..., Any],
        plan_phase: Callable[..., Any],
        verify_phase: Callable[..., Any],
        leaf_executor: Callable[..., Any],
        trigger: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        name: str = "veya-boss-mode",
        output_dir: Path | str | None = None,
        llm_caller: Callable[..., Any] | None = None,
    ) -> None:
        self.name = name
        self.inspector = inspector
        self.intent_phase = intent_phase
        self.plan_phase = plan_phase
        self.verify_phase = verify_phase
        self.leaf_executor = leaf_executor
        self.llm_caller = llm_caller
        self.trigger = trigger or {"on_demand": True}
        self.config = config or {}
        self.output_dir = Path(output_dir) if output_dir else Path(".")
        self._running = False

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def run_goal(
        self,
        project_root: Path | str,
        goal: str,
        *,
        goal_id: str = "boss",
    ) -> dict[str, Any]:
        token = init_breaker()
        self._running = True
        root = Path(project_root)
        try:
            snapshot = await self._capture(root)
            intent = await self._call(
                self.intent_phase,
                config={"goal_id": goal_id},
                input_data={
                    "snapshot": snapshot,
                    "goal": goal,
                    "llm_caller": self.llm_caller,
                },
                output_dir=self.output_dir,
            )
            if not isinstance(intent, dict) or intent.get("status") != "completed":
                return {"status": "blocked_intent_failed", "intent": intent}
            brief = (intent.get("findings") or {}).get("brief") or {}
            action = str(brief.get("action") or "")
            if action == "ask":
                return {
                    "status": "blocked_needs_clarification",
                    "interpretation": brief.get("interpretation"),
                    "questions": brief.get("questions") or [],
                }
            if action == "refuse":
                return {
                    "status": "blocked_intent_refused",
                    "reasons": brief.get("reasons") or [],
                }
            plan = await self._call(
                self.plan_phase,
                config={
                    "goal_id": goal_id,
                    "default_assignee": self.config.get("default_assignee", "hicode"),
                    "max_leaf_tasks": self.config.get("max_leaf_tasks", 40),
                },
                input_data={
                    "snapshot": snapshot,
                    "goal": goal,
                    "intent_brief": brief,
                    "llm_caller": self.llm_caller,
                },
                output_dir=self.output_dir,
            )
            if not isinstance(plan, dict) or plan.get("status") != "completed":
                return {"status": "blocked_plan_failed", "plan": plan}
            findings = plan.get("findings") or {}
            graph = findings.get("graph") or {}
            nodes = [TaskNode.model_validate(t) for t in graph.get("tasks") or []]
            if not nodes:
                return {"status": "blocked_plan_failed", "plan": plan}
            completed: set[str] = set()
            safety = 0
            max_retries = int(self.config.get("max_retries_per_task", 2))
            while self._running and safety < 200:
                safety += 1
                ready = pick_ready_task_ids(nodes, completed_ids=completed)
                if not ready:
                    break
                node = next(n for n in nodes if n.id == ready[0])
                if node.assignee == "ask":
                    node.status = "blocked"
                    return {
                        "status": "blocked_needs_clarification",
                        "failed": node.id,
                        "questions": [
                            f"Task {node.id} requires a human decision before dispatch."
                        ],
                        "completed": sorted(completed),
                    }
                node.status = "running"
                leaf = await self._execute_leaf(root, node)
                evidence = await self._evidence(root, leaf)
                verify = await self._call(
                    self.verify_phase,
                    config={},
                    input_data={
                        "task": node.model_dump(),
                        "leaf_result": evidence,
                        "llm_caller": self.llm_caller,
                    },
                    output_dir=self.output_dir,
                )
                vfind = (verify or {}).get("findings") or {}
                passed = bool(vfind.get("passed"))
                if passed and evidence.get("status") != "blocked":
                    node.status = "completed"
                    completed.add(node.id)
                    continue
                node.retries += 1
                correction = str(vfind.get("correction_instruction") or "").strip()
                if correction and node.retries <= max_retries:
                    node.instruction = f"{node.instruction}\n\n{correction}"
                    node.status = "pending"
                    continue
                node.status = "blocked"
                return {
                    "status": "blocked_verify_failed",
                    "failed": node.id,
                    "reason": vfind.get("summary"),
                    "completed": sorted(completed),
                    "tasks": len(nodes),
                }
            status = "completed" if len(completed) == len(nodes) else "blocked"
            return {
                "status": status,
                "completed": sorted(completed),
                "tasks": len(nodes),
            }
        finally:
            self._running = False
            reset_breaker(token)

    async def _capture(self, project_root: Path) -> Any:
        fn = self.inspector
        if hasattr(fn, "capture_snapshot"):
            fn = fn.capture_snapshot
        snap = await self._invoke_flexible(fn, project_root)
        if hasattr(snap, "model_dump"):
            return snap.model_dump()
        return snap

    async def _execute_leaf(self, project_root: Path, node: TaskNode) -> dict[str, Any]:
        try:
            leaf = await self._call(
                self.leaf_executor,
                project_root=project_root,
                instruction=node.instruction,
                constitution_text="",
                assignee=node.assignee,
            )
        except TypeError:
            leaf = await self._call(
                self.leaf_executor,
                project_root=project_root,
                instruction=node.instruction,
                assignee=node.assignee,
            )
        return leaf if isinstance(leaf, dict) else {"status": "blocked", "stdout": str(leaf)}

    async def _invoke_flexible(self, fn: Callable[..., Any], project_root: Path) -> Any:
        try:
            result = fn()
            if inspect.isawaitable(result):
                result = await result
            return result
        except TypeError:
            result = fn(project_root)
            if inspect.isawaitable(result):
                return await result
            return result

    async def _evidence(self, project_root: Path, leaf: dict[str, Any]) -> dict[str, Any]:
        evidence = dict(leaf)
        if evidence.get("git_diff"):
            return evidence
        try:
            snap = await self._capture(project_root)
        except (OSError, TypeError, ValueError):
            return evidence
        if isinstance(snap, dict) and snap.get("git_diff"):
            evidence["git_diff"] = snap["git_diff"]
        return evidence

    async def _call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if args and not kwargs:
            result = fn(*args)
        elif args:
            result = fn(*args, **kwargs)
        else:
            result = fn(**kwargs)
        if inspect.isawaitable(result):
            return await result
        return result


register_skeleton("boss_orchestration", BossOrchestrationEngine)
register_skeleton("BossOrchestrationEngine", BossOrchestrationEngine)
