"""Reusable Action Gateway engine skeleton."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Callable

from obase.action import ActionDecision, ActionRequest
from omodul.action_gateway import (
    ExecuteGovernedActionConfig,
    ExecuteGovernedActionInput,
    GovernActionConfig,
    GovernActionInput,
    execute_governed_action,
    govern_action,
)
from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


def _one(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


class ActionGatewayEngine(EngineSkeleton):
    """On-demand governance/execution mechanism with injected callables."""

    injection_points: dict[str, Injection] = {
        "policy_evaluator": Injection(
            kind="oskill", cardinality="1", description="stateless action policy evaluator"
        ),
        "audit_append": Injection(
            kind="oprim", cardinality="1", description="atomic audit append operation"
        ),
        "executor": Injection(
            kind="oprim", cardinality="1", description="atomic tool invocation operation"
        ),
        "side_effect_record": Injection(
            kind="oprim", cardinality="1", description="atomic side-effect recording operation"
        ),
        "approval_resolver": Injection(
            kind="layer4", cardinality="0..1", description="project approval resolver"
        ),
        "audit_writer": Injection(
            kind="layer4", cardinality="1", description="project audit persistence adapter"
        ),
        "side_effect_recorder": Injection(
            kind="layer4", cardinality="0..1", description="project SideEffectLedger adapter"
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        policy_evaluator: Callable[..., Any] | list[Callable[..., Any]],
        audit_append: Callable[..., Any] | list[Callable[..., Any]],
        executor: Callable[..., Any] | list[Callable[..., Any]],
        side_effect_record: Callable[..., Any] | list[Callable[..., Any]],
        audit_writer: Callable[..., Any] | list[Callable[..., Any]],
        approval_resolver: Callable[..., Any] | list[Callable[..., Any]] | None = None,
        side_effect_recorder: Callable[..., Any] | list[Callable[..., Any]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.policy_evaluator = _one(policy_evaluator)
        self.audit_append = _one(audit_append)
        self.executor = _one(executor)
        self.side_effect_record = _one(side_effect_record)
        self.audit_writer = _one(audit_writer)
        self.approval_resolver = _one(approval_resolver)
        self.side_effect_recorder = _one(side_effect_recorder)
        self.trigger = trigger
        self.config = config
        self._running = False
        self._invoke_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def invoke(
        self,
        request: ActionRequest,
        *,
        physical_executor: Callable[[ActionRequest], Any] | None = None,
        operation_key: str = "",
        target_ref: str = "",
        capability: str = "manual_only",
        output_dir: Path | None = None,
        on_step: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        """Run govern then execute; physical mutation remains injected."""
        if self.policy_evaluator is None or self.audit_append is None or self.audit_writer is None:
            return {"status": "failed", "error": "Action Gateway injections are incomplete"}
        base = output_dir or Path(self.config.get("output_dir", ".veya/action_gateway"))
        governed = await govern_action(
            GovernActionConfig(),
            GovernActionInput(
                request=request,
                policy_evaluator=self.policy_evaluator,
                approval_resolver=self.approval_resolver,
                audit_append=self.audit_append,
                audit_writer=self.audit_writer,
            ),
            base,
            on_step=on_step,
        )
        raw_decision = governed.get("decision", {})
        decision = ActionDecision(
            verdict=str(raw_decision.get("verdict", "DENY")),  # type: ignore[arg-type]
            reason=str(raw_decision.get("reason", "")),
            policy_id=raw_decision.get("policy_id"),
            approved=bool(raw_decision.get("approved", False)),
            request_id=request.request_id,
        )
        if decision.verdict != "ALLOW":
            return governed

        selected_executor = self.executor
        if physical_executor is not None:

            async def selected_executor(request_value: ActionRequest) -> Any:
                result = self.executor(request_value, physical_executor)
                if inspect.isawaitable(result):
                    return await result
                return result

        result = await execute_governed_action(
            ExecuteGovernedActionConfig(),
            ExecuteGovernedActionInput(
                request=request,
                decision=decision,
                executor=selected_executor,
                audit_append=self.audit_append,
                audit_writer=self.audit_writer,
                side_effect_record=self.side_effect_record,
                side_effect_recorder=self.side_effect_recorder,
                operation_key=operation_key,
                target_ref=target_ref,
                capability=capability,
            ),
            base,
            on_step=on_step,
        )
        self._invoke_count += 1
        if result.get("status") == "failed":
            self._last_error = str(result.get("error"))
        return result

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "invoke_count": self._invoke_count,
                "last_error": self._last_error,
                "injections": [
                    "policy_evaluator",
                    "audit_append",
                    "executor",
                    "side_effect_record",
                    "approval_resolver",
                    "audit_writer",
                    "side_effect_recorder",
                ],
            },
        }


register_skeleton("action_gateway", ActionGatewayEngine)

__all__ = ["ActionGatewayEngine"]
