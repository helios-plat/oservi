"""Reusable tool governance engine skeleton."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any, Callable, cast

from obase.tool_governance import Grant, ToolCallRequest, ToolSpec
from omodul.governed_tool_transaction import GovernedToolConfig, GovernedToolInput
from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


def _one(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


class ToolGovernanceEngine(EngineSkeleton):
    """Route native and MCP calls into one injected governed transaction."""

    injection_points: dict[str, Injection] = {  # noqa: RUF012
        "registry": Injection("obase", "0..1", "native/MCP tool contract registry"),
        "tool_resolve": Injection("oprim", "1", "versioned native/MCP tool lookup"),
        "prepare_tool_execution": Injection("oskill", "1", "grant and effect preparation"),
        "governed_tool_transaction": Injection("omodul", "1", "canonical governed transaction"),
        "govern_action": Injection("omodul", "1", "PR-09 Action Gateway governance"),
        "execute_governed_action": Injection("omodul", "1", "PR-09 Action Gateway execution"),
        "policy_evaluator": Injection("oskill", "1", "stateless action policy"),
        "tool_call": Injection("oprim", "1", "native tool atomic"),
        "mcp_call": Injection("oprim", "1", "MCP tool atomic"),
        "credential_resolve": Injection("oprim", "1", "late-bound credential atomic"),
        "secret_read": Injection("oprim", "0..1", "late-bound secret atomic"),
        "audit_append": Injection("oprim", "1", "audit append atomic"),
        "audit_writer": Injection("layer4", "1", "existing audit persistence adapter"),
        "approval_resolver": Injection("layer4", "0..1", "existing user approval adapter"),
        "side_effect_record": Injection("oprim", "0..1", "existing ledger atomic"),
        "side_effect_recorder": Injection("layer4", "0..1", "existing SideEffectLedger adapter"),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        registry: Any = None,
        tool_resolve: Callable[..., Any],
        prepare_tool_execution: Callable[..., Any],
        governed_tool_transaction: Callable[..., Any],
        govern_action: Callable[..., Any],
        execute_governed_action: Callable[..., Any],
        policy_evaluator: Callable[..., Any],
        tool_call: Callable[..., Any],
        mcp_call: Callable[..., Any],
        credential_resolve: Callable[..., Any],
        secret_read: Callable[..., Any] | None = None,
        audit_append: Callable[..., Any],
        audit_writer: Callable[..., Any],
        approval_resolver: Callable[..., Any] | None = None,
        side_effect_record: Callable[..., Any] | None = None,
        side_effect_recorder: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.registry = registry
        self.tool_resolve = _one(tool_resolve)
        self.prepare_tool_execution = _one(prepare_tool_execution)
        self.governed_tool_transaction = _one(governed_tool_transaction)
        self.govern_action = _one(govern_action)
        self.execute_governed_action = _one(execute_governed_action)
        self.policy_evaluator = _one(policy_evaluator)
        self.tool_call = _one(tool_call)
        self.mcp_call = _one(mcp_call)
        self.credential_resolve = _one(credential_resolve)
        self.secret_read = _one(secret_read)
        self.audit_append = _one(audit_append)
        self.audit_writer = _one(audit_writer)
        self.approval_resolver = _one(approval_resolver)
        self.side_effect_record = _one(side_effect_record)
        self.side_effect_recorder = _one(side_effect_recorder)
        self.trigger = trigger
        self.config = config
        self._running = False
        self._call_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def invoke(
        self,
        request: ToolCallRequest,
        *,
        spec: ToolSpec | None = None,
        registry: Any = None,
        grant: Grant | None = None,
        executor: Any = None,
        mcp_client: Any = None,
        credential_resolver: Any = None,
        operation_key: str = "",
        target_ref: str = "",
        capability: str = "manual_only",
        output_dir: Path | None = None,
        on_step: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke the canonical transaction; no physical callable is stored."""
        if not self._running:
            return {
                "status": "failed",
                "error": {"type": "EngineStopped", "message": "tool governance engine is stopped"},
                "executed": False,
            }
        if grant is not None and grant is not request.grant:
            from dataclasses import replace

            request = replace(request, grant=grant)
        try:
            result = self.governed_tool_transaction(
                GovernedToolConfig(),
                GovernedToolInput(
                    request=request,
                    spec=spec,
                    registry=self.registry if registry is None else registry,
                    tool_resolve=self.tool_resolve,
                    prepare_tool_execution=self.prepare_tool_execution,
                    govern_action=self.govern_action,
                    execute_governed_action=self.execute_governed_action,
                    policy_evaluator=self.policy_evaluator,
                    approval_resolver=self.approval_resolver,
                    audit_append=self.audit_append,
                    audit_writer=self.audit_writer,
                    tool_call=self.tool_call,
                    mcp_call=self.mcp_call,
                    executor=executor,
                    mcp_client=mcp_client,
                    credential_resolve=self.credential_resolve,
                    secret_read=self.secret_read,
                    credential_resolver=credential_resolver,
                    side_effect_record=self.side_effect_record,
                    side_effect_recorder=self.side_effect_recorder,
                    operation_key=operation_key,
                    target_ref=target_ref,
                    capability=capability,
                ),
                output_dir or Path(self.config.get("output_dir", ".veya/tool_governance")),
                on_step=on_step,
            )
            if inspect.isawaitable(result):
                result = await result
            self._call_count += 1
            return cast(dict[str, Any], result)
        except Exception as exc:
            self._last_error = type(exc).__name__
            return {
                "status": "failed",
                "error": {"type": type(exc).__name__, "message": "tool governance failed"},
                "executed": False,
            }

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "call_count": self._call_count,
                "last_error": self._last_error,
                "governance": "action_gateway",
            },
        }


register_skeleton("tool_governance", ToolGovernanceEngine)

__all__ = ["ToolGovernanceEngine"]
