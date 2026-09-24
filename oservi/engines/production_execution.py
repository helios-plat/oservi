"""Generic, on-demand execution engine for one injected omodul operation.

The engine deliberately has no persistence, user data or provider selection.
Applications inject one standard omodul transaction plus an optional event sink
and own all database, queue and tenant concerns themselves.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, ClassVar, Literal

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


ExecutionStatus = Literal["succeeded", "failed", "cancelled"]
EventSink = Callable[[dict[str, Any]], Awaitable[None] | None]
Operation = Callable[[dict[str, Any], dict[str, Any], Path], Awaitable[dict[str, Any]] | dict[str, Any]]


@dataclass(frozen=True)
class ExecutionEvent:
    """Portable progress event; applications decide how and where to persist it."""

    stage: str
    progress_pct: float | None = None
    message: str | None = None
    error: dict[str, str] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "progress_pct": self.progress_pct,
            "message": self.message,
            "error": self.error,
            "metadata": self.metadata,
        }


class ProductionExecutionEngine(EngineSkeleton):
    """Execute one standard ``omodul(config, input_data, output_dir)`` operation."""

    injection_points: ClassVar[dict[str, Injection]] = {
        "operation": Injection("omodul", "1", "One standard production transaction"),
        "event_sink": Injection("layer4", "0..1", "Optional application progress projection"),
    }
    trigger_mode = "on_demand"

    def __init__(
        self,
        *,
        operation: Operation,
        event_sink: EventSink | None = None,
        trigger: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        name: str = "production-execution",
    ) -> None:
        self.operation = operation
        self.event_sink = event_sink
        self.trigger = trigger or {"on_demand": True}
        self.config = config or {}
        self.name = name
        self._stopped = False

    async def _emit(self, event: ExecutionEvent) -> None:
        if self.event_sink is None:
            return
        emitted = self.event_sink(event.to_dict())
        if inspect.isawaitable(emitted):
            await emitted

    async def run(
        self, *, input_data: dict[str, Any], output_dir: str | Path, config: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if self._stopped:
            return {
                "status": "cancelled",
                "error": {"code": "ENGINE_STOPPED", "message": "execution was stopped before start"},
                "artifacts": [],
            }

        merged_config = {**self.config, **(config or {})}
        resolved_output = Path(output_dir)
        resolved_output.mkdir(parents=True, exist_ok=True)
        await self._emit(ExecutionEvent(stage="started", progress_pct=0.0))
        try:
            produced = self.operation(merged_config, input_data, resolved_output)
            result = await produced if inspect.isawaitable(produced) else produced
        except asyncio.CancelledError:
            await asyncio.shield(self._emit(ExecutionEvent(stage="cancelled", error={"code": "CANCELLED", "message": "execution cancelled"})))
            raise
        except Exception as exc:
            error = {"code": type(exc).__name__.upper(), "message": str(exc)[:500]}
            await self._emit(ExecutionEvent(stage="failed", error=error))
            return {"status": "failed", "error": error, "artifacts": []}

        if not isinstance(result, dict):
            error = {"code": "INVALID_OPERATION_RESULT", "message": "omodul result must be a dict"}
            await self._emit(ExecutionEvent(stage="failed", error=error))
            return {"status": "failed", "error": error, "artifacts": []}

        status = result.get("status", "succeeded")
        if status not in {"succeeded", "failed", "cancelled"}:
            error = {"code": "INVALID_OPERATION_STATUS", "message": f"unsupported status: {status}"}
            await self._emit(ExecutionEvent(stage="failed", error=error))
            return {"status": "failed", "error": error, "artifacts": []}

        normalized = {"artifacts": [], "error": None, **result, "status": status}
        await self._emit(ExecutionEvent(stage=status, progress_pct=100.0 if status == "succeeded" else None, error=normalized.get("error")))
        return normalized

    def stop(self) -> None:
        self._stopped = True


register_skeleton("production_execution", ProductionExecutionEngine)
