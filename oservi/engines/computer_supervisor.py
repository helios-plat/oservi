"""Reusable Computer Supervisor engine skeleton."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from obase.computer import ComputerHandle, ComputerProfile
from omodul.computer_session import (
    PrepareComputerSessionConfig,
    PrepareComputerSessionInput,
)

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


def _one(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


async def _call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


class ComputerSupervisorEngine(EngineSkeleton):
    """On-demand lifecycle engine with all physical operations injected."""

    injection_points: dict[str, Injection] = {  # noqa: RUF012
        "computer_create": Injection(
            kind="oprim", cardinality="1", description="atomic computer creation"
        ),
        "computer_start": Injection(
            kind="oprim", cardinality="1", description="atomic computer start"
        ),
        "computer_status": Injection(
            kind="oprim", cardinality="1", description="atomic computer status read"
        ),
        "computer_attach": Injection(
            kind="oprim", cardinality="1", description="atomic local/client attach"
        ),
        "computer_stop": Injection(
            kind="oprim", cardinality="1", description="atomic computer stop"
        ),
        "computer_reset": Injection(
            kind="oprim", cardinality="1", description="atomic computer reset"
        ),
        "readiness_evaluator": Injection(
            kind="oskill", cardinality="1", description="stateless readiness evaluator"
        ),
        "prepare_computer_session": Injection(
            kind="omodul", cardinality="1", description="computer preparation transaction"
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        computer_create: Callable[..., Any] | list[Callable[..., Any]],
        computer_start: Callable[..., Any] | list[Callable[..., Any]],
        computer_status: Callable[..., Any] | list[Callable[..., Any]],
        computer_attach: Callable[..., Any] | list[Callable[..., Any]],
        computer_stop: Callable[..., Any] | list[Callable[..., Any]],
        computer_reset: Callable[..., Any] | list[Callable[..., Any]],
        readiness_evaluator: Callable[..., Any] | list[Callable[..., Any]],
        prepare_computer_session: Callable[..., Any] | list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.computer_create = _one(computer_create)
        self.computer_start = _one(computer_start)
        self.computer_status = _one(computer_status)
        self.computer_attach = _one(computer_attach)
        self.computer_stop = _one(computer_stop)
        self.computer_reset = _one(computer_reset)
        self.readiness_evaluator = _one(readiness_evaluator)
        self.prepare_computer_session = _one(prepare_computer_session)
        self.trigger = trigger
        self.config = config
        self._running = False
        self._operation_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def create(self, profile: ComputerProfile | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.computer_create, profile)

    async def start(self, handle: ComputerHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.computer_start, handle)

    async def status(self, handle: ComputerHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.computer_status, handle)

    async def attach(self, handle: ComputerHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.computer_attach, handle)

    async def stop_computer(self, handle: ComputerHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.computer_stop, handle)

    async def reset(self, handle: ComputerHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.computer_reset, handle)

    async def _invoke(self, operation: Callable[..., Any], *args: Any) -> dict[str, Any]:
        try:
            result = await _call(operation, *args)
            self._operation_count += 1
            if isinstance(result, dict) and result.get("ok") is False:
                self._last_error = str(result.get("error") or "computer operation failed")
            return cast(dict[str, Any], result)
        except Exception as exc:  # noqa: BLE001 - engine boundary fails closed
            self._last_error = f"{type(exc).__name__}: {exc}"
            return {"ok": False, "status": "failed", "error": self._last_error}

    async def prepare(
        self,
        profile: ComputerProfile,
        *,
        attach: bool = False,
        output_dir: Path | None = None,
        on_step: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        """Run the injected omodul preparation transaction."""
        if not self._running:
            return {"ok": False, "status": "failed", "error": "computer supervisor is stopped"}
        try:
            result = await _call(
                self.prepare_computer_session,
                PrepareComputerSessionConfig(),
                PrepareComputerSessionInput(
                    profile=profile,
                    computer_create=self.computer_create,
                    computer_start=self.computer_start,
                    computer_status=self.computer_status,
                    readiness_evaluator=self.readiness_evaluator,
                    computer_attach=self.computer_attach,
                    attach=attach,
                ),
                output_dir or Path(self.config.get("output_dir", ".veya/computer")),
                on_step=on_step,
            )
            self._operation_count += 1
            if isinstance(result, dict) and result.get("status") == "failed":
                self._last_error = str((result.get("error") or {}).get("message", ""))
            return cast(dict[str, Any], result)
        except Exception as exc:  # noqa: BLE001 - engine boundary fails closed
            self._last_error = f"{type(exc).__name__}: {exc}"
            return {"ok": False, "status": "failed", "error": self._last_error}

    def health(self) -> dict[str, Any]:
        configured = all(
            value is not None
            for value in (
                self.computer_create,
                self.computer_start,
                self.computer_status,
                self.computer_attach,
                self.computer_stop,
                self.computer_reset,
                self.readiness_evaluator,
                self.prepare_computer_session,
            )
        )
        return {
            "status": "healthy" if self._running and configured else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "configured": configured,
                "operation_count": self._operation_count,
                "last_error": self._last_error,
                "remote_worker": False,
            },
        }


register_skeleton("computer_supervisor", ComputerSupervisorEngine)

__all__ = ["ComputerSupervisorEngine"]
