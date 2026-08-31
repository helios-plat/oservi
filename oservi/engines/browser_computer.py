"""Reusable browser/computer lifecycle engine skeleton."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from obase.browser import BrowserProfile, BrowserSessionHandle
from obase.computer import ComputerProfile
from omodul.browser_session import PrepareBrowserSessionConfig, PrepareBrowserSessionInput

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


def _handle(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        candidate = value.get("handle") or value.get("browser") or value.get("computer") or value
        return candidate if isinstance(candidate, Mapping) else None
    return None


class BrowserComputerEngine(EngineSkeleton):
    """On-demand browser lifecycle mechanism bound to an existing computer."""

    injection_points: dict[str, Injection] = {  # noqa: RUF012
        "computer_prepare": Injection(
            kind="layer4", cardinality="1", description="existing Computer Supervisor prepare"
        ),
        "browser_create": Injection(
            kind="oprim", cardinality="1", description="atomic browser session creation"
        ),
        "browser_start": Injection(
            kind="oprim", cardinality="1", description="atomic browser session start"
        ),
        "browser_status": Injection(
            kind="oprim", cardinality="1", description="atomic browser session status read"
        ),
        "browser_attach": Injection(
            kind="oprim", cardinality="1", description="atomic browser view attach"
        ),
        "browser_stop": Injection(
            kind="oprim", cardinality="1", description="atomic browser session stop"
        ),
        "browser_reset": Injection(
            kind="oprim", cardinality="1", description="atomic browser session reset"
        ),
        "browser_set_control_state": Injection(
            kind="oprim", cardinality="1", description="atomic browser control-state update"
        ),
        "prepare_browser_session": Injection(
            kind="omodul", cardinality="1", description="browser preparation transaction"
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        computer_prepare: Callable[..., Any] | list[Callable[..., Any]],
        browser_create: Callable[..., Any] | list[Callable[..., Any]],
        browser_start: Callable[..., Any] | list[Callable[..., Any]],
        browser_status: Callable[..., Any] | list[Callable[..., Any]],
        browser_attach: Callable[..., Any] | list[Callable[..., Any]],
        browser_stop: Callable[..., Any] | list[Callable[..., Any]],
        browser_reset: Callable[..., Any] | list[Callable[..., Any]],
        browser_set_control_state: Callable[..., Any] | list[Callable[..., Any]],
        prepare_browser_session: Callable[..., Any] | list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.computer_prepare = _one(computer_prepare)
        self.browser_create = _one(browser_create)
        self.browser_start = _one(browser_start)
        self.browser_status = _one(browser_status)
        self.browser_attach = _one(browser_attach)
        self.browser_stop = _one(browser_stop)
        self.browser_reset = _one(browser_reset)
        self.browser_set_control_state = _one(browser_set_control_state)
        self.prepare_browser_session = _one(prepare_browser_session)
        self.trigger = trigger
        self.config = config
        self._running = False
        self._operation_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def prepare(
        self,
        computer_profile: ComputerProfile,
        browser_profile: BrowserProfile | Mapping[str, Any],
        *,
        attach: bool = False,
        output_dir: Path | None = None,
        on_step: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        """Prepare the existing computer, then create its browser session."""
        if not self._running:
            return {"ok": False, "status": "failed", "error": "browser computer is stopped"}
        base = output_dir or Path(self.config.get("output_dir", ".veya/browser"))
        try:
            computer_result = await _call(
                self.computer_prepare,
                computer_profile,
                attach=attach,
                output_dir=base,
                on_step=on_step,
            )
            if not isinstance(computer_result, Mapping) or computer_result.get("ok") is False:
                return {
                    "ok": False,
                    "status": "failed",
                    "error": str(
                        (computer_result or {}).get("error") or "computer preparation failed"
                    ),
                    "computer": dict(computer_result or {}),
                }
            computer = _handle(computer_result)
            if computer is None:
                return {
                    "ok": False,
                    "status": "failed",
                    "error": "computer prepare returned no handle",
                }
            computer_id = str(computer.get("computer_id") or "")
            selected_input = (
                browser_profile
                if isinstance(browser_profile, BrowserProfile)
                else BrowserProfile(**dict(browser_profile))
            )
            if selected_input.computer_id and selected_input.computer_id != computer_id:
                return {
                    "ok": False,
                    "status": "failed",
                    "error": "browser profile is bound to a different computer",
                }
            selected_browser = (
                selected_input
                if selected_input.computer_id
                else replace(selected_input, computer_id=computer_id)
            )
            result = await _call(
                self.prepare_browser_session,
                PrepareBrowserSessionConfig(),
                PrepareBrowserSessionInput(
                    profile=selected_browser,
                    computer=computer,
                    browser_create=self.browser_create,
                    browser_start=self.browser_start,
                    browser_status=self.browser_status,
                    browser_attach=self.browser_attach,
                    attach=attach,
                ),
                base,
                on_step=on_step,
            )
            self._operation_count += 1
            if isinstance(result, Mapping) and result.get("status") == "failed":
                self._last_error = str(result.get("error"))
            return cast(dict[str, Any], result)
        except Exception as exc:  # noqa: BLE001 - engine boundary fails closed
            self._last_error = f"{type(exc).__name__}: {exc}"
            return {"ok": False, "status": "failed", "error": self._last_error}

    async def status(self, handle: BrowserSessionHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.browser_status, handle)

    async def stop_browser(
        self, handle: BrowserSessionHandle | Mapping[str, Any]
    ) -> dict[str, Any]:
        return await self._invoke(self.browser_stop, handle)

    async def reset(self, handle: BrowserSessionHandle | Mapping[str, Any]) -> dict[str, Any]:
        return await self._invoke(self.browser_reset, handle)

    async def take_control(
        self, handle: BrowserSessionHandle | Mapping[str, Any]
    ) -> dict[str, Any]:
        return await self._invoke(self.browser_set_control_state, handle, state="HUMAN_CONTROL")

    async def return_control(
        self, handle: BrowserSessionHandle | Mapping[str, Any]
    ) -> dict[str, Any]:
        return await self._invoke(self.browser_set_control_state, handle, state="AGENT_CONTROL")

    async def _invoke(
        self, operation: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        try:
            result = await _call(operation, *args, **kwargs)
            self._operation_count += 1
            if isinstance(result, dict) and result.get("ok") is False:
                self._last_error = str(result.get("error") or "browser operation failed")
            return cast(dict[str, Any], result)
        except Exception as exc:  # noqa: BLE001 - engine boundary fails closed
            self._last_error = f"{type(exc).__name__}: {exc}"
            return {"ok": False, "status": "failed", "error": self._last_error}

    def health(self) -> dict[str, Any]:
        configured = all(
            value is not None
            for value in (
                self.computer_prepare,
                self.browser_create,
                self.browser_start,
                self.browser_status,
                self.browser_attach,
                self.browser_stop,
                self.browser_reset,
                self.browser_set_control_state,
                self.prepare_browser_session,
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


register_skeleton("browser_computer", BrowserComputerEngine)

__all__ = ["BrowserComputerEngine"]
