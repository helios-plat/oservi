"""VeyaEvolutionEngine — file-change signal → feedback omodul. No LoRA body."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


class VeyaEvolutionEngine(EngineSkeleton):
    """Listen for Layer-4 save/commit events and run implicit feedback.

    ``lora_trainer`` is optional and never implemented here.
    """

    injection_points: ClassVar[dict] = {
        "shadow_vcs_hook": Injection(
            kind="layer4",
            cardinality="1",
            description="async iterator of IDE/save commit events",
        ),
        "feedback_processor": Injection(
            kind="omodul",
            cardinality="1",
            description="implicit_feedback_processor",
        ),
        "lora_trainer": Injection(
            kind="omodul",
            cardinality="0..1",
            description="optional PEFT/LoRA pipeline — not in this skeleton",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        shadow_vcs_hook: Callable[..., Any],
        feedback_processor: Callable[..., Any],
        lora_trainer: Callable[..., Any] | None = None,
        trigger: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        name: str = "veya-evolution",
        output_dir: Path | str | None = None,
    ) -> None:
        self.name = name
        self.shadow_vcs_hook = shadow_vcs_hook
        self.feedback_processor = feedback_processor
        self.lora_trainer = lora_trainer
        self.trigger = trigger or {"on_signal": True}
        self.config = config or {}
        self.output_dir = Path(output_dir) if output_dir else Path(".")
        self._running = False
        self._dispatched = 0
        self.last_result: dict[str, Any] | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def listen_and_dispatch(self) -> list[dict[str, Any]]:
        self._running = True
        results: list[dict[str, Any]] = []
        hook = self.shadow_vcs_hook
        stream = hook() if callable(hook) else hook
        if inspect.isawaitable(stream):
            stream = await stream
        async for event in stream:
            if not self._running:
                break
            rec = await self.dispatch(event)
            results.append(rec)
        return results

    async def dispatch(self, event: dict[str, Any]) -> dict[str, Any]:
        config = {
            "repo_path": event.get("repo_path"),
            "file_path": event.get("file_path"),
            "v0_commit": event.get("v0_commit", ""),
            "v1_commit": event.get("v1_commit", ""),
        }
        input_data = {
            "entity_id": event.get("entity_id", ""),
            "llm_caller": event.get("llm_caller"),
            "graph_pool": event.get("graph_pool"),
        }
        rec = await self._call(
            self.feedback_processor,
            config=config,
            input_data=input_data,
            output_dir=self.output_dir,
        )
        self._dispatched += 1
        self.last_result = rec if isinstance(rec, dict) else {"status": "completed", "raw": rec}
        return self.last_result

    async def _call(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        result = fn(**kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "dispatched": self._dispatched,
                "has_lora": self.lora_trainer is not None,
            },
        }


register_skeleton("veya_evolution", VeyaEvolutionEngine)
