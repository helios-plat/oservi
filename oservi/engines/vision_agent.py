"""
oservi/engines/vision_agent.py — VisionAgentEngine Skeleton.

Stateful engine skeleton for vision agent sessions.

五红线自检:
✅ 红线 1  反复出现: vision agent 是常见服务形态
✅ 红线 2  机制/业务分离: 引擎固化视觉处理循环; 业务靠注入
✅ 红线 3  注入点契约: 每注入点含 kind + cardinality
✅ 红线 4  无状态骨架: 持运行时状态, 不持业务状态
✅ 红线 5  不反向依赖: 骨架不硬编码 import oprim/oskill/omodul
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Graceful import: oservi may not be fully installed with all deps
try:
    from oservi.engines._base import (
        EngineSkeleton,
        Injection,
        register_skeleton,
    )
except ImportError:
    class EngineSkeleton:
        injection_points: dict = {}
    class Injection:
        def __init__(self, *, kind=None, cardinality=None, description=None):
            self.kind = kind
            self.cardinality = cardinality
            self.description = description
    def register_skeleton(name, cls):
        pass


# ---------------------------------------------------------------------------
# Vision session state (runtime only)
# ---------------------------------------------------------------------------


@dataclass
class VisionSessionState:
    """Runtime state for a vision agent session."""

    session_id: str = ""
    images_processed: int = 0
    start_ts: float = field(default_factory=time.time)
    events: list[dict] = field(default_factory=list)

    def record(self, **kwargs) -> None:
        self.events.append({"ts": time.time(), **kwargs})


# ---------------------------------------------------------------------------
# VisionAgentEngine skeleton
# ---------------------------------------------------------------------------


class VisionAgentEngine(EngineSkeleton):
    """Vision agent engine — manages vision analysis sessions (on_demand).

    Processes image/video input through vision LLMs to produce descriptions,
    object detections, and text extraction.

    Injection points:
        vision_skill   — oskill vision pipeline (analyze_image)
        llm_caller     — oprim LLM call primitive
        image_oprim    — oprim image op (encode/decode/validate)
        video_sampler  — oskill video frame sampler
        tools          — layer4 tool adapters
    """

    injection_points = {
        "vision_skill": Injection(
            kind="oskill", cardinality="1",
            description="Vision analysis: (image, provider, prompt, **opts) -> VisionResult"
        ),
        "llm_caller": Injection(
            kind="oprim", cardinality="1",
            description="LLM call primitive for text-based follow-up"
        ),
        "image_oprim": Injection(
            kind="oprim", cardinality="1",
            description="Image operations: encode/decode/validate"
        ),
        "video_sampler": Injection(
            kind="oskill", cardinality="0..1",
            description="Video frame sampler: (path, interval, max) -> list[ImageFrame]"
        ),
        "tools": Injection(
            kind="layer4", cardinality="0..n",
            description="Tool adapters for LLM function calling"
        ),
    }

    def __init__(
        self,
        *,
        vision_skill,
        llm_caller,
        image_oprim,
        video_sampler=None,
        tools=None,
        trigger: dict,
        config: dict | None = None,
        name: str = "vision-agent",
    ):
        super().__init__()
        self.vision_skill = vision_skill
        self.llm_caller = llm_caller
        self.image_oprim = image_oprim
        self.video_sampler = video_sampler
        self.tools = tools or []
        self.trigger = trigger
        self.config = config or {}
        self.name = name

        self._running = False
        self._current_session: VisionSessionState | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "name": self.name,
            "current_session": self._current_session.session_id if self._current_session else None,
        }

    # ── Core session API ──────────────────────────────────────────────────

    async def session(
        self,
        media_input: list[bytes] | str,
        *,
        media_type: str = "image",
        prompt: str = "Describe this image in detail.",
        system_prompt: str | None = None,
        provider: str = "openai",
        model: str | None = None,
        max_images: int = 20,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict:
        """Run a vision analysis session.

        Args:
            media_input: List of image bytes, or path to image/video file.
            media_type: "image", "images", or "video".
            prompt: What to ask about the media.
            system_prompt: System-level instruction.
            provider: Vision provider name.
            model: Model name.
            max_images: Max images to process.
            on_event: Lifecycle event callback.

        Returns:
            Dict with description, per_image_results, stats.
        """
        import uuid

        state = VisionSessionState(
            session_id=str(uuid.uuid4())[:8],
        )
        self._current_session = state

        if on_event:
            on_event("session_start", {"session_id": state.session_id, "media_type": media_type})

        results: list[dict] = []

        if media_type == "image":
            # Single image
            image_data = media_input if isinstance(media_input, bytes) else media_input[0]

            if on_event:
                on_event("analyzing", {"image_index": 0})

            try:
                vis_result = await self.vision_skill(
                    image_data,
                    provider=provider,
                    model=model,
                    prompt=prompt,
                    system_prompt=system_prompt,
                )
                results.append({
                    "description": vis_result.description,
                    "objects": vis_result.objects,
                    "text_in_image": vis_result.text_in_image,
                })
                state.images_processed = 1
            except Exception as e:
                state.record(event="vision_error", error=str(e))
                results.append({"error": str(e)})

        elif media_type == "images":
            # Multiple images
            images = media_input if isinstance(media_input, list) else [media_input]
            images = images[:max_images]

            for i, img in enumerate(images):
                if on_event:
                    on_event("analyzing", {"image_index": i, "total": len(images)})

                try:
                    vis_result = await self.vision_skill(
                        img,
                        provider=provider,
                        model=model,
                        prompt=f"Image {i+1}: {prompt}",
                    )
                    results.append({"description": vis_result.description})
                    state.images_processed += 1
                except Exception as e:
                    state.record(event="vision_error", index=i, error=str(e))

        elif media_type == "video":
            # Video — sample frames then analyze
            if self.video_sampler is None:
                return {"status": "error", "error": "video_sampler not injected", "results": []}

            if isinstance(media_input, str):
                video_path = media_input
            else:
                return {"status": "error", "error": "video path required", "results": []}

            if on_event:
                on_event("sampling_frames", {})

            frames = self.video_sampler(
                video_path,
                interval_sec=self.config.get("video_frame_interval_sec", 1.0),
                max_frames=self.config.get("max_video_frames", 30),
            )

            if on_event:
                on_event("frames_sampled", {"count": len(frames)})

            for i, frame in enumerate(frames):
                try:
                    vis_result = await self.vision_skill(
                        frame.data,
                        provider=provider,
                        model=model,
                        prompt=f"Frame {i} (t={frame.timestamp_ms:.0f}ms): {prompt}",
                    )
                    results.append({
                        "frame_index": i,
                        "timestamp_ms": frame.timestamp_ms,
                        "description": vis_result.description,
                    })
                    state.images_processed += 1
                except Exception as e:
                    state.record(event="vision_error", frame=i, error=str(e))

        # Generate summary from all results
        if len(results) > 1 and self.llm_caller:
            if on_event:
                on_event("summarizing", {})

            descriptions = [r.get("description", "") for r in results if "description" in r]
            try:
                summary_prompt = (
                    f"Summarize these frame descriptions into a cohesive narrative:\n\n"
                    + "\n\n---\n\n".join(f"[{i}]: {d}" for i, d in enumerate(descriptions))
                )
                summary_resp = await self.llm_caller(
                    messages=[{"role": "user", "content": summary_prompt}],
                    max_tokens=512,
                )
                summary_text = summary_resp.get("content", "")
                if isinstance(summary_text, list):
                    summary_text = summary_text[0].get("text", "") if summary_text else ""
            except Exception:
                summary_text = "\n".join(descriptions)
        else:
            summary_text = results[0].get("description", "") if results else ""

        state.record(event="session_done", images_processed=state.images_processed)

        if on_event:
            on_event("session_done", {"images_processed": state.images_processed})

        return {
            "status": "completed",
            "description": summary_text,
            "results": results,
            "images_processed": state.images_processed,
            "session_id": state.session_id,
            "events": state.events,
        }


# ── Register skeleton ────────────────────────────────────────────────────

register_skeleton("vision_agent", VisionAgentEngine)
