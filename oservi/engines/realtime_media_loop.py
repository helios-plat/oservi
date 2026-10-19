"""Realtime Media Loop Engine Skeleton — 实时媒体循环引擎.

机制 (固化):
- 持续循环：从注入的 media_source 拉取帧 → 帧处理器（VAD/STT）→
  响应合成器（LLM/TTS）→ 输出到 media_sink
- 每轮可被 stop() 打断；on_signal / on_interval 触发
- 处理器全部注入，机制无业务

业务 (注入):
- media_source: 1 个帧源（async iterator 或 async next_frame()）
- frame_processor: 1 个帧处理器（async (frame, ctx) -> event dict）
- responder: 0..1 个响应器（async (event, ctx) -> Any）
- media_sink: 0..1 个输出汇（async (output, ctx) -> None）

红线对照:
- 红线 2/3/4/5: 全注入；无状态骨架；不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, AsyncIterator, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class RealtimeMediaLoopEngine(EngineSkeleton):
    """实时媒体循环引擎骨架.

    Example::

        engine = RealtimeMediaLoopEngine(
            media_source=audio_frame_source,      # async iterator of frames
            frame_processor=vad_and_stt_fn,
            responder=llm_responder,              # async (event, ctx) -> reply
            media_sink=audio_sink,                # async (output, ctx) -> None
            trigger={"on_interval": 0.05},
            config={"max_iterations": 1000},
            name="voice-loop",
        )
        await engine.run()
    """

    injection_points: ClassVar[dict] = {
        "media_source": Injection(
            kind="obase",
            cardinality="1",
            description="帧源：async iterator 或 async next_frame()",
        ),
        "frame_processor": Injection(
            kind="oprim",
            cardinality="1",
            description="帧处理器：async (frame, ctx) -> event dict",
        ),
        "responder": Injection(
            kind="oskill",
            cardinality="0..1",
            description="响应合成器：async (event, ctx) -> Any",
        ),
        "media_sink": Injection(
            kind="obase",
            cardinality="0..1",
            description="输出汇：async (output, ctx) -> None",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        media_source: Any,
        frame_processor: Callable[..., Any],
        responder: Callable[..., Any] | None = None,
        media_sink: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.media_source = media_source
        self.frame_processor = frame_processor
        self.responder = responder
        self.media_sink = media_sink
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._iterations = 0
        self._last_error: str | None = None

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """持续拉帧处理直到 stop() / 源耗尽 / max_iterations。"""
        input_data = input_data or {}
        max_iterations = int(input_data.get("max_iterations") or self.config.get("max_iterations", 1000))
        ctx: dict[str, Any] = {"loop": self.name, "iteration": 0}
        self._running = True
        self._stop_event.clear()
        self._iterations = 0

        try:
            iterator = self._as_iterator()
            while not self._stop_event.is_set():
                if self._iterations >= max_iterations:
                    break
                try:
                    frame = await anext(iterator)
                except StopAsyncIteration:
                    break
                except Exception as exc:
                    self._last_error = f"source: {type(exc).__name__}: {exc}"
                    return {"status": "error", "reason": self._last_error, "iterations": self._iterations}

                ctx["iteration"] = self._iterations
                try:
                    event = await self.frame_processor(frame, ctx)
                    if event is None:
                        self._iterations += 1
                        continue
                    if on_step:
                        on_step({"iteration": self._iterations, "event": event})
                    if self.responder is not None:
                        response = await self.responder(event, ctx)
                        if response is not None and self.media_sink is not None:
                            await self.media_sink(response, ctx)
                except Exception as exc:
                    self._last_error = f"processor: {type(exc).__name__}: {exc}"
                    logger.warning("%s iteration %s failed: %s", self.name, self._iterations, exc)
                self._iterations += 1
        finally:
            self._running = False

        reason = (
            "stopped" if self._stop_event.is_set()
            else "source exhausted" if self._iterations > 0
            else "max_iterations"
        )
        return {
            "status": "completed",
            "reason": reason,
            "iterations": self._iterations,
            "last_error": self._last_error,
        }

    def _as_iterator(self) -> AsyncIterator[Any]:
        """把注入的帧源归一化为 async iterator。"""
        if hasattr(self.media_source, "__aiter__"):
            return self.media_source.__aiter__()
        if hasattr(self.media_source, "next_frame"):

            class _Adapter:
                def __init__(self, source: Any) -> None:
                    self._source = source

                def __aiter__(self) -> "_Adapter":
                    return self

                async def __anext__(self) -> Any:
                    frame = self._source.next_frame()
                    if inspect.isawaitable(frame):
                        frame = await frame
                    if frame is None:
                        raise StopAsyncIteration
                    return frame

            return _Adapter(self.media_source)
        raise TypeError(
            "realtime_media_loop: media_source must be an async iterator "
            "or provide async/sync next_frame()"
        )

    # ------------------------------------------------------------------
    # 控制
    # ------------------------------------------------------------------

    def stop(self) -> None:
        """优雅停止：当前迭代完成后退出循环。"""
        self._stop_event.set()

    @property
    def iterations(self) -> int:
        return self._iterations

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "idle",
            "details": {
                "name": self.name,
                "running": self._running,
                "iterations": self._iterations,
                "has_responder": self.responder is not None,
                "has_media_sink": self.media_sink is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("realtime_media_loop", RealtimeMediaLoopEngine)
