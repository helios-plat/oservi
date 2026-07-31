"""Event Webhook Dispatcher Engine Skeleton.

机制 (固化):
- on_signal 触发: 收到事件 → 并发 fan-out 到所有 subscribers
- asyncio.gather(..., return_exceptions=True) 并发拉起 (SPEC: 并发)
- 单个 subscriber 异常被捕获记录, 不向其余 subscriber 传播
- 每个 subscriber 结果通过 on_step 回调可观测

业务 (注入):
- subscribers: omodul callable 列表 (并发 fan-out 目标)

红线对照:
- 红线 2 (机制/业务分离): 订阅者业务全靠注入
- 红线 3 (注入契约): subscribers=omodul(1..n)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class EventWebhookDispatcherEngine(EngineSkeleton):
    """事件 webhook 分发引擎骨架 (on_signal 并发 fan-out).

    机制:
    - run() 非阻塞: on_signal 引擎只置位 _running 表示就绪, 不占循环.
    - dispatch(event, payload) 是事件驱动入口: 用 asyncio.gather 并发
      拉起所有 subscribers (SPEC: "asyncio.gather 并发拉起").
    - 每个 subscriber 以 keyword args 调用: sub(event=event, payload=payload).
    - 单个 subscriber 抛异常 → 被 gather(return_exceptions=True) 捕获,
      记入 errors, 不传播, 不影响其余 subscriber.

    Example::

        engine = EventWebhookDispatcherEngine(
            subscribers=[notify_slack, notify_telegram, audit_log],
            trigger={"on_signal": True},
            config={},
            name="order-events",
        )
        engine.run()  # 非阻塞, 置位就绪
        result = asyncio.run(engine.dispatch("order.created", {"order_id": 1}))
    """

    injection_points: ClassVar[dict] = {
        "subscribers": Injection(
            kind="omodul",
            cardinality="1..n",
            description="omoduls fanned out concurrently on a signal",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        subscribers: list[Callable[..., Any]] | Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.subscriber_list: list[Callable[..., Any]] = (
            subscribers if isinstance(subscribers, list) else [subscribers]
        )
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None
        self._dispatch_count = 0

    async def _invoke(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        """Sync/async-agnostic callable invocation (keyword args)."""
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        raw = fn(**kwargs)
        if asyncio.iscoroutine(raw):
            return await raw
        return raw

    def run(self) -> None:
        """on_signal 引擎: 非阻塞, 仅置位 _running 表示就绪."""
        self._running = True
        logger.info(f"EventWebhookDispatcherEngine '{self.name}' ready for on_signal dispatch")

    def stop(self) -> None:
        """优雅停止."""
        self._running = False
        self._stop_event.set()

    async def dispatch(
        self,
        event: str,
        payload: dict[str, Any],
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Fan out an event to ALL subscribers CONCURRENTLY.

        Args:
            event: Event name / topic string.
            payload: Event payload dict passed to each subscriber.
            on_step: Optional callback called per subscriber with
                     {"subscriber": str, "ok": bool}.

        Returns:
            {"status": "completed", "event": event,
             "results": [...], "errors": [...]}
        """
        self._dispatch_count += 1

        async def _call(sub: Callable[..., Any]) -> Any:
            return await self._invoke(sub, event=event, payload=payload)

        # 并发拉起所有 subscribers; return_exceptions 捕获单个异常
        raw_results = await asyncio.gather(
            *(_call(sub) for sub in self.subscriber_list),
            return_exceptions=True,
        )

        results: list[Any] = []
        errors: list[dict[str, Any]] = []
        for sub, raw in zip(self.subscriber_list, raw_results):
            sub_name = getattr(sub, "__name__", repr(sub))
            if isinstance(raw, BaseException):
                self._last_error = f"subscriber {sub_name}: {type(raw).__name__}: {raw}"
                logger.warning(
                    f"EventWebhookDispatcherEngine '{self.name}' subscriber "
                    f"{sub_name} failed: {raw}"
                )
                errors.append({"subscriber": sub_name, "error": str(raw)})
                ok = False
            else:
                results.append(raw)
                ok = True
            if on_step:
                try:
                    on_step({"subscriber": sub_name, "ok": ok})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed for {sub_name}: {cb_err}")

        return {
            "status": "completed",
            "event": event,
            "results": results,
            "errors": errors,
        }

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "subscribers_count": len(self.subscriber_list),
                "dispatch_count": self._dispatch_count,
                "last_error": self._last_error,
            },
        }


register_skeleton("event_webhook_dispatcher", EventWebhookDispatcherEngine)
