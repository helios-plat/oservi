"""Event Webhook Dispatcher Engine Skeleton.

机制 (固化):
- 持续运行,通过 obase.mq.EventBus 订阅 trigger["on_signal"] 指定的 topic
- 每收到一条事件,`asyncio.gather` 并发调用所有注入的 subscribers
- 单个 subscriber 失败不影响其余 subscriber(gather 用 return_exceptions=True)

业务 (注入):
- subscribers: omodul callable 列表(收到事件 payload 后各自执行,如发通知、
  同步 ERP 等)

这是 obase.EventBus 的第一个真实消费方——EventBus 只提供
publish/subscribe 原语,"收到事件后并发拉起 N 个订阅方,单个失败不拖累其余"
这个编排机制属于 oservi 而不是 obase(obase 无业务/无状态,这里是"有状态
持续运行"的机制)。

红线对照:
- 红线 2 (机制/业务分离): 具体订阅方业务全靠注入,骨架不关心事件内容
- 红线 3 (注入契约): subscribers=omodul(1..n)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包(仅 import obase,允许——见
  GOVERNANCE.md 依赖方向 oservi → omodul → oskill → oprim → obase)
"""

from __future__ import annotations

import asyncio
import logging
import inspect
from typing import Any, Callable, ClassVar

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)


class EventWebhookDispatcherEngine(EngineSkeleton):
    """事件 webhook 派发引擎骨架:监听信号,并发拉起所有订阅方。

    Example::

        engine = EventWebhookDispatcherEngine(
            subscribers=[send_order_confirmation_email, sync_order_to_erp],
            trigger={"on_signal": "order.placed"},
            config={"redis_url": "redis://localhost:6379/0"},
            name="order-placed-dispatcher",
        )
        engine.run()  # blocking
    """

    injection_points: ClassVar[dict] = {
        "subscribers": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Callables invoked concurrently for each received event payload",
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
        self.subscriber_list = subscribers if isinstance(subscribers, list) else [subscribers]
        if "on_signal" not in trigger:
            raise ValueError("EventWebhookDispatcherEngine trigger must contain 'on_signal'")
        self.topic = trigger["on_signal"]
        self.trigger = trigger
        self.config = config

        self._running = False
        self._stop_event = asyncio.Event()
        self._dispatch_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        """Start signal-driven engines without blocking the caller."""
        if self._running:
            raise RuntimeError(f"EventWebhookDispatcherEngine {self.name} already running")
        if self.trigger.get("on_signal"):
            self._running = True
            self._stop_event.clear()
            logger.info(f"EventWebhookDispatcherEngine '{self.name}' ready for signal dispatch")
            return
        self.serve()

    def serve(self) -> None:
        """Run the legacy event-bus subscription loop synchronously."""
        if self._running:
            raise RuntimeError(f"EventWebhookDispatcherEngine {self.name} already running")
        self._running = True
        logger.info(f"EventWebhookDispatcherEngine '{self.name}' starting on topic {self.topic!r}")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"EventWebhookDispatcherEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止主循环."""
        self._running = False
        self._stop_event.set()

    async def _run_loop(self) -> None:
        from obase.mq import EventBus

        bus = EventBus(redis_url=self.config.get("redis_url", "redis://localhost:6379/0"))
        poll_timeout = self.config.get("poll_timeout_seconds", 5.0)
        try:
            while self._running:
                try:
                    await bus.subscribe(self.topic, self._on_event, timeout=poll_timeout)
                except Exception as e:
                    self._last_error = f"{type(e).__name__}: {e}"
                    logger.warning(
                        f"EventWebhookDispatcherEngine '{self.name}' subscribe error: {e}"
                    )
                    await asyncio.sleep(1.0)
        finally:
            await bus.close()

    async def _on_event(self, payload: dict[str, Any]) -> None:
        results = await asyncio.gather(
            *[self._call_subscriber(sub, payload) for sub in self.subscriber_list],
            return_exceptions=True,
        )
        self._dispatch_count += 1
        for sub, result in zip(self.subscriber_list, results, strict=True):
            if isinstance(result, BaseException):
                self._last_error = f"{sub.__name__}: {result}"
                logger.warning(
                    f"EventWebhookDispatcherEngine '{self.name}' subscriber "
                    f"{sub.__name__} failed: {result}"
                )

    async def _call_subscriber(
        self, subscriber: Callable[..., Any], payload: dict[str, Any]
    ) -> Any:
        result = subscriber(event=payload)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "topic": self.topic,
                "subscribers_count": len(self.subscriber_list),
                "dispatch_count": self._dispatch_count,
                "last_error": self._last_error,
            },
        }


    async def _invoke(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
            """Sync/async-agnostic callable invocation (keyword args)."""
            if inspect.iscoroutinefunction(fn):
                return await fn(**kwargs)
            raw = fn(**kwargs)
            if asyncio.iscoroutine(raw):
                return await raw
            return raw

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

register_skeleton("event_webhook_dispatcher", EventWebhookDispatcherEngine)
