"""3O 范式全栈 — oservi 守护引擎 (元素 25).

机制固化: 订阅触发主题 → 调用注入 worker → 发布结果主题。
业务全靠 manifest 注入 (worker), 引擎自身零业务逻辑。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from obase import IDaemon_Bus

from oservi.engine_manifest import ServiceManifest


class DaemonEngine:
    """守护引擎骨架: 触发订阅 → worker 执行 → 结果发布。

    Args:
        manifest: 服务清单 (名称决定默认主题)。
        bus: 守护总线契约实现 (依赖注入)。
        worker: 业务处理 callable (注入填料), 接收 payload 返回结果字典。
        trigger_topic: 触发主题 (默认 engine/{name}/trigger)。
        result_topic: 结果主题 (默认 engine/{name}/result)。
    """

    def __init__(
        self,
        manifest: ServiceManifest,
        *,
        bus: IDaemon_Bus,
        worker: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
        trigger_topic: str | None = None,
        result_topic: str | None = None,
    ):
        self.manifest = manifest
        self.bus = bus
        self.worker = worker
        self.trigger_topic = trigger_topic or f"engine/{manifest.name}/trigger"
        self.result_topic = result_topic or f"engine/{manifest.name}/result"
        self._running = False

    async def handle_once(self, payload: dict[str, Any]) -> dict[str, Any]:
        """处理单次触发: 执行 worker 并发布结果 (不进入常驻订阅)。"""
        result = await self.worker(payload)
        await self.bus.publish(self.result_topic, result)
        return result

    async def serve(self) -> None:
        """常驻服务: 订阅触发主题, 逐条处理直至 stop。"""
        if self._running:
            return
        self._running = True
        subscription = await self.bus.subscribe(self.trigger_topic)
        try:
            while self._running:
                payload = await subscription.receive()
                result = await self.worker(payload)
                await self.bus.publish(self.result_topic, result)
        finally:
            subscription.close()

    async def stop(self) -> None:
        """请求常驻循环退出 (下一次消息处理后生效)。"""
        self._running = False
        # 唤醒可能在阻塞接收中的 serve 循环
        await asyncio.sleep(0)
