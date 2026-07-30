"""Bulk Import Worker Engine Skeleton.

机制 (固化):
- 通过 obase.mq.EventBus 订阅 trigger["on_signal"] 指定的 topic,收到一次
  信号即跑一次导入
- fetcher 返回的行迭代器(同步或异步均可)逐行消费,每次只处理一行,不把
  整个文件读进内存("流式处理大 CSV/Excel,防 OOM")
- 单行处理失败只计入 error_count,不中断整个流(避免一行脏数据拖垮整批)

业务 (注入):
- fetcher: oprim callable(接收 signal payload,返回/yield 行 dict 的
  同步或异步可迭代对象——具体怎么读 CSV/Excel、怎么连数据源,全是 fetcher
  自己的事,骨架完全不关心文件格式)
- processor: omodul callable(逐行处理,接收单行 dict)

红线对照:
- 红线 2 (机制/业务分离): 文件格式/数据源/单行业务处理全靠注入
- 红线 3 (注入契约): fetcher=oprim(1) / processor=omodul(1)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any, Callable, ClassVar

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)


async def _as_async_iterable(rows: Any) -> AsyncIterator[Any]:
    """Normalize a sync or async iterable/generator into an async iterator,
    so the engine's consume loop stays uniform regardless of what shape the
    injected fetcher returns."""
    if hasattr(rows, "__aiter__"):
        async for row in rows:
            yield row
    else:
        for row in rows:
            yield row


class BulkImportWorkerEngine(EngineSkeleton):
    """批量导入引擎骨架:流式取行 + 逐行处理,防 OOM。

    Example::

        engine = BulkImportWorkerEngine(
            fetcher=stream_csv_rows,
            processor=create_product_from_row,
            trigger={"on_signal": "bulk_import.requested"},
            config={"redis_url": "redis://localhost:6379/0"},
            name="product-csv-importer",
        )
        engine.run()  # blocking
    """

    injection_points: ClassVar[dict] = {
        "fetcher": Injection(
            kind="oprim",
            cardinality="1",
            description="Streams row dicts (sync or async iterable) given the signal payload",
        ),
        "processor": Injection(
            kind="omodul",
            cardinality="1",
            description="Processes a single row dict",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        fetcher: Callable[..., Any] | list[Callable[..., Any]],
        processor: Callable[..., Any] | list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.fetcher = fetcher[0] if isinstance(fetcher, list) else fetcher
        self.processor = processor[0] if isinstance(processor, list) else processor
        if "on_signal" not in trigger:
            raise ValueError("BulkImportWorkerEngine trigger must contain 'on_signal'")
        self.topic = trigger["on_signal"]
        self.trigger = trigger
        self.config = config

        self._running = False
        self._jobs_processed = 0
        self._last_row_count = 0
        self._last_error_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        """启动持续运行循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"BulkImportWorkerEngine {self.name} already running")
        self._running = True
        logger.info(f"BulkImportWorkerEngine '{self.name}' starting on topic {self.topic!r}")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"BulkImportWorkerEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止主循环."""
        self._running = False

    async def _run_loop(self) -> None:
        from obase.mq import EventBus

        bus = EventBus(redis_url=self.config.get("redis_url", "redis://localhost:6379/0"))
        poll_timeout = self.config.get("poll_timeout_seconds", 5.0)
        try:
            while self._running:
                try:
                    await bus.subscribe(self.topic, self._on_signal, timeout=poll_timeout)
                except Exception as e:
                    self._last_error = f"{type(e).__name__}: {e}"
                    logger.warning(f"BulkImportWorkerEngine '{self.name}' subscribe error: {e}")
                    await asyncio.sleep(1.0)
        finally:
            await bus.close()

    async def _on_signal(self, payload: dict[str, Any]) -> None:
        row_count = 0
        error_count = 0

        rows = self.fetcher(signal=payload)
        if asyncio.iscoroutine(rows):
            rows = await rows

        async for row in _as_async_iterable(rows):
            row_count += 1
            try:
                result = self.processor(row=row)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                error_count += 1
                self._last_error = f"row {row_count}: {e}"
                logger.warning(f"BulkImportWorkerEngine '{self.name}' row {row_count} failed: {e}")

        self._jobs_processed += 1
        self._last_row_count = row_count
        self._last_error_count = error_count
        logger.info(
            f"BulkImportWorkerEngine '{self.name}' finished job: "
            f"{row_count} rows, {error_count} errors"
        )

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "topic": self.topic,
                "jobs_processed": self._jobs_processed,
                "last_row_count": self._last_row_count,
                "last_error_count": self._last_error_count,
                "last_error": self._last_error,
            },
        }


register_skeleton("bulk_import_worker", BulkImportWorkerEngine)
