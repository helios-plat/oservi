"""Bulk Import Worker Engine Skeleton.

机制 (固化):
- on_signal 触发: 收到信号 → 从 fetcher 流式拉记录 → processor 逐条处理
- async-generator 流式迭代, 不一次性载入全部记录 (anti-OOM)
- 单条处理异常被捕获记录, 不中断整体导入
- 每条结果通过 on_step 回调可观测

业务 (注入):
- fetcher: oprim callable, yield/return 输入记录 (e.g. CSV rows)
- processor: omodul callable, 逐条 (或逐块) 应用

fetcher 兼容形态 (全部支持):
- sync generator function (yield ...)
- async generator function (async yield ...)
- sync callable 返回 iterable (list / generator)
- async callable 返回 iterable

红线对照:
- 红线 2 (机制/业务分离): 抓取/处理业务全靠注入
- 红线 3 (注入契约): fetcher=oprim(1) / processor=omodul(1)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
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


class BulkImportWorkerEngine(EngineSkeleton):
    """批量导入 worker 引擎骨架 (on_signal 流式处理, anti-OOM).

    机制:
    - run() 非阻塞: on_signal 引擎只置位 _running 表示就绪.
    - process() 是事件驱动入口: 用 async generator 从 fetcher 流式拉
      记录, 逐条交给 processor. 记录是惰性 yield 的, 任意时刻只有
      当前记录在内存 (anti-OOM; 分块可由 fetcher yield 批次实现).
    - 单条 processor 抛异常 → 记入 errors, failed += 1, 继续下一条.

    Example::

        def read_csv_rows():
            with open("rows.csv") as fh:
                for line in fh:
                    yield {"raw": line}

        engine = BulkImportWorkerEngine(
            fetcher=read_csv_rows,
            processor=upsert_product,
            trigger={"on_signal": True},
            config={},
            name="product-import",
        )
        engine.run()  # 非阻塞
        result = asyncio.run(engine.process())
    """

    injection_points: ClassVar[dict] = {
        "fetcher": Injection(
            kind="oprim",
            cardinality="1",
            description="Yields/returns input records, e.g. CSV rows",
        ),
        "processor": Injection(
            kind="omodul",
            cardinality="1",
            description="omodul applied per record/batch",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        fetcher: Callable[..., Any],
        processor: Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.fetcher = fetcher
        self.processor = processor
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    async def _invoke(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        """Sync/async-agnostic callable invocation (keyword args)."""
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        raw = fn(**kwargs)
        if asyncio.iscoroutine(raw):
            return await raw
        return raw

    async def _stream_records(self) -> AsyncIterator[Any]:
        """Lazily stream records from fetcher (anti-OOM).

        Supports sync/async generator functions and sync/async callables
        returning a sync or async iterable.
        """
        fetcher = self.fetcher

        # async generator function → async for directly
        if inspect.isasyncgenfunction(fetcher):
            async for record in fetcher():
                yield record
            return

        # sync generator function → calling produces a lazy generator
        if inspect.isgeneratorfunction(fetcher):
            for record in fetcher():
                yield record
            return

        # plain sync/async callable returning an iterable
        if inspect.iscoroutinefunction(fetcher):
            produced = await fetcher()
        else:
            produced = fetcher()
            if asyncio.iscoroutine(produced):
                produced = await produced

        if hasattr(produced, "__aiter__"):
            async for record in produced:
                yield record
        else:
            for record in produced:
                yield record

    def run(self) -> None:
        """on_signal 引擎: 非阻塞, 仅置位 _running 表示就绪."""
        self._running = True
        logger.info(f"BulkImportWorkerEngine '{self.name}' ready for on_signal dispatch")

    def stop(self) -> None:
        """优雅停止."""
        self._running = False
        self._stop_event.set()

    async def process(
        self,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Stream records from fetcher and apply processor per record.

        Args:
            on_step: Optional callback called per record with
                     {"index": int, "ok": bool}.

        Returns:
            {"status": "completed", "processed": int, "failed": int,
             "errors": [...]}
        """
        processed = 0
        failed = 0
        errors: list[dict[str, Any]] = []

        index = 0
        async for record in self._stream_records():
            try:
                await self._invoke(self.processor, record=record)
                processed += 1
                ok = True
            except Exception as e:
                failed += 1
                ok = False
                self._last_error = (
                    f"record {index}: {type(e).__name__}: {e}"
                )
                logger.warning(
                    f"BulkImportWorkerEngine '{self.name}' record {index} failed: {e}"
                )
                errors.append({"index": index, "error": str(e)})

            if on_step:
                try:
                    on_step({"index": index, "ok": ok})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed at record {index}: {cb_err}")

            index += 1

        return {
            "status": "completed",
            "processed": processed,
            "failed": failed,
            "errors": errors,
        }

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "last_error": self._last_error,
            },
        }


register_skeleton("bulk_import_worker", BulkImportWorkerEngine)
