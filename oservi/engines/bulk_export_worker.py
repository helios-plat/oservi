"""Bulk Export Worker Engine Skeleton.

机制 (固化):
- on_signal 触发: 收到信号 → 从 fetcher 流式拉记录 → formatter 格式化
  → uploader 分块上传
- async-generator 流式迭代 + 分块上传, 不一次性载入全部记录 (anti-OOM)
- 每条结果通过 on_step 回调可观测

业务 (注入):
- fetcher: oprim callable, 流式产出源记录
- formatter: oskill callable, 把记录/块格式化为输出 bytes/str
- uploader: obase callable, 上传格式化输出 (e.g. obase.fs)

红线对照:
- 红线 2 (机制/业务分离): 抓取/格式化/上传业务全靠注入
- 红线 3 (注入契约): fetcher=oprim(1) / formatter=oskill(1) / uploader=obase(1)
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


class BulkExportWorkerEngine(EngineSkeleton):
    """批量导出 worker 引擎骨架 (on_signal 流式 format + upload, anti-OOM).

    机制:
    - run() 非阻塞: on_signal 引擎只置位 _running 表示就绪.
    - export() 是事件驱动入口: 用 async generator 从 fetcher 流式拉
      记录, 逐条经 formatter 格式化为 bytes/str, 再交 uploader 分块
      上传. 记录惰性 yield, 任意时刻只有当前块在内存 (anti-OOM).
    - uploader 约定: 写入/追加到稳定目标并返回该目标位置 (location);
      引擎取最后一次非 None 返回作为结果 location.

    Example::

        engine = BulkExportWorkerEngine(
            fetcher=stream_products,
            formatter=to_csv_line,
            uploader=obase_fs_append,
            trigger={"on_signal": True},
            config={},
            name="product-export",
        )
        engine.run()  # 非阻塞
        result = asyncio.run(engine.export())
    """

    injection_points: ClassVar[dict] = {
        "fetcher": Injection(
            kind="oprim",
            cardinality="1",
            description="Streams source records",
        ),
        "formatter": Injection(
            kind="oskill",
            cardinality="1",
            description="Formats a record/chunk to output bytes/str",
        ),
        "uploader": Injection(
            kind="obase",
            cardinality="1",
            description="Uploads the formatted output, e.g. obase.fs",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        fetcher: Callable[..., Any],
        formatter: Callable[..., Any],
        uploader: Callable[..., Any],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.fetcher = fetcher
        self.formatter = formatter
        self.uploader = uploader
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

        if inspect.isasyncgenfunction(fetcher):
            async for record in fetcher():
                yield record
            return

        if inspect.isgeneratorfunction(fetcher):
            for record in fetcher():
                yield record
            return

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
        logger.info(f"BulkExportWorkerEngine '{self.name}' ready for on_signal dispatch")

    def stop(self) -> None:
        """优雅停止."""
        self._running = False
        self._stop_event.set()

    async def export(
        self,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Stream records, format via formatter, upload via uploader.

        Args:
            on_step: Optional callback called per record with
                     {"index": int, "exported": bool}.

        Returns:
            {"status": "completed", "exported": int, "location": Any}
        """
        exported = 0
        location: Any = None

        index = 0
        async for record in self._stream_records():
            formatted = await self._invoke(self.formatter, record=record)
            upload_result = await self._invoke(self.uploader, data=formatted)
            if upload_result is not None:
                location = upload_result
            exported += 1

            if on_step:
                try:
                    on_step({"index": index, "exported": True})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed at record {index}: {cb_err}")

            index += 1

        return {
            "status": "completed",
            "exported": exported,
            "location": location,
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


register_skeleton("bulk_export_worker", BulkExportWorkerEngine)
