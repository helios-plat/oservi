"""Bulk Export Worker Engine Skeleton.

机制 (固化):
- 通过 obase.mq.EventBus 订阅 trigger["on_signal"] 指定的 topic,收到一次
  信号即跑一次导出
- fetcher 返回的行迭代器逐行消费,每行经 formatter 转成一行文本,增量写入
  本地临时文件(不在内存里攒整份内容——"流式导出防 OOM"),取完后一次性
  交给 uploader 上传,再清理临时文件
- 单行格式化失败只计入 error_count,不中断整个流

业务 (注入):
- fetcher: oprim callable(接收 signal payload,返回/yield 行 dict)
- formatter: oskill 纯函数(单行 dict → 一行文本,不做 IO)
- uploader: obase callable(签名同 obase.fs.FileStorage.upload:
  `upload(*, local_path, key) -> str`,把生成好的临时文件上传到目标存储)

红线对照:
- 红线 2 (机制/业务分离): 数据源/格式/存储目的地全靠注入
- 红线 3 (注入契约): fetcher=oprim(1) / formatter=oskill(1) / uploader=obase(1)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Callable, ClassVar

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)


async def _as_async_iterable(rows: Any) -> AsyncIterator[Any]:
    """Normalize a sync or async iterable/generator into an async iterator."""
    if hasattr(rows, "__aiter__"):
        async for row in rows:
            yield row
    else:
        for row in rows:
            yield row


class BulkExportWorkerEngine(EngineSkeleton):
    """批量导出引擎骨架:流式取行 + 格式化 + 上传,防 OOM。

    Example::

        engine = BulkExportWorkerEngine(
            fetcher=stream_orders,
            formatter=order_to_csv_line,
            uploader=s3_storage.upload,
            trigger={"on_signal": "bulk_export.requested"},
            config={"redis_url": "redis://localhost:6379/0"},
            name="orders-csv-exporter",
        )
        engine.run()  # blocking
    """

    injection_points: ClassVar[dict] = {
        "fetcher": Injection(
            kind="oprim",
            cardinality="1",
            description="Streams row dicts (sync or async iterable) given the signal payload",
        ),
        "formatter": Injection(
            kind="oskill",
            cardinality="1",
            description="Pure function: row dict -> one line of export text",
        ),
        "uploader": Injection(
            kind="obase",
            cardinality="1",
            description="upload(*, local_path, key) -> str, e.g. obase.fs.FileStorage.upload",
        ),
    }
    trigger_mode: str = "on_signal"

    def __init__(
        self,
        *,
        fetcher: Callable[..., Any] | list[Callable[..., Any]],
        formatter: Callable[..., Any] | list[Callable[..., Any]],
        uploader: Callable[..., Any] | list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.fetcher = fetcher[0] if isinstance(fetcher, list) else fetcher
        self.formatter = formatter[0] if isinstance(formatter, list) else formatter
        self.uploader = uploader[0] if isinstance(uploader, list) else uploader
        if "on_signal" not in trigger:
            raise ValueError("BulkExportWorkerEngine trigger must contain 'on_signal'")
        self.topic = trigger["on_signal"]
        self.trigger = trigger
        self.config = config

        self._running = False
        self._jobs_processed = 0
        self._last_row_count = 0
        self._last_error_count = 0
        self._last_upload_key: str | None = None
        self._last_error: str | None = None

    def run(self) -> None:
        """启动持续运行循环 (阻塞)."""
        if self._running:
            raise RuntimeError(f"BulkExportWorkerEngine {self.name} already running")
        self._running = True
        logger.info(f"BulkExportWorkerEngine '{self.name}' starting on topic {self.topic!r}")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"BulkExportWorkerEngine '{self.name}' stopped")

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
                    logger.warning(f"BulkExportWorkerEngine '{self.name}' subscribe error: {e}")
                    await asyncio.sleep(1.0)
        finally:
            await bus.close()

    async def _on_signal(self, payload: dict[str, Any]) -> None:
        row_count = 0
        error_count = 0
        upload_key = payload.get("upload_key", f"{self.name}-export.txt")

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", delete=False, encoding="utf-8")
        tmp_path = Path(tmp.name)
        try:
            rows = self.fetcher(signal=payload)
            if asyncio.iscoroutine(rows):
                rows = await rows

            async for row in _as_async_iterable(rows):
                row_count += 1
                try:
                    line = self.formatter(row=row)
                except Exception as e:
                    error_count += 1
                    self._last_error = f"row {row_count}: {e}"
                    logger.warning(
                        f"BulkExportWorkerEngine '{self.name}' row {row_count} failed: {e}"
                    )
                    continue
                tmp.write(line if line.endswith("\n") else line + "\n")
            tmp.close()

            result = self.uploader(local_path=tmp_path, key=upload_key)
            if asyncio.iscoroutine(result):
                await result
        finally:
            tmp_path.unlink(missing_ok=True)

        self._jobs_processed += 1
        self._last_row_count = row_count
        self._last_error_count = error_count
        self._last_upload_key = upload_key
        logger.info(
            f"BulkExportWorkerEngine '{self.name}' finished job: "
            f"{row_count} rows, {error_count} errors, uploaded to {upload_key!r}"
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
                "last_upload_key": self._last_upload_key,
                "last_error": self._last_error,
            },
        }


register_skeleton("bulk_export_worker", BulkExportWorkerEngine)
