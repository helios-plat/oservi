"""Source Watcher Engine Skeleton — 通用源订阅（arxiv / gutenberg / oapen / ...）。

机制 (固化):
- on_interval 触发（按源频率：arxiv 6h，gutenberg/oapen 30d）
- 按 source_type 调对应 search oprim
- diff 对比已处理 external_id
- 可选 LLM 筛选
- 逐个：http_download_file → process_inbox_substrate
- 记录已处理 external_id（增量）

业务 (注入):
- searchers:    dict[str, oprim] (1+) — {source_type: search_fn}
- download:     oprim (1)            — http_download_file
- ingest:       omodul (1)           — process_inbox_substrate
- subscription: layer4 (1)           — get_processed_ids / mark_processed
- filter:       oskill (0..1)        — LLM 筛选（可选）
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import tempfile
from pathlib import Path
from typing import Any, Callable, ClassVar

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)


class SourceWatcherEngine(EngineSkeleton):
    """通用源订阅引擎骨架。

    Example::
        engine = SourceWatcherEngine(
            searchers={"arxiv": arxiv_search, "gutenberg": gutenberg_search},
            download=http_download_file,
            ingest=process_inbox_substrate,
            subscription=layer4_store,
            trigger={"on_interval": 21600},
            config={"source_type": "arxiv", "query": {"categories": ["q-fin.TR"]}},
            name="arxiv-q-fin",
        )
    """

    injection_points: ClassVar[dict] = {
        "searchers": Injection(kind="oprim", cardinality="1+",
                               description="源检索函数字典 {source_type: search_fn}"),
        "download":  Injection(kind="oprim", cardinality="1",
                               description="文件下载 (http_download_file)"),
        "ingest":    Injection(kind="omodul", cardinality="1",
                               description="文件入库 (process_inbox_substrate)"),
        "subscription": Injection(kind="layer4", cardinality="1",
                                  description="get_processed_ids / mark_processed"),
        "filter":    Injection(kind="oskill", cardinality="0..1",
                               description="LLM 筛选（可选）"),
    }

    def __init__(self, *, searchers, download, ingest, subscription,
                 filter=None, trigger=None, config=None, name="source-watcher"):
        self._searchers = searchers
        self._download = download
        self._ingest = ingest
        self._subscription = subscription
        self._filter = filter
        self._trigger = trigger or {"on_interval": 21600}
        self._config = config or {}
        self._name = name
        self._running = False
        self._tick_count = 0
        self._last_error: str | None = None

    async def _tick(self) -> dict[str, Any]:
        source_type = self._config.get("source_type", "arxiv")
        query       = self._config.get("query", {})
        max_results = self._config.get("max_results", 20)
        work_dir    = Path(self._config.get("work_dir", tempfile.gettempdir())) / "source_watcher"
        work_dir.mkdir(parents=True, exist_ok=True)
        user_id_hash = self._config.get("user_id_hash", "source_watcher")
        force_ipv4   = self._config.get("force_ipv4", False)

        search_fn = self._searchers.get(source_type)
        if not search_fn:
            return {"ingested": 0, "new_items": 0, "error": f"unknown source_type: {source_type}"}

        try:
            items = await search_fn(max_results=max_results, **query) \
                if inspect.iscoroutinefunction(search_fn) \
                else search_fn(max_results=max_results, **query)
        except Exception as exc:
            self._last_error = str(exc)
            return {"ingested": 0, "new_items": 0, "error": str(exc)}

        try:
            processed_ids = set(
                await self._subscription.get_processed_ids(self._name)
                if inspect.iscoroutinefunction(self._subscription.get_processed_ids)
                else self._subscription.get_processed_ids(self._name)
            )
        except Exception:
            processed_ids = set()

        new_items = [it for it in items if it.external_id not in processed_ids]
        if not new_items:
            return {"ingested": 0, "new_items": 0}

        if self._filter and self._config.get("llm_filter"):
            try:
                new_items = await self._filter(new_items, llm_filter=self._config["llm_filter"]) \
                    if inspect.iscoroutinefunction(self._filter) \
                    else self._filter(new_items, llm_filter=self._config["llm_filter"])
            except Exception:
                pass

        ingested, failed = [], []
        for item in new_items:
            ext  = {"pdf": ".pdf", "epub": ".epub", "txt": ".txt"}.get(item.file_type, ".bin")
            dest = work_dir / f"{item.external_id.replace('/', '_')}{ext}"
            try:
                dl_kw = {"force_ipv4": force_ipv4} if force_ipv4 else {}
                if inspect.iscoroutinefunction(self._download):
                    await self._download(item.download_url, dest, **dl_kw)
                else:
                    await asyncio.to_thread(self._download, item.download_url, dest, **dl_kw)

                kw = {"file_path": dest, "user_id_hash": user_id_hash,
                      "medium_hint": item.file_type,
                      "metadata_override": {"external_id": item.external_id,
                                            "source": source_type, **item.metadata}}
                result = await self._ingest(**kw) \
                    if inspect.iscoroutinefunction(self._ingest) \
                    else await asyncio.to_thread(self._ingest, **kw)

                (ingested if isinstance(result, dict) and result.get("status") == "completed"
                 else failed).append(item.external_id)
            except Exception as exc:
                logger.error("source_watcher.ingest_failed id=%s error=%s", item.external_id, exc)
                failed.append(item.external_id)
            finally:
                dest.unlink(missing_ok=True)

        if ingested:
            try:
                if inspect.iscoroutinefunction(self._subscription.mark_processed):
                    await self._subscription.mark_processed(self._name, ingested)
                else:
                    self._subscription.mark_processed(self._name, ingested)
            except Exception as exc:
                logger.error("source_watcher.mark_failed error=%s", exc)

        return {"new_items": len(new_items), "ingested": len(ingested), "failed": len(failed)}

    def run(self) -> None:
        interval = self._trigger.get("on_interval", 21600)
        self._running = True
        async def _loop():
            while self._running:
                self._tick_count += 1
                try:
                    await self._tick()
                except Exception as exc:
                    self._last_error = str(exc)
                await asyncio.sleep(interval)
        try:
            asyncio.get_running_loop().create_task(_loop())
        except RuntimeError:
            asyncio.run(_loop())

    def stop(self): self._running = False

    def health(self) -> dict[str, Any]:
        return {"name": self._name,
                "status": "running" if self._running else "stopped",
                "tick_count": self._tick_count,
                "last_error": self._last_error,
                "trigger": self._trigger}


register_skeleton("source_watcher", SourceWatcherEngine)
