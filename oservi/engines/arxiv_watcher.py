"""ArXiv Watcher Engine Skeleton.

机制 (固化):
- on_interval 触发（默认 21600 秒，每 6 小时扫一次）
- 调 arxiv_search oprim 查论文列表
- diff 对比已处理 arxiv_id，找新论文
- 可选 LLM 筛选
- 逐个：http_download_file → ingest_substrate 入库
- 记录已处理 arxiv_id（增量）

业务 (注入):
- search:       oprim (1)    — arxiv_search
- download:     oprim (1)    — http_download_file
- ingest:       omodul (1)   — process_inbox_substrate
- subscription: layer4 (1)   — get_processed_ids / mark_processed
- filter:       oskill (0..1) — LLM 筛选（可选）

红线对照:
- 红线 2: 查询/下载/入库全靠注入
- 红线 3: 5 注入点声明
- 红线 4: 只持运行态
- 红线 5: 不 import 3O 四包
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import tempfile
from pathlib import Path
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class ArxivWatcherEngine(EngineSkeleton):
    """ArXiv Watcher 引擎骨架 (on_interval，默认 21600 秒).

    Example::
        engine = ArxivWatcherEngine(
            search=arxiv_search,
            download=http_download_file,
            ingest=process_inbox_substrate,
            subscription=layer4_arxiv_subscription_store,
            trigger={"on_interval": 21600},
            config={
                "categories": ["q-fin.TR"],
                "keywords": "reinforcement learning",
                "max_results": 20,
                "work_dir": "/tmp/arxiv",
                "rate_limit_sleep": 3.0,
            },
            name="arxiv-watcher",
        )
        engine.run()
    """

    injection_points: ClassVar[dict] = {
        "search": Injection(
            kind="oprim",
            cardinality="1",
            description="论文检索 (arxiv_search)",
        ),
        "download": Injection(
            kind="oprim",
            cardinality="1",
            description="PDF 下载 (http_download_file)",
        ),
        "ingest": Injection(
            kind="omodul",
            cardinality="1",
            description="PDF 入库 (process_inbox_substrate)",
        ),
        "subscription": Injection(
            kind="layer4",
            cardinality="1",
            description="订阅存储：get_processed_ids / mark_processed",
        ),
        "filter": Injection(
            kind="oskill",
            cardinality="0..1",
            description="LLM 论文筛选（可选）",
        ),
    }

    def __init__(
        self,
        *,
        search: Callable,
        download: Callable,
        ingest: Callable,
        subscription: Any,
        filter: Callable | None = None,
        trigger: dict | None = None,
        config: dict | None = None,
        name: str = "arxiv-watcher",
    ) -> None:
        self._search = search
        self._download = download
        self._ingest = ingest
        self._subscription = subscription
        self._filter = filter
        self._trigger = trigger or {"on_interval": 21600}
        self._config = config or {}
        self._name = name
        # 运行态
        self._running = False
        self._tick_count = 0
        self._last_error: str | None = None

    async def _tick(self) -> dict[str, Any]:
        """单次扫描：search → diff → filter → download → ingest。"""
        categories = self._config.get("categories", [])
        keywords = self._config.get("keywords")
        author = self._config.get("author")
        after_date = self._config.get("after_date")
        max_results = self._config.get("max_results", 20)
        work_dir = Path(self._config.get("work_dir", tempfile.gettempdir())) / "arxiv"
        work_dir.mkdir(parents=True, exist_ok=True)
        rate_limit_sleep = self._config.get("rate_limit_sleep", 3.0)
        user_id_hash = self._config.get("user_id_hash", "arxiv_watcher")

        # Step 1: 查论文列表
        try:
            if inspect.iscoroutinefunction(self._search):
                papers = await self._search(
                    categories=categories,
                    keywords=keywords,
                    author=author,
                    after_date=after_date,
                    max_results=max_results,
                    rate_limit_sleep=rate_limit_sleep,
                )
            else:
                papers = self._search(
                    categories=categories,
                    keywords=keywords,
                    author=author,
                    after_date=after_date,
                    max_results=max_results,
                )
        except Exception as exc:
            logger.error("arxiv_watcher.search_failed name=%s error=%s", self._name, exc)
            self._last_error = str(exc)
            return {"ingested": 0, "new_papers": 0, "error": str(exc)}

        # Step 2: diff
        try:
            processed_ids: set[str] = set(
                await self._subscription.get_processed_ids(self._name)
                if inspect.iscoroutinefunction(self._subscription.get_processed_ids)
                else self._subscription.get_processed_ids(self._name)
            )
        except Exception:
            processed_ids = set()

        new_papers = [p for p in papers if p.arxiv_id not in processed_ids]
        if not new_papers:
            logger.info("arxiv_watcher.no_new name=%s", self._name)
            return {"ingested": 0, "new_papers": 0}

        # Step 3: filter（可选 LLM）
        if self._filter is not None:
            llm_filter = self._config.get("llm_filter")
            if llm_filter:
                try:
                    if inspect.iscoroutinefunction(self._filter):
                        new_papers = await self._filter(
                            new_papers, llm_filter=llm_filter, llm=None
                        )
                    else:
                        new_papers = self._filter(
                            new_papers, llm_filter=llm_filter, llm=None
                        )
                except Exception as exc:
                    logger.warning("arxiv_watcher.filter_failed error=%s", exc)

        # Step 4+5: 逐个下载 + 入库
        ingested: list[str] = []
        failed: list[str] = []

        for paper in new_papers:
            pdf_path = work_dir / f"{paper.arxiv_id.replace('/', '_')}.pdf"
            try:
                # 下载 PDF
                if inspect.iscoroutinefunction(self._download):
                    await self._download(url=paper.pdf_url, dest=pdf_path)
                else:
                    await asyncio.to_thread(
                        self._download, url=paper.pdf_url, dest=pdf_path
                    )

                # 入库
                ingest_kwargs = {
                    "file_path": pdf_path,
                    "user_id_hash": user_id_hash,
                    "medium_hint": "pdf",
                    "metadata_override": {
                        "arxiv_id": paper.arxiv_id,
                        "title": paper.title,
                        "authors": ", ".join(paper.authors),
                        "published": paper.published,
                        "categories": ",".join(paper.categories),
                        "source": "arxiv",
                    },
                }
                if inspect.iscoroutinefunction(self._ingest):
                    result = await self._ingest(**ingest_kwargs)
                else:
                    result = await asyncio.to_thread(self._ingest, **ingest_kwargs)

                if isinstance(result, dict) and result.get("status") == "completed":
                    ingested.append(paper.arxiv_id)
                else:
                    failed.append(paper.arxiv_id)

            except Exception as exc:
                logger.error(
                    "arxiv_watcher.ingest_failed arxiv_id=%s error=%s",
                    paper.arxiv_id, exc,
                )
                failed.append(paper.arxiv_id)
            finally:
                # 清理临时 PDF（入库后不保留原文件）
                if pdf_path.exists():
                    pdf_path.unlink(missing_ok=True)

        # Step 6: 记录已处理
        if ingested:
            try:
                if inspect.iscoroutinefunction(self._subscription.mark_processed):
                    await self._subscription.mark_processed(self._name, ingested)
                else:
                    self._subscription.mark_processed(self._name, ingested)
            except Exception as exc:
                logger.error("arxiv_watcher.mark_failed error=%s", exc)

        logger.info(
            "arxiv_watcher.tick name=%s new=%d ingested=%d failed=%d",
            self._name, len(new_papers), len(ingested), len(failed),
        )
        return {
            "new_papers": len(new_papers),
            "ingested": len(ingested),
            "failed": len(failed),
        }

    def run(self) -> None:
        """阻塞运行，on_interval 定时触发。"""
        interval = self._trigger.get("on_interval", 21600)
        self._running = True

        async def _loop() -> None:
            while self._running:
                self._tick_count += 1
                try:
                    await self._tick()
                except Exception as exc:
                    logger.error("arxiv_watcher.loop_error error=%s", exc)
                    self._last_error = str(exc)
                await asyncio.sleep(interval)

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_loop())
        except RuntimeError:
            asyncio.run(_loop())

    def stop(self) -> None:
        self._running = False

    def health(self) -> dict[str, Any]:
        return {
            "name": self._name,
            "status": "running" if self._running else "stopped",
            "tick_count": self._tick_count,
            "last_error": self._last_error,
            "trigger": self._trigger,
        }


register_skeleton("arxiv_watcher", ArxivWatcherEngine)
