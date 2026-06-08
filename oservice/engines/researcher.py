"""Researcher Engine Skeleton.

收敛设计 (Stratum ResearcherAgent + 工业成熟模式):
- LLM rewrite query → 多搜索词
- 并发 web search (注入 search oprim)
- 并发 URL fetch (注入 fetch oprim, 安全检索)
- URL canonical 去重
- 落库 (注入 ingest_omodul)

机制 (固化):
- 持续运行 (on-demand 或 cron 触发)
- 多步检索流水线
- 并发控制 + 异常隔离
- 去重 + 限流

业务 (注入):
- search_oprim (oprim, 1..n): SearxNG / Bing / Google CSE 等
- fetch_oprim (oprim, 1): URL 抓取 (含 SSRF 防护)
- llm_caller (oprim, 1): LLM caller oprim (v1.0 §3.2 形态 3: 单 LLM = oprim)
- ingest_omodul (omodul, 0..1): 主库 omodul 落库 (无则不落, 只返回结果)

红线对照:
- 红线 1 (≥2 实证): Stratum ResearcherAgent + 工业模式 (Perplexity / GPT Researcher 等同模式)
- 红线 2 (机制/业务分离): 业务全靠注入, 骨架不含具体 search / LLM 实现
- 红线 3 (注入契约): search_oprim=oprim(1..n) / fetch_oprim=oprim(1) / llm_caller=obase(1) / ingest_omodul=omodul(0..1)
- 红线 4 (无状态骨架): 状态只在 ResearcherEngine 实例
"""

import asyncio
import logging
from typing import Callable, Any
from urllib.parse import urlparse, urlunparse

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class ResearcherEngine(EngineSkeleton):
    """Researcher 引擎骨架.
    
    机制 (骨架固化):
    - LLM rewrite query → N 搜索词
    - 并发调用 search_oprim 收集 URL
    - URL canonical 去重
    - 并发调用 fetch_oprim 抓 HTML
    - (可选) 调 ingest_omodul 落库
    - 返回结构化 {query, search_terms, articles, ingested_ids}
    
    Example:
        manifest = ServiceManifest(
            name="stratum-researcher",
            skeleton="researcher",
            inject={
                "search_oprim": [oprim.searxng_search],
                "fetch_oprim": [oprim.url_fetch_ssrf_safe],
                "llm_caller": [obase.ProviderRegistry.get("llm", "default")],
                "ingest_omodul": [omodul.process_inbox_substrate],  # 可选
            },
            trigger={"on_demand": True},
            config={
                "max_search_terms": 5,
                "max_articles_per_term": 5,
                "max_total_articles": 20,
                "fetch_concurrency": 5,
            },
        )
        engine = assemble(manifest)
        result = await engine.research(query="量子计算最新进展", user_id="user_123")
    """
    
    injection_points = {
        "search_oprim": Injection(
            kind="oprim",
            cardinality="1..n",
            description="web 搜索 oprim (searxng / Bing / Google CSE 等)",
        ),
        "fetch_oprim": Injection(
            kind="oprim",
            cardinality="1",
            description="URL 抓取 oprim (含 SSRF 防护, e.g. url_fetch_ssrf_safe)",
        ),
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM caller oprim (v1.0 §3.2 形态 3: 单 LLM = oprim, e.g. oprim.llm_call)",
        ),
        "ingest_omodul": Injection(
            kind="omodul",
            cardinality="0..1",
            description="(可选) 落库 omodul (e.g. process_inbox_substrate). 缺则不落库",
        ),
    }
    
    def __init__(
        self,
        *,
        search_oprim: list[Callable[..., Any]],
        fetch_oprim: list[Callable[..., Any]],
        llm_caller: list[Callable[..., Any]],
        ingest_omodul: list[Callable[..., Any]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.search_oprim = search_oprim
        self.fetch_oprim_fn = fetch_oprim[0]  # cardinality=1
        self.llm_caller = llm_caller[0]       # cardinality=1
        self.ingest_omodul_fn = ingest_omodul[0] if ingest_omodul else None
        self.trigger = trigger
        self.config = config
        
        self._running = False
        self._research_count = 0
        self._last_error: str | None = None
    
    # ===== 引擎生命周期 =====
    
    def run(self) -> None:
        """Researcher 引擎默认是 on-demand, run() 仅标 running=True.
        
        服务层通过 research() 方法主动触发. 如要 cron 触发, 用 trigger.on_cron + 外部调度.
        """
        self._running = True
        logger.info(f"ResearcherEngine '{self.name}' ready")
    
    def stop(self) -> None:
        self._running = False
    
    # ===== 核心方法 =====
    
    async def research(
        self,
        *,
        query: str,
        user_id: str | None = None,
        max_articles_override: int | None = None,
    ) -> dict[str, Any]:
        """执行一次 research workflow.
        
        Args:
            query: 用户查询
            user_id: 用户 id (落库用, 可选)
            max_articles_override: 临时覆盖 config.max_total_articles
        
        Returns:
            {
                "query": str,
                "search_terms": list[str],
                "articles": list[{"url", "title", "snippet", "content"}],
                "ingested_substrate_ids": list[str],  # ingest_omodul 启用时
                "status": "completed" | "partial" | "failed",
                "error": str | None,
            }
        """
        max_total = max_articles_override or self.config.get("max_total_articles", 20)
        max_search_terms = self.config.get("max_search_terms", 5)
        max_per_term = self.config.get("max_articles_per_term", 5)
        
        try:
            # 1. LLM rewrite query → 搜索词列表
            search_terms = await self._llm_rewrite(query, max_search_terms)
            
            if not search_terms:
                return {
                    "query": query,
                    "search_terms": [],
                    "articles": [],
                    "ingested_substrate_ids": [],
                    "status": "partial",
                    "error": "llm_rewrite_returned_empty",
                }
            
            # 2. 并发搜索
            unique_articles = await self._search_and_dedupe(
                search_terms, max_per_term, max_total
            )
            
            # 3. 并发 fetch HTML
            articles_with_content = await self._fetch_articles(unique_articles)
            
            # 4. (可选) 落库
            ingested_ids = []
            if self.ingest_omodul_fn and user_id:
                ingested_ids = await self._ingest_articles(
                    articles_with_content, query, user_id
                )
            
            self._research_count += 1
            
            return {
                "query": query,
                "search_terms": search_terms,
                "articles": articles_with_content,
                "ingested_substrate_ids": ingested_ids,
                "status": "completed",
                "error": None,
            }
        
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            logger.exception(f"ResearcherEngine '{self.name}' research failed")
            return {
                "query": query,
                "search_terms": [],
                "articles": [],
                "ingested_substrate_ids": [],
                "status": "failed",
                "error": str(e),
            }
    
    # ===== 内部方法 =====
    
    async def _llm_rewrite(self, query: str, max_terms: int) -> list[str]:
        """LLM rewrite query → 搜索词列表."""
        prompt = f"""Rewrite this query into {max_terms} concise search terms for web search.

Query: {query}

Return ONLY a JSON array of strings, no other text.
Example: ["term 1", "term 2", "term 3"]"""
        
        try:
            resp = await self._call_llm(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=512,
            )
            return self._parse_search_terms(resp, max_terms)
        except Exception as e:
            logger.warning(f"LLM rewrite failed: {e}")
            return [query]  # fallback: 用原 query 作为唯一搜索词
    
    async def _call_llm(self, **kwargs) -> Any:
        """统一 LLM 调用接口 (sync/async 都支持)."""
        result = self.llm_caller(**kwargs)
        if asyncio.iscoroutine(result):
            result = await result
        return result
    
    def _parse_search_terms(self, resp: Any, max_terms: int) -> list[str]:
        """解析 LLM 响应到搜索词列表."""
        import json
        try:
            content = resp.get("content", "") if isinstance(resp, dict) else str(resp)
            # 尝试提取 JSON array (LLM 可能返回有 markdown 包装)
            content = content.strip()
            if "```" in content:
                content = content.split("```")[1].lstrip("json\n").rstrip("`\n")
            terms = json.loads(content)
            if isinstance(terms, list):
                return [str(t).strip() for t in terms if t][:max_terms]
        except (json.JSONDecodeError, AttributeError, IndexError):
            pass
        return []
    
    async def _search_and_dedupe(
        self,
        search_terms: list[str],
        max_per_term: int,
        max_total: int,
    ) -> list[dict[str, Any]]:
        """并发调所有 search_oprim, 跨搜索词去重."""
        all_results: list[list[dict]] = []
        
        # 对每个搜索词, 并发调所有 search_oprim
        for term in search_terms:
            term_results = await asyncio.gather(
                *[
                    self._call_search_oprim(oprim_fn, term, max_per_term)
                    for oprim_fn in self.search_oprim
                ],
                return_exceptions=True,
            )
            
            for r in term_results:
                if isinstance(r, Exception):
                    logger.warning(f"search oprim failed for term '{term}': {r}")
                    continue
                all_results.append(r)
        
        # URL canonical 去重
        seen_urls: set[str] = set()
        unique: list[dict[str, Any]] = []
        for results in all_results:
            for article in results:
                canonical = self._canonicalize_url(article.get("url", ""))
                if not canonical or canonical in seen_urls:
                    continue
                seen_urls.add(canonical)
                unique.append(article)
                if len(unique) >= max_total:
                    return unique
        return unique
    
    async def _call_search_oprim(
        self, oprim_fn: Callable, query: str, max_results: int
    ) -> list[dict[str, Any]]:
        """调单个 search oprim (sync/async 都支持)."""
        result = oprim_fn(query=query, max_results=max_results)
        if asyncio.iscoroutine(result):
            result = await result
        return result if isinstance(result, list) else []
    
    def _canonicalize_url(self, url: str) -> str:
        """URL canonicalization for dedup."""
        if not url:
            return ""
        try:
            p = urlparse(url)
            return urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", "", ""))
        except Exception:
            return url
    
    async def _fetch_articles(
        self, articles: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """并发调 fetch_oprim 抓 HTML."""
        concurrency = self.config.get("fetch_concurrency", 5)
        semaphore = asyncio.Semaphore(concurrency)
        
        async def fetch_one(article: dict) -> dict | None:
            async with semaphore:
                try:
                    result = self.fetch_oprim_fn(url=article["url"])
                    if asyncio.iscoroutine(result):
                        result = await result
                    return {**article, "content": result}
                except Exception as e:
                    logger.warning(f"fetch failed {article.get('url')}: {e}")
                    return None
        
        results = await asyncio.gather(*[fetch_one(a) for a in articles])
        return [r for r in results if r is not None]
    
    async def _ingest_articles(
        self,
        articles: list[dict[str, Any]],
        query: str,
        user_id: str,
    ) -> list[str]:
        """调 ingest_omodul 落库 (每篇 article 一个 substrate)."""
        ingested = []
        for article in articles:
            try:
                result = self.ingest_omodul_fn(
                    content=article.get("content", ""),
                    source_url=article.get("url"),
                    tags=[f"research:{hash(query) & 0xFFFFFFFF:08x}"],
                    user_id=user_id,
                )
                if asyncio.iscoroutine(result):
                    result = await result
                # omodul 返 dict, 提取 substrate_id (findings 字段)
                if isinstance(result, dict):
                    sub_id = result.get("findings", {}).get("substrate_id") if isinstance(result.get("findings"), dict) else None
                    if sub_id:
                        ingested.append(sub_id)
            except Exception as e:
                logger.warning(f"ingest failed for {article.get('url')}: {e}")
        return ingested
    
    # ===== 健康检查 =====
    
    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "research_count": self._research_count,
                "search_providers_count": len(self.search_oprim),
                "ingest_enabled": self.ingest_omodul_fn is not None,
                "last_error": self._last_error,
            },
        }


# 注册
register_skeleton("researcher", ResearcherEngine)
