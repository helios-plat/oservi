"""Researcher + FeedTracker 引擎测试."""

import pytest
from unittest.mock import MagicMock, AsyncMock
from oservice import (
    ServiceManifest,
    ManifestValidationError,
    ResearcherEngine,
    FeedTrackerEngine,
    assemble,
    list_skeletons,
)


# ===== Fake oprim/obase/omodul (设置 __module__ 让 oservice 识别) =====

async def fake_searxng_search(*, query, max_results=10):
    return [
        {"url": f"https://a.example.com/{query}", "title": f"A {query}", "snippet": "..."},
        {"url": f"https://b.example.com/{query}", "title": f"B {query}", "snippet": "..."},
    ]
fake_searxng_search.__module__ = "oprim.searxng_search"


async def fake_url_fetch(*, url):
    return f"<html>content of {url}</html>"
fake_url_fetch.__module__ = "oprim.url_fetch_ssrf_safe"


async def fake_llm_caller(*, messages, max_tokens=512):
    """模拟 LLM 返回 JSON array of terms."""
    return {"content": '["quantum computing", "quantum supremacy", "qubit"]'}
fake_llm_caller.__module__ = "obase.providers.llm"


async def fake_ingest_omodul(*, content, source_url=None, tags=None, user_id=None, title=None):
    return {
        "findings": {"substrate_id": f"sub_{hash(source_url) & 0xFFFF:04x}"},
        "status": "completed",
        "error": None,
    }
fake_ingest_omodul.__module__ = "omodul.process_inbox_substrate"


# Feed tracker fakes
async def fake_fetch_rss_feed(*, url, etag=None, last_modified=None):
    if etag == "304-etag":
        return {"status": 304}
    return {
        "status": 200,
        "etag": "new-etag",
        "last_modified": "Wed, 04 Jun 2026 12:00:00 GMT",
        "entries": [
            {"id": "e1", "title": "Entry 1", "link": "https://feed.example.com/e1", "content": "C1"},
            {"id": "e2", "title": "Entry 2", "link": "https://feed.example.com/e2", "content": "C2"},
        ],
    }
fake_fetch_rss_feed.__module__ = "oprim.fetch_rss_feed"


async def fake_diff_detector(*, current_entries, previous_entry_ids):
    new = [e for e in current_entries if e.get("id") not in previous_entry_ids]
    return {"new_entries": new}
fake_diff_detector.__module__ = "oprim.feed_diff_detector"


async def fake_subscription_query(*, last_check_before=None, status=None):
    return {
        "findings": {
            "subscriptions": [
                {
                    "id": "sub_1",
                    "user_id": "user_a",
                    "url": "https://feed.example.com/rss",
                    "feed_type": "rss",
                    "etag": None,
                    "last_modified": None,
                    "previous_entry_ids": ["e0"],  # e1, e2 都是新的
                },
            ],
        },
    }
fake_subscription_query.__module__ = "omodul.query_subscriptions"


async def fake_subscription_update(**kwargs):
    return {"status": "completed"}
fake_subscription_update.__module__ = "omodul.update_subscription"


# ===== Researcher 测试 =====

class TestResearcherRegistration:
    def test_researcher_registered(self):
        assert "researcher" in list_skeletons()
    
    def test_researcher_assemble_basic(self):
        m = ServiceManifest(
            name="test-researcher",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [fake_llm_caller],
            },
            trigger={"on_demand": True},
            config={"max_total_articles": 5},
        )
        service = assemble(m)
        assert isinstance(service, ResearcherEngine)
        assert service.name == "test-researcher"
        assert len(service.search_oprim) == 1
        assert service.ingest_omodul_fn is None  # 0..1 cardinality, 未注入 OK
    
    def test_researcher_with_ingest(self):
        m = ServiceManifest(
            name="test-r-with-ingest",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [fake_llm_caller],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_demand": True},
            config={},
        )
        service = assemble(m)
        assert service.ingest_omodul_fn is not None
    
    def test_researcher_fetch_oprim_cardinality_1(self):
        """fetch_oprim 是 cardinality=1, 不能给 2 个."""
        m = ServiceManifest(
            name="bad",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch, fake_url_fetch],  # 错
                "llm_caller": [fake_llm_caller],
            },
            trigger={"on_demand": True},
            config={},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1"):
            assemble(m)


class TestResearcherWorkflow:
    @pytest.mark.asyncio
    async def test_research_end_to_end(self):
        m = ServiceManifest(
            name="r1",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [fake_llm_caller],
            },
            trigger={"on_demand": True},
            config={"max_total_articles": 5, "max_search_terms": 3},
        )
        engine = assemble(m)
        result = await engine.research(query="quantum")
        
        assert result["status"] == "completed"
        assert result["query"] == "quantum"
        assert len(result["search_terms"]) == 3
        assert len(result["articles"]) > 0
        assert all("content" in a for a in result["articles"])
    
    @pytest.mark.asyncio
    async def test_research_dedupe_urls(self):
        """同 URL 跨多搜索词应去重."""
        m = ServiceManifest(
            name="r-dedupe",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [fake_llm_caller],
            },
            trigger={"on_demand": True},
            config={"max_total_articles": 20},
        )
        engine = assemble(m)
        # LLM 返 3 terms, 每 term 2 URL, 但 URL 用 term 拼成 → 6 unique
        result = await engine.research(query="test")
        assert len(result["articles"]) == 6
    
    @pytest.mark.asyncio
    async def test_research_with_ingest(self):
        m = ServiceManifest(
            name="r-ingest",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [fake_llm_caller],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_demand": True},
            config={},
        )
        engine = assemble(m)
        result = await engine.research(query="x", user_id="u1")
        assert len(result["ingested_substrate_ids"]) > 0
    
    @pytest.mark.asyncio
    async def test_research_llm_failure_fallback(self):
        """LLM 失败时降级用原 query 作搜索词."""
        async def bad_llm(*, messages, max_tokens=512):
            raise RuntimeError("LLM down")
        bad_llm.__module__ = "obase.providers.llm"
        
        m = ServiceManifest(
            name="r-bad-llm",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [bad_llm],
            },
            trigger={"on_demand": True},
            config={},
        )
        engine = assemble(m)
        result = await engine.research(query="x")
        # LLM 失败 → fallback 用 query 作唯一 term
        assert result["status"] == "completed"
        assert result["search_terms"] == ["x"]
    
    def test_researcher_health(self):
        m = ServiceManifest(
            name="r-h",
            skeleton="researcher",
            inject={
                "search_oprim": [fake_searxng_search],
                "fetch_oprim": [fake_url_fetch],
                "llm_caller": [fake_llm_caller],
            },
            trigger={"on_demand": True},
            config={},
        )
        engine = assemble(m)
        h = engine.health()
        assert h["details"]["search_providers_count"] == 1
        assert h["details"]["ingest_enabled"] is False


# ===== FeedTracker 测试 =====

class TestFeedTrackerRegistration:
    def test_feed_tracker_registered(self):
        assert "feed_tracker" in list_skeletons()
    
    def test_feed_tracker_assemble_basic(self):
        m = ServiceManifest(
            name="ft-1",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fake_fetch_rss_feed],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [fake_subscription_query],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_interval": 3600},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, FeedTrackerEngine)
    
    def test_feed_tracker_no_trigger_raises(self):
        m = ServiceManifest(
            name="bad",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fake_fetch_rss_feed],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [fake_subscription_query],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={},  # 无 on_cron / on_interval
            config={},
        )
        with pytest.raises(ValueError, match="on_cron"):
            assemble(m)


class TestFeedTrackerTick:
    @pytest.mark.asyncio
    async def test_tick_processes_new_entries(self):
        m = ServiceManifest(
            name="ft-tick",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fake_fetch_rss_feed],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [fake_subscription_query],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_interval": 3600},
            config={},
        )
        engine = assemble(m)
        result = await engine.tick()
        # 1 订阅, e1+e2 新增 → 2 落库
        assert result["feeds_checked"] == 1
        assert result["new_entries"] == 2
    
    @pytest.mark.asyncio
    async def test_tick_handles_304(self):
        """304 应跳过, 不调 diff/ingest."""
        async def fetch_304(*, url, etag=None, last_modified=None):
            return {"status": 304}
        fetch_304.__module__ = "oprim.fetch_rss_feed"
        
        # 订阅含 etag → fetch 返 304
        async def query_with_etag(**kwargs):
            return {"findings": {"subscriptions": [{
                "id": "sub_1", "user_id": "u", "url": "x", "feed_type": "rss",
                "etag": "304-etag", "previous_entry_ids": [],
            }]}}
        query_with_etag.__module__ = "omodul.query_subscriptions"
        
        m = ServiceManifest(
            name="ft-304",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fetch_304],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [query_with_etag],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_interval": 3600},
            config={},
        )
        engine = assemble(m)
        result = await engine.tick()
        assert result["new_entries"] == 0
    
    @pytest.mark.asyncio
    async def test_tick_empty_subscriptions(self):
        """无订阅时直接返."""
        async def empty_query(**kwargs):
            return {"findings": {"subscriptions": []}}
        empty_query.__module__ = "omodul.query_subscriptions"
        
        m = ServiceManifest(
            name="ft-empty",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fake_fetch_rss_feed],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [empty_query],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_interval": 3600},
            config={},
        )
        engine = assemble(m)
        result = await engine.tick()
        assert result["feeds_checked"] == 0
        assert result["new_entries"] == 0
    
    @pytest.mark.asyncio
    async def test_tick_isolation_on_fetch_error(self):
        """单个订阅 fetch 失败不阻塞 tick 整体."""
        async def fetch_fails(*, url, etag=None, last_modified=None):
            raise RuntimeError("fetch error")
        fetch_fails.__module__ = "oprim.fetch_rss_feed"
        
        m = ServiceManifest(
            name="ft-err",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fetch_fails],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [fake_subscription_query],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_interval": 3600},
            config={},
        )
        engine = assemble(m)
        result = await engine.tick()
        # 不 raise, fetch_failed sub 返 new_entries=0 (但 feeds_checked 仍计)
        # 这里测的是 process_subscription 内捕获 fetch 错, 不影响整体
        assert isinstance(result, dict)
    
    def test_feed_tracker_health(self):
        m = ServiceManifest(
            name="ft-h",
            skeleton="feed_tracker",
            inject={
                "fetch_feed_oprim": [fake_fetch_rss_feed],
                "diff_oprim": [fake_diff_detector],
                "subscription_query_omodul": [fake_subscription_query],
                "subscription_update_omodul": [fake_subscription_update],
                "ingest_omodul": [fake_ingest_omodul],
            },
            trigger={"on_interval": 3600},
            config={},
        )
        engine = assemble(m)
        h = engine.health()
        assert h["details"]["fetch_providers_count"] == 1
        assert h["details"]["tick_count"] == 0
