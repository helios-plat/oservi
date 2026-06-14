"""Researcher + FeedTracker 引擎测试."""

import pytest
from unittest.mock import MagicMock, AsyncMock
from oservi import (
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
fake_llm_caller.__module__ = "oprim.llm_call"  # v1.0 §3.2 单 LLM = oprim


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
fake_subscription_query.__module__ = "stratum.services.feed_subscriptions"


async def fake_subscription_update(**kwargs):
    return {"status": "completed"}
fake_subscription_update.__module__ = "stratum.services.feed_subscriptions"


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
        bad_llm.__module__ = "oprim.llm_call"
        
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

def _make_ft_manifest(name: str, fetch_fn=None, sub_fn=None, ingest_fn=None) -> ServiceManifest:
    """Helper: build FeedTracker manifest with new 3-injection-point API."""
    fetch_fn = fetch_fn or fake_fetch_event
    sub_fn = sub_fn or fake_subscription_store
    inject: dict = {
        "fetch_event": [fetch_fn],
        "subscription": [sub_fn],
    }
    if ingest_fn is not None:
        inject["ingest"] = [ingest_fn]
    return ServiceManifest(
        name=name,
        skeleton="feed_tracker",
        inject=inject,
        trigger={"on_interval": 5},
        config={},
    )


async def fake_fetch_event(*, config=None):
    return [
        {"id": "e1", "title": "Entry 1", "url": "https://feed.example.com/e1"},
        {"id": "e2", "title": "Entry 2", "url": "https://feed.example.com/e2"},
    ]
fake_fetch_event.__module__ = "oprim.fetch_event"


async def fake_subscription_store(*, event=None):
    return {"status": "ok"}
fake_subscription_store.__module__ = "stratum.services.feed_subscriptions"


class TestFeedTrackerRegistration:
    def test_feed_tracker_registered(self):
        assert "feed_tracker" in list_skeletons()

    def test_feed_tracker_assemble_basic(self):
        service = assemble(_make_ft_manifest("ft-1"))
        assert isinstance(service, FeedTrackerEngine)

    def test_feed_tracker_missing_required_injection_raises(self):
        m = ServiceManifest(
            name="bad",
            skeleton="feed_tracker",
            inject={
                # subscription (required, cardinality=1) missing → ManifestValidationError
                "fetch_event": [fake_fetch_event],
            },
            trigger={"on_interval": 5},
            config={},
        )
        with pytest.raises(ManifestValidationError):
            assemble(m)


class TestFeedTrackerTick:
    @pytest.mark.asyncio
    async def test_tick_returns_dict(self):
        engine = assemble(_make_ft_manifest("ft-tick"))
        result = await engine.tick()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_tick_events_fetched(self):
        engine = assemble(_make_ft_manifest("ft-count"))
        result = await engine.tick()
        assert result["events_fetched"] == 2

    @pytest.mark.asyncio
    async def test_tick_events_processed(self):
        engine = assemble(_make_ft_manifest("ft-proc"))
        result = await engine.tick()
        assert result["events_processed"] == 2

    @pytest.mark.asyncio
    async def test_tick_empty_events(self):
        """fetch_event returns empty list → 0 processed."""
        async def empty_fetch(*, config=None):
            return []
        empty_fetch.__module__ = "oprim.fetch_event"
        engine = assemble(_make_ft_manifest("ft-empty", fetch_fn=empty_fetch))
        result = await engine.tick()
        assert result["events_fetched"] == 0
        assert result["events_processed"] == 0

    @pytest.mark.asyncio
    async def test_tick_fetch_error_does_not_raise(self):
        """fetch_event error → tick returns dict (no raise)."""
        async def bad_fetch(*, config=None):
            raise RuntimeError("fetch error")
        bad_fetch.__module__ = "oprim.fetch_event"
        engine = assemble(_make_ft_manifest("ft-err", fetch_fn=bad_fetch))
        result = await engine.tick()
        assert isinstance(result, dict)
        assert result["events_fetched"] == 0

    @pytest.mark.asyncio
    async def test_tick_ingest_called(self):
        ingested = []
        async def ingest_fn(*, event=None):
            ingested.append(event)
        ingest_fn.__module__ = "omodul.ingest"
        engine = assemble(_make_ft_manifest("ft-ingest", ingest_fn=ingest_fn))
        await engine.tick()
        assert len(ingested) == 2

    @pytest.mark.asyncio
    async def test_tick_without_ingest(self):
        """ingest=None is ok (0..1 cardinality)."""
        engine = assemble(_make_ft_manifest("ft-no-ingest"))
        result = await engine.tick()
        assert result["events_processed"] == 2

    @pytest.mark.asyncio
    async def test_tick_count_increments(self):
        engine = assemble(_make_ft_manifest("ft-tc"))
        await engine.tick()
        await engine.tick()
        assert engine._tick_count == 2

    def test_feed_tracker_health(self):
        engine = assemble(_make_ft_manifest("ft-h"))
        h = engine.health()
        assert isinstance(h, dict)
        assert "details" in h

    def test_feed_tracker_health_tick_count(self):
        engine = assemble(_make_ft_manifest("ft-hc"))
        h = engine.health()
        assert h["details"]["tick_count"] == 0
