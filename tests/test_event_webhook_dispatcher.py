"""EventWebhookDispatcherEngine tests.

The persistent run()/subscribe() loop needs a real Redis EventBus, so most
tests exercise the dispatch mechanism directly via `_on_event` (same pattern
as test_alerter.py calling `_iterate_once`/`_dispatch_to_channels` directly).
One end-to-end integration test (real Redis, auto-skips if unavailable)
verifies the full run()/EventBus.subscribe() wiring, not just `_on_event`.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from oservi import ManifestValidationError, ServiceManifest, assemble, list_skeletons
from oservi.engines.event_webhook_dispatcher import EventWebhookDispatcherEngine


def make_subscriber(recorder):
    def subscriber(*, event):
        recorder.append(event)
        return {"ok": True}

    subscriber.__module__ = "omodul.fake"
    return subscriber


def make_async_subscriber(recorder):
    async def subscriber(*, event):
        recorder.append(event)
        return {"ok": True}

    subscriber.__module__ = "omodul.fake"
    return subscriber


def subscriber_raises(*, event):
    raise RuntimeError("subscriber failed")


subscriber_raises.__module__ = "omodul.fake"


def _make_engine(**overrides):
    recorder: list = []
    defaults = dict(
        subscribers=[make_subscriber(recorder)],
        trigger={"on_signal": "order.placed"},
        config={},
        name="test-dispatcher",
    )
    defaults.update(overrides)
    return EventWebhookDispatcherEngine(**defaults), recorder


class TestEventWebhookDispatcherRegistration:
    def test_registered(self):
        assert "event_webhook_dispatcher" in list_skeletons()

    def test_injection_points(self):
        pts = EventWebhookDispatcherEngine.injection_points
        assert pts["subscribers"].kind == "omodul"
        assert pts["subscribers"].cardinality == "1..n"

    def test_trigger_mode(self):
        assert EventWebhookDispatcherEngine.trigger_mode == "on_signal"

    def test_assemble_basic(self):
        recorder: list = []
        m = ServiceManifest(
            name="dispatcher-1",
            skeleton="event_webhook_dispatcher",
            inject={"subscribers": [make_subscriber(recorder)]},
            trigger={"on_signal": "order.placed"},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, EventWebhookDispatcherEngine)

    def test_missing_on_signal_raises(self):
        with pytest.raises(ValueError, match="on_signal"):
            EventWebhookDispatcherEngine(
                subscribers=[lambda *, event: None],
                trigger={},
                config={},
                name="bad",
            )

    def test_cardinality_1n_enforced(self):
        m = ServiceManifest(
            name="bad",
            skeleton="event_webhook_dispatcher",
            inject={"subscribers": []},
            trigger={"on_signal": "x"},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1..n"):
            assemble(m)


class TestEventWebhookDispatcherDispatch:
    def test_single_subscriber_called(self):
        engine, recorder = _make_engine()
        asyncio.run(engine._on_event({"order_id": "o1"}))
        assert recorder == [{"order_id": "o1"}]
        assert engine.health()["details"]["dispatch_count"] == 1

    def test_multiple_subscribers_all_called(self):
        recorder1: list = []
        recorder2: list = []
        engine, _ = _make_engine(
            subscribers=[make_subscriber(recorder1), make_subscriber(recorder2)]
        )
        asyncio.run(engine._on_event({"order_id": "o1"}))
        assert recorder1 == [{"order_id": "o1"}]
        assert recorder2 == [{"order_id": "o1"}]

    def test_async_subscriber_supported(self):
        recorder: list = []
        engine, _ = _make_engine(subscribers=[make_async_subscriber(recorder)])
        asyncio.run(engine._on_event({"x": 1}))
        assert recorder == [{"x": 1}]

    def test_one_subscriber_failure_does_not_block_others(self):
        recorder: list = []
        engine, _ = _make_engine(subscribers=[subscriber_raises, make_subscriber(recorder)])
        asyncio.run(engine._on_event({"x": 1}))
        assert recorder == [{"x": 1}]
        assert engine.health()["details"]["last_error"] is not None

    def test_single_subscriber_not_list(self):
        engine = EventWebhookDispatcherEngine(
            subscribers=lambda *, event: None,
            trigger={"on_signal": "t"},
            config={},
            name="t",
        )
        assert len(engine.subscriber_list) == 1


class TestEventWebhookDispatcherHealth:
    def test_health_initial_stopped(self):
        engine, _ = _make_engine()
        h = engine.health()
        assert h["status"] == "stopped"
        assert h["details"]["topic"] == "order.placed"
        assert h["details"]["subscribers_count"] == 1

    def test_stop_sets_running_false(self):
        engine, _ = _make_engine()
        engine._running = True
        engine.stop()
        assert engine._running is False


class TestEventWebhookDispatcherIntegration:
    """Real Redis EventBus, exercising the full run()/subscribe() loop end to
    end rather than just the internal _on_event method. Auto-skips if Redis
    is unavailable at TEST_REDIS_URL."""

    @pytest.fixture(autouse=True)
    async def _require_redis(self):
        try:
            import redis.asyncio as redis_lib
        except ImportError:
            pytest.skip("redis package not installed")

        url = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/0")
        client = redis_lib.Redis.from_url(url)
        try:
            await client.ping()
        except Exception:
            pytest.skip("Redis not available at TEST_REDIS_URL")
        finally:
            await client.aclose()
        self.redis_url = url

    async def test_run_receives_published_event_end_to_end(self):
        from obase.mq import EventBus

        topic = "it:event_webhook_dispatcher:test"
        recorder: list = []
        engine = EventWebhookDispatcherEngine(
            subscribers=[make_subscriber(recorder)],
            trigger={"on_signal": topic},
            config={"redis_url": self.redis_url, "poll_timeout_seconds": 1.0},
            name="it-dispatcher",
        )

        run_task = asyncio.get_event_loop().run_in_executor(None, engine.run)
        await asyncio.sleep(0.3)  # let the subscription register with Redis

        publisher = EventBus(redis_url=self.redis_url)
        await publisher.publish(topic, {"order_id": "it-o1"})
        await publisher.close()

        await asyncio.sleep(0.5)  # let the event be consumed
        engine.stop()
        await asyncio.wait_for(run_task, timeout=5.0)

        assert recorder == [{"order_id": "it-o1"}]
