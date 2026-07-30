"""CronSchedulerEngine tests.

The persistent run() loop sleeps until the next cron fire, so these tests
exercise the mechanism directly via `_fire_once` (same pattern as
test_alerter.py's `_iterate_once`) rather than waiting out a real schedule.
"""

from __future__ import annotations

import asyncio

import pytest

from oservi import ManifestValidationError, ServiceManifest, assemble, list_skeletons
from oservi.engines.cron_scheduler_engine import CronSchedulerEngine


def make_task(recorder):
    def task():
        recorder.append(1)

    task.__module__ = "omodul.fake"
    return task


def make_async_task(recorder):
    async def task():
        recorder.append("async")

    task.__module__ = "omodul.fake"
    return task


def task_raises():
    raise RuntimeError("task failed")


task_raises.__module__ = "omodul.fake"


def _make_engine(**overrides):
    recorder: list = []
    defaults = dict(
        tasks=[make_task(recorder)],
        trigger={"on_cron": "0 3 * * *"},
        config={},
        name="test-cron",
    )
    defaults.update(overrides)
    return CronSchedulerEngine(**defaults), recorder


class TestCronSchedulerRegistration:
    def test_registered(self):
        assert "cron_scheduler_engine" in list_skeletons()

    def test_injection_points(self):
        pts = CronSchedulerEngine.injection_points
        assert pts["tasks"].kind == "omodul"
        assert pts["tasks"].cardinality == "1..n"

    def test_trigger_mode(self):
        assert CronSchedulerEngine.trigger_mode == "on_cron"

    def test_assemble_basic(self):
        recorder: list = []
        m = ServiceManifest(
            name="cron-1",
            skeleton="cron_scheduler_engine",
            inject={"tasks": [make_task(recorder)]},
            trigger={"on_cron": "*/5 * * * *"},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, CronSchedulerEngine)

    def test_missing_on_cron_raises(self):
        with pytest.raises(ValueError, match="on_cron"):
            CronSchedulerEngine(tasks=[lambda: None], trigger={}, config={}, name="bad")

    def test_invalid_cron_expr_raises(self):
        with pytest.raises(Exception):  # noqa: B017 — croniter's own parse error
            CronSchedulerEngine(
                tasks=[lambda: None],
                trigger={"on_cron": "not a cron expr"},
                config={},
                name="bad",
            )

    def test_cardinality_1n_enforced(self):
        m = ServiceManifest(
            name="bad",
            skeleton="cron_scheduler_engine",
            inject={"tasks": []},
            trigger={"on_cron": "0 3 * * *"},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1..n"):
            assemble(m)


class TestCronSchedulerFire:
    def test_fire_once_calls_task(self):
        engine, recorder = _make_engine()
        asyncio.run(engine._fire_once())
        assert recorder == [1]
        assert engine.health()["details"]["fire_count"] == 1

    def test_fire_once_calls_multiple_tasks(self):
        recorder: list = []
        engine, _ = _make_engine(tasks=[make_task(recorder), make_task(recorder)])
        asyncio.run(engine._fire_once())
        assert recorder == [1, 1]

    def test_async_task_supported(self):
        recorder: list = []
        engine, _ = _make_engine(tasks=[make_async_task(recorder)])
        asyncio.run(engine._fire_once())
        assert recorder == ["async"]

    def test_task_exception_does_not_block_others(self):
        recorder: list = []
        engine, _ = _make_engine(tasks=[task_raises, make_task(recorder)])
        asyncio.run(engine._fire_once())
        assert recorder == [1]
        assert engine.health()["details"]["last_error"] is not None

    def test_single_task_not_list(self):
        engine = CronSchedulerEngine(
            tasks=lambda: None,
            trigger={"on_cron": "0 3 * * *"},
            config={},
            name="t",
        )
        assert len(engine.task_list) == 1


class TestCronSchedulerHealth:
    def test_health_initial(self):
        engine, _ = _make_engine()
        h = engine.health()
        assert h["status"] == "stopped"
        assert h["details"]["cron_expr"] == "0 3 * * *"
        assert h["details"]["fire_count"] == 0

    def test_stop_sets_running_false(self):
        engine, _ = _make_engine()
        engine._running = True
        engine.stop()
        assert engine._running is False
