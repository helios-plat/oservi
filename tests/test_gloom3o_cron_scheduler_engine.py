"""CronSchedulerEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

from oservi import list_skeletons
from oservi.engines.cron_scheduler_engine import CronSchedulerEngine


# ===== Fake tasks =====

def task_ok(*, tick_no, task_no):
    return {"tick": tick_no, "task": task_no}


async def async_task_ok(*, tick_no, task_no):
    return {"async": True, "task": task_no}


def task_raises(*, tick_no, task_no):
    raise ValueError("task error")


def _make_engine(**overrides):
    defaults = dict(
        tasks=[task_ok],
        trigger={"on_cron": "0 3 * * *"},
        config={},
        name="test-cron",
    )
    defaults.update(overrides)
    return CronSchedulerEngine(**defaults)


# ===== Tests =====

def test_cron_scheduler_engine_registered():
    assert "cron_scheduler_engine" in list_skeletons()


def test_injection_points_keys():
    pts = CronSchedulerEngine.injection_points
    assert "tasks" in pts


def test_injection_point_tasks_kind():
    pt = CronSchedulerEngine.injection_points["tasks"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1..n"


def test_trigger_mode_on_cron():
    assert CronSchedulerEngine.trigger_mode == "on_cron"


def test_instantiates_with_single_task():
    engine = _make_engine(tasks=task_ok)  # single callable, not list
    assert len(engine.task_list) == 1


def test_run_once_runs_all_tasks_sequentially():
    engine = _make_engine(tasks=[task_ok, task_ok, task_ok])
    results = asyncio.run(engine.run_once())
    assert len(results) == 3
    assert [r["task"] for r in results] == [0, 1, 2]


def test_run_once_async_task():
    engine = _make_engine(tasks=[async_task_ok])
    results = asyncio.run(engine.run_once())
    assert results[0]["async"] is True


def test_run_once_mixed_sync_async():
    engine = _make_engine(tasks=[task_ok, async_task_ok])
    results = asyncio.run(engine.run_once())
    assert len(results) == 2


def test_run_once_increments_tick_count():
    engine = _make_engine(tasks=[task_ok])
    asyncio.run(engine.run_once())
    asyncio.run(engine.run_once())
    assert engine._tick_count == 2


def test_run_once_task_error_captured_not_raised():
    engine = _make_engine(tasks=[task_raises, task_ok])
    results = asyncio.run(engine.run_once())
    assert "error" in results[0]
    assert results[1]["task"] == 1  # second task still ran


def test_run_loop_stops_on_stop_event():
    engine = _make_engine(tasks=[task_ok], config={"interval_seconds": 0.01})
    t = threading.Thread(target=engine.run)
    t.start()
    time.sleep(0.05)
    engine.stop()
    t.join(timeout=2)
    assert not t.is_alive()
    assert engine._tick_count >= 1


def test_run_raises_if_already_running():
    engine = _make_engine()
    engine._running = True
    with pytest.raises(RuntimeError):
        engine.run()


def test_resolve_interval_from_config():
    engine = _make_engine(config={"interval_seconds": 42})
    assert engine._resolve_interval() == 42.0


def test_health_reports_running_and_tick_count():
    engine = _make_engine(tasks=[task_ok, task_ok])
    asyncio.run(engine.run_once())
    h = engine.health()
    assert h["status"] == "stopped"  # not running the loop
    assert h["details"]["tick_count"] == 1
    assert h["details"]["tasks_count"] == 2


def test_stop_sets_event():
    engine = _make_engine()
    engine.stop()
    assert engine._stop_event.is_set()
    assert engine._running is False
