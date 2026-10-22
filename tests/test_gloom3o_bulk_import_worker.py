"""BulkImportWorkerEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.bulk_import_worker import BulkImportWorkerEngine


# ===== Fake fetchers (multiple supported shapes) =====

def sync_gen_fetcher():
    yield {"id": 1}
    yield {"id": 2}
    yield {"id": 3}


async def async_gen_fetcher():
    yield {"id": 1}
    yield {"id": 2}


def sync_list_fetcher():
    return [{"id": 1}, {"id": 2}]


async def async_list_fetcher():
    return [{"id": 1}]


def empty_fetcher():
    return []


# ===== Fake processors =====

def proc_ok(*, record):
    return record


async def async_proc_ok(*, record):
    return record


def proc_raises_on_2(*, record):
    if record["id"] == 2:
        raise ValueError("bad record")
    return record


def _make_engine(**overrides):
    defaults = dict(
        fetcher=sync_gen_fetcher,
        processor=proc_ok,
        trigger={"on_signal": True},
        config={},
        name="test-import",
    )
    defaults.update(overrides)
    return BulkImportWorkerEngine(**defaults)


# ===== Tests =====

def test_bulk_import_worker_registered():
    assert "bulk_import_worker" in list_skeletons()


def test_injection_points_keys():
    pts = BulkImportWorkerEngine.injection_points
    assert "fetcher" in pts
    assert "processor" in pts


def test_injection_point_fetcher_kind():
    pt = BulkImportWorkerEngine.injection_points["fetcher"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_processor_kind():
    pt = BulkImportWorkerEngine.injection_points["processor"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1"


def test_trigger_mode_on_signal():
    assert BulkImportWorkerEngine.trigger_mode == "on_signal"


def test_run_is_non_blocking_and_sets_running():
    engine = _make_engine()
    engine.run()
    assert engine._running is True


def test_process_sync_generator_fetcher():
    engine = _make_engine(fetcher=sync_gen_fetcher)
    result = asyncio.run(engine.process())
    assert result["status"] == "completed"
    assert result["processed"] == 3
    assert result["failed"] == 0


def test_process_async_generator_fetcher():
    engine = _make_engine(fetcher=async_gen_fetcher)
    result = asyncio.run(engine.process())
    assert result["processed"] == 2


def test_process_sync_list_fetcher():
    engine = _make_engine(fetcher=sync_list_fetcher)
    result = asyncio.run(engine.process())
    assert result["processed"] == 2


def test_process_async_list_fetcher():
    engine = _make_engine(fetcher=async_list_fetcher)
    result = asyncio.run(engine.process())
    assert result["processed"] == 1


def test_process_async_processor():
    engine = _make_engine(fetcher=sync_list_fetcher, processor=async_proc_ok)
    result = asyncio.run(engine.process())
    assert result["processed"] == 2


def test_process_failing_processor_captured_not_raised():
    engine = _make_engine(fetcher=sync_gen_fetcher, processor=proc_raises_on_2)
    result = asyncio.run(engine.process())
    assert result["status"] == "completed"
    assert result["processed"] == 2  # ids 1 and 3
    assert result["failed"] == 1  # id 2
    assert len(result["errors"]) == 1
    assert result["errors"][0]["index"] == 1


def test_process_empty_fetcher():
    engine = _make_engine(fetcher=empty_fetcher)
    result = asyncio.run(engine.process())
    assert result["processed"] == 0
    assert result["failed"] == 0


def test_on_step_callback_per_record():
    called = []
    engine = _make_engine(fetcher=sync_gen_fetcher)
    asyncio.run(engine.process(on_step=called.append))
    assert len(called) == 3
    assert [c["index"] for c in called] == [0, 1, 2]
    assert all(c["ok"] for c in called)


def test_health_reports_state():
    engine = _make_engine()
    engine.run()
    h = engine.health()
    assert h["status"] == "healthy"
    assert h["details"]["running"] is True


def test_stop_sets_event():
    engine = _make_engine()
    engine.stop()
    assert engine._stop_event.is_set()
    assert engine._running is False
