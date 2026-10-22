"""BulkExportWorkerEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.bulk_export_worker import BulkExportWorkerEngine


# ===== Fake fetchers =====

def sync_gen_fetcher():
    yield {"id": 1}
    yield {"id": 2}
    yield {"id": 3}


async def async_gen_fetcher():
    yield {"id": 1}
    yield {"id": 2}


def sync_list_fetcher():
    return [{"id": 1}, {"id": 2}]


def empty_fetcher():
    return []


# ===== Fake formatters =====

def fmt(*, record):
    return f"line:{record['id']}"


async def async_fmt(*, record):
    return f"aline:{record['id']}"


# ===== Fake uploader =====

def uploader_loc(*, data):
    return "/out/export.csv"


def _make_engine(**overrides):
    defaults = dict(
        fetcher=sync_gen_fetcher,
        formatter=fmt,
        uploader=uploader_loc,
        trigger={"on_signal": True},
        config={},
        name="test-export",
    )
    defaults.update(overrides)
    return BulkExportWorkerEngine(**defaults)


# ===== Tests =====

def test_bulk_export_worker_registered():
    assert "bulk_export_worker" in list_skeletons()


def test_injection_points_keys():
    pts = BulkExportWorkerEngine.injection_points
    assert "fetcher" in pts
    assert "formatter" in pts
    assert "uploader" in pts


def test_injection_point_fetcher_kind():
    pt = BulkExportWorkerEngine.injection_points["fetcher"]
    assert pt.kind == "oprim"
    assert pt.cardinality == "1"


def test_injection_point_formatter_kind():
    pt = BulkExportWorkerEngine.injection_points["formatter"]
    assert pt.kind == "oskill"
    assert pt.cardinality == "1"


def test_injection_point_uploader_kind():
    pt = BulkExportWorkerEngine.injection_points["uploader"]
    assert pt.kind == "obase"
    assert pt.cardinality == "1"


def test_trigger_mode_on_signal():
    assert BulkExportWorkerEngine.trigger_mode == "on_signal"


def test_run_is_non_blocking_and_sets_running():
    engine = _make_engine()
    engine.run()
    assert engine._running is True


def test_export_sync_generator_fetcher():
    engine = _make_engine(fetcher=sync_gen_fetcher)
    result = asyncio.run(engine.export())
    assert result["status"] == "completed"
    assert result["exported"] == 3
    assert result["location"] == "/out/export.csv"


def test_export_async_generator_fetcher():
    engine = _make_engine(fetcher=async_gen_fetcher)
    result = asyncio.run(engine.export())
    assert result["exported"] == 2


def test_export_sync_list_fetcher():
    engine = _make_engine(fetcher=sync_list_fetcher)
    result = asyncio.run(engine.export())
    assert result["exported"] == 2


def test_export_async_formatter():
    engine = _make_engine(fetcher=sync_list_fetcher, formatter=async_fmt)
    result = asyncio.run(engine.export())
    assert result["exported"] == 2


def test_export_streams_formatted_chunks_to_uploader():
    uploaded = []

    def cap_uploader(*, data):
        uploaded.append(data)
        return "/out/x.csv"

    engine = _make_engine(fetcher=sync_gen_fetcher, formatter=fmt, uploader=cap_uploader)
    result = asyncio.run(engine.export())
    assert uploaded == ["line:1", "line:2", "line:3"]
    assert result["exported"] == 3
    assert result["location"] == "/out/x.csv"


def test_export_empty_fetcher():
    engine = _make_engine(fetcher=empty_fetcher)
    result = asyncio.run(engine.export())
    assert result["exported"] == 0
    assert result["location"] is None


def test_on_step_callback_per_record():
    called = []
    engine = _make_engine(fetcher=sync_gen_fetcher)
    asyncio.run(engine.export(on_step=called.append))
    assert len(called) == 3
    assert [c["index"] for c in called] == [0, 1, 2]
    assert all(c["exported"] for c in called)


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
