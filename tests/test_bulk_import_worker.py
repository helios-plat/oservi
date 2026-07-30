"""BulkImportWorkerEngine tests.

The persistent run()/subscribe() loop needs a real Redis EventBus, so these
tests exercise the streaming mechanism directly via `_on_signal`.
"""

from __future__ import annotations

import asyncio

import pytest

from oservi import ManifestValidationError, ServiceManifest, assemble, list_skeletons
from oservi.engines.bulk_import_worker import BulkImportWorkerEngine


def sync_fetcher(*, signal):
    return [{"row": 1}, {"row": 2}, {"row": 3}]


async def async_fetcher(*, signal):
    async def gen():
        for i in range(3):
            yield {"row": i}

    return gen()


def empty_fetcher(*, signal):
    return []


def make_processor(recorder):
    def processor(*, row):
        recorder.append(row)

    processor.__module__ = "omodul.fake"
    return processor


def processor_fails_on_row_2(*, row):
    if row["row"] == 2:
        raise RuntimeError("bad row")


for fn in [sync_fetcher, async_fetcher, empty_fetcher]:
    fn.__module__ = "oprim.fake"
processor_fails_on_row_2.__module__ = "omodul.fake"


def _make_engine(**overrides):
    recorder: list = []
    defaults = dict(
        fetcher=sync_fetcher,
        processor=make_processor(recorder),
        trigger={"on_signal": "bulk_import.requested"},
        config={},
        name="test-importer",
    )
    defaults.update(overrides)
    return BulkImportWorkerEngine(**defaults), recorder


class TestBulkImportWorkerRegistration:
    def test_registered(self):
        assert "bulk_import_worker" in list_skeletons()

    def test_injection_points(self):
        pts = BulkImportWorkerEngine.injection_points
        assert pts["fetcher"].kind == "oprim"
        assert pts["fetcher"].cardinality == "1"
        assert pts["processor"].kind == "omodul"
        assert pts["processor"].cardinality == "1"

    def test_trigger_mode(self):
        assert BulkImportWorkerEngine.trigger_mode == "on_signal"

    def test_assemble_basic(self):
        recorder: list = []
        m = ServiceManifest(
            name="importer-1",
            skeleton="bulk_import_worker",
            inject={"fetcher": sync_fetcher, "processor": make_processor(recorder)},
            trigger={"on_signal": "bulk_import.requested"},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, BulkImportWorkerEngine)

    def test_missing_on_signal_raises(self):
        with pytest.raises(ValueError, match="on_signal"):
            BulkImportWorkerEngine(
                fetcher=sync_fetcher,
                processor=lambda *, row: None,
                trigger={},
                config={},
                name="bad",
            )

    def test_cardinality_1_enforced(self):
        m = ServiceManifest(
            name="bad",
            skeleton="bulk_import_worker",
            inject={"fetcher": [sync_fetcher, sync_fetcher], "processor": lambda *, row: None},
            trigger={"on_signal": "t"},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1"):
            assemble(m)


class TestBulkImportWorkerStreaming:
    def test_sync_iterable_rows_processed(self):
        engine, recorder = _make_engine()
        asyncio.run(engine._on_signal({}))
        assert recorder == [{"row": 1}, {"row": 2}, {"row": 3}]
        h = engine.health()
        assert h["details"]["last_row_count"] == 3
        assert h["details"]["last_error_count"] == 0
        assert h["details"]["jobs_processed"] == 1

    def test_async_iterable_rows_processed(self):
        recorder: list = []
        engine, _ = _make_engine(fetcher=async_fetcher, processor=make_processor(recorder))
        asyncio.run(engine._on_signal({}))
        assert recorder == [{"row": 0}, {"row": 1}, {"row": 2}]

    def test_empty_rows(self):
        engine, recorder = _make_engine(fetcher=empty_fetcher)
        asyncio.run(engine._on_signal({}))
        assert recorder == []
        assert engine.health()["details"]["last_row_count"] == 0

    def test_row_processing_error_counted_not_fatal(self):
        engine, _ = _make_engine(processor=processor_fails_on_row_2)
        asyncio.run(engine._on_signal({}))
        h = engine.health()
        assert h["details"]["last_row_count"] == 3
        assert h["details"]["last_error_count"] == 1
        assert h["details"]["last_error"] is not None

    def test_fetcher_single_not_list(self):
        engine, _ = _make_engine()
        assert engine.fetcher is sync_fetcher


class TestBulkImportWorkerHealth:
    def test_health_initial(self):
        engine, _ = _make_engine()
        h = engine.health()
        assert h["status"] == "stopped"
        assert h["details"]["topic"] == "bulk_import.requested"
        assert h["details"]["jobs_processed"] == 0

    def test_stop_sets_running_false(self):
        engine, _ = _make_engine()
        engine._running = True
        engine.stop()
        assert engine._running is False
