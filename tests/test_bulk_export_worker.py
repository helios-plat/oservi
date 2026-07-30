"""BulkExportWorkerEngine tests.

The persistent run()/subscribe() loop needs a real Redis EventBus, so these
tests exercise the streaming mechanism directly via `_on_signal`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from oservi import ServiceManifest, assemble, list_skeletons
from oservi.engines.bulk_export_worker import BulkExportWorkerEngine


def sync_fetcher(*, signal):
    return [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]


def empty_fetcher(*, signal):
    return []


def csv_formatter(*, row):
    return f"{row['id']},{row['name']}"


def formatter_fails_on_id_2(*, row):
    if row["id"] == 2:
        raise ValueError("bad row")
    return f"{row['id']},{row['name']}"


for fn in [sync_fetcher, empty_fetcher]:
    fn.__module__ = "oprim.fake"
for fn in [csv_formatter, formatter_fails_on_id_2]:
    fn.__module__ = "oskill.fake"


class _Uploader:
    def __init__(self):
        self.calls: list[dict] = []

    def record(self, *, local_path, key):
        self.calls.append({"local_path": local_path, "key": key, "content": local_path.read_text()})
        return key


def make_uploader():
    """Returns (uploader_recorder, upload_callable). The callable is a plain
    function (not a bound method) so it can carry a fake __module__ for the
    assembler's kind detection."""
    recorder = _Uploader()

    def upload(*, local_path, key):
        return recorder.record(local_path=local_path, key=key)

    upload.__module__ = "obase.fake"
    recorder.upload = upload
    return recorder


def _make_engine(**overrides):
    uploader = make_uploader()
    defaults = dict(
        fetcher=sync_fetcher,
        formatter=csv_formatter,
        uploader=uploader.upload,
        trigger={"on_signal": "bulk_export.requested"},
        config={},
        name="test-exporter",
    )
    defaults.update(overrides)
    return BulkExportWorkerEngine(**defaults), uploader


class TestBulkExportWorkerRegistration:
    def test_registered(self):
        assert "bulk_export_worker" in list_skeletons()

    def test_injection_points(self):
        pts = BulkExportWorkerEngine.injection_points
        assert pts["fetcher"].kind == "oprim"
        assert pts["formatter"].kind == "oskill"
        assert pts["uploader"].kind == "obase"
        assert pts["fetcher"].cardinality == "1"
        assert pts["formatter"].cardinality == "1"
        assert pts["uploader"].cardinality == "1"

    def test_trigger_mode(self):
        assert BulkExportWorkerEngine.trigger_mode == "on_signal"

    def test_assemble_basic(self):
        uploader = make_uploader()
        m = ServiceManifest(
            name="exporter-1",
            skeleton="bulk_export_worker",
            inject={
                "fetcher": sync_fetcher,
                "formatter": csv_formatter,
                "uploader": uploader.upload,
            },
            trigger={"on_signal": "bulk_export.requested"},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, BulkExportWorkerEngine)

    def test_missing_on_signal_raises(self):
        uploader = make_uploader()
        with pytest.raises(ValueError, match="on_signal"):
            BulkExportWorkerEngine(
                fetcher=sync_fetcher,
                formatter=csv_formatter,
                uploader=uploader.upload,
                trigger={},
                config={},
                name="bad",
            )


class TestBulkExportWorkerStreaming:
    def test_rows_formatted_and_uploaded(self):
        engine, uploader = _make_engine()
        asyncio.run(engine._on_signal({}))
        assert len(uploader.calls) == 1
        content = uploader.calls[0]["content"]
        assert content == "1,a\n2,b\n"

        h = engine.health()
        assert h["details"]["last_row_count"] == 2
        assert h["details"]["last_error_count"] == 0
        assert h["details"]["jobs_processed"] == 1

    def test_temp_file_cleaned_up_after_upload(self):
        engine, uploader = _make_engine()
        asyncio.run(engine._on_signal({}))
        uploaded_path: Path = uploader.calls[0]["local_path"]
        assert not uploaded_path.exists()

    def test_custom_upload_key_from_payload(self):
        engine, uploader = _make_engine()
        asyncio.run(engine._on_signal({"upload_key": "custom/export.csv"}))
        assert uploader.calls[0]["key"] == "custom/export.csv"

    def test_default_upload_key_when_absent(self):
        engine, uploader = _make_engine()
        asyncio.run(engine._on_signal({}))
        assert uploader.calls[0]["key"] == "test-exporter-export.txt"

    def test_formatter_error_counted_row_skipped(self):
        engine, uploader = _make_engine(formatter=formatter_fails_on_id_2)
        asyncio.run(engine._on_signal({}))
        content = uploader.calls[0]["content"]
        assert content == "1,a\n"  # row 2 skipped due to formatter error
        h = engine.health()
        assert h["details"]["last_row_count"] == 2
        assert h["details"]["last_error_count"] == 1

    def test_empty_rows_still_uploads_empty_file(self):
        engine, uploader = _make_engine(fetcher=empty_fetcher)
        asyncio.run(engine._on_signal({}))
        assert uploader.calls[0]["content"] == ""


class TestBulkExportWorkerHealth:
    def test_health_initial(self):
        engine, _ = _make_engine()
        h = engine.health()
        assert h["status"] == "stopped"
        assert h["details"]["topic"] == "bulk_export.requested"
        assert h["details"]["jobs_processed"] == 0

    def test_stop_sets_running_false(self):
        engine, _ = _make_engine()
        engine._running = True
        engine.stop()
        assert engine._running is False
