"""WAVE G gates (oservi): SourceWatcherEngine changed-file events."""

from __future__ import annotations

from oservi.engines.source_watcher import SourceWatcherEngine


def _make_engine(**overrides):
    defaults = dict(
        searchers={},
        download=lambda *a, **k: {},
        ingest=lambda *a, **k: {"status": "completed"},
        subscription=object(),
        trigger={"on_interval": 3600},
        config={},
        name="test-watcher",
    )
    defaults.update(overrides)
    return SourceWatcherEngine(**defaults)


def test_emit_add_modify_delete(tmp_path):
    engine = _make_engine()
    (tmp_path / "a.py").write_text("x = 1\n")
    first = engine.emit_file_changes(root=tmp_path)
    assert {e["kind"] for e in first} == {"added"}

    (tmp_path / "a.py").write_text("x = 2\n")
    (tmp_path / "b.py").write_text("y = 1\n")
    second = engine.emit_file_changes(root=tmp_path)
    kinds = sorted(e["kind"] for e in second)
    assert kinds == ["added", "modified"]

    (tmp_path / "b.py").unlink()
    third = engine.emit_file_changes(root=tmp_path)
    assert [e["kind"] for e in third] == ["deleted"]


def test_emit_rename(tmp_path):
    engine = _make_engine()
    (tmp_path / "old.py").write_text("x = 1\n")
    engine.emit_file_changes(root=tmp_path)
    (tmp_path / "old.py").rename(tmp_path / "new.py")
    events = engine.emit_file_changes(root=tmp_path)
    assert len(events) == 1
    assert events[0]["kind"] == "renamed"
    assert events[0]["old_path"].endswith("old.py")
    assert events[0]["path"].endswith("new.py")


def test_emit_no_changes_stable(tmp_path):
    engine = _make_engine()
    (tmp_path / "a.py").write_text("x = 1\n")
    engine.emit_file_changes(root=tmp_path)
    assert engine.emit_file_changes(root=tmp_path) == []
