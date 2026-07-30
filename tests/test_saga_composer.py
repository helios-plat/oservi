"""SagaComposerEngine tests."""

from __future__ import annotations

import asyncio

import pytest

from oservi import ManifestValidationError, ServiceManifest, assemble, list_skeletons
from oservi.engines.saga_composer import SagaComposerEngine


def step_ok(*, input_data, step_no):
    return {"status": "completed", "step_no": step_no}


def step_fail(*, input_data, step_no):
    return {"status": "failed", "step_no": step_no, "error": "boom"}


def step_raises(*, input_data, step_no):
    raise RuntimeError("step raised")


async def async_step_ok(*, input_data, step_no):
    return {"status": "completed", "step_no": step_no}


def make_compensation(recorder):
    def compensation(*, input_data, step_no, step_result):
        recorder.append(step_no)
        return {"status": "completed", "compensated_step": step_no}

    return compensation


def compensation_raises(*, input_data, step_no, step_result):
    raise RuntimeError("compensation raised")


for fn in [step_ok, step_fail, step_raises, compensation_raises]:
    fn.__module__ = "omodul.fake"
async_step_ok.__module__ = "omodul.fake"


def _make_engine(**overrides):
    defaults = dict(
        steps=[step_ok],
        compensations=None,
        trigger={"on_demand": True},
        config={},
        name="test-saga",
    )
    defaults.update(overrides)
    return SagaComposerEngine(**defaults)


class TestSagaComposerRegistration:
    def test_registered(self):
        assert "saga_composer" in list_skeletons()

    def test_injection_points(self):
        pts = SagaComposerEngine.injection_points
        assert pts["steps"].kind == "omodul"
        assert pts["steps"].cardinality == "1..n"
        assert pts["compensations"].kind == "omodul"
        assert pts["compensations"].cardinality == "0..n"

    def test_trigger_mode(self):
        assert SagaComposerEngine.trigger_mode == "on_demand"

    def test_assemble_basic(self):
        m = ServiceManifest(
            name="saga-1",
            skeleton="saga_composer",
            inject={"steps": [step_ok], "compensations": []},
            trigger={"on_demand": True},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, SagaComposerEngine)

    def test_cardinality_1n_steps_enforced(self):
        m = ServiceManifest(
            name="bad",
            skeleton="saga_composer",
            inject={"steps": []},
            trigger={"on_demand": True},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1..n"):
            assemble(m)


class TestSagaComposerForward:
    def test_all_steps_succeed(self):
        engine = _make_engine(steps=[step_ok, step_ok, step_ok])
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "completed"
        assert len(result["results"]) == 3

    def test_single_step_not_list(self):
        engine = _make_engine(steps=step_ok)
        assert len(engine.step_list) == 1

    def test_async_step_supported(self):
        engine = _make_engine(steps=[async_step_ok])
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "completed"

    def test_step_exception_becomes_failed_result(self):
        engine = _make_engine(steps=[step_raises])
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "failed"
        assert result["failed_step"] == 0


class TestSagaComposerCompensation:
    def test_failure_triggers_reverse_compensation(self):
        order = []
        comp0 = make_compensation(order)
        comp1 = make_compensation(order)
        engine = _make_engine(
            steps=[step_ok, step_ok, step_fail],
            compensations=[comp0, comp1],
        )
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "failed"
        assert result["failed_step"] == 2
        # steps 0 and 1 completed before the failure at step 2; compensate them
        # in reverse order (1 then 0).
        assert order == [1, 0]

    def test_missing_compensation_skipped(self):
        # Only one compensation provided for two completed steps — the
        # uncompensated step should just be skipped, not raise.
        order = []
        comp0 = make_compensation(order)
        engine = _make_engine(
            steps=[step_ok, step_ok, step_fail],
            compensations=[comp0],
        )
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "failed"
        assert order == [0]

    def test_no_compensations_at_all(self):
        engine = _make_engine(steps=[step_ok, step_fail], compensations=None)
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "failed"
        assert result["compensated"] == []

    def test_compensation_exception_captured(self):
        engine = _make_engine(steps=[step_ok, step_fail], compensations=[compensation_raises])
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "failed"
        assert result["compensated"][0]["result"]["status"] == "failed"

    def test_first_step_fails_no_compensation_needed(self):
        engine = _make_engine(steps=[step_fail], compensations=[])
        result = asyncio.run(engine.run(input_data={}))
        assert result["status"] == "failed"
        assert result["failed_step"] == 0
        assert result["compensated"] == []


class TestSagaComposerCallback:
    def test_on_step_called_for_forward_and_compensate(self):
        calls = []
        engine = _make_engine(steps=[step_ok, step_fail], compensations=[make_compensation([])])
        asyncio.run(engine.run(input_data={}, on_step=calls.append))
        phases = [c["phase"] for c in calls]
        assert phases == ["forward", "forward", "compensate"]


class TestSagaComposerHealth:
    def test_health_reports_counts(self):
        engine = _make_engine(steps=[step_ok, step_ok], compensations=[make_compensation([])])
        h = engine.health()
        assert h["details"]["steps_count"] == 2
        assert h["details"]["compensations_count"] == 1

    def test_stop_sets_event(self):
        engine = _make_engine()
        engine.stop()
        assert engine._stop_event.is_set()
