"""StateMachineEngine tests."""

from __future__ import annotations

import asyncio

import pytest

from oservi import ManifestValidationError, ServiceManifest, assemble, list_skeletons
from oservi.engines.state_machine_engine import StateMachineEngine


def mark_paid(*, from_status, to_status, input_data):
    return {"marked": True}


def cancel(*, from_status, to_status, input_data):
    return {"canceled": True}


async def async_transition(*, from_status, to_status, input_data):
    return {"async": True}


def transition_raises(*, from_status, to_status, input_data):
    raise RuntimeError("transition failed")


def validator_allow(*, from_status, to_status, input_data):
    return True


def validator_deny(*, from_status, to_status, input_data):
    return False


def validator_raises(*, from_status, to_status, input_data):
    raise RuntimeError("validator error")


for fn in [mark_paid, cancel, transition_raises]:
    fn.__module__ = "omodul.fake"
async_transition.__module__ = "omodul.fake"
for fn in [validator_allow, validator_deny, validator_raises]:
    fn.__module__ = "oskill.fake"


_MAP = {"draft->pending": "mark_paid", "pending->canceled": "cancel"}


def _make_engine(**overrides):
    defaults = dict(
        transitions=[mark_paid, cancel],
        validators=None,
        trigger={"on_demand": True},
        config={"transition_map": dict(_MAP)},
        name="test-fsm",
    )
    defaults.update(overrides)
    return StateMachineEngine(**defaults)


class TestStateMachineRegistration:
    def test_registered(self):
        assert "state_machine_engine" in list_skeletons()

    def test_injection_points(self):
        pts = StateMachineEngine.injection_points
        assert pts["transitions"].kind == "omodul"
        assert pts["transitions"].cardinality == "1..n"
        assert pts["validators"].kind == "oskill"
        assert pts["validators"].cardinality == "0..n"

    def test_trigger_mode(self):
        assert StateMachineEngine.trigger_mode == "on_demand"

    def test_assemble_basic(self):
        m = ServiceManifest(
            name="fsm-1",
            skeleton="state_machine_engine",
            inject={"transitions": [mark_paid, cancel], "validators": []},
            trigger={"on_demand": True},
            config={"transition_map": dict(_MAP)},
        )
        service = assemble(m)
        assert isinstance(service, StateMachineEngine)

    def test_cardinality_1n_transitions_enforced(self):
        m = ServiceManifest(
            name="bad",
            skeleton="state_machine_engine",
            inject={"transitions": []},
            trigger={"on_demand": True},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1..n"):
            assemble(m)


class TestStateMachineTransitions:
    def test_legal_transition_succeeds(self):
        engine = _make_engine()
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "completed"
        assert result["transition"] == "draft->pending"
        assert result["result"]["marked"] is True

    def test_illegal_transition_rejected(self):
        engine = _make_engine()
        result = asyncio.run(engine.run(from_status="pending", to_status="draft"))
        assert result["status"] == "failed"
        assert "illegal transition" in result["error"]

    def test_unknown_transition_function_rejected(self):
        engine = _make_engine(config={"transition_map": {"draft->pending": "ghost_function"}})
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "failed"
        assert "not found" in result["error"]

    def test_transition_exception_captured(self):
        engine = _make_engine(
            transitions=[transition_raises],
            config={"transition_map": {"draft->pending": "transition_raises"}},
        )
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "failed"

    def test_async_transition_supported(self):
        engine = _make_engine(
            transitions=[async_transition],
            config={"transition_map": {"draft->pending": "async_transition"}},
        )
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "completed"
        assert result["result"]["async"] is True


class TestStateMachineValidators:
    def test_validator_allows(self):
        engine = _make_engine(validators=[validator_allow])
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "completed"

    def test_validator_denies(self):
        engine = _make_engine(validators=[validator_deny])
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "failed"
        assert "rejected" in result["error"]

    def test_validator_exception_rejects(self):
        engine = _make_engine(validators=[validator_raises])
        result = asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert result["status"] == "failed"

    def test_single_validator_not_list(self):
        engine = _make_engine(validators=validator_allow)
        assert len(engine.validator_list) == 1


class TestStateMachineCallback:
    def test_on_step_called_once(self):
        calls = []
        engine = _make_engine()
        asyncio.run(engine.run(from_status="draft", to_status="pending", on_step=calls.append))
        assert len(calls) == 1
        assert calls[0]["status"] == "completed"


class TestStateMachineHealth:
    def test_health_reports_known_transitions(self):
        engine = _make_engine()
        h = engine.health()
        assert h["details"]["known_transitions"] == sorted(_MAP.keys())
        assert h["details"]["transitions_count"] == 2

    def test_transition_count_increments(self):
        engine = _make_engine()
        asyncio.run(engine.run(from_status="draft", to_status="pending"))
        assert engine.health()["details"]["transition_count"] == 1

    def test_stop_is_noop(self):
        engine = _make_engine()
        engine.stop()  # must not raise
