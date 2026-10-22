"""StateMachineEngine tests — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import list_skeletons
from oservi.engines.state_machine_engine import StateMachineEngine


# ===== Fake transitions (keyed by __name__ unless explicit .name) =====

def paid(*, state, to_state, input_data):
    return {"transitioned_to": to_state, "from": state}


async def async_shipped(*, state, to_state, input_data):
    return {"transitioned_to": to_state, "async": True}


def _custom_impl(*, state, to_state, input_data):
    return {"transitioned_to": to_state, "custom": True}


_custom_impl.name = "custom_state"  # explicit name-keying


def boom(*, state, to_state, input_data):
    raise ValueError("transition blew up")


# ===== Fake validators (guards) =====

def allow_all(*, state, to_state, input_data):
    return True


def deny_all(*, state, to_state, input_data):
    return False


async def async_allow(*, state, to_state, input_data):
    return True


def raising_guard(*, state, to_state, input_data):
    raise RuntimeError("guard crashed")


def _make_engine(**overrides):
    defaults = dict(
        transitions=[paid],
        validators=None,
        trigger={"on_demand": True},
        config={},
        name="test-fsm",
    )
    defaults.update(overrides)
    return StateMachineEngine(**defaults)


# ===== Tests =====

def test_state_machine_engine_registered():
    assert "state_machine_engine" in list_skeletons()


def test_injection_points_keys():
    pts = StateMachineEngine.injection_points
    assert "transitions" in pts
    assert "validators" in pts


def test_injection_point_transitions_kind():
    pt = StateMachineEngine.injection_points["transitions"]
    assert pt.kind == "omodul"
    assert pt.cardinality == "1..n"


def test_injection_point_validators_kind():
    pt = StateMachineEngine.injection_points["validators"]
    assert pt.kind == "oskill"
    assert pt.cardinality == "0..n"


def test_trigger_mode_on_demand():
    assert StateMachineEngine.trigger_mode == "on_demand"


def test_instantiates_with_single_transition():
    engine = _make_engine(transitions=paid)  # single callable, not list
    assert len(engine.transition_list) == 1


def test_name_keying_by_dunder_name():
    engine = _make_engine(transitions=[paid, async_shipped])
    assert "paid" in engine._transitions
    assert "async_shipped" in engine._transitions


def test_name_keying_by_explicit_name_attr():
    engine = _make_engine(transitions=[_custom_impl])
    assert "custom_state" in engine._transitions


def test_run_valid_transition_completed():
    engine = _make_engine(transitions=[paid])
    result = asyncio.run(engine.run(state="new", to_state="paid"))
    assert result["status"] == "completed"
    assert result["from_state"] == "new"
    assert result["to_state"] == "paid"
    assert result["result"]["transitioned_to"] == "paid"


def test_run_async_transition():
    engine = _make_engine(transitions=[async_shipped])
    result = asyncio.run(engine.run(state="paid", to_state="async_shipped"))
    assert result["status"] == "completed"
    assert result["result"]["async"] is True


def test_run_unknown_transition_blocked_not_raised():
    engine = _make_engine(transitions=[paid])
    result = asyncio.run(engine.run(state="new", to_state="nonexistent"))
    assert result["status"] == "blocked"
    assert result["reason"] == "unknown_transition"


def test_run_no_target_state_blocked():
    engine = _make_engine(transitions=[paid])
    result = asyncio.run(engine.run(state="new", to_state=None))
    assert result["status"] == "blocked"
    assert result["reason"] == "no_target_state"


def test_validator_denies_blocks_transition():
    engine = _make_engine(transitions=[paid], validators=[deny_all])
    result = asyncio.run(engine.run(state="new", to_state="paid"))
    assert result["status"] == "blocked"
    assert result["reason"] == "guard_rejected"


def test_async_validator_allows_transition():
    engine = _make_engine(transitions=[paid], validators=[async_allow])
    result = asyncio.run(engine.run(state="new", to_state="paid"))
    assert result["status"] == "completed"


def test_raising_validator_treated_as_deny():
    engine = _make_engine(transitions=[paid], validators=[raising_guard])
    result = asyncio.run(engine.run(state="new", to_state="paid"))
    assert result["status"] == "blocked"
    assert result["reason"] == "guard_rejected"


def test_transition_error_blocked_not_raised():
    engine = _make_engine(transitions=[boom])
    result = asyncio.run(engine.run(state="new", to_state="boom"))
    assert result["status"] == "blocked"
    assert result["reason"] == "transition_error"
    assert "error" in result


def test_on_step_callback_called():
    called = []
    engine = _make_engine(transitions=[paid])
    asyncio.run(engine.run(state="new", to_state="paid", on_step=called.append))
    assert len(called) == 1
    assert called[0]["status"] == "completed"


def test_health_reports_known_states():
    engine = _make_engine(transitions=[paid, async_shipped])
    h = engine.health()
    assert h["details"]["transitions_count"] == 2
    assert "paid" in h["details"]["known_states"]


def test_stop_sets_event():
    engine = _make_engine()
    engine.stop()
    assert engine._stop_event.is_set()
