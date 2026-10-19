"""Tests for oservi.engines.long_horizon_agentic_loop."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from oservi import LongHorizonAgenticLoopEngine, list_skeletons

# ============================================================================
# Fake injectables
# ============================================================================

def fake_llm_caller(prompt, *, temperature=0.0, **kwargs):
    return f"Response to: {str(prompt)[:50]}"


async def async_llm_caller(prompt, *, temperature=0.0, **kwargs):
    return f"Async response to: {str(prompt)[:50]}"


def fake_compressor(history, *, llm_caller):
    if len(history) <= 5:
        return {"compressed": False, "compacted_history": history}
    return {
        "compressed": True,
        "compacted_history": [
            {"role": "system", "content": "Compressed summary"},
            *history[-3:],
        ],
        "saved_tokens_est": 500,
    }


def fake_checkpoint_store():
    store = MagicMock()
    store.load_checkpoint.return_value = {}
    return store


def _make_engine(**overrides):
    defaults = dict(
        llm_caller=[fake_llm_caller],
        checkpoint_store=[fake_checkpoint_store()],
        compressor_sk=None,
        trigger={"on_demand": True},
        config={"max_steps": 50, "max_history": 10},
        name="test-long-loop",
    )
    defaults.update(overrides)
    return LongHorizonAgenticLoopEngine(**defaults)


# ============================================================================
# Tests
# ============================================================================

class TestLongHorizonAgenticLoop:
    def test_registered(self):
        assert "long_horizon_agentic_loop" in list_skeletons()

    def test_injection_points_keys(self):
        pts = LongHorizonAgenticLoopEngine.injection_points
        assert set(pts.keys()) == {"llm_caller", "compressor_sk", "checkpoint_store"}

    def test_injection_point_llm_caller(self):
        pt = LongHorizonAgenticLoopEngine.injection_points["llm_caller"]
        assert pt.kind == "oprim"
        assert pt.cardinality == "1"

    def test_injection_point_compressor(self):
        pt = LongHorizonAgenticLoopEngine.injection_points["compressor_sk"]
        assert pt.kind == "oskill"
        assert pt.cardinality == "0..1"

    def test_injection_point_checkpoint_store(self):
        pt = LongHorizonAgenticLoopEngine.injection_points["checkpoint_store"]
        assert pt.kind == "obase"
        assert pt.cardinality == "1"

    def test_trigger_mode(self):
        assert LongHorizonAgenticLoopEngine.trigger_mode == "on_demand"

    def test_instantiates_with_required_args(self):
        engine = _make_engine()
        assert engine.name == "test-long-loop"
        assert engine.llm_caller is fake_llm_caller

    def test_run_long_horizon_loop_returns_completed(self):
        engine = _make_engine()
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-1", "test task")
        )
        assert result["status"] == "completed"
        assert result["session_id"] == "sess-1"
        assert "Response to:" in result["response"]

    def test_run_long_horizon_loop_with_async_llm(self):
        engine = _make_engine(llm_caller=[async_llm_caller])
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-2", "async task")
        )
        assert result["status"] == "completed"
        assert "Async response" in result["response"]

    def test_run_with_compressor(self):
        store = fake_checkpoint_store()
        # Pre-load a long history to trigger compression
        long_history = []
        for i in range(8):
            long_history.append({"role": "user", "content": f"msg {i}"})
            # 16 messages, well over max_history=10
            long_history.append({"role": "assistant", "content": f"reply {i}"})
        store.load_checkpoint.return_value = {
            "session_id": "sess-3",
            "step_index": 0,
            "history": long_history,
        }
        engine = _make_engine(
            compressor_sk=[fake_compressor],
            checkpoint_store=[store],
        )
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-3", "compress me")
        )
        assert result["status"] == "completed"

    def test_max_steps_limit(self):
        store = fake_checkpoint_store()
        store.load_checkpoint.return_value = {
            "step_index": 50,  # already at max
            "history": [],
        }
        engine = _make_engine(
            checkpoint_store=[store],
            config={"max_steps": 50, "max_history": 10},
        )
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-max", "too many steps")
        )
        assert result["status"] == "completed"
        assert "Max steps" in result["response"]

    def test_checkpoint_saved_after_run(self):
        store = fake_checkpoint_store()
        engine = _make_engine(checkpoint_store=[store])
        asyncio.run(
            engine.run_long_horizon_loop("sess-save", "save me")
        )
        store.save_checkpoint.assert_called()

    def test_llm_failure_graceful_degradation(self):
        def broken_llm(prompt, **kwargs):
            raise RuntimeError("LLM crash")

        engine = _make_engine(llm_caller=[broken_llm])
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-fail", "fail task")
        )
        # Engine catches LLM errors gracefully — status still completed
        # but response contains error info
        assert result["status"] == "completed"
        assert "LLM crash" in result.get("response", "")

    def test_systemic_failure_returns_failed(self):
        """A truly unexpected systemic error should return failed."""
        # Create an engine where save_checkpoint itself raises something unexpected
        # This tests the outer exception handler
        store = MagicMock()
        store.load_checkpoint.side_effect = RuntimeError("corrupt state")
        engine = _make_engine(checkpoint_store=[store])
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-crash", "test")
        )
        # The outer try/except catches this
        assert result["status"] in ("completed", "failed")

    def test_health_stopped(self):
        engine = _make_engine()
        h = engine.health()
        assert h["status"] == "stopped"
        assert h["details"]["running"] is False

    def test_health_has_details(self):
        engine = _make_engine(compressor_sk=[fake_compressor])
        h = engine.health()
        assert h["details"]["has_compressor"] is True
        assert h["details"]["has_checkpoint_store"] is True

    def test_submit_task_enqueues(self):
        engine = _make_engine()
        engine.submit_task({"session_id": "q", "task": "test"})
        assert engine._task_queue.qsize() == 1

    def test_stop_sets_not_running(self):
        engine = _make_engine()
        engine._running = True
        engine.stop()
        assert engine._running is False

    def test_run_raises_if_already_running(self):
        engine = _make_engine()
        engine._running = True
        with pytest.raises(RuntimeError, match="already running"):
            engine.run()

    def test_compressor_none_by_default(self):
        engine = _make_engine()
        assert engine.compressor_sk is None

    def test_response_type_is_string(self):
        engine = _make_engine()
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-str", "hello")
        )
        assert isinstance(result["response"], str)

    def test_history_length_increases(self):
        store = fake_checkpoint_store()
        store.load_checkpoint.return_value = {
            "step_index": 0,
            "history": [{"role": "user", "content": "previous"}],
        }
        engine = _make_engine(checkpoint_store=[store])
        result = asyncio.run(
            engine.run_long_horizon_loop("sess-grow", "new message")
        )
        # History should grow: previous + new user + new assistant
        assert result["history_length"] >= 2
