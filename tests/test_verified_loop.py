"""WAVE C gates: verified long-horizon loop on AgenticLoopEngine.

VERIFIED_CHECKPOINT / REJECTED_RESULT_NOT_PROGRESS / FAILURE_EVIDENCE_PRESERVED
RECOVER_LAST_ACCEPTED / FRESH_CONTEXT_ROUND / BOUNDED_ROUND / ROUND_LEDGER
CANCELLATION + OSERVI_BUSINESS_PERSISTENCE=0 + NEW_OSERVI_ENGINE_COUNT=0.
"""

from __future__ import annotations

import asyncio

import pytest

from oservi.engines.agentic_loop import AgenticLoopEngine


def _make_engine(**overrides):
    async def _llm(**kwargs):
        return {"thought": "t"}

    def _turn(**kwargs):
        return {"ok": True}

    defaults = dict(
        llm_caller=_llm,
        tools=[],
        turn_handler=_turn,
        trigger={"on_demand": True},
        config={},
        name="test-verified-loop",
    )
    defaults.update(overrides)
    return AgenticLoopEngine(**defaults)


@pytest.mark.asyncio()
async def test_verified_checkpoint_accepted():
    engine = _make_engine()

    def executor(ctx):
        return {"progress": {"step1": "done"}}

    def verifier(result):
        return {"accepted": True}

    out = await engine.run_verified("reach goal", executor, verifier, max_rounds=3)
    assert out["status"] == "COMPLETED"
    assert out["verified_state"] == {"step1": "done"}
    statuses = [r["status"] for r in out["round_ledger"]]
    assert "ACCEPTED" in statuses
    assert out["round_ledger"][0].get("checkpoint") == {"step1": "done"}


@pytest.mark.asyncio()
async def test_rejected_not_progress_evidence_preserved():
    engine = _make_engine()
    calls = {"n": 0}

    def executor(ctx):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"progress": {"bad": "data"}}
        return {"progress": {"good": "data"}}

    def verifier(result):
        return {"accepted": "good" in str(result), "reason": "bad round"}

    out = await engine.run_verified("goal", executor, verifier, max_rounds=3)
    assert out["verified_state"] == {"good": "data"}
    assert "bad" not in out["verified_state"]
    assert len(out["failure_evidence"]) == 1
    assert out["failure_evidence"][0]["round"] == 1
    statuses = [r["status"] for r in out["round_ledger"]]
    assert "REJECTED" in statuses and "RECOVERING" in statuses


@pytest.mark.asyncio()
async def test_recover_last_accepted_checkpoint():
    engine = _make_engine()
    seen_states = []

    def executor(ctx):
        seen_states.append(dict(ctx["verified_state"]))
        n = len(seen_states)
        if n == 1:
            return {"progress": {"a": 1}}
        if n == 2:
            return {"progress": {"b": 2}}  # rejected, must not persist
        return {"progress": {"c": 3}}

    def verifier(result):
        return {"accepted": "b" not in str(result)}

    out = await engine.run_verified(
        "goal", executor, verifier, max_rounds=3, is_complete=lambda s: "c" in s
    )
    # round3 的 fresh context 必须从 last accepted (a=1) 恢复,不含 b
    assert seen_states[2] == {"a": 1}
    assert out["verified_state"] == {"a": 1, "c": 3}


@pytest.mark.asyncio()
async def test_fresh_context_round_shape():
    engine = _make_engine()
    captured = {}

    def executor(ctx):
        captured.update(ctx)
        return {"progress": {"x": 1}}

    out = await engine.run_verified("my-goal", executor, lambda r: True, max_rounds=1)
    assert captured["original_goal"] == "my-goal"
    assert set(captured) == {
        "original_goal",
        "verified_state",
        "remaining_work",
        "failure_evidence",
        "bounded_next_step",
    }
    assert out["status"] == "COMPLETED"


@pytest.mark.asyncio()
async def test_bounded_round_and_ledger():
    engine = _make_engine()
    out = await engine.run_verified(
        "never",
        lambda ctx: {"progress": {"k": 1}},
        lambda r: {"accepted": True},
        max_rounds=2,
    )
    assert out["rounds_executed"] == 1
    # is_complete 缺省 None -> 首轮 ACCEPTED 即停;显式永不完成则应跑满
    out2 = await engine.run_verified(
        "never",
        lambda ctx: {"progress": {"k": 1}},
        lambda r: {"accepted": True},
        max_rounds=2,
        is_complete=lambda s: False,
    )
    assert out2["rounds_executed"] == 2
    assert len([r for r in out2["round_ledger"] if r["status"] == "ACCEPTED"]) == 2


@pytest.mark.asyncio()
async def test_cancellation():
    engine = _make_engine()

    async def slow_executor(ctx):
        await asyncio.sleep(30)
        return {"progress": {}}

    task = asyncio.create_task(
        engine.run_verified("goal", slow_executor, lambda r: True, max_rounds=3)
    )
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_no_new_engine_and_no_business_persistence():
    import pathlib

    import oservi.engines as engines_pkg

    engine_files = [p.stem for p in pathlib.Path(engines_pkg.__file__).parent.glob("*.py")]
    for forbidden in (
        "long_horizon_engine",
        "recovery_engine",
        "checkpoint_engine",
        "verifier_engine",
    ):
        assert forbidden not in engine_files
    src = pathlib.Path(engines_pkg.__file__).parent.joinpath("agentic_loop.py").read_text()
    for forbidden in ("AgentSessionDB", "goal_run_store", "project_persistence", "GoalRun("):
        assert forbidden not in src
