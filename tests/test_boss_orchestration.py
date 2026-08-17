"""BossOrchestrationEngine: G0 then G1 DAG, skip plan on ask."""

from __future__ import annotations

import pytest
from omodul.phase_closed_loop_plan import phase_closed_loop_plan
from omodul.phase_evidence_verify import phase_evidence_verify
from omodul.phase_intent_triage import phase_intent_triage

from oservi import BossOrchestrationEngine, list_skeletons


def test_registered() -> None:
    names = list_skeletons()
    assert "boss_orchestration" in names
    assert "BossOrchestrationEngine" in names


def test_injection_points() -> None:
    pts = BossOrchestrationEngine.injection_points
    assert set(pts) == {
        "inspector",
        "intent_phase",
        "plan_phase",
        "verify_phase",
        "leaf_executor",
    }


async def _inspector():
    return {"git_diff": "", "ast_summary": {}, "active_files": []}


def _contract_task(ident: str, title: str, *, depends: list[str] | None = None) -> dict:
    return {
        "id": ident,
        "title": title,
        "files": [f"{ident.lower()}.py"],
        "logic": f"implement {title}",
        "forbidden": ["do not expand scope"],
        "acceptance": [f"{ident} in diff"],
        "depends_on": depends or [],
        "assignee": "hicode",
    }


async def _plan_caller(*, messages, max_tokens):
    return {
        "ok": True,
        "tasks": [
            _contract_task("T1", "One"),
            _contract_task("T2", "Two", depends=["T1"]),
        ],
    }


async def _verify_caller(*, messages, max_tokens):
    return {"ok": True, "passed": True, "reasoning": "ok"}


async def _intent_plan(*, messages, max_tokens):
    return {
        "ok": True,
        "action": "plan",
        "interpretation": "do the two leaf edits",
        "in_scope_files": ["t1.py", "t2.py"],
        "out_of_scope_files": [],
        "acceptance_draft": ["both diffs land"],
        "questions": [],
        "reasons": ["clear"],
    }


async def _intent_ask(*, messages, max_tokens):
    return {
        "ok": True,
        "action": "ask",
        "interpretation": "",
        "questions": ["改 auth.py 还是 session.py？"],
        "reasons": ["ambiguous"],
    }


async def _leaf(*, project_root, instruction, assignee="hicode", **kwargs):
    return {"status": "completed", "git_diff": "+def foo", "stdout": "ok"}


async def _routed(*, messages, max_tokens):
    joined = " ".join(str(m.get("content") or "") for m in messages)
    if "意图分诊官" in joined:
        return await _intent_plan(messages=messages, max_tokens=max_tokens)
    if "QA" in joined or "Acceptance" in joined:
        return await _verify_caller(messages=messages, max_tokens=max_tokens)
    return await _plan_caller(messages=messages, max_tokens=max_tokens)


_calls = {"verify": 0}


async def _fail_once_caller(*, messages, max_tokens):
    joined = " ".join(str(m.get("content") or "") for m in messages)
    if "意图分诊官" in joined:
        return await _intent_plan(messages=messages, max_tokens=max_tokens)
    if "QA" in joined or "Acceptance" in joined:
        _calls["verify"] += 1
        if _calls["verify"] == 1:
            return {"ok": True, "passed": False, "reasoning": "missing foo"}
        return {"ok": True, "passed": True, "reasoning": "ok"}
    return await _plan_caller(messages=messages, max_tokens=max_tokens)


def _engine(tmp_path, caller, **kwargs):
    return BossOrchestrationEngine(
        inspector=_inspector,
        intent_phase=phase_intent_triage,
        plan_phase=phase_closed_loop_plan,
        verify_phase=phase_evidence_verify,
        leaf_executor=_leaf,
        llm_caller=caller,
        output_dir=tmp_path / "out",
        **kwargs,
    )


@pytest.mark.asyncio
async def test_run_two_tasks(tmp_path) -> None:
    rec = await _engine(tmp_path, _routed).run_goal(tmp_path, "do work", goal_id="g-boss")
    assert rec["status"] == "completed"
    assert rec["completed"] == ["T1", "T2"]


@pytest.mark.asyncio
async def test_retry_on_failed_verify(tmp_path) -> None:
    _calls["verify"] = 0
    rec = await _engine(tmp_path, _fail_once_caller, config={"max_retries_per_task": 2}).run_goal(
        tmp_path, "do work", goal_id="g-retry"
    )
    assert rec["status"] == "completed"
    assert rec["completed"] == ["T1", "T2"]
    assert _calls["verify"] >= 2


@pytest.mark.asyncio
async def test_ask_skips_plan(tmp_path) -> None:
    async def _ask_only(*, messages, max_tokens):
        return await _intent_ask(messages=messages, max_tokens=max_tokens)

    async def _boom_plan(*args, **kwargs):
        raise AssertionError("plan must not run after ask")

    engine = BossOrchestrationEngine(
        inspector=_inspector,
        intent_phase=phase_intent_triage,
        plan_phase=_boom_plan,
        verify_phase=phase_evidence_verify,
        leaf_executor=_leaf,
        llm_caller=_ask_only,
        output_dir=tmp_path / "out",
    )
    rec = await engine.run_goal(tmp_path, "fix it", goal_id="g-ask")
    assert rec["status"] == "blocked_needs_clarification"
    assert rec["questions"]
