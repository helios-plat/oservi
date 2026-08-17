"""SpecDrivenGoalEngine walks a two-node speckit DAG."""

from __future__ import annotations

import pytest

from omodul.execution_health_monitor import execution_health_monitor
from omodul.phase_spec_driven_plan import phase_spec_driven_plan
from omodul.phase_verify_leaf_task import phase_verify_leaf_task
from oprim._jailed_executor import execute_leaf_with_constitution
from oservi import SpecDrivenGoalEngine, list_skeletons


def test_registered() -> None:
    assert "spec_driven_goal" in list_skeletons()


def test_injection_points() -> None:
    pts = SpecDrivenGoalEngine.injection_points
    assert set(pts) == {"leaf_executor", "plan_phase", "verify_phase", "health_monitor"}


async def _ok_verify(config, input_data, output_dir, **kwargs):
    return {"status": "completed", "findings": {"passed": True}}


@pytest.mark.asyncio
async def test_run_two_tasks(tmp_path) -> None:
    spec = tmp_path / ".speckit"
    spec.mkdir()
    (spec / "constitution.md").write_text("Must use fetch\n", encoding="utf-8")
    (spec / "tasks.md").write_text(
        "- [ ] T1 One\n  Acceptance: one\n- [ ] T2 Two\n  Depends: T1\n",
        encoding="utf-8",
    )
    engine = SpecDrivenGoalEngine(
        leaf_executor=execute_leaf_with_constitution,
        plan_phase=phase_spec_driven_plan,
        verify_phase=_ok_verify,
        health_monitor=execution_health_monitor,
        output_dir=tmp_path / "out",
    )
    rec = await engine.run_goal(tmp_path, goal_id="g-run")
    assert rec["status"] == "completed"
    assert rec["completed"] == ["T1", "T2"]


@pytest.mark.asyncio
async def test_missing_spec_blocks(tmp_path) -> None:
    engine = SpecDrivenGoalEngine(
        leaf_executor=execute_leaf_with_constitution,
        plan_phase=phase_spec_driven_plan,
        verify_phase=phase_verify_leaf_task,
        health_monitor=execution_health_monitor,
        output_dir=tmp_path / "out",
    )
    rec = await engine.run_goal(tmp_path, goal_id="g-miss")
    assert rec["status"] == "blocked_invalid_spec"
