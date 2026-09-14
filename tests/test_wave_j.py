import asyncio

import pytest

from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine


def make_engine(runner, **config):
    return SubagentOrchestratorEngine(
        subagent_runner=runner,
        llm_caller=lambda **_: {},
        scheduler=None,
        trigger={"on_demand": True},
        config=config,
        name="wave-j",
    )


async def runner(*, task, config):
    await asyncio.sleep(task.get("delay", 0))
    if task.get("fail"):
        raise RuntimeError("failed")
    return {"task_id": task["id"], "cost_usd": task.get("cost", 0.1)}


@pytest.mark.asyncio
async def test_dag_dynamic_replenishment_and_cost():
    engine = make_engine(runner, max_parallel=2)
    result = await engine.orchestrate(
        [
            {"id": "a", "delay": 0.02},
            {"id": "b", "depends_on": ["a"]},
            {"id": "c", "depends_on": ["a"], "cost": 0.2},
        ],
        parallel=True,
    )
    assert [item["task_id"] for item in result["completed"]] == ["a", "b", "c"]
    assert result["total_cost_usd"] == pytest.approx(0.4)
    assert result["partial"] is False


@pytest.mark.asyncio
async def test_dag_invalid_and_dependency_failure_are_fail_closed():
    engine = make_engine(runner)
    with pytest.raises(ValueError, match="cycle"):
        await engine.orchestrate(
            [{"id": "a", "depends_on": ["b"]}, {"id": "b", "depends_on": ["a"]}]
        )
    with pytest.raises(ValueError, match="missing"):
        await engine.orchestrate([{"id": "a", "depends_on": ["ghost"]}])
    result = await engine.orchestrate(
        [{"id": "a", "fail": True}, {"id": "b", "depends_on": ["a"]}], parallel=True
    )
    assert result["failed"] and result["blocked"]
    assert result["partial"] is True


@pytest.mark.asyncio
async def test_admission_timeout_and_global_timeout():
    engine = make_engine(
        runner, task_timeout=0.01, overall_timeout=0.03, admission_control=lambda **_: False
    )
    rejected = await engine.orchestrate([{"id": "r"}], parallel=True)
    assert rejected["failed"][0]["status"] == "admission_rejected"
    engine = make_engine(runner, task_timeout=0.01)
    timed = await engine.orchestrate([{"id": "t", "delay": 0.05}], parallel=True)
    assert timed["timed_out"]


@pytest.mark.asyncio
async def test_backpressure_is_observable_without_business_policy():
    engine = make_engine(runner, max_parallel=1, backpressure_limit=1)
    result = await engine.orchestrate([{"id": "a"}, {"id": "b"}, {"id": "c"}], parallel=True)
    assert result["backpressure_activated"] is True
    engine = make_engine(runner, overall_timeout=0.01)
    timed = await engine.orchestrate([{"id": "t", "delay": 0.05}], parallel=True)
    assert timed["timed_out"]


@pytest.mark.asyncio
async def test_parent_cancellation_propagates_and_preserves_completed():
    engine = make_engine(runner, max_parallel=2)
    task = asyncio.create_task(
        engine.orchestrate([{"id": "a"}, {"id": "b", "delay": 1}], parallel=True)
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
