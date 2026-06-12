"""ActionPlannerEngine 测试 — ≥10 tests."""

from __future__ import annotations

import asyncio
import pytest

from oservi import ActionPlannerEngine, list_skeletons


# ===== Fake injectables =====


def fake_llm_two_steps(*, symptom, context):
    return [
        {
            "plugin_id": "restart_service",
            "params": {"service": "nginx"},
            "description": "Restart nginx",
        },
        {"plugin_id": "check_logs", "params": {"lines": 50}, "description": "Check error logs"},
    ]


def fake_llm_empty(*, symptom, context):
    return []


def fake_llm_error(*, symptom, context):
    raise RuntimeError("LLM unavailable")


def fake_registry_ok(plugin_id):
    return lambda **kwargs: {"done": True, "plugin": plugin_id}


def fake_registry_missing(plugin_id):
    return None


def fake_rag(*, query):
    return f"Runbook for '{query}': restart nginx then check logs."


# ===== Tests =====


def test_action_planner_registered():
    assert "action_planner" in list_skeletons()


def test_action_planner_instantiates():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={},
        name="test-planner",
    )
    assert engine.name == "test-planner"
    assert engine.rag is None


def test_action_planner_injection_points():
    pts = ActionPlannerEngine.injection_points
    assert pts["llm_provider"].kind == "obase"
    assert pts["plugin_registry"].kind == "layer4"
    assert pts["plugin_registry"].cardinality == "1"
    assert pts["rag"].kind == "oskill"
    assert pts["rag"].cardinality == "0..1"


def test_execute_plan_two_steps_ok():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={"max_retries": 0},
        name="t",
    )
    result = asyncio.run(engine._execute_plan({"symptom": "nginx down"}))
    assert result["status"] == "completed"
    assert result["steps_total"] == 2
    assert result["steps_ok"] == 2


def test_execute_plan_missing_plugin_fails_step():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_missing,
        trigger={"on_interval": 0},
        config={"max_retries": 0},
        name="t",
    )
    result = asyncio.run(engine._execute_plan({"symptom": "db slow"}))
    assert result["status"] == "partial"
    assert all(s["status"] == "failed" for s in result["step_results"])


def test_execute_plan_empty_steps_returns_completed():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_empty,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    result = asyncio.run(engine._execute_plan({"symptom": "nothing"}))
    assert result["status"] == "completed"
    assert result["steps_total"] == 0


def test_execute_plan_llm_error_returns_empty_steps():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_error,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    result = asyncio.run(engine._execute_plan({"symptom": "crash"}))
    assert result["steps_total"] == 0


def test_execute_plan_with_rag_context():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        rag=fake_rag,
        trigger={"on_interval": 0},
        config={"max_retries": 0},
        name="t",
    )
    result = asyncio.run(engine._execute_plan({"symptom": "nginx 502"}))
    assert result["status"] == "completed"


def test_execute_plan_abort_on_failure():
    call_count = [0]

    def registry_fail_first(plugin_id):
        def fn(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("step 1 failed")
            return {"ok": True}

        return fn

    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=registry_fail_first,
        trigger={"on_interval": 0},
        config={"max_retries": 0, "abort_on_failure": True},
        name="t",
    )
    result = asyncio.run(engine._execute_plan({"symptom": "crash"}))
    assert result["status"] == "partial"
    # aborted after step 1 → only 1 step executed
    assert len(result["step_results"]) == 1


def test_submit_request_enqueues():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    engine.submit_request({"symptom": "test"})
    assert engine._request_queue.qsize() == 1


def test_health_shows_has_rag():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        rag=fake_rag,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    assert engine.health()["details"]["has_rag"] is True


def test_health_shows_no_rag():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    assert engine.health()["details"]["has_rag"] is False


def test_stop_sets_not_running():
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={},
        name="t",
    )
    engine._running = True
    engine.stop()
    assert engine._running is False


def test_on_plan_done_callback():
    done = []
    engine = ActionPlannerEngine(
        llm_provider=fake_llm_two_steps,
        plugin_registry=fake_registry_ok,
        trigger={"on_interval": 0},
        config={"max_retries": 0},
        name="t",
        on_plan_done=done.append,
    )
    result = asyncio.run(engine._execute_plan({"symptom": "x"}))
    engine.on_plan_done(result)
    assert len(done) == 1 and done[0]["status"] == "completed"
