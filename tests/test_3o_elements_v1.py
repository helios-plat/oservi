from __future__ import annotations

import pytest
from omodul.accepted_progress_projection import (
    AcceptanceVerdict,
    AcceptedProgressProjection,
    EvidenceRef,
    GoalRunEvent,
)
from omodul.experiment_engine import Baseline, ExperimentEngine, ExperimentSpec, TrialResult

from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine
from oservi.observation_journal import JsonlObservationStore, ObservationJournal
from oservi.persistent_agent_session import (
    JsonSessionStore,
    MemorySessionProvider,
    PersistentAgentSession,
    SessionSpec,
)
from oservi.workspace_fleet import (
    JsonWorkspaceStore,
    MemoryWorkspaceProvider,
    WorkspaceConflict,
    WorkspaceFleet,
    WorkspaceSnapshot,
    WorkspaceSpec,
)


@pytest.mark.asyncio
async def test_persistent_session_detach_restore_and_idempotent_terminate(tmp_path) -> None:
    store = JsonSessionStore(tmp_path / "sessions")
    session = PersistentAgentSession(provider=MemorySessionProvider(), store=store)
    created = await session.create(SessionSpec("workspace", session_id="stable", scope="tenant"))
    await session.start()
    await session.attach({"attachment_id": "client"})
    await session.detach()
    assert (await session.state()).status == "detached"

    restored = PersistentAgentSession(provider=MemorySessionProvider(), store=store)
    state = await restored.restore("stable", scope="tenant")
    assert state.session_id == created.session_id
    await restored.send("after reconnect")
    stopped = await restored.terminate()
    assert stopped.status == "stopped"
    assert (await restored.terminate()).status == "stopped"


@pytest.mark.asyncio
async def test_workspace_fleet_isolates_fork_diff_release_and_restart(tmp_path) -> None:
    store = JsonWorkspaceStore(tmp_path / "workspaces")
    fleet = WorkspaceFleet(provider=MemoryWorkspaceProvider(), store=store)
    source = await fleet.allocate(WorkspaceSpec("repo", workspace_id="source", scope="tenant"))
    child = await fleet.fork(
        source,
        WorkspaceSpec("repo", workspace_id="child", scope="tenant"),
        scope="tenant",
    )
    await fleet.bind(child, "session", scope="tenant")
    left = await fleet.snapshot(source, scope="tenant")
    right = await fleet.snapshot(child, scope="tenant")
    assert (await fleet.compare(left, right)).equal
    await fleet.release(child, scope="tenant")
    assert not (await fleet.release(child, scope="tenant")).lease.active

    restarted = WorkspaceFleet(provider=MemoryWorkspaceProvider(), store=store)
    loaded = await restarted.snapshot("child", scope="tenant")
    assert loaded.workspace_id == "child"
    await restarted.destroy("child", scope="tenant")
    assert (await restarted.destroy("child", scope="tenant")).destroyed


@pytest.mark.asyncio
async def test_workspace_branch_collision_and_snapshot_scope_are_fail_closed() -> None:
    fleet = WorkspaceFleet(provider=MemoryWorkspaceProvider())
    await fleet.allocate(WorkspaceSpec("repo", branch="feature", scope="a"))
    with pytest.raises(WorkspaceConflict):
        await fleet.allocate(WorkspaceSpec("repo", branch="feature", scope="a"))
    with pytest.raises(WorkspaceConflict):
        await fleet.compare(
            WorkspaceSnapshot("left", "r", scope="a"),
            WorkspaceSnapshot("right", "r", scope="b"),
        )

    await fleet.allocate(WorkspaceSpec("repo", workspace_id="same", scope="a"))
    await fleet.allocate(WorkspaceSpec("repo", workspace_id="same", scope="b"))
    with pytest.raises(WorkspaceConflict):
        await fleet.compare("same", "same", left_scope="a", right_scope="b")


@pytest.mark.asyncio
async def test_observation_journal_deduplicates_and_preserves_scope(tmp_path) -> None:
    journal = ObservationJournal(store=JsonlObservationStore(tmp_path / "observations.jsonl"))
    first = await journal.append(
        source="runtime",
        source_id="runtime-1",
        scope="tenant-a",
        subject="session",
        kind="terminal",
        summary="output",
        source_event_id="event-1",
        goal_run_id="goal-1",
        provenance={"provider": "test"},
    )
    duplicate = await journal.append(
        source="runtime",
        source_id="runtime-1",
        scope="tenant-a",
        subject="session",
        kind="terminal",
        summary="different payload is still the same source event",
        source_event_id="event-1",
    )
    assert duplicate.id == first.id
    assert len(await journal.query(scope="tenant-a")) == 1
    assert not await journal.query(scope="tenant-b")
    assert (await journal.timeline("tenant-a")).observations[0].goal_run_id == "goal-1"


@pytest.mark.asyncio
async def test_orchestrator_fanout_binds_sessions_without_selecting_winner() -> None:
    async def runner(*, task, config):
        return {"task_id": task["id"], "workspace_id": task["workspace_id"]}

    fleet = WorkspaceFleet(provider=MemoryWorkspaceProvider())

    def make_session(_spec):
        return PersistentAgentSession(provider=MemorySessionProvider())

    engine = SubagentOrchestratorEngine(
        subagent_runner=runner,
        llm_caller=lambda **_: {},
        workspace_fleet=fleet,
        session_factory=make_session,
        trigger={"on_demand": True},
        config={"max_parallel": 2},
        name="workspace-composition",
    )
    result = await engine.orchestrate_workspace_candidates(
        [{"id": "a"}, {"id": "b"}],
        workspace_specs=[
            {"source_ref": "repo", "scope": "tenant"},
            {"source_ref": "repo", "scope": "tenant"},
        ],
    )
    assert len(result["candidates"]) == 2
    assert result["winner"] is None
    assert result["acceptance_authority"] == 0


class _WorkspaceTrialExecution:
    def __init__(self, fleet: WorkspaceFleet) -> None:
        self.fleet = fleet

    async def execute(self, trial) -> TrialResult:
        workspace = await self.fleet.allocate(
            WorkspaceSpec(
                source_ref="research-source",
                workspace_id=f"trial-{trial.iteration}",
                scope="experiment",
            )
        )
        snapshot = await self.fleet.snapshot(workspace, scope="experiment")
        return TrialResult(
            trial_id=trial.trial_id,
            candidate_id=trial.candidate.candidate_id,
            metrics={"score": 2.0},
            outputs={"workspace_id": workspace.workspace_id, "snapshot": snapshot.digest},
            evidence_refs=(f"workspace:{workspace.workspace_id}",),
        )


@pytest.mark.asyncio
async def test_experiment_workspace_metric_and_accepted_progress_composition() -> None:
    fleet = WorkspaceFleet(provider=MemoryWorkspaceProvider())
    engine = ExperimentEngine(
        execution_port=_WorkspaceTrialExecution(fleet),
    )
    await engine.create(
        ExperimentSpec("research", "workspace candidate", baseline=Baseline({"score": 1.0}))
    )
    trial = await engine.propose_trial()
    assert trial is not None
    result = await engine.execute_trial(trial)
    metric = await engine.evaluate(result, trial=trial)
    comparison = await engine.compare(metric, experiment_id="research")
    recommendation = await engine.decide(comparison, experiment_id="research", trial=result)
    assert recommendation.decision == "accept"
    assert recommendation.authority == "candidate_recommendation"

    projection = AcceptedProgressProjection()
    projection.apply_event(GoalRunEvent("trial-complete", trial.trial_id, "completed"))
    assert not projection.accepted()
    projection.apply_verdict(
        AcceptanceVerdict(
            "research-verdict",
            trial.trial_id,
            "PASS",
            evidence_refs=(EvidenceRef(result.evidence_refs[0], kind="workspace"),),
        )
    )
    assert projection.snapshot().accepted_ids == (trial.trial_id,)
