"""VeyaEvolutionEngine dispatch + hook loop."""

from __future__ import annotations

import pytest
from omodul.implicit_feedback_processor import implicit_feedback_processor

from oservi import VeyaEvolutionEngine, list_skeletons


def test_registered() -> None:
    assert "veya_evolution" in list_skeletons()


def test_injection_points() -> None:
    pts = VeyaEvolutionEngine.injection_points
    assert set(pts) == {"shadow_vcs_hook", "feedback_processor", "lora_trainer"}
    assert pts["shadow_vcs_hook"].kind == "layer4"
    assert pts["lora_trainer"].cardinality == "0..1"


@pytest.mark.asyncio
async def test_dispatch_style_noise(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f(x):\n    return x\n", encoding="utf-8")

    async def hook():
        if False:
            yield {}

    engine = VeyaEvolutionEngine(
        shadow_vcs_hook=hook,
        feedback_processor=implicit_feedback_processor,
        output_dir=tmp_path / "out",
    )
    rec = await engine.dispatch(
        {"repo_path": repo, "file_path": "a.py", "v0_commit": "", "v1_commit": ""}
    )
    assert rec["status"] == "completed"
    assert rec["findings"]["action"] == "ignored_style_noise"


@pytest.mark.asyncio
async def test_listen_one_event(tmp_path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f(x):\n    return x\n", encoding="utf-8")

    async def hook():
        yield {"repo_path": repo, "file_path": "a.py", "v0_commit": "", "v1_commit": ""}

    engine = VeyaEvolutionEngine(
        shadow_vcs_hook=hook,
        feedback_processor=implicit_feedback_processor,
        output_dir=tmp_path / "out",
    )
    results = await engine.listen_and_dispatch()
    assert len(results) == 1
    assert results[0]["findings"]["action"] == "ignored_style_noise"
