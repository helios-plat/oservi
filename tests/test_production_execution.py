import asyncio
from pathlib import Path

import pytest

from oservi import ProductionExecutionEngine, list_skeletons


@pytest.mark.asyncio
async def test_production_engine_normalizes_success_and_emits_events(tmp_path: Path) -> None:
    events: list[dict] = []

    async def operation(config: dict, input_data: dict, output_dir: Path) -> dict:
        assert config == {"profile": "standard"}
        assert input_data == {"topic": "hello"}
        assert output_dir == tmp_path
        return {"status": "succeeded", "artifacts": [{"path": "video.mp4"}]}

    engine = ProductionExecutionEngine(
        operation=operation,
        event_sink=events.append,
        config={"profile": "standard"},
    )
    result = await engine.run(input_data={"topic": "hello"}, output_dir=tmp_path)

    assert result["status"] == "succeeded"
    assert result["error"] is None
    assert [event["stage"] for event in events] == ["started", "succeeded"]
    assert "production_execution" in list_skeletons()


@pytest.mark.asyncio
async def test_production_engine_returns_structured_ordinary_failure(tmp_path: Path) -> None:
    async def operation(config: dict, input_data: dict, output_dir: Path) -> dict:
        raise RuntimeError("provider offline")

    result = await ProductionExecutionEngine(operation=operation).run(
        input_data={}, output_dir=tmp_path
    )

    assert result["status"] == "failed"
    assert result["error"]["code"] == "RUNTIMEERROR"


@pytest.mark.asyncio
async def test_production_engine_propagates_cancellation(tmp_path: Path) -> None:
    async def operation(config: dict, input_data: dict, output_dir: Path) -> dict:
        raise asyncio.CancelledError

    engine = ProductionExecutionEngine(operation=operation)
    with pytest.raises(asyncio.CancelledError):
        await engine.run(input_data={}, output_dir=tmp_path)
