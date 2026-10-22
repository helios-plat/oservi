from __future__ import annotations

import pytest

from obase import MemoryBank
from oservi.memory_api import MemoryApi


@pytest.mark.asyncio
async def test_memory_api_add_search_list_stats_and_delete(tmp_path):
    bank = MemoryBank(tmp_path / "memory-api.sqlite3")
    api = MemoryApi(bank)

    memory_id = await api.add(
        memory_id="MEM-API-1",
        context_description="cache stampede mitigation",
        failed_attempt={"action": "uncached reads", "reason": "thundering herd"},
        successful_strategy={"action": "single-flight", "reason": "deduplicate requests"},
        context_vector=[0.25, 0.75],
    )
    assert memory_id == "MEM-API-1"

    results = await api.search("cache stampede single-flight", top_k=2)
    assert len(results) == 1
    assert results[0]["memory_id"] == "MEM-API-1"
    assert results[0]["context_vector"] == [0.25, 0.75]
    assert results[0]["successful_strategy"]["action"] == "single-flight"

    listed = await api.list_all()
    assert [item["memory_id"] for item in listed] == ["MEM-API-1"]
    assert await api.stats() == {"memory_count": 1}
    assert await api.delete("MEM-API-1") is True
    assert await api.delete("MEM-API-1") is False
    assert await api.stats() == {"memory_count": 0}
    bank.close()
