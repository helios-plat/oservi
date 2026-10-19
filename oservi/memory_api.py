"""3O 范式内化 — 记忆库管理 API (oservi).

对注入的 MemoryBank 暴露增删查改接口 (机制层, 无业务语义)。
"""

from __future__ import annotations

from typing import Any

from obase.memory_bank import MemoryBank, MemoryItem


class MemoryApi:
    """ReasoningBank 管理 API (memory 依赖注入)。"""

    def __init__(self, memory: MemoryBank):
        self.memory = memory

    async def add(
        self,
        memory_id: str,
        context_description: str,
        failed_attempt: dict[str, str],
        successful_strategy: dict[str, str],
        context_vector: list[float] | None = None,
    ) -> str:
        item = MemoryItem(
            memory_id=memory_id,
            context_description=context_description,
            failed_attempt=failed_attempt,
            successful_strategy=successful_strategy,
            context_vector=context_vector or [],
        )
        await self.memory.add(item)
        return memory_id

    async def search(self, error_signature: str, *, top_k: int = 3) -> list[dict[str, Any]]:
        items = await self.memory.search_experience(error_signature, top_k=top_k)
        return [i.to_dict() for i in items]

    async def list_all(self) -> list[dict[str, Any]]:
        return [i.to_dict() for i in await self.memory.list_all()]

    async def delete(self, memory_id: str) -> bool:
        return await self.memory.delete(memory_id)

    async def stats(self) -> dict[str, Any]:
        return {"memory_count": await self.memory.count()}
