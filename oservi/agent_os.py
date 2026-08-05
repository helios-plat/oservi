"""oservi.agent_os — Agent OS assembly container.

3O layer: oservi (engine assembly).
Brings every subsystem together into one lifecycle-managed container:
master brain + automata + vault + rag. The host wires concrete providers
via ``build_agent_os`` and calls ``start()`` / ``shutdown()`` from its
app lifespan.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from oservi.master_agent import MasterAgent

_log = logging.getLogger(__name__)


class AgentOS:
    """Assembly container: master brain + background daemon + subsystems."""

    def __init__(
        self,
        master: MasterAgent,
        *,
        automata: Any | None = None,
        vault: Any | None = None,
        rag: Any | None = None,
    ):
        self.master = master
        self.automata = automata
        self.vault = vault
        self.rag = rag
        self._started = False

    # ── 生命周期 ─────────────────────────────────────────────────────
    def start(self) -> None:
        """Bring the Agent OS online (host calls from app lifespan)."""
        if self._started:
            return
        self._started = True
        _log.info("agent_os: online (master + automata + vault + rag)")

    def shutdown(self) -> None:
        """Graceful stop: persist automata jobs etc."""
        if not self._started:
            return
        if self.automata is not None:
            try:
                self.automata.shutdown()
            except Exception:  # noqa: BLE001 — shutdown must never raise
                _log.exception("agent_os: automata shutdown failed")
        self._started = False
        _log.info("agent_os: offline")

    def status(self) -> dict:
        return {
            "started": self._started,
            "master": {
                "max_rounds": self.master.max_rounds,
                "tools": len(self.master.get_all_tool_schemas()),
            },
            "automata_attached": self.automata is not None,
            "vault_attached": self.vault is not None,
            "rag_attached": self.rag is not None,
        }

    async def chat(self, prompt: str, **kwargs: Any) -> dict:
        return await self.master.chat_stream(prompt, **kwargs)


def build_agent_os(
    *,
    llm_caller: Callable,
    tools: Any,
    skill_hub: Any,
    memory: Any,
    swarm: Any,
    vault: Any,
    automata_factory: Callable[[], Any] | None = None,
    rag_factory: Callable[[], Any] | None = None,
    notify: Callable[[dict], None] | None = None,
    cost_calculator: Callable[[dict], float] | None = None,
    max_rounds: int = 5,
    temperature: float = 0.2,
    automata: Any | None = None,
    rag: Any | None = None,
) -> AgentOS:
    """Assemble the full Agent OS from host-provided concrete components."""
    master = MasterAgent(
        llm_caller=llm_caller,
        tools=tools,
        skill_hub=skill_hub,
        memory=memory,
        swarm=swarm,
        vault=vault,
        automata_factory=automata_factory,
        rag_factory=rag_factory,
        notify=notify,
        max_rounds=max_rounds,
        temperature=temperature,
        cost_calculator=cost_calculator,
    )
    return AgentOS(
        master=master,
        automata=automata,
        vault=vault,
        rag=rag,
    )
