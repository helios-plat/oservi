"""oservi.agent_team_monitor — 事件溯源团队实时监督 (pi-workbench 复刻)。

多 Agent 协作从"静态 summary"变"可监督": 订阅 event_bus (agent.* 主题),
从事件流**投影**出每个 agent 的 live_state (status/current_task/last_message/
artifacts), 支持历史重放 (bus.history) 与实时订阅。

事件契约 (agent 通过 event_bus.publish 上报):
    publish("agent.start",   {"agent_id", "task", ...})
    publish("agent.message", {"agent_id", "message", "task"})
    publish("agent.end",     {"agent_id", "status", "artifacts": [...]})
    publish("agent.error",   {"agent_id", "error"})

分层: oservi (编排) — 依赖 obase.event_bus / 审计, 零 veya 反向依赖。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from obase.event_bus import Event, EventBus

_TOPICS = ("agent.start", "agent.message", "agent.end", "agent.error")


@dataclass
class AgentLiveState:
    """单 agent 实时状态 (事件投影)。"""

    agent_id: str
    status: str = "idle"           # idle | running | done | error
    current_task: str = ""
    last_message: str = ""
    artifacts: list[str] = field(default_factory=list)
    last_event_at: float = 0.0
    event_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "status": self.status,
            "current_task": self.current_task,
            "last_message": self.last_message,
            "artifacts": self.artifacts,
            "last_event_at": self.last_event_at,
            "event_count": self.event_count,
        }


class TeamMonitor:
    """团队监视器: 订阅事件流 → 投影 live_state + 事件流队列。"""

    def __init__(self, team_id: str, agents: list[str],
                 event_bus: EventBus | None = None) -> None:
        self.team_id = team_id
        self.agents = agents
        self.bus = event_bus or EventBus()
        self._states: dict[str, AgentLiveState] = {
            aid: AgentLiveState(agent_id=aid) for aid in agents}
        self._events: list[dict[str, Any]] = []          # 事件流 (订阅接口)
        self._lock = threading.RLock()
        self._handler: Callable[[Event], None] | None = None

    # ── 订阅 ──────────────────────────────────────────────────────────
    def start(self) -> TeamMonitor:
        """注册 event_bus 通配订阅 + 重放历史。"""
        self._handler = self._on_event
        self.bus.subscribe("*", self._handler)
        # 事件溯源: 重放历史 (bus.history)
        for evt in self.bus.history():
            if evt.type in _TOPICS:
                self._on_event(evt)
        return self

    def stop(self) -> None:
        if self._handler is not None:
            self.bus.unsubscribe("*", self._handler)
            self._handler = None

    # ── 事件处理 (投影) ───────────────────────────────────────────────
    def _on_event(self, event: Event) -> None:
        payload = event.payload or {}
        agent_id = payload.get("agent_id")
        if not agent_id or agent_id not in self._states:
            return
        with self._lock:
            st = self._states[agent_id]
            st.event_count += 1
            st.last_event_at = time.time()
            et = event.type
            if et == "agent.start":
                st.status = "running"
                st.current_task = str(payload.get("task", ""))
            elif et == "agent.message":
                st.last_message = str(payload.get("message", ""))
                if payload.get("task"):
                    st.current_task = str(payload["task"])
            elif et == "agent.end":
                st.status = str(payload.get("status", "done"))
                arts = payload.get("artifacts") or []
                if isinstance(arts, list):
                    st.artifacts = [str(a) for a in arts]
            elif et == "agent.error":
                st.status = "error"
                st.last_message = str(payload.get("error", ""))
            self._events.append({"type": et, "agent_id": agent_id,
                                 "payload": payload, "ts": event.ts
                                 if hasattr(event, "ts") else time.time()})
            if len(self._events) > 200:
                self._events = self._events[-200:]

    # ── 查询 ──────────────────────────────────────────────────────────
    def live_state(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {aid: st.to_dict() for aid, st in self._states.items()}

    def event_stream(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return self._events[-limit:]


def monitor_team(team_id: str, agents: list[str],
                 event_bus: EventBus | None = None,
                 *, trace_id: str | None = None) -> dict[str, Any]:
    """主入口: 启动团队监视, 返回实时状态 + 事件流 (事件溯源投影)。

    状态由事件投影得出 (非静态 summary); 状态变更可经 event_stream 订阅轮询。
    """
    monitor = TeamMonitor(team_id, agents, event_bus)
    monitor.start()
    state = monitor.live_state()
    stream = monitor.event_stream()

    # 审计: 监视器启动 (状态投影基线)
    try:
        from pathlib import Path

        from oprim._audit_emit import AuditEvent, JsonlSink

        sink = JsonlSink(str(Path.home() / ".veya" / "audit" / "team-monitor.jsonl"))
        sink.write(AuditEvent(
            event_type="diagnose",
            trace_id=trace_id or f"team_{team_id}",
            inputs={"team_id": team_id, "agents": agents,
                    "projected": {aid: s["status"] for aid, s in state.items()}},
        ))
    except Exception:
        import logging

        logging.getLogger("veya.team_monitor").warning(
            "audit write failed", exc_info=True)

    return {
        "team_id": team_id,
        "live_state": state,
        "event_stream": stream,
        "projection": "event-sourced",
    }


__all__ = ["AgentLiveState", "TeamMonitor", "monitor_team"]
