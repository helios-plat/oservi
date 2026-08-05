"""oservi.event_workflow_engine — AutoAgent-style event-driven workflow engine.

Provides ``make_event`` (register coroutine → BaseEvent), ``listen_group``
(group listeners with any/all trigger), ``drive(input, ctx)`` (topological
execution), and ``goto``/``abort`` dynamic flow control.

3O element: ``oservi.event_workflow_engine`` (``EventWorkflowEngine``).
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from typing import Any, Callable


class EventWorkflowEngine:
    """AutoAgent flow engine: make_event → listen_group → drive."""

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._events: dict[str, _Event] = {}
        self._listeners: dict[str, list[str]] = {}  # source_event_id → [listener_event_ids]
        self._groups: dict[str, list[str]] = {}  # group_name → [event_ids]
        self._retrigger: dict[str, str] = {}  # group → "all" | "any"

    # -- event registration -----------------------------------------------
    def make_event(self, name: str = None, func: Callable | None = None) -> Callable:
        """Decorator: register an async function as a workflow event."""
        if func is not None:
            return self._register(name or func.__name__, func)

        def decorator(fn: Callable) -> Callable:
            return self._register(name or fn.__name__, fn)

        return decorator

    def _register(self, name: str, func: Callable) -> Callable:
        sid = hashlib.md5(inspect.getsource(func).encode()).hexdigest()[:16]
        self._events[sid] = _Event(name, func, sid)
        return func

    def get_event(self, name: str) -> str:
        """Look up an event id by name."""
        for sid, ev in self._events.items():
            if ev.name == name:
                return sid
        return name

    # -- group wiring -----------------------------------------------------
    def listen_group(self, markers: list[str], name: str = None, retrigger: str = "all") -> Callable:
        """Wire a group listener: when all/any source events complete, fire."""
        group_name = name or "grp_" + hashlib.md5(str(markers).encode()).hexdigest()[:8]
        self._retrigger[group_name] = retrigger
        self._groups[group_name] = []
        for m in markers:
            self._listeners.setdefault(m, []).append(group_name)

        def decorator(fn: Callable) -> Callable:
            ev = self._register(group_name, fn)
            self._groups[group_name].append(ev.id)
            return fn

        return decorator

    def listen_start(self, name: str) -> None:
        """Mark an event as a start node (no dependencies)."""
        ev = self.get_event(name)
        self._listeners.setdefault("__start__", []).append(ev)

    # -- drive ------------------------------------------------------------
    async def drive(self, system_input: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute the workflow DAG.  Start events run first; groups fire when ready."""
        ctx = dict(system_input or {})
        completed: set[str] = set()
        start_sids = self._listeners.get("__start__", [])
        if not start_sids:
            # auto-discover: events not listening to anyone
            all_listeners = set()
            for glist in self._listeners.values():
                for g in glist:
                    all_listeners.update(self._groups.get(g, []))
            start_sids = [eid for eid in self._events if eid not in all_listeners]

        # collect group listener events
        group_triggers: dict[str, set[str]] = {}
        for g_name, eids in self._groups.items():
            group_triggers[g_name] = set()
            for eid in eids:
                for src, glist in self._listeners.items():
                    if g_name in glist:
                        group_triggers[g_name].add(src)

        results: dict[str, Any] = {}

        async def _fire(eid: str, name: str) -> dict[str, Any]:
            ev = self._events.get(eid)
            if ev is None:
                return {"error": f"unknown event {eid}"}
            try:
                return await ev.func(
                    _EventInput({"event_id": eid, "name": ev.name}),
                    _EventCtx(ctx),
                )
            except Exception as exc:
                return {"error": str(exc)}

        # BFS-style: ready queue of events whose dependencies are all completed
        ready: list[tuple[str, str]] = [
            (eid, self._events[eid].name) for eid in start_sids if eid in self._events
        ]
        while ready:
            # run current level in parallel
            batch = [
                _fire(eid, name) for eid, name in ready
            ]
            batch_results = await asyncio.gather(*batch)
            for (eid, name), br in zip(ready, batch_results):
                results[name] = br
                completed.add(eid)

            # find next ready events
            next_events = []
            remaining = set(self._events) - completed - set()
            for eid in remaining:
                ev = self._events[eid]
                # check which groups it belongs to
                for g_name, g_eids in self._groups.items():
                    if eid in g_eids:
                        # check if all trigger sources for this group are done
                        triggers = group_triggers.get(g_name, set())
                        if triggers.issubset(completed):
                            retrigger = self._retrigger.get(g_name, "all")
                            if retrigger == "any" or triggers == (triggers & completed):
                                # fire all events in this group
                                for geid in g_eids:
                                    if geid not in completed and geid not in {n for n, _ in next_events}:
                                        next_events.append((geid, self._events[geid].name))
            ready = next_events

            if not ready and remaining:
                # fire any remaining
                for eid in remaining:
                    if eid not in completed:
                        ready.append((eid, self._events[eid].name))

        return {
            "status": "completed",
            "completed_events": len(completed),
            "total_events": len(self._events),
            "results": results,
        }

    def reset(self) -> None:
        self._events.clear()
        self._listeners.clear()
        self._groups.clear()
        self._retrigger.clear()


class _Event:
    def __init__(self, name: str, func: Callable, sid: str) -> None:
        self.name = name
        self.func = func
        self.id = sid


class _EventInput(dict):
    pass


class _EventCtx(dict):
    pass
