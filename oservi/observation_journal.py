"""Canonical durable observation stream.

Observations are a provenance-preserving input to context or memory.  They
are not evidence, GoalRun events, or an acceptance authority.  The journal
only appends and queries facts through an injected store.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from obase.element_contract import ElementContract, zero_authority


class ObservationError(Exception):
    """Observation validation or persistence error."""


@dataclass(frozen=True, slots=True)
class Observation:
    id: str
    timestamp: float
    source: str
    source_id: str
    scope: str
    subject: str
    kind: str
    summary: str
    payload_ref: str | Mapping[str, Any] | None = None
    provenance: Mapping[str, Any] = field(default_factory=dict)
    trace_id: str | None = None
    goal_run_id: str | None = None
    source_event_id: str | None = None
    idempotency_key: str | None = None
    processed_at: float | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        for name in ("id", "source", "source_id", "scope", "subject", "kind"):
            if not getattr(self, name):
                raise ValueError(f"Observation.{name} must be non-empty")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "source": self.source,
            "source_id": self.source_id,
            "scope": self.scope,
            "subject": self.subject,
            "kind": self.kind,
            "summary": self.summary,
            "payload_ref": self.payload_ref,
            "provenance": dict(self.provenance),
            "trace_id": self.trace_id,
            "goal_run_id": self.goal_run_id,
            "source_event_id": self.source_event_id,
            "idempotency_key": self.idempotency_key,
            "processed_at": self.processed_at,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Observation:
        return cls(
            id=str(data.get("id", data.get("observation_id", ""))),
            timestamp=float(data.get("timestamp", data.get("ts", time.time()))),
            source=str(data.get("source", "unknown")),
            source_id=str(data.get("source_id", "unknown")),
            scope=str(data.get("scope", "default")),
            subject=str(data.get("subject", "unknown")),
            kind=str(data.get("kind", "unknown")),
            summary=str(data.get("summary", "")),
            payload_ref=data.get("payload_ref"),
            provenance=dict(data.get("provenance", {})),
            trace_id=data.get("trace_id"),
            goal_run_id=data.get("goal_run_id"),
            source_event_id=data.get("source_event_id"),
            idempotency_key=data.get("idempotency_key"),
            processed_at=data.get("processed_at"),
            schema_version=int(data.get("schema_version", 1)),
        )


@dataclass(frozen=True, slots=True)
class ObservationId:
    value: str

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ObservationKind:
    value: str

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ObservationSource:
    value: str

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class ObservationRef:
    observation_id: str
    source: str
    scope: str

    def to_dict(self) -> dict[str, str]:
        return {
            "observation_id": self.observation_id,
            "source": self.source,
            "scope": self.scope,
        }


@dataclass(frozen=True, slots=True)
class ObservationBatch:
    observations: tuple[Observation, ...]
    batch_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "observations": [item.to_dict() for item in self.observations],
        }


@dataclass(frozen=True, slots=True)
class ObservationQuery:
    scope: str = "default"
    source: str | None = None
    source_id: str | None = None
    subject: str | None = None
    kind: str | None = None
    trace_id: str | None = None
    goal_run_id: str | None = None
    since: float | None = None
    until: float | None = None
    limit: int | None = None

    def __post_init__(self) -> None:
        if not self.scope:
            raise ValueError("ObservationQuery.scope must be non-empty")
        if self.limit is not None and self.limit < 0:
            raise ValueError("ObservationQuery.limit must be >= 0")


@dataclass(frozen=True, slots=True)
class ObservationTimeline:
    scope: str
    observations: tuple[Observation, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "observations": [item.to_dict() for item in self.observations],
        }


@runtime_checkable
class ObservationStorePort(Protocol):
    async def append(self, observation: Observation) -> Observation: ...

    async def get(self, observation_id: str, *, scope: str = "default") -> Observation | None: ...

    async def query(self, query: ObservationQuery) -> Sequence[Observation]: ...

    async def mark_processed(self, observation_id: str, *, scope: str = "default") -> bool: ...

    async def compact_refs(self, *, scope: str = "default") -> int: ...


class MemoryObservationStore:
    def __init__(self) -> None:
        self._items: dict[tuple[str, str], Observation] = {}
        self._dedupe: dict[tuple[str, str, str], str] = {}

    async def append(self, observation: Observation) -> Observation:
        key = _dedupe_key(observation)
        if key is not None:
            existing_id = self._dedupe.get(key)
            if existing_id is not None:
                return self._items[(observation.scope, existing_id)]
        item_key = (observation.scope, observation.id)
        existing = self._items.get(item_key)
        if existing is not None:
            return existing
        self._items[item_key] = observation
        if key is not None:
            self._dedupe[key] = observation.id
        return observation

    async def append_batch(self, observations: Iterable[Observation]) -> tuple[Observation, ...]:
        items: list[Observation] = []
        for item in observations:
            items.append(await self.append(item))
        return tuple(items)

    async def get(self, observation_id: str, *, scope: str = "default") -> Observation | None:
        return self._items.get((scope, observation_id))

    async def query(self, query: ObservationQuery) -> Sequence[Observation]:
        values = [item for (scope, _), item in self._items.items() if scope == query.scope]
        return _filter_sort(values, query)

    async def mark_processed(self, observation_id: str, *, scope: str = "default") -> bool:
        key = (scope, observation_id)
        item = self._items.get(key)
        if item is None:
            return False
        self._items[key] = Observation.from_dict({**item.to_dict(), "processed_at": time.time()})
        return True

    async def compact_refs(self, *, scope: str = "default") -> int:
        del scope
        return 0


class JsonlObservationStore:
    """Append-only versioned store suitable for restart/replay tests."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _read(self) -> list[Observation]:
        if not self.path.exists():
            return []
        items: list[Observation] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                items.append(Observation.from_dict(json.loads(line)))
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                raise ObservationError(f"malformed observation store: {exc}") from exc
        return items

    async def append(self, observation: Observation) -> Observation:
        items = self._read()
        key = _dedupe_key(observation)
        for item in items:
            if item.scope == observation.scope and item.id == observation.id:
                return item
            if key is not None and _dedupe_key(item) == key:
                return item
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(observation.to_dict(), ensure_ascii=False, sort_keys=True) + "\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
        return observation

    async def append_batch(self, observations: Iterable[Observation]) -> tuple[Observation, ...]:
        items: list[Observation] = []
        for item in observations:
            items.append(await self.append(item))
        return tuple(items)

    async def get(self, observation_id: str, *, scope: str = "default") -> Observation | None:
        return next(
            (item for item in self._read() if item.scope == scope and item.id == observation_id),
            None,
        )

    async def query(self, query: ObservationQuery) -> Sequence[Observation]:
        return _filter_sort([item for item in self._read() if item.scope == query.scope], query)

    async def mark_processed(self, observation_id: str, *, scope: str = "default") -> bool:
        items = self._read()
        found = False
        out: list[Observation] = []
        for item in items:
            if item.scope == scope and item.id == observation_id:
                found = True
                item = Observation.from_dict({**item.to_dict(), "processed_at": time.time()})
            out.append(item)
        if found:
            temporary = self.path.with_suffix(".jsonl.tmp")
            temporary.write_text(
                "".join(
                    json.dumps(item.to_dict(), ensure_ascii=False, sort_keys=True) + "\n"
                    for item in out
                ),
                encoding="utf-8",
            )
            temporary.replace(self.path)
        return found

    async def compact_refs(self, *, scope: str = "default") -> int:
        del scope
        return 0


class ObservationJournal:
    """Durable observation append/query facade."""

    ELEMENT_CONTRACT = ElementContract(
        element_id="observation_journal",
        element_version=1,
        owner_repo="oservi",
        canonical_import="oservi.observation_journal",
        canonical_export="ObservationJournal",
        input_contract="runtime/tool/session/computer observations with provenance",
        output_contract="Observation, ObservationBatch, ObservationQuery and timelines",
        dependency_ports=("ObservationStorePort",),
        state_model="append-only observations keyed by scope and source event",
        persistence_model="versioned idempotent store; memory or JSONL provider",
        authority_declaration=zero_authority(),
        async_contract="all storage operations are awaitable; no private scheduler",
        failure_semantics="malformed or cross-scope records are rejected; store errors surface",
        recovery_semantics="restart reloads the durable observation stream with exact deduplication",
        observability_contract="source, provenance, trace and GoalRun linkage remain queryable",
        compatibility_contract="Observation is neither Evidence nor GoalRun Event nor Memory",
        conformance_suite=(
            "APPEND_DURABLE",
            "DUPLICATE_EVENT",
            "SESSION_LINKAGE",
            "GOALRUN_LINKAGE",
            "TIMELINE_ORDER",
            "SCOPE_ISOLATION",
            "MULTI_USER_ISOLATION",
            "RESTART_RECOVERY",
            "OBSERVATION_NOT_EVIDENCE",
            "OBSERVATION_NOT_AUTHORITY",
        ),
    )

    def __init__(self, *, store: ObservationStorePort | None = None) -> None:
        self.store = store or MemoryObservationStore()

    async def append(
        self,
        observation: Observation | Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> Observation:
        if isinstance(observation, Observation):
            if fields:
                raise TypeError("fields cannot accompany an Observation instance")
            item = observation
        else:
            data = dict(observation or {})
            data.update(fields)
            data.setdefault("id", str(uuid.uuid4()))
            data.setdefault("timestamp", time.time())
            data.setdefault("summary", "")
            data.setdefault("payload_ref", None)
            data.setdefault("provenance", {})
            item = Observation.from_dict(data)
        return await self.store.append(item)

    async def append_batch(
        self, observations: Iterable[Observation | Mapping[str, Any]]
    ) -> ObservationBatch:
        items: list[Observation] = []
        for item in observations:
            items.append(await self.append(item))
        return ObservationBatch(tuple(items), str(uuid.uuid4()))

    async def get(self, observation_id: str, *, scope: str = "default") -> Observation | None:
        return await self.store.get(observation_id, scope=scope)

    async def query(
        self, query: ObservationQuery | Mapping[str, Any] | None = None, **filters: Any
    ) -> list[Observation]:
        if query is None:
            query = ObservationQuery(**filters) if filters else ObservationQuery()
        elif not isinstance(query, ObservationQuery):
            query = ObservationQuery(**dict(query))
        return list(await self.store.query(query))

    async def timeline(
        self, scope: str = "default", *, limit: int | None = None
    ) -> ObservationTimeline:
        values = await self.query(ObservationQuery(scope=scope, limit=limit))
        return ObservationTimeline(scope, tuple(values))

    async def mark_processed(self, observation_id: str, *, scope: str = "default") -> bool:
        return await self.store.mark_processed(observation_id, scope=scope)

    async def compact_refs(self, *, scope: str = "default") -> int:
        return await self.store.compact_refs(scope=scope)


def _dedupe_key(item: Observation) -> tuple[str, str, str] | None:
    if item.source_event_id:
        return (item.scope, item.source, item.source_event_id)
    if item.idempotency_key:
        return (item.scope, item.source, item.idempotency_key)
    return None


def _filter_sort(items: Sequence[Observation], query: ObservationQuery) -> list[Observation]:
    result = [
        item
        for item in items
        if (query.source is None or item.source == query.source)
        and (query.source_id is None or item.source_id == query.source_id)
        and (query.subject is None or item.subject == query.subject)
        and (query.kind is None or item.kind == query.kind)
        and (query.trace_id is None or item.trace_id == query.trace_id)
        and (query.goal_run_id is None or item.goal_run_id == query.goal_run_id)
        and (query.since is None or item.timestamp >= query.since)
        and (query.until is None or item.timestamp <= query.until)
    ]
    result.sort(key=lambda item: (item.timestamp, item.id))
    return result[: query.limit] if query.limit is not None else result


__all__ = [
    "JsonlObservationStore",
    "MemoryObservationStore",
    "Observation",
    "ObservationBatch",
    "ObservationError",
    "ObservationId",
    "ObservationJournal",
    "ObservationKind",
    "ObservationQuery",
    "ObservationRef",
    "ObservationSource",
    "ObservationStorePort",
    "ObservationTimeline",
]
