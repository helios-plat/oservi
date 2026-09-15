"""Canonical persistent agent/terminal session element.

The element stores durable session identity and delegates all terminal or
remote process operations to an injected ``SessionProvider``.  It has no
knowledge of Veya, GoalRun, or any client process.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from obase.element_contract import ElementContract, zero_authority

SESSION_STATUSES = (
    "creating",
    "starting",
    "working",
    "idle",
    "blocked",
    "detached",
    "stopped",
    "failed",
)


class SessionError(Exception):
    """Session lifecycle or provider error."""


class SessionProviderUnavailable(SessionError):
    """A provider operation is not configured for the requested session."""


SessionId = str


def _provider_key(scope: str, session_id: str) -> tuple[str, str]:
    return scope, session_id


@dataclass(frozen=True, slots=True)
class AgentSessionRef:
    session_id: SessionId
    provider: str
    external_session_ref: str = ""
    scope: str = "default"

    @property
    def id(self) -> str:
        return self.session_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "provider": self.provider,
            "external_session_ref": self.external_session_ref,
            "scope": self.scope,
        }


@dataclass(frozen=True, slots=True)
class SessionSpec:
    workspace_ref: str
    agent_type: str = "terminal"
    command: tuple[str, ...] = ()
    env: Mapping[str, str] = field(default_factory=dict)
    cwd: str | None = None
    session_id: SessionId | None = None
    scope: str = "default"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.workspace_ref:
            raise ValueError("workspace_ref must be non-empty")
        if isinstance(self.command, str):
            object.__setattr__(self, "command", (self.command,))
        else:
            object.__setattr__(self, "command", tuple(str(item) for item in self.command))

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_ref": self.workspace_ref,
            "agent_type": self.agent_type,
            "command": list(self.command),
            "env": dict(self.env),
            "cwd": self.cwd,
            "session_id": self.session_id,
            "scope": self.scope,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SessionSpec:
        return cls(
            workspace_ref=str(data.get("workspace_ref", "")),
            agent_type=str(data.get("agent_type", "terminal")),
            command=tuple(str(item) for item in data.get("command", [])),
            env={str(key): str(value) for key, value in data.get("env", {}).items()},
            cwd=data.get("cwd"),
            session_id=data.get("session_id"),
            scope=str(data.get("scope", "default")),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(frozen=True, slots=True)
class ProcessRef:
    pid: int | None = None
    external_ref: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"pid": self.pid, "external_ref": self.external_ref, "metadata": dict(self.metadata)}


@dataclass(frozen=True, slots=True)
class TerminalSnapshot:
    cols: int = 80
    rows: int = 24
    buffer: str = ""
    cursor_line: int = 0
    cursor_column: int = 0
    captured_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "cols": self.cols,
            "rows": self.rows,
            "buffer": self.buffer,
            "cursor_line": self.cursor_line,
            "cursor_column": self.cursor_column,
            "captured_at": self.captured_at,
        }

    @classmethod
    def from_value(cls, value: Any) -> TerminalSnapshot:
        if isinstance(value, TerminalSnapshot):
            return value
        if not isinstance(value, Mapping):
            return cls(buffer=str(value), captured_at=time.time())
        return cls(
            cols=int(value.get("cols", 80)),
            rows=int(value.get("rows", 24)),
            buffer=str(value.get("buffer", value.get("text", ""))),
            cursor_line=int(value.get("cursor_line", 0)),
            cursor_column=int(value.get("cursor_column", 0)),
            captured_at=float(value.get("captured_at", value.get("timestamp", time.time()))),
        )


@dataclass(frozen=True, slots=True)
class SessionAttachment:
    attachment_id: str
    client_id: str = ""
    read_only: bool = False
    attached_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "attachment_id": self.attachment_id,
            "client_id": self.client_id,
            "read_only": self.read_only,
            "attached_at": self.attached_at,
        }


@dataclass(frozen=True, slots=True)
class SessionEvent:
    event_id: str
    session_id: str
    kind: str
    timestamp: float
    payload: Mapping[str, Any] = field(default_factory=dict)
    sequence: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "session_id": self.session_id,
            "kind": self.kind,
            "timestamp": self.timestamp,
            "payload": dict(self.payload),
            "sequence": self.sequence,
        }


@dataclass(frozen=True, slots=True)
class SessionState:
    session_id: str
    status: str
    workspace_ref: str
    agent_type: str
    provider: str
    external_session_ref: str = ""
    process: ProcessRef | None = None
    terminal: TerminalSnapshot | None = None
    attachments: tuple[SessionAttachment, ...] = ()
    last_durable_state: str = "creating"
    last_event_sequence: int = 0
    updated_at: float = 0.0
    error: str | None = None
    scope: str = "default"

    def __post_init__(self) -> None:
        if self.status not in SESSION_STATUSES:
            raise ValueError(f"unsupported session status: {self.status!r}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "status": self.status,
            "workspace_ref": self.workspace_ref,
            "agent_type": self.agent_type,
            "provider": self.provider,
            "external_session_ref": self.external_session_ref,
            "process": self.process.to_dict() if self.process else None,
            "terminal": self.terminal.to_dict() if self.terminal else None,
            "attachments": [item.to_dict() for item in self.attachments],
            "last_durable_state": self.last_durable_state,
            "last_event_sequence": self.last_event_sequence,
            "updated_at": self.updated_at,
            "error": self.error,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SessionState:
        process = data.get("process")
        terminal = data.get("terminal")
        return cls(
            session_id=str(data["session_id"]),
            status=str(data.get("status", "creating")),
            workspace_ref=str(data.get("workspace_ref", "")),
            agent_type=str(data.get("agent_type", "terminal")),
            provider=str(data.get("provider", "unknown")),
            external_session_ref=str(data.get("external_session_ref", "")),
            process=(
                ProcessRef(
                    pid=int(process["pid"]) if process.get("pid") is not None else None,
                    external_ref=str(process.get("external_ref", "")),
                    metadata=dict(process.get("metadata", {})),
                )
                if isinstance(process, Mapping)
                else None
            ),
            terminal=TerminalSnapshot.from_value(terminal) if terminal else None,
            attachments=tuple(
                SessionAttachment(
                    attachment_id=str(item.get("attachment_id", "")),
                    client_id=str(item.get("client_id", "")),
                    read_only=bool(item.get("read_only", False)),
                    attached_at=float(item.get("attached_at", 0.0)),
                )
                for item in data.get("attachments", [])
            ),
            last_durable_state=str(data.get("last_durable_state", data.get("status", "creating"))),
            last_event_sequence=int(data.get("last_event_sequence", 0)),
            updated_at=float(data.get("updated_at", 0.0)),
            error=data.get("error"),
            scope=str(data.get("scope", "default")),
        )


@dataclass(frozen=True, slots=True)
class SessionRecord:
    spec: SessionSpec
    state: SessionState
    events: tuple[SessionEvent, ...] = ()
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec": self.spec.to_dict(),
            "state": self.state.to_dict(),
            "events": [event.to_dict() for event in self.events],
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SessionRecord:
        return cls(
            spec=SessionSpec.from_dict(data.get("spec", {})),
            state=SessionState.from_dict(data.get("state", {})),
            events=tuple(
                SessionEvent(
                    event_id=str(item.get("event_id", "")),
                    session_id=str(item.get("session_id", "")),
                    kind=str(item.get("kind", "")),
                    timestamp=float(item.get("timestamp", 0.0)),
                    payload=dict(item.get("payload", {})),
                    sequence=int(item.get("sequence", 0)),
                )
                for item in data.get("events", [])
            ),
            schema_version=int(data.get("schema_version", 1)),
        )


@runtime_checkable
class SessionProvider(Protocol):
    async def create(self, spec: SessionSpec) -> AgentSessionRef | Mapping[str, Any]: ...

    async def start(self, ref: AgentSessionRef) -> ProcessRef | Mapping[str, Any] | None: ...

    async def attach(
        self, ref: AgentSessionRef, attachment: SessionAttachment
    ) -> Mapping[str, Any] | None: ...

    async def detach(self, ref: AgentSessionRef, attachment: SessionAttachment | None) -> None: ...

    async def send(self, ref: AgentSessionRef, data: str | bytes) -> Any: ...

    async def signal(self, ref: AgentSessionRef, signal: str) -> Any: ...

    async def snapshot(self, ref: AgentSessionRef) -> TerminalSnapshot | Mapping[str, Any]: ...

    async def terminate(self, ref: AgentSessionRef) -> None: ...

    async def restore(self, ref: AgentSessionRef, state: SessionState) -> Any: ...


@runtime_checkable
class SessionStorePort(Protocol):
    async def save(self, record: SessionRecord) -> None: ...

    async def load(self, session_id: str, *, scope: str = "default") -> SessionRecord | None: ...


class MemorySessionProvider:
    """Deterministic provider for contract tests and local composition."""

    name = "memory"

    def __init__(self) -> None:
        self._refs: dict[tuple[str, str], AgentSessionRef] = {}
        self._buffers: dict[tuple[str, str], str] = {}
        self._started: set[tuple[str, str]] = set()
        self._terminated: set[tuple[str, str]] = set()

    async def create(self, spec: SessionSpec) -> AgentSessionRef:
        session_id = spec.session_id or str(uuid.uuid4())
        ref = AgentSessionRef(session_id, self.name, f"{self.name}:{session_id}", spec.scope)
        key = _provider_key(spec.scope, session_id)
        existing = self._refs.get(key)
        if existing is not None:
            return existing
        self._refs[key] = ref
        self._buffers.setdefault(key, "")
        return ref

    async def start(self, ref: AgentSessionRef) -> ProcessRef:
        key = _provider_key(ref.scope, ref.session_id)
        if key in self._terminated:
            raise SessionError("cannot restart a terminated session")
        self._started.add(key)
        return ProcessRef(external_ref=f"process:{ref.session_id}")

    async def attach(
        self, ref: AgentSessionRef, attachment: SessionAttachment
    ) -> Mapping[str, Any]:
        del attachment
        if _provider_key(ref.scope, ref.session_id) not in self._refs:
            raise SessionError("unknown session reference")
        return {"attached": True}

    async def detach(self, ref: AgentSessionRef, attachment: SessionAttachment | None) -> None:
        del ref, attachment

    async def send(self, ref: AgentSessionRef, data: str | bytes) -> Mapping[str, Any]:
        key = _provider_key(ref.scope, ref.session_id)
        if key not in self._started:
            raise SessionError("session is not started")
        text = data.decode("utf-8", errors="replace") if isinstance(data, bytes) else data
        self._buffers[key] = self._buffers.get(key, "") + text
        return {"accepted": len(text)}

    async def signal(self, ref: AgentSessionRef, signal: str) -> Mapping[str, Any]:
        return {"session_id": ref.session_id, "signal": signal}

    async def snapshot(self, ref: AgentSessionRef) -> TerminalSnapshot:
        return TerminalSnapshot(
            buffer=self._buffers.get(_provider_key(ref.scope, ref.session_id), ""),
            captured_at=time.time(),
        )

    async def terminate(self, ref: AgentSessionRef) -> None:
        key = _provider_key(ref.scope, ref.session_id)
        self._terminated.add(key)
        self._started.discard(key)

    async def restore(self, ref: AgentSessionRef, state: SessionState) -> None:
        """Reassociate an external process/session after provider restart."""
        key = _provider_key(ref.scope, ref.session_id)
        self._refs[key] = ref
        self._buffers.setdefault(
            key,
            state.terminal.buffer if state.terminal is not None else "",
        )
        if state.status not in {"stopped", "failed"}:
            self._started.add(key)


class LocalPtyProvider(MemorySessionProvider):
    """Provider slot for a local PTY implementation; no process is created by the element."""

    name = "local-pty"


class SshSessionProvider(MemorySessionProvider):
    name = "ssh"


class ContainerSessionProvider(MemorySessionProvider):
    name = "container"


class ExternalAgentProvider(MemorySessionProvider):
    name = "external-agent"


class MemorySessionStore:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str], SessionRecord] = {}

    async def save(self, record: SessionRecord) -> None:
        self._records[(record.state.scope, record.state.session_id)] = record

    async def load(self, session_id: str, *, scope: str = "default") -> SessionRecord | None:
        return self._records.get((scope, session_id))


class JsonSessionStore:
    """Atomic, versioned JSON session store with scope-isolated keys."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, session_id: str, scope: str) -> Path:
        key = hashlib.sha256(f"{scope}\0{session_id}".encode()).hexdigest()
        return self.root / f"{key}.json"

    async def save(self, record: SessionRecord) -> None:
        path = self._path(record.state.session_id, record.state.scope)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True), encoding="utf-8"
        )
        tmp.replace(path)

    async def load(self, session_id: str, *, scope: str = "default") -> SessionRecord | None:
        try:
            data = json.loads(self._path(session_id, scope).read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, ValueError):
            return None
        return SessionRecord.from_dict(data)


class PersistentAgentSession:
    """Durable lifecycle facade over an injected terminal/agent provider."""

    ELEMENT_CONTRACT = ElementContract(
        element_id="persistent_agent_session",
        element_version=1,
        owner_repo="oservi",
        canonical_import="oservi.persistent_agent_session",
        canonical_export="PersistentAgentSession",
        input_contract="SessionSpec and provider lifecycle commands",
        output_contract="stable SessionState, SessionEvent and TerminalSnapshot",
        dependency_ports=("SessionProvider", "SessionStorePort"),
        state_model="creating/starting/working/idle/blocked/detached/stopped/failed",
        persistence_model="versioned scope-isolated SessionRecord store",
        authority_declaration=zero_authority(),
        async_contract="all provider and lifecycle operations are awaitable",
        failure_semantics="provider failure marks failed and preserves error for recovery",
        recovery_semantics="restore uses stable session identity and external reference",
        observability_contract="durable lifecycle events and terminal snapshots remain inspectable",
        compatibility_contract="no client/process/GoalRun implementation dependency",
        conformance_suite=(
            "DETACH_REATTACH",
            "CLIENT_DISCONNECT_SURVIVES",
            "PROCESS_RESTART_RESTORE",
            "SESSION_ID_STABLE",
            "REMOTE_ATTACH",
            "BLOCKED_STATE_VISIBLE",
            "TERMINATE_IDEMPOTENT",
            "GOALRUN_AUTHORITY",
        ),
    )

    def __init__(
        self,
        *,
        provider: SessionProvider,
        store: SessionStorePort | None = None,
        clock: Any | None = None,
    ) -> None:
        self.provider = provider
        self.store = store or MemorySessionStore()
        self._clock = clock or time.time
        self._spec: SessionSpec | None = None
        self._ref: AgentSessionRef | None = None
        self._state: SessionState | None = None
        self._events: list[SessionEvent] = []

    async def create(self, spec: SessionSpec | Mapping[str, Any]) -> SessionState:
        normalized = spec if isinstance(spec, SessionSpec) else SessionSpec.from_dict(spec)
        ref_raw = await self.provider.create(normalized)
        ref = (
            ref_raw
            if isinstance(ref_raw, AgentSessionRef)
            else AgentSessionRef(
                session_id=str(ref_raw.get("session_id", normalized.session_id or uuid.uuid4())),
                provider=str(ref_raw.get("provider", getattr(self.provider, "name", "provider"))),
                external_session_ref=str(ref_raw.get("external_session_ref", "")),
                scope=str(ref_raw.get("scope", normalized.scope)),
            )
        )
        now = self._clock()
        self._spec = SessionSpec(
            workspace_ref=normalized.workspace_ref,
            agent_type=normalized.agent_type,
            command=normalized.command,
            env=normalized.env,
            cwd=normalized.cwd,
            session_id=ref.session_id,
            scope=normalized.scope,
            metadata=normalized.metadata,
        )
        self._ref = ref
        self._state = SessionState(
            session_id=ref.session_id,
            status="creating",
            workspace_ref=normalized.workspace_ref,
            agent_type=normalized.agent_type,
            provider=ref.provider,
            external_session_ref=ref.external_session_ref,
            last_durable_state="creating",
            updated_at=now,
            scope=normalized.scope,
        )
        self._record("created", {})
        await self._persist()
        return self._state

    async def start(self) -> SessionState:
        spec, ref, state = self._require()
        del spec
        if state.status == "stopped":
            raise SessionError("cannot start a stopped session")
        if state.status == "working":
            return state
        self._state = self._replace_state(state, status="starting", error=None)
        self._record("starting", {})
        await self._persist()
        try:
            process_raw = await self.provider.start(ref)
            process = self._process(process_raw)
            self._state = self._replace_state(
                self._state,
                status="working",
                process=process,
                last_durable_state="working",
                error=None,
            )
            self._record("started", {"process": process.to_dict() if process else None})
            await self._persist()
            return self._state
        except Exception as exc:
            self._state = self._replace_state(
                self._state, status="failed", last_durable_state="failed", error=str(exc)
            )
            self._record("start_failed", {"error": str(exc)})
            await self._persist()
            raise

    async def attach(
        self,
        attachment: SessionAttachment | Mapping[str, Any] | None = None,
    ) -> SessionState:
        _, ref, state = self._require()
        self._ensure_operable(state, "attach")
        item = (
            attachment
            if isinstance(attachment, SessionAttachment)
            else SessionAttachment(
                attachment_id=str((attachment or {}).get("attachment_id", uuid.uuid4())),
                client_id=str((attachment or {}).get("client_id", "")),
                read_only=bool((attachment or {}).get("read_only", False)),
                attached_at=self._clock(),
            )
        )
        try:
            await self.provider.attach(ref, item)
            attachments = tuple(
                existing
                for existing in state.attachments
                if existing.attachment_id != item.attachment_id
            ) + (item,)
            self._state = self._replace_state(
                state,
                status="working" if state.status != "blocked" else state.status,
                attachments=attachments,
            )
            self._record("attached", item.to_dict())
            await self._persist()
            return self._state
        except Exception as exc:
            self._state = self._replace_state(state, status="failed", error=str(exc))
            self._record("attach_failed", {"error": str(exc)})
            await self._persist()
            raise

    async def detach(
        self, attachment: SessionAttachment | Mapping[str, Any] | None = None
    ) -> SessionState:
        _, ref, state = self._require()
        item = self._find_attachment(attachment, state)
        await self.provider.detach(ref, item)
        remaining = tuple(
            existing
            for existing in state.attachments
            if item is None or existing.attachment_id != item.attachment_id
        )
        self._state = self._replace_state(state, status="detached", attachments=remaining)
        self._record("detached", {"attachment_id": item.attachment_id if item else None})
        await self._persist()
        return self._state

    async def restore(
        self, session_id: str | None = None, *, scope: str = "default"
    ) -> SessionState:
        wanted = session_id or (self._state.session_id if self._state else None)
        if not wanted:
            raise SessionError("session_id is required for restore")
        record = await self.store.load(wanted, scope=scope)
        if record is None:
            raise SessionError(f"session not found: {wanted}")
        self._spec = record.spec
        self._state = record.state
        self._events = list(record.events)
        self._ref = AgentSessionRef(
            session_id=record.state.session_id,
            provider=record.state.provider,
            external_session_ref=record.state.external_session_ref,
            scope=record.state.scope,
        )
        restore = getattr(self.provider, "restore", None)
        if callable(restore):
            restored = restore(self._ref, self._state)
            if inspect.isawaitable(restored):
                await restored
        self._state = self._replace_state(self._state)
        self._record("restored", {})
        await self._persist()
        return self._state

    async def send(self, data: str | bytes) -> Any:
        _, ref, state = self._require()
        self._ensure_operable(state, "send")
        result = await self.provider.send(ref, data)
        self._state = self._replace_state(state, status="working", error=None)
        self._record("sent", {"bytes": len(data)})
        await self._persist()
        return result

    async def signal(self, signal: str) -> Any:
        _, ref, state = self._require()
        self._ensure_operable(state, "signal")
        result = await self.provider.signal(ref, signal)
        self._record("signal", {"signal": signal})
        self._state = self._replace_state(state)
        await self._persist()
        return result

    async def snapshot(self) -> TerminalSnapshot:
        _, ref, state = self._require()
        terminal = TerminalSnapshot.from_value(await self.provider.snapshot(ref))
        self._state = self._replace_state(state, terminal=terminal)
        self._record("snapshot", {"captured_at": terminal.captured_at})
        await self._persist()
        return terminal

    async def state(self) -> SessionState:
        if self._state is None:
            raise SessionError("session has not been created or restored")
        return self._state

    async def terminate(self) -> SessionState:
        if self._state is None or self._ref is None:
            raise SessionError("session has not been created or restored")
        if self._state.status == "stopped":
            return self._state
        await self.provider.terminate(self._ref)
        self._state = self._replace_state(
            self._state, status="stopped", last_durable_state="stopped", error=None
        )
        self._record("terminated", {})
        await self._persist()
        return self._state

    def _require(self) -> tuple[SessionSpec, AgentSessionRef, SessionState]:
        if self._spec is None or self._ref is None or self._state is None:
            raise SessionError("session has not been created or restored")
        return self._spec, self._ref, self._state

    @staticmethod
    def _ensure_operable(state: SessionState, operation: str) -> None:
        if state.status in {"stopped", "failed"}:
            raise SessionError(f"cannot {operation} a {state.status} session")

    def _replace_state(self, state: SessionState, **changes: Any) -> SessionState:
        changes["updated_at"] = self._clock()
        changes["last_event_sequence"] = len(self._events) + 1
        if "last_durable_state" not in changes and "status" in changes:
            changes["last_durable_state"] = changes["status"]
        # Preserve typed provider values (ProcessRef/TerminalSnapshot and
        # attachment tuples) instead of round-tripping them through JSON.
        return replace(state, **changes)

    def _record(self, kind: str, payload: Mapping[str, Any]) -> None:
        session_id = self._state.session_id if self._state else ""
        self._events.append(
            SessionEvent(
                event_id=str(uuid.uuid4()),
                session_id=session_id,
                kind=kind,
                timestamp=self._clock(),
                payload=dict(payload),
                sequence=len(self._events) + 1,
            )
        )

    async def _persist(self) -> None:
        if self._spec is not None and self._state is not None:
            await self.store.save(SessionRecord(self._spec, self._state, tuple(self._events)))

    @staticmethod
    def _process(value: ProcessRef | Mapping[str, Any] | None) -> ProcessRef | None:
        if value is None or isinstance(value, ProcessRef):
            return value
        return ProcessRef(
            pid=int(value["pid"]) if value.get("pid") is not None else None,
            external_ref=str(value.get("external_ref", value.get("process_ref", ""))),
            metadata=dict(value.get("metadata", {})),
        )

    @staticmethod
    def _find_attachment(
        value: SessionAttachment | Mapping[str, Any] | None, state: SessionState
    ) -> SessionAttachment | None:
        if isinstance(value, SessionAttachment):
            return value
        if isinstance(value, Mapping):
            wanted = str(value.get("attachment_id", ""))
            return next((item for item in state.attachments if item.attachment_id == wanted), None)
        return state.attachments[-1] if state.attachments else None


__all__ = [
    "AgentSessionRef",
    "ContainerSessionProvider",
    "ExternalAgentProvider",
    "JsonSessionStore",
    "LocalPtyProvider",
    "MemorySessionProvider",
    "MemorySessionStore",
    "PersistentAgentSession",
    "ProcessRef",
    "SessionAttachment",
    "SessionError",
    "SessionEvent",
    "SessionId",
    "SessionProvider",
    "SessionProviderUnavailable",
    "SessionRecord",
    "SessionSpec",
    "SessionState",
    "SessionStorePort",
    "SshSessionProvider",
    "TerminalSnapshot",
]
