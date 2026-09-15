"""Canonical isolated workspace allocation element.

``WorkspaceFleet`` manages where work happens, not what an agent should do.
Provider implementations own filesystem, Git, container, or SSH mechanics;
the fleet only enforces identity, scope, lease, and collision invariants.
``compare`` returns evidence-shaped differences and never chooses a winner.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from obase.element_contract import ElementContract, zero_authority

WORKSPACE_TYPES = (
    "git_worktree",
    "directory_copy",
    "container",
    "remote_ssh",
    "ephemeral",
)


class WorkspaceError(Exception):
    """Workspace lifecycle or isolation error."""


class WorkspaceConflict(WorkspaceError):
    """A scope, branch, lease, or workspace ownership invariant was violated."""


WorkspaceId = str


def _provider_key(scope: str, workspace_id: str) -> tuple[str, str]:
    return scope, workspace_id


@dataclass(frozen=True, slots=True)
class WorkspaceSpec:
    source_ref: str
    workspace_type: str = "ephemeral"
    workspace_id: WorkspaceId | None = None
    revision: str | None = None
    branch: str | None = None
    owner_id: str = ""
    scope: str = "default"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.source_ref:
            raise ValueError("source_ref must be non-empty")
        if self.workspace_type not in WORKSPACE_TYPES:
            raise ValueError(f"unsupported workspace type: {self.workspace_type!r}")
        if self.workspace_id is not None:
            candidate = Path(str(self.workspace_id))
            if candidate.is_absolute() or candidate.name != str(self.workspace_id):
                raise ValueError("workspace_id must be a single safe identifier")

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_ref": self.source_ref,
            "workspace_type": self.workspace_type,
            "workspace_id": self.workspace_id,
            "revision": self.revision,
            "branch": self.branch,
            "owner_id": self.owner_id,
            "scope": self.scope,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkspaceSpec:
        return cls(
            source_ref=str(data.get("source_ref", data.get("repo", ""))),
            workspace_type=str(data.get("workspace_type", data.get("kind", "ephemeral"))),
            workspace_id=(
                str(data["workspace_id"]) if data.get("workspace_id") is not None else None
            ),
            revision=data.get("revision"),
            branch=data.get("branch"),
            owner_id=str(data.get("owner_id", "")),
            scope=str(data.get("scope", "default")),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(frozen=True, slots=True)
class WorkspaceLease:
    workspace_id: WorkspaceId
    lease_id: str
    owner_id: str
    scope: str = "default"
    expires_at: float | None = None
    active: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "lease_id": self.lease_id,
            "owner_id": self.owner_id,
            "scope": self.scope,
            "expires_at": self.expires_at,
            "active": self.active,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkspaceLease:
        return cls(
            workspace_id=str(data.get("workspace_id", "")),
            lease_id=str(data.get("lease_id", "")),
            owner_id=str(data.get("owner_id", "")),
            scope=str(data.get("scope", "default")),
            expires_at=data.get("expires_at"),
            active=bool(data.get("active", True)),
        )


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    workspace_id: WorkspaceId
    revision: str
    files: Mapping[str, str] = field(default_factory=dict)
    path: str | None = None
    captured_at: float = 0.0
    schema_version: int = 1
    scope: str = "default"

    @property
    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(dict(self.files), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "revision": self.revision,
            "files": dict(self.files),
            "path": self.path,
            "captured_at": self.captured_at,
            "schema_version": self.schema_version,
            "scope": self.scope,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkspaceSnapshot:
        return cls(
            workspace_id=str(data.get("workspace_id", "")),
            revision=str(data.get("revision", "")),
            files={str(key): str(value) for key, value in data.get("files", {}).items()},
            path=data.get("path"),
            captured_at=float(data.get("captured_at", 0.0)),
            schema_version=int(data.get("schema_version", 1)),
            scope=str(data.get("scope", "default")),
        )


@dataclass(frozen=True, slots=True)
class WorkspaceDiff:
    left_workspace_id: WorkspaceId
    right_workspace_id: WorkspaceId
    added: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()
    changed: tuple[str, ...] = ()
    left_revision: str = ""
    right_revision: str = ""

    @property
    def equal(self) -> bool:
        return not (self.added or self.removed or self.changed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_workspace_id": self.left_workspace_id,
            "right_workspace_id": self.right_workspace_id,
            "added": list(self.added),
            "removed": list(self.removed),
            "changed": list(self.changed),
            "left_revision": self.left_revision,
            "right_revision": self.right_revision,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceCandidate:
    candidate_id: str
    workspace_id: WorkspaceId
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "workspace_id": self.workspace_id,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class WorkspaceBinding:
    binding_id: str
    workspace_id: WorkspaceId
    session_ref: str
    owner_id: str = ""
    scope: str = "default"

    def to_dict(self) -> dict[str, Any]:
        return {
            "binding_id": self.binding_id,
            "workspace_id": self.workspace_id,
            "session_ref": self.session_ref,
            "owner_id": self.owner_id,
            "scope": self.scope,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkspaceBinding:
        return cls(
            binding_id=str(data.get("binding_id", "")),
            workspace_id=str(data.get("workspace_id", "")),
            session_ref=str(data.get("session_ref", "")),
            owner_id=str(data.get("owner_id", "")),
            scope=str(data.get("scope", "default")),
        )


@dataclass(frozen=True, slots=True)
class WorkspaceRecord:
    spec: WorkspaceSpec
    lease: WorkspaceLease
    path: str | None = None
    bindings: tuple[WorkspaceBinding, ...] = ()
    snapshot: WorkspaceSnapshot | None = None
    destroyed: bool = False
    schema_version: int = 1

    @property
    def workspace_id(self) -> str:
        return self.lease.workspace_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "spec": self.spec.to_dict(),
            "lease": self.lease.to_dict(),
            "path": self.path,
            "bindings": [item.to_dict() for item in self.bindings],
            "snapshot": self.snapshot.to_dict() if self.snapshot else None,
            "destroyed": self.destroyed,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> WorkspaceRecord:
        snapshot = data.get("snapshot")
        return cls(
            spec=WorkspaceSpec.from_dict(data.get("spec", {})),
            lease=WorkspaceLease.from_dict(data.get("lease", {})),
            path=data.get("path"),
            bindings=tuple(WorkspaceBinding.from_dict(item) for item in data.get("bindings", [])),
            snapshot=(
                snapshot
                if isinstance(snapshot, WorkspaceSnapshot)
                else WorkspaceSnapshot.from_dict(snapshot)
                if isinstance(snapshot, Mapping)
                else None
            ),
            destroyed=bool(data.get("destroyed", False)),
            schema_version=int(data.get("schema_version", 1)),
        )


@runtime_checkable
class WorkspaceProvider(Protocol):
    async def allocate(self, spec: WorkspaceSpec) -> Mapping[str, Any] | WorkspaceRecord: ...

    async def fork(
        self, source: WorkspaceRecord, spec: WorkspaceSpec
    ) -> Mapping[str, Any] | WorkspaceRecord: ...

    async def bind(self, record: WorkspaceRecord, binding: WorkspaceBinding) -> Any: ...

    async def snapshot(self, record: WorkspaceRecord) -> WorkspaceSnapshot | Mapping[str, Any]: ...

    async def diff(
        self, left: WorkspaceSnapshot, right: WorkspaceSnapshot
    ) -> WorkspaceDiff | Mapping[str, Any]: ...

    async def release(self, record: WorkspaceRecord) -> None: ...

    async def destroy(self, record: WorkspaceRecord) -> None: ...


@runtime_checkable
class WorkspaceStorePort(Protocol):
    async def save(self, record: WorkspaceRecord) -> None: ...

    async def load(
        self, workspace_id: str, *, scope: str = "default"
    ) -> WorkspaceRecord | None: ...


class MemoryWorkspaceProvider:
    """Non-I/O provider used by tests and explicit dry-run compositions."""

    name = "memory"

    def __init__(self) -> None:
        self.records: dict[tuple[str, str], WorkspaceRecord] = {}
        self.files: dict[tuple[str, str], dict[str, str]] = {}

    async def allocate(self, spec: WorkspaceSpec) -> WorkspaceRecord:
        workspace_id = spec.workspace_id or str(uuid.uuid4())
        key = _provider_key(spec.scope, workspace_id)
        if key in self.records:
            raise WorkspaceConflict(f"workspace id collision in scope {spec.scope}: {workspace_id}")
        lease = WorkspaceLease(workspace_id, f"lease:{workspace_id}", spec.owner_id, spec.scope)
        record = WorkspaceRecord(spec, lease)
        self.records[key] = record
        self.files.setdefault(key, {})
        return record

    async def fork(self, source: WorkspaceRecord, spec: WorkspaceSpec) -> WorkspaceRecord:
        record = await self.allocate(spec)
        self.files[_provider_key(spec.scope, record.workspace_id)] = dict(
            self.files.get(_provider_key(source.spec.scope, source.workspace_id), {})
        )
        return record

    async def bind(self, record: WorkspaceRecord, binding: WorkspaceBinding) -> None:
        del record, binding

    async def snapshot(self, record: WorkspaceRecord) -> WorkspaceSnapshot:
        return WorkspaceSnapshot(
            record.workspace_id,
            record.spec.revision or "working",
            dict(self.files.get(_provider_key(record.spec.scope, record.workspace_id), {})),
            captured_at=time.time(),
            scope=record.spec.scope,
        )

    async def diff(self, left: WorkspaceSnapshot, right: WorkspaceSnapshot) -> WorkspaceDiff:
        left_files, right_files = dict(left.files), dict(right.files)
        return WorkspaceDiff(
            left.workspace_id,
            right.workspace_id,
            tuple(sorted(set(right_files) - set(left_files))),
            tuple(sorted(set(left_files) - set(right_files))),
            tuple(
                sorted(
                    key
                    for key in set(left_files) & set(right_files)
                    if left_files[key] != right_files[key]
                )
            ),
            left.revision,
            right.revision,
        )

    async def release(self, record: WorkspaceRecord) -> None:
        key = _provider_key(record.spec.scope, record.workspace_id)
        self.records[key] = WorkspaceRecord(
            record.spec,
            WorkspaceLease(
                record.lease.workspace_id,
                record.lease.lease_id,
                record.lease.owner_id,
                record.lease.scope,
                record.lease.expires_at,
                False,
            ),
            record.path,
            record.bindings,
            record.snapshot,
            record.destroyed,
        )

    async def destroy(self, record: WorkspaceRecord) -> None:
        key = _provider_key(record.spec.scope, record.workspace_id)
        self.files.pop(key, None)
        self.records[key] = WorkspaceRecord(
            record.spec,
            record.lease,
            record.path,
            record.bindings,
            record.snapshot,
            True,
        )


class LocalWorkspaceProvider(MemoryWorkspaceProvider):
    """Reference local provider that copies a source under an owned root."""

    name = "local"

    def __init__(self, root: str | Path) -> None:
        super().__init__()
        self.root = Path(root).resolve()

    async def allocate(self, spec: WorkspaceSpec) -> WorkspaceRecord:
        record = await super().allocate(spec)
        return self._materialize(record, Path(spec.source_ref))

    async def fork(self, source: WorkspaceRecord, spec: WorkspaceSpec) -> WorkspaceRecord:
        record = await super().allocate(spec)
        source_path = Path(source.path) if source.path else Path(source.spec.source_ref)
        return self._materialize(record, source_path)

    def _materialize(self, record: WorkspaceRecord, source: Path) -> WorkspaceRecord:
        self.root.mkdir(parents=True, exist_ok=True)
        scope_part = (
            "default"
            if record.spec.scope == "default"
            else hashlib.sha256(record.spec.scope.encode("utf-8")).hexdigest()[:16]
        )
        destination = (self.root / scope_part / record.workspace_id).resolve()
        if self.root not in destination.parents:
            raise WorkspaceError("workspace destination escapes provider root")
        source = source.resolve()
        if source.exists() and source.is_dir():
            shutil.copytree(source, destination, dirs_exist_ok=False)
        else:
            destination.mkdir(parents=True, exist_ok=False)
        updated = WorkspaceRecord(
            record.spec,
            record.lease,
            str(destination),
            record.bindings,
            record.snapshot,
            False,
        )
        self.records[_provider_key(record.spec.scope, record.workspace_id)] = updated
        return updated

    async def destroy(self, record: WorkspaceRecord) -> None:
        if record.path:
            path = Path(record.path).resolve()
            if self.root in path.parents and path != self.root:
                shutil.rmtree(path, ignore_errors=False)
        await super().destroy(record)


class MemoryWorkspaceStore:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str], WorkspaceRecord] = {}

    async def save(self, record: WorkspaceRecord) -> None:
        self._records[(record.lease.scope, record.workspace_id)] = record

    async def load(self, workspace_id: str, *, scope: str = "default") -> WorkspaceRecord | None:
        return self._records.get((scope, workspace_id))


class JsonWorkspaceStore:
    """Atomic, versioned workspace record store for restart/restore tests."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, workspace_id: str, scope: str) -> Path:
        key = hashlib.sha256(f"{scope}\0{workspace_id}".encode()).hexdigest()
        return self.root / f"{key}.json"

    async def save(self, record: WorkspaceRecord) -> None:
        path = self._path(record.workspace_id, record.spec.scope)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(path)

    async def load(self, workspace_id: str, *, scope: str = "default") -> WorkspaceRecord | None:
        path = self._path(workspace_id, scope)
        try:
            return WorkspaceRecord.from_dict(json.loads(path.read_text(encoding="utf-8")))
        except (FileNotFoundError, OSError, ValueError, TypeError, KeyError):
            return None


class WorkspaceFleet:
    """Lease and isolation facade over an injected workspace provider."""

    ELEMENT_CONTRACT = ElementContract(
        element_id="workspace_fleet",
        element_version=1,
        owner_repo="oservi",
        canonical_import="oservi.workspace_fleet",
        canonical_export="WorkspaceFleet",
        input_contract="scoped WorkspaceSpec, lease and binding requests",
        output_contract="WorkspaceRecord, snapshots, diffs and bindings",
        dependency_ports=("WorkspaceProvider", "WorkspaceStorePort"),
        state_model="scope-keyed workspace records, leases and bindings",
        persistence_model="versioned scope-isolated WorkspaceRecord store",
        authority_declaration=zero_authority(),
        async_contract="provider operations are awaitable; provider owns I/O scheduling",
        failure_semantics="collision/cross-scope access fails before provider mutation",
        recovery_semantics="records and leases restore by stable workspace id",
        observability_contract="snapshot digest, lease and diff are inspectable",
        compatibility_contract="compare returns differences only; no winner authority",
        conformance_suite=(
            "WORKSPACE_ISOLATION",
            "WORKTREE_FANOUT",
            "CROSS_WRITE",
            "BRANCH_COLLISION",
            "SNAPSHOT_REPRODUCIBLE",
            "DIFF_CORRECT",
            "RELEASE_IDEMPOTENT",
            "REMOTE_WORKSPACE",
            "SECOND_EXECUTION_AUTHORITY",
        ),
    )

    def __init__(
        self,
        *,
        provider: WorkspaceProvider | None = None,
        store: WorkspaceStorePort | None = None,
    ) -> None:
        self.provider = provider or MemoryWorkspaceProvider()
        self.store = store or MemoryWorkspaceStore()
        self._records: dict[tuple[str, str], WorkspaceRecord] = {}
        self._branches: dict[tuple[str, str], str] = {}

    async def allocate(self, spec: WorkspaceSpec | Mapping[str, Any]) -> WorkspaceRecord:
        normalized = spec if isinstance(spec, WorkspaceSpec) else WorkspaceSpec.from_dict(spec)
        await self._ensure_available(normalized)
        self._check_branch(normalized)
        raw = await self.provider.allocate(normalized)
        record = self._normalize_record(raw, normalized)
        self._register(record)
        await self.store.save(record)
        return record

    async def fork(
        self,
        source: str | WorkspaceRecord,
        spec: WorkspaceSpec | Mapping[str, Any] | None = None,
        *,
        scope: str = "default",
    ) -> WorkspaceRecord:
        source_record = await self._get(source, scope=scope)
        normalized = (
            spec
            if isinstance(spec, WorkspaceSpec)
            else WorkspaceSpec.from_dict(spec)
            if spec is not None
            else WorkspaceSpec(
                source_ref=source_record.spec.source_ref,
                workspace_type=source_record.spec.workspace_type,
                revision=source_record.spec.revision,
                owner_id=source_record.spec.owner_id,
                scope=source_record.spec.scope,
            )
        )
        await self._ensure_available(normalized)
        self._check_scope(source_record, normalized.scope)
        self._check_branch(normalized)
        raw = await self.provider.fork(source_record, normalized)
        record = self._normalize_record(raw, normalized)
        self._register(record)
        await self.store.save(record)
        return record

    async def bind(
        self,
        workspace: str | WorkspaceRecord,
        session_ref: str | WorkspaceBinding,
        *,
        owner_id: str = "",
        scope: str = "default",
    ) -> WorkspaceBinding:
        record = await self._get(workspace, scope=scope)
        if isinstance(session_ref, WorkspaceBinding):
            binding = session_ref
        else:
            binding = WorkspaceBinding(
                binding_id=str(uuid.uuid4()),
                workspace_id=record.workspace_id,
                session_ref=session_ref,
                owner_id=owner_id or record.spec.owner_id,
                scope=record.spec.scope,
            )
        self._check_scope(record, binding.scope)
        await self.provider.bind(record, binding)
        updated = WorkspaceRecord(
            record.spec,
            record.lease,
            record.path,
            tuple(item for item in record.bindings if item.binding_id != binding.binding_id)
            + (binding,),
            record.snapshot,
            record.destroyed,
        )
        await self._replace(updated)
        return binding

    async def snapshot(
        self, workspace: str | WorkspaceRecord, *, scope: str = "default"
    ) -> WorkspaceSnapshot:
        record = await self._get(workspace, scope=scope)
        self._ensure_live(record)
        snapshot_raw = await self.provider.snapshot(record)
        snapshot = (
            snapshot_raw
            if isinstance(snapshot_raw, WorkspaceSnapshot)
            else WorkspaceSnapshot(
                workspace_id=str(snapshot_raw.get("workspace_id", record.workspace_id)),
                revision=str(snapshot_raw.get("revision", record.spec.revision or "working")),
                files={str(k): str(v) for k, v in snapshot_raw.get("files", {}).items()},
                path=snapshot_raw.get("path", record.path),
                captured_at=float(snapshot_raw.get("captured_at", time.time())),
                scope=str(snapshot_raw.get("scope", record.spec.scope)),
            )
        )
        await self._replace(
            WorkspaceRecord(
                record.spec,
                record.lease,
                record.path,
                record.bindings,
                snapshot,
                record.destroyed,
            )
        )
        return snapshot

    async def diff(
        self,
        left: str | WorkspaceRecord | WorkspaceSnapshot,
        right: str | WorkspaceRecord | WorkspaceSnapshot,
        *,
        scope: str = "default",
        left_scope: str | None = None,
        right_scope: str | None = None,
    ) -> WorkspaceDiff:
        left_snapshot = (
            left
            if isinstance(left, WorkspaceSnapshot)
            else await self.snapshot(left, scope=left_scope or scope)
        )
        right_snapshot = (
            right
            if isinstance(right, WorkspaceSnapshot)
            else await self.snapshot(right, scope=right_scope or scope)
        )
        self._check_snapshot_scope(left_snapshot, right_snapshot)
        raw = await self.provider.diff(left_snapshot, right_snapshot)
        if isinstance(raw, WorkspaceDiff):
            return raw
        return WorkspaceDiff(
            left_snapshot.workspace_id,
            right_snapshot.workspace_id,
            tuple(str(item) for item in raw.get("added", [])),
            tuple(str(item) for item in raw.get("removed", [])),
            tuple(str(item) for item in raw.get("changed", [])),
            left_snapshot.revision,
            right_snapshot.revision,
        )

    async def compare(
        self,
        left: str | WorkspaceRecord | WorkspaceSnapshot,
        right: str | WorkspaceRecord | WorkspaceSnapshot,
        *,
        scope: str = "default",
        left_scope: str | None = None,
        right_scope: str | None = None,
    ) -> WorkspaceDiff:
        """Return a diff only; this method has no winner or acceptance output."""

        return await self.diff(
            left,
            right,
            scope=scope,
            left_scope=left_scope,
            right_scope=right_scope,
        )

    async def release(
        self, workspace: str | WorkspaceRecord, *, scope: str = "default"
    ) -> WorkspaceRecord:
        record = await self._get(workspace, scope=scope)
        if not record.lease.active:
            return record
        await self.provider.release(record)
        updated = WorkspaceRecord(
            record.spec,
            WorkspaceLease(
                record.lease.workspace_id,
                record.lease.lease_id,
                record.lease.owner_id,
                record.lease.scope,
                record.lease.expires_at,
                False,
            ),
            record.path,
            record.bindings,
            record.snapshot,
            record.destroyed,
        )
        await self._replace(updated)
        return updated

    async def destroy(
        self, workspace: str | WorkspaceRecord, *, scope: str = "default"
    ) -> WorkspaceRecord:
        record = await self._get(workspace, scope=scope)
        if record.destroyed:
            return record
        await self.provider.destroy(record)
        updated = WorkspaceRecord(
            record.spec,
            record.lease,
            record.path,
            record.bindings,
            record.snapshot,
            True,
        )
        await self._replace(updated)
        if record.spec.branch:
            self._branches.pop((record.spec.scope, record.spec.branch), None)
        return updated

    async def _get(
        self, workspace: str | WorkspaceRecord, *, scope: str = "default"
    ) -> WorkspaceRecord:
        record: WorkspaceRecord | None
        if isinstance(workspace, WorkspaceRecord):
            record = workspace
            self._check_scope(record, scope)
        else:
            record = self._records.get((scope, workspace))
            if record is None:
                record = await self.store.load(workspace, scope=scope)
        if record is None:
            raise WorkspaceError(f"workspace not found: {workspace}")
        self._register(record)
        return record

    def _register(self, record: WorkspaceRecord) -> None:
        key = (record.spec.scope, record.workspace_id)
        self._records[key] = record
        if record.spec.branch and not record.destroyed:
            key = (record.spec.scope, record.spec.branch)
            existing = self._branches.get(key)
            if existing is not None and existing != record.workspace_id and not record.destroyed:
                raise WorkspaceConflict(
                    f"branch collision in scope {record.spec.scope}: {record.spec.branch}"
                )
            self._branches[key] = record.workspace_id
        elif record.spec.branch:
            branch_key = (record.spec.scope, record.spec.branch)
            if self._branches.get(branch_key) == record.workspace_id:
                self._branches.pop(branch_key, None)

    async def _replace(self, record: WorkspaceRecord) -> None:
        self._register(record)
        # Persistence is deliberately a port; provider/store owns its I/O.
        await self.store.save(record)

    def _normalize_record(
        self, raw: Mapping[str, Any] | WorkspaceRecord, spec: WorkspaceSpec
    ) -> WorkspaceRecord:
        if isinstance(raw, WorkspaceRecord):
            self._check_scope(raw, spec.scope)
            if spec.workspace_id is not None and raw.workspace_id != spec.workspace_id:
                raise WorkspaceConflict("provider changed the requested workspace id")
            return raw
        workspace_id = str(raw.get("workspace_id", spec.workspace_id or uuid.uuid4()))
        lease = WorkspaceLease(
            workspace_id,
            str(raw.get("lease_id", f"lease:{workspace_id}")),
            str(raw.get("owner_id", spec.owner_id)),
            str(raw.get("scope", spec.scope)),
            raw.get("expires_at"),
            bool(raw.get("active", True)),
        )
        return WorkspaceRecord(spec, lease, raw.get("path"))

    async def _ensure_available(self, spec: WorkspaceSpec) -> None:
        if spec.workspace_id is None:
            return
        key = (spec.scope, spec.workspace_id)
        existing = self._records.get(key)
        if existing is None:
            existing = await self.store.load(spec.workspace_id, scope=spec.scope)
        if existing is not None:
            raise WorkspaceConflict(
                f"workspace id collision in scope {spec.scope}: {spec.workspace_id}"
            )

    def _check_branch(self, spec: WorkspaceSpec) -> None:
        if not spec.branch:
            return
        existing = self._branches.get((spec.scope, spec.branch))
        if existing is not None:
            raise WorkspaceConflict(f"branch collision in scope {spec.scope}: {spec.branch}")

    @staticmethod
    def _check_scope(record: WorkspaceRecord, scope: str) -> None:
        if record.spec.scope != scope:
            raise WorkspaceConflict("cross-scope workspace access")

    @staticmethod
    def _ensure_live(record: WorkspaceRecord) -> None:
        if record.destroyed:
            raise WorkspaceError("workspace has been destroyed")

    @staticmethod
    def _check_snapshot_scope(left: WorkspaceSnapshot, right: WorkspaceSnapshot) -> None:
        # Workspace ids are opaque; provider/fleet scope checks happen before snapshots.
        if not left.workspace_id or not right.workspace_id:
            raise WorkspaceError("snapshot must identify both workspaces")
        if left.scope != right.scope:
            raise WorkspaceConflict("cross-scope snapshot comparison")


__all__ = [
    "WORKSPACE_TYPES",
    "JsonWorkspaceStore",
    "LocalWorkspaceProvider",
    "MemoryWorkspaceProvider",
    "MemoryWorkspaceStore",
    "WorkspaceBinding",
    "WorkspaceCandidate",
    "WorkspaceConflict",
    "WorkspaceDiff",
    "WorkspaceError",
    "WorkspaceFleet",
    "WorkspaceId",
    "WorkspaceLease",
    "WorkspaceProvider",
    "WorkspaceRecord",
    "WorkspaceSnapshot",
    "WorkspaceSpec",
    "WorkspaceStorePort",
]
