"""Tamper-evident, human-reviewed trading proposals. No broker or order execution API."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal
from uuid import uuid4

ProposalState = Literal["pending_approval", "approved", "rejected"]
ReviewDecision = Literal["approve", "reject"]
_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9._:/-]{0,31}$")
_GENESIS_HASH = "0" * 64


class ProposalError(ValueError):
    """Invalid proposal, invalid review, or compromised proposal journal."""


def _now_iso(value: datetime | str | None = None) -> str:
    if value is None:
        moment = datetime.now(UTC)
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ProposalError("timestamp must be ISO-8601") from exc
    else:
        moment = value
    if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() is None:
        raise ProposalError("timestamps must include a timezone")
    return moment.astimezone(UTC).isoformat(timespec="seconds")


def _quantity(value: str | Decimal | int | float) -> str:
    if isinstance(value, bool):
        raise ProposalError("quantity must be a positive finite decimal")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ProposalError("quantity must be a positive finite decimal") from exc
    if not number.is_finite() or number <= 0:
        raise ProposalError("quantity must be a positive finite decimal")
    exponent = number.as_tuple().exponent
    if number.adjusted() > 62 or exponent < -63:
        raise ProposalError("quantity precision or magnitude is too large")
    normalized = format(number.normalize(), "f")
    if len(normalized) > 64:
        raise ProposalError("quantity precision or magnitude is too large")
    return normalized


@dataclass(frozen=True)
class TradeProposal:
    proposal_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    quantity: str
    rationale: str
    created_by: str
    state: ProposalState
    created_at: str
    updated_at: str
    reviewed_by: str | None = None
    review_note: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.proposal_id, str) or not re.fullmatch(r"[0-9a-f-]{36}", self.proposal_id):
            raise ProposalError("proposal_id must be a UUID")
        if not isinstance(self.symbol, str) or not _SYMBOL.fullmatch(self.symbol):
            raise ProposalError("symbol must be uppercase and use only market-symbol characters")
        if self.side not in {"BUY", "SELL"}:
            raise ProposalError("side must be BUY or SELL")
        _quantity(self.quantity)
        if not isinstance(self.rationale, str) or not self.rationale.strip() or len(self.rationale) > 4000:
            raise ProposalError("rationale must contain 1..4000 characters")
        if not isinstance(self.created_by, str) or not self.created_by.strip() or len(self.created_by) > 160:
            raise ProposalError("created_by must identify a proposer")
        if self.state not in {"pending_approval", "approved", "rejected"}:
            raise ProposalError("invalid proposal state")
        _now_iso(self.created_at)
        _now_iso(self.updated_at)
        if self.reviewed_by is not None and (not self.reviewed_by.strip() or len(self.reviewed_by) > 160):
            raise ProposalError("reviewed_by must be a non-empty identity")
        if self.review_note is not None and len(self.review_note) > 2000:
            raise ProposalError("review_note may not exceed 2000 characters")
        if self.state == "pending_approval" and self.reviewed_by is not None:
            raise ProposalError("pending proposal cannot have a reviewer")
        if self.state != "pending_approval" and not self.reviewed_by:
            raise ProposalError("reviewed proposal must identify the reviewer")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "proposal_id": self.proposal_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": _quantity(self.quantity),
            "rationale": self.rationale,
            "created_by": self.created_by,
            "state": self.state,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "reviewed_by": self.reviewed_by,
            "review_note": self.review_note,
        }

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "TradeProposal":
        return cls(**value)  # type: ignore[arg-type]


class ProposalStore:
    """Append-only hash-chained proposal snapshots guarded by an OS file lock.

    This class exposes proposal creation/review only. Approval records intent and
    cannot submit, route, simulate, or execute a financial order.
    """

    schema_version = 1
    _thread_lock = threading.RLock()

    def __init__(self, path: str | Path) -> None:
        requested = Path(path).expanduser()
        if requested.exists() and requested.is_symlink():
            raise ProposalError("proposal journal must not be a symbolic link")
        self.path = requested.parent.resolve() / requested.name

    @staticmethod
    def _canonical(value: dict[str, object]) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    def _load_unlocked(self, stream) -> tuple[dict[str, TradeProposal], str, int]:
        stream.seek(0)
        raw = stream.read()
        if not raw:
            return {}, _GENESIS_HASH, 0
        try:
            content = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ProposalError("proposal journal is not UTF-8") from exc
        if not content.endswith("\n"):
            raise ProposalError("proposal journal has an incomplete final record")
        proposals: dict[str, TradeProposal] = {}
        previous_hash, expected_seq = _GENESIS_HASH, 1
        for line_no, line in enumerate(content.splitlines(), 1):
            try:
                entry = json.loads(line)
                if entry.get("schema_version") != self.schema_version:
                    raise ProposalError(f"unsupported proposal journal schema at line {line_no}")
                body = {key: entry[key] for key in ("seq", "prev_hash", "event", "proposal")}
                if body["seq"] != expected_seq or body["prev_hash"] != previous_hash:
                    raise ProposalError(f"proposal journal chain discontinuity at line {line_no}")
                digest = hashlib.sha256(self._canonical(body)).hexdigest()
                if not isinstance(entry.get("hash"), str) or entry["hash"] != digest:
                    raise ProposalError(f"proposal journal hash mismatch at line {line_no}")
                proposal = TradeProposal.from_dict(body["proposal"])
                event = body["event"]
                if event == "created":
                    if proposal.proposal_id in proposals or proposal.state != "pending_approval":
                        raise ProposalError(f"invalid proposal creation event at line {line_no}")
                elif event == "reviewed":
                    current = proposals.get(proposal.proposal_id)
                    if current is None or current.state != "pending_approval" or proposal.state == "pending_approval":
                        raise ProposalError(f"invalid proposal review event at line {line_no}")
                    if current.created_by != proposal.created_by or current.symbol != proposal.symbol or current.side != proposal.side or current.quantity != proposal.quantity or current.rationale != proposal.rationale or current.created_at != proposal.created_at:
                        raise ProposalError(f"proposal terms changed during review at line {line_no}")
                    if current.created_by == proposal.reviewed_by:
                        raise ProposalError(f"self-approval found at line {line_no}")
                else:
                    raise ProposalError(f"unknown proposal journal event at line {line_no}")
                proposals[proposal.proposal_id] = proposal
                previous_hash, expected_seq = digest, expected_seq + 1
            except ProposalError:
                raise
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                raise ProposalError(f"invalid proposal journal record at line {line_no}") from exc
        return proposals, previous_hash, expected_seq - 1

    def _locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.is_symlink():
            raise ProposalError("proposal journal must not be a symbolic link")
        return self.path.open("a+b")

    def _append_unlocked(self, stream, *, seq: int, prev_hash: str, event: str, proposal: TradeProposal) -> str:
        body: dict[str, object] = {
            "seq": seq,
            "prev_hash": prev_hash,
            "event": event,
            "proposal": proposal.to_dict(),
        }
        digest = hashlib.sha256(self._canonical(body)).hexdigest()
        record = {"schema_version": self.schema_version, **body, "hash": digest}
        stream.seek(0, os.SEEK_END)
        stream.write(self._canonical(record) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
        return digest

    def create(
        self,
        *,
        symbol: str,
        side: str,
        quantity: str | Decimal | int | float,
        rationale: str,
        created_by: str,
        created_at: datetime | str | None = None,
    ) -> TradeProposal:
        symbol = symbol.strip().upper() if isinstance(symbol, str) else ""
        created_by = created_by.strip() if isinstance(created_by, str) else ""
        rationale = rationale.strip() if isinstance(rationale, str) else ""
        stamp = _now_iso(created_at)
        proposal = TradeProposal(
            proposal_id=str(uuid4()),
            symbol=symbol,
            side=side,  # type: ignore[arg-type]
            quantity=_quantity(quantity),
            rationale=rationale,
            created_by=created_by,
            state="pending_approval",
            created_at=stamp,
            updated_at=stamp,
        )
        with self._thread_lock, self._locked() as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                _, previous_hash, seq = self._load_unlocked(stream)
                self._append_unlocked(stream, seq=seq + 1, prev_hash=previous_hash, event="created", proposal=proposal)
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        return proposal

    def review(
        self,
        proposal_id: str,
        *,
        decision: ReviewDecision,
        reviewer: str,
        note: str = "",
        reviewed_at: datetime | str | None = None,
    ) -> TradeProposal:
        if decision not in {"approve", "reject"}:
            raise ProposalError("decision must be approve or reject")
        if not isinstance(reviewer, str) or not reviewer.strip() or len(reviewer) > 160:
            raise ProposalError("reviewer must identify the reviewer")
        if not isinstance(note, str) or len(note) > 2000:
            raise ProposalError("review note may not exceed 2000 characters")
        stamp = _now_iso(reviewed_at)
        with self._thread_lock, self._locked() as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                proposals, previous_hash, seq = self._load_unlocked(stream)
                current = proposals.get(proposal_id)
                if current is None:
                    raise ProposalError("unknown trade proposal")
                if current.state != "pending_approval":
                    raise ProposalError("proposal has already been reviewed")
                if current.created_by == reviewer.strip():
                    raise ProposalError("the proposer cannot approve or reject their own proposal")
                updated = replace(
                    current,
                    state="approved" if decision == "approve" else "rejected",
                    updated_at=stamp,
                    reviewed_by=reviewer.strip(),
                    review_note=note.strip() or None,
                )
                self._append_unlocked(stream, seq=seq + 1, prev_hash=previous_hash, event="reviewed", proposal=updated)
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        return updated

    def get(self, proposal_id: str) -> TradeProposal:
        with self._thread_lock, self._locked() as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
            try:
                proposals, _, _ = self._load_unlocked(stream)
                try:
                    return proposals[proposal_id]
                except KeyError as exc:
                    raise ProposalError("unknown trade proposal") from exc
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def list(self, *, state: ProposalState | None = None, limit: int = 100) -> list[TradeProposal]:
        if state is not None and state not in {"pending_approval", "approved", "rejected"}:
            raise ProposalError("invalid proposal state filter")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ProposalError("limit must be an integer in [1, 500]")
        with self._thread_lock, self._locked() as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
            try:
                proposals, _, _ = self._load_unlocked(stream)
                selected = [item for item in proposals.values() if state is None or item.state == state]
                return sorted(selected, key=lambda item: (item.created_at, item.proposal_id), reverse=True)[:limit]
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
