from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from oservi.alice_trade_proposals import ProposalError, ProposalStore

STAMP = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)


def _create(store: ProposalStore):
    return store.create(symbol="BTC-USDT", side="BUY", quantity="0.0250", rationale="Review a manually authored thesis.", created_by="analyst-a", created_at=STAMP)


def test_proposal_is_pending_until_an_independent_review(tmp_path: Path) -> None:
    store = ProposalStore(tmp_path / "proposals.jsonl")
    proposal = _create(store)
    assert proposal.symbol == "BTC-USDT"
    assert proposal.quantity == "0.025"
    assert proposal.state == "pending_approval"
    assert proposal.created_by == "analyst-a"
    with pytest.raises(ProposalError, match="cannot approve"):
        store.review(proposal.proposal_id, decision="approve", reviewer="analyst-a", reviewed_at=STAMP)
    reviewed = store.review(proposal.proposal_id, decision="approve", reviewer="reviewer-b", note="Reviewed manually.", reviewed_at=STAMP)
    assert reviewed.state == "approved"
    assert store.get(proposal.proposal_id) == reviewed
    assert store.list(state="pending_approval") == []
    assert store.list(state="approved") == [reviewed]
    with pytest.raises(ProposalError, match="already been reviewed"):
        store.review(proposal.proposal_id, decision="reject", reviewer="reviewer-c", reviewed_at=STAMP)


def test_rejection_is_terminal_and_keeps_reason(tmp_path: Path) -> None:
    store = ProposalStore(tmp_path / "proposals.jsonl")
    proposal = _create(store)
    reviewed = store.review(proposal.proposal_id, decision="reject", reviewer="risk-reviewer", note="Insufficient evidence", reviewed_at=STAMP)
    assert reviewed.state == "rejected"
    assert reviewed.review_note == "Insufficient evidence"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [("symbol", "btc-usdt", "uppercase"), ("side", "SHORT", "BUY or SELL"), ("quantity", "0", "positive"), ("quantity", "NaN", "positive"), ("quantity", "Infinity", "positive"), ("quantity", "1e99999999", "too large")],
)
def test_invalid_proposal_terms_rejected(tmp_path: Path, field: str, value: str, message: str) -> None:
    store = ProposalStore(tmp_path / "proposals.jsonl")
    params = dict(symbol="BTC-USDT", side="BUY", quantity="1", rationale="Why", created_by="analyst")
    params[field] = value
    with pytest.raises(ProposalError, match=message):
        store.create(**params)


def test_tampered_hash_chain_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "proposals.jsonl"
    store = ProposalStore(path)
    _create(store)
    entry = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    entry["proposal"]["rationale"] = "tampered after proposal"
    path.write_text(json.dumps(entry) + "\n", encoding="utf-8")
    with pytest.raises(ProposalError, match="hash mismatch"):
        store.list()


def test_incomplete_final_record_rejected(tmp_path: Path) -> None:
    path = tmp_path / "proposals.jsonl"
    _create(ProposalStore(path))
    path.write_text(path.read_text(encoding="utf-8") + '{"partial":', encoding="utf-8")
    with pytest.raises(ProposalError, match="incomplete final record"):
        ProposalStore(path).list()
