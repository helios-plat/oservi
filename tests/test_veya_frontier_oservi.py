"""Tests for Veya 前沿探索元素 — oservi 层 (1 个).

Covers: mechanism_game_loop（博弈竞价调度引擎）.
"""

from __future__ import annotations



async def _bid_a(task):
    return 10


async def _bid_b(task):
    return 5


async def _run_a(task):
    return {"done_by": "a"}


async def _run_b(task):
    return {"done_by": "b"}


def _engine(**kw):
    from oservi.engines import MechanismGameLoopEngine

    defaults = dict(
        biddable_agents=[
            {"id": "a", "bid_func": _bid_a, "handler": _run_a},
            {"id": "b", "bid_func": _bid_b, "handler": _run_b},
        ],
        auction_ledger_op=lambda **k: None,
        trigger={"on_demand": True},
        config={},
        name="auction-test",
    )
    defaults.update(kw)
    return MechanismGameLoopEngine(**defaults)


class TestMechanismGameLoop:
    async def test_registered(self):
        from oservi.engines import list_skeletons

        assert "mechanism_game_loop" in list_skeletons()

    async def test_lowest_bid_wins(self):
        engine = _engine()
        r = await engine.run_auction_turn({"id": "t1", "req": "x"})
        assert r["status"] == "completed"
        assert r["winning_agent"] == "b"
        assert r["winning_bid"] == 5
        assert r["execution_result"] == {"done_by": "b"}

    async def test_ledger_written(self):
        ledger: list[tuple] = []

        def ledger_op(task_id, winner, bids):
            ledger.append((task_id, winner, bids))

        engine = _engine(auction_ledger_op=ledger_op)
        await engine.run_auction_turn({"id": "t2"})
        assert ledger and ledger[0][0] == "t2" and ledger[0][1] == "b"
        assert engine.health()["details"]["auction_rounds"] == 1

    async def test_bid_failure_isolated(self):
        async def broken_bid(task):
            raise RuntimeError("bid crash")

        engine = _engine(
            biddable_agents=[
                {"id": "a", "bid_func": broken_bid, "handler": _run_a},
                {"id": "b", "bid_func": _bid_b, "handler": _run_b},
            ]
        )
        r = await engine.run_auction_turn({"id": "t3"})
        assert r["status"] == "completed" and r["winning_agent"] == "b"

    async def test_no_agents(self):
        engine = _engine(biddable_agents=[])
        r = await engine.run_auction_turn({"id": "t4"})
        assert r["status"] == "failed" and "no biddable" in r["error"]

    async def test_stop(self):
        engine = _engine()
        engine.stop()
        r = await engine.run_auction_turn({"id": "t5"})
        assert r["status"] == "failed" and "stopped" in r["error"]

    async def test_run_batch(self):
        engine = _engine()
        r = await engine.run(input_data={"tasks": [{"id": "a1"}, {"id": "a2"}]})
        assert r["status"] == "completed"
        assert len(r["outcomes"]) == 2
        assert all(o["winning_agent"] == "b" for o in r["outcomes"])
        assert len(r["ledger"]) == 2

    def test_injection_points(self):
        engine = _engine()
        assert "biddable_agents" in engine.injection_points
        assert engine.injection_points["biddable_agents"].kind == "omodul"
        assert engine.injection_points["auction_ledger_op"].cardinality == "1"
