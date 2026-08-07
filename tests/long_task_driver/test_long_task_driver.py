"""oservi.long_task_driver 行为测试矩阵。

覆盖 (用户场景: 连续两天"写→测试→修→又写"):
  1. 两天多轮循环: day1 两轮写测 → 跨进程 resume → day2 续跑 → 全部完成;
  2. 配额暂停/恢复: 超支 → paused_by_quota (pre_round 硬拦截) → 充值恢复;
  3. automata 唤醒: wakeup_prompt 生成续跑 prompt → 新进程 resume 续跑;
  4. 引擎接线: AgenticLoop.session(long_task=driver) 配额暂停状态;
  5. 引擎接线: 正常轮 post_round 写 quota_consumed 事件;
  6. resume 对齐配额 + 完整性。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# 开发接线: 3O 主库是 src 布局, 注入仓库路径
_THREE_O = Path(__file__).resolve().parents[3]
for _lib in ("obase", "oprim", "omodul", "oskill", "oservi"):
    _p = str(_THREE_O / _lib)
    if _p not in sys.path:
        sys.path.insert(0, _p)

from oservi.agentic_loop import AgenticLoop
from oservi.long_task_driver import (
    LongTaskDriver,
    RoundOutcome,
    open_long_task,
)

# ---------------------------------------------------------------------------
# 引擎 stub
# ---------------------------------------------------------------------------


async def _fake_llm(messages, tools=None, max_tokens=None, thinking_budget=None, system=None):
    """返回 tool_use (触发 iteration_done → post_round), 产生正 cost。"""
    return {
        "content": [
            {
                "type": "tool_use",
                "id": "tu-1",
                "name": "no_such_tool",
                "input": {},
            }
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 10, "output_tokens": 5},
    }


async def _fake_llm_end(messages, tools=None, max_tokens=None, thinking_budget=None, system=None):
    """直接 end_turn (回归: 不触发 post_round, 引擎行为不受 long_task 影响)。"""
    return {
        "content": [{"type": "text", "text": "done"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 5},
    }


def _make_engine(*, max_iterations: int = 1) -> AgenticLoop:
    return AgenticLoop(max_iterations=max_iterations).assemble(llm_caller=_fake_llm, tools=[])


def _round(cost: float, todo: str | None = None, status: str = "done"):
    """构造一轮引擎结果 (宿主转写格式)。"""

    async def engine(suffix: str) -> dict:
        result: dict = {"cost_usd": cost}
        if todo:
            result["todo_updates"] = [{"todo_id": todo, "title": todo, "status": status}]
            result["evidence"] = [{"kind": "test_pass", "detail": {"round": cost}, "todo_id": todo}]
        return result

    return engine


# ---------------------------------------------------------------------------
# 1. 两天多轮循环 (核心场景)
# ---------------------------------------------------------------------------


async def test_two_days_multi_round_loop(tmp_path):
    # Day 1: 两轮"写→测"
    d1 = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    await d1.ensure_goal("重构结算模块")
    await d1.run_round(_round(0.1, "t1"))
    await d1.run_round(_round(0.2, "t2"))
    assert d1.kernel.goal.todos["t1"].status == "done"
    assert d1.kernel.goal.todos["t2"].status == "done"
    assert len(d1.kernel.goal.evidence) == 2

    # Day 2: 全新驱动 (模拟新进程) 从事件流恢复
    d2 = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    ctx = await d2.resume()
    assert ctx["title"] == "重构结算模块"
    assert ctx["quota"]["spent"] == pytest.approx(0.3)  # 配额从事件流恢复
    assert ctx["integrity_ok"] is True
    assert ctx["next_action"] is None  # t1/t2 已完成

    # Day 2 续跑: 新增第三项
    await d2.run_round(_round(0.05, "t3"))
    assert d2.kernel.is_complete()
    assert d2.kernel.check_integrity().ok
    assert d2.quota.spent_usd == pytest.approx(0.35)

    # 最终: 再跨实例重建, 三天成果全部持久化
    final = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    fctx = await final.resume()
    assert fctx["quota"]["spent"] == pytest.approx(0.35)
    assert fctx["integrity_ok"] is True


# ---------------------------------------------------------------------------
# 2. 配额暂停/恢复
# ---------------------------------------------------------------------------


async def test_quota_pause_resume_loop(tmp_path):
    driver = open_long_task(tmp_path, goal_id="g1", budget_usd=0.3)
    await driver.ensure_goal("g")
    # 轮 1: 0.2 ≤ 0.3 OK (run_round 透传引擎结果, 无 status 字段)
    r1 = await driver.run_round(_round(0.2, "t1"))
    assert r1.get("status") != "paused_by_quota"
    # 轮 2: 0.2 → 累计 0.4 > 0.3 → 本轮完成但超支 → paused
    r2 = await driver.run_round(_round(0.2, "t2"))
    assert r2["status"] == "paused_by_quota"
    assert driver.quota.paused is True
    # 轮 3: pre_round 硬拦截, 不执行引擎
    called = []

    async def spy(suffix: str) -> dict:
        called.append(suffix)
        return {"cost_usd": 0.0}

    r3 = await driver.run_round(spy)
    assert r3["status"] == "paused_by_quota"
    assert not called  # 引擎未被调用
    # 充值恢复
    await driver.quota.resume(new_budget=1.0)
    r4 = await driver.run_round(_round(0.1, "t3"))
    assert r4.get("status") != "paused_by_quota"
    assert driver.quota.spent_usd == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# 3. automata 唤醒 (跨天续跑)
# ---------------------------------------------------------------------------


async def test_automata_wakeup_flow(tmp_path):
    # Day 1: 建 goal, 建 t1/t2 两个 todo, 完成 t1 (剩 t2)
    d1 = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    await d1.ensure_goal("重构结算模块")
    await d1.kernel.update_todo("t1", title="拆服务", status="done")
    await d1.kernel.update_todo("t2", title="写单测")

    # 注册 cron: 生成续跑 prompt (automata register_cron_task 的 task_prompt)
    prompt = d1.wakeup_prompt()
    assert "g1" in prompt
    assert "下一步" in prompt
    assert "t2" in prompt  # next_action 提示
    assert "剩余配额" in prompt

    # 模拟 automata 到点唤醒: 新进程 resume + 用 wakeup prompt 续跑
    d2 = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    ctx = await d2.resume()
    assert ctx["next_action"] == "t2"
    await d2.run_round(_round(0.2, "t2"))
    assert d2.kernel.is_complete()

    # 再唤醒时 next_action 为 None (全部完成)
    d3 = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    ctx3 = await d3.resume()
    assert ctx3["next_action"] is None
    assert "全部" in d3.wakeup_prompt()


# ---------------------------------------------------------------------------
# 4. 引擎接线: AgenticLoop.session(long_task=driver)
# ---------------------------------------------------------------------------


async def test_engine_session_paused_by_quota(tmp_path):
    store = AppendOnlyEventStoreLocal(tmp_path / "g.jsonl")
    driver = LongTaskDriver(store, goal_id="g1", budget_usd=0.5)
    await driver.ensure_goal("g")
    # 昨天配额耗尽: 真实超支 (写 quota_consumed + quota_paused 事件)
    await driver.quota.record_usage(0.3)
    with pytest.raises(Exception) as exc:
        await driver.quota.record_usage(0.3)  # 0.6 > 0.5
    assert "budget" in str(exc.value).lower()
    assert driver.quota.paused is True

    engine = _make_engine()
    result = await engine.session("写代码", long_task=driver)
    assert result["status"] == "paused_by_quota"
    assert result["iterations"] == 0
    assert "quota" in result["result"].lower()


async def test_engine_session_writes_quota_events(tmp_path):
    store = AppendOnlyEventStoreLocal(tmp_path / "g.jsonl")
    driver = LongTaskDriver(store, goal_id="g1", budget_usd=10.0)
    await driver.ensure_goal("g")

    engine = _make_engine()
    result = await engine.session("写代码", long_task=driver)
    assert result["status"] == "completed"
    # post_round 把本轮 cost 写入事件流 (tool_use 触发 iteration_done)
    types = [e["type"] for e in store.replay()]
    assert "quota_consumed" in types
    assert driver.quota.spent_usd > 0
    # 引擎无 long_task 时行为不受影响 (回归)
    engine2 = AgenticLoop(max_iterations=1).assemble(llm_caller=_fake_llm_end, tools=[])
    result2 = await engine2.session("写代码")
    assert result2["status"] == "completed"


# ---------------------------------------------------------------------------
# 5. post_round 直接调用 (RoundOutcome)
# ---------------------------------------------------------------------------


async def test_post_round_with_typed_outcome(tmp_path):
    driver = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    await driver.ensure_goal("g")
    await driver.post_round(
        RoundOutcome(
            cost_usd=0.3,
            todo_updates=[{"todo_id": "t1", "status": "done", "note": "完成"}],
            evidence=[{"kind": "review", "detail": "ok", "todo_id": "t1"}],
        )
    )
    fresh = open_long_task(tmp_path, goal_id="g1", budget_usd=5.0)
    ctx = await fresh.resume()
    assert ctx["next_action"] is None
    assert ctx["quota"]["spent"] == pytest.approx(0.3)
    assert fresh.kernel.goal.todos["t1"].note == "完成"


# ---------------------------------------------------------------------------
# 6. resume 配额对齐
# ---------------------------------------------------------------------------


async def test_resume_aligns_quota_and_integrity(tmp_path):
    driver = open_long_task(tmp_path, goal_id="g1", budget_usd=3.0)
    await driver.ensure_goal("g")
    await driver.run_round(_round(0.4, "t1"))
    await driver.run_round(_round(0.6, "t2"))
    assert driver.quota.spent_usd == pytest.approx(1.0)

    fresh = open_long_task(tmp_path, goal_id="g1", budget_usd=3.0)
    ctx = await fresh.resume()
    assert ctx["quota"]["spent"] == pytest.approx(1.0)
    assert ctx["quota"]["remaining"] == pytest.approx(2.0)
    assert ctx["last_seq"] == 2 + 2 * 2 + 1  # goal + 2×(todo+evidence)
    assert ctx["integrity_ok"] is True


# 本地别名 (避免测试头部 import 链过重)
def AppendOnlyEventStoreLocal(path):
    from obase.loop_event_store import AppendOnlyEventStore

    return AppendOnlyEventStore(path)
