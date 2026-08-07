"""oservi.master_agent chat_stream 长程钩子测试。

覆盖:
  1. 默认 None 回归 (不传 long_task 行为与原来完全一致);
  2. 配额暂停: pre_round quota_ok=False → status=paused_by_quota, 不执行引擎轮;
  3. 工具轮触发 post_round: tool_call 轮执行后写事件流;
  4. 首轮注入 next_action 提示 (prompt_suffix);
  5. 钩子异常 → status=failed (不崩循环/不挂起)。
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

# 开发接线: 3O 主库是 src 布局, 注入仓库路径
# tests/test_x.py → parents[2] = platform/3O
_THREE_O = Path(__file__).resolve().parents[2]
for _lib in ("obase", "oprim", "omodul", "oskill", "oservi"):
    _p = str(_THREE_O / _lib)
    if _p not in sys.path:
        sys.path.insert(0, _p)

from oservi.master_agent import MasterAgent

# ---------------------------------------------------------------------------
# stubs
# ---------------------------------------------------------------------------


class FakeTools:
    def get_all_schemas(self) -> list[dict]:
        return []

    def list_tools(self) -> list[str]:
        return []

    def describe(self, name: str) -> str:
        return name

    def has(self, name: str) -> bool:
        return False

    async def execute(self, name: str, kwargs: dict) -> str:
        return f"[tool {name} executed]"


class FakeSkillHub:
    def get_all_schemas(self) -> list[dict]:
        return []

    def list_skills(self) -> list[str]:
        return []

    def describe(self, name: str) -> str:
        return name

    def reload_skills(self) -> dict:
        return {"loaded": 0, "skipped": 0}

    async def execute(self, name: str, kwargs: dict) -> str:
        return "[skill executed]"


class FakeMemory:
    def inject_subconscious(self) -> str:
        return ""

    def add_preference(self, **kwargs: Any) -> str:
        return "ok"

    def remove_preference(self, **kwargs: Any) -> str:
        return "ok"


class FakeSwarm:
    async def run_swarm(self, overarching_goal: str, sub_tasks: list[dict]) -> str:
        return "swarm done"


class FakeVault:
    async def execute_secure_tool(self, **kwargs: Any) -> str:
        return "vault done"


class FakeLongTask:
    """记录调用的长程驱动 stub。"""

    def __init__(self, *, quota_ok: bool = True, suffix: str = "") -> None:
        self.quota_ok = quota_ok
        self.suffix = suffix
        self.pre_calls = 0
        self.post_calls = 0
        self.post_outcomes: list[dict] = []

    async def pre_round(self):
        self.pre_calls += 1
        return SimpleNamespace(
            quota_ok=self.quota_ok,
            remaining_usd=0.0 if not self.quota_ok else 5.0,
            prompt_suffix=self.suffix,
            next_action=None,
            goal_summary="todo 1/2 done",
        )

    async def post_round(self, outcome: dict) -> None:
        self.post_calls += 1
        self.post_outcomes.append(outcome)


class RecordingLLM:
    def __init__(self, responses: list[dict]) -> None:
        self.responses = list(responses)
        self.calls: list[list[dict]] = []

    async def __call__(self, messages: list, **kwargs: Any) -> dict:
        self.calls.append(list(messages))
        return self.responses.pop(0)


def _direct_response(text: str = "done") -> dict:
    return {
        "choices": [{"message": {"role": "assistant", "content": text}}],
        "usage": {"input_tokens": 10, "output_tokens": 5},
    }


def _tool_call_response() -> dict:
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc-1",
                            "type": "function",
                            "function": {"name": "fake_tool", "arguments": "{}"},
                        }
                    ],
                }
            }
        ],
        "usage": {"input_tokens": 10, "output_tokens": 5},
    }


def _make_agent(llm: RecordingLLM, *, max_rounds: int = 8) -> MasterAgent:
    return MasterAgent(
        llm_caller=llm,
        tools=FakeTools(),
        skill_hub=FakeSkillHub(),
        memory=FakeMemory(),
        swarm=FakeSwarm(),
        vault=FakeVault(),
        max_rounds=max_rounds,
    )


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


async def test_default_no_long_task_regression():
    """不传 long_task: 行为与原来完全一致 (直答一轮 success)。"""
    llm = RecordingLLM([_direct_response("hello")])
    agent = _make_agent(llm)
    result = await agent.chat_stream("hi", session_id="s1")
    assert result["status"] == "success"
    assert result["final_answer"] == "hello"
    assert result["rounds"] == 1


async def test_quota_pause_blocks_engine_round():
    """配额暂停: pre_round quota_ok=False → paused_by_quota, 引擎未执行。"""
    fake = FakeLongTask(quota_ok=False)
    llm = RecordingLLM([_direct_response("should not run")])
    agent = _make_agent(llm)
    result = await agent.chat_stream("写代码", session_id="s1", long_task=fake)
    assert result["status"] == "paused_by_quota"
    assert result["rounds"] == 0
    assert fake.pre_calls >= 1
    assert fake.post_calls == 0
    assert not llm.calls  # LLM 未被调用


async def test_post_round_on_tool_round():
    """工具轮执行后触发 post_round; 直答轮结束 (success)。"""
    fake = FakeLongTask(quota_ok=True, suffix=" [下一步 t2]")
    llm = RecordingLLM([_tool_call_response(), _direct_response("完成")])
    agent = _make_agent(llm, max_rounds=4)
    result = await agent.chat_stream("重构结算模块", session_id="s1", long_task=fake)
    assert result["status"] == "success"
    assert fake.pre_calls == 2  # 每轮一次
    assert fake.post_calls == 1  # 仅工具轮
    assert fake.post_outcomes[0]["cost_usd"] >= 0.0


async def test_prompt_suffix_injected_first_round():
    """首轮把 next_action 提示注入 user 消息尾部。"""
    fake = FakeLongTask(quota_ok=True, suffix=" [长程任务] 下一步应执行: t2: 写单测")
    llm = RecordingLLM([_direct_response("ok")])
    agent = _make_agent(llm)
    await agent.chat_stream("写代码", session_id="s1", long_task=fake)
    first_user = llm.calls[0][-1]
    assert first_user["role"] == "user"
    assert "下一步应执行: t2" in first_user["content"]
    assert "写代码" in first_user["content"]


async def test_hook_error_returns_failed():
    """钩子抛异常 → status=failed (不挂起/不崩循环)。"""

    class BoomLongTask:
        async def pre_round(self):
            raise RuntimeError("event store corrupt")

    llm = RecordingLLM([_direct_response("x")])
    agent = _make_agent(llm)
    result = await agent.chat_stream("写代码", session_id="s1", long_task=BoomLongTask())
    assert result["status"] == "failed"
    assert "long task hook error" in result["error"]
    assert not llm.calls
