"""oservi.agent_bench_harness — 确定性 Agent 基准评测 (Vigla harness 复刻)。

在**确定性 mock 环境**中评测多 vendor 编码 agent:
  - mock_tools: 工具调用注入确定性响应 (无真实网络/LLM 分支)
  - 任务集: repo/prompt/gold_patch → 逐 vendor 执行
  - 输出: completion_rate / pass_rate (gold_patch diff 比对) / cost / token

分层: oservi (编排) — 复用 run_subagent / _estimate_cost 思路, mock 层完全注入。
"""

from __future__ import annotations

import difflib
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BenchTask:
    """基准任务: 仓库 + 提示词 + 金标准补丁。"""

    repo: str
    prompt: str
    gold_patch: str          # 期望改动 (文本; 比对用相似度)
    id: str = ""


@dataclass
class BenchResult:
    """单 vendor 评测结果。"""

    vendor: str
    completion_rate: float = 0.0
    pass_rate: float = 0.0
    cost: float = 0.0
    token_usage: dict[str, int] = field(default_factory=dict)
    tasks_total: int = 0
    tasks_passed: int = 0
    duration_s: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "vendor": self.vendor,
            "completion_rate": round(self.completion_rate, 4),
            "pass_rate": round(self.pass_rate, 4),
            "cost": round(self.cost, 4),
            "token_usage": self.token_usage,
            "tasks_total": self.tasks_total,
            "tasks_passed": self.tasks_passed,
            "duration_s": round(self.duration_s, 2),
        }


def _patch_similarity(produced: str, gold: str) -> float:
    """gold_patch 相似度 (SequenceMatcher, 0-1)。"""
    if not gold:
        return 1.0 if not produced else 0.0
    return difflib.SequenceMatcher(None, gold.strip(), (produced or "").strip()).ratio()


def agent_bench_harness(
    tasks: list[dict[str, Any]],
    vendors: list[str],
    *,
    executor: Callable[[str, str, dict[str, Any]], dict[str, Any]] | None = None,
    mock_tools: dict[str, Any] | None = None,
    pass_threshold: float = 0.8,
    cost_fn: Callable[[str, dict[str, int]], float] | None = None,
) -> dict[str, Any]:
    """确定性基准评测。

    Args:
        tasks: [{"repo", "prompt", "gold_patch", "id"}]
        vendors: ["claude", "codex", "pi", ...]
        executor: 执行函数 (vendor, prompt, ctx) -> {"output", "token_usage"}。
                  缺省用 mock executor: mock_tools 注入 + gold_patch 回显 (确定性)。
        mock_tools: mock 工具响应表 {"tool_name": "response"}; 缺省 {"read_file": "MOCK:..."}
        pass_threshold: pass 判定相似度阈值
        cost_fn: (vendor, usage) -> cost USD; 缺省每 1k token 0.002 线性
    """
    mock_tools = mock_tools or {"read_file": "MOCK_FILE_CONTENT", "run_tests": "MOCK: all pass"}
    results: dict[str, BenchResult] = {}

    def _mock_executor(vendor: str, prompt: str, ctx: dict[str, Any]) -> dict[str, Any]:
        # 确定性: 工具调用回显 mock 响应 + 输出 = gold_patch 截断 (可复现)
        tool_calls = ctx.get("tool_calls") or ["read_file"]
        usage = {"prompt_tokens": 500 + len(prompt), "completion_tokens": 300}
        return {"output": (ctx.get("gold_patch") or "MOCK_OUTPUT")[:2000],
                "token_usage": usage, "tool_responses": [mock_tools.get(t, "MOCK") for t in tool_calls]}

    exec_fn = executor or _mock_executor

    for vendor in vendors:
        t0 = time.time()
        br = BenchResult(vendor=vendor, tasks_total=len(tasks))
        for task in tasks:
            bt = BenchTask(**{k: v for k, v in task.items() if k in BenchTask.__dataclass_fields__})
            ctx = {"gold_patch": bt.gold_patch, "tool_calls": task.get("tool_calls")}
            try:
                res = exec_fn(vendor, bt.prompt, ctx)
            except Exception:  # noqa: BLE001 - 单任务失败计入 0
                res = {"output": "", "token_usage": {}}
            usage = res.get("token_usage") or {}
            sim = _patch_similarity(str(res.get("output", "")), bt.gold_patch)
            br.tasks_passed += 1 if sim >= pass_threshold else 0
            for k, v in usage.items():
                br.token_usage[k] = br.token_usage.get(k, 0) + int(v)
        br.completion_rate = br.tasks_passed / br.tasks_total if br.tasks_total else 0.0
        br.pass_rate = br.completion_rate
        total_tokens = sum(br.token_usage.values())
        if cost_fn:
            br.cost = float(cost_fn(vendor, br.token_usage))
        else:
            br.cost = total_tokens * 0.002 / 1000.0
        br.duration_s = time.time() - t0
        results[vendor] = br

    return {"per_vendor": {v: br.to_dict() for v, br in results.items()},
            "summary": {"vendors": vendors, "tasks": len(tasks),
                        "best": max(results, key=lambda v: results[v].pass_rate)
                        if results else None}}


__all__ = ["BenchResult", "BenchTask", "agent_bench_harness"]
