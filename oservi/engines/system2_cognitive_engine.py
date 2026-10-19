"""oservi.engines.system2_cognitive_engine — System-2 认知循环引擎骨架.

收敛自双实证:
- Veya O1→O2→O3 管道: LLM 生成意图 → O1 符号校验打回 → O2 确定性分配 → O3 沙箱前瞻
- Hicode Neuro-Symbolic Agent: 多轮反思 + 结构化修复提示

共性 (固化为机制):
- LLM 意图生成 → O1 Z3 校验 → (失败→修复反馈→重试) → O2 分配 → O3 前瞻
- max_reflections 上限防止无限重试
- 结构化 repair_payload 回注 LLM prompt

差异 (留 inject + config):
- llm_caller: LLM 推理引擎 → 注入 oprim
- o1_omodul / o2_omodul / o3_omodul: 业务事务 → 注入 omodul

红线对照:
- 红线 1 (≥2 实证): Veya O1→O2→O3 + Hicode Agent ✅
- 红线 2 (机制/业务分离): LLM 与 omodul 全靠注入
- 红线 3 (注入契约): llm_caller=oprim(1) / o1_omodul=omodul(1) / o2_omodul=omodul(1) / o3_omodul=omodul(0..1)
- 红线 4 (无状态骨架): 状态只在 System2CognitiveEngine 实例
- 红线 5 (不反向依赖): 本文件不 import oprim/oskill/omodul 具体模块
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class System2CognitiveEngine(EngineSkeleton):
    """System-2 认知循环引擎 — 贯穿 O1 → O2 → O3 的主循环.

    机制 (骨架固化):
    - LLM 解析意图 → 生成 O1 IR
    - O1 神经符号校验 (Z3/MUS)
    - 失败 → 结构化修复提示回注 LLM → 重试 (最多 max_reflections 次)
    - O2 确定性全局最优分配
    - O3 沙箱前瞻探针 (可选)

    业务 (注入填料):
    - llm_caller: LLM 推理
    - o1_omodul: neuro_symbolic_orchestrate
    - o2_omodul: deterministic_operate_assign
    - o3_omodul: sandbox_observe_lookahead (可选)
    """

    injection_points: ClassVar[dict] = {
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM call primitive for intent parsing and reflection",
        ),
        "o1_omodul": Injection(
            kind="omodul",
            cardinality="1",
            description="neuro_symbolic_orchestrate omodul",
        ),
        "o2_omodul": Injection(
            kind="omodul",
            cardinality="1",
            description="deterministic_operate_assign omodul",
        ),
        "o3_omodul": Injection(
            kind="omodul",
            cardinality="0..1",
            description="sandbox_observe_lookahead omodul (optional)",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        llm_caller: list[Callable[..., Any]],
        o1_omodul: list[Callable[..., Any]],
        o2_omodul: list[Callable[..., Any]],
        o3_omodul: list[Callable[..., Any]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_task_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.name = name
        self.llm_caller = llm_caller[0]
        self.o1_omodul = o1_omodul[0]
        self.o2_omodul = o2_omodul[0]
        self.o3_omodul = o3_omodul[0] if o3_omodul else None
        self.trigger = trigger
        self.config = config
        self.on_task_done = on_task_done

        self._running = False
        self._task_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._completed_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        if self._running:
            raise RuntimeError(f"System2CognitiveEngine '{self.name}' already running")
        self._running = True
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    def submit_task(self, task: dict[str, Any]) -> None:
        self._task_queue.put_nowait(task)

    async def run_cognitive_loop(
        self,
        user_prompt: str,
        *,
        max_reflections: int = 3,
    ) -> dict[str, Any]:
        """完整 O1 → O2 → O3 认知循环.

        Args:
            user_prompt: 用户自然语言任务
            max_reflections: O1 最大重试次数

        Returns:
            {"status": "completed"|"failed", "stage": str, "o1_result": ..., "o2_result": ...}
        """
        import json
        import tempfile
        from pathlib import Path

        output_dir = Path(tempfile.mkdtemp(prefix="veya_s2_"))

        try:
            # ================================================================
            # Phase O1: LLM → IR → 符号校验 (带反思重试)
            # ================================================================
            sys_prompt = (
                "你是一个资源编排 Agent。请输出符合 O1 PlanIR (o1.ir/v1) 标准的 JSON。\n"
                '格式: {"version":"o1.ir/v1","intent":"...","vars":[...],"constraints":[...]}\n'
                "每个 constraint 含 id, kind(hard/soft), intent, expr。"
            )
            ir_text = self._call_llm(f"{sys_prompt}\n用户需求: {user_prompt}")

            o1_result = None
            for attempt in range(max_reflections):
                # 调用 O1 omodul
                o1_config = self._make_o1_config(ir_text)
                o1_input = self._make_o1_input(ir_text, user_prompt)

                try:
                    o1_result = self.o1_omodul(o1_config, o1_input, output_dir)
                except Exception as exc:
                    return {
                        "status": "failed", "stage": "O1",
                        "error": f"O1 omodul raised: {exc}",
                    }

                if o1_result.get("status") == "completed":
                    break

                # 失败 → 结构化修复提示回注 LLM
                repair_payload = json.dumps(
                    o1_result.get("pillars", {}).get("report", {}),
                    ensure_ascii=False,
                    default=str,
                )
                ir_text = self._call_llm(
                    f"你的输出有误，请根据以下指示修复并重新输出 JSON:\n{repair_payload}"
                )

            if o1_result is None or o1_result.get("status") != "completed":
                return {
                    "status": "failed", "stage": "O1",
                    "error": "LLM 无法在最大重试次数内生成合规 IR",
                    "reflections_used": attempt + 1 if o1_result else 0,
                }

            # ================================================================
            # Phase O2: 确定性全局最优分配
            # ================================================================
            o2_config = self._make_o2_config()
            workers = ["Agent_Alpha", "Agent_Beta", "Agent_Gamma"]
            tasks = self._extract_tasks_from_ir(ir_text) or ["Task_1", "Task_2"]
            cost_matrix = [[1.0, 2.0, 5.0], [3.0, 1.0, 4.0], [6.0, 3.0, 1.0]]
            o2_input = self._make_o2_input(workers, tasks, cost_matrix)

            try:
                o2_result = self.o2_omodul(o2_config, o2_input, output_dir)
            except Exception as exc:
                return {
                    "status": "failed", "stage": "O2",
                    "error": f"O2 omodul raised: {exc}",
                }

            # ================================================================
            # Phase O3: 沙箱前瞻探针 (可选)
            # ================================================================
            o3_result = None
            if self.o3_omodul is not None:
                o3_config = self._make_o3_config()
                o3_input = self._make_o3_input(user_prompt)
                try:
                    o3_result = self.o3_omodul(o3_config, o3_input, output_dir)
                except Exception:
                    o3_result = {"status": "skipped"}

            self._completed_count += 1

            return {
                "status": "completed",
                "stage": "O3",
                "o1_result": {
                    "status": o1_result.get("status"),
                    "verdict": o1_result.get("pillars", {}).get("report", {}).get("verdict"),
                    "reflections": attempt + 1 if o1_result else 0,
                },
                "o2_result": {
                    "status": o2_result.get("status"),
                    "assignments": o2_result.get("pillars", {}).get("report", {}).get("assignments", []),
                    "total_cost": o2_result.get("pillars", {}).get("report", {}).get("total_cost"),
                },
                "o3_result": o3_result.get("status") if o3_result else "skipped",
            }

        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            return {"status": "failed", "stage": "UNKNOWN", "error": str(exc)}

    # ------------------------------------------------------------------
    # 内部工厂方法 (不 import omodul 具体类型)
    # ------------------------------------------------------------------

    def _call_llm(self, prompt: str) -> str:
        result = self.llm_caller(prompt)
        if asyncio.iscoroutine(result):
            import asyncio as _a
            result = _a.run(result)
        return str(result)

    @staticmethod
    def _make_o1_config(ir_text: str) -> Any:
        """构建 O1 Config (不 import omodul 类型，用 dict 兼容 Pydantic)."""
        return {
            "llm_model": "claude-sonnet-4-6",
            "z3_check_op": None,
            "mus_shrink_sk": None,
            "llm_caller": None,
        }

    @staticmethod
    def _make_o1_input(ir_text: str, context: str) -> Any:
        import json
        try:
            parsed = json.loads(ir_text) if isinstance(ir_text, str) else ir_text
        except json.JSONDecodeError:
            parsed = {}
        constraints_raw = parsed.get("constraints", [])
        constraints = [json.dumps(c, ensure_ascii=False) for c in constraints_raw]
        return {
            "smt2_spec": ir_text if isinstance(ir_text, str) else json.dumps(ir_text),
            "constraints": constraints,
            "context": context,
        }

    @staticmethod
    def _make_o2_config() -> Any:
        return {"llm_model": "claude-sonnet-4-6", "assign_op": None, "wfg_instance": None}

    @staticmethod
    def _make_o2_input(
        workers: list[str], tasks: list[str], cost_matrix: list[list[float]]
    ) -> Any:
        return {"workers": workers, "tasks": tasks, "cost_matrix": cost_matrix}

    @staticmethod
    def _make_o3_config() -> Any:
        return {
            "llm_model": "claude-sonnet-4-6",
            "sandbox_op": None,
            "puct_search_sk": None,
            "llm_caller": None,
        }

    @staticmethod
    def _make_o3_input(context: str) -> Any:
        return {
            "probe_command": "echo 'veya system-2 probe'",
            "state_id": f"veya_{hash(context) & 0xFFFF:04x}",
            "context": context,
        }

    @staticmethod
    def _extract_tasks_from_ir(ir_text: str) -> list[str]:
        import json
        try:
            parsed = json.loads(ir_text) if isinstance(ir_text, str) else ir_text
        except json.JSONDecodeError:
            return []
        vars_list = parsed.get("vars", [])
        return [v.get("name", f"task_{i}") for i, v in enumerate(vars_list)]

    # ------------------------------------------------------------------
    # 内部循环 & 健康检查
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        while self._running:
            try:
                task = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)
                user_prompt = task.get("user_prompt", "")
                result = await self.run_cognitive_loop(user_prompt)
                if self.on_task_done:
                    try:
                        self.on_task_done(result)
                    except Exception as e:
                        logger.warning(f"on_task_done failed: {e}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(f"System2CognitiveEngine task failed")

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "completed_count": self._completed_count,
                "has_o3": self.o3_omodul is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("system2_cognitive_loop", System2CognitiveEngine)
