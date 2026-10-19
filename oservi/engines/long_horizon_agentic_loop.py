"""Long Horizon Agentic Loop Engine Skeleton.

收敛自双实证:
- LongHorizon-Harness: checkpoint → resume → execute → checkpoint cycle
- loopx: long-running agent loop with context compression

共性 (固化为机制):
- 长程运行循环：支持断点水合恢复
- Checkpoint 快照：每步自动落盘，启动时自动恢复
- 上下文压缩：历史超限时自动压缩摘要
- max_steps 上限防止无限循环

差异 (留 inject + config):
- llm_caller: LLM 推理引擎 → 注入 oprim
- compressor_sk: 上下文压缩技能 → 注入 oskill
- checkpoint_store: 快照存储 → 注入 obase

红线对照:
- 红线 1 (≥2 实证): LongHorizon-Harness + loopx ✅
- 红线 2 (机制/业务分离): LLM/压缩/存储全靠注入
- 红线 3 (注入契约): llm_caller=oprim(1) / compressor_sk=oskill(0..1) / checkpoint_store=obase(1)
- 红线 4 (无状态骨架): 状态只在 LongHorizonAgenticLoopEngine 实例
- 红线 5 (不反向依赖): 本文件不 import oprim/oskill/omodul
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


class LongHorizonAgenticLoopEngine(EngineSkeleton):
    """长程运行 Agentic 循环引擎骨架.

    机制 (骨架固化):
    - 断点水合恢复：启动时从 checkpoint_store 加载历史状态
    - Checkpoint 快照：每步执行后自动持久化
    - 上下文压缩：历史超 max_history 时自动压缩
    - max_steps 上限 (config.max_steps, 默认 50)
    - 任务完成 / 超限 → on_task_done 回调

    业务 (注入填料):
    - llm_caller: LLM 推理，接收 (prompt/messages) → 返回响应
    - compressor_sk: 上下文压缩技能（可选）
    - checkpoint_store: 快照存储（持久化/恢复）

    Example::

        manifest = ServiceManifest(
            name="long-horizon-agent",
            skeleton="long_horizon_agentic_loop",
            inject={
                "llm_caller": [my_llm_caller],
                "compressor_sk": [my_compressor],
                "checkpoint_store": [my_store],
            },
            trigger={"on_demand": True},
            config={"max_steps": 50, "max_history": 10},
        )
    """

    injection_points: ClassVar[dict] = {
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM call primitive for reasoning",
        ),
        "compressor_sk": Injection(
            kind="oskill",
            cardinality="0..1",
            description="Optional context compression skill",
        ),
        "checkpoint_store": Injection(
            kind="obase",
            cardinality="1",
            description="Checkpoint store for snapshot persistence",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        llm_caller: list[Callable[..., Any]],
        checkpoint_store: list[Any],
        compressor_sk: list[Callable[..., Any]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_task_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.name = name
        self.llm_caller = llm_caller[0]  # cardinality=1
        self.checkpoint_store = checkpoint_store[0]  # cardinality=1
        self.compressor_sk = compressor_sk[0] if compressor_sk else None
        self.trigger = trigger
        self.config = config
        self.on_task_done = on_task_done

        # 运行期状态 (红线 4)
        self._running = False
        self._task_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._completed_count = 0
        self._last_error: str | None = None

    # ===== 引擎生命周期 =====

    def run(self) -> None:
        """启动持续运行循环."""
        if self._running:
            raise RuntimeError(
                f"LongHorizonAgenticLoopEngine '{self.name}' already running"
            )
        self._running = True
        logger.info(f"LongHorizonAgenticLoopEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"LongHorizonAgenticLoopEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止引擎."""
        self._running = False

    def submit_task(self, task: dict[str, Any]) -> None:
        """提交一个长程任务到队列."""
        self._task_queue.put_nowait(task)

    # ===== 核心方法 =====

    async def run_long_horizon_loop(
        self,
        session_id: str,
        task: str,
    ) -> dict[str, Any]:
        """运行长程任务循环，带自动上下文压缩与快照恢复.

        Args:
            session_id: 会话标识
            task: 任务描述

        Returns:
            {"status": "completed"|"failed", "session_id": str, "response": str, ...}
        """
        try:
            # 1. 水合加载恢复
            state = {}
            if self.checkpoint_store is not None and hasattr(
                self.checkpoint_store, "load_checkpoint"
            ):
                try:
                    state = self.checkpoint_store.load_checkpoint(session_id)
                except Exception:
                    state = {}

            history: list[dict[str, Any]] = state.get("history", [])
            history.append({"role": "user", "content": task})

            # 2. 上下文压缩检查
            max_history = self.config.get("max_history", 10)
            if (
                len(history) > max_history
                and self.compressor_sk is not None
            ):
                try:
                    comp_res = self.compressor_sk(
                        history,
                        llm_caller=self.llm_caller,
                    ) if callable(self.compressor_sk) else {"compressed": False, "compacted_history": history}

                    if isinstance(comp_res, dict) and comp_res.get("compressed"):
                        history = comp_res["compacted_history"]
                except Exception:
                    # 压缩失败不中断流程
                    pass

            # 3. 执行 LLM 推演
            max_steps = self.config.get("max_steps", 50)
            step_count = state.get("step_index", 0)

            if step_count >= max_steps:
                return {
                    "status": "completed",
                    "session_id": session_id,
                    "response": f"Max steps ({max_steps}) reached. Task halted.",
                    "history_length": len(history),
                    "step_index": step_count,
                }

            try:
                import json

                prompt = json.dumps(history, ensure_ascii=False)
            except Exception:
                prompt = str(history)

            try:
                response = self.llm_caller(prompt, temperature=0.1)
                if asyncio.iscoroutine(response):
                    response = await response
            except Exception as e:
                response = f"LLM call failed: {type(e).__name__}: {e}"

            history.append({"role": "assistant", "content": str(response)})
            step_count += 1

            # 4. 状态持久化落盘
            if self.checkpoint_store is not None and hasattr(
                self.checkpoint_store, "save_checkpoint"
            ):
                try:
                    self.checkpoint_store.save_checkpoint(session_id, {
                        "session_id": session_id,
                        "step_index": step_count,
                        "history": history,
                        "status": "in_progress",
                    })
                except Exception:
                    pass

            self._completed_count += 1

            return {
                "status": "completed",
                "session_id": session_id,
                "response": str(response),
                "history_length": len(history),
                "step_index": step_count,
            }

        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            return {
                "status": "failed",
                "session_id": session_id,
                "response": "",
                "error": str(e),
                "history_length": 0,
                "step_index": 0,
            }

    # ===== 内部循环 =====

    async def _run_loop(self) -> None:
        """内部消费循环."""
        while self._running:
            try:
                task = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)
                session_id = task.get("session_id", "default")
                task_desc = task.get("task", "")
                result = await self.run_long_horizon_loop(session_id, task_desc)
                if self.on_task_done:
                    try:
                        self.on_task_done(result)
                    except Exception as e:
                        logger.warning(f"on_task_done callback failed: {e}")
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(
                    f"LongHorizonAgenticLoopEngine '{self.name}' task failed"
                )

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        """健康检查."""
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "queue_size": self._task_queue.qsize(),
                "completed_count": self._completed_count,
                "has_compressor": self.compressor_sk is not None,
                "has_checkpoint_store": self.checkpoint_store is not None,
                "last_error": self._last_error,
            },
        }


# 注册到骨架注册表
register_skeleton("long_horizon_agentic_loop", LongHorizonAgenticLoopEngine)
