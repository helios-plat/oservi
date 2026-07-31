"""Saga Composer Engine Skeleton.

机制 (固化):
- 按顺序执行注入的 omodul steps (forward 阶段)
- 任一步失败 (返回 {"status": "failed"} 或抛异常) → 停止 forward
- 失败后按 REVERSE 顺序执行 compensations (best-effort 补偿)
- 每步结果通过 on_step 回调可观测

业务 (注入):
- steps: omodul callable 列表 (有序 forward 执行)
- compensations: omodul callable 列表 (失败时逆序补偿)

与 sequential_composer 的区别:
- sequential_composer: 单步出错仍继续后续步骤 (无补偿)
- saga_composer: 单步出错立即停止 forward, 逆序跑补偿 (事务语义)

红线对照:
- 红线 2 (机制/业务分离): 步骤/补偿业务全靠注入
- 红线 3 (注入契约): steps=omodul(1..n) / compensations=omodul(0..n)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class SagaComposerEngine(EngineSkeleton):
    """Saga 组合引擎骨架 (forward 执行 + reverse 补偿).

    Saga 语义:
    1. forward 阶段: 按注入顺序执行每个 step omodul.
    2. 若某步返回 dict 且 status == "failed", 或抛异常 → 立即停止 forward.
    3. 失败后进入补偿阶段: 按 REVERSE 顺序执行 compensations (best-effort).
       单个补偿出错被捕获记录, 不会掩盖原始失败, 也不中断其余补偿.
    4. 全部成功 → status="completed"; 失败且有补偿 → "compensated";
       失败且无补偿 → "failed".

    这与 sequential_composer 相反: 后者单步出错仍继续, 且无补偿.

    Example::

        engine = SagaComposerEngine(
            steps=[reserve_stock, charge_payment, ship_order],
            compensations=[refund_payment, release_stock],  # 逆序执行
            trigger={"on_demand": True},
            config={},
            name="order-saga",
        )
        result = asyncio.run(engine.run(input_data={"order_id": 42}))
    """

    injection_points: ClassVar[dict] = {
        "steps": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Ordered forward omodul steps",
        ),
        "compensations": Injection(
            kind="omodul",
            cardinality="0..n",
            description="Compensation omoduls run in REVERSE on failure",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        steps: list[Callable[..., Any]] | Callable[..., Any],
        compensations: list[Callable[..., Any]] | Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.step_list: list[Callable[..., Any]] = steps if isinstance(steps, list) else [steps]
        if compensations is None:
            self.compensation_list: list[Callable[..., Any]] = []
        else:
            self.compensation_list = compensations if isinstance(compensations, list) else [compensations]
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    async def _invoke(self, fn: Callable[..., Any], **kwargs: Any) -> Any:
        """Sync/async-agnostic callable invocation (keyword args)."""
        if inspect.iscoroutinefunction(fn):
            return await fn(**kwargs)
        raw = fn(**kwargs)
        if asyncio.iscoroutine(raw):
            return await raw
        return raw

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Execute forward steps; on failure run compensations in reverse.

        Args:
            input_data: Initial data passed to each step / compensation.
            output_dir: Optional output directory hint (passed through).
            on_step: Optional callback called after each forward step with
                     {"step_no": int, "result": Any}.

        Returns:
            {"status": "completed" | "failed" | "compensated",
             "results": [step_result, ...],
             "compensations_run": [comp_result, ...]}
        """
        input_data = input_data or {}
        results: list[Any] = []
        forward_failed = False

        # ===== forward 阶段 =====
        for i, step in enumerate(self.step_list):
            try:
                result = await self._invoke(step, input_data=input_data, step_no=i)
            except Exception as e:
                self._last_error = f"step {i} {getattr(step, '__name__', step)}: {type(e).__name__}: {e}"
                logger.warning(f"SagaComposerEngine '{self.name}' step {i} failed: {e}")
                result = {"status": "failed", "error": str(e), "step_no": i}

            results.append(result)

            if on_step:
                try:
                    on_step({"step_no": i, "result": result})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed at step {i}: {cb_err}")

            if isinstance(result, dict) and result.get("status") == "failed":
                forward_failed = True
                break

        # ===== 补偿阶段 (REVERSE, best-effort) =====
        compensations_run: list[Any] = []
        if forward_failed:
            reason = self._last_error or "forward step failed"
            for j, comp in enumerate(reversed(self.compensation_list)):
                try:
                    comp_result = await self._invoke(
                        comp,
                        input_data=input_data,
                        compensation_no=j,
                        reason=reason,
                    )
                except Exception as e:
                    # 补偿失败被捕获记录, 不掩盖原始失败, 不中断其余补偿
                    logger.warning(
                        f"SagaComposerEngine '{self.name}' compensation {j} "
                        f"{getattr(comp, '__name__', comp)} failed: {e}"
                    )
                    comp_result = {"status": "failed", "error": str(e), "compensation_no": j}
                compensations_run.append(comp_result)

        if not forward_failed:
            status = "completed"
        elif compensations_run:
            status = "compensated"
        else:
            status = "failed"

        return {
            "status": status,
            "results": results,
            "compensations_run": compensations_run,
        }

    def stop(self) -> None:
        """Signal the engine to stop (no-op for on_demand engines)."""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "steps_count": len(self.step_list),
                "compensations_count": len(self.compensation_list),
                "last_error": self._last_error,
            },
        }


register_skeleton("saga_composer", SagaComposerEngine)
