"""Saga Composer Engine Skeleton.

机制 (固化):
- 正向按顺序执行注入的 steps 列表
- 任一 step 返回 {"status": "failed", ...}(omodul 标准失败返回,不是异常)
  时,停止正向执行,对已完成的 step 逆序执行对应 compensations
- 每步/每条补偿结果通过 on_step 回调可观测

业务 (注入):
- steps: omodul callable 列表 (有序正向执行)
- compensations: omodul callable 列表 (与 steps 按下标一一对应的补偿动作,
  可以比 steps 短——某个 step 没有对应补偿就传 None 占位或干脆少给)

跟 sequential_composer 的区别:sequential_composer 只捕获异常、不感知业务
返回值里的 status 字段、没有补偿概念;saga_composer 专门为"多步业务事务,
任一步失败要把前面已完成的步骤退回去"这种场景服务(典型:支付扣款 +
库存扣减,库存不足时要把已经扣的款退回去)。

红线对照:
- 红线 2 (机制/业务分离): 步骤与补偿业务全靠注入,骨架不判断具体业务失败原因
- 红线 3 (注入契约): steps=omodul(1..n) / compensations=omodul(0..n)
- 红线 4 (无状态骨架): 状态只在实例
- 红线 5 (不反向依赖): 不 import 3O 四包
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable, ClassVar

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)


async def _call(fn: Callable[..., Any], **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(**kwargs)
    result = fn(**kwargs)
    if asyncio.iscoroutine(result):
        return await result
    return result


class SagaComposerEngine(EngineSkeleton):
    """Saga 编排引擎骨架:正向执行 + 失败逆序补偿。

    Example::

        engine = SagaComposerEngine(
            steps=[charge_payment, reserve_inventory],
            compensations=[refund_payment, release_inventory],
            trigger={"on_demand": True},
            config={},
            name="checkout-saga",
        )
        result = asyncio.run(engine.run(input_data={"order_id": "o1"}))
    """

    injection_points: ClassVar[dict] = {
        "steps": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Ordered forward-execution omodul steps",
        ),
        "compensations": Injection(
            kind="omodul",
            cardinality="0..n",
            description="Per-step compensation omodul callables, index-aligned with steps",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        steps: list[Callable[..., Any]] | Callable[..., Any],
        compensations: list[Callable[..., Any] | None] | Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.step_list: list[Callable[..., Any]] = steps if isinstance(steps, list) else [steps]
        if compensations is None:
            self.compensation_list: list[Callable[..., Any] | None] = []
        elif isinstance(compensations, list):
            self.compensation_list = compensations
        else:
            self.compensation_list = [compensations]
        self.trigger = trigger
        self.config = config

        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    def _compensation_for(self, step_no: int) -> Callable[..., Any] | None:
        if step_no < len(self.compensation_list):
            return self.compensation_list[step_no]
        return None

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Execute steps forward; on the first failed step, run compensations
        for already-completed steps in reverse order.

        Args:
            input_data: Initial data passed to every step/compensation call.
            output_dir: Optional output directory hint (unused by the skeleton
                itself, passed through for injected callables that need it).
            on_step: Optional callback invoked after each forward step and
                each compensation with {"phase": "forward"|"compensate",
                "step_no": int, "result": Any}.

        Returns:
            {"status": "completed", "results": [...]} on full forward success, or
            {"status": "failed", "failed_step": int, "results": [...],
             "compensated": [...]} if a step failed (compensations already run).
        """
        input_data = input_data or {}
        results: list[Any] = []
        failed_step: int | None = None

        for i, step in enumerate(self.step_list):
            try:
                result = await _call(step, input_data=input_data, step_no=i)
            except Exception as e:
                self._last_error = f"step {i} {getattr(step, '__name__', step)}: {e}"
                logger.warning(f"SagaComposerEngine '{self.name}' step {i} raised: {e}")
                result = {"status": "failed", "error": str(e)}

            results.append(result)
            if on_step:
                try:
                    on_step({"phase": "forward", "step_no": i, "result": result})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed at step {i}: {cb_err}")

            if isinstance(result, dict) and result.get("status") == "failed":
                failed_step = i
                break

        if failed_step is None:
            return {"status": "completed", "results": results}

        compensated: list[Any] = []
        for i in range(failed_step - 1, -1, -1):
            compensation = self._compensation_for(i)
            if compensation is None:
                continue
            try:
                comp_result = await _call(
                    compensation, input_data=input_data, step_no=i, step_result=results[i]
                )
            except Exception as e:
                self._last_error = (
                    f"compensation {i} {getattr(compensation, '__name__', compensation)}: {e}"
                )
                logger.warning(f"SagaComposerEngine '{self.name}' compensation {i} raised: {e}")
                comp_result = {"status": "failed", "error": str(e)}

            compensated.append({"step_no": i, "result": comp_result})
            if on_step:
                try:
                    on_step({"phase": "compensate", "step_no": i, "result": comp_result})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed at compensation {i}: {cb_err}")

        return {
            "status": "failed",
            "failed_step": failed_step,
            "results": results,
            "compensated": compensated,
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
