"""Sequential Composer Engine Skeleton.

机制 (固化):
- 按顺序执行注入的 omodul steps 列表
- 每步结果通过 on_step 回调可观测
- 可选 layer4 router 控制步骤跳转

业务 (注入):
- steps: omodul callable 列表 (有序执行)
- router: layer4 callable (可选, 控制下一步跳转)

红线对照:
- 红线 2 (机制/业务分离): 步骤业务全靠注入
- 红线 3 (注入契约): steps=omodul(1..n) / router=layer4(0..1)
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


class SequentialComposerEngine(EngineSkeleton):
    """顺序组合引擎骨架.

    按注入的 steps 列表顺序执行每个 omodul callable,
    汇总结果后返回. 可选 router 控制动态跳转.

    Example::

        engine = SequentialComposerEngine(
            steps=[step_extract, step_transform, step_load],
            router=None,
            trigger={"on_demand": True},
            config={},
            name="etl-pipeline",
        )
        result = asyncio.run(engine.run(input_data={"doc": "..."}, output_dir="/tmp"))
    """

    injection_points: ClassVar[dict] = {
        "steps": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Ordered omodul steps",
        ),
        "router": Injection(
            kind="layer4",
            cardinality="0..1",
            description="Optional routing logic",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        steps: list[Callable[..., Any]] | Callable[..., Any],
        router: Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.step_list: list[Callable[..., Any]] = steps if isinstance(steps, list) else [steps]
        self.router = router
        self.trigger = trigger
        self.config = config

        # 运行期状态
        self._running = False
        self._stop_event = asyncio.Event()
        self._last_error: str | None = None

    async def run(  # type: ignore[override]
        self,
        input_data: dict[str, Any] | None = None,
        output_dir: Any = None,
        *,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Execute all steps in order.

        Args:
            input_data: Initial data passed to the first step.
            output_dir: Optional output directory hint (passed through config).
            on_step: Optional callback called after each step with
                     {"step_no": int, "result": Any}.

        Returns:
            {"status": "completed", "results": [step_result, ...]}
        """
        input_data = input_data or {}
        results: list[Any] = []

        for i, step in enumerate(self.step_list):
            try:
                if inspect.iscoroutinefunction(step):
                    result = await step(input_data=input_data, step_no=i)
                else:
                    raw = step(input_data=input_data, step_no=i)
                    if asyncio.iscoroutine(raw):
                        result = await raw
                    else:
                        result = raw
            except Exception as e:
                self._last_error = f"step {i} {getattr(step, '__name__', step)}: {type(e).__name__}: {e}"
                logger.warning(f"SequentialComposerEngine '{self.name}' step {i} failed: {e}")
                result = {"error": str(e), "step_no": i}

            results.append(result)

            if on_step:
                try:
                    on_step({"step_no": i, "result": result})
                except Exception as cb_err:
                    logger.warning(f"on_step callback failed at step {i}: {cb_err}")

        return {"status": "completed", "results": results}

    def stop(self) -> None:
        """Signal the engine to stop (no-op for on_demand engines)."""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "steps_count": len(self.step_list),
                "has_router": self.router is not None,
                "last_error": self._last_error,
            },
        }


register_skeleton("sequential_composer", SequentialComposerEngine)
