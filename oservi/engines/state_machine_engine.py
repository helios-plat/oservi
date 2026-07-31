"""State Machine Engine Skeleton.

机制 (固化):
- 按目标状态名查 transition callable (name-keyed map)
- 迁移前先咨询 validators (guards), 任一返回 falsy → 阻断
- 非法/未知迁移 → 返回 blocked 结果, 绝不抛异常
- 迁移结果通过 on_step 回调可观测

业务 (注入):
- transitions: omodul callable 列表 (按名字建索引)
- validators: oskill callable 列表 (可选 guards)

名字索引约定 (name-keying):
  ServiceManifest.inject 是 dict[str, list[Callable]], 装配器只处理
  list/single/None, 不支持 dict-of-callables. 因此 transitions 以 LIST
  注入, 引擎内部按每个 callable 的名字建 name→callable map:
    key = getattr(t, "name", None) or t.__name__
  迁移时以目标状态名 (to_state) 查表. 给 transition callable 设置显式
  `name` 属性即可自定义键, 否则回退到函数 __name__.

红线对照:
- 红线 2 (机制/业务分离): 迁移/校验业务全靠注入
- 红线 3 (注入契约): transitions=omodul(1..n) / validators=oskill(0..n)
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


class StateMachineEngine(EngineSkeleton):
    """状态机引擎骨架 (name-keyed transitions + guard validators).

    机制:
    1. __init__ 时把注入的 transitions LIST 建成 name→callable map
       (key = 显式 `name` 属性, 否则 __name__).
    2. run(state, to_state=...) 以 to_state 查 transition:
       - 未知目标 → status="blocked", reason="unknown_transition" (不抛异常)
       - 任一 validator 返回 falsy → status="blocked", reason="guard_rejected"
       - 通过校验 → 调 transition, status="completed"

    Example::

        def to_paid(*, state, to_state, input_data): ...
        to_paid.name = "paid"  # 显式键 (可选, 否则用 __name__)

        engine = StateMachineEngine(
            transitions=[to_paid, to_shipped],
            validators=[guard_payment_ok],
            trigger={"on_demand": True},
            config={},
            name="order-fsm",
        )
        result = asyncio.run(engine.run(state="new", to_state="paid"))
    """

    injection_points: ClassVar[dict] = {
        "transitions": Injection(
            kind="omodul",
            cardinality="1..n",
            description="State transition omoduls, keyed by name",
        ),
        "validators": Injection(
            kind="oskill",
            cardinality="0..n",
            description="Optional guards consulted before a transition",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        transitions: list[Callable[..., Any]] | Callable[..., Any],
        validators: list[Callable[..., Any]] | Callable[..., Any] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.transition_list: list[Callable[..., Any]] = (
            transitions if isinstance(transitions, list) else [transitions]
        )
        if validators is None:
            self.validator_list: list[Callable[..., Any]] = []
        else:
            self.validator_list = validators if isinstance(validators, list) else [validators]
        self.trigger = trigger
        self.config = config

        # name→callable map (name-keying 约定)
        self._transitions: dict[str, Callable[..., Any]] = {}
        for t in self.transition_list:
            key = getattr(t, "name", None) or getattr(t, "__name__", None)
            if key is not None:
                self._transitions[key] = t

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

    def _blocked(self, state: Any, to_state: Any, reason: str) -> dict[str, Any]:
        return {
            "status": "blocked",
            "from_state": state,
            "to_state": to_state,
            "reason": reason,
            "result": None,
        }

    async def run(  # type: ignore[override]
        self,
        state: Any = None,
        *,
        to_state: str | None = None,
        input_data: dict[str, Any] | None = None,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Attempt a state transition to ``to_state``.

        Args:
            state: Current state (passed to validators / transition).
            to_state: Target state name; used to look up the transition.
            input_data: Optional payload passed to validators / transition.
            on_step: Optional callback called with the final result dict.

        Returns:
            {"status": "completed" | "blocked",
             "from_state": ..., "to_state": ..., "reason": str | None,
             "result": Any}

        Illegal / unknown transitions return a "blocked" result; never raise.
        """
        input_data = input_data or {}

        if to_state is None:
            result = self._blocked(state, to_state, "no_target_state")
            self._notify(on_step, result)
            return result

        transition = self._transitions.get(to_state)
        if transition is None:
            result = self._blocked(state, to_state, "unknown_transition")
            self._notify(on_step, result)
            return result

        # guards: 任一 validator 返回 falsy → 阻断
        for validator in self.validator_list:
            try:
                allowed = await self._invoke(
                    validator, state=state, to_state=to_state, input_data=input_data
                )
            except Exception as e:
                self._last_error = (
                    f"validator {getattr(validator, '__name__', validator)}: "
                    f"{type(e).__name__}: {e}"
                )
                logger.warning(f"StateMachineEngine '{self.name}' validator failed: {e}")
                allowed = False
            if not allowed:
                result = self._blocked(state, to_state, "guard_rejected")
                self._notify(on_step, result)
                return result

        # 执行迁移
        try:
            transition_result = await self._invoke(
                transition, state=state, to_state=to_state, input_data=input_data
            )
        except Exception as e:
            self._last_error = (
                f"transition {getattr(transition, '__name__', transition)}: "
                f"{type(e).__name__}: {e}"
            )
            logger.warning(f"StateMachineEngine '{self.name}' transition failed: {e}")
            result = self._blocked(state, to_state, "transition_error")
            result["error"] = str(e)
            self._notify(on_step, result)
            return result

        result = {
            "status": "completed",
            "from_state": state,
            "to_state": to_state,
            "reason": None,
            "result": transition_result,
        }
        self._notify(on_step, result)
        return result

    def _notify(
        self,
        on_step: Callable[[dict[str, Any]], None] | None,
        result: dict[str, Any],
    ) -> None:
        if on_step:
            try:
                on_step(result)
            except Exception as cb_err:
                logger.warning(f"on_step callback failed: {cb_err}")

    def stop(self) -> None:
        """Signal the engine to stop (no-op for on_demand engines)."""
        self._stop_event.set()

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "transitions_count": len(self._transitions),
                "validators_count": len(self.validator_list),
                "known_states": sorted(self._transitions.keys()),
                "last_error": self._last_error,
            },
        }


register_skeleton("state_machine_engine", StateMachineEngine)
