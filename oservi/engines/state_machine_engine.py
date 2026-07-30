"""State Machine Engine Skeleton.

机制 (固化):
- 校验 (from_status, to_status) 是否在合法转移表内,不在表内直接拒绝
  ("拦截非法状态跳跃")
- 依次跑所有注入的 validators,任一返回 False 就拒绝转移
- 通过校验后调用对应的转移 omodul 执行实际业务动作

业务 (注入):
- transitions: omodul callable 列表(哪些函数负责哪个转移,由
  config["transition_map"] 里的 "from->to" -> 函数名 映射决定,不是靠字典
  类型的注入点——通用装配器的 Injection 只理解 list[Callable],没法直接
  校验 dict[str, Callable] 形状的注入,所以"状态图"整个放进 config,inject
  只放纯 callable 列表,这是本骨架相对 SPEC 原始描述 `transitions: dict[str,
  omodul]` 的一个必要落地调整)
- validators: oskill 纯函数列表(合法性附加校验,如"金额必须已结清"这种
  跟状态图本身无关的业务规则)

红线对照:
- 红线 2 (机制/业务分离): 具体状态图、转移动作、校验规则全靠注入/config
- 红线 3 (注入契约): transitions=omodul(1..n) / validators=oskill(0..n)
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


class StateMachineEngine(EngineSkeleton):
    """状态机防呆引擎骨架:拦截非法状态跳跃 + 校验器 + 执行转移动作。

    Example::

        engine = StateMachineEngine(
            transitions=[mark_draft_order_paid, cancel_order, archive_order],
            validators=[],
            trigger={"on_demand": True},
            config={
                "transition_map": {
                    "draft->pending": "mark_draft_order_paid",
                    "pending->canceled": "cancel_order",
                    "pending->archived": "archive_order",
                },
            },
            name="order-state-machine",
        )
        result = asyncio.run(
            engine.run(from_status="draft", to_status="pending", input_data={"order_id": "o1"})
        )
    """

    injection_points: ClassVar[dict] = {
        "transitions": Injection(
            kind="omodul",
            cardinality="1..n",
            description="Transition-executing omodul callables, looked up by name via "
            "config['transition_map']",
        ),
        "validators": Injection(
            kind="oskill",
            cardinality="0..n",
            description="Additional pure validators; any returning False blocks the transition",
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
        transition_list = transitions if isinstance(transitions, list) else [transitions]
        self._transitions_by_name = {fn.__name__: fn for fn in transition_list}
        if validators is None:
            self.validator_list: list[Callable[..., Any]] = []
        elif isinstance(validators, list):
            self.validator_list = validators
        else:
            self.validator_list = [validators]
        self.trigger = trigger
        self.config = config
        self.transition_map: dict[str, str] = config.get("transition_map", {})

        self._last_error: str | None = None
        self._transition_count = 0

    async def run(  # type: ignore[override]
        self,
        *,
        from_status: str,
        to_status: str,
        input_data: dict[str, Any] | None = None,
        on_step: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Attempt a single state transition.

        Args:
            from_status: Current state.
            to_status: Desired next state.
            input_data: Business context passed to validators and the
                transition callable.
            on_step: Optional callback invoked once with the outcome.

        Returns:
            {"status": "completed", "transition": "from->to", "result": ...} on
            success, or {"status": "failed", "error": ...} if the transition is
            illegal, a validator rejects it, or the transition callable itself
            fails.
        """
        input_data = input_data or {}
        key = f"{from_status}->{to_status}"

        if key not in self.transition_map:
            self._last_error = f"illegal transition: {key}"
            return self._reject(key, self._last_error, on_step)

        for validator in self.validator_list:
            try:
                ok = await _call(
                    validator, from_status=from_status, to_status=to_status, input_data=input_data
                )
            except Exception as e:
                self._last_error = f"validator {validator.__name__} raised: {e}"
                return self._reject(key, self._last_error, on_step)
            if not ok:
                self._last_error = f"validator {validator.__name__} rejected transition {key}"
                return self._reject(key, self._last_error, on_step)

        fn_name = self.transition_map[key]
        fn = self._transitions_by_name.get(fn_name)
        if fn is None:
            self._last_error = f"transition function {fn_name!r} not found in injected transitions"
            return self._reject(key, self._last_error, on_step)

        try:
            result = await _call(
                fn, from_status=from_status, to_status=to_status, input_data=input_data
            )
        except Exception as e:
            self._last_error = f"transition {fn_name} raised: {e}"
            return self._reject(key, self._last_error, on_step)

        self._transition_count += 1
        outcome = {"status": "completed", "transition": key, "result": result}
        if on_step:
            try:
                on_step(outcome)
            except Exception as cb_err:
                logger.warning(f"on_step callback failed: {cb_err}")
        return outcome

    def _reject(
        self, key: str, message: str, on_step: Callable[[dict[str, Any]], None] | None
    ) -> dict[str, Any]:
        logger.warning(f"StateMachineEngine '{self.name}' rejected {key}: {message}")
        outcome = {"status": "failed", "transition": key, "error": message}
        if on_step:
            try:
                on_step(outcome)
            except Exception as cb_err:
                logger.warning(f"on_step callback failed: {cb_err}")
        return outcome

    def stop(self) -> None:
        """No-op for on_demand engines."""

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy",
            "details": {
                "name": self.name,
                "transitions_count": len(self._transitions_by_name),
                "validators_count": len(self.validator_list),
                "known_transitions": sorted(self.transition_map.keys()),
                "transition_count": self._transition_count,
                "last_error": self._last_error,
            },
        }


register_skeleton("state_machine_engine", StateMachineEngine)
