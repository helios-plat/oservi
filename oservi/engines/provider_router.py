"""Provider Router service engine.

The engine owns only the reusable sequencing mechanism. Provider metadata,
selection, transport, usage persistence, and Layer4 context are injected.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from omodul.provider_inference import ProviderInferenceConfig, ProviderInferenceInput

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton


def _one(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


async def _call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


class ProviderRouterEngine(EngineSkeleton):
    """On-demand provider selection and inference transaction mechanism."""

    injection_points: dict[str, Injection] = {  # noqa: RUF012
        "select_provider": Injection(
            kind="oskill", cardinality="1", description="stateless execution provider selector"
        ),
        "fallback_decision": Injection(
            kind="oskill", cardinality="1", description="stateless explicit fallback policy"
        ),
        "provider_call": Injection(
            kind="oprim", cardinality="1", description="one provider call primitive"
        ),
        "usage_record": Injection(
            kind="oprim", cardinality="1", description="one usage recording primitive"
        ),
        "provider_inference_transaction": Injection(
            kind="omodul", cardinality="1", description="canonical inference transaction"
        ),
        "provider_caller": Injection(
            kind="layer4", cardinality="1", description="credential-owning provider adapter"
        ),
        "provider_health_probe": Injection(
            kind="oprim", cardinality="0..1", description="optional one-provider health probe"
        ),
        "pricing_lookup": Injection(
            kind="oprim", cardinality="0..1", description="optional pricing lookup"
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        select_provider: Callable[..., Any] | list[Callable[..., Any]],
        fallback_decision: Callable[..., Any] | list[Callable[..., Any]],
        provider_call: Callable[..., Any] | list[Callable[..., Any]],
        usage_record: Callable[..., Any] | list[Callable[..., Any]],
        provider_inference_transaction: Callable[..., Any] | list[Callable[..., Any]],
        provider_caller: Callable[..., Any] | list[Callable[..., Any]],
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        provider_health_probe: Callable[..., Any] | list[Callable[..., Any]] | None = None,
        pricing_lookup: Callable[..., Any] | list[Callable[..., Any]] | None = None,
    ) -> None:
        self.name = name
        self.select_provider = _one(select_provider)
        self.fallback_decision = _one(fallback_decision)
        self.provider_call = _one(provider_call)
        self.usage_record = _one(usage_record)
        self.provider_inference_transaction = _one(provider_inference_transaction)
        self.provider_caller = _one(provider_caller)
        self.provider_health_probe = _one(provider_health_probe)
        self.pricing_lookup = _one(pricing_lookup)
        self.trigger = trigger
        self.config = config
        self._running = False
        self._request_count = 0
        self._last_error: str | None = None

    def run(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    async def infer(
        self,
        request: ProviderInferenceInput | Mapping[str, Any],
        *,
        output_dir: Path | None = None,
        on_step: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        """Run the injected inference transaction for one request."""
        if not self._running:
            return {"status": "failed", "error": {"type": "RouterStopped"}}
        if isinstance(request, ProviderInferenceInput):
            input_data = request
            config_data: Mapping[str, Any] = self.config
        else:
            raw = dict(request)
            config_data = cast(Mapping[str, Any], raw.pop("config", self.config))
            raw.update(
                {
                    "provider_call": self.provider_call,
                    "select_provider": self.select_provider,
                    "fallback_decision": self.fallback_decision,
                    "usage_record": self.usage_record,
                    "provider_caller": raw.get("provider_caller", self.provider_caller),
                }
            )
            input_data = ProviderInferenceInput(**raw)
        allowed = {
            "llm_provider",
            "llm_model",
            "capability",
            "streaming",
            "max_attempts",
            "fallback_policy",
            "preferred_provider",
            "strict_pricing",
        }
        transaction_config = ProviderInferenceConfig(
            **{key: value for key, value in config_data.items() if key in allowed}
        )
        try:
            result = await _call(
                self.provider_inference_transaction,
                transaction_config,
                input_data,
                output_dir or Path(self.config.get("output_dir", ".veya/provider")),
                on_step=on_step,
            )
            self._request_count += 1
            if isinstance(result, dict) and result.get("status") == "failed":
                self._last_error = str(result.get("error"))
            return cast(dict[str, Any], result)
        except Exception as exc:  # noqa: BLE001 - service boundary fails closed
            self._last_error = f"{type(exc).__name__}: {exc}"
            return {"status": "failed", "error": {"type": type(exc).__name__, "message": str(exc)}}

    async def probe(self, provider: Any, **kwargs: Any) -> Any:
        if self.provider_health_probe is None:
            return {"healthy": None, "status": "unconfigured"}
        return await _call(self.provider_health_probe, provider, **kwargs)

    def lookup_pricing(self, query: Mapping[str, Any]) -> Any:
        if self.pricing_lookup is None:
            return None
        return self.pricing_lookup(query, table=self.config.get("pricing_table"))

    def health(self) -> dict[str, Any]:
        required = (
            self.select_provider,
            self.fallback_decision,
            self.provider_call,
            self.usage_record,
            self.provider_inference_transaction,
            self.provider_caller,
        )
        return {
            "status": "healthy"
            if self._running and all(item is not None for item in required)
            else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "request_count": self._request_count,
                "last_error": self._last_error,
                "semantic_routing": False,
                "injections": [
                    "select_provider",
                    "fallback_decision",
                    "provider_call",
                    "usage_record",
                    "provider_inference_transaction",
                    "provider_caller",
                ],
            },
        }


register_skeleton("provider_router", ProviderRouterEngine)

__all__ = ["ProviderRouterEngine"]
