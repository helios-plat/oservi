"""oservi.trigger_register — 触发器统一注册 (KiroCrew 复刻)。

cron / webhook / event 三类触发器统一注册到绑定表, 触发即驱动指定 workflow
(EventWorkflowEngine.drive 或装配层注入的回调), 触发记录审计。

  - cron:    spec="cron 表达式" → 登记 (调度执行由装配层 scheduler 消费)
  - webhook: spec=路径 → 分配端点 /api/v1/trigger/<binding_id> (主仓接线)
  - event:   spec=event_bus 主题 → 订阅, 事件到达即触发

分层: oservi (编排) — 依赖 obase.event_bus / EventWorkflowEngine, 零 veya 反向依赖。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from obase.event_bus import Event, EventBus

TRIGGER_KINDS = ("cron", "webhook", "event")
TRIGGERS_FILE = Path.home() / ".veya" / "triggers.json"


class TriggerRegistry:
    """触发器注册表: 绑定表 (JSON 持久化) + event 订阅 + 触发审计。"""

    def __init__(self, event_bus: EventBus | None = None,
                 triggers_file: str = "") -> None:
        self.bus = event_bus or EventBus()
        self._file = Path(triggers_file or TRIGGERS_FILE)
        self._bindings: dict[str, dict[str, Any]] = {}
        self._callbacks: dict[str, Callable[[dict[str, Any]], Any]] = {}
        self._lock = threading.RLock()
        self._load()

    def _load(self) -> None:
        if not self._file.exists():
            return
        try:
            self._bindings = json.loads(self._file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            self._bindings = {}

    def _save(self) -> None:
        self._file.parent.mkdir(parents=True, exist_ok=True)
        self._file.write_text(json.dumps(self._bindings, ensure_ascii=False, indent=2),
                              encoding="utf-8")

    # ── 注册 ──────────────────────────────────────────────────────────
    def register(self, kind: str, spec: str, workflow_id: str, *,
                 binding: dict[str, Any] | None = None,
                 callback: Callable[[dict[str, Any]], Any] | None = None) -> dict[str, Any]:
        """统一注册: cron/webhook/event → 绑定条目。"""
        if kind not in TRIGGER_KINDS:
            raise ValueError(f"未知触发器类型: {kind}; 可选 {TRIGGER_KINDS}")
        if not spec:
            raise ValueError("spec 必填 (cron 表达式 / webhook 路径 / event 主题)")

        binding_id = f"trg_{uuid.uuid4().hex[:10]}"
        endpoint = ""
        if kind == "webhook":
            endpoint = f"/api/v1/trigger/{binding_id}"

        with self._lock:
            self._bindings[binding_id] = {
                "binding_id": binding_id,
                "kind": kind,
                "spec": spec,
                "workflow_id": workflow_id,
                "binding": dict(binding or {}),
                "endpoint": endpoint,
                "active": True,
                "created_at": time.time(),
                "last_triggered_at": 0.0,
                "trigger_count": 0,
            }
            if callback is not None:
                self._callbacks[binding_id] = callback
            if kind == "event":
                self.bus.subscribe(spec, self._make_handler(binding_id))
            self._save()

        return dict(self._bindings[binding_id])

    def _make_handler(self, binding_id: str) -> Callable[[Event], None]:
        def handler(event: Event) -> None:
            self.trigger(binding_id, {"event": event.type,
                                      "payload": event.payload or {}})
        return handler

    # ── 触发 ──────────────────────────────────────────────────────────
    def trigger(self, binding_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """手动/自动触发绑定 (记录审计 + 调用回调)。"""
        with self._lock:
            b = self._bindings.get(binding_id)
            if b is None or not b.get("active"):
                return {"ok": False, "error": "绑定不存在或已停用"}
            b["trigger_count"] = b.get("trigger_count", 0) + 1
            b["last_triggered_at"] = time.time()
            self._save()
            binding = dict(b)
            callback = self._callbacks.get(binding_id)

        # 触发审计
        try:
            from oprim._audit_emit import AuditEvent, JsonlSink

            sink = JsonlSink(str(Path.home() / ".veya" / "audit" / "trigger.jsonl"))
            sink.write(AuditEvent(
                event_type="decide",
                trace_id=f"trg_{binding_id}",
                inputs={"binding_id": binding_id, "kind": binding.get("kind"),
                        "workflow_id": binding.get("workflow_id")},
                decision={"triggered": True, "payload_keys": list(payload.keys())},
            ))
        except Exception:
            import logging

            logging.getLogger("veya.trigger").warning("audit failed", exc_info=True)

        if callback is not None:
            try:
                result = callback(payload)
                return {"ok": True, "binding_id": binding_id,
                        "workflow_id": binding.get("workflow_id"),
                        "callback": str(result)[:500] if result else "ok"}
            except Exception as e:  # noqa: BLE001
                return {"ok": False, "binding_id": binding_id, "error": str(e)[:500]}
        return {"ok": True, "binding_id": binding_id,
                "workflow_id": binding.get("workflow_id"), "dispatched": True}

    # ── 管理 ──────────────────────────────────────────────────────────
    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(b) for b in self._bindings.values()]

    def deactivate(self, binding_id: str) -> bool:
        with self._lock:
            b = self._bindings.get(binding_id)
            if b is None:
                return False
            b["active"] = False
            self._save()
            return True


def trigger_register(
    trigger: dict[str, Any],
    workflow_id: str,
    *,
    binding: dict[str, Any] | None = None,
    registry: TriggerRegistry | None = None,
) -> dict[str, Any]:
    """主入口: 统一注册触发器。"""
    reg = registry or TriggerRegistry()
    kind = str(trigger.get("kind", "")).lower()
    spec = str(trigger.get("spec", ""))
    return reg.register(kind, spec, workflow_id, binding=binding)


__all__ = ["TRIGGER_KINDS", "TriggerRegistry", "trigger_register"]
