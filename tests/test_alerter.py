"""AlerterEngine 测试 — 双实证收敛后的机制验证."""

import asyncio
import pytest
from unittest.mock import MagicMock
from oservice import (
    ServiceManifest,
    ManifestValidationError,
    AlerterEngine,
    assemble,
    list_skeletons,
)


# ===== Fake oprim evaluators (mock 业务规则) =====

def fake_evaluator_sector_collapse(*, config):
    """模拟 oprim.detect_sector_collapse 返回 1 个告警事件."""
    return [{
        "entity_id": "sector_collapse_001",
        "severity": "high",
        "message": "Sector A collapsed 5%",
    }]


def fake_evaluator_no_events(*, config):
    """模拟无告警的 evaluator (返回空 list)."""
    return []


def fake_evaluator_multi_events(*, config):
    """模拟多事件 evaluator."""
    return [
        {"entity_id": "e1", "severity": "warn", "message": "M1"},
        {"entity_id": "e2", "severity": "critical", "message": "M2"},
    ]


def fake_evaluator_dict_return(*, config):
    """模拟单 dict 返回 (非 list)."""
    return {"entity_id": "single", "severity": "warn", "message": "S"}


def fake_evaluator_raises(*, config):
    """模拟 evaluator 抛异常."""
    raise RuntimeError("simulated evaluator failure")


# 设置 __module__ 让 oservice 装配器识别为 oprim
for fn in [
    fake_evaluator_sector_collapse,
    fake_evaluator_no_events,
    fake_evaluator_multi_events,
    fake_evaluator_dict_return,
    fake_evaluator_raises,
]:
    fn.__module__ = "oprim.fake"  # type: ignore[attr-defined]


# ===== Fake obase channels =====

class _ChannelCalls:
    """记录 channel 调用."""
    def __init__(self):
        self.calls = []


def make_fake_channel(recorder):
    """工厂: 创建一个记录调用的 channel callable."""
    def fake_channel(*, text, chat_id, bot_token):
        recorder.calls.append({"text": text, "chat_id": chat_id})
        return {"success": True}
    fake_channel.__module__ = "obase.notify.fake_channel"
    return fake_channel


def fake_channel_telegram(*, text, chat_id, bot_token):
    """模拟 obase.notify.telegram_send."""
    return {"success": True, "message_id": 123}


fake_channel_telegram.__module__ = "obase.notify.telegram"  # type: ignore[attr-defined]


# ===== 测试: 注册 + 装配 =====

class TestAlerterRegistration:
    def test_alerter_registered(self):
        assert "alerter" in list_skeletons()
    
    def test_alerter_assemble_basic(self):
        m = ServiceManifest(
            name="test-alerter-1",
            skeleton="alerter",
            inject={
                "evaluators": [fake_evaluator_sector_collapse],
                "channels": [fake_channel_telegram],
            },
            trigger={"on_interval": 60},
            config={},
        )
        service = assemble(m)
        assert isinstance(service, AlerterEngine)
        assert service.name == "test-alerter-1"
        assert len(service.evaluators) == 1
        assert len(service.channels) == 1
    
    def test_alerter_no_trigger_raises(self):
        m = ServiceManifest(
            name="bad",
            skeleton="alerter",
            inject={
                "evaluators": [fake_evaluator_sector_collapse],
                "channels": [fake_channel_telegram],
            },
            trigger={},  # 无 on_interval / on_cron
            config={},
        )
        with pytest.raises(ValueError, match="on_interval"):
            assemble(m)
    
    def test_alerter_cardinality_1n_evaluators_enforced(self):
        m = ServiceManifest(
            name="bad",
            skeleton="alerter",
            inject={"evaluators": [], "channels": [fake_channel_telegram]},
            trigger={"on_interval": 60},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1..n"):
            assemble(m)


# ===== 测试: 评估器调用 =====

class TestAlerterEvaluatorInvocation:
    @pytest.mark.asyncio
    async def test_call_evaluator_returns_list(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        events = await engine._call_evaluator(fake_evaluator_sector_collapse)
        assert len(events) == 1
        assert events[0]["entity_id"] == "sector_collapse_001"
    
    @pytest.mark.asyncio
    async def test_call_evaluator_returns_empty(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_no_events],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        events = await engine._call_evaluator(fake_evaluator_no_events)
        assert events == []
    
    @pytest.mark.asyncio
    async def test_call_evaluator_dict_wrapped_to_list(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_dict_return],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        events = await engine._call_evaluator(fake_evaluator_dict_return)
        assert len(events) == 1
        assert events[0]["entity_id"] == "single"


# ===== 测试: 节流 =====

class TestAlerterThrottle:
    def test_throttle_same_entity_within_window_filtered(self):
        import time as t
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"throttle_seconds": 600},
            name="t",
        )
        events = [
            {"entity_id": "e1", "severity": "warn", "message": "first"},
        ]
        filtered_1 = engine._filter_throttled_and_deduped(events)
        assert len(filtered_1) == 1
        
        # 立即重复 → 被节流
        filtered_2 = engine._filter_throttled_and_deduped(events)
        assert len(filtered_2) == 0
    
    def test_different_entities_not_throttled(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"throttle_seconds": 600},
            name="t",
        )
        events = [
            {"entity_id": "e1", "severity": "warn", "message": "m1"},
            {"entity_id": "e2", "severity": "warn", "message": "m2"},
        ]
        filtered = engine._filter_throttled_and_deduped(events)
        assert len(filtered) == 2
    
    def test_no_throttle_when_throttle_seconds_zero(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"throttle_seconds": 0},  # 关节流
            name="t",
        )
        events = [{"entity_id": "e1", "severity": "warn", "message": "m"}]
        f1 = engine._filter_throttled_and_deduped(events)
        f2 = engine._filter_throttled_and_deduped(events)
        assert len(f1) == 1 and len(f2) == 1


# ===== 测试: 去重 =====

class TestAlerterDedup:
    def test_dedup_same_bucket_filtered(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"dedup_bucket_seconds": 3600},  # 1h 桶, 无节流
            name="t",
        )
        events_1 = [{"entity_id": "e1", "severity": "warn", "message": "m1"}]
        events_2 = [{"entity_id": "e1", "severity": "warn", "message": "m2 different message"}]
        f1 = engine._filter_throttled_and_deduped(events_1)
        f2 = engine._filter_throttled_and_deduped(events_2)
        assert len(f1) == 1
        assert len(f2) == 0  # 同 entity + 同 severity 同桶 → 去重


# ===== 测试: 静音时段 =====

class TestAlerterQuietHours:
    def test_no_quiet_hours_config(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        assert engine._is_in_quiet_hours() is False
    
    def test_invalid_quiet_hours_returns_false(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"quiet_hours": {"start": "invalid", "end": "x"}},
            name="t",
        )
        assert engine._is_in_quiet_hours() is False


# ===== 测试: 推送到通道 =====

class TestAlerterDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_calls_all_channels(self):
        recorder = _ChannelCalls()
        ch1 = make_fake_channel(recorder)
        ch2 = make_fake_channel(recorder)
        
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[ch1, ch2],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        event = {"entity_id": "e1", "severity": "warn", "message": "test msg"}
        await engine._dispatch_to_channels(event)
        
        assert len(recorder.calls) == 2  # 2 channels each called once
        for call in recorder.calls:
            assert "test msg" in call["text"]
            assert "[WARN]" in call["text"]


# ===== 测试: 完整迭代 (端到端 mock) =====

class TestAlerterFullIteration:
    @pytest.mark.asyncio
    async def test_iterate_once_calls_evaluators_and_dispatches(self):
        recorder = _ChannelCalls()
        ch = make_fake_channel(recorder)
        
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse, fake_evaluator_no_events],
            channels=[ch],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        await engine._iterate_once()
        
        # sector_collapse 返 1 事件 → 推送; no_events 返空 → 不推
        assert len(recorder.calls) == 1
        assert "sector_collapse_001" in recorder.calls[0]["text"]
    
    @pytest.mark.asyncio
    async def test_iterate_once_evaluator_exception_isolated(self):
        recorder = _ChannelCalls()
        ch = make_fake_channel(recorder)
        
        engine = AlerterEngine(
            evaluators=[fake_evaluator_raises, fake_evaluator_sector_collapse],
            channels=[ch],
            trigger={"on_interval": 60},
            config={},
            name="t",
        )
        # 不应 raise, 失败 evaluator 跳过, 成功的继续
        await engine._iterate_once()
        assert len(recorder.calls) == 1


# ===== 测试: 健康检查 =====

class TestAlerterHealth:
    def test_health_initial(self):
        engine = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={},
            name="health-test",
        )
        h = engine.health()
        assert h["status"] == "stopped"  # not running yet
        assert h["details"]["name"] == "health-test"
        assert h["details"]["evaluators_count"] == 1
        assert h["details"]["channels_count"] == 1
        assert h["details"]["iteration_count"] == 0


# ===== 测试: 红线 4 — 无状态骨架定义 =====

class TestAlerterNoStaticState:
    def test_two_instances_have_independent_state(self):
        e1 = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"throttle_seconds": 60},
            name="e1",
        )
        e2 = AlerterEngine(
            evaluators=[fake_evaluator_sector_collapse],
            channels=[fake_channel_telegram],
            trigger={"on_interval": 60},
            config={"throttle_seconds": 60},
            name="e2",
        )
        events = [{"entity_id": "shared", "severity": "warn", "message": "m"}]
        f1 = e1._filter_throttled_and_deduped(events)
        # e2 不受 e1 节流影响 (实例状态独立)
        f2 = e2._filter_throttled_and_deduped(events)
        assert len(f1) == 1 and len(f2) == 1
