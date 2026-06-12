"""基础测试: manifest + assembler 红线校验."""

import pytest
from oservi import (
    ServiceManifest,
    ManifestValidationError,
    EngineSkeleton,
    Injection,
    assemble,
    validate_manifest,
    register_skeleton,
    list_skeletons,
)


# 测试用的最小引擎骨架
class _MockAlerter(EngineSkeleton):
    """测试用 mock alerter."""
    
    injection_points = {
        "evaluators": Injection(kind="oprim", cardinality="1..n", description="评估器"),
        "channels": Injection(kind="obase", cardinality="1..n", description="推送通道"),
    }
    
    def __init__(self, *, evaluators, channels, trigger, config, name):
        self.evaluators = evaluators
        self.channels = channels
        self.trigger = trigger
        self.config = config
        self.name = name
        self._running = False
    
    def run(self) -> None:
        self._running = True
    
    def stop(self) -> None:
        self._running = False


# Fake oprim/obase callable (用于测试 kind 检测)
def fake_oprim_evaluator():
    """模拟 oprim 元素."""
    pass


def fake_obase_channel():
    """模拟 obase 元素."""
    pass


# 设置 __module__ 让 _detect_element_kind 能识别
fake_oprim_evaluator.__module__ = "oprim.fake_evaluator"  # type: ignore[attr-defined]
fake_obase_channel.__module__ = "obase.notify.fake_channel"  # type: ignore[attr-defined]


def fake_unknown_callable():
    """模拟来源 unknown 的 callable."""
    pass


fake_unknown_callable.__module__ = "stratum.services.fake"  # type: ignore[attr-defined]


@pytest.fixture(scope="module", autouse=True)
def register_mock_alerter():
    """模块级注册 mock_alerter."""
    register_skeleton("mock_alerter", _MockAlerter)
    yield


# ===== ServiceManifest 基础测试 =====

class TestServiceManifest:
    def test_minimal_manifest(self):
        m = ServiceManifest(
            name="test-service",
            skeleton="mock_alerter",
            inject={},
            trigger={"on_interval": 60},
        )
        assert m.name == "test-service"
        assert m.config == {}
    
    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name cannot be empty"):
            ServiceManifest(name="", skeleton="x", inject={}, trigger={})
    
    def test_empty_skeleton_raises(self):
        with pytest.raises(ValueError, match="skeleton cannot be empty"):
            ServiceManifest(name="x", skeleton="", inject={}, trigger={})
    
    def test_inject_must_be_dict(self):
        with pytest.raises(TypeError, match="inject must be dict"):
            ServiceManifest(name="x", skeleton="y", inject="not-a-dict", trigger={})  # type: ignore


# ===== 红线 3: 注入契约校验 =====

class TestInjectionContractRedline3:
    def test_skeleton_not_registered_raises(self):
        m = ServiceManifest(
            name="test",
            skeleton="nonexistent_skeleton",
            inject={},
            trigger={},
        )
        with pytest.raises(ManifestValidationError, match="not registered"):
            validate_manifest(m)
    
    def test_cardinality_1n_missing_raises(self):
        m = ServiceManifest(
            name="test",
            skeleton="mock_alerter",
            inject={"evaluators": [], "channels": [fake_obase_channel]},  # 缺 evaluators
            trigger={},
        )
        with pytest.raises(ManifestValidationError, match="cardinality=1..n"):
            validate_manifest(m)
    
    def test_kind_mismatch_oprim_position_filled_by_obase_raises(self):
        m = ServiceManifest(
            name="test",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_obase_channel],  # 错: obase 塞 oprim 位
                "channels": [fake_obase_channel],
            },
            trigger={},
        )
        with pytest.raises(ManifestValidationError, match="expected kind='oprim'"):
            validate_manifest(m)
    
    def test_unknown_kind_raises(self):
        m = ServiceManifest(
            name="test",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_unknown_callable],
                "channels": [fake_obase_channel],
            },
            trigger={},
        )
        with pytest.raises(ManifestValidationError, match="got 'unknown'"):
            validate_manifest(m)
    
    def test_undeclared_injection_point_raises(self):
        m = ServiceManifest(
            name="test",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_oprim_evaluator],
                "channels": [fake_obase_channel],
                "typo_point": [fake_oprim_evaluator],  # mock_alerter 没声明此注入点
            },
            trigger={},
        )
        with pytest.raises(ManifestValidationError, match="not declared in skeleton"):
            validate_manifest(m)
    
    def test_valid_manifest_passes(self):
        m = ServiceManifest(
            name="test",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_oprim_evaluator],
                "channels": [fake_obase_channel],
            },
            trigger={"on_interval": 60},
        )
        validate_manifest(m)  # 不应 raise


# ===== assemble 端到端 =====

class TestAssemble:
    def test_assemble_returns_engine_instance(self):
        m = ServiceManifest(
            name="test-alerter-1",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_oprim_evaluator],
                "channels": [fake_obase_channel],
            },
            trigger={"on_interval": 60},
            config={"key": "value"},
        )
        service = assemble(m)
        assert isinstance(service, _MockAlerter)
        assert service.name == "test-alerter-1"
        assert service.evaluators == [fake_oprim_evaluator]
        assert service.channels == [fake_obase_channel]
        assert service.trigger == {"on_interval": 60}
        assert service.config == {"key": "value"}
    
    def test_assemble_invalid_manifest_raises(self):
        m = ServiceManifest(
            name="bad",
            skeleton="mock_alerter",
            inject={},  # 缺所有必填注入
            trigger={},
        )
        with pytest.raises(ManifestValidationError):
            assemble(m)
    
    def test_assembled_service_can_run_and_stop(self):
        m = ServiceManifest(
            name="lifecycle-test",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_oprim_evaluator],
                "channels": [fake_obase_channel],
            },
            trigger={},
        )
        service = assemble(m)
        assert service._running is False
        service.run()
        assert service._running is True
        service.stop()
        assert service._running is False
    
    def test_assembled_service_health(self):
        m = ServiceManifest(
            name="health-test",
            skeleton="mock_alerter",
            inject={
                "evaluators": [fake_oprim_evaluator],
                "channels": [fake_obase_channel],
            },
            trigger={},
        )
        service = assemble(m)
        h = service.health()
        assert h["status"] == "healthy"


# ===== Registry =====

class TestRegistry:
    def test_mock_alerter_registered(self):
        assert "mock_alerter" in list_skeletons()
    
    def test_double_register_raises(self):
        with pytest.raises(ValueError, match="already registered"):
            register_skeleton("mock_alerter", _MockAlerter)
    
    def test_register_non_skeleton_raises(self):
        class NotASkeleton:
            pass
        
        with pytest.raises(TypeError, match="must subclass EngineSkeleton"):
            register_skeleton("bad_skel", NotASkeleton)  # type: ignore[arg-type]
