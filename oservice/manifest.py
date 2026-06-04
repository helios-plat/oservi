"""Service Manifest — 声明式服务定义.

Manifest 是装配输入. assemble(manifest) 输出可 run 的 Service.

设计取舍 (vs YAML / K8s DSL):
- Python dataclass: 类型系统 + IDE 校验白嫖, 易调试
- 无 deploy 段: 容器化交项目现有 Docker 流程, assembler 单一职责
- 无 status 段: v0.1 朴素装配, 无 reconcile, 无 spec/status 分离 (无实证)

无实证之前不上 K8s 级 DSL. 等 ≥2 项目需要 "几十个服务自动 reconcile" 再扩展.
"""

from dataclasses import dataclass, field
from typing import Callable, Any


@dataclass
class ServiceManifest:
    """声明式服务定义.
    
    Args:
        name: 服务唯一标识 (e.g. "tide-realtime-alerter")
        skeleton: 引擎骨架名 (e.g. "alerter" / "collector" / "scheduler")
        inject: 注入填料字典. key = 骨架声明的注入点名, value = callable 列表
                e.g. {"evaluators": [oprim.X, oprim.Y], "channels": [obase.notify.telegram_send]}
        trigger: 触发配置 (e.g. {"on_interval": 300} / {"on_cron": "0 3 * * *"})
        config: 业务实例参数 (e.g. {"thresholds": {"warn": 0.5}})
    
    Example:
        >>> from oprim import detect_sector_collapse
        >>> from obase.notify import telegram_send
        >>> manifest = ServiceManifest(
        ...     name="tide-alerter",
        ...     skeleton="alerter",
        ...     inject={"evaluators": [detect_sector_collapse], "channels": [telegram_send]},
        ...     trigger={"on_interval": 300},
        ...     config={"thresholds": {"warn": 0.5}},
        ... )
    """
    
    name: str
    skeleton: str
    inject: dict[str, list[Callable[..., Any]]]
    trigger: dict[str, Any]
    config: dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ServiceManifest.name cannot be empty")
        if not self.skeleton:
            raise ValueError("ServiceManifest.skeleton cannot be empty")
        if not isinstance(self.inject, dict):
            raise TypeError("ServiceManifest.inject must be dict")
        if not isinstance(self.trigger, dict):
            raise TypeError("ServiceManifest.trigger must be dict")


class ManifestValidationError(Exception):
    """Manifest 校验失败 (装配前校验阶段)."""
