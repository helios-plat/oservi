"""3O 范式全栈 — oservi 引擎清单 (v3.0 规范引擎定义, 元素 25 前置契约).

engine_manifest 是规范引用入口; ServiceManifest 实现单源收敛于 oservi.manifest
(既有实现为 spec 版本的超集: 多一个 config 字段, 引擎注入契约完全兼容)。
"""

from __future__ import annotations

from typing import Literal

from oservi.manifest import ManifestValidationError, ServiceManifest, TriggerMode

InjectionKind = Literal["oprim", "oskill", "omodul", "obase", "layer4"]

__all__ = [
    "ServiceManifest",
    "TriggerMode",
    "ManifestValidationError",
    "InjectionKind",
]
