"""oservice — 3O 服务装配层 (第 5 个 helios-plat 主库包).

定位:
  obase    无状态工具
  oservice 有状态机制 (引擎骨架 + 装配器)

5 红线:
  1. ≥2 真实项目实证 (首要)
  2. 机制/业务分离
  3. 注入点类型契约
  4. 无状态骨架定义
  5. 不反向依赖 (3O 四包禁 import oservice)

完整治理: docs/GOVERNANCE.md
"""

__version__ = "0.4.3"

from oservice.manifest import ServiceManifest, ManifestValidationError
from oservice.assembler import assemble, validate_manifest
from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
    get_skeleton,
    list_skeletons,
)

# 引擎 (触发注册到 registry)
from oservice.engines.alerter import AlerterEngine
from oservice.engines.researcher import ResearcherEngine
from oservice.engines.feed_tracker import FeedTrackerEngine
from oservice.engines.triage import TriageEngine
from oservice.engines.agentic_loop import AgenticLoopEngine
from oservice.engines.action_planner import ActionPlannerEngine
from oservice.engines.app_installer import AppInstallerEngine

__all__ = [
    "__version__",
    # Manifest
    "ServiceManifest",
    "ManifestValidationError",
    # Assembler
    "assemble",
    "validate_manifest",
    # Engine 基础设施
    "EngineSkeleton",
    "Injection",
    "register_skeleton",
    "get_skeleton",
    "list_skeletons",
    # Engines
    "AlerterEngine",
    "ResearcherEngine",
    "FeedTrackerEngine",
    "TriageEngine",
    "AgenticLoopEngine",
    "ActionPlannerEngine",
    "AppInstallerEngine",
]
