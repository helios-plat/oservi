"""oservice — 3O 服务装配层 (第 5 个 helios-plat 主库包).

定位:
  obase    无状态工具
  oservice 有状态机制 (引擎骨架 + 装配器)

5 红线:
  1. ≥2 真实项目实证 (首要)
  2. 机制/业务分离
  3. 注入点类型契约
  4. 无状态骨架定义
  5. 不反向依赖 (3O 四包禁 import oservi)

完整治理: docs/GOVERNANCE.md
"""

__version__ = "1.1.0"

from oservi.manifest import ServiceManifest, ManifestValidationError
from oservi.assembler import assemble, validate_manifest
from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
    get_skeleton,
    list_skeletons,
)

# 引擎 (触发注册到 registry)
from oservi.engines.alerter import AlerterEngine
from oservi.engines.researcher import ResearcherEngine
from oservi.engines.feed_tracker import FeedTrackerEngine
from oservi.engines.triage import TriageEngine
from oservi.engines.agentic_loop import AgenticLoopEngine
from oservi.engines.action_planner import ActionPlannerEngine
from oservi.engines.app_installer import AppInstallerEngine
from oservi.engines.sequential_composer import SequentialComposerEngine
from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine
from oservi.engines.mcp_bridge import McpBridgeEngine

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
    "SequentialComposerEngine",
    "SubagentOrchestratorEngine",
    "McpBridgeEngine",
]

from oservi.engines.channel_watcher import ChannelWatcherEngine
from oservi.engines.arxiv_watcher import ArxivWatcherEngine
