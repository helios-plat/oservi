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

from oservi.assembler import assemble, validate_manifest
from oservi.daemon_engine import DaemonEngine
from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    get_skeleton,
    list_skeletons,
    register_skeleton,
)
from oservi.engines.action_planner import ActionPlannerEngine
from oservi.engines.agentic_loop import AgenticLoopEngine

# 引擎 (触发注册到 registry)
from oservi.engines.alerter import AlerterEngine
from oservi.engines.app_installer import AppInstallerEngine
from oservi.engines.bulk_export_worker import BulkExportWorkerEngine
from oservi.engines.bulk_import_worker import BulkImportWorkerEngine
from oservi.engines.cron_scheduler_engine import CronSchedulerEngine
from oservi.engines.event_webhook_dispatcher import EventWebhookDispatcherEngine
from oservi.engines.feed_tracker import FeedTrackerEngine
from oservi.engines.mcp_bridge import McpBridgeEngine
from oservi.engines.researcher import ResearcherEngine
from oservi.engines.saga_composer import SagaComposerEngine
from oservi.engines.sequential_composer import SequentialComposerEngine
from oservi.engines.state_machine_engine import StateMachineEngine
from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine
from oservi.engines.triage import TriageEngine
from oservi.manifest import ManifestValidationError, ServiceManifest
from oservi.mcp_graft import GraftMcpTools
from oservi.memory_api import MemoryApi
from oservi.sft_exporter import build_sft_records, export_trajectories

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
    "DaemonEngine",
    "GraftMcpTools",
    "MemoryApi",
    "build_sft_records",
    "export_trajectories",
    "ActionPlannerEngine",
    "AppInstallerEngine",
    "SequentialComposerEngine",
    "SubagentOrchestratorEngine",
    "McpBridgeEngine",
    "SagaComposerEngine",
    "StateMachineEngine",
    "EventWebhookDispatcherEngine",
    "CronSchedulerEngine",
    "BulkImportWorkerEngine",
    "BulkExportWorkerEngine",
    "ChannelWatcherEngine",
    "ArxivWatcherEngine",
    "SourceWatcherEngine",
    "LongHorizonAgenticLoopEngine",
    "GraphSkillsAgenticLoopEngine",
    "System2CognitiveEngine",
]

from oservi.engines.arxiv_watcher import ArxivWatcherEngine
from oservi.engines.channel_watcher import ChannelWatcherEngine
from oservi.engines.graph_skills_agentic_loop import GraphSkillsAgenticLoopEngine
from oservi.engines.long_horizon_agentic_loop import LongHorizonAgenticLoopEngine
from oservi.engines.source_watcher import SourceWatcherEngine
from oservi.engines.system2_cognitive_engine import System2CognitiveEngine
