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

__version__ = "1.4.0"

# Public contract for service composition.  Engines remain stateless and only
# describe injected dependencies; application persistence stays outside this
# package.
__manifest__ = {
    "package": "oservi",
    "version": __version__,
    "elements": [
        {
            "name": "ProductionExecutionEngine",
            "kind": "oservi",
            "module": "oservi.engines.production_execution",
            "signature": "run(*, input_data, output_dir, config=None) -> dict",
            "depends_on": ["omodul.operation"],
            "pillars": ["cost", "fingerprint", "trail", "report"],
        },
        {
            "name": "SequentialComposerEngine",
            "kind": "oservi",
            "module": "oservi.engines.sequential_composer",
            "signature": "run(input_data) -> dict",
            "depends_on": ["omodul.operation"],
            "pillars": ["cost", "fingerprint", "trail", "report"],
        },
    ],
}

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
from oservi.engines.production_execution import ExecutionEvent, ProductionExecutionEngine
from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine
from oservi.engines.mcp_bridge import McpBridgeEngine
from oservi.engines.saga_composer import SagaComposerEngine
from oservi.engines.state_machine_engine import StateMachineEngine
from oservi.engines.event_webhook_dispatcher import EventWebhookDispatcherEngine
from oservi.engines.bulk_import_worker import BulkImportWorkerEngine
from oservi.engines.bulk_export_worker import BulkExportWorkerEngine

__all__ = [
    "__version__",
    "__manifest__",
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
    "ExecutionEvent",
    "ProductionExecutionEngine",
    "SubagentOrchestratorEngine",
    "McpBridgeEngine",
    "SagaComposerEngine",
    "StateMachineEngine",
    "EventWebhookDispatcherEngine",
    "CronSchedulerEngine",
    "BulkImportWorkerEngine",
    "BulkExportWorkerEngine",
]

from oservi.engines.channel_watcher import ChannelWatcherEngine
from oservi.engines.arxiv_watcher import ArxivWatcherEngine

from oservi.engines.source_watcher import SourceWatcherEngine

try:  # pragma: no cover - depends on optional installation extras
    from oservi.engines.cron_scheduler_engine import CronSchedulerEngine
except ModuleNotFoundError as exc:
    if exc.name != "croniter":
        raise
    CronSchedulerEngine = None  # type: ignore[assignment,misc]

if CronSchedulerEngine is None:
    __all__.remove("CronSchedulerEngine")
