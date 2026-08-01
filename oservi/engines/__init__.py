"""引擎骨架集合.

Phase 1:
- alerter (Aegis AlertEngine + Tide AlertSchedulerEngine 双实证, 待实施)

待 ≥2 实证后入库:
- webhook_dispatcher
- collector
- scheduler
- api_server
- agentic_loop
- orchestrator (推后)

引擎骨架准入 5 红线见 docs/GOVERNANCE.md
"""

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
    get_skeleton,
    list_skeletons,
)

# 引擎自动导入触发 register_skeleton
from oservi.engines import alerter as _alerter  # noqa: F401
from oservi.engines import researcher as _researcher  # noqa: F401
from oservi.engines import feed_tracker as _feed_tracker  # noqa: F401
from oservi.engines import sequential_composer as _sequential_composer  # noqa: F401
from oservi.engines import subagent_orchestrator as _subagent_orchestrator  # noqa: F401
from oservi.engines import mcp_bridge as _mcp_bridge  # noqa: F401
from oservi.engines import saga_composer as _saga_composer  # noqa: F401
from oservi.engines import state_machine_engine as _state_machine_engine  # noqa: F401
from oservi.engines import event_webhook_dispatcher as _event_webhook_dispatcher  # noqa: F401
from oservi.engines import bulk_import_worker as _bulk_import_worker  # noqa: F401
from oservi.engines import bulk_export_worker as _bulk_export_worker  # noqa: F401
from oservi.engines import production_execution as _production_execution  # noqa: F401

# croniter is an optional runtime dependency for the cron engine.  Do not make
# every on-demand engine unusable in a minimal installation just by importing
# the package root.
try:  # pragma: no cover - depends on optional installation extras
    from oservi.engines import cron_scheduler_engine as _cron_scheduler_engine  # noqa: F401
except ModuleNotFoundError as exc:
    if exc.name != "croniter":
        raise

from oservi.engines.alerter import AlerterEngine
from oservi.engines.researcher import ResearcherEngine
from oservi.engines.feed_tracker import FeedTrackerEngine
from oservi.engines.sequential_composer import SequentialComposerEngine
from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine
from oservi.engines.mcp_bridge import McpBridgeEngine
from oservi.engines.saga_composer import SagaComposerEngine
from oservi.engines.state_machine_engine import StateMachineEngine
from oservi.engines.event_webhook_dispatcher import EventWebhookDispatcherEngine
from oservi.engines.bulk_import_worker import BulkImportWorkerEngine
from oservi.engines.bulk_export_worker import BulkExportWorkerEngine
from oservi.engines.production_execution import ProductionExecutionEngine

try:  # pragma: no cover - depends on optional installation extras
    from oservi.engines.cron_scheduler_engine import CronSchedulerEngine
except ModuleNotFoundError as exc:
    if exc.name != "croniter":
        raise
    CronSchedulerEngine = None  # type: ignore[assignment,misc]

__all__ = [
    "EngineSkeleton",
    "Injection",
    "register_skeleton",
    "get_skeleton",
    "list_skeletons",
    "AlerterEngine",
    "ResearcherEngine",
    "FeedTrackerEngine",
    "SequentialComposerEngine",
    "SubagentOrchestratorEngine",
    "McpBridgeEngine",
    "SagaComposerEngine",
    "StateMachineEngine",
    "EventWebhookDispatcherEngine",
    "BulkImportWorkerEngine",
    "BulkExportWorkerEngine",
    "ProductionExecutionEngine",
]

if CronSchedulerEngine is not None:
    __all__.append("CronSchedulerEngine")
