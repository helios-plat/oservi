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

from oservi.engines.alerter import AlerterEngine
from oservi.engines.researcher import ResearcherEngine
from oservi.engines.feed_tracker import FeedTrackerEngine
from oservi.engines.sequential_composer import SequentialComposerEngine
from oservi.engines.subagent_orchestrator import SubagentOrchestratorEngine
from oservi.engines.mcp_bridge import McpBridgeEngine

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
]
