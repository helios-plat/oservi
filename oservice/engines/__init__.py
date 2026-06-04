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

from oservice.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
    get_skeleton,
    list_skeletons,
)

# 引擎自动导入触发 register_skeleton
from oservice.engines import alerter as _alerter  # noqa: F401
from oservice.engines import researcher as _researcher  # noqa: F401
from oservice.engines import feed_tracker as _feed_tracker  # noqa: F401

from oservice.engines.alerter import AlerterEngine
from oservice.engines.researcher import ResearcherEngine
from oservice.engines.feed_tracker import FeedTrackerEngine

__all__ = [
    "EngineSkeleton",
    "Injection",
    "register_skeleton",
    "get_skeleton",
    "list_skeletons",
    "AlerterEngine",
    "ResearcherEngine",
    "FeedTrackerEngine",
]
