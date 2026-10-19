"""Graph Skills Agentic Loop Engine Skeleton.

收敛自双实证:
- graphify: AST-parsed code graph for structured understanding and impact analysis
- skills/sol-advisor: Domain-specific rules injection for guided coding

共性 (固化为机制):
- Skills 规则动态注入：根据文件上下文挂载适用规则
- AST 图谱理解：解析代码结构为节点-边图
- 规则引导循环：Rules → Prompt → LLM → 验证 → 迭代

差异 (留 inject + config):
- llm_caller: LLM 推理引擎 → 注入 oprim
- skills_registry: Skills 注册表 → 注入 obase
- graph_parse_op: AST 图解析器 → 注入 oprim

红线对照:
- 红线 1 (≥2 实证): graphify + skills/sol-advisor ✅
- 红线 2 (机制/业务分离): LLM/注册表/解析器全靠注入
- 红线 3 (注入契约): llm_caller=oprim(1) / skills_registry=obase(1) / graph_parse_op=oprim(0..1)
- 红线 4 (无状态骨架): 状态只在 GraphSkillsAgenticLoopEngine 实例
- 红线 5 (不反向依赖): 本文件不 import oprim/oskill/omodul
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, ClassVar

from oservi.engines._base import (
    EngineSkeleton,
    Injection,
    register_skeleton,
)

logger = logging.getLogger(__name__)


class GraphSkillsAgenticLoopEngine(EngineSkeleton):
    """集成图谱理解、Skills 注入与数据管线的 Agent 调度引擎.

    机制 (骨架固化):
    - Skills 动态注入：从 SkillsRegistry 匹配规则并合成 System Prompt
    - AST 图谱解析：解析代码文件生成节点-边结构
    - 规则引导循环：每次用户任务带上匹配的规则前缀
    - 支持 on_demand 单次调用模式

    业务 (注入填料):
    - llm_caller: LLM 推理，接收 (prompt) → 返回响应
    - skills_registry: Skills/Rules 注册表
    - graph_parse_op: AST 图解析器（可选）

    Example::

        manifest = ServiceManifest(
            name="graph-skills-agent",
            skeleton="graph_skills_agentic_loop",
            inject={
                "llm_caller": [my_llm_caller],
                "skills_registry": [my_registry],
                "graph_parse_op": [my_parser],
            },
            trigger={"on_demand": True},
            config={},
        )
    """

    injection_points: ClassVar[dict] = {
        "llm_caller": Injection(
            kind="oprim",
            cardinality="1",
            description="LLM call primitive for reasoning",
        ),
        "skills_registry": Injection(
            kind="obase",
            cardinality="1",
            description="Skills/Rules registry for context matching",
        ),
        "graph_parse_op": Injection(
            kind="oprim",
            cardinality="0..1",
            description="Optional AST graph parser for code understanding",
        ),
    }
    trigger_mode: str = "on_demand"

    def __init__(
        self,
        *,
        llm_caller: list[Callable[..., Any]],
        skills_registry: list[Any],
        graph_parse_op: list[Callable[..., Any]] | None = None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
        on_task_done: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.name = name
        self.llm_caller = llm_caller[0]  # cardinality=1
        self.skills_registry = skills_registry[0]  # cardinality=1
        self.graph_parse_op = graph_parse_op[0] if graph_parse_op else None
        self.trigger = trigger
        self.config = config
        self.on_task_done = on_task_done

        # 运行期状态 (红线 4)
        self._running = False
        self._task_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._completed_count = 0
        self._last_error: str | None = None

    # ===== 引擎生命周期 =====

    def run(self) -> None:
        """启动持续运行循环."""
        if self._running:
            raise RuntimeError(
                f"GraphSkillsAgenticLoopEngine '{self.name}' already running"
            )
        self._running = True
        logger.info(f"GraphSkillsAgenticLoopEngine '{self.name}' starting")
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False
            logger.info(f"GraphSkillsAgenticLoopEngine '{self.name}' stopped")

    def stop(self) -> None:
        """优雅停止引擎."""
        self._running = False

    def submit_task(self, task: dict[str, Any]) -> None:
        """提交任务到队列."""
        self._task_queue.put_nowait(task)

    # ===== 核心方法 =====

    async def run_guided_loop(
        self,
        task: str,
        context_file: str,
    ) -> dict[str, Any]:
        """运行受 Skills 规范指导与 AST 代码图拓扑约束的推演循环.

        Args:
            task: 用户任务描述
            context_file: 上下文文件名（用于规则匹配）

        Returns:
            {
                "status": "completed" | "failed",
                "injected_rules_count": int,
                "response": str,
                "graph_nodes": int (if graph_parse_op enabled),
            }
        """
        try:
            # 1. 动态获取 Rules 注入前缀
            rules: list[str] = []
            if self.skills_registry is not None and hasattr(
                self.skills_registry, "match_rules"
            ):
                try:
                    rules = self.skills_registry.match_rules(context_file)
                except Exception:
                    rules = []

            if rules:
                system_prefix = "[SYSTEM_RULES]: " + "; ".join(rules) + "\n\n"
            else:
                system_prefix = ""

            # 2. 可选：AST 图谱解析
            graph_nodes = 0
            if self.graph_parse_op is not None:
                try:
                    # 尝试读取文件内容（简化：仅记节点数）
                    graph_result = self.graph_parse_op(
                        task,
                        file_path=context_file,
                    )
                    if isinstance(graph_result, dict):
                        graph_nodes = len(graph_result.get("nodes", []))
                except Exception:
                    graph_nodes = 0

            # 3. LLM 推演
            full_prompt = f"{system_prefix}User Task: {task}\nFile: {context_file}"

            try:
                response = self.llm_caller(full_prompt, temperature=0.1)
                if asyncio.iscoroutine(response):
                    response = await response
                response_str = str(response) if not isinstance(response, str) else response
            except Exception as e:
                response_str = f"LLM call failed: {type(e).__name__}: {e}"

            self._completed_count += 1

            return {
                "status": "completed",
                "injected_rules_count": len(rules),
                "response": response_str,
                "graph_nodes": graph_nodes,
            }

        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            return {
                "status": "failed",
                "injected_rules_count": 0,
                "response": "",
                "graph_nodes": 0,
                "error": str(e),
            }

    # ===== 内部循环 =====

    async def _run_loop(self) -> None:
        """内部消费循环."""
        while self._running:
            try:
                task = await asyncio.wait_for(self._task_queue.get(), timeout=1.0)
                task_desc = task.get("task", "")
                context_file = task.get("context_file", "main.py")
                result = await self.run_guided_loop(task_desc, context_file)
                if self.on_task_done:
                    try:
                        self.on_task_done(result)
                    except Exception as e:
                        logger.warning(f"on_task_done callback failed: {e}")
            except TimeoutError:
                continue
            except Exception as e:
                self._last_error = f"{type(e).__name__}: {e}"
                logger.exception(
                    f"GraphSkillsAgenticLoopEngine '{self.name}' task failed"
                )

    # ===== 健康检查 =====

    def health(self) -> dict[str, Any]:
        """健康检查."""
        return {
            "status": "healthy" if self._running else "stopped",
            "details": {
                "name": self.name,
                "running": self._running,
                "queue_size": self._task_queue.qsize(),
                "completed_count": self._completed_count,
                "has_graph_parser": self.graph_parse_op is not None,
                "has_skills_registry": self.skills_registry is not None,
                "last_error": self._last_error,
            },
        }


# 注册
register_skeleton("graph_skills_agentic_loop", GraphSkillsAgenticLoopEngine)
