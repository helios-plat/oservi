"""Darwin Evolution Engine Skeleton — 达尔文算子自进化闭环.

收敛自双实证:
- Veya Coprocessor 网格搜索: 参数空间并发回测 → 夏普择优
- Aegis 运维监控 + Automata 后台调度: 实盘表现衰减检测 → 自动处置

共性 (固化为机制):
- 影子测试 (Shadow Mode): 变体算子不接管真实资金, 后台静默记录滑点/预测准确率
- 基因突变 (Mutation): 衰减算子 → 生成 n 个变种 (注入 variant_fn; 缺省确定性 AST 突变)
- 优胜劣汰 (Selection): 变种并发回测 → 夏普最高者成为候选, 生成 PRD 升级申请
- 人工审批 (Promotion): PRD 批准后自动替换 ACTIVE 算子, 保留谱系 (lineage)

差异 (留 inject + config):
- 回测执行 → 注入 layer4 backtest_fn (Veya 注入 QuantCoprocessor 沙箱)
- 变体生成 → 注入 layer4 variant_fn (Veya 注入 Genesis LLM; None → 确定性 AST 突变)
- 审批通知 → 注入 obase notify_fn (Veya 注入 NotificationCenter)

红线对照:
- 红线 1 (≥2 实证): Coprocessor 网格 + Aegis 监控 ✅
- 红线 2 (机制/业务分离): 回测/突变业务全靠注入, 骨架不含行情/LLM 逻辑 ✅
- 红线 3 (注入契约): backtest_fn=layer4(1) / variant_fn=layer4(0..1) / notify_fn=obase(0..1) ✅
- 红线 4 (无状态骨架): 状态只在 DarwinEvolutionEngine 实例 + state_path 持久化 ✅
- 红线 5 (不反向依赖): 仅 import oservi.engines._base ✅
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from oservi.engines._base import EngineSkeleton, Injection, register_skeleton

logger = logging.getLogger(__name__)

# 确定性突变的数值缩放因子 (基因突变: 调整平滑系数)
_MUTATION_FACTORS = (0.8, 1.25, 1.1)
# 非线性惩罚强度 (变种 3: 加入非线性惩罚项)
_PENALTY_STRENGTH = 0.05
# 信号列命名模式 (非线性惩罚注入目标)
_SIGNAL_COL_RE = re.compile(r"(signal|score|weight|position|allocation|exposure|decision)", re.IGNORECASE)


class DarwinEvolutionEngine(EngineSkeleton):
    """达尔文算子自进化引擎骨架.

    机制 (骨架固化):
    - register_operator: 登记 ACTIVE 算子
    - record_shadow:     影子流量观测 (滑点/预测准确率) → 衰减检测
    - evolve:            突变 3 变种 → 并发回测 → 择优 → PRD 申请 (通知)
    - promote:           审批通过 → 替换 ACTIVE + 谱系记录
    - run/stop:          后台循环: 周期检查衰减算子并自动进化

    业务 (注入填料):
    - backtest_fn: async (code: str, params: dict, asset_id: str)
                   -> {"sharpe": float|None, "total_return": float|None, "error": str|None}
    - variant_fn:  async (code: str, n: int) -> list[str] (Genesis 语义级突变);
                   None → 引擎内确定性 AST 突变 (参数平滑系数扰动)
    - notify_fn:   (payload: dict) -> None (PRD 升级申请推送; 可同步)

    Example:
        engine = DarwinEvolutionEngine(
            name="veya-darwin",
            backtest_fn=coprocessor_backtest,
            variant_fn=genesis_mutate,
            notify_fn=notify_prd,
            trigger={"on_interval": 3600},
            config={
                "state_path": "~/.veya/darwin/state.json",
                "shadow_min_samples": 5,
                "decay_accuracy_below": 0.55,
                "decay_slippage_above": 0.02,
                "mutation_count": 3,
                "backtest_asset_id": "BTCUSDT",
                "backtest_params": {},
            },
        )
    """

    injection_points = {
        "backtest_fn": Injection(
            kind="layer4",
            cardinality="1",
            description="回测注入: async (code, params, asset_id) -> {sharpe, total_return, error}",
        ),
        "variant_fn": Injection(
            kind="layer4",
            cardinality="0..1",
            description="变体生成 (Genesis): async (code, n) -> list[str]; None → 确定性 AST 突变",
        ),
        "notify_fn": Injection(
            kind="obase",
            cardinality="0..1",
            description="PRD 升级申请通知: (payload) -> None",
        ),
    }

    def __init__(
        self,
        *,
        backtest_fn: Callable[..., Any],
        variant_fn: Callable[..., Any] | None,
        notify_fn: Callable[..., Any] | None,
        trigger: dict[str, Any],
        config: dict[str, Any],
        name: str,
    ) -> None:
        self.name = name
        self.backtest_fn = backtest_fn
        self.variant_fn = variant_fn
        self.notify_fn = notify_fn
        self.trigger = trigger
        self.config = config

        # 运行期状态 (红线 4: 仅实例存在)
        self._running = False
        self._iteration_count = 0
        self._last_error: str | None = None
        self._operators: dict[str, dict[str, Any]] = {}

        # 持久化 (机制: JSON 原子写)
        state_path = config.get("state_path")
        self.state_path = Path(state_path).expanduser() if state_path else None
        if self.state_path:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self._load_state()

        if "on_interval" not in trigger and "on_cron" not in trigger:
            raise ValueError("DarwinEvolutionEngine trigger must contain 'on_interval' or 'on_cron'")

    # ===== 注册表操作 (机制) ==============================================

    def register_operator(self, code: str, name: str | None = None) -> str:
        """登记一个 ACTIVE 算子 (来自 Genesis 锻造 / 人工提交)."""
        op_id = uuid.uuid4().hex[:12]
        self._operators[op_id] = {
            "id": op_id,
            "name": name or f"operator_{op_id[:6]}",
            "code": code,
            "status": "ACTIVE",
            "lineage": [],                      # 谱系: 被替换掉的祖先代码
            "fitness_history": [],              # [{ts, sharpe, source}]
            "shadow": {"observations": [], "accuracy_avg": None, "slippage_avg": None},
            "candidate": None,                  # {code, sharpe, total_return, variants, prd_path, created_at}
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        self._save_state()
        logger.info("DarwinEngine '%s': registered %s (%s)", self.name, op_id, name or "unnamed")
        return op_id

    def list_operators(self) -> list[dict[str, Any]]:
        """列出算子种群 (含状态/衰减指标/候选)."""
        out = []
        for op in self._operators.values():
            info = {k: v for k, v in op.items() if k != "code"}
            info["code_len"] = len(op["code"])
            info["shadow"] = {
                "samples": len(op["shadow"]["observations"]),
                "accuracy_avg": op["shadow"]["accuracy_avg"],
                "slippage_avg": op["shadow"]["slippage_avg"],
                "decayed": self._is_decayed(op),
            }
            out.append(info)
        return sorted(out, key=lambda o: o["created_at"])

    def get_operator(self, op_id: str) -> dict[str, Any] | None:
        op = self._operators.get(op_id)
        if op is None:
            return None
        info = {k: v for k, v in op.items() if k != "code"}
        info["code_len"] = len(op["code"])
        info["decayed"] = self._is_decayed(op)
        return info

    def get_prd(self, op_id: str) -> str | None:
        """PRD 升级申请文档 (markdown)."""
        op = self._operators.get(op_id)
        if not op or not op.get("candidate"):
            return None
        path = Path(op["candidate"]["prd_path"])
        return path.read_text(encoding="utf-8") if path.exists() else None

    # ===== 影子测试 (机制) =================================================

    def record_shadow(
        self,
        op_id: str,
        *,
        slippage: float,
        accuracy: float,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """记录一条影子流量观测 (不接管真实资金).

        影子观测达标后若衰减 (准确率 < decay_accuracy_below 或
        滑点 > decay_slippage_above) → 返回 decayed=True, 供调用方触发进化.
        """
        op = self._operators.get(op_id)
        if op is None:
            raise KeyError(f"operator {op_id} not found")
        obs = {"ts": time.time(), "slippage": slippage, "accuracy": accuracy, **(extra or {})}
        op["shadow"]["observations"].append(obs)
        accs = [o["accuracy"] for o in op["shadow"]["observations"]]
        slips = [o["slippage"] for o in op["shadow"]["observations"]]
        op["shadow"]["accuracy_avg"] = sum(accs) / len(accs)
        op["shadow"]["slippage_avg"] = sum(slips) / len(slips)
        op["updated_at"] = time.time()
        self._save_state()
        decayed = self._is_decayed(op)
        return {"operator_id": op_id, "samples": len(op["shadow"]["observations"]),
                "accuracy_avg": op["shadow"]["accuracy_avg"], "slippage_avg": op["shadow"]["slippage_avg"],
                "decayed": decayed}

    def _is_decayed(self, op: dict[str, Any]) -> bool:
        sh = op["shadow"]
        if len(sh["observations"]) < int(self.config.get("shadow_min_samples", 5)):
            return False
        acc_below = sh["accuracy_avg"] is not None and sh["accuracy_avg"] < float(self.config.get("decay_accuracy_below", 0.55))
        slip_above = sh["slippage_avg"] is not None and sh["slippage_avg"] > float(self.config.get("decay_slippage_above", 0.02))
        return acc_below or slip_above

    # ===== 进化闭环 (机制) =================================================

    async def evolve(self, op_id: str, *, force: bool = False) -> dict[str, Any]:
        """执行一轮达尔文进化: 突变 → 并发回测 → 择优 → PRD 申请.

        Returns:
            dict: {status, operator_id, variants, results, winner, candidate, prd_path, ...}
        """
        op = self._operators.get(op_id)
        if op is None:
            raise KeyError(f"operator {op_id} not found")
        if op["status"] == "CANDIDATE" and not force:
            return {"status": "already_candidate", "operator_id": op_id,
                    "candidate": op["candidate"]}

        n = int(self.config.get("mutation_count", 3))
        asset_id = str(self.config.get("backtest_asset_id", "default"))
        params: dict[str, Any] = self.config.get("backtest_params", {})

        # 1. 基因突变 (注入 Genesis; 缺省确定性 AST 突变)
        variants: list[str] = []
        if self.variant_fn is not None:
            try:
                variants = list(await self.variant_fn(op["code"], n)) or []
            except Exception as exc:  # pragma: no cover - LLM 层失败不阻断进化
                logger.warning("DarwinEngine variant_fn failed: %s; falling back to AST mutation", exc)
                variants = []
        if not variants:
            variants = self._mutate(op["code"], n)
        variants = variants[:n]
        logger.info("DarwinEngine '%s': %d variants generated for %s", self.name, len(variants), op_id)

        # 2. 优胜劣汰: 并发回测
        results = await asyncio.gather(
            *(self._safe_backtest(v, idx, asset_id, params) for idx, v in enumerate(variants))
        )
        winners = [r for r in results if r.get("sharpe") is not None]
        if not winners:
            return {"status": "failed", "operator_id": op_id, "variants": len(variants),
                    "results": results, "reason": "all variants backtest failed"}

        best = max(winners, key=lambda r: r["sharpe"])
        old_sharpe = op["fitness_history"][-1]["sharpe"] if op["fitness_history"] else None

        # 3. PRD 升级申请
        prd_path = self._write_prd(op, variants, results, best, old_sharpe)
        op["candidate"] = {
            "code": best["code"],
            "sharpe": best["sharpe"],
            "total_return": best.get("total_return"),
            "variants": [{"index": i, "sharpe": r.get("sharpe"), "total_return": r.get("total_return"),
                          "error": r.get("error")} for i, r in enumerate(results)],
            "prd_path": str(prd_path),
            "created_at": time.time(),
        }
        op["status"] = "CANDIDATE"
        op["updated_at"] = time.time()
        self._save_state()

        if self.notify_fn is not None:
            try:
                self.notify_fn({
                    "type": "PRD_REVIEW_REQUIRED",
                    "operator_id": op_id,
                    "operator_name": op["name"],
                    "prd_path": str(prd_path),
                    "old_sharpe": old_sharpe,
                    "new_sharpe": best["sharpe"],
                    "approve_endpoint": f"/evolution/operators/{op_id}/promote",
                })
            except Exception as exc:  # pragma: no cover
                logger.warning("DarwinEngine notify_fn failed: %s", exc)

        return {
            "status": "candidate_ready",
            "operator_id": op_id,
            "variants": len(variants),
            "results": results,
            "winner": {"index": results.index(best), "sharpe": best["sharpe"],
                       "total_return": best.get("total_return")},
            "candidate": op["candidate"],
            "prd_path": str(prd_path),
        }

    async def _safe_backtest(self, code: str, index: int, asset_id: str, params: dict) -> dict[str, Any]:
        try:
            res = await self.backtest_fn(code, params, asset_id)
            return {"index": index, "code": code, "sharpe": res.get("sharpe"),
                    "total_return": res.get("total_return"), "error": res.get("error")}
        except Exception as exc:
            return {"index": index, "code": code, "sharpe": None, "total_return": None, "error": str(exc)}

    def promote(self, op_id: str) -> dict[str, Any]:
        """审批 PRD → 用候选算子替换 ACTIVE (谱系保留旧代码)."""
        op = self._operators.get(op_id)
        if op is None:
            raise KeyError(f"operator {op_id} not found")
        cand = op.get("candidate")
        if cand is None:
            raise ValueError(f"operator {op_id} has no pending candidate (run evolve first)")
        op["lineage"].append(op["code"])
        op["code"] = cand["code"]
        op["fitness_history"].append({
            "ts": time.time(), "sharpe": cand["sharpe"], "source": "evolution",
        })
        op["status"] = "ACTIVE"
        op["candidate"] = None
        op["updated_at"] = time.time()
        self._save_state()
        logger.info("DarwinEngine '%s': %s promoted (lineage depth %d)", self.name, op_id, len(op["lineage"]))
        return {"status": "promoted", "operator_id": op_id, "lineage_depth": len(op["lineage"]),
                "new_sharpe": cand["sharpe"]}

    # ===== 确定性基因突变 (机制: AST 参数扰动) =============================

    def _mutate(self, code: str, n: int = 3) -> list[str]:
        """无 Genesis 时的确定性突变: AST 数值字面量缩放 + 非线性惩罚项.

        - v1: 全部数值参数 ×0.8  (平滑系数调低)
        - v2: 全部数值参数 ×1.25 (平滑系数调高)
        - v3: ×1.1 + 对首个信号列注入非线性惩罚 (极端信号压缩, 抑制过拟合)
        """
        try:
            ast.parse(code)  # 预检语法
        except SyntaxError:
            return [code] * n  # 无法解析 → 原样变体 (回测自会判死刑)
        out: list[str] = []
        for i, factor in enumerate(_MUTATION_FACTORS[:n]):
            # 每个变体独立解析 (NodeTransformer 原地修改, 必须隔离)
            variant_tree = ast.parse(code)
            variant_tree = ast.fix_missing_locations(_scale_literals(variant_tree, factor))
            if i == 2:
                variant_tree = ast.fix_missing_locations(_add_nonlinear_penalty(variant_tree))
            variant_code = ast.unparse(variant_tree)
            out.append(variant_code if variant_code != code else code + f"\n# (darwin) variant {i + 1}: no tunable literals found\n")
        return out

    # ===== 主循环 ==========================================================

    def run(self) -> None:
        if self._running:
            raise RuntimeError(f"DarwinEvolutionEngine {self.name} already running")
        self._running = True
        logger.info("DarwinEvolutionEngine '%s' starting", self.name)
        try:
            asyncio.run(self._run_loop())
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    async def _run_loop(self) -> None:
        interval = self.trigger.get("on_interval", 3600)
        while self._running:
            try:
                await self._iterate_once()
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                logger.exception("DarwinEngine '%s' iteration failed", self.name)
            self._iteration_count += 1
            await asyncio.sleep(interval)

    async def _iterate_once(self) -> None:
        """后台巡检: 影子样本达标的衰减算子自动进化."""
        for op_id, op in list(self._operators.items()):
            if op["status"] != "ACTIVE":
                continue
            if self._is_decayed(op):
                logger.info("DarwinEngine '%s': decay detected on %s → evolving", self.name, op_id)
                await self.evolve(op_id)

    def health(self) -> dict[str, Any]:
        return {
            "status": "healthy" if not self._last_error else "unhealthy",
            "details": {
                "operators": len(self._operators),
                "candidates": sum(1 for o in self._operators.values() if o["status"] == "CANDIDATE"),
                "iterations": self._iteration_count,
                "last_error": self._last_error,
            },
        }

    # ===== 持久化 (机制) ===================================================

    def _load_state(self) -> None:
        if not self.state_path or not self.state_path.exists():
            return
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            self._operators = data.get("operators", {})
        except (json.JSONDecodeError, OSError):  # pragma: no cover
            logger.warning("DarwinEngine state load failed, starting empty")

    def _save_state(self) -> None:
        if not self.state_path:
            return
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"operators": self._operators}, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.state_path)

    # ===== PRD 模板 (机制) =================================================

    def _write_prd(
        self, op: dict[str, Any], variants: list[str], results: list[dict],
        best: dict[str, Any], old_sharpe: float | None,
    ) -> Path:
        state_dir = (self.state_path.parent if self.state_path else Path.cwd() / ".veya_darwin")
        state_dir.mkdir(parents=True, exist_ok=True)
        path = state_dir / f"prd_{op['id']}_{int(time.time())}.md"
        rows = "\n".join(
            f"| V{i + 1} | {r.get('sharpe') if r.get('sharpe') is not None else 'FAIL'} | "
            f"{r.get('total_return') if r.get('total_return') is not None else '-'} | "
            f"{r.get('error') or '-'} |"
            for i, r in enumerate(results)
        )
        md = f"""# 达尔文升级申请 (PRD) — {op['name']} ({op['id']})

## 背景
- 影子测试样本: {len(op['shadow']['observations'])}
- 平均预测准确率: {op['shadow']['accuracy_avg']}
- 平均滑点: {op['shadow']['slippage_avg']}
- 衰减判定: accuracy < {self.config.get('decay_accuracy_below')} 或 slippage > {self.config.get('decay_slippage_above')}
- 旧算子夏普: {old_sharpe}

## 变体回测 (并发, 隔离沙箱)
| 变体 | 夏普 | 总收益 | 错误 |
|------|------|--------|------|
{rows}

## 推荐
- 胜出变体: V{results.index(best) + 1}, 夏普 **{best['sharpe']}**

## 升级与回滚
- 批准: `POST /evolution/operators/{op['id']}/promote` → ACTIVE 原子替换, 旧代码进入 lineage (可回滚)
- 拒绝: 候选保留, ACTIVE 算子继续影子观测

> 生成: DarwinEvolutionEngine (oservi) • 未经审批不触碰实盘
"""
        path.write_text(md, encoding="utf-8")
        return path


# ---------------------------------------------------------------------------
# AST 突变工具 (模块级纯函数, 便于单测)
# ---------------------------------------------------------------------------

def _scale_literals(tree: ast.AST, factor: float) -> ast.AST:
    """将所有数值字面量 × factor (跳过 0/1/布尔/字符串)."""

    class _Scaler(ast.NodeTransformer):
        def visit_Constant(self, node: ast.Constant) -> ast.AST:
            if isinstance(node.value, bool):
                return node
            if isinstance(node.value, (int, float)) and node.value not in (0, 1):
                new = ast.Constant(value=node.value * factor)
                return ast.copy_location(new, node)
            return node

    return _Scaler().visit(tree)


def _add_nonlinear_penalty(tree: ast.AST) -> ast.AST:
    """对首个信号列赋值注入非线性惩罚: E → E * (1 - strength * abs(E)).

    模拟 Genesis 的"加入非线性惩罚项"突变: 极端信号被压缩, 抑制过拟合.
    找不到信号列 → 原样返回.
    """

    class _Penalizer(ast.NodeTransformer):
        def __init__(self) -> None:
            self._done = False

        def visit_Assign(self, node: ast.Assign) -> ast.AST:
            if self._done:
                return node
            for t in node.targets:
                if isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant) \
                        and isinstance(t.slice.value, str) and _SIGNAL_COL_RE.search(t.slice.value):
                    strength = ast.Constant(value=_PENALTY_STRENGTH)
                    one = ast.Constant(value=1.0)
                    penalty = ast.BinOp(
                        left=one, op=ast.Sub(),
                        right=ast.BinOp(left=strength, op=ast.Mult(),
                                        right=ast.Call(func=ast.Name(id="abs", ctx=ast.Load()),
                                                       args=[node.value], keywords=[])),
                    )
                    node.value = ast.BinOp(left=node.value, op=ast.Mult(), right=penalty)
                    self._done = True
                    break
            return self.generic_visit(node)

    return _Penalizer().visit(tree)


register_skeleton("darwin_evolution", DarwinEvolutionEngine)

__all__ = ["DarwinEvolutionEngine"]
