"""3O 范式内化 — 经验轨迹导出器 (oservi).

将演化轨迹导出为 HuggingFace 数据集格式 (JSONL, 零 HF 依赖)。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def build_sft_records(trajectories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """轨迹 → SFT 三元组 (instruction/input/output)。

    以 Evo_Debug 型轨迹为例: 报错为 instruction, 原码为 input, 修复码为 output。
    """
    records: list[dict[str, Any]] = []
    for t in trajectories:
        if t.get("operator_used") == "Evo_Debug" and t.get("output_code"):
            records.append(
                {
                    "instruction": f"Fix the bug: {t.get('error_trace', '')[:500]}",
                    "input": t.get("input_code", ""),
                    "output": t.get("output_code", ""),
                }
            )
    return records


def export_trajectories(
    trajectories: list[dict[str, Any]],
    *,
    output_path: str | Path,
    format: str = "jsonl",
) -> Path:
    """导出轨迹到数据集文件 (jsonl)。

    Args:
        trajectories: 轨迹条目列表。
        output_path: 目标文件路径。
        format: 仅支持 jsonl。

    Returns:
        落盘路径。
    """
    if format != "jsonl":
        raise ValueError(f"Unsupported export format: {format}")
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for t in trajectories:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    return path
