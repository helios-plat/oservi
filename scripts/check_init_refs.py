#!/usr/bin/env python3
"""release 前置检查：验证 __init__.py 引用的所有模块文件真实存在。
支持：单文件模块（xxx.py）和包模块（xxx/__init__.py）。
"""
import ast, pathlib, sys

def check(init_path: str) -> int:
    path = pathlib.Path(init_path)
    pkg_dir = path.parent
    content = path.read_text()
    tree = ast.parse(content)
    missing = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            pkg = pkg_dir.name
            if mod.startswith(f"{pkg}.") or mod.startswith(f"_{pkg}"):
                rel = mod.replace(".", "/")
                # 检查 .py 文件 或 目录/__init__.py
                as_file = pkg_dir.parent / (rel + ".py")
                as_pkg  = pkg_dir.parent / rel / "__init__.py"
                if not as_file.exists() and not as_pkg.exists():
                    missing.append(f"❌ {mod} → {as_file} 或 {as_pkg}")
    if missing:
        print(f"[BLOCK] {init_path} 引用了不存在的模块:")
        for m in missing:
            print(f"  {m}")
        return 1
    print(f"✅ {init_path} 所有引用均存在")
    return 0

if __name__ == "__main__":
    sys.exit(check(sys.argv[1]) if len(sys.argv) > 1 else 1)
