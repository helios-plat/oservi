#!/usr/bin/env python3
"""release 前置检查：验证 __init__.py 引用的所有模块文件真实存在。"""
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
                rel = mod.replace(".", "/") + ".py"
                full = pkg_dir.parent / rel
                if not full.exists():
                    missing.append(f"❌ {mod} → {full}")
    if missing:
        print(f"[BLOCK] {init_path} 引用了不存在的模块:")
        for m in missing:
            print(f"  {m}")
        return 1
    print(f"✅ {init_path} 所有引用均存在")
    return 0

if __name__ == "__main__":
    sys.exit(check(sys.argv[1]) if len(sys.argv) > 1 else 1)
