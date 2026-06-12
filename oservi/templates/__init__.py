"""templates/ — 拷贝用脚手架 (非 import 库).

准入线 (docs/GOVERNANCE.md §8):
  templates/ 只放 "拷贝用的脚手架代码", 不是 import 的库.

判据:
  - 该被 import 复用 → 归 obase 或 oservice.engines
  - 该被拷贝改写 → 进 templates/

不放:
  - JWT 鉴权 (归 obase.auth, import 复用)
  - 通用业务模板 (归 oservice.engines)

放:
  - FastAPI 项目起手式
  - Next.js 前端骨架
  - Docker 标准基镜像参考
  - 项目初始化脚本
"""
