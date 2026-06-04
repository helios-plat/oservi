# Changelog

All notable changes to oservice will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-04

### Added
- `researcher` engine skeleton (Stratum ResearcherAgent 实证)
  - 注入点: search_oprim (oprim 1..n) / fetch_oprim (oprim 1) / llm_caller (obase 1) / ingest_omodul (omodul 0..1)
  - 机制: LLM rewrite query → 多搜索词并发 search → URL canonical 去重 → 并发 fetch → (可选) 落库
  - 业务靠注入, 骨架不含具体 search/LLM 实现
- `feed_tracker` engine skeleton (Stratum FeedTrackerAgent 实证)
  - 注入点: fetch_feed_oprim (oprim 1..n) / diff_oprim (oprim 1) / subscription_query_omodul + subscription_update_omodul + ingest_omodul (omodul 1)
  - 机制: cron/interval 调度 → 查待检查订阅 → 并发抓取 (HTTP 304 conditional) → diff → 落库 → 更新状态
  - 异常隔离: 单订阅失败不阻塞 tick 整体

### Tests
- 18 个新增测试 (research workflow 端到端 / dedupe / LLM 失败降级 / feed tick / 304 / 空订阅 / 异常隔离)
- 总 53 测试全绿 (17 assembler + 18 alerter + 18 researcher/feed_tracker)

### Notes
- 治理判定: 反复出现的引擎形态直接入 oservice, 不等"第 2 实证才入". 资产池性质优先, 新项目自然受益.

## [0.1.0] - 2026-06-03

### Added
- Initial oservice package as 5th helios-plat pillar
- `oservice.manifest.ServiceManifest` dataclass for declarative service definition
- `oservice.assembler.assemble()` 朴素版 assembler (无 reconcile, 无 deploy 段)
- `oservice.engines._base.EngineSkeleton` 基类 + `Injection` 注入点声明
- Governance baseline at `docs/GOVERNANCE.md`
- 5 准入红线 (≥2 实证 / 机制业务分离 / 注入契约 / 无状态骨架 / 不反向依赖)

### Engines (Phase 1)
- `alerter` engine skeleton (双实证收敛: Aegis C2 AlertEngine + Tide v4 AlertSchedulerEngine)
  - 注入点: evaluators (oprim, 1..n) + channels (obase, 1..n)
  - 机制: interval 调度 / 节流 / 去重 / 静音时段 / 多通道推送
  - 业务靠注入, 骨架代码零业务逻辑 (红线 2)
  - 18 个测试覆盖注册 / 装配 / evaluator 调用 / 节流 / 去重 / 静音 / dispatch / 异常隔离 / 健康检查 / 多实例独立状态

### Notes
- Python 3.14
- Dependencies: obase v0.9.0 / oprim v2.24.1 / oskill v3.8.0 / omodul v1.15.0
- B path: git+ssh tag references
