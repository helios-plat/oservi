# Changelog

All notable changes to oservi will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-06-13

### Changed (BREAKING)
- 包改名 `oservice` → `oservi`; 所有 `from oservice import` 须改为 `from oservi import`
- GitHub repo 改名: `helios-plat/oservice` → `helios-plat/oservi`
- Python 包目录 `oservice/` → `oservi/`

## [0.4.2] - 2026-06-06

### Fixed
- `TriageEngine.injection_points["llm_provider"]` kind 错标 `"obase"`, 应为 `"oprim"` (LLM
  调用本质是 oprim 基础操作, 参照 v0.3.0 ResearcherEngine.llm_caller 修正先例)
- 注入点名从 `llm_provider` 改为 `llm_caller`, 与 ResearcherEngine 命名一致

### Notes
- Sweep 确认: AgenticLoopEngine / ActionPlannerEngine / AppInstallerEngine injection_points 均完整
- 对外破坏性变化: 任何直接用 `inject={"llm_provider": ...}` 装配 TriageEngine 的调用须改为
  `inject={"llm_caller": ...}`, 注入 callable 须来自 oprim 层

## [0.4.1] - 2026-06-05

### Added
- `TriggerMode` 类型别名加 `"on_signal"`: 事件驱动触发语义 (区别于 `on_demand` 单次调用)
- `TriageEngine.run()` 实现 `on_signal`/`on_demand` 非阻塞模式 (`_ready=True`, 立即返回)
- `TriageEngine.process(signal)`: 事件驱动入口, caller 收信号后主动调用

### Fixed
- v0.4.0 TriageEngine `__init__` 只接受 `on_interval`/`on_cron`, 但 Owner SPEC 写
  `trigger={"on_signal": True}`, 实际装配会 `ValueError`. 本 PATCH 补全 `on_signal`/`on_demand`.

### Notes
- `on_demand` vs `on_signal` 语义区分:
  - `on_demand`: 单次调用 (`ResearcherEngine.research(query=...)`)
  - `on_signal`: 事件流 (`TriageEngine.process(signal)` 被 dispatcher 反复调)
- Aegis Brain signal dispatcher 调 `triage.process(signal)` 模式可正常运行

## [0.4.0] - 2026-06-05

### Added (Aegis 3O Element IMPL SPEC v1.0 — B1)
- `TriageEngine` (`skeleton="triage"`): LLM 分诊引擎 — 事件拉取 → filters 链过滤 → LLM 优先级评分 → 去重 → min_score 筛选 → on_triage_result 回调. 注入: llm_provider=obase(1) + filters=layer4(0..n). 双实证: Aegis C2 TriageEngine + Tide v5 SignalClassifier.
- `AgenticLoopEngine` (`skeleton="agentic_loop"`): ReAct 自主代理循环引擎 — thought/action/observation 循环, 工具路由, max_steps 上限, 可选 RAG 上下文. 注入: llm_provider=obase(1) + tools=oprim(1..n) + knowledge_retrieval=oskill(0..1). 双实证: Aegis C2 AgenticLoop + Helios-Refactorer AutoAgent.
- `ActionPlannerEngine` (`skeleton="action_planner"`): 行动计划引擎 — RAG 检索 → LLM 计划生成 → plugin_registry 路由执行 → 步骤重试 → on_plan_done 回调. 注入: llm_provider=obase(1) + plugin_registry=layer4(1) + rag=oskill(0..1). 双实证: Aegis C2 ActionPlanner + Tide v5 RemediationPlanner.
- `AppInstallerEngine` (`skeleton="app_installer"`): App 安装流水线引擎 — catalog_fetch → compose_pull → compose_up → caddy_route_add → verify_health. 注入: catalog_fetch=oprim(1) + compose_up=oprim(1) + compose_pull=oprim(1) + caddy_route_add=oskill(1) + verify_health=oskill(1). 双实证: Aegis AppStation + Tide DevShop.
- 55 新增测试 (4 × ≥10 tests), 全部绿.

## [0.3.0] - 2026-06-04

### Changed (BREAKING)
- `ResearcherEngine.llm_caller` 注入点 kind 从 `"obase"` 改为 `"oprim"` (v1.0 §3.2 形态 3: 单 LLM = oprim)
- `FeedTrackerEngine.subscription_query_omodul` 改名 `subscription_query`, kind 从 `"omodul"` 改 `"layer4"`
- `FeedTrackerEngine.subscription_update_omodul` 改名 `subscription_update`, kind 同上

### Added
- `InjectionKind` 新增 `"layer4"` 选项: 项目服务层 callable (thin wrapper / 业务特定 CRUD / 非主库元素)
  - 装配器对 layer4 注入跳过 __module__ 来源校验
  - 适用: 引擎需要项目特定数据访问 (e.g. Stratum 订阅 DuckDB CRUD), 但不入主库

### Fixed
- ResearcherEngine v0.2.0 设计错误: 把 LLM caller kind 设成 obase (实际 LLM 调用本质是 oprim)
- FeedTrackerEngine v0.2.0 设计错误: 订阅 CRUD 标 omodul kind (实际是项目服务层 thin wrapper)

### Migration (Stratum / 其他 layer 4 项目)
ResearcherEngine 装配:
  inject={
      "llm_caller": [oprim.llm_call],  # 不再用 ProviderRegistry.get
      ...
  }

FeedTrackerEngine 装配:
  inject={
      "subscription_query": [stratum_thin_wrapper_query],  # layer4, 来自服务层
      "subscription_update": [stratum_thin_wrapper_update],
      ...
  }

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

## [1.1.1] — 2026-06-18
### Added
- channel_watcher: 频道订阅引擎（复用 feed_tracker 模式）
  注入点: list_videos(oprim/1) + filter_videos(oskill/0..1) +
          ingest_media(omodul/1) + subscription(layer4/1)
  流程: list → diff → filter → ingest → mark_processed（增量）
  trigger: on_interval（默认 3600s）

## [1.1.2] — 2026-06-19
### Added
- arxiv_watcher: arXiv 论文订阅引擎（复用 channel_watcher 模式）
  注入点: search(oprim/1) + download(oprim/1) + ingest(omodul/1) +
          subscription(layer4/1) + filter(oskill/0..1)
  流程: search → diff → filter → download → ingest → mark_processed（增量）
  默认 6 小时扫一次，入库后清理本地 PDF

## [1.2.0] — 2026-06-19
### Added
- source_watcher: 通用源订阅引擎（arxiv/gutenberg/oapen 统一框架）
  注入点: searchers(oprim/1+) + download(oprim/1) + ingest(omodul/1) +
          subscription(layer4/1) + filter(oskill/0..1)
  config.source_type 决定调哪个 search_fn
  config.force_ipv4 支持 OAPEN 等 IPv6 超时场景

## [1.4.0] — 2026-07-30

### Added (SPEC §5 — Medusa 3O commerce vertical orchestration engines)

`3O_MEDUSA_IMPLEMENTATION_SPEC.md`(平台根目录)§5 要求的 6 个引擎全部到位。
**治理提醒**:这批引擎未走"≥2 真实项目实证"准入红线(见 docs/GOVERNANCE.md
红线 1)——soffy 明确选择直接建骨架、事后补实证,不是治理流程被绕过而不
自知,写在这里供 Wiki 后续把关时查阅。

- `saga_composer` — 正向执行 steps,任一步返回 `{"status": "failed"}` 时逆序
  执行 compensations(典型:支付扣款 + 库存扣减失败时的退款+释放库存)。
- `state_machine_engine` — 状态图放在 `config["transition_map"]`(不是注入点
  本身,通用装配器的 Injection 只理解 `list[Callable]`,没法校验
  `dict[str, Callable]` 形状),`transitions`/`validators` 保持纯 callable 列表
  注入契约。
- `event_webhook_dispatcher` — `obase.mq.EventBus` 的第一个真实消费方:订阅
  一个 topic,收到事件后 `asyncio.gather` 并发拉起所有 subscribers,单个失败
  不拖累其余。
- `cron_scheduler_engine` — 引入 `croniter` 新依赖做真实 cron 表达式解析
  (原有 `alerter` 的 on_cron 只是 on_interval 的别名,没有真正解析;这次为
  "cron_scheduler_engine"这个名字对应的语义做对)。
- `bulk_import_worker` / `bulk_export_worker` — 流式取行(fetcher 返回同步或
  异步可迭代对象,骨架逐行消费不囤积内存),export 侧格式化结果先写本地
  临时文件再一次性上传(`uploader` 签名对齐 `obase.fs.FileStorage.upload`),
  上传后清理临时文件。

### Notes
- 本地跑测试前额外装了 `pydantic-settings`(obase.config 需要,但 obase 自己
  的 pyproject.toml 未声明这个依赖——本仓库未改 obase,仅记录此坑)。
