# oservice

**3O 服务装配层 — 第 5 个 helios-plat 主库包**

## 定位

```
3O 业务构成单位:
  oprim    原子操作
  oskill   算法/技能
  omodul   端到端业务事务

横切配套:
  obase    无状态工具 (外部世界适配 + 横切工具)

服务装配 (本包):
  oservice 有状态引擎 (引擎骨架 + 装配器 + 模板)
```

oservice 与 oprim/oskill/omodul/obase 平级, 独立 GitHub repo + SemVer.

## 边界 (关键取舍)

- **obase = 无状态工具** (hash / auth / provider / cost / notify / audit)
- **oservice = 有状态机制** (调度循环 / 生命周期 / 心跳 / 重试 / IO 协调)

把"持续运行的引擎"塞进"无状态工具库"会污染 obase 定位 → oservice 独立成包.

## 治理

完整治理基线: `docs/GOVERNANCE.md`

5 红线 (引擎骨架准入):
1. **≥2 真实项目实证** (首要红线, 防凭空臆造)
2. **机制/业务分离** (业务靠注入, 骨架代码零业务逻辑)
3. **注入点类型契约** (kind + cardinality, 装配校验)
4. **无状态骨架定义** (状态只在 Service 实例运行期)
5. **不反向依赖** (3O 四包禁 import oservice, CI block)

## 用法

```python
from oservice import assemble, ServiceManifest
from oprim import detect_sector_collapse, detect_dragon_switch
from obase.notify import telegram_send

manifest = ServiceManifest(
    name="tide-realtime-alerter",
    skeleton="alerter",
    inject={
        "evaluators": [detect_sector_collapse, detect_dragon_switch],
        "channels": [telegram_send],
    },
    trigger={"on_interval": 300},
    config={"thresholds": {...}},
)

service = assemble(manifest)
service.run()
```

## Phase 1 引擎

| 引擎 | 实证来源 | 状态 |
|------|---------|------|
| `alerter` | Aegis AlertEngine + Tide AlertSchedulerEngine | Phase 1 (≥2 实证 ✅) |
| `webhook_dispatcher` | Aegis C2 WebhookDispatcher (待第 2 实证) | 推后 |
| 其他 (collector / scheduler / api_server / agentic_loop / orchestrator) | 待 ≥2 实证 | 推后 |

## Status

v0.1.0 (Initial). Phase 1 alerter 引擎实施中.

依赖:
- obase v0.9.0
- oprim v2.24.1
- oskill v3.8.0
- omodul v1.15.0
- Python 3.14
