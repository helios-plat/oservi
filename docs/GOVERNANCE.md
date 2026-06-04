# oservice Governance Baseline v0.1

**性质**: oservice 立项治理基线 + 3O SPEC v1.1 §7.5 草稿来源
**日期**: 2026-06-03
**Status**: Active (引擎清单与注入契约待 5 项目真实引擎收敛确认)

---

## 0. 立项决定

**oservice 作为第 5 个独立包, 与 oprim/oskill/omodul/obase 平级.**

为什么独立成包而非 obase 子模块:
- obase = 无状态工具 (hash/auth/provider/cost)
- oservice = 有状态机制 (调度循环、生命周期、心跳)
- 把"持续运行的引擎"塞进"无状态工具库"会污染 obase 定位

---

## 1. 五包定位

```
3O 业务构成单位 (v1.0 不变):
  oprim    原子操作
  oskill   算法/技能
  omodul   端到端业务事务

横切配套 (v1.0 不变):
  obase    无状态工具

服务装配 (v1.1 新增):
  oservice 有状态机制
```

5 个独立 packages, 各自 GitHub repo + SemVer + release.

---

## 2. 准入 5 红线 (MUST)

### 红线 1 — ≥2 真实项目实证 (首要)

骨架入库必须能用 ≥2 个现有项目的真实引擎代码反向验证. 无 ≥2 实证 = 禁止入库.

这是 oservice 灵魂红线. oprim 合规客观可判 (是不是原子操作), 引擎骨架易凭空臆造.

### 红线 2 — 机制/业务分离

骨架只含机制 (调度/生命周期/IO 协调/心跳/重试), 业务靠注入 (3O 元素).

判据: "换个项目, 骨架代码一行不改吗?"

### 红线 3 — 注入点类型契约

每个注入点声明 kind (oprim/oskill/omodul/obase) + cardinality (1 / 0..1 / 1..n / 0..n).
装配时校验, 类型不符 / 基数不满足 → 拒绝装配.

### 红线 4 — 无状态骨架定义

骨架定义必须无状态. 状态只在被装配出的 Service 实例运行期.

### 红线 5 — 不反向依赖

依赖严格单向 (高 → 低):
```
oservice → omodul → oskill → oprim → obase
```

3O 四包任一反向 import oservice = CI block.

---

## 3. Manifest (Python dataclass)

不上 YAML / K8s DSL. 用 Python dataclass + IDE 类型校验.

```python
@dataclass
class ServiceManifest:
    name: str
    skeleton: str
    inject: dict[str, list[Callable]]
    trigger: dict
    config: dict = field(default_factory=dict)
```

无 deploy 段 (容器化交项目 Docker 流程).
无 status 段 (v0.1 无 reconcile, 无 spec/status 分离).

---

## 4. Assembler 朴素契约 (v0.1)

```python
def assemble(manifest: ServiceManifest) -> Service:
    """一次性装配:
       读 manifest → 校验注入 → 加载骨架 → 注入元素 → 返回可 .run() 的 Service
       
       不做 reconcile (无项目实证, 砍掉 K8s Operator 幂等).
       不管部署.
    """
```

砍幂等 reconcile 的理由: 5 项目无任一在用"幂等 reconcile 装配服务", 跟 K8s Operator 搬来的概念无实证, 恰是红线 1 要禁的凭空设计.

---

## 5. 触发模型 (吸收 Dagster)

声明优先, 命令兜底:

| 原语 | 含义 |
|------|------|
| `on_cron(expr)` | cron 定时 |
| `on_interval(sec)` | 固定间隔 |
| `on_event(topic)` | 事件驱动 |
| `on_upstream(ref)` | 上游完成后触发 |

真正特殊触发 → 项目自写 (专属逃生通道).

---

## 6. templates/ 准入线

templates/ 只放"拷贝用脚手架", 不是 import 的库.

| 维度 | engines/ | templates/ |
|------|----------|------------|
| 使用 | `import oservice` 调用 | 项目复制改写 |
| 本质 | 机制库 (版本契约) | 起手式 (无契约) |
| 准入 | 5 红线 | "是可复用起手式" |

判据: import 复用 → engines/obase, 拷贝改写 → templates/

**JWT 鉴权该回 obase.auth (import 复用), 不在 templates.**

---

## 7. omodul vs oservice 边界

| | omodul | oservice 引擎 |
|---|--------|--------------|
| 本质 | 一次性业务事务 | 持续运行 / 调度驱动 |
| 生命周期 | 算完返回, 进程结束 | start/stop/health, 长生命周期 |
| 判据 | **跑完就结束** | **常驻 / 被调度反复触发** |

关键衔接: v1.0 §5.14 禁 omodul 调 sibling omodul, 规定"多 omodul 协作必须在服务层".
**oservice.orchestrator 是这条禁令的正式归属.**

---

## 8. 专属服务逃生通道 (保留 v1.0 §6.2)

assembler 吃不下的服务 → 项目自管手写, 不强制装配.

复利纪律: 专属服务形态稳定 + 第二个项目复现 → 抽象成新骨架沉淀进 oservice (过红线 1 ≥2 实证).

---

## 9. 引擎入库治理执行

**前 5 个引擎入库 (M0-M3 阶段), Wiki 人工把关.**

引擎骨架"范式合规"比 oprim 难判 (oprim 客观, 骨架需架构判断). 一旦设计错被 N 个项目依赖, 纠错成本是项目数倍.

Owner Claude 验范式合规后, Wiki 亲自确认"两个实证项目的引擎真的能套进这个骨架吗".

5 个引擎后骨架接口模式收敛, Owner 可独立判.

---

## 10. Phase 1 落地

| 引擎 | 实证来源 | 状态 |
|------|---------|------|
| `alerter` | Aegis AlertEngine + Tide AlertSchedulerEngine | Phase 1 (≥2 实证 ✅) |
| `webhook_dispatcher` | Aegis C2 + 待第 2 实证 | 推后 |
| collector / scheduler / api_server / agentic_loop | 待 ≥2 实证 | 推后 |
| orchestrator | 实证不足 | 推后 |

预计 1-2 月引擎清单收敛稳定.

---

## 11. v1.1 SPEC 待办映射

| SPEC 项 | 本基线对应 |
|---------|----------|
| §1 范畴扩展 "3O + obase + oservice" | §1 |
| §7 obase vs oservice 区分 | §0, §1 |
| §7.5 oservice 定位 + 准入 + Manifest + 边界 | §2-§7 |
| §9 Step 15 服务层改 "oservice 装配 + 专属自管" | §8 |
| §11 治理陷阱 "凭空建引擎骨架未实证" | §2 红线 1 |
| §14 治理 oservice 准入 5 红线 + SemVer | §2 |

---

## 12. 一句话总结

> oservice = 第 5 个独立包 (引擎骨架 + 装配器 + 脚手架), "有状态机制" 区别于 obase "无状态工具".
> 准入 5 红线以 ≥2 真实引擎实证为首, 裁决一切骨架 (含 assembler 自身无实证的幂等 reconcile + deploy 段被砍).
> 依赖严格单向 (oservice 最上层, 3O 四包禁反向). templates/ 只放拷贝脚手架.
> omodul (跑完结束) vs oservice 引擎 (常驻调度) 以"常驻否"划界.
> orchestrator 是 §5.14"多 omodul 协作"的正式归属.
> 引擎骨架最高杠杆最难判, 入库前 5 个由 Wiki 亲自把关. 专属服务保留逃生通道随复现收敛.
