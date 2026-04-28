# Codex 5 天推进总账

范围：

```text
from b2b3e88dc95d8192682fb45c79d15bd329a1d94f
to   a6608e2
```

本报告用于归档 Codex 启用以来的阶段性推进。它是历史报告，不替代：

1. `AGENTS.md`
2. `docs/PROJECT_BASELINE.md`
3. `docs/STANDARD_PATCH_FRAMEWORK.md`
4. `docs/CURRENT_STATE.md`

## 1. 总体结论

这 5 天的工作不是单点 patch，而是把项目从“单线程聊天推进”推进到“可多线程交接、可审计、可恢复”的阶段。

核心成果分为六组：

1. Codex 协作与文档交接体系。
2. data_hub / live 共享数据链路。
3. 1m / idx 数据质量与 hub-vs-klines 审计。
4. snapback-sabc live 与 sim/live/bn 审计闭环。
5. Spring-SABC 主基线、结构过滤与审计工具。
6. Binance REST quota / ban window / live 运行保护。

## 2. Codex 文档体系

相关提交：

```text
b2b3e88 docs: add Codex collaboration docs
7e59715 docs: add codex project handoff guide
```

已完成：

1. 建立 `PROJECT_BASELINE.md / STANDARD_PATCH_FRAMEWORK.md / CURRENT_STATE.md` 三层基线。
2. 新增 `AGENTS.md`，作为新 Codex 线程根入口。
3. 精简 `docs/README.md` 与 `docs/新聊天开场白.md`。
4. 将旧协作文档、旧报告、旧入口归档到 `docs/archive/`。
5. 明确线程切换纪律：项目事实来自仓库、代码、日志、state、落盘输出和 git 历史。

当前意义：

```text
长任务可以拆到多个 Codex 线程中完成，线程死亡不再等于项目失忆。
```

## 3. data_hub / live 协同

相关提交节选：

```text
d791a93 live: enforce latest hub snapshot schema
9818a4b live: include api stats in vol alerts
5e62f2c live: require explicit hub config schema
0ce0f27 data_hub 缺字段min_24h_quote_volume测试fail back
dcfc910 data_hub 缺字段exclude_symbols测试failback
cba7b40 data_hub 补足完整json字段
89f7d65 live: report finalize quality stats
b00acb4 live: move hub health stats to data hub
b9af79c live: refresh market rollsum on full universe
7bd6bf5 live: centralize shared hub snapshots
5d0c19d live: incrementally refill shared bars
b80bf1b live: track binance rest quota state
```

已完成：

1. hub 配置 schema 显式化，缺字段 fail-fast。
2. hub snapshot 字段补齐，并集中到 shared hub 管理。
3. live 从 hub bars 读取 universe 指标，减少指标源漂移。
4. full universe rollsum refresh、shared bars refill、finalize quality stats 已推进。
5. hub health stats 下沉到 data hub。
6. REST quota state、API stats、ban window 保护与告警增强。

当前主要风险：

1. REST 限频与 ban window 仍需持续观察。
2. live/hub/sim 三者的 universe 与 24h_vol 口径仍需对表。
3. snapshot 对人工审计仍偏工程化，解释层可继续补。

## 4. 1m / idx 数据质量

相关提交节选：

```text
d11c7d2 fix: define end_ms in klines backfill
3ba9641 fix: keep idx fields empty during contract writes
72434e2 fix: preserve real idx during klines rewrites
86ee313 delisted: track confirmed symbols in json
1178c40 live: require continuous recent windows
14c7fbf live: normalize closed bar fetch windows
0affb6a live: refill partial rollsum windows
4eecf12 live: fast-fail static index price 400s
f1788ea live: auto-increment augment idx sync
d706308 audit: add klines watermark incremental modes
59e3a74 audit: align hub-vs-klines cache windows
429dc8f audit: read finalized symbols from shared hub
```

已完成：

1. 修复 klines backfill 明确 bug。
2. contract 写入与 idx 字段责任拆清：没有真实 idx 时保持空，不伪造。
3. rewrite 时保留已有真实 idx。
4. 对 static index price 400 进行 fail-fast。
5. delisted confirmed symbols 进入 json 管理。
6. 数据质量审计工具支持 watermark / incremental 模式。
7. hub-vs-klines 审计改为重叠区间对表，并接入 shared hub finalized symbols。

当前意义：

```text
1m 数据、idx 字段、hub rollsum 的审计能力明显增强，后续可以支撑更稳的 sim/live 对齐。
```

## 5. snapback-sabc live / audit

相关提交节选：

```text
80cae54 max_history_window_mins=130
06bb25f strategies/snapback/config.highfreq.json 启动live
8e6ca14 strategies/snapback/config.highfreq.json 启动live market_total_24h_vol_min=35B
ed25985 live: fix stage5 market vol gate
f6c4f5b live: restore pandas import in stage5 audit
94c83bb 实盘账户参数strategies/snapback/live_config.chen912.json strategies/snapback/live_config.junjie2026.json
ebc1d84 live: align snapback market vol source
```

已完成：

1. snapback highfreq live 配置进入实盘观察阶段。
2. stage5 audit 相关运行问题修复。
3. live market vol source 已向 hub/source 口径对齐。
4. 多账户 live config 已加入。
5. live projection、live signals、live trades、bn truth、triplet audit 闭环已经形成。

当前 pending：

1. sim/live 一致性验证仍是 snapback 后续核心任务。
2. market_total_24h_vol、24h_vol、universe gate 的 sim/live/hub 对齐仍需专项对表。
3. 条件委托 / algo 父单是否进入 bn truth 独立真相层仍待决策。

## 6. Spring-SABC

相关提交节选：

```text
5a5b453 spring: persist sim decision audit context
500638f strategies/spring/config.json 参数对齐1924基线版本 跑全量回测
9209123 spring: cap rebound ratio in structure filter
31400af spring: add pre-a structure filters
612c02f spring: require non-negative pre-a trend
369342f strategies/spring/config.json max_risk_pct 0.99 -> 0.08
b66553d strategies/spring/config.json max_risk_pct 0.08->0.12
a6608e2 strategies/spring/config.json max_risk_pct 0.12->0.1
```

已完成：

1. Spring-SABC 的策略语义集中到唯一活跃语义文档。
2. sim decision audit context 落盘，支持后续审计工具复盘结构判断。
3. gamma volume / pre-A / exit behavior 等审计工具继续增强。
4. rebound ratio 加入上限。
5. pre-A structure filters 加入。
6. pre-A trend 当前要求非负。
7. 主配置多轮调整，当前 HEAD 中 `max_risk_pct = 0.1`。

当前 pending：

1. 继续审计 2026-04 等坏月份 / 坏 regime。
2. 确认 `max_risk_pct = 0.1` 的正式阶段定位。
3. 若继续调 Spring 结构过滤，必须同步维护审计工具。

## 7. 工具与目录整理

相关提交节选：

```text
c2d16ed chore: reorganize audit scripts
cb23800 chore: group infra data quality audits
e83d717 tools/make_md5_line_suffix_copies.py tools/常用命令
a283f93 tools/常用命令
4611f1f 更新 常用命令
```

已完成：

1. audit tools 按领域整理到 data_quality / spring / snapback / maintenance 等目录。
2. MD5/line suffix copy 工具增强。
3. 常用命令与常用审计命令持续沉淀。

当前注意：

```text
当前存在未提交本地改动：

- `tools/常用命令`
```

## 8. 后续推荐拆线程方式

### 8.1 data_hub 与 live 协同

建议拆成：

1. 线程 A：只读 hub snapshot / logs / config，确认当前现场。
2. 线程 B：只对齐 live 消费 hub bars 的输入口径。
3. 线程 C：只做 sim feeder 与 hub/live 24h_vol 对表。
4. 线程 D：只修一个明确偏离点。
5. 线程 E：只做验证与文档归档。

### 8.2 snapback-sabc sim/live 一致性

建议拆成：

1. 线程 A：锁定 sim 输入输出样本。
2. 线程 B：锁定 live stage / hub 输入样本。
3. 线程 C：逐字段对表。
4. 线程 D：定位一个偏离原因。
5. 线程 E：单问题 patch。
6. 线程 F：验证并更新 `CURRENT_STATE.md`。

### 8.3 Spring-SABC

建议拆成：

1. 线程 A：固定当前 config 与语义。
2. 线程 B：审计 2026-04 坏月份。
3. 线程 C：只处理一个结构过滤假设。
4. 线程 D：回测验证。
5. 线程 E：更新语义或归档报告。

## 9. 最重要的协作结论

```text
项目状态不能只留在聊天里。
```

从现在开始，任何长任务结束前都要判断是否需要更新：

1. `docs/CURRENT_STATE.md`
2. 对应策略语义文档
3. `docs/archive/reports/` 下的阶段报告

这条纪律是后续多 Codex 线程协作的核心防漂移机制。
