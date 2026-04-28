# 当前项目状态
（`CURRENT_STATE.md`）

更新时间：2026-04-28

## 0. 文档定位

本文件只记录 **当前现场状态**。它回答：

```text
我们当前正在做什么、已经做到哪里、下一步做什么。
```

本文件不是项目宪法，也不是 patch 交付框架。若发生冲突，优先级固定为：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`

详细阶段流水账见：

```text
docs/archive/reports/2026-04-28_codex_5day_progress_summary.md
```

---

## 1. 当前项目定位

### 1.1 当前仓库

```text
bn_research_core
```

### 1.2 当前活跃主线

当前不是单一策略开发现场，而是以下几条主线并行：

1. `data_hub` 与 `live` 协同稳定化。
2. `snapback-sabc` 的 live 观察与 sim/live/bn 审计闭环。
3. `spring-sabc` 的主基线、结构过滤与审计工具完善。
4. 1m / idx 数据质量、hub-vs-klines 对表与基础设施审计。
5. Codex 多线程交接文档体系。

### 1.3 当前阶段目标

```text
让 live 数据链路、hub 共享数据、策略信号、交易执行、审计落盘与文档交接都进入可复核、可续接、可长期维护状态。
```

---

## 2. 当前固定入口与现场路径

### 2.1 文档入口

```text
AGENTS.md
docs/README.md
docs/PROJECT_BASELINE.md
docs/STANDARD_PATCH_FRAMEWORK.md
docs/CURRENT_STATE.md
```

新线程用户侧可复制模板：

```text
docs/新Codex线程开场白.txt
```

### 2.2 配置入口

```text
market_data_hub_config.json
strategies/snapback/config.highfreq.json
strategies/spring/config.json
```

### 2.3 live / audit 现场

```text
state/live/snapback_mybwin139.state.json
state/live_audit/snapback_mybwin139.jsonl
output/live_projection
```

---

## 3. 最近 5 天总账结论

本节从 commit `b2b3e88dc95d8192682fb45c79d15bd329a1d94f` 之后到当前 HEAD `a6608e2` 的本地 git 历史归纳。

### 3.1 Codex 协作体系

已完成：

1. 新增 Codex 协作文档体系。
2. 新增根入口 `AGENTS.md`。
3. 精简 `docs/README.md` 与 `docs/新聊天开场白.md`。
4. 将旧协作文档、旧入口、旧审计报告归档到 `docs/archive/`。
5. 明确新线程启动纪律：不依赖旧聊天记忆，先读仓库文档与现场事实。

当前结论：

```text
线程可以切换；项目状态必须沉淀在仓库文档、代码、日志、state、落盘输出和 git 历史中。
```

### 3.2 data_hub / live 协同

已完成：

1. `market_data_hub_config.json` 改为显式 schema，缺少关键字段必须 fail-fast。
2. `data_hub` 补齐完整 json 字段，并增加配置快照发布能力。
3. hub snapshot 由 shared hub 集中管理。
4. live 侧从 hub bars 获取 universe 指标，降低 live 与 hub 指标源漂移。
5. 增加 finalized symbols 读取能力，审计工具可读取 shared hub finalized 结果。
6. full universe rollsum refresh、shared bars 增量 refill、部分 rollsum window refill 已推进。
7. 增加 finalize quality stats 与 hub health stats，可用于现场观察。
8. 增加 Binance REST quota / ban window / API stats 相关保护与观测。

当前配置事实：

```text
market_data_hub_config.json:
- enabled = true
- min_24h_quote_volume = 30000000
- history_window_mins = 180
- rollsum_refresh_batch_size = 80
- exclude_symbols 已显式配置，包含大市值币与 TradFi 品种
```

当前 pending：

1. 继续观察是否长期无 `-1003` / ban window 风险。
2. 继续确认 live 消费 hub 数据后，sim/live 指标口径是否仍有残余偏离。
3. 继续补齐人工审计可读性，例如 prefilter 原始名单与 snapshot 字段解释。

### 3.3 1m / idx 数据质量

已完成：

1. 修复 `klines_1m_store.py` backfill 中 `end_ms` 未定义问题。
2. contract 写入时保持 idx 字段为空，避免伪 idx。
3. klines rewrite 时保留真实 idx 字段，避免覆盖已有真实 idx。
4. static index price 400 场景 fail-fast。
5. 新增/增强 delisted confirmed symbols 记录。
6. 新增 1m 数据质量与 idx completeness 审计能力。
7. hub-vs-klines 审计改为 overlap only，并对 cache window / finalized symbols 做了对齐。
8. 增加 augment idx 增量同步能力。

当前 pending：

1. data_hub、klines_1m、idx 字段三者仍需持续审计对表。
2. 若再改 1m 写入链路，必须明确 contract 与 idx 字段责任，禁止伪兼容。

### 3.4 snapback-sabc live / audit

已完成：

1. `config.highfreq.json` 已进入 live 测试参数状态。
2. live `max_history_window_mins`、`market_total_24h_vol_min` 等参数多轮调整。
3. live 侧已对齐 snapback market vol source。
4. stage5 audit 的 market vol gate 与 pandas import 问题已修复。
5. live bot message 增加完整日志。
6. live projection / live_signals / live_trades / bn truth / triplet audit 主线已经打通。
7. ENTRY 后 SL / TP 建立顺序与 SL 保护失败 fail-fast 风险修补已完成。
8. 实盘账户 live config 已新增/调整。

当前配置事实：

```text
strategies/snapback/config.highfreq.json:
- runtime.max_history_window_mins = 150
- universe.24h_quote_volume_min = 30000000
- universe.market_total_24h_vol_min = 20000000000
- structure.s_to_c_window.mins = 60
- structure.election_rule = drop_pct_top1
- exit_policy.time_stop.max_hold_mins = 4
```

当前 pending：

1. 持续做 snapback sim/live 一致性验证。
2. 继续明确 live 使用 hub 后与 sim feeder 的 universe / 24h_vol 口径差异。
3. 是否为 bn truth 增加条件委托 / algo 父单独立真相层，尚未决定。
4. triplet audit 是否显式解释父单 ID 与基础子单 ID 差异，尚未决定。

### 3.5 Spring-SABC

已完成：

1. Spring-SABC 主语义基线已集中到 `docs/Spring-SABC项目语义基线.md`。
2. sim decision audit context 已落盘，支持 pre-A / gamma volume 等审计工具读取。
3. 增加 rebound ratio 上限过滤。
4. 增加 pre-A structure filters。
5. pre-A trend 当前要求非负。
6. Spring 配置曾对齐 1924 基线，并多轮调整 `max_risk_pct`；回测事实显示硬过滤收紧到 `0.12/0.10/0.08` 没有改善综合表现。
7. 当前配置中 BREAKEVEN_GUARD 关闭，保留代码语义但不作为当前主基线启用。
8. `max_risk_pct` 已从 Spring 活跃语义中删除；风险距离改为动态开仓金额计算依据。

当前配置事实：

```text
strategies/spring/config.json:
- strategy_name = spring-sabc
- max_history_window_mins = 130
- min_24h_chg_pct = 30
- min_24h_quote_volume = 50000000
- score_top_n = 3
- gamma_ac_vol_ratio_min = 1.0
- rebound.ratio_min = 0.75
- rebound.ratio_max = 1.2
- pre_a.chg_pct_min = 0.0
- take_profit_pct = -1
- max_hold_mins = 60
- breakeven_guard.enabled = false
- base_order_notional_usdt = 100
- full_notional_risk_pct = 0.05
```

当前 pending：

1. 基于 `SPRING_V1_30D_P6_0427T1606_ALL` 作为结构毕业候选，重跑动态 sizing 后的正式 sim。
2. 继续审计 Spring-SABC 坏月份 / 坏 regime，尤其 2026-04。
3. 若再调整 Spring 结构过滤或 sizing 参数，必须同步评估审计工具是否需要扩展。

### 3.6 audit tools / 目录治理

已完成：

1. audit scripts 已按 data_quality / spring / snapback / maintenance 等方向整理。
2. `make_md5_line_suffix_copies.py` 等工具增强。
3. 常用审计命令与常用命令文件持续维护。

当前注意：

```text
当前仍有未提交本地改动；新线程开始时必须先看 git status，不要误把它当成已提交事实：

- tools/常用命令
```

---

## 4. 当前明确不做

1. 不引入 SHORT 语义、字段、分支或实现。
2. 不把旧归档报告反向覆盖当前活跃文档。
3. 不再依赖旧聊天记忆推进长任务。
4. 不在未锁基线时进入正式 patch。
5. 不在一个 patch 中混合性能、结构、逻辑多个目标。
6. 未经批准，不 `git push`，不碰生产发布。

---

## 5. 下一步建议顺序

### 5.1 data_hub / live 协同

```text
1. 读取 hub snapshot / market snapshot / live logs，确认当前最新现场。
2. 对齐 live 消费 hub bars 后的 universe / 24h_vol 与 sim feeder 口径。
3. 继续观察 REST quota / ban window 状态。
4. 必要时补充 snapshot 可读性，不先改策略逻辑。
```

### 5.2 snapback-sabc sim/live 一致性验证

```text
1. 锁定一个 bar_ts / symbol 样本。
2. 对齐 sim 输入、hub 输入、live stage 输入。
3. 对比 24h_vol / market_total_24h_vol / stage5 fail reason / signal。
4. 只在偏离事实明确后进入单问题 patch。
```

### 5.3 Spring-SABC

```text
1. 固定当前 config 事实。
2. 用动态开仓金额语义重跑 `0427T1606` 候选基线，确认收益、回撤与 2026-04 表现。
3. 若要改 pre-A / rebound / sizing 参数，先形成语义说明，再做单问题 patch。
```

### 5.4 文档

```text
1. 每个长任务结束前判断是否更新 CURRENT_STATE.md。
2. 阶段性审计结论写入 docs/archive/reports/。
3. 新线程必须从 AGENTS.md + docs/README.md + CURRENT_STATE.md 恢复现场。
```
