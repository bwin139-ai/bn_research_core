# Snapback Sim/Live Consistency Audit Spec

更新时间：2026-05-06

## 1. 文档定位

本文是 Snapback 策略 `sim/live` 一致性审计的长期规格文档。

它回答：

```text
每次审计 Snapback sim/live 一致性时，必须看什么、如何匹配、如何分类、哪些偏差算问题、哪些偏差只做观测。
```

本文不是一次性审计报告。每次新线程执行 Snapback `sim/live` 一致性审计时，应按本文规格复用同一套观测口径；若审计中发现新的稳定判断标准，应更新本文，而不是只停留在聊天记录中。

若本文与项目总规则冲突，优先级仍按：

1. `docs/PROJECT_BASELINE.md`
2. `docs/STANDARD_PATCH_FRAMEWORK.md`
3. `docs/CURRENT_STATE.md`
4. 本文

## 2. 审计目标

Snapback `sim/live` 一致性审计分两层：

1. 策略信号一致性：
   - 同一重叠时间段内，`sim` 与 `live` 对同一市场事实是否产生一致的 Snapback 策略信号。
   - 重点审计 ABC 结构、结构字段、价格字段、过滤原因与 live-only / sim-only 的可解释性。
2. live trading lifecycle 完整性：
   - live 已产生且通过执行门禁的信号，是否按 Snapback 策略语义完成 ENTRY、SL、TP、time-stop、离场识别、state 清理、cooldown 与落盘。

审计结论不能只写“数量一致”或“symbol 一致”。必须能解释每个 mismatch 属于哪一类。

## 3. 常用输入

### 3.1 sim 输出

```text
output/**/sim_signals.{run_id}.jsonl
output/**/sim_trades.{run_id}.jsonl
output/**/sim_summary.{run_id}.json
```

### 3.2 live projection / trade 输出

```text
output/live_projection/live_signals.{run_id}.jsonl
output/live_projection/live_trades.{run_id}.jsonl
output/live_projection/*.jsonl
```

### 3.3 live audit / stage audit

```text
state/live_audit/snapback_{account}.YYYY-MM-DD.jsonl
state/live_audit/stage_audit/snapback_{account}.{stage}.YYYY-MM-DD.jsonl
state/live_audit/market_data_hub/{account}/daily/YYYY-MM-DD/*.jsonl
state/live/snapback_{account}.state.json
```

### 3.4 Binance truth / local market data

按审计问题需要读取：

```text
data/klines_1m/**
state/live_audit/**/market_data_hub/**
交易所 positions / open orders / order truth 查询结果
```

如需判断 live lifecycle，交易所 truth 是最终事实源；本地 state 只是策略状态机视角。

## 4. 时间与匹配规则

### 4.1 重叠窗口

只审计 `sim` 与 `live` 都覆盖的时间段。窗口边界必须用绝对北京时间写明，例如：

```text
2026-05-01 13:00 BJ ~ 2026-05-03 13:30 BJ
```

### 4.2 signal identity

Snapback 信号优先按以下 identity 匹配：

```text
(symbol, signal_time)
```

必要时辅助检查：

```text
c_time
a_time
b_time
account
run_id
```

`signal_time` 应是 `C+1m`。如果 live 消费到 `C+2m` 或旧 payload，属于 live data gate 问题。

### 4.3 ABC identity

匹配到同一 `(symbol, signal_time)` 后，必须继续比较：

```text
A time / A price
B time / B price
C time / C price
AB length
BC length
```

只要 ABC 不同，不能仅因 `(symbol, signal_time)` 一致就判为完全一致。

## 5. 信号字段审计优先级

### 5.1 必审硬字段

以下字段直接影响策略事实、信号是否成立或交易风险，必须审计：

```text
symbol
signal_time
a_time
b_time
c_time
a_index_price / a_index_high
b_index_price / c_index_low
c_index_price used by active structure rules
current_price used by Snapback signal
structure.rebound.ratio
structure.selloff.a_to_c_drop_pct
structure.s_to_c_window.chg_pct
resolved_sl_price
entry / sl / tp intent prices
```

其中 `B=C` 同 bar 时，`c_index_low / b_index_price` 是高优先级硬字段。它会直接影响：

```text
current_price > b_index_price
rebound_ratio
SL
是否通过结构判断
```

### 5.2 精度容忍

仅小数点精度、序列化精度、浮点舍入造成的微小差异，可判为一致。

如果差异已经改变过滤结果、排序、SL、TP、entry eligibility 或 lifecycle 行为，不能按精度差异放过。

### 5.3 降权观测字段

`c_index_close` 在 Snapback 当前有效语义中通常只影响：

```text
structure.basis.c_pct
```

当前最优基线长期将 `structure.basis.c_pct` 配置为近似忽略状态（例如 `[-1, 1]` 这类宽区间），因此：

```text
c_index_close 偏差需要记录，但默认不作为核心不一致项。
```

例外：

1. 若某次配置重新启用严格 `basis.c_pct` gate，则 `c_index_close` 必须恢复为硬字段。
2. 若 `c_index_close` 偏差导致 signal pass/fail、rank 或执行意图变化，则必须升级为不一致。

## 6. finalize / pseudo-finalized 审计标准

Live data hub 当前通过 candidate finalize probe 判断 C bar 是否稳定。历史审计结论：

1. 伪闭合更常表现为 `c_index_close` 与事后历史数据偏差。
2. `c_index_close` 偏差在当前 Snapback 语义中通常低影响。
3. 少数情况下伪闭合会影响 `c_index_low / b_index_price`。
4. 当 `B=C` 同 bar 时，`c_index_low / b_index_price` 偏差是高影响风险。

因此以后审计必须区分：

```text
c_index_close 偏差：记录、归因、默认降权。
c_index_low / b_index_price 偏差：核心审计项，尤其 B=C 同 bar。
```

若发现 live 因 `c_index_low / b_index_price` 偏差导致 sim/live pass/fail 不同，应优先审计：

```text
candidate finalize probe rounds
probe interval
first/last candidate snapshot
finalized payload anchor
stage4/stage5 fail reason
hub full_df vs candidate_cross_section
```

## 7. Live-only / Sim-only 分类

发现 live-only 或 sim-only 时，不得直接判为策略不一致。必须先分类。

### 7.1 Live-only 可解释类型

以下类型通常不判为策略信号不一致，但必须记录：

```text
sim 早一轮信号已成交并进入 cooldown，live 早一轮因 live-only execution guard 跳过，后续同 symbol 再次 live eligible。
live pre-entry price guard 通过/跳过导致 execution lifecycle 与 sim 不同。
交易所实时价格、SL 距离、最小下单规则等 live-only 执行保护造成差异。
live cooldown / state / exchange truth 与 sim 持仓模拟状态不同。
```

### 7.2 Sim-only 可解释类型

以下类型通常不判为策略结构不一致，但必须记录：

```text
live data hub 未提供可用 candidate payload。
stage2 universe metric frame 缺失，例如 contract_24h_metric_empty。
finalized full_df 有 symbol，但 candidate_cross_section 缺 symbol。
live stage4/stage5 缺现场记录，但已有 audit gap event 解释缺席边界。
live pre-entry guard 跳过执行，不应要求 sim 对齐该 live-only 执行保护。
```

### 7.3 必须判为待修复的不一致

以下情况必须作为问题推进：

```text
同一市场数据事实下，sim/live ABC 选择不同且无法由 live data quality 解释。
同一 ABC 下，硬结构字段计算不同并改变 pass/fail。
live 使用旧 finalized payload、错 anchor payload 或 C+2m payload。
live stage 缺席且没有 audit 记录能解释缺席边界。
live lifecycle 未按 ENTRY -> SL-first -> TP-after-SL -> reconcile/exit/cooldown 语义落盘。
```

## 8. Stage Audit 检查顺序

每个 mismatch 应按以下顺序定位：

1. Universe：
   - stage2 是否通过。
   - `contract_24h_metric_empty` / `metric_frame_present` / `metric_frame_empty` / `contract_metric_prefetch_error` 是否解释缺席。
2. Finalize：
   - hub finalized summary 是否包含 symbol。
   - `full_df_only_symbols` 是否包含 symbol。
   - 是否出现 `c_bar_finalize_cross_section_missing`。
3. Strategy input：
   - 是否出现 `candidate_cross_section_missing_after_finalize`。
   - stage4 input snapshot 是否存在。
4. Structure：
   - stage5 structure audit 是否存在。
   - fail reason 是否与 sim pass/fail 差异对应。
5. Signal：
   - live projection 是否写 signal。
   - signal fields 是否与 sim 一致。
6. Execution：
   - pre-entry guard / local state / exchange precheck / cooldown 是否阻断。
   - 若执行，是否进入完整 lifecycle。

## 9. Trading Lifecycle 审计标准

Live trading lifecycle 与策略信号审计分开做。一个信号可以策略一致，但 live execution 被合理跳过。

### 9.1 必查事件链

已执行 live 信号必须检查：

```text
signal_detected
pre_entry_price_guard_pass / skip
execution_plan_ready
entry_submitted / entry_fill_observed
sl_submitted
tp_submitted
position_closed_detected
live_trade_closed projection
state open_trade cleared
cooldown set/refreshed
```

### 9.2 ENTRY / SL / TP 顺序

Snapback live 必须保持：

```text
ENTRY filled -> SL submitted/verified -> TP submitted/verified
```

SL 保护失败时不能静默继续裸仓；必须进入明确的保护失败处理语义。

### 9.3 Exit reason

离场原因必须能从交易所 truth 与本地 live trade 落盘互相解释：

```text
TAKE_PROFIT
STOP_LOSS
TIME_STOP
SL_SUBMIT_FAILED_FLATTEN
```

若交易所 position 已归零、open orders 已清空，但本地 state 仍保留 open_trade，属于 reconcile/state 清理问题。

## 10. 审计报告输出格式

每次审计建议输出以下结构：

```text
1. 审计窗口与输入文件
2. 总量对齐
   - sim signals
   - live signals
   - matched
   - sim-only
   - live-only
3. 硬字段一致性
   - ABC
   - c_index_low / b_index_price
   - rebound_ratio
   - SL / TP / entry intent
4. 降权观测
   - c_index_close 偏差
   - basis.c_pct 影响判断
5. sim-only / live-only 分类明细
6. live trading lifecycle 审计
7. 结论
   - 完全一致
   - 已解释偏差
   - 需要 patch 的问题
   - 需要补审计字段的问题
```

审计报告中必须明确：

```text
哪些偏差影响策略事实；
哪些偏差只影响观测字段；
哪些偏差来自 live-only 执行保护；
哪些偏差是审计记录不足，暂时不能下结论。
```

## 11. 当前固化结论

截至 2026-05-06，以下结论已固化为后续审计标准：

1. `c_index_close` 偏差默认记录但降权，除非当前配置重新启用严格 `basis.c_pct` 约束或它改变策略结果。
2. `c_index_low / b_index_price` 是核心硬字段，尤其 `B=C` 同 bar 时必须审计。
3. `pre_entry_min_sl_distance_pct` 属于 live execution guard，不要求 sim 对齐；出现 live skip 时应作为 live-only 执行保护观测，不默认判为 sim/live 策略不一致。
4. stage2 universe 通过但 stage4/stage5 缺席时，必须查 hub finalized full_df、candidate_cross_section 与新增 gap audit event。
5. live trading lifecycle 必须独立审计，不能用 signal 一致性替代。
