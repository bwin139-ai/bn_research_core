# Sweep-Reclaim 项目语义基线

## 0. 策略定位

`sweep-reclaim` 是一套 LONG-only、1m 级别、顺势延续型结构策略。

它与 `spring-sabc` 一样，先在强势候选池中选出 TopN，再检查洗盘修复结构；它不使用 idx，不使用 3m / 5m 或更长周期 K 线。

`sweep-reclaim` 的核心结构不是 `A -> B` 连续 N 分钟下跌，而是：

```text
强势票在最近 N 分钟窗口内扫出阶段低点 B 后，快速收回。
```

## 1. 策略命名

正式策略名：

```text
sweep-reclaim
```

策略短名：

```text
SWR
```

## 2. 价格语义

`sweep-reclaim` 只使用 contract 价格与 contract bars。

禁止使用：

```text
idx
idx_bars
basis
```

凡涉及价格字段，必须明确来自 contract bars。

## 3. 公共时间语义

`sweep-reclaim` 完全继承项目公共语义：

```text
HBs = 全部已收盘历史 bars
CB = 当前 bar
signal_time = entry_time = CB
```

结构完成点：

```text
C = HBs[0]
CB = C + 1
```

策略在 CB 时刻观察 HBs，并在 CB 产生信号与入场。

投喂边界：

```text
strategy logic / signal 生产层只允许消费 HBs 数据。
cross_section、24h_chg、24h_vol、rank、score、结构识别窗口全部锚定 C = HBs[0]。
若使用全市场 24h 聚合指标，market_total_24h_vol 同样必须锚定同一个 C；
live 不允许把不同 symbol 的不同 latest_bar_ts rolling 结果混成一个市场总量。
CB 数据只允许用于 signal 之后的执行撮合、entry price / pre-entry price 与最终 TP 解析。
```

禁止：

```text
用未来 bars 确认结构
把 C 放在 CB
让 signal_time 与 entry_time 脱离 CB
```

## 4. Universe 语义

`sweep-reclaim` 的 universe 先筛强势票，再做结构检查。

候选币必须至少满足：

```text
24h_chg_pct >= universe.min_24h_chg_pct
24h_quote_volume >= universe.min_24h_quote_volume
symbol 不在 universe.exclude_symbols 中
```

对通过硬条件的候选币，沿用 Spring-SABC 的强势 TopN 排名语义：

```text
score = rank(chg_24h) + rank(vol_24h)
score 越小越优
取 score_top_n 名进入 structure 检查
```

审计字段语义应与 Spring-SABC 保持一致：

```text
rank_chg_24h
rank_vol_24h
score
score_rank_all
selected_score_order
selected_for_structure
universe_hard_gate_pass
```

## 5. 结构锚点语义

`sweep-reclaim` 使用以下内部结构锚点：

```text
H -> gamma -> B -> C -> CB
```

全部结构锚点 `H / gamma / B / C` 都必须属于 HBs。

其中：

```text
C = HBs[0]
CB = C + 1
```

## 6. B 点语义

`B` 是最近 N 分钟支撑窗口内的阶段最低点。

配置字段：

```text
structure.support_window_mins
```

当前 performance baseline 使用：

```text
support_window_mins = 180
```

成立条件：

```text
b_low = support_window 内最低 low
bars_bc >= 1
```

若多个 bar 的 low 同为窗口最低点，取最靠近 C 的那一个作为 B。

这表示策略关注最近一次扫低后的收回，而不是反复引用更早的同价低点。

## 7. H 点语义

`H` 是同一 support window 内、位于 B 左侧的最高 `close` 点。

价格字段：

```text
h_close
```

若多个 bar 的 close 同为最高值，取最靠近 B 的那一个作为 H。

禁止用 `h_high` 替代 `h_close`，避免单根上影线污染 `hb_drop`。

## 8. H -> B 跌幅语义

`hb_drop` 表示 H close 到 B low 的下跌幅度：

```text
hb_drop = (h_close - b_low) / h_close
```

成立条件：

```text
hb_drop >= structure.hb_drop.min
```

`H -> B` 必须具有足够跌幅，否则不构成有辨识度的扫低洗盘结构。

## 9. B -> C 修复语义

`bc_rebound` 表示 B low 到 C close 相对 H -> B 跌幅的修复比例：

```text
bc_rebound = (c_close - b_low) / (h_close - b_low)
```

成立条件：

```text
structure.rebound.bc_rebound_min <= bc_rebound <= structure.rebound.bc_rebound_max
```

`C` 使用 `c_close`，禁止用 `c_high` 替代。

同时要求：

```text
c_close > b_close
```

这保证修复不是只从针尖反弹，而是已收盘实体具备收回事实。

## 10. 修复速度语义

定义：

```text
bars_hb = B 与 H 的 1m bar 距离
bars_bc = C 与 B 的 1m bar 距离
bc_over_hb_bars = bars_bc / bars_hb
```

成立条件：

```text
bars_hb >= structure.rebound.hb_bars_min
bars_bc >= structure.rebound.bc_bars_min
bc_over_hb_bars <= structure.rebound.bc_over_hb_bars_max
```

策略主语义使用比例约束，而不是单独使用固定 `bars_bc` 数量。

例如：

```text
bc_over_hb_bars_max = 0.3
```

表示 `H -> B` 下跌 10 根 1m bar，`B -> C` 最多允许 3 根 1m bar 完成修复。

可保留一个宽松的绝对上限：

```text
bars_bc <= structure.rebound.bc_bars_max
```

该上限只是防止超长磨蹭修复进入样本，不是主速度语义。

## 11. gamma 与放量语义

`gamma` 是从 B 向左回看与 `B -> C` 等距离的点：

```text
gamma = B - bars_bc
```

结构顺序必须严格满足：

```text
H -> gamma -> B -> C
```

若 `gamma` 不在 H 右侧，结构不成立；不得兜底换窗口。

`vol_climax` 表示扫低收回段相对前置下跌背景段的放量比例：

```text
vol_climax = avg_quote_volume(gamma, C] / avg_quote_volume(H, gamma]
```

成立条件：

```text
vol_climax >= structure.vol_climax.ratio_min
```

区间口径：

```text
avg_quote_volume(gamma, C] = sum quote_asset_volume of (gamma, C] / bars_gamma_c
avg_quote_volume(H, gamma] = sum quote_asset_volume of (H, gamma] / bars_h_gamma
```

`H -> gamma` 长度必须大于 0。

## 12. C 点成立语义

当以下条件全部成立，并且 `C = HBs[0]`，则认为 `sweep-reclaim` 结构完成：

```text
B 是 support_window 内阶段最低点
H 是 B 左侧最高 close 点
hb_drop 达到阈值
bc_rebound 位于配置区间
bc_over_hb_bars 不超过速度上限
vol_climax 达到阈值
c_close > b_close
H -> gamma -> B -> C 顺序合法
```

此时：

```text
C 是结构完成点
CB 是观察点
CB 也是信号点与入场点
```

## 13. 入场语义

`sweep-reclaim` 在 CB 时刻观察 HBs。

若 `C = HBs[0]` 且完整结构成立，则在 CB 产生 LONG 信号并执行入场。

## 14. 止损语义

止损锚点固定为：

```text
stop_loss_price = b_close
```

禁止用 `b_low` 替代主止损锚点。

语义原因：

```text
B close 是扫低后结构是否仍被守住的关键价格。
b_low 更容易受到瞬时插针噪音影响，不适合作为主止损锚点。
```

## 15. 止盈语义

止盈使用基于风险距离的 R 倍数：

```text
risk_distance = entry_price - stop_loss_price
take_profit_price = entry_price + risk_distance * exit_policy.take_profit_r_multiple
```

当前基础语义可使用：

```text
take_profit_r_multiple = 1.0
```

## 16. 配置语义

`sweep-reclaim` 配置必须显式提供以下段：

```text
strategy_name
runtime
universe
structure
exit_policy
risk_controls
```

缺少配置字段、字段含义冲突、价格字段混用、时间锚点越界，都必须 fail-fast。

禁止：

```text
默认值兜底
旧字段兼容
用 Spring-SABC 的 A/B/C 连续下跌语义替代 SWR 的 H/gamma/B/C 语义
```

## 17. Performance Baseline

当前 `sweep-reclaim` performance baseline：

```text
run_id = SWR_V1_30D_P6_0506T2125
period = 2025-04-18T00:00:00+08:00 -> 2026-05-06T10:00:00+08:00
scheduler = 30D / P6
success_count = 13
failed_count = 0
trades = 405
signals = 407
skipped_signals = 2
decision_audit_rows = 458822
viz_png = 405
return_simple_net_pct = 185.04
return_compound_net_pct = 428.33
max_drawdown_simple_net = 23.15U / 19.38%
max_drawdown_compound_net = 52.40U / 17.36%
```

该 baseline 对应当前核心参数：

```text
support_window_mins = 180
hb_drop.min = 0.06
bc_rebound_min = 0.20
bc_rebound_max = 0.40
hb_bars_min = 3
bc_bars_min = 1
bc_bars_max = 30
bc_over_hb_bars_max = 0.30
vol_climax.ratio_min = 3.0
score_top_n = 3
take_profit_r_multiple = 1.0
```
