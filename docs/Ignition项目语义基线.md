# Ignition / IGN 项目语义基线

## 0. 定位

0.1 `Ignition`，缩写 `IGN`，属于 `momentum_ignition` 策略家族。  
0.2 IGN 面向山寨币放量启动后站稳、台阶抬高或稳步爬升的 LONG-only 点火结构。  
0.3 IGN 第一阶段只做 observer / audit / bot alert，不做实盘交易，不产生 entry / TP / SL 订单。  
0.4 IGN 不属于 `snapback / spring / SWR` 的结构回收策略，不复用其 A/B/C/S 结构语义。  

## 1. 数据语义

1.1 IGN 第一阶段复用 live 公共 `market_data_hub_runner.py` 产出的 `finalized_candidate_inputs`。  
1.2 hub 的 `min_24h_quote_volume` 仍是工程预过滤，只决定哪些 symbol 构建 HBs payload。  
1.3 IGN observer 只读取 `full_df` 中已闭合的 1m HBs，不读取当前未闭合 CB 的 OHLCV。  
1.4 第一阶段观察窗口为最近 180 根已闭合 1m bar。  
1.5 若 hub payload 缺失、过期、symbol 历史不足、字段缺失或窗口不连续，observer 必须 fail-fast 或在该 symbol 上记录明确 reject reason，不得静默兜底。  

## 2. 结构定义

2.1 IGN 捕捉的不是第一根爆拉，而是启动后中后段仍然稳定、有承接、有抬高的结构。  
2.2 基础趋势骨架：
- 最近 180m 总体涨幅必须高于显式阈值。
- 最近 `0-30m`、`30-60m`、`60-120m`、`120-180m` 四段中，多数分段必须上涨。
- 单个分段允许小幅横盘或轻微回撤，但不得出现超过显式阈值的弱段。
2.3 稳定性门槛：
- 最近 180m 最大回撤不得超过显式阈值。
- 当前价格必须接近 180m 高点，不能已经明显塌回。
- 1m 大振幅 bar 数量必须受控，避免上蹿下跳。
- 最近 60m 大阴线数量必须受控。
- 分段低点应整体抬高或至少不能明显破坏结构。
2.4 成交量确认：
- 最近 30m quote volume 必须相对前 150m 均值有显式倍数提升。
- 成交量确认只作为点火强度事实，不替代价格结构稳定性。

2.5 `IGN_BASE` 点火筑台子型：
- `IGN_BASE` 使用 1m HBs 的 `A-B-C` 三段结构，只做 observer / audit / bot alert，不下单。
- `A-B` 只定义点火前参考箱体，不用作“平静”一票否决；`AB` 的涨跌幅、振幅、斜率与点火量能只作为背景画像写入 audit，供后续复盘。
- `B` 是点火段，必须满足二选一：单根 1m 阳线收盘涨幅不低于显式阈值，或从 B 开始连续 3 根 1m 阳线且三阳总收盘涨幅不低于显式阈值。
- `B` 的点火收盘价必须突破 `AB_box_high`；这是 `AB` 对信号的唯一硬门槛。
- `B-C` 是点火后确认期，默认 3 根 1m bar。确认期不使用下影线做一票否决，只取确认期每根 K 线的收盘价下沿 `BC_close_floor = min(close_1..close_N)`。
- `BC_close_floor` 必须高于“点火收盘价减去点火涨幅的显式可回吐比例”：

```text
ignition_gain = ignition_close - ignition_start_price
BC_close_floor >= ignition_close - ignition_gain * bc_max_gain_pullback_pct
```

- 该语义表达的是：点火后的价格必须守住点火成果的大部分，而不是只要站在 `AB_box_high` 上方就算有效。

## 3. 第一阶段输出

3.1 observer 每次扫描输出：
- `scan_id`
- `account`
- `latest_closed_bar_ts`
- `latest_closed_bar_bj`
- `passed_count`
- `early_passed_count`
- `base_passed_count`
- `top_candidates`
- `top_early_candidates`
- `top_base_candidates`
- `rejected_summary`
- `early_rejected_summary`
- `base_rejected_summary`
- `alert_cooldown_secs`
- `alert_suppressed_count`
- `early_alert_suppressed_count`
- `base_alert_suppressed_count`
- `summary_log_interval_secs`
3.2 单个 symbol 输出必须包含：
- `r_30m`
- `r_30_60m`
- `r_60_120m`
- `r_120_180m`
- `r_180m`
- `positive_segment_count`
- `max_drawdown_180m`
- `near_high_drawdown`
- `large_range_count`
- `large_red_count_60m`
- `volume_boost_30m`
- `low_lift_count`
- `structure_score`
- `reject_reasons`
- `early_passed`
- `early_reject_reasons`
- `base_passed`
- `base_reject_reasons`
- `base_profile`
3.3 observer 输出分为两层：
- `IGN_EARLY`：早期观察层，允许结构还没有完全确认，但必须已经出现 180m 上行、最近 30m 抬升、量能放大、低点抬高和可接受回撤；同时必须低于早期层最大 180m 涨幅阈值，避免已经明显过热的结构仍被称为 early。用途是提醒人工盯盘，不代表可直接追入。
- `IGN`：确认层，要求更高的 180m 总涨幅、结构分、贴近高点、量能和稳定性；用途是记录已经确认的点火结构。
- `IGN_BASE`：点火筑台层，要求 B 点火突破 `AB_box_high`，且后续 BC 确认期的收盘价下沿守住点火涨幅的大部分；用途是捕捉“点火后不回落、市场接受新价格层”的更窄子型。
3.4 bot 推送只允许发送通过阈值的候选摘要，避免刷屏。若同一 symbol 同一轮已经通过 `IGN` 确认层，则不再重复发送 `IGN_EARLY`。同一账户、同一层级、同一 symbol 的重复推送必须受显式冷却时间约束。Telegram 推送不展示 `account` 与 `scan_id`，必须展示信号生成时间 `sig=HH:MM`；完整 `account/scan_id` 继续保留在 audit JSON 中。
`IGN_BASE` 推送面向人工复盘，不展示 `account` 与 `scan_id`，必须展示信号生成时间 `sig`、`A/B/C` bar 时间，以及 `ABhi` 对应 bar 时间，时间显示到 `HH:MM`。
`IGN_BASE` 的推送去重身份必须包含 `symbol + mode + ignition_start_bar_ts + ignition_end_bar_ts + bc_end_bar_ts`；同一组 `A/B/C` 结构只允许推送一次，不能在 symbol 冷却结束后重复推送旧结构。
3.5 observer 的普通扫描 summary 不应每分钟刷 `INFO` 日志。`runtime.summary_log_interval_secs` 控制无新推送时的低频 heartbeat；当产生新的 `IGN` / `IGN_EARLY` / `IGN_BASE` 推送或单次非 loop 扫描时，仍必须即时写 `INFO`。

## 4. 交易边界

4.1 第一阶段禁止下单。  
4.2 后续若进入交易阶段，必须先新增独立执行语义基线，至少定义：
- 入场方式：突破、回踩、分批或其它。
- 硬止损。
- 最大持仓时间。
- 单 symbol 并发限制。
- 与 `alt_reclaim` 和 `CAL` 的账户级风险隔离方式。
4.3 未完成交易阶段语义基线前，IGN 只能作为 observer 运行。
