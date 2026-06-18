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

## 3. 第一阶段输出

3.1 observer 每次扫描输出：
- `scan_id`
- `account`
- `latest_closed_bar_ts`
- `latest_closed_bar_bj`
- `passed_count`
- `top_candidates`
- `rejected_summary`
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
3.3 bot 推送只允许发送通过阈值的候选摘要，避免刷屏。  

## 4. 交易边界

4.1 第一阶段禁止下单。  
4.2 后续若进入交易阶段，必须先新增独立执行语义基线，至少定义：
- 入场方式：突破、回踩、分批或其它。
- 硬止损。
- 最大持仓时间。
- 单 symbol 并发限制。
- 与 `alt_reclaim` 和 `CAL` 的账户级风险隔离方式。
4.3 未完成交易阶段语义基线前，IGN 只能作为 observer 运行。
