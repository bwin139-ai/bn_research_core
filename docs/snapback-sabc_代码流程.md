
candidates
 - 候选池
 - 意思是：已经通过完整 structure 检查的币种集合。
candidate_count
 - 候选池数量
 - 意思是：这一轮一共有多少个币通过了完整 structure 检查，进入 candidates
cross_section
 - 横截面总表
 - 这一轮准备拿来给策略检查的一张“总表”。每一行是一个币种，每一列是这个币当前这一轮的横向指标。
 - 里面常见会有：close chg_24h vol_24h close_idx high_idx low_idx
 - signal 生产侧只能使用已闭合的 C = HBs[0] 数据；24h 指标必须锚定 C。
market_total_24h_vol
 - 全市场 24h 成交额聚合指标
 - 属于 signal 生产侧数据事实，必须和 cross_section / vol_24h 一样锚定同一个 C = HBs[0]
 - live 不允许把不同 symbol 的不同 latest_bar_ts rolling 结果混合成一个总量；允许来源是已证明覆盖同一 C 的 market-wide rolling state，或显式标记为 market-wide 的同一 finalized C-anchor HBs payload 中 `cross_section.vol_24h` 全量聚合。
 - 策略候选池 payload 的局部 `cross_section.vol_24h` 不能作为全市场总量兜底；若没有同 C 且 market-wide 的来源，Snapback live 只能维护交易生命周期并跳过新信号扫描。
cs
 - 经过 universe 过滤后的横截面子集
 - cross_section 是原始横向总表
 - cs 是通过 universe 过滤后的子集
full_df
 - 历史K线资料包
 - 意思是：这一轮相关币种各自对应的历史 K 线数据集合
 - 其中每个 symbol 都对应一份自己的历史 bars 数据
latest_closed_bar_ts
 - 最近刚刚确认收盘的那根 bar 的时间戳
 - 意思是：当前这一轮 live / sim 所围绕的“最新已收盘 bar”的时间标记
 - 可以理解成：本轮判断到底是围绕哪一根刚收完的K线在工作
candidate_symbols
 - 本轮待扫描币种名单
 - 这名字非常容易误导，必须记住：它不是 candidates。它只是 live 入口层挑出来“本轮准备扫描”的一批币种名单。
build_market_snapshot(...)
 - 市场快照准备步骤
 - 意思是：先拿一份当前市场的总体快照，确定本轮围绕哪根 bar 工作，并准备 ticker 类信息。
 - 注意：这一步不是 structure 检查、也不是 candidates
build_live_inputs(...)
 - live 输入资料构建步骤
 - 把本轮待扫描币种的 cross_section 和 full_df 准备出来，供后面策略使用
_finalize_candidate_payload(...)
 - 闭合确认步骤
 - 意思是：再确认本轮刚收完的 bar 是否已经稳定，不是临时值、漂移值。
 - 可以理解成：开工前再确认一次资料是不是已经定稿
on_kline_close(...)
 - 策略主判断函数
 - 意思是：策略真正开始做
 - universe 过滤、structure 检查、生成 candidates、选出 top1、形成 signal
active_symbols
 - 当前已在交易中的币种集合
 - 意思是：当前已经有持仓，或者有挂单，正在占用交易席位的币。
 - 策略在扫描时会避开它们，避免重复开仓。
base_order_notional_usdt
 - sim 固定开仓名义金额
 - 来自策略配置 risk_controls.base_order_notional_usdt。
 - sim 侧生成 signal 后由 run_backtest 注入 signal / order / trade 流水。
 - 绩效统计优先按 position_notional_usdt * pnl_pct 计算实际 USDT 盈亏。
position_notional_usdt
 - 本笔 sim 交易实际使用的开仓名义金额。
 - 对 snapback sim 当前等于 base_order_notional_usdt。
 - 不应再由后处理阶段静默假设为 100U。
entry_notional_usdt
 - snapback live 配置中的实盘开仓名义金额。
 - live 下单数量按 entry_notional_usdt / current_price 计算。
 - 它与 sim 的 base_order_notional_usdt 都是显式资金字段，但分别属于 sim 与 live 配置边界。
