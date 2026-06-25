# 当前项目状态
（`CURRENT_STATE.md`）

更新时间：2026-06-18

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
4. `sweep-reclaim` 的新策略语义基线与参数骨架。
5. `tvr` 的 TradFi Value Reclaim live-first 数据端建设。
6. 1m / idx 数据质量、hub-vs-klines 对表与基础设施审计。
7. Codex 多线程交接文档体系。
8. `core-anchor-ladder` / CAL / 锚梯策略的核心资产阶梯策略语义设计。
9. `ignition` / IGN 点火动量 observer 的语义与候选扫描建设。

### 1.3 当前阶段目标

```text
让 live 数据链路、hub 共享数据、策略信号、交易执行、审计落盘与文档交接都进入可复核、可续接、可长期维护状态。
```

### 1.4 策略家族当前分类

当前策略按 `strategy_family` 分为三类：

1. `alt_reclaim` / 山寨币结构回收类：包含 `snapback`、`spring`、`sweep-reclaim` / `SWR`。该类面向高流动性、高波动山寨币的短线结构回收机会，不使用投资/基本面逻辑，必须有明确止损、时间止损和持仓生命周期约束。
2. `core_ladder` / 核心资产阶梯类：当前包含 `CAL`。该类面向核心资产永续合约，使用显式白名单、分批 ladder、独立 lot TP 和策略本金上限，不复用山寨币结构回收类的候选池、止损或持仓时间语义。
3. `momentum_ignition` / 点火动量类：当前第一阶段策略为 `Ignition` / `IGN` observer。该类面向放量启动后站稳并二次抬升的强势结构，不抢第一根启动；第一阶段复用 hub 产出的已闭合 1m HBs，只做候选扫描、审计和告警，不做交易。

`alt_reclaim` 当前共用 `market_data_hub_runner.py` 作为 live 公共数据投喂服务。`market_data_hub_config.json` 的 `min_24h_quote_volume=30000000` 是 hub 侧第一层工程预过滤：hub 读取 Binance futures 24h ticker 的 `quoteVolume`，先筛出 24h 成交额不低于 3000 万 U 的候选 symbol，再为这些 symbol 构建 HBs payload 并发布 `candidate_inputs` / `finalized_candidate_inputs`。该过滤只决定是否构建策略输入 payload；策略最终信号仍由各自 `logic.py` 基于 C-anchor HBs payload 内的 per-symbol 24h 指标、排名和结构字段生成。

IGN 第一阶段新增语义基线：

```text
docs/Ignition项目语义基线.md
```

当前实现入口：

```text
strategies/ignition/config.observer.json
strategies/ignition/observer.py
```

实现边界：

1. IGN observer 只读取 `market_data_hub` shared `finalized_candidate_inputs`，不自行下单，不写策略持仓 state。
2. 每个 symbol 使用 `full_df` 最近 180 根已闭合 1m bar，计算 `0-30m / 30-60m / 60-120m / 120-180m` 四段收益、180m 总涨幅、最大回撤、贴近高点程度、大振幅 bar 数、大阴线数、30m 成交额放大倍数、分段低点抬高次数与 `structure_score`。
3. 第一版确认层 `IGN` 通过门槛显式写入 `strategies/ignition/config.observer.json`，默认要求 180m 总涨幅不低于 `25%`、至少 3 个分段上涨、最大回撤不高于 `18%`、距离 180m 高点回撤不高于 `8%`、30m 成交额放大倍数不低于 `1.5`、结构分不低于 `75`。
4. 2026-06-18 新增早期观察层 `IGN_EARLY`，默认要求 180m 总涨幅不低于 `12%` 且不高于 `18%`、最近 30m 涨幅不低于 `2%`、至少 2 个分段上涨、30m 成交额放大倍数不低于 `1.2`、最大回撤不高于 `22%`、距离 180m 高点回撤不高于 `16%`、最近 60m 大阴线不超过 4 根、至少 1 次分段低点抬高、结构分不低于 `60`。`IGN_EARLY` 只用于人工早期观察；若同一轮同一 symbol 已通过确认层 `IGN`，不重复发送早期层消息。
5. 2026-06-18 `IGN_EARLY` 增加同账户、同层级、同 symbol 的推送冷却，默认 `runtime.alert_cooldown_secs=1800`，避免同一候选每分钟重复推送；冷却状态落盘到 `state/live/ignition_observer_alerts.<account>.json`，仅在通知开启时写入。
6. 2026-06-19 新增 `IGN_BASE` 点火筑台子型：使用 1m `A-B-C` 结构，默认 `AB=60` 根、当前敏捷观察配置 `BC=3` 根；`AB` 只提供箱体高点与背景画像，不用涨跌幅/振幅一票否决；`B` 必须满足单根 1m 收盘涨幅不低于 `5%` 或 3 连阳总涨幅不低于 `8%`，且点火收盘突破 `AB_box_high`；`BC` 使用确认期收盘价下沿，要求守住点火涨幅的默认 `90%` 以上。`IGN_BASE` 只推送观察消息，不交易，bot 标题使用独立图标 `🚀 [IGN_BASE]`。
7. observer 每次扫描落盘 stage audit `ignition_observer`，输出 `top_candidates`、`top_early_candidates`、`top_base_candidates`、`rejected_summary`、`early_rejected_summary`、`base_rejected_summary`、推送计数与冷却抑制计数；bot 推送默认关闭，只在显式 `--notify` 或配置开启时推送通过候选。
8. 2026-06-20 IGN observer stdout 已降噪：普通无新推送扫描 summary 降为 `DEBUG`，`runtime.summary_log_interval_secs=600` 控制低频 `INFO` heartbeat；产生新的 `IGN` / `IGN_EARLY` / `IGN_BASE` 推送或单次非 loop 扫描时仍即时写 `INFO`。扫描与 stage audit 落盘频率不变。
9. 2026-06-20 `IGN_BASE` bot 消息改为复盘坐标格式：标题不再展示 `account/scan_id`，改为展示 `sig=HH:MM`；候选行展示 `A/B/C` bar 时间、`ABhi` 价格及其所在 bar 时间、`Cfloor`，避免用 Telegram 发送时间倒推结构。
10. 2026-06-20 `IGN` 与 `IGN_EARLY` bot 消息同步去掉标题中的 `account/scan_id`，改为展示 `sig=HH:MM`；完整 `account/scan_id` 仍保留在 `ignition_observer` audit 记录中。
11. 2026-06-21 `IGN_BASE` 推送去重从 `layer:symbol` 冷却改为结构身份永久去重：`IGN_BASE:{symbol}:{mode}:{ignition_start_bar_ts}:{ignition_end_bar_ts}:{bc_end_bar_ts}`。同一组 `A/B/C` 结构只推送一次，避免旧结构在 30 分钟冷却结束后重复推送；`IGN` / `IGN_EARLY` 仍保留按 `layer:symbol` 的冷却语义。
12. 2026-06-23 `IGN_BASE` Telegram 推送增加新鲜度门槛：只有 `bc_end_bar_ts == latest_closed_bar_ts` 的结构才进入推送与 alert 去重；180m 窗口中仍成立但 `C` 已经不是最新闭合 bar 的历史结构仅保留在 audit/top_base_candidates，不再补推，避免类似 `C=08:10` 到 `sig=10:07` 的滞后提醒。

### 1.5 Core Anchor Ladder 当前设计现场

当前新增核心资产阶梯策略语义基线：

```text
docs/Core-Anchor-Ladder项目语义基线.md
```

已确认语义：

1. CAL 面向黄金、原油、优质美股映射合约等核心资产，例如 `XAUUSDT`、`CLUSDT`、`BZUSDT`、`MUUSDT`、`NVDAUSDT`、`GOOGLUSDT`。
2. CAL 与 Snapback / Spring / SWR 山寨币短周期结构策略分离，不复用其结构语义、止损或持仓时间语义。
3. `P0` 是外部手动底仓，可来自 Binance App / Web 或管理员门户手动 LONG 入口；CAL 不管理、不止盈、不平仓、不写入策略 state。
4. `P1/P2/P3` 是 CAL 自动策略 lot，必须通过策略专属 client order id 与本地 lot state 区分。
5. 每个账户每个 symbol 同一时间最多一个 active ladder；无 active `P1` 时用 `data.h_anchor_lookback_hours` 配置的最近 N 根 1h contract bars 最高价 `H` 触发 `P1`，允许包含当前未闭合 1h bar；当前实盘配置为 `24`，即 24H 高点。
6. `P1` 建立后，`P2/P3` 锚定 `P1.entry_price`，不再使用最新 `H`；同一时刻每个 level 最多一个 active lot，但 `P2/P3` TP 关闭后，只要 `P1` 仍 active 且价格再次满足 trigger，允许重复回补同一 level；只有 `P1` 已关闭且当前 ladder 全部策略 lot 关闭后，才允许按当前配置重新计算最新 `H` 开启下一轮 `P1`。
7. 每个策略 lot 独立 TP，TP 价格为 `entry_price * (1 + take_profit_pct)`；同一 ladder 内必须满足 `P3.tp_price < P2.tp_price < P1.tp_price`。
8. 所有 entry / TP 必须 maker-only，当前 Binance USD-M 对应 `LIMIT + GTX`。
9. CAL 不绑定每分钟开头运行，第一版按 `collection.interval_secs=10` 高频轮询；前提是核心资产白名单很小，通常只监控 1-2 个 symbol。`H` 锚点默认每 60 秒刷新一次，由 `data.h_anchor_refresh_secs` 显式配置；H 的 1h bar 回看根数由 `data.h_anchor_lookback_hours` 显式配置；每 10 秒循环只刷新盘口、账户事实、本地 state 与触发判断。
10. 第一阶段不做历史 backtest 参数准入，参数由用户基于核心资产基本面和个人经验显式配置；但必须先做 dry-run / audit-only。
11. 若 TP 单调关系异常、`P1` TP 已成交但 `P2/P3` 仍未关闭、TP 丢失/被撤/终态异常、lot state 无法归属等 invariant violation 出现，策略进入 `PAUSED_BY_INVARIANT_VIOLATION`：进程必须持续运行并继续 reconcile / audit / bot CRITICAL，但禁止任何新 BUY 和新 ladder。

当前第一阶段已进入代码并推进到小账户 live smoke：

```text
strategies/cal/config.decision_audit.json
strategies/cal/config.live_trader.stark21.json
strategies/cal/decision_audit.py
strategies/cal/live_trader.py
```

实现边界：

1. `config.decision_audit.json` 当前账户为 `stark21`，交易 symbol 为 `MUUSDT` 与 `SKHYNIXUSDT`。
2. `MUUSDT` ladder 参数为 `P1 drop_pct=0.02 / 10U`、`P2 drop_pct=0.01 / 12U`、`P3 drop_pct=0.025 / 15U`，TP 为 `0.03`；`SKHYNIXUSDT` 显式覆盖为 `P1 drop_pct=0.02 / 15U`、`P2 drop_pct=0.01 / 20U`、`P3 drop_pct=0.025 / 25U`，TP 为 `0.01`；两品种 `max_symbol_strategy_notional_usdt` 分别为 `MUUSDT=37U`、`SKHYNIXUSDT=60U`，`max_total_strategy_notional_usdt=97U`。
3. 执行杠杆显式配置为 `25`，position mode 为 `HEDGE`，margin type 为 `CROSSED`。
4. 配置默认 `collection.interval_secs=10`，不绑定每分钟开头。
5. `H` 使用 `data.h_anchor_lookback_hours` 配置的最近 N 根 1h contract bars 的最高价，并允许包含当前未闭合 1h bar；当前实盘配置为 24H。
6. `H` 默认每 60 秒刷新一次，缓存路径为 `state/live_audit/cal/decision/h_anchor_cache.json`，刷新间隔由 `data.h_anchor_refresh_secs` 显式配置；10 秒循环内只刷新盘口、账户 position / open orders、本地 CAL state 与触发判断。
7. 账户事实读取 LONG position 与 symbol open orders；外部 LONG position 记为估算 `P0`，不写入 CAL state。
8. 若 entry size 因交易所 step size、min qty 或 min notional 归一化后无效，live trader 自动暂停对应 symbol，记录日志并推送 bot；进程继续运行。
9. 新增 `live_trader.py`，配置入口为 `config.live_trader.stark21.json`，显式 `allow_live_order=true`。
10. live trader 每轮先 reconcile pending entry / open lots，再构建 decision；真实下单只走公共 BN_EXEC/Gateway 的 `LIMIT + GTX`。
11. entry 因 maker-only 约束挂单失败、`EXPIRED` 或 `REJECTED` 时，live trader 会重读 best bid 并继续重试，直到交易所接受 maker entry；非 maker 约束类错误仍记录并中断本次 entry。
12. `POST_ONLY` BUY entry 若部分成交，不撤剩余单，不提前挂部分 TP；继续等待整张 entry 完全成交后再建立策略 lot 与 TP。完全未成交且超过 TTL 的 entry 可撤销并清理 pending。
13. 信号、entry submitted、open/TP submitted、exit 均写 audit / stdout log，并推送 bot 消息；BN_EXEC 仍负责交易所执行层通知。
14. 非 CAL open order、CAL state 外的 CAL client order id、TP 单调关系异常、P2/P3 无 P1、策略 lot 数量大于交易所 LONG position 等都会阻断新 intent 或进入 paused/invariant 路径。
15. CAL live entry gating 只阻断同 symbol 的 pending entry 或同 level 重复 entry；已有 `P1` open 不得阻断 `P2/P3`，已有 `P2` open 不得阻断 `P3`。
16. 本地最小验证已完成：`py_compile` 通过，配置加载通过，mock 决策验证 P1 ready 与 H anchor cache 命中行为通过；live mock 验证 entry pending 写入、部分成交不撤单不提前挂 TP、maker reject 后重读 best bid 并重试成功。
17. 2026-06-10 服务器 `stark21` CAL 进程已启动并完成 `MUUSDT` 与 `SKHYNIXUSDT` P1 smoke：SIGNAL、ENTRY maker、OPEN/TP maker 均有 stdout log 与 bot 推送；state 显示 `MUUSDT` P1 open lot entry `909.94`、TP `937.23`，`SKHYNIXUSDT` P1 open lot entry `1351.1`、TP `1364.61`。本轮新增 stdout 降噪：普通 10 秒空循环不再每轮输出 `loop finished`，只按 `logging.summary_interval_secs=3600` 输出 summary；signal / entry / open / exit / exception 仍即时输出。公共 BN_EXEC 识别新增 `CAL` client order id，避免 CAL 订单显示为 `BN`；CAL 策略侧日志、bot 标题与 BN_EXEC 消息统一显示 `⚓ CAL`。
18. 2026-06-10 修复 live entry gating 后，服务器 `SKHYNIXUSDT` P2 已成功进入真实下单路径：第一次 `POST_ONLY` maker reject 后重读 best bid 重试成功，P2 open lot entry `1331.78`、TP `1345.09`，state 显示 pending entry 为 `None`。
19. 2026-06-10 将 `MUUSDT` 默认 ladder drop 调整为 `0.02/0.01/0.025` 后，服务器已同步并重启 CAL；新配置生效，`MUUSDT` P2 已触发并 open，entry `890.15`、TP `916.85`。同轮观察到 `SKHYNIXUSDT` P3 触发、open 后 TP 离场。
20. 2026-06-10 修复 CAL H anchor 整点刷新边界：当 Binance 在整点后短时间未返回当前未闭合 1h bar 时，decision audit 会向前多取 1 根，并使用最近可用的 48 根连续 1h bar 计算 H，避免 `47 < 48` 导致 live loop 异常。
21. 2026-06-10 新增 `chen912` 与 `junjie2026` 两个 CAL live 配置，均只交易 `SKHYNIXUSDT`，ladder drop 为 `0.03/0.05/0.12`，TP 为 `0.02`，杠杆 `25`。`chen912` 三档 notional 为 `3000/3000/4000U`，策略本金上限 `10000U`；`junjie2026` 三档 notional 为 `250/250/333U`，策略本金上限 `833U`。
22. 2026-06-11 修复 CAL open lot reconcile 顺序：先查询每个策略 lot 的 TP 订单并关闭已 `FILLED` 的 lot，再用剩余 open lots 与交易所 LONG position 做 `position_qty_below_cal_open_lot_qty` invariant 检查。该修复覆盖 `chen912` / `junjie2026` 的 `SKHYNIXUSDT` P2 已 TP 成交但 P1 仍持有时被误暂停的问题；仅当该误暂停原因已恢复一致时自动解除对应 symbol 暂停。
23. 2026-06-12 CAL 配置 schema 将杠杆从全局 `execution.leverage` 改为按品种显式 `execution.symbol_leverage`。`chen912` 与 `junjie2026` 移除 `SKHYNIXUSDT` 后续新扫描，新增 `MUUSDT` 与 `SPCXUSDT`：两品种 ladder drop 均为 `0.05/0.05/0.12`，TP 均为 `0.025`，杠杆分别为 `MUUSDT=25`、`SPCXUSDT=20`；`chen912` 三档 notional 为 `6000/6000/6000U`，`junjie2026` 为 `600/600/600U`。旧 `SKHYNIXUSDT` 持仓与 TP 挂单不由新配置主动撤销，用户可手动处理。
24. 2026-06-13 管理员门户 `/view_history` 现场发现 `junjie2026` 的 `orders/trades/income` 已同步到 6 月 12/13 日，但 `positions` 只停在 6 月 9 日；根因是 `exchange_history_sync` 派生仓位时遇到历史边界成交 `NVDAUSDT trade_id=7389742`：本地仅追踪到 `1.76` LONG open qty，却收到 `SELL LONG 6.14`。本轮修复为：可匹配部分正常写 `CLOSED`，超额平仓部分写 `INCOMPLETE / close_qty_exceeds_open_qty`，并继续派生后续仓位；该修复只影响管理员门户历史账本，不改变任何策略下单逻辑。
25. 2026-06-15 审计 `chen912` `/view_history` 中 `SPCXUSDT LONG close_qty_exceeds_open_qty`：Binance API 事实显示本地缺失 `2026-06-13 03:14:10` 的 CAL 开仓成交 `trade_id=24824298/24824299`，对应 `order_id=137816499`、数量 `19.31+17.04=36.35`；本地只记录了 `2026-06-15 06:26:06` TP 平仓 `trade_id=28871488`。根因不是旧边界仓位，而是传统金融映射品种 maker 零手续费开仓没有 realized PnL/commission income，日常 `income-first active symbols` 未发现该 symbol；后续再发现 SPCXUSDT 时 per-symbol 起点被 24h 默认窗口截断，漏掉中间开仓。本轮修复 `exchange_history_sync`：每轮同步 symbol 集合扩展为 `income active ∪ current non-zero positions ∪ current open orders ∪ explicit symbols`，但不全量扫描 historical symbols；已有成功 cursor 的 source/symbol 从 cursor overlap 继续，不再被默认 24h 窗口截断；`positions` 改为从当前 trades 全量重建派生视图；当本轮同步触达的 symbol 仍产生 `INCOMPLETE` 时，从 `exchange_history_start_time` 对该 symbol 做一次定向 orders/trades 回补并重新派生 positions。
26. 2026-06-15 管理员门户交易消息可读性优化：`BN_EXEC` 交易事件首行新增状态图标，`✅` 表示成功、`❌` 表示失败、`🔵` 表示其他/中性；`/trade` 与 `/hedge_short` 的多账户命令汇总回复也按行加同一套图标，便于从多条成交/撤单/失败消息中快速扫读结果。
27. 2026-06-17 CAL 新增 P2+ 重复止盈下移语义：同一 `P1` active ladder 内，`P2/P3` 每次 `TAKE_PROFIT` 后递增该 level 的 `repeat_counts`；后续触发价使用 `effective_drop_pct = base drop_pct + P2..当前level 的 repeat_count * repeat_drop_step_pct`，因此 `P2` 重复止盈会下移 `P2/P3`，`P3` 重复止盈会下移 `P3`。当前三份 CAL decision config 均显式配置 `repeat_drop_step_pct=0.01`，即每次重复止盈后触发回撤增加 1 个百分点；`P1` 关闭且 ladder 清空后 `repeat_counts` 清零。
28. 2026-06-17 Snapback / Spring / Sweep-Reclaim 三套 live 策略开仓金额下调：`chen912` 从 `10000U` 调整为 `7500U`，`junjie2026` 从 `833U` 调整为 `650U`。Snapback 修改 `entry_notional_usdt`；Spring 与 Sweep-Reclaim 修改 `base_order_notional_usdt`，并同步调整 live execution 持仓 notional guard：`chen912=6750~8250U`、`junjie2026=550~750U`。`stark21` smoke 参数保持不变。
29. 2026-06-18 `chen912` API key 恢复后，只恢复 `CAL` live 进程，不恢复 `snapback_chen912` / `spring_chen912` / `sweep_reclaim_chen912`。`chen912` CAL 仍交易 `MUUSDT` 与 `SPCXUSDT`，drop 为 `0.05/0.05/0.12`，TP 为 `0.025`，杠杆分别为 `MUUSDT=25`、`SPCXUSDT=20`；三档 notional 从 `6000/6000/6000U` 调低为 `600/600/600U`，单 symbol 策略本金上限为 `1800U`，总策略本金上限为 `3600U`。`process_monitor_config.json` 同步改为期待 `chen912` 只运行 CAL，三套山寨币策略 chen912 进程期待数量为 0。
30. 2026-06-18 修复 `exchange_history_sync` 的 orders 终态刷新盲区：本地 orders 账本中仍为 `NEW/PARTIALLY_FILLED` 的订单会让该 symbol 的 orders 查询起点回拨到这些订单的创建时间，直到 Binance `allOrders` 返回 `FILLED/CANCELED/EXPIRED/REJECTED` 等终态并通过 order_id upsert 覆盖旧记录。该修复覆盖 CAL TP 挂单创建较早、成交较晚时 `/view_history` 历史委托漏显示 `平多`，但仓位历史因 trades 已完整而正常的场景；不改变任何策略下单逻辑。
31. 2026-06-18 管理员门户 `/trade close` / 交互式 close / SL 的旧 exit 撤单逻辑从“同账户同品种同方向同类型全部撤销”改为“容量冲突才撤销”：当已有 LONG SELL exit 剩余数量 + 新 exit 数量不超过当前 LONG position qty 时，旧单保留并与新单共存；只有超过仓位容量时才按手动单、外部单、策略单的优先级撤掉足够释放容量的旧单。该修复避免手动管理 bot 手动仓位时误撤 CAL 独立 TP。
32. 2026-06-18 管理员门户可读性优化：`/trade pending` 与账户详情挂单列表不再展示 `oid`，每条挂单前增加来源图标（`🦅` Snapback、`🌱` Spring、`📈` SWR、`⚓` CAL、`🧰` bot 手动、`🟨` Binance 官方/外部）；`/trade` 与 `BN_EXEC` 的 Telegram 可见交易输出不再展示 `oid`，`cid` 压缩为 `MAN_ENT` / `CAL_TP` 这类语义片段，完整 `cid/oid` 仍保留在执行日志中用于审计。
33. 2026-06-18 CAL 的 P1 `H` 锚点回看窗口从固定代码语义改为显式 `data.h_anchor_lookback_hours` 配置，当前三份 CAL decision config 均设为 `24`；`chen912` / `junjie2026` 的默认 ladder `P1.drop_pct` 从 `0.05` 调整为 `0.04`，`P2/P3` 仍为 `0.05/0.12` 且继续锚定 `P1.entry_price`。
34. 2026-06-19 `stark21` 的策略常驻进程已按生产运维要求停止：`spring_stark21`、`sweep_reclaim_stark21`、`cal_stark21` 当前不运行；服务器未发现 `snapback_stark21` 常驻进程。`process_monitor_config.json` 同步改为不再期待 `snapback_stark21_highfreq`、`spring_stark21`、`sweep_reclaim_stark21` 进程，三者 `min_count/max_count` 均为 `0` 且移除 heartbeat stale 检查。
35. 2026-06-23 CAL 放宽同品种非 CAL open order precheck：非 CAL `BUY/LONG` open order 视为外部 `P0` 入场，不阻断 CAL；非 CAL `SELL/LONG` open order 只在剩余卖出数量不超过估算外部 `P0` 数量时允许共存，若会吃到 CAL open lots、方向无法分类或为 `closePosition`，仍阻断新 CAL BUY。`chen912` 与 `junjie2026` 的 MUUSDT 暂时保留在 CAL 配置中用于 reconcile，但通过 `ladder.symbol_levels.MUUSDT` 将 P1/P2/P3 新开仓金额降为 `50/50/50U`；MUUSDT 单 symbol 策略本金上限设为 `700U`，用于覆盖历史 600U P1 与后续 50U P2/P3；SPCXUSDT 保持 `600/600/600U`。
36. 2026-06-25 `chen912` 与 `junjie2026` 的 CAL `MUUSDT` ladder override 从临时 `50/50/50U` 恢复为 `600/600/600U`，`MUUSDT` 单 symbol 策略本金上限恢复为 `1800U`，总策略本金上限恢复为 `3600U`；`SPCXUSDT` 参数不变。

当前下一步：

```text
CAL live trader 已在服务器常驻运行；继续观察 `chen912`、`junjie2026` 的核心资产 ladder 触发、TP 成交、重启恢复，以及交易所最小下单粒度对新增核心资产参数的影响。`stark21` 当前策略进程已停用；`chen912` 当前仅恢复 CAL，snapback / spring / SWR 暂不运行。IGN 当前进入 observer 第一阶段，下一步应先接入生产常驻扫描并积累 1-2 天候选审计，再讨论交易化入口、硬止损和持仓时间。
```

### 1.6 2026-05-23 三策略 sim/live 一致性审计闭环

本轮围绕 `sweep-reclaim` / `spring-sabc` / `snapback-sabc` 的 mybwin139 重叠窗口完成复跑收尾。服务器执行环境为 `/root/bn_research_core`，审计窗口保持与前轮一致：

```text
2026-05-11 13:30:00+08:00 ~ 2026-05-22 13:00:00+08:00
```

新回测 run id：

```text
SWR_SmokeTest_V1_0523T2039
Spring_SmokeTest_V1_0523T2041
Snapback_SmokeTest_0523T2041
```

对齐结果：

```text
SWR:
- sim signals = 70
- live signals = 91
- matched = 70
- sim_only = 0
- live_only = 21

Spring:
- sim signals = 86
- live signals = 99
- matched = 84
- sim_only = 2
- live_only = 15

Snapback:
- sim signals = 150
- live signals = 157
- matched = 146
- sim_only = 4
- live_only = 11
```

已闭合结论：

1. 本地 1m / idx 数据完整性已在服务器做全量审计与修复；`STARUSDT` 缺口修复后，Snapback `STARUSDT 2026-05-15 16:36` 已在新 sim 中恢复并与 live 对齐。
2. Spring 两个结构差异样本已由 feeder precision patch 解决：`STORJUSDT 2026-05-16 08:55` 与 `PROVEUSDT 2026-05-21 16:00` 均在新 sim 中出现并与 live 对齐。
3. `STARUSDT -4028 Leverage 5 is not valid` 不作为待修代码项；live 遇到该类交易所能力限制时跳过交易并保留审计记录。
4. `UNKNOWN_EXIT` / 外部平仓归因增强暂不改变下单或平仓行为，后续若推进，应按偏审计的 `ARCH_ONLY` 路线增强 attribution detail。

剩余差异归因：

1. SWR `live_only=21` 中 20 笔在 sim decision audit 内归因为 `cooldown_active`，1 笔 `MLNUSDT` 无本地 sim row，属于 delisted / 本地数据尾部不可补事实，不构成新策略语义 patch。
2. Spring `live_only=15` 中 11 笔在 sim decision audit 内归因为 `cooldown_active`，1 笔为 `baseline_window_insufficient_bars`，3 笔无本地 sim row（`MLNUSDT` 两笔、`PROVEUSDT 2026-05-22 13:11` 一笔）；`sim_only=2` 分别为 live 侧 `cooldown_active` 与 `score_not_in_top_n`。这批差异来自 live 生命周期/本地数据窗口/当时 ranking 输入边界，不再归为 Spring 结构逻辑差异。
3. Snapback 新 sim 对齐了已修复的 `STARUSDT 16:36`；剩余差异集中在 live 执行状态、交易所能力跳过、delisted 无本地尾部、以及 signal-only live 文件无法直接解释的少量候选边界。当前不新增策略逻辑 patch。

当前状态：

```text
本轮三策略 sim/live 一致性审计的代码修复项已收口。
后续若继续推进，应优先作为审计可观测性增强，而不是改交易语义。
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
docs/SNAPBACK_SIM_LIVE_AUDIT_SPEC.md
docs/Sweep-Reclaim项目语义基线.md
docs/TVR项目语义基线.md
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
strategies/sweep_reclaim/config.json
strategies/tvr/config.data_hub.json
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
6. shared bars 增量 refill 与 24h metric 预取能力已推进。
7. 增加 finalize quality stats 与 hub health stats，可用于现场观察。
8. 增加 Binance REST quota / ban window / API stats 相关保护与观测。
9. `market_data_hub` 的 candidate / finalized / market snapshot 已按账户与北京时间日期落盘到 `state/live_audit/market_data_hub/{account}/daily/YYYY-MM-DD/`，当前无需纳入 live audit 主文件分片 patch。
10. 2026-05-08 已确认 hub-owned 全市场 1m rollsum 无法在当前每分钟 API/工程约束下收敛为全市场同一 C-anchor 事实；继续使用它会让 Snapback 新扫描长期 blocked，并污染 Spring/SWR 候选可见性。
11. 2026-05-08 已将 live 主数据路径改回稳定模型：`data_hub` 候选初筛使用 Binance futures 24h ticker 的 `quoteVolume` 与 `min_24h_quote_volume=30000000`，只决定是否构建 HBs payload；策略 logic.py 消费的 per-symbol 24h 指标、排名和结构字段仍来自同一 C-anchor HBs payload。
12. Snapback live `market_total_24h_vol` 改为直接使用 Binance futures 24h ticker API 汇总，作为 live-only 市场总量 gate；该字段不再承诺 sim/live 严格一致，后续一致性审计应标记为 live-source 例外。

当前配置事实：

```text
market_data_hub_config.json:
- enabled = true
- min_24h_quote_volume = 30000000
- history_window_mins = 180
- exclude_symbols 已显式配置，包含大市值币与 TradFi 品种
```

当前 pending：

1. 继续观察是否长期无 `-1003` / ban window 风险。
2. 部署 2026-05-08 live ticker patch 后，需要重启 data_hub 与三套策略 live，再观察 Binance REST quota 30 轮统计是否回到升级前稳定区间，并确认 data_hub 不再出现 492/529 HBs 候选扩张。
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
9. `klines_1m_store.py` 默认交易所 universe 已从“排除 USDC”修正为仅接受 `quoteAsset=USDT` 且 symbol 以 `USDT` 结尾；手工 symbols、force include、confirmed delisted 与本地 shard 目录发现非 USDT symbol 时 fail-fast。

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
9. snapback sim 配置新增显式 `risk_controls.base_order_notional_usdt`，sim 信号与交易流水不再依赖后处理默认 100U。
10. 2026-04-29 已完成 `Snapback_SmokeTest_0429T2229` 与 mybwin139 live 重叠窗口一致性审计：sim 16 笔信号全部在 live 中按 `(symbol, c_time)` 匹配；2 笔 live-only（`IRUSDT 15:48 C`、`DAMUSDT 15:49 C`）已确认为 17:00 BJ 交易所 delist 前后的已解释样本，不继续追查。
11. live audit 主事件与 stage audit 已改为按北京时间日期分片落盘：`state/live_audit/snapback_{account}.YYYY-MM-DD.jsonl`、`state/live_audit/{strategy}_{account}.YYYY-MM-DD.jsonl`、`state/live_audit/stage_audit/snapback_{account}.{stage}.YYYY-MM-DD.jsonl`，便于后续按日期做 retention 清理。
12. 2026-04-30 已完成 `c8d8689 live: wait snapback finalized payload anchor` 部署后验证：北京时间 15:00 后三账户共 9 笔 snapback live 信号（`mybwin139` 5 笔、`junjie2026` 2 笔、`chen912` 2 笔）全部满足 `bar_bj = c_bar_bj + 1min`，未再出现旧问题中的 `C+2m` 消费；9 笔均为 `candidate_payload_wait_ok=true`，且 `expected_latest_closed_bar_ts / expected_signal_time_ts` 与 finalized payload 实际 anchor 匹配。当前观测表明该 patch 已把 live 消费约束回正确的 finalized payload anchor；实际 `signal_detected / entry_submitted` 仍发生在 `bar_bj` 后约 33-42 秒，这是等待 hub finalize 完成后的预期时序，不是旧的一分钟漂移。
13. 2026-04-30 已定位 RAVEUSDT 17:59 BJ 在 `junjie2026` / `chen912` 的 live 离场未输出问题：交易所 truth 显示两账户 `RAVEUSDT` LONG position 已归零、open orders 为空、TP order 为 `FILLED`、SL algo order 为 `EXPIRED`，但本地 `state/live/snapback_{account}.state.json` 仍保留 `symbols.RAVEUSDT.open_trade.status=OPEN`。根因是 `run_live._run_once()` 在 `market_total_24h_vol < market_total_24h_vol_min` 时早退，早退点位于 `build_consumer_reconcile_plan()` / `prepare_consumer_loop_gate()` / open_trade reconcile 之前；18:31 后两账户每分钟只记录 `market_total_24h_vol_below_min_skip`，导致已有仓位 TP/SL 同步被阻断。已将该 gate 改为“reconcile 后阻断新扫描”：低于市场总量阈值时仍先维护已有 pending/open trade，再跳过新信号扫描。
14. 2026-04-30 已复盘 `HUSDT 18:15 BJ` 在 `junjie2026` / `chen912` 的 `SL fail -> TIME_STOP` 表象：两账户 entry 后提交 SL 均被 Binance 拒绝 `APIError(code=-2021): Order would immediately trigger.`，随后代码进入 `entry_sl_fail_flatten` 风险分支。现场价格对比：C bar=`2026-04-30 18:14 BJ`，C close=`0.17015`，`C+1m` open=`0.17016`，signal current_price=`0.17015`，resolved SL=`0.16837356`，C close 到 SL 仍有约 `1.06%` 空间；但实际 entry 发生在 `18:15:37 BJ` 左右，`junjie2026` entry=`0.1681068`、`chen912` entry=`0.1679185`，分别已低于 SL 约 `0.16%` / `0.27%`，因此 SL submit 必然被交易所视为立即触发风险。已完成两步修补：一是新增 live pre-entry price guard，在提交市价 entry 前即时读取并落盘 `pre_entry_price`，价格源与 SL `workingType=CONTRACT_PRICE` 对齐；若 `(pre_entry_price - resolved_sl_price) / resolved_sl_price` 小于显式配置阈值，则跳过 entry 并写 audit/stage 记录。二是将 SL 提交失败后的应急平仓从正常 `TIME_STOP` 语义中拆出：client order id leg 使用 `SNP_SF`，BN exec / audit / live trade reason 使用 `SL_SUBMIT_FAILED_FLATTEN`，同时保留原 `time_stop_*` 订单身份字段用于既有 reconcile 查询，避免已有状态机路径失联。
15. 2026-05-01 已优化 Snapback live Telegram bot 消息的可读格式，不改变交易语义、下单顺序、state/audit 字段或执行风控。消息统一为多行头 `[HH:MM:SS 🦅 SNP] {account}`，其中时间取具体业务事件时间：signal 使用 `signal_time`，ENTRY/SL/TP 使用交易所订单创建/更新时间，离场使用 `trade_row.exit_time`。雷达、ENTRY、SL、TP、开仓、离场消息均保留原核心字段，但拆为多行展示；Snapback 使用 `SNP`，公共 BN_EXEC 通知会从 client order id 识别 `SNP`/`SPR`，并在第二行追加 `【BN_EXEC】` 标记以区分公共执行层消息与策略侧消息。
16. 2026-05-06 已增强 Snapback live stage2 universe 审计可观测性：`contract_24h_metric_empty` 等 24h metric 缺口不再只落归一化 fail reason，stage2 现在同时记录 `metric_frame_present`、`metric_frame_empty`、`metric_frame_rows`、`metric_frame_min/max_ts/bj`、`metric_frame_contains_c_bar`、`contract_metric_reason` 与 `contract_metric_prefetch_error`。该 patch 不改变 universe 过滤语义，仅用于后续 sim/live 一致性审计还原 per-symbol 合约 K 线 metric prefetch / frame 构建失败原因。
17. 2026-05-06 已增强 Snapback live `finalized full_df -> candidate_cross_section -> stage4/stage5` 审计链：finalize summary 增加 `full_df_only_symbol_count/full_df_only_symbols`；当 finalize refresh 的 `full_df` 已有 symbol 但 `cross_section` 缺 symbol 时，写 `c_bar_finalize_cross_section_missing` event；进入策略逻辑前若仍存在 `full_df` 有、`cross_section` 无的 symbol，写 `candidate_cross_section_missing_after_finalize` event，并在 `stage4_input_snapshot` 与 `stage5_structure_audit` 中为该 symbol 落 `input_pass_to_logic=false`、`fail_reason=missing_from_cross_section_after_finalize`、C bar full_df 快照、candidate error/stale reason 与 finalize passed/delayed 标记。该 patch 不改变 Snapback 信号或交易语义，只用于避免 AIOTUSDT 这类 sim 有信号、live 无 stage4/stage5 现场时无法定位缺席边界。
18. 2026-05-06 已将 data hub `finalized_candidate_inputs` 的 candidate finalize probe 间隔从 2 秒加固为 3 秒，仍保持两轮快照一致视为闭合、deadline 为 `signal_time+50s`。原因是 SKYAIUSDT 暴露出 `B=C` 同 bar 时 `c_index_low/b_index_price` 伪闭合会直接改变 `current_price > b_index_price`、`rebound_ratio` 与 SL，影响策略事实；本刀先用最小方式降低 index low 伪闭合概率，后续若继续复现再升级到三次一致或 near-deadline 关键字段复核。
19. 2026-05-06 新增 `docs/SNAPBACK_SIM_LIVE_AUDIT_SPEC.md` 作为 Snapback sim/live 一致性审计长期规格文档。后续新线程做 Snapback sim/live 审计时，应按该文档统一审计窗口、输入文件、signal/ABC 匹配、硬字段与降权字段、live-only/sim-only 分类、stage audit 定位顺序与 live trading lifecycle 检查口径；其中 `c_index_close` 默认记录但降权，`c_index_low/b_index_price` 在 `B=C` 同 bar 时为核心硬字段。
20. 2026-05-08 Snapback 的 `market_total_24h_vol` gate 改为“生命周期优先”：低于市场总量阈值时，live 不再在 reconcile / finalized payload / loop gate 之前早退，而是先等待当前 C finalized payload、维护 pending/open/reconcile 生命周期，然后只阻断新扫描。
21. 2026-05-08 Snapback live `market_total_24h_vol` 改为 Binance futures 24h ticker API 汇总；该字段是 live-only 市场总量 gate，不再作为 sim/live 严格一致审计字段。
22. 2026-05-08 Snapback live 新增显式 `precheck_scope` 与 `strategy_concurrency_scope`，对齐 Spring/SWR 的 live 并发语义。当前 live config 使用 `symbol + symbol`：其它策略在同账户其它 symbol 的持仓/挂单不再阻断 Snapback startup 或整轮 scan；这些交易所活动仍作为 active symbols 传入策略，防止同 symbol 重复交易。若将 `precheck_scope` 改为 `account_flat`，则保留账户级空仓/无挂单才允许启动、扫描和下单的保守语义。

当前配置事实：

```text
strategies/snapback/config.highfreq.json:
- runtime.max_history_window_mins = 150
- universe.24h_quote_volume_min = 30000000
- universe.market_total_24h_vol_min = 20000000000
- structure.s_to_c_window.mins = 60
- structure.election_rule = drop_pct_top1
- exit_policy.time_stop.max_hold_mins = 4
- risk_controls.base_order_notional_usdt = 100
live_config.*.json:
- pre_entry_min_sl_distance_pct = 0.003
- precheck_scope = symbol
- strategy_concurrency_scope = symbol
- `pre_entry_min_sl_distance_pct` 属于 live 执行风控配置，已由 `run_live.py` / `run_consumer.py` 的 live config loader 校验；`core/config_loader.py` 仅校验策略语义配置，不承接该字段。
- `precheck_scope` 表达交易所下单前检查范围（`symbol` / `account_flat`）；`strategy_concurrency_scope` 表达 Snapback 策略自身并发约束（`symbol` / `account`）。
```

当前 pending：

1. 持续做 snapback sim/live 一致性验证。
2. `Snapback_SmokeTest_0429T2229` 的 4 笔历史 C 点 `close_idx / basis_c_pct` 偏差审计已形成结论：`IRUSDT 2026-04-29 04:42 C` 与 `LYNUSDT 2026-04-29 16:37 C` 是 candidate 初始 index 快照即与事后 Binance 历史值不同，finalize round 1 连续两次相同后毕业；`AIOTUSDT 2026-04-29 07:32 C` 与 `BROCCOLI714USDT 2026-04-29 20:06 C` 在 finalize probe 中发生过改写，但最终毕业值仍与事后 Binance 历史值不同。4 笔均确认 candidate/finalize 阶段为 index cache miss，即当时重新请求了 Binance `/fapi/v1/indexPriceKlines`；当前交易所历史值与本地 parquet / sim 输入一致，不与 live 当时 hub 值一致。结论：snapback 结构逻辑与 `klines_1m` 不是第一嫌疑，偏差来自 hub 对 index C bar 的工程近似判定，即连续两次 index 快照相同就视为 finalized；Binance API 当前没有直接提供“index C bar 已最终稳定”的确定事实。现阶段不改逻辑，继续跟踪该类早期/未稳定 index 快照复现概率。
3. 继续明确 snapback sim `base_order_notional_usdt` 与 live `entry_notional_usdt` 的账户资金口径关系。
4. 是否为 bn truth 增加条件委托 / algo 父单独立真相层，尚未决定。
5. triplet audit 是否显式解释父单 ID 与基础子单 ID 差异，尚未决定。
6. 部署 2026-05-08 live ticker 与 Snapback symbol-scope gate patch 后，需要重点验证 Snapback live 新扫描恢复；`market_total_24h_vol` 后续只审计为 live-source gate，不再要求与 sim 严格一致。
7. 部署 live pre-entry SL distance guard 与 `SL_SUBMIT_FAILED_FLATTEN` 独立离场语义后，观察是否出现 `pre_entry_price_guard_skip`；若仍发生 SL submit fail，应确认落盘 reason / custom id / 通知不再混入正常 `TIME_STOP`。

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
9. Spring live 侧启动第一刀架构边界：新增公共 LONG-only live execution intent contract，并新增 Spring signal -> execution intent adapter。
10. 新增 Spring projection-only live runner：只读取 hub finalized candidate inputs，调用 Spring sim 同源逻辑，校验 execution intent 并落盘观察 projection；不触交易所、不下单。
11. Spring projection-only live runner 增加正式 loop 模式：支持按分钟边界运行、限制迭代次数、写 heartbeat；仍然不触交易所、不下单、不维护订单生命周期。
12. 新增 `strategies/spring/config.live_loose.json`，仅用于 projection-only 链路压测和尽快覆盖 signal -> execution intent 路径；不得作为 Spring 策略基线或绩效结论。
13. 新增公共 dry-run execution plan：`core/live/execution_plan.py` 消费 LONG-only execution intent，产出 orphan/local/exchange precheck、quantity、client order id、SL/TP/time-stop plan 与 state transition plan；不调用交易所、不写 live state。
14. Spring live runner 支持可选只读 exchange verified dry-run：`--dry-run-verify-exchange` 会读取交易所 positions/open orders 与本地 live state，用于验证 orphan/precheck；仍然不下单、不写 live state。
15. 新增公共 live execution runner：`core/live/execution_runner.py` 消费已验证 LONG intent + execution plan + 外部 live execution JSON；显式执行 entry MARKET、SL-first、TP-after-SL、state/audit/cooldown，SL 提交失败时按配置提交 market flatten。
16. Spring runner 增加显式 `--execute-live` + `--live-execution-config` 一次性实盘入口；默认仍不下单，且 `--execute-live` 当前只支持 once 模式，不支持 loop 常驻。
17. 新增 `strategies/spring/config.live_smoke_10u.json` 与 `strategies/spring/live_execution.smoke_10u.json`，用于 10U 小仓位实盘 smoke；所有实盘执行参数从 JSON 读取，代码不内置 10U、杠杆、重试、冷却等测试参数。
18. 2026-04-29 15:05 BJ，已在阿里云 `mybwin139` 执行一次 Spring 10U / 5x 实盘 smoke：`SKYAIUSDT` entry 成交，Spring `SPR_SL` 与 `SPR_TP` 保护单建立成功。
19. 2026-04-29 15:09-15:10 BJ，确认 Spring smoke 发生跨策略串线 incident：Snapback live 捕获并维护了 Spring open_trade，取消 Spring `SPR_TP/SPR_SL`，提交 Snapback `SNP_TS` time-stop 并完成离场。
20. 2026-04-29 已提交、推送并部署 Spring/Snapback live state ownership 隔离 patch：`11d1b22 live: isolate strategy state ownership`。
21. 2026-04-29 20:17 BJ，重启 3 个 Snapback live 进程后执行 Spring 10U / 5x live smoke：`SKYAIUSDT` entry 成交，`SPR_SL` 与 `SPR_TP` 提交成功；随后交易所真相显示 `SPR_TP` 立即成交、`SPR_SL` 自动 EXPIRED，仓位与挂单为空。未复现 Snapback 串线，但暴露 Spring state 缺少 post-entry reconcile / exit monitor，Spring state 仍记录 `OPEN`。
22. 2026-04-29 已提交、推送并部署 Spring live once 公共 post-entry reconcile patch：`d56d5b9 live: reconcile spring post-entry exits`。`core/live/execution_runner.py` 在 entry/SL/TP 建立后立即查询 LONG position 与 symbol open orders；若交易所仓位和挂单均为空，则查询 TP/SL/TS 订单事实，推断 exit reason，写 Spring audit event，并清理 strategy-specific `open_trade`。
23. 2026-04-29 21:10 BJ，服务器 `/root/bn_research_core` 已拉取 `d56d5b9` 并用 `/root/service_env/bin/python` 完成 py_compile。随后用新 post-entry reconcile 逻辑处理 20:17 Spring smoke 残留：交易所 position/open orders 为空，`SPR_TP` 为 `FILLED`、`SPR_SL` 为 `EXPIRED`，Spring audit 写入 `spring_position_closed_detected` 与 `spring_state_cleared_after_exit`，`state/live/spring_sabc_mybwin139.state.json` 中 `SKYAIUSDT.open_trade` 已清空。
24. 2026-04-29 21:11 BJ，Spring 10U smoke dry-run 仍有 `SKYAIUSDT` 信号，交易所 precheck 为空仓无挂单，但 local precheck 因 `cooldown_until_bj = 2026-04-30 00:17:00` 返回 `local_cooldown_active`，因此未继续执行新的实盘下单。
25. 2026-04-29 21:18 BJ，服务器启动 Spring 10U smoke watcher：`pid=4138786`，脚本 `output/live_projection/spring_smoke_live_watch.sh`，日志 `output/live_projection/spring_smoke_live_watch.20260429T131846Z.log`。watcher 每分钟先 dry-run，只有 `ok_to_execute=true` 才调用 `--execute-live`；当前首轮仍只有 `SKYAIUSDT` signal，因 `local_cooldown_active` 等待，未触发 live 下单。
26. 2026-04-29 21:29 BJ，watcher 发现 `AIOTUSDT` 可执行并完成 10U live smoke：ENTRY filled qty=101 avg=0.09517，SL submitted stop=0.09216，TP submitted price=0.10495。后续交易所事实显示 SL filled avg=0.0921878、TP expired；21:38 BJ 手动调用 post-entry reconcile 后，Spring state 清理，exit_reason=`STOP_LOSS`。
27. 2026-04-29 21:35 BJ，watcher 又发现 `TACUSDT` 可执行并完成 10U live smoke：ENTRY filled qty=612 avg=0.0163321，SL submitted stop=0.016236，TP submitted price=0.016418。后续交易所事实显示 TP filled avg=0.016418、SL expired；21:38 BJ 手动调用 post-entry reconcile 后，Spring state 清理，exit_reason=`TAKE_PROFIT`。
28. 2026-04-29 21:38 BJ，因 smoke 目标已覆盖且 watcher 已连续开出两笔真实交易，已停止临时 watcher `pid=4138786`。复查交易所 positions/open orders 为空；Spring state 中 `AIOTUSDT`、`TACUSDT`、`SKYAIUSDT` 均无 open_trade / pending_entry_order，仅保留各自 cooldown。
29. 2026-04-29 已提交、推送并部署 Spring live loop patch：`15eecc6 live: run spring execution loop`。该 patch 允许 `--loop --execute-live`，每轮先对 Spring state 全部 open_trade 做交易所事实 reconcile，再执行 signal scan；普通 precheck blocker 记录为 `execution_blocked_by_precheck`，不再让 loop 崩溃；新增账户级 Spring local active gate，防止其它 symbol stale/open state 时继续开下一笔。
30. 2026-04-29 21:59 BJ，服务器用 `/root/service_env/bin/python` 跑新版本 `--loop --execute-live --max-iterations 1` 验证通过：本轮 signal=`SKYAIUSDT`，`dry_run_execution_plan.ok_to_execute=false`，blocker=`local_cooldown_active`；`lifecycle_reconcile` 返回无剩余 open/pending，`account_local_precheck` 为空，`live_execution_result.outcome=execution_blocked_by_precheck`。交易所 positions/open orders 仍为空；Spring state 无 open_trade/pending_entry，仅保留 `AIOTUSDT/SKYAIUSDT/TACUSDT` cooldown。
31. 本地已推进 Spring active time-stop patch：`core/live/execution_runner.py` 的 loop reconcile 在 LONG position 仍存在时，会使用 Spring live runner 从 hub `full_df` 提取的最新闭合 C close 检查 `max_hold_mins / time_stop_min_profit_pct`；到期且收益不足时先撤 TP/SL，再提交 `SPR_TS` market flatten，并设置 `exit_submit_inflight`，后续仍由同一公共 reconcile 根据 TP/SL/TS 交易所事实清理 state。
32. 本地已推进 Spring open_trade bracket verify/repair patch：loop reconcile 发现 LONG position 仍 open 且未处于 `exit_submit_inflight` 时，会校验本策略 TP/SL 是否仍存在于交易所 open orders；若缺失则按 `open_trade.tp_price / sl_trigger_price` 与当前 position qty 补挂，补挂后再次查询 open orders 验证。补挂或验证失败会写 state error / audit 并 fail-fast 保留 open_trade，账户级 local active gate 会继续阻止新开仓。
33. 2026-05-01 文档 checkpoint：下一刀将把 Spring 的 SL submit failed emergency flatten 从普通 TIME_STOP 语义拆出，对齐 Snapback 的独立 protective flatten 语义。目标字段/事件：exit reason=`SL_SUBMIT_FAILED_FLATTEN`，custom id leg=`SPR_SF`，BN exec order_role=`SL_SUBMIT_FAILED_FLATTEN`，audit event=`spring_sl_submit_failed_flatten_submitted` / `spring_sl_submit_failed_flatten_filled`，并在后续 terminal projection 中保留 `protective_flatten_*` 字段。
34. 本地已推进 Spring SL submit failed protective flatten patch：SL 提交失败后的应急平仓使用 `SPR_SF` client id leg 与 `SL_SUBMIT_FAILED_FLATTEN` BN exec order_role；open_trade 同时写 `time_stop_exit_reason` 与 `protective_flatten_*` 字段；后续 reconcile 若该 flatten 订单成交，exit_reason 落为 `SL_SUBMIT_FAILED_FLATTEN` 并写 `spring_sl_submit_failed_flatten_filled` audit。
35. 2026-05-01 已按 Snapback Telegram 新标准补齐 Spring live 策略侧消息格式，不改变交易语义、下单顺序、state/audit 字段或执行风控。Spring 策略侧消息使用多行头 `[HH:MM:SS 🌱 SPR] {account}`；signal 使用 `signal_time`，开仓确认使用 entry 交易所订单事件时间，离场确认使用 exit 交易所订单事件时间。公共 BN_EXEC ENTRY/SL/TP 消息已由 `core/live/binance_exec.py` 从 `SPR` client order id 识别并在第二行追加 `【BN_EXEC】`。Spring live execution config 显式新增 `notify_enabled` / `notify_on_signal_locked` / `notify_on_order_submit` / `notify_on_exit_detected` / `notify_on_order_error`。
36. 2026-05-01 本地已推进 Spring live lifecycle 对齐 Snapback 基线 patch：`strategies/spring/run_live.py` 将 hub payload anchor 校验收紧为 `signal_time_ts == latest_closed_bar_ts + 60000`，锁死 Spring `CB=C+1m`；`core/live/execution_runner.py` 补齐 Spring pending entry terminal/recovery reconcile、flat 但仍有残余 open orders 时的 exit 推断与清理、TIME_STOP submit failed 后 bracket repair、TS inflight 终态但 LONG position 仍 open 时的 reset+repair、terminal exit 后 live trade projection 落盘与 exit cooldown 刷新。该 patch 仍保持 Spring 走公共 LONG-only lifecycle，不复制 Snapback 私有 consumer 架构。
37. 2026-05-01 本地已推进 Spring live 正式入口命名 patch：正式入口统一为 `strategies/spring/run_live.py`，旧过渡入口从源码树删除，不保留 wrapper、alias 或兼容路径；projection / heartbeat / run_id / row metadata 统一使用 `spring_live`、`spring_live_heartbeat`、`SPRINGLIVE` 与 `run_mode=live`；loose 压测配置改名为 `strategies/spring/config.live_loose.json`。后续 Spring live 审计从 `run_live.py` 与公共 `core/live/execution_runner.py` 开始。
38. 2026-05-01 本地已推进 Spring loop finalized payload anchor wait patch：`strategies/spring/run_live.py` 的 loop 不再按 `--hub-max-age-secs` 单次读取任意 fresh finalized payload；每轮从 scheduled signal check epoch 推导 `expected_signal_time_ts` 与 `expected_latest_closed_bar_ts`，按 1s 轮询等待 finalized payload 精确匹配当前 C anchor 且包含 `finalize_summary`，deadline 对齐 Snapback 为 `expected_signal_time_ts + 50s`。deadline 前未等到时，本轮写 `finalized_candidate_payload_not_ready` projection/heartbeat 并继续下一轮，不消费旧 payload、不下单。`--signal-check-second` 默认值从 2 改为 5，对齐 Snapback live 起始检查秒。
39. 2026-05-01 本地已推进 live 三段架构边界 patch：项目正式采用 `Live Data Gate -> Signal Generation -> Execution Lifecycle` 术语；新增 `core/live/live_data_gate.py` 承接信号生成前的公共 finalized payload anchor gate，Spring `strategies/spring/run_live.py` 改为复用该公共模块。`signal` 仍只表示策略计算后的信号结果，信号生成前的数据输入层统一称为 `Live Data Gate`，不得称为 `Signal Input`。
40. 2026-05-06 已补 Spring sim/live 决策审计排名字段：策略逻辑同源产出 `rank_chg_24h`、`rank_vol_24h`、`score_rank_all`、`selected_score_order`、`score_top_n`、`selected_for_structure`、`universe_hard_gate_pass`；sim `spring_decision_audit` 新增 `decision_scoreboard`，live `spring_live` projection 新增完整 `decision_audit` 与同口径 `decision_scoreboard`。`score` 是综合分数不是名次，历史 `score_order` 仅表示 topN 内顺序；后续 Spring sim/live 一致性审计应以 `score_rank_all` 判断全候选排名、以 `selected_score_order/selected_for_structure` 判断是否进入 structure 检查。
41. 2026-05-07 Spring smoke `Spring_SmokeTest_V1_0507T1944` 与 `mybwin139` live 重叠审计确认：11/11 信号按 `(symbol, signal_time)` 匹配，结构字段一致；此前看到的 `chg_24h / vol_24h / rank / score` 差异来自 sim signal 文件记录了 CB cross_section，而 live 严格使用 `C=HBs[0]` finalized payload。进一步对表显示 live 24h 指标逐笔匹配 sim decision audit 的 C 行 scoreboard，不匹配 sim CB 行指标。
42. 2026-05-07 已将公共语义明确为：所有策略的 `logic.py` / signal 生产层只能消费 HBs 数据，CB 数据只允许进入 signal 之后的执行撮合、entry price / pre-entry price 与最终 TP 解析。共享回测 runner 已修正 Spring/SWR 的策略逻辑投喂：`strategy.on_kline_close(signal_time=CB, cross_section=C)`，同时保留 CB cross_section 用于 sim 执行价注入和撮合。Snapback sim 已检查，其 logic 当前以 `current_time_ms=C` 运行并自行产出 `signal_time=C+1m`，本环节未发现同类 CB 投喂偏差。
43. 2026-05-08 已修正 Spring/SWR sim 回测起始边界：HBs 策略的共享 runner 会额外加载 `--start` 前 1 分钟的闭合 C bar，确保首根 `signal_time=CB` 也能读取 `latest_closed_bar=CB-1m`；实际时间步进仍严格从用户传入的 `--start` 开始，不多跑预读 bar。
44. 2026-06-17 Spring/SWR 共享 live execution lifecycle 修复交易所执行失败韧性：`hedge_mode` / `cross_margin` / `leverage` ensure 失败不再抛出进程级异常，而是记录 state error、写 execution audit、推送 bot、标记本 bar 已处理并返回本笔交易失败，常驻 loop 继续运行。Spring 与 SWR 全部 live execution 配置显式新增 `allow_leverage_max_downgrade=true`；当 Binance 拒绝配置杠杆且 leverage bracket 显示交易所最大杠杆低于配置值时，执行层允许降到交易所最大杠杆继续下单，并写 `spring_leverage_downgraded` / `sweep_reclaim_leverage_downgraded` audit 与 bot 消息。

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
- full_notional_risk_pct = 0.99
```

当前 pending：

1. 基于 `SPRING_V1_30D_P6_0427T1606_ALL` 作为结构毕业候选，重跑动态 sizing 后的正式 sim。
2. 继续审计 Spring-SABC 坏月份 / 坏 regime，尤其 2026-04。
3. 若再调整 Spring 结构过滤或 sizing 参数，必须同步评估审计工具是否需要扩展。
4. Spring/Snapback live state ownership 隔离 patch 已提交并部署，20:17 smoke 未复现 Snapback 接管。
5. Spring live loop patch 已部署并完成 1 轮服务器验证；后续如要继续 smoke，应优先使用内置 `--loop --execute-live`，不再启动临时外层 watcher。
6. Snapback live 不得维护、取消、离场或写入非 `SNP` 策略的 open_trade；Spring live 不得写入 Snapback state 文件。
7. 下一次 Spring 实盘 smoke 不应绕过 cooldown；若信号仍为 `SKYAIUSDT`，需等 `2026-04-30 00:17:00 BJ` 之后或等无 cooldown 的新标的信号。
8. 当前 Spring smoke watcher 已停止；正式常驻前必须使用 Python 内置 loop lifecycle，不再依赖临时外层 shell watcher。
9. 21:29 与 21:35 两笔 smoke 说明：live once 即时 post-entry reconcile 只能捕获“执行返回前已经离场”的情况；若 TP/SL 在返回后触发，需要后续 loop reconcile / exit monitor 清理。
10. 已补账户级 Spring local active gate 与每轮 open_trade reconcile；后续再评估是否继续补 time-stop 主动提交/撤单能力。

### 3.6 Sweep-Reclaim

已完成：

1. 新增 Sweep-Reclaim / SWR 策略语义基线：`docs/Sweep-Reclaim项目语义基线.md`。
2. 新增策略参数骨架：`strategies/sweep_reclaim/config.json`。
3. 新增策略包目录：`strategies/sweep_reclaim/`。
4. 新增 SWR sim 结构识别逻辑：`strategies/sweep_reclaim/logic.py`。
5. `core/config_loader.py` 已新增 `sweep-reclaim` fail-fast 配置校验。
6. `strategies/run_backtest.py` 已支持 `--strategy sweep-reclaim`，首版复用 Spring 强势 TopN decision audit 写法并输出 `sweep_reclaim_decision_audit.{run_id}.jsonl`。
7. `core/engine/broker.py` 已允许 SWR 使用 Spring 同形的 sim exit policy 段：`max_hold_mins` / `time_stop_min_profit_pct` / `breakeven_guard`。
8. `core/analysis/visualizer.py` 已将 SWR 复盘图文件名前缀改为 `SWR_`，Snapback 仍保持 `SNAP_`。
9. `strategies/schedule_backtests.py` 已支持 `--strategy sweep-reclaim` 全量并行调度，并在 post-merge 阶段合并 `sweep_reclaim_decision_audit.{run_id}.jsonl` 为 `sweep_reclaim_decision_audit.{runset}_ALL.jsonl`。
10. 本地已完成 SWR scheduler `--dry-run` 验证，能按 batch 生成 `strategies/run_backtest.py --strategy sweep-reclaim` 命令。
11. 2026-05-07 已新增 SWR live 侧代码入口：
    - `strategies/sweep_reclaim/live_execution.py`
    - `strategies/sweep_reclaim/run_live.py`
    - `strategies/sweep_reclaim/live_execution.smoke_10u.json`
12. 2026-05-07 公共 live execution intent 已支持 `risk_reward_r_multiple` / `take_profit_r_multiple`，用于表达 SWR 的 R 倍数止盈语义；Spring 原 `risk_reward_1r` / `fixed_pct` 语义保持不变。
13. 2026-05-08 SWR live 日志与消息推送统一使用 `📈 SWR` 作为可观察标识；公共 BN_EXEC 会从 `SWR` client order id 识别并展示同一符号。

当前语义事实：

```text
strategy_name = sweep-reclaim
strategy_code = SWR
LONG-only
1m contract bars only
C = HBs[0]
CB = C + 1
signal_time = entry_time = CB
H -> gamma -> B -> C -> CB
B = support_window 内最低 low，当前参数 support_window_mins = 180
H = B 左侧最高 close 点
hb_drop = (h_close - b_low) / h_close
bc_rebound = (c_close - b_low) / (h_close - b_low)
gamma = B - bars_bc
vol_climax = avg_quote_volume(gamma, C] / avg_quote_volume(H, gamma]
SL = b_close
TP = entry_price + risk_distance * take_profit_r_multiple
```

当前配置事实：

```text
strategies/sweep_reclaim/config.json:
- runtime.bar_interval = 1m
- runtime.max_history_window_mins = 300
- universe.min_24h_chg_pct = 30
- universe.min_24h_quote_volume = 50000000
- universe.score_top_n = 3
- structure.support_window_mins = 180
- structure.hb_drop.min = 0.06
- structure.rebound.bc_rebound_min = 0.2
- structure.rebound.bc_rebound_max = 0.4
- structure.rebound.hb_bars_min = 3
- structure.rebound.bc_bars_min = 1
- structure.rebound.bc_bars_max = 30
- structure.rebound.bc_over_hb_bars_max = 0.3
- structure.vol_climax.ratio_min = 3.0
- exit_policy.stop_loss_anchor = b_close
- exit_policy.take_profit_r_multiple = 1.0
```

当前 performance baseline：

```text
run_id = SWR_V1_30D_P6_0506T2125
strategy = sweep-reclaim
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

当前 pending：

1. `SWR_V1_30D_P6_0506T2125` 已作为当前 performance baseline。
2. SWR live 侧已完成代码入口与本地 intent / dry-run plan 小样本验证，但尚未在服务器读取 hub payload 跑真实 live projection dry-run。
3. SWR live 尚未启动真实下单；首次使用 `--execute-live` 前必须先做服务器 dry-run projection 与 exchange precheck 验证。
4. 后续若调整 SWR 参数，必须同步更新语义文档或明确为实验配置。

已确认 incident：

```text
2026-04-29 Spring/Snapback live state ownership 串线

现场事实：
- Spring 真实开仓：
  - symbol = SKYAIUSDT
  - entry_client_order_id = x-7Qv8Kw2S_SPR_EN_0429150516_95e6a6
  - sl_client_order_id = x-7Qv8Kw2S_SPR_SL_0429150516_95e6a6
  - tp_client_order_id = x-7Qv8Kw2S_SPR_TP_0429150516_95e6a6
  - Spring audit 文件 = state/live_audit/spring_sabc_mybwin139.jsonl
- Snapback live 后续事件：
  - 15:09:11 BJ: time_stop_cancel_tp_ok 取消 x-7Qv8Kw2S_SPR_TP_0429150516_95e6a6
  - 15:09:11 BJ: time_stop_cancel_sl_ok 取消 x-7Qv8Kw2S_SPR_SL_0429150516_95e6a6
  - 15:09:12 BJ: time_stop_submitted 提交 x-7Qv8Kw2S_SNP_TS_0429150516_95e6a6
  - 15:10:07 BJ: position_closed_detected 记录 exit_reason = TIME_STOP
- 复查时交易所 positions/open orders 均为空，state 中 SKYAIUSDT open_trade 已清空。

初步根因：
- core/live/live_state.py 当前文件名固定为 live/snapback_{account}.state.json。
- Spring execution_runner 复用 load_live_state/save_symbol_state/set_open_trade，导致 Spring open_trade 写入 Snapback state namespace。
- Snapback live maintain loop 读取同一个 state namespace，未按 strategy_code/client_order_id 策略归属过滤 open_trade，于是把 SPR open_trade 当作 Snapback 仓位维护。

影响：
- Spring 实盘 smoke 的离场由 Snapback live 执行和记录。
- 离场 time-stop client id 使用 SNP，而 entry/SL/TP 使用 SPR。
- 当前 2026-04-29 这笔 smoke 不再是纯 Spring live execution lifecycle 样本，只能作为跨策略串线 incident 样本。

incident 记录时的下一刀建议：
1. live_state namespace 必须 strategy-specific，例如 spring_sabc_{account}.state.json / snapback_{account}.state.json。
2. public live execution contract 必须显式携带 strategy_name/strategy_code/state_namespace。
3. Snapback maintain/reconcile 必须拒绝维护非 SNP open_trade/order_root/client_order_id。
4. Spring 在拥有独立 state 与 reconcile/exit monitor 前，不再执行新的实盘下单。
```

当前 ownership 修复状态：

```text
Patch 分类：LOGIC_ONLY

已提交并部署：
- commit = 11d1b22 live: isolate strategy state ownership
- core/live/live_state.py
  - live state 文件名改为 strategy-specific。
  - 默认 snapback 仍写 live/snapback_{account}.state.json。
  - Spring 显式写 live/spring_sabc_{account}.state.json。
- core/live/execution_runner.py
  - Spring live execution state 写入使用 intent.strategy_name。
  - open_trade / pending_entry payload 写入 strategy_name 与 strategy_code。
- strategies/spring/run_live.py
  - dry-run local_state_snapshot 读取 spring-sabc namespace。
- strategies/snapback/trade_consumer.py
  - Snapback pending/open_trade 写入 SNP 归属字段。
  - Snapback reconcile 发现非 SNP 或未知归属 payload 时阻断并写 audit event，不取消、不平仓、不接管。

本地验证：
- python3 -m py_compile core/live/live_state.py core/live/execution_runner.py strategies/spring/run_live.py strategies/snapback/trade_consumer.py
- live_state 临时目录写入验证：snapback_acct.state.json 与 spring_sabc_acct.state.json 分离。
- Snapback ownership helper 验证：SNP=true，SPR/spring-sabc/unknown/mixed=false。

服务器部署验证：
- 阿里云 `/root/bn_research_core` HEAD = 11d1b22。
- 3 个 Snapback live 进程已于 2026-04-29 20:14 BJ 重启并加载新代码。
- 服务器 py_compile 通过。
```

20:17 Spring live smoke 事实：

```text
run_id = SPRING_SMOKE_LIVE_0429T2017
symbol = SKYAIUSDT
signal_time_bj = 2026-04-29 20:17:00
order_root = 0429201742_c1b964
entry_client_order_id = x-7Qv8Kw2S_SPR_EN_0429201742_c1b964
sl_client_order_id = x-7Qv8Kw2S_SPR_SL_0429201742_c1b964
tp_client_order_id = x-7Qv8Kw2S_SPR_TP_0429201742_c1b964

交易所真相：
- ENTRY: FILLED, avg_price = 0.27252, qty = 37
- TP: FILLED, avg_price = 0.27277, qty = 37, realized_pnl = 0.00925
- SL: EXPIRED
- 复查 positions/open orders 均为空。

state / audit 结论：
- 20:17 smoke 后，state/live/spring_sabc_mybwin139.state.json 中 SKYAIUSDT open_trade.strategy_code = SPR，status = OPEN。
- 21:10 部署 `d56d5b9` 后，manual post-entry reconcile 已根据交易所 TP FILLED / SL EXPIRED 事实清空 Spring `open_trade`。
- state/live/snapback_mybwin139.state.json 中 SKYAIUSDT 无 open_trade。
- Snapback audit 未出现取消本次 SPR_SL/SPR_TP 或提交 SNP_TS 的记录。

该 smoke 暴露的历史风险：
- 当时 Spring one-shot execution runner 只补了一次即时 post-entry reconcile，尚无循环式 Spring reconcile / exit monitor。
- 后续 Spring live loop、active time-stop、bracket repair、protective flatten 与 lifecycle 对齐 patch 已把这些能力补到公共 LONG-only runner；现阶段仍需要用服务器 live smoke / projection 继续确认真实交易所路径。
```

当前 Spring live 架构事实：

```text
总体架构边界：
- Snapback 当前仍是老结构：信号识别、下单、持仓维护、reconcile、离场落盘集中在策略自己的 live/consumer 代码中。
- 新 live 结构正式分四段：`Live Data Gate -> Signal Gate -> Strategy Signal Logic -> Execution Lifecycle`。
- Spring live 正在走新结构：`Live Data Gate`、`Signal Gate` 与 `Execution Lifecycle` 逐步沉到公共 LONG-only live 模块；策略层只负责 `Strategy Signal Logic` 与 signal -> ValidatedLiveExecutionIntent adapter。
- `core/live/execution_intent.py` 只是公共 contract 入口，不承载全部生命周期逻辑。
- `core/live/live_data_gate.py` 承载信号生成前的公共 live 数据门禁：expected C / signal_time 推导、hub finalized payload anchor wait、deadline / stale payload 防护。
- `core/live/signal_gate.py` 承载策略信号逻辑前的公共 live signal gate：汇总命令行 active symbols、本策略 pending/open symbols 与本策略 cooldown symbols；cooldown 默认按 `strategy_name + account + symbol` 维度隔离。
- 公共 live execution lifecycle 的完整目标是：
  signal adapter -> ValidatedLiveExecutionIntent -> execution_plan -> entry/SL/TP -> strategy-specific state/audit -> open_trade reconcile -> TP/SL/TS exit_reason -> state close -> live_trades/projection -> cooldown。
- 未来第三、第四套 LONG 策略应只新增自己的 strategy signal logic / signal adapter / strategy_name / strategy_code / config，复用公共 Live Data Gate、Signal Gate 与 Execution Lifecycle；不得复制 Snapback 老式策略私有 live 闭环。
- 后续新增 Spring live 生命周期能力应继续补在公共 LONG-only live execution lifecycle 中，而不是写成 Spring 私有闭环。
- Snapback 若未来迁移到公共层，必须单独拆刀；当前不得在 Spring 修复刀中混改 Snapback 架构。

core/live/execution_intent.py:
- 定义 ValidatedLiveExecutionIntent
- 只允许 LONG
- fail-fast 校验 strategy/account/symbol/time/price/notional/SL/TP/hold/time-stop/signal_snapshot

core/live/live_data_gate.py:
- 定义公共 Live Data Gate 边界
- 提供 `expected_snapshot_from_signal_check_epoch(...)`
- 提供 `wait_finalized_candidate_inputs_for_snapshot(...)`
- 要求 finalized payload 精确匹配本轮 `expected_latest_closed_bar_ts / expected_signal_time_ts`
- 要求 payload 包含 `finalize_summary`
- deadline 默认对齐 Snapback：`expected_signal_time_ts + 50s`
- deadline 未等到时返回 not_ready diagnostics，由策略 runner 写 projection/heartbeat 并跳过本轮交易

core/live/execution_plan.py:
- 定义 dry-run execution plan
- 输入 ValidatedLiveExecutionIntent
- 输出 local/exchange precheck、quantity、client order ids、entry/SL/TP/time-stop plan、state transition plan
- exchange snapshot 未提供时标记 exchange_precheck_not_verified，计划不可执行但仍可审计
- 不调用 Binance、不写 live state

core/live/execution_runner.py:
- 定义配置驱动 live execution runner
- 输入 ValidatedLiveExecutionIntent + execution plan + 外部 live execution JSON
- 要求 execution config 与 intent 的 strategy/account/side/mode 对齐
- 要求 local/exchange precheck verified，支持 account_flat 级别 orphan 阻断
- 调用 Binance 执行 entry MARKET、SL STOP_MARKET closePosition、TP LIMIT
- 入场后先建 SL，SL 成功后才建 TP
- SL 建立失败时按 JSON 中 `stop_loss_failure_action=submit_market_flatten` 提交 market flatten
- 写 live state pending/open_trade/cooldown/error 与 live audit event
- 当前覆盖 entry、保护单建立、live once 入场后的即时 post-entry reconcile
- 本地 loop patch 新增 strategy-level open_trade reconcile 与 account local active precheck：每轮可清理已由 TP/SL/TS 离场的 stale open_trade；若仍有 open/pending，则阻断新的 live entry
- 本地 active time-stop patch 新增到期检查：使用最新闭合 C close 计算收益；若 `held_mins >= max_hold_mins` 且收益低于 `time_stop_min_profit_pct`，先取消 TP/SL，再提交 TS market flatten，并等待后续 reconcile 清理 state
- 本地 bracket verify/repair patch 新增持仓保护单维护：position 仍 open 且未处于 `exit_submit_inflight` 时，校验 TP/SL open order 绑定；缺失则按 open_trade 记录补挂，补挂后再次验证；失败写 error/audit 并保留 open_trade
- 本地 SL submit failed protective flatten patch 新增入场保护失败独立离场语义：SL 提交失败后的应急 market flatten 不再复用 `SPR_TS` / `TIME_STOP`，改用 `SPR_SF` / `SL_SUBMIT_FAILED_FLATTEN`，并写 `protective_flatten_client_order_id`、`protective_flatten_exchange_order_id`、`protective_flatten_exit_reason`
- 本地 finalized payload anchor wait patch 新增 loop 侧当前轮 payload 等待：每轮按 scheduled signal check epoch 推导 expected C / signal_time，等待 hub finalized payload 精确匹配后才允许 signal scan / execution；超时记录 `finalized_candidate_payload_not_ready` 并进入下一轮，不消费旧 payload。
- 对照 Snapback live，Spring 公共 lifecycle 本地已补齐 pending entry terminal/recovery reconcile、time-stop submit failed 后保护单修复、inflight TS 终态但 position 仍 open 的修复、terminal exit 的 live trade projection 专用落盘与 exit cooldown 刷新；下一步需要用本地最小测试与后续服务器 live smoke / projection 继续确认真实交易所路径。

core/live/audit_log.py:
- 保留既有 snapback audit 写入入口
- 新增 strategy-specific audit 写入入口，Spring live execution event 写入 `spring_sabc_{account}.jsonl`

strategies/spring/live_execution.py:
- 定义 SPRING_LIVE_STRATEGY_CODE = SPR
- 将 Spring-SABC signal 显式转换为公共 execution intent
- 要求 signal.action = BUY
- 使用 signal.position_notional_usdt 作为 live 下单名义金额来源

strategies/spring/run_live.py:
- Spring live runner 入口
- 通过 `core/live/live_data_gate.py` 等待并读取当前轮 shared hub finalized_candidate_inputs
- 调用 SpringSABCStrategy.on_kline_close(...)
- signal 存在时生成并校验公共 execution intent
- signal 存在时生成 dry_run_execution_plan 并落盘
- 支持 `--dry-run-verify-exchange` 读取只读交易所快照与本地 live state 快照
- 支持显式 `--execute-live --live-execution-config ...` 真实下单
- 写入 output/live_projection/spring_live.{run_id}.jsonl
- 支持 `--loop`、`--execute-live`、`--max-iterations`、`--signal-check-second`
- loop 模式下等待当前轮 expected finalized payload，deadline=`signal_time+50s`；超时只写 not_ready projection/heartbeat，不交易
- 写入 output/live_projection/spring_live_heartbeat.{run_id}.json
- 默认不下单；只有 `--execute-live`、外部 live execution JSON、account local gate 与 exchange/local precheck 同时满足时才会触发真实交易

strategies/spring/config.live_loose.json:
- projection-only 专用 loose 配置
- 主用途是放宽 universe/structure 门槛，尽快产生 signal 样本以验证 execution intent 落盘路径
- 不得用于正式 sim/live 策略基线判断

strategies/spring/config.live_smoke_10u.json:
- 小仓位实盘 smoke 专用 Spring 策略配置
- 继承 loose signal 门槛以尽快出信号
- 2026-06-06 起 stark21 smoke 实盘开仓金额调整为 `base_order_notional_usdt = 20`；文件名中的 `10u` 仅保留历史命名
- 不得作为 Spring 策略基线或绩效结论

strategies/spring/live_execution.smoke_10u.json:
- 小仓位实盘 smoke 专用 live execution contract
- `execution_mode = live_once`
- `allow_live_order = true`
- `precheck_scope = symbol`
- `strategy_concurrency_scope = symbol`
- `pre_entry_min_sl_distance_pct = 0.003`
- `min_position_notional_usdt = 16`
- `max_position_notional_usdt = 24.0`
- `leverage = 5`
- 要求 local/exchange/symbol filters 均 verified
- 同日 Snapback stark21 `live_config.highfreq.json` 的 `entry_notional_usdt` 与 SWR stark21 `config_smoke_10u.json` / `live_execution.smoke_10u.json` 同步调整为 20U 口径。
```

### 3.7 TVR / TradFi Value Reclaim

当前定位：

```text
TVR 是 Binance USD-M TradFi 永续合约的 LONG-only live-first 策略路线。
它不复用山寨币三策略的结构语义，第一阶段已建设 data_hub、decision audit 与小资金 maker-only live trader。
当前已经确认后续 TVR 工程分三件事：完整历史数据落盘、percentile reclaim backtest、按分位触发修正实盘入场逻辑。
```

已完成：

1. 新增 `docs/TVR项目语义基线.md`，明确 TVR 是 TradFi Value Reclaim，面向黄金、白银、原油等 TradFi 映射合约。
2. 明确 TVR 第一阶段不做传统 sim，先做 live-first data_hub，以 live facts 和历史价格统计校准后续入场参数。
3. 新增 `strategies/tvr/config.data_hub.json` 与 `strategies/tvr/data_hub.py`。
4. TVR data_hub 第一版只采集并落盘事实，不下单、不写 live state、不改现有三套策略语义。
5. 当前事实流包括：
   - 当前 TVR live 白名单 universe snapshot：先由 Binance futures exchangeInfo 识别 TradFi 合约，再按 `config.data_hub.json` 的显式 `universe.symbols` 白名单过滤，只记录 live 当前事实。
   - 当前 funding snapshot：来自 `/fapi/v1/premiumIndex`，用于后续入场门禁事实。
   - 历史 funding bootstrap：来自 `/fapi/v1/fundingRate`，只用于研究审计，不作为 live 入场依赖。
   - rolling 24h stats：来自 TVR 自有增量 price history raw store，按 decision window 计算 `min/max/mean/median/p1/p5/p10/p20/p50/latest`。
   - 后续 research history store：用于保存所有 TradFi 品种上市以来完整 1m contract kline，服务 TVR backtest。
6. TVR data_hub 落盘路径为 `state/live_audit/tvr/data_hub/{stream}/YYYY-MM-DD/tradfi_{stream}.jsonl`。
7. 2026-05-08 22:44 BJ，本地已用真实 Binance 连接跑通一次最小采集：
   `python3 strategies/tvr/data_hub.py --once --skip-price-history-sync --skip-funding-history-bootstrap`。
   本轮确认 `contractType=TRADIFI_PERPETUAL` + `underlyingSubType=TradFi` 可识别 34 个 TradFi 合约，并写入 universe / funding / price_24h 三类 snapshot。
8. 新增 `strategies/tvr/config.decision_audit.json` 与 `strategies/tvr/decision_audit.py`：
   - 仅读取 TVR data_hub 最新 facts。
   - 校验 data_hub 新鲜度、`data_scope=global`、producer、symbol 覆盖和字段可读性。
   - 应用 JSON `tradable_symbols` 白名单、低频缓存的 `entry_percentile` 阈值、实时 24h ticker、`funding_rate_entry_max` 和配置化 notional cap。
   - 只落盘 audit-only `POST_ONLY_MAKER_*` intent，不提交 Binance 订单。
9. TVR decision audit 落盘路径为 `state/live_audit/tvr/decision/YYYY-MM-DD/tvr_decision_audit.jsonl`。
10. 新增 `strategies/tvr/config.live_trader.smoke_10u.json` 与 `strategies/tvr/live_trader.py`：
   - 每轮内部调用 `decision_audit` 同源构建函数读取全局 data_hub facts，生成本账户 selected intents。
   - 由同一个 live_trader 进程按原 decision audit 路径落盘审计；实盘常驻不再依赖单独 decision_audit 进程。
   - 显式 `allow_live_order=true` 才允许真实下单。
   - entry/TP 提交已统一改走 `core/live/binance_exec.py` 的 BN_EXEC 执行通知路径，TVR 不再直接绕过公共 Binance execution 入口调用 `futures_create_order`。
   - 入场触发后读取实时盘口 best bid，使用 `LIMIT + GTX` post-only maker BUY。
   - 若 post-only 因盘口下移而直接 `EXPIRED/REJECTED`，在 entry attempt window 内继续读取 best bid 并重试。
   - pending entry 若出现 `PARTIALLY_FILLED`，先按显式 `partial_fill_wait_secs` 等待继续成交；等待到期或 entry TTL 到期后撤剩余挂单，并按已成交数量挂 TP。
   - 入场成交后使用 `LIMIT + GTX` post-only maker SELL 挂止盈。
   - open trade reconcile 会查询 TP 订单与 LONG position；TP 成交时清理 state 并输出 EXIT，position 已关闭但 TP 未成交时按外部 `POSITION_CLOSED` 清理并取消孤儿 TP，position 仍存在但 TP 查询失败/终态未成交时 fail-fast 并推送 CRITICAL。open trade state 已记录 `opened_utc_ms/opened_bj`，其来源是 entry 成交后 TP 提交完成并建立 open_trade 的时间，不是 entry 挂单创建时间；EXIT 时会写 `closed_utc_ms/closed_bj` 并计算 `holding_ms/holding_minutes/holding_text`。
   - TVR 策略侧新增 OPEN / EXIT / CRITICAL bot 输出，BN_EXEC 侧继续负责 ENTRY/TP/CANCEL 等交易所执行通知。
   - TVR live 在真实入场尝试前新增策略侧“雷达锁定”日志与 bot 输出，字段包括 symbol、entry_percentile、实时 24h return、分位阈值、预估入场价/数量、开仓金额、TP 与 decision run id。
   - TVR live stdout 已降噪：普通 `open_trade_wait`、`pending_entry_wait`、`entry_skipped_local_active_symbol` 只落 audit；stdout 只打印真实动作、异常或按显式配置的周期 heartbeat。
   - TVR live 在已有 pending/open 时会先过滤 active symbols；若账户已有 TVR active，则按 `active_decision_interval_secs` 对新增候选 decision 降频，避免持仓期间每 2 秒重复请求 24h ticker。
   - 第一版不设置价格止损，不做 SHORT，不做对冲，不自动加仓。
   - 本地 state / 交易所 symbol 维度有 pending、position 或 open orders 时阻断新 entry。
11. TVR live trader 落盘路径为 `state/live_audit/tvr/live/YYYY-MM-DD/tvr_live_trader.jsonl`，live state 路径为 `state/live/tvr_{account}.state.json`。
12. 新增 `strategies/tvr/config.history_backfill.json` 与 `strategies/tvr/history_backfill.py`：
   - 读取当前 TradFi exchangeInfo universe，使用 `onboardDate` 作为每个 symbol 的历史起点。
   - 通过 Binance REST Gateway 低优先级调用 `futures_klines`，按 symbol cursor 断点续传。
   - 写入 research history store：`state/research/tvr/klines_1m/{symbol}/{YYYY-MM}.parquet`。
   - 写入 cursor：`state/research/tvr/history_backfill_state.json`。
   - 写入 audit：`state/research/tvr/audit/history_backfill/YYYY-MM-DD/tvr_history_backfill.jsonl`。
13. 新增 `strategies/tvr/config.percentile_backtest.json` 与 `strategies/tvr/percentile_tp_backtest.py`：
   - 只读取 TVR research history store，不访问 Binance API。
   - 每个时刻只用该时刻之前的 rolling 24h return 样本计算 `p1/p5/p10/p20/p50`，避免未来函数。
   - 触发条件为 `current_24h_return <= selected_percentile_return`。
   - 第一版 entry price 使用当前 1m close，TP 用后续 K 线 high 是否触达判断。
   - 同一 symbol 在一笔样本 TP 或 max_hold 结束前不重复入场。
   - 输出 summary/sample CSV 与 JSONL audit。

当前配置事实：

```text
strategies/tvr/config.data_hub.json:
- data_scope = global
- gateway_account = mybwin139
- universe.underlying_subtype = TradFi
- universe.quote_asset = USDT
- universe.contract_type = TRADIFI_PERPETUAL
- universe.status = TRADING
- universe.symbols = CLUSDT, BZUSDT, XAUUSDT, XAGUSDT
- collection.interval_secs = 60
- funding_history.lookback_days = 90
- price_history.interval = 1m
- price_history.decision_window_days = 30
- price_history.minimum_history_days = 14
- price_history.initial_sync_lookback_days = 30
- price_history.stable_lag_minutes = 3
- price_history.rolling_window_hours = 24
- price_history.archive_enabled = true
- price_history.archive_after_days = 30
- decision_audit.data_hub.max_age_secs = 300
- decision_audit.universe.tradable_symbols = CLUSDT, BZUSDT, XAUUSDT, XAGUSDT
- decision_audit.collection.interval_secs = 2
- decision_audit.decision.entry_percentile = p50
- decision_audit.decision.percentile_refresh_secs = 3600
- decision_audit.decision.funding_rate_entry_max = 0.0001
- decision_audit.decision.take_profit_pct = 0.005
- decision_audit.risk.symbol_notional_usdt = CLUSDT:10, BZUSDT:10, XAUUSDT:10, XAGUSDT:10
- decision_audit.risk.max_symbol_notional_usdt = CLUSDT:10, BZUSDT:10, XAUUSDT:10, XAGUSDT:10
- decision_audit.risk.max_total_notional_usdt = 40
- live_trader.allow_live_order = true
- live_trader.decision_audit.config_path = strategies/tvr/config.decision_audit.json
- live_trader.collection.interval_secs = 2
- live_trader.collection.active_decision_interval_secs = 300
- live_trader.logging.summary_interval_secs = 3600
- live_trader.execution.symbol_notional_usdt = CLUSDT:10, BZUSDT:10, XAUUSDT:10, XAGUSDT:10
- live_trader.execution.max_entry_notional_usdt = CLUSDT:15, BZUSDT:15, XAUUSDT:15, XAGUSDT:15
- live_trader.execution.max_symbol_entry_notional_usdt = CLUSDT:30, BZUSDT:30, XAUUSDT:30, XAGUSDT:30
- live_trader.execution.max_open_trades = 4
- live_trader.execution.leverage = 10
- live_trader.execution.post_only_time_in_force = GTX
- live_trader.execution.entry_price_mode = BEST_BID
- live_trader.execution.entry_best_bid_offset_ticks = 0
- live_trader.execution.entry_attempt_window_secs = 30
- live_trader.execution.entry_retry_sleep_secs = 0.5
- live_trader.execution.entry_max_attempts = 30
- live_trader.execution.entry_order_ttl_secs = 120
- live_trader.execution.partial_fill_wait_secs = 6
- live_trader.execution.take_profit_pct = 0.005
- live_trader.recovery.enabled = true
- live_trader.recovery.anchor = HIGHEST_OPEN_ENTRY
- live_trader.recovery.grid_step_pct = 0.01
- live_trader.recovery.min_spacing_hours = 6
- history_backfill.history.interval = 1m
- history_backfill.history.kline_limit = 1500
- history_backfill.history.max_requests_per_run = 120
- history_backfill.history.max_batches_per_symbol_per_run = 1
- history_backfill.history.request_sleep_secs = 0.1
- percentile_backtest.history_store.root = research/tvr/klines_1m
- percentile_backtest.backtest.lookback_days = 30
- percentile_backtest.backtest.minimum_history_days = 14
- percentile_backtest.backtest.entry_percentiles = p1, p5, p10, p20, p50
- percentile_backtest.backtest.take_profit_pcts = 0.005, 0.01
- percentile_backtest.backtest.max_hold_hours = 336
```

当前边界：

```text
TVR data_hub 可以复用 Binance REST client、Binance REST Gateway、REST quota guard、北京时间转换、JSONL audit 落盘和自有 parquet price history raw store。
TVR 第一阶段不得复用现有 market_data_hub 的 HBs/finalized payload 语义。
TVR decision audit 不查询 Binance 账户、不接执行层、不写交易 live state。
TVR data_hub 只采集 JSON `universe.symbols` 显式白名单；TVR decision audit 的可交易品种由 JSON `tradable_symbols` 显式白名单决定，且必须被 data_hub universe 覆盖。
TVR live trader 是第一版小资金实盘 smoke 执行层，只支持 maker-only entry/TP，不设置价格止损。
TVR live trader 的 Binance 下单请求必须走公共 BN_EXEC/Gateway 路径；策略侧只保留 decision、maker retry、state reconcile 与生命周期通知。
TVR 后续真实交易端必须继续遵守 LONG-only、maker-only、funding_rate_entry_max 与账户级限仓边界。
TVR 当前 live 逻辑使用 current_24h_return <= selected_percentile_return 触发入场，触发后按实时盘口 best bid 侧 maker 入场。
TVR 下一阶段生产目标已固定为“全局唯一 data_hub + 每账户一个 live_trader”：data_hub 只落公共 facts，不归属交易账户；live_trader 后续应内嵌本账户 decision 构建；账户差异只来自显式配置，尤其 per-symbol notional。
TVR recovery ladder 已进入实盘代码路径，当前 smoke 配置已显式开启 `recovery.enabled=true`，`grid_step_pct=0.01`，`min_spacing_hours=6`。live 会先用同一 decision gate 判断 active symbol，并只在满足 funding、24h return、最高价未平 lot 锚点、`grid_step_pct`、`min_spacing_hours` 与 `max_symbol_entry_notional_usdt` 后提交 `RECOVERY` maker entry；每个 lot 独立 TP、独立 reconcile，不再用单个 `open_trade` 判断多 lot 退出。
```

三刀计划：

```text
第一刀 ARCH_ONLY:
- data_hub 改为全局公共 facts source。
- decision_audit 不再要求 data_hub account 等于交易账户。
- 不合并 decision/live，不改开仓金额。

第二刀 ARCH_ONLY:
- live_trader 每轮直接读取全局 data_hub facts 并构建本账户 intent。
- decision_audit.py 保留为审计/debug 工具，实盘常驻不再依赖单独 decision 进程。

第三刀 LOGIC_ONLY:
- fixed order_notional_usdt 改为 per-symbol notional。
- 同步 TVR live 配置为 p10 + TP 0.5%。
```

当前 pending：

1. 第一刀已进入代码：TVR research history store / history backfill 已新增，下一步需要服务器 dry-run 后启动补齐，对所有 TradFi 品种尽量补齐上市以来完整 1m contract kline。
2. 第二刀已进入代码：TVR percentile reclaim backtest 已新增，下一步需要在服务器 research history store 有足够历史数据后运行并审计结果。
3. 第三刀已进入代码：TVR decision_audit / live_trader 实盘入场逻辑已改为 `current_24h_return <= selected_percentile_return`，触发后按实时盘口 best bid 侧 `BUY LIMIT GTX`，并支持 post-only 失败重定价重试。
4. 后续可在服务器常驻启动 TVR data_hub loop；price history sync 在 loop 中只维护当前 live 白名单 symbol，并按 cursor state 增量补齐，不应每次启动重拉完整决策窗口。
5. 若 Binance exchangeInfo 的 TradFi 分类字段继续变化，先以落盘事实修正 TVR universe 识别语义，不得硬编码品种兜底。
6. TVR live trader 可先用 `--dry-run` 在服务器读取全局 data_hub facts 并构建本账户 decision 验证，再用小资金账户启动 `config.live_trader.smoke_10u.json` 做 maker-only live smoke。
7. 2026-05-09 已完成 TVR data_hub 工程化改造：price history 从研究型全量 bootstrap 改为 raw parquet store + per-symbol cursor + Gateway retry/backoff + decision window stats + archive 分流。
8. 2026-05-10 TVR 多账户生产化第一刀已进入代码：`data_hub` 配置改为 `data_scope=global` + `gateway_account`，落盘 facts 写 `producer=tvr_data_hub`，`decision_audit` 不再要求 data_hub facts 归属某个交易账户。
9. 2026-05-10 TVR 多账户生产化第二刀已进入代码：`live_trader` 每轮内部调用 `decision_audit.build_decision_audit()` 构建本账户 decision，并由同一进程写 decision audit；实盘常驻不再需要启动独立 `decision_audit.py --loop`。
10. 2026-05-10 TVR 多账户生产化第三刀已进入代码：decision/live 配置均改为 per-symbol notional map，当前 smoke 四白名单均为 10U；TP 已同步为 `0.5%`。
11. 2026-05-10 TVR data_hub 已改为只采集显式 live 白名单 symbol，当前白名单为 `CLUSDT/BZUSDT/XAUUSDT/XAGUSDT`；新增品种应先用 `history_backfill.py --symbols ...` 补齐 research history，再用 `percentile_tp_backtest.py --symbols ...` 验证参数，合格后再加入 live 白名单。
12. 2026-05-10 TVR 新增 `p50` entry percentile 档位，当前 smoke decision 配置切到 `p50` 以提高观察期触发频率；`p50` 不是默认生产档位，生产回收前应重新切回经 backtest 认可的低分位。
13. 2026-05-10 TVR live 执行层已对齐公共 BN_EXEC 路径：`BUY/SELL LIMIT GTX` 不再由 `live_trader.py` 直接调用 Binance client，而是通过公共 execution helper 进入 Gateway quota/ban guard、BN_EXEC audit/log/bot 体系。
14. 2026-05-10 TVR live lifecycle 已补齐 open_trade reconcile：常驻循环会同时看 TP 订单与 LONG position，正常 TP 成交会清理 state 并推送 EXIT；外部平仓会清理 state 并取消残留 TP；持仓仍存在但 TP 丢失/终态失败时会 CRITICAL + fail-fast。
15. 2026-05-10 TVR live stdout 已降噪：`iteration started/finished` 改为 DEBUG；普通 wait/skip 不再每 2 秒 INFO，改为按 `logging.summary_interval_secs=3600` 聚合输出 heartbeat，真实 entry/TP/exit/critical 仍即时输出。
16. 2026-05-11 TVR live 新增持仓期 decision 降频：已有 TVR active 时仍每 2 秒做 lifecycle reconcile，但新增候选 decision 至少间隔 `active_decision_interval_secs=300` 秒；本轮 decision 只对非 active tradable symbols 请求 live 24h ticker。
17. 2026-05-11 TVR live 补齐 signal-lock 可观测性：在准备提交入场前输出 `TVR signal locked` 日志、写 live audit `signal_events`，并向 `tvr` Telegram 队列推送“雷达锁定”消息；TVR 策略侧 bot 与公共 BN_EXEC 通知头统一使用 `🏛 TVR`，撤单成功消息尽量使用交易所返回时间避免 `UNKNOWN`。
18. 2026-05-11 TVR pending entry 的 `PARTIALLY_FILLED` 处理改为显式等待窗口：当前 `partial_fill_wait_secs=6`，窗口内继续等待原 maker entry 完成更多成交；窗口到期或 entry TTL 到期后再撤剩余并按已成交数量挂 maker TP。
19. 2026-05-11 已将 TVR recovery ladder 计划落入 `docs/TVR项目语义基线.md`：每 symbol 新增计划字段 `max_symbol_entry_notional_usdt`，按 open lots 的 entry_notional 本金成本限制最大投入；recovery 锚点计划使用最高价未平 lot，`grid_step_pct=5%` 时三层门槛形如 `100 -> 95 -> 90`，并要求 `min_spacing_hours` 防止快速用尽层数。
20. 2026-05-11 TVR EXIT 可观测性补齐：EXIT log / bot / closed trade audit 均补充持仓时长，字段来自 open state 的 `opened_utc_ms` 与退出时 `closed_utc_ms` 的差值；`opened_utc_ms` 口径为 TP 提交后 open_trade 建立时间，并新增 `opened_time_source=tp_order_submitted_after_entry_fill`。
21. 2026-05-11 TVR recovery 第一刀进入代码：`config.live_trader.smoke_10u.json` 新增 `execution.max_symbol_entry_notional_usdt` 与 `recovery` 显式配置；`live_trader.load_config()` 会校验 symbol key、单笔 notional 不超过 symbol 本金上限、`anchor=HIGHEST_OPEN_ENTRY`。本刀不改变实盘入场/退出行为。
22. 2026-05-11 TVR recovery 第二刀进入代码：`live_trader` 新建 open state 时会把当前单笔 `open_trade` 镜像到 `open_lots`，字段包含 `lot_id/lot_role/entry_price/entry_qty/entry_notional_usdt/TP/order/opened_utc_ms`；退出时同步清空 `open_trade` 与 `open_lots`；已有旧 state 若只有 `open_trade`，reconcile 时会补齐 `open_lots`。本刀仍不开放 recovery entry。
23. 2026-05-11 TVR recovery 第三刀进入代码：`recovery.enabled=true` 在非 dry-run 实盘模式仍 fail-fast；`--dry-run` 模式下会对 active symbol 运行同一 decision gate，并输出 `dry_run_recovery_entry_ready` / `dry_run_recovery_entry_blocked`，检查 funding、24h return、最高价未平 lot 锚点、`grid_step_pct`、`min_spacing_hours` 与 `max_symbol_entry_notional_usdt`。本刀只评估，不下 recovery 单。
24. 2026-05-11 TVR recovery 第四刀进入代码：实盘路径支持 `RECOVERY` lot 入场；`recovery.enabled=true` 时 active symbol 通过 recovery gate 后可提交 maker entry，pending fill 后追加 `open_lots` 并为该 lot 独立挂 maker TP。`open_lots` reconcile 会逐 lot 查询 TP；多 lot 下某个 TP 成交只清理对应 lot，全部仓位关闭时清空所有 lots；单 lot TP filled 但 LONG position 仍 open 仍保持 fail-fast。当前 smoke 配置已显式开启 recovery：`enabled=true`、`grid_step_pct=0.01`、`min_spacing_hours=6`。

### 3.8 live data gate finalized payload 观测

已完成：

1. `core/live/live_data_gate.py` 新增公共 `finalized_payload_not_ready` 观测能力。
2. Spring / Sweep-Reclaim live 在 finalized candidate payload 未按当前 C / signal_time ready 时，不再逐条向主 live log 输出 `finalized payload not ready`。
3. Snapback live 的同类 finalized payload wait 失败也接入同一公共观测落盘与 60 分钟摘要 helper。
4. 三套策略会将该事件落盘到：
   `state/live_audit/live_data_gate/{strategy_name}/{account}/YYYY-MM-DD/finalized_payload_not_ready.jsonl`
5. 主 live log 改为进程内每 60 分钟输出最近 60 分钟汇总：事件数、按 reason 聚合、最新 signal_time。
6. Snapback live 已删除私有 finalized payload wait / mismatch 实现，改为直接调用公共 `core/live/live_data_gate.py` wait helper，并保留原 deadline 50 秒语义。

### 3.9 Binance REST Gateway / API 额度治理

当前定位：

```text
Binance REST Gateway 是项目内 Binance REST 出口治理层，目标是成为“总电表 + 分级总电闸”。
```

已完成：

1. 新增 `core/live/binance_rest_gateway.py`，定义 Binance REST 请求优先级：
   - `LOW`
   - `NORMAL`
   - `HIGH`
   - `CRITICAL`
2. 第一版分级 gate 阈值：
   - `LOW/NORMAL`: `used_weight_1m >= 2000` 时拒绝。
   - `HIGH`: `used_weight_1m >= 2300` 时拒绝。
   - `CRITICAL`: `used_weight_1m >= 2350` 时拒绝。
   - Binance hard limit 仍按 `2400` 记录。
3. 新增统一拒绝异常 `BinanceRestGatewayRejected`，拒绝码包括：
   - `BN_REST_GATE_LOW_NORMAL_QUOTA_CLOSED`
   - `BN_REST_GATE_HIGH_QUOTA_CLOSED`
   - `BN_REST_GATE_CRITICAL_QUOTA_CLOSED`
   - `BN_REST_GATE_BAN_WINDOW_ACTIVE`
4. 增强 `core/live/rate_limit_guard.py`：`record_binance_rest_quota()` 除继续覆盖写 latest snapshot 外，同时 append 写 usage ledger。
5. usage ledger 路径：
   `output/shared_market/binance_rest_usage/YYYY-MM-DD/binance_rest_usage.jsonl`
6. TVR data_hub 已迁移为第一批 Gateway consumer：
   - universe / funding / ticker 当前事实为 `NORMAL`
   - funding history / historical klines bootstrap 为 `LOW`
7. 行情层已迁移为第二批 Gateway consumer：
   - `core/live/market_data.py` 的 `futures_time / exchangeInfo / ticker / futures_klines` 走 Gateway。
   - `core/live/binance_client.py` 的 `indexPriceKlines` helper 走 Gateway。
   - 本刀只改变 Binance REST 出口路径，不改变 HBs/finalized payload、候选过滤或策略语义。
8. 2026-05-08 23:58 BJ，本地已用真实 Binance 连接完成行情层 Gateway smoke：
   `futures_time`、`futures_klines(XAUUSDT, limit=2)`、`indexPriceKlines(XAUUSDT, limit=2)` 均成功返回，并写入 usage ledger。
9. 执行层普通只读查询已迁移为第三批 Gateway consumer，统一标记为 `HIGH`：
   - `core/live/binance_exec.py` 的 `futures_account / futures_exchange_info / futures_symbol_ticker / futures_get_open_orders / futures_get_order / futures_position_information / futures_get_position_mode / futures_get_all_orders / futures_account_trades / futures_income_history` 走 Gateway。
   - Gateway 对 `call_client_method()` / `call_futures_public()` 的 Binance API 异常也会写 usage ledger；若响应头存在，则同步更新 latest quota snapshot。
   - 本刀只迁移普通 python-binance 只读查询；下单、撤单、改仓位模式、改保证金模式、改杠杆、algo signed REST 仍保留既有路径。
10. 2026-05-09 00:07 BJ，本地已用真实 Binance 连接完成执行层只读 Gateway smoke：
    - `futures_exchange_info(XAUUSDT)`、`futures_symbol_ticker(XAUUSDT)` 成功返回，并以 `HIGH/ok` 写入 usage ledger。
    - `futures_account`、`futures_get_open_orders(XAUUSDT)`、`futures_position_information` 因本机出口 IP/API 权限被 Binance 返回 `-2015`，但均以 `HIGH/error` 写入 usage ledger，并带出当时 `used_weight_1m`。
11. 执行层 algo signed REST 已迁移为第四批 Gateway consumer：
    - Gateway 新增 `call_futures_signed()`，统一处理 futures signed REST 的 gate、签名、请求、异常和 usage ledger。
    - `GET /fapi/v1/openAlgoOrders`、`GET /fapi/v1/algoOrder` 标记为 `HIGH`。
    - `POST /fapi/v1/algoOrder`、`DELETE /fapi/v1/algoOrder` 标记为 `CRITICAL`。
    - 本刀只改变 signed REST 出口路径和优先级治理，不改变 SL algo 下单 payload、撤单 payload、订单归一化或策略语义。
12. 2026-05-09 00:14 BJ，本地已用真实 Binance 连接完成 algo signed GET Gateway smoke：
    `GET /fapi/v1/openAlgoOrders` 因本机出口 IP/API 权限被 Binance 返回 `-2015`，但已以 `HIGH/error` 写入 usage ledger，并带出当时 `used_weight_1m`。
13. 执行层普通写操作已迁移为第五批 Gateway consumer，统一标记为 `CRITICAL`：
    - `futures_create_order`
    - `futures_cancel_order`
    - `futures_change_position_mode`
    - `futures_change_margin_type`
    - `futures_change_leverage`
    - 本刀只改变普通 python-binance 写接口出口路径和优先级治理，不改变 entry/TP/time-stop payload、取消订单 payload、仓位模式/保证金/杠杆参数、订单归一化或策略语义。
14. 2026-05-09 00:21 BJ，本地已完成执行层普通写操作 Gateway 静态验证：
    `core/live/binance_exec.py` 已无旧 `_call_client_with_retry` / `_record_client_quota` / `sleep_if_binance_rest_*` 写路径残留；普通写接口均通过 `_call_gateway_client_with_retry(... priority=CRITICAL)`。
15. `strategies/klines_1m_store.py` 批量/补数公开 REST 已迁移为第六批 Gateway consumer：
    - Gateway 新增 `request_futures_public()`，用于需要保留自定义 HTTP status/retry 语义的公开 futures REST 调用。
    - `exchangeInfo` 标记为 `NORMAL`。
    - contract/index 1m klines 标记为 `LOW`。
    - 本刀保留 `klines_1m_store.py` 原有 418 ban 记录、429 退避、400 index price 静态错误处理、parquet/state 写入语义。
16. 2026-05-09 00:31 BJ，本地已用真实 Binance 连接完成 `klines_1m_store.py` Gateway smoke：
    `exchangeInfo` 成功返回 720 个 symbols，并以 `NORMAL/ok` 写入 usage ledger；`XAUUSDT` contract klines limit=2 成功返回 2 行，并以 `LOW/ok` 写入 usage ledger。
17. 新增 Binance REST Gateway coverage 审计护栏：
    - 脚本：`audit_tools/maintenance/audit_binance_rest_gateway_coverage.py`
    - 扫描直接 Binance host 引用、直接 `requests/session` Binance HTTP 调用、绕过 Gateway 获取 `get_client/load_account_secrets`、直接 `_request_futures_api`、直接 `client.futures_*` 调用。
    - 当前允许 Gateway 自身、`core/live/binance_client.py` 的 client 构造、`strategies/klines_1m_store.py` 的 `BASE_URL` 常量。
    - 2026-05-09 本地执行结果：`findings=0`。
18. 废除 DataHub 局部 `binance_rest_quota 30轮统计` 推送，改为 Gateway usage ledger 真实用量汇总：
    - 新增 `read_binance_rest_usage_summary()`，从 `output/shared_market/binance_rest_usage/YYYY-MM-DD/binance_rest_usage.jsonl` 聚合窗口内所有 Gateway consumer 请求。
    - 窗口真实 weight 口径为每个 UTC minute bucket 的 `max(used_weight_1m)`，再对窗口内 minute 求和；同时输出 `request_count`、`ok/error/rejected_by_gateway`、`priority_counts`、`peak_1m`、`latest_1m`、order count 峰值。
    - `market_data_hub_runner.py` 不再发送 `[DataHub] binance_rest_quota 30轮统计`；新 Gateway 口径记录为 `binance_rest_gateway_usage_stats`。2026-05-29 起 Telegram 仅在 `error_count > 0`、`rejected_by_gateway_count > 0` 或 `peak_1m >= 1800` 时发送精简 `🚦 [Gateway] REST warning`，健康窗口静默；完整 payload 仍写 audit/log。
    - 本地 180m ledger smoke 成功：可聚合 request/priority/weight 峰值；该口径来自 Gateway usage ledger，不再是 DataHub 对 latest quota snapshot 的局部采样。
19. 2026-05-09 部署后服务器 smoke 发现订单类 REST 响应可能带出 `used_weight_1m=-1`，会污染 Gateway latest/delta 记账。已在 `core/live/rate_limit_guard.py` 增加非负 weight 护栏：usage summary 忽略负数 weight；quota snapshot 遇到同一分钟无有效 weight 但有 order-count 的响应时保留上一条有效 weight，避免分级 gate 失去最近总用量事实。

2026-05-29 起，`market_data_hub_runner.py` 的 finalize 质量 Telegram 推送改为健康窗口静默：只有 `deadline_hit_count > 0`、`timeout_round_count > 0`、`all_passed_count < window_rounds` 或 `verify_failed_count_max > 0` 时发送精简 `⏱️ [DataHub] finalize warning`；仅 `delayed_finalize_count_max > 0` 不再触发 Telegram warning，完整 `finalize_quality_stats` payload 仍写 audit/log。

2026-05-29 起，`market_data_hub_runner.py` 的 `market_total_24h_vol` Telegram 推送保留每 30 轮固定输出，但展示改为精简 B 口径：`📊 [DataHub] market_total_24h_vol (B)` + `min/max/avg`；API 与落盘双口径字段不再进入常规 Telegram 文本，完整 `market_total_24h_vol_stats` payload 仍写 audit/log。若窗口内出现 warming/not_ready 或 missing/partial/stale/new 非零，消息追加一行 `⚠️` 异常摘要。

当前边界：

```text
TVR data_hub、行情层、执行层普通只读查询、执行层 algo signed REST、执行层普通写操作、`klines_1m_store.py` 批量/补数公开 REST 已接入 Binance REST Gateway。
执行层下单/撤单/仓位模式/保证金/杠杆写操作当前均为 CRITICAL。
后续新增任何 Binance REST consumer 必须显式声明 priority，不得绕过 Gateway。
```

当前 pending：

1. 部署后观察 `[Gateway] Binance REST usage 30m` 推送是否覆盖所有 live/data_hub/bn_sync/klines consumer。
2. `tools/bn_sync` 当前通过 `binance_exec.py` 间接接入 Gateway；后续若新增直接 Binance REST 请求，默认按 `LOW` 或 `NORMAL` 分类。
3. 后续可把 `audit_tools/maintenance/audit_binance_rest_gateway_coverage.py` 纳入常规 pre-deploy / CI 检查。

### 3.10 audit tools / 目录治理

已完成：

1. audit scripts 已按 data_quality / spring / snapback / maintenance 等方向整理。
2. `make_md5_line_suffix_copies.py` 等工具增强。
3. 常用审计命令与常用命令文件持续维护。
4. 2026-04-28 已执行一次服务器磁盘清理：删除 mybwin139 旧 stage3 parquet、旧 stage3 enriched 日文件，并清理 Spring 回测审计中除 `SPRING_V1_30D_P6_0427T1606` 外的历史 decision audit。清理后服务器可用空间从约 14G 提升到约 43G。

当前服务器清理纪律：

```text
清理历史 state/audit/output 前必须做二次快照稳定性检查：

1. 第一次记录候选文件 path / size / mtime / inode / run_id。
2. 间隔至少 10 秒后再次记录同一候选集合。
3. 只有两次 path / size / mtime / inode 完全一致，且无活跃进程或 open file handle，才可删除。
4. 当前日期、当前 run_id、仍在增长的文件、live/data_hub/backtest 活跃产物，默认不删。
5. 若发现关联长跑进程，必须先报告进程与文件关系，等待用户明确授权后才能 stop/kill/restart。
6. 删除后必须复查磁盘空间、剩余文件和是否仍有同 run_id 新文件生成。
```

当前保留事实：

```text
output/state/spring_decision_audit.SPRING_V1_30D_P6_0427T1606*.jsonl
```

当前注意：

```text
当前仍有未提交本地改动；新线程开始时必须先看 git status，不要误把它当成已提交事实：

- tools/常用命令
- tools/常用命令-过去.txt
- docs/新Codex线程开场白.txt
```

---

## 4. 当前明确不做

1. 不向策略路径引入 SHORT 语义、字段、分支或实现；手动 `hedge_short` 仅作为账户级管理员门户例外。
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
3. 对比 per-symbol 24h_vol / stage5 fail reason / signal；market_total_24h_vol 仅记录为 live-source gate，不作为严格一致字段。
4. 只在偏离事实明确后进入单问题 patch。
```

### 5.3 Spring-SABC live lifecycle

```text
当前审计起点：

1. Spring live 正式入口是 `strategies/spring/run_live.py`。
2. 旧过渡入口已从源码树删除，不保留 wrapper、alias 或兼容路径。
3. 运行产物命名收敛为 `spring_live.{run_id}.jsonl` 与 `spring_live_heartbeat.{run_id}.json`，默认 run_id 前缀为 `SPRINGLIVE_`。
4. loop 消费 finalized_candidate_inputs 时必须匹配本轮 expected C anchor，deadline 为 `signal_time+50s`；不得用 fresh 但非当前轮的 payload 产生信号或交易。
5. Spring live 在参数与 live execution config 校验通过后会立即写 `[Spring-Live] runner started | account=... | run_id=... | mode=...` 日志；当 `--execute-live` 且 live execution config `notify_enabled=true` 时，同步写入 `spring` PUSH 队列，使 `nohup` 日志不必等第一轮 projection 才能看到启动时间。
6. Spring live 常规每分钟 projection 结果只落盘到 `spring_live.{run_id}.jsonl` 与 heartbeat，不再写 `wrote projection` INFO 日志刷屏。
7. Spring live 已复用公共 `core/live/signal_gate.py`：本策略 pending/open symbol 会在信号生成前并入 active symbols，本策略 cooldown map 会在信号生成前灌入策略，使持仓或 cooldown 期间同一 symbol 不再每分钟重复打印 `Spring雷达锁定`；projection 同时保留 `configured_active_symbols`、实际 `active_symbols`、`live_state_active_symbols`、`cooldown_symbols` 与 `signal_gate` 供审计。
8. Spring live execution config 显式区分 `precheck_scope` 与 `strategy_concurrency_scope`：`precheck_scope` 只表达交易所下单前检查范围（`symbol` / `account_flat`）；`strategy_concurrency_scope` 表达同账户同策略并发约束（`symbol` / `account`）。当前 smoke 配置为 `strategy_concurrency_scope=symbol`，仅同 symbol pending/open/cooldown 会在 Strategy Signal Logic 前阻断新 signal；若要账户级单仓并发，需要显式改为 `account`。
   - 2026-06-06 公共 live execution runner 修复交易所 symbol filter 失败路径：当小额配置导致开仓数量低于交易所最小数量、step floor 后数量为 0、或名义金额低于交易所最小值时，`core/live/execution_runner.py` 记录 signal/error/audit、按 `notify_on_order_error` 推送 bot 消息、标记该 bar 已处理并返回 `skipped_symbol_filter_precheck_failed`；不再抛出未捕获 `ValueError` 导致 Spring/SWR live 进程退出。
9. 公共 BN_EXEC 事件支持按调用方传入 `notify_label`；Spring execution runner 传 `spring`，避免 Spring 的 ENTRY/SL/TP/CANCEL 执行通知落到 `snapback` 队列。
10. Spring pre-A 语义已从 pattern window 左边界漂移改为 A 点前固定窗口：`structure.pre_a.window_mins=60`。`pre_a_chg_pct`、pre-A range、high-to-A-close、close position、up/down 统计均锚定该固定 S→A 区间；`runtime.max_history_window_mins` 必须覆盖 `structure.pattern_window_mins + structure.pre_a.window_mins`，否则 fail-fast。
11. Spring B 低点确认已从 A-B 区间最低 low 收紧为 A-C 区间最低 low：若 B 之后、C 之前出现任何低于 B_low 的 X 点，待定 B 失效，算法继续搜索其它 B；若无其它合法 B，本轮不产生信号。
12. Spring 价格时态已拆开：`strategies/spring/logic.py` 不再从 HBs/cross_section 产出 `signal.current_price` 或最终 `tp_price`；sim 侧策略逻辑只消费 `C=HBs[0]` 的 cross_section，并在 `signal_time=CB` 用 CB open 注入可复现执行价；live 侧在公共 execution lifecycle 中 entry 前读取并落盘 `pre_entry_price`，真实 entry fill 后再按 `risk_reward_1r` 重算 TP。执行层必须保证 LONG 的最终 TP 高于真实 entry，避免 BUSDT 23:21 这类 C_open 被误当 current price 后提交低于 entry 的 TP。
13. live_trades 闭仓 projection 必须保留 entry 审计字段：`pre_entry_price`、`pre_entry_price_source`、`resolved_tp_price_source`，用于复盘真实 entry 前价格、真实 fill 与最终 TP 计算来源。
14. live 实盘执行当前保护规则：非 ASCII symbol 不进入 symbol-specific signed API 下单链路。Spring/SWR 公共 execution runner 返回 `skipped_unsupported_live_symbol_non_ascii`，Snapback consumer 在 precheck 前返回同名 outcome；两者都会记录 state/audit 并标记该 bar 已处理，避免中文 symbol 触发 `-1022` 后杀死进程。
15. 后续若继续推进 Spring live 逻辑 patch，仍需按单问题框架重新锁定 `strategies/spring/run_live.py` 与 `core/live/execution_runner.py` 基线。
```

### 5.3.1 Production process monitor

```text
2026-06-06 新增 `core/process_monitor.py` 与 `process_monitor_config.json`，用于生产常驻进程巡检。监控器只读 `ps` 进程表与策略 heartbeat/state 文件，不接触交易所、不启停进程、不修改策略 state。每轮写 `output/logs/process_monitor.log`，异常或恢复时通过 `core.message_bridge.send_to_bot(..., label="admin")` 进入 bot 队列，重复异常按 `default_alert_repeat_secs` 抑制刷屏。

当前显式纳入巡检的生产进程：`run_manual_trade_bot.py`、`core/notify/tg_queue_sender.py`、`core.exchange_history_sync`、`core/live/market_data_hub_runner.py`、Snapback 三账户 live、Spring 三账户 live、Sweep-Reclaim 三账户 live。TVR 仍处于实验阶段，暂不纳入 `process_monitor_config.json`，避免未上线进程产生生产误报。

Spring / Sweep-Reclaim 通过 `output/live_projection/*_heartbeat.*.json` 检查心跳鲜度；Snapback 通过 `state/live/snapback_{account}.state.json` 的 `last_loop_bj` 检查心跳鲜度。若目标进程缺失、重复、命令行参数不匹配，或 heartbeat 超过配置阈值未更新，则记录 warning 并推送 admin bot 消息。
```

### 5.4 TVR live lifecycle

```text
1. TVR live 对 open_trade reconcile 中的临时 signed 查询错误单独处理：TP 订单查询或 LONG 持仓查询如果返回 `code=-1021` / `outside of the recvWindow` / `Timestamp for this request`，不会杀死 live 进程。
2. TVR LONG 持仓查询复用 `execution.order_retry_max` 与 `execution.api_retry_delay_secs` 做短重试；重试后仍失败才返回 `open_trade_transient_signed_query_failed`。
3. 该 transient 事件会写入 `state/live/tvr_{account}.state.json` 的 `last_error_*`，进入 live audit jsonl，并通过 `tvr` Telegram 队列告警；同一 account/symbol/operation 进程内 30 分钟节流一次，避免刷屏。
4. transient 事件不会标记 position/order reconcile 成功时间，不会清理 `open_trade`，也不会推进入场/离场判断；下一轮继续 reconcile。
```

### 5.5 Spring-SABC sim / 参数

```text
1. 固定当前 config 事实。
2. 用动态开仓金额语义重跑 `0427T1606` 候选基线，确认收益、回撤与 2026-04 表现。
3. 若要改 pre-A / rebound / sizing 参数，先形成语义说明，再做单问题 patch。
```

### 5.6 文档

```text
1. 每个长任务结束前判断是否更新 CURRENT_STATE.md。
2. 阶段性审计结论写入 docs/archive/reports/。
3. 新线程必须从 AGENTS.md + docs/README.md + CURRENT_STATE.md 恢复现场。
```

### 5.7 manual Telegram bot 迁移

```text
2026-05-12 已在当前项目新增 root `run_manual_trade_bot.py` 与 `core/manual_trade_bot.py`，用于替代旧项目 `/root/BN_strategy/main.py` 的账户查询与必要手动交易入口。2026-05-15 已将其语义定位写入 `PROJECT_BASELINE.md`：该进程是账户级管理员门户，管理范围覆盖 API 手动订单、API 自动策略订单，以及通过 Binance App / Web 产生的订单、成交、持仓、挂单与资金流水；文件名中的 `manual` 仅为历史命名。

当前迁移边界：
1. 保留菜单，当前 admin 显示顺序为：/set_s、/trade_open、/trade_close、/trade_other、/account_detail、/view_history、/rebate_report、/hedge_short、/fav、/hs_fav、/edit_symbols、/hs_edit_symbols、/hs_set_s、/open、/close、/pending_orders、/stop_market、/set_current_account、/status；关键菜单显示文案包含图标：`🎯 Set Trade Symbol`、`💼 Account Detail`、`🔴 Hedge Short`、`⚙️ Trade Favorites`、`🧰 Open`；前三个交易命令显示文案分别为 `Command Open`、`Command Close`、`Command Other`。Telegram BotCommand 不支持纯展示、不可点击的空白分隔菜单项，因此不在命令列表中插入假 separator。`/edit_symbols` 由独立 conversation 处理并优先于 `/set_s` 输入会话，且只接收 `ADD` / `DEL` / `LIST` / `DONE` 文本，避免从其它会话切换后误抢 `/open` 的 `Q` / notional 等输入。
2. 删除旧菜单：/view_monitor_status、/hedge_open、/hedge_close、/view_monitor_config、/edit_monitor_config、/add_viewer、/remove_viewer。
3. `/trade` 与旧 `/open` / `/close` / `/stop_market` 手动交易入口固定 LONG-only，只展示和处理 LONG position / LONG pending orders；手动 hedge short overlay 必须走独立 `/hedge_short` namespace。
4. 手动交易不再接入旧项目 `my_binance.py`，统一复用 `core/live/binance_exec.py` 与 Binance REST Gateway。
5. 服务器旧进程 `/root/service_env/bin/python -u main.py` 仍属于 `/root/BN_strategy`，未停止、未切换；部署和切换需要单独授权。
6. 新增 `/set_s` 菜单命令与 `/set s` 当前交易 symbol 入口：输入后显示当前 `SYMBOL LEVERAGE`，并从 `manual_trade_symbols.json` 弹出 symbol/leverage 按钮供点选；也可继续手动输入 `HYPEUSDT 20x`，写入 `state/manual_trade_current_symbol.json`。这里的杠杆只作为 `/trade` 简化命令的记录；设置入口本身不调用 Binance API 修改任何账户或品种杠杆。
7. `/trade open` 默认使用 `/set s` 维护的当前 symbol / leverage，也支持在 action 后显式携带 `SYMBOL`；显式 symbol 必须存在于 `manual_trade_symbols.json`，其杠杆从该白名单读取。支持多账户开仓：
   - `/trade open [SYMBOL] ACCOUNT NOTIONAL[ | ACCOUNT NOTIONAL...]`，默认等价于 `PO`，只提交 post-only 长挂单，不启动 watcher，不自动撤单，不自动挂 SL/TP。
   - `/trade open [SYMBOL] ACCOUNT NOTIONAL[ | ACCOUNT NOTIONAL...] M [SL PRICE] [TP PRICE]`
   - `/trade open [SYMBOL] ACCOUNT NOTIONAL[ | ACCOUNT NOTIONAL...] PO [SL PRICE] [TP PRICE]`
   - `/trade open [SYMBOL] ACCOUNT NOTIONAL[ | ACCOUNT NOTIONAL...] L PRICE`
   - `M` 使用市价 entry，成交后按输入挂 SL/TP；`SL` 与 `TP` 可只输入其中一个，`SL 0` / `TP 0` 表示跳过对应保护单。
   - `PO` 表示 post-only entry，使用 order book best bid 提交 `LIMIT + GTX` maker 单；命令立即返回，后台 watcher 按 `account + symbol` 并行追踪。
   - `L` 表示普通 `LIMIT + GTC` LONG entry，只提交限价开仓，不自动挂 SL/TP。
   - 未输入 `M/PO/L` 时等价于 `PO`，因此 `/trade open [SYMBOL] ACCOUNT NOTIONAL SL PRICE` 会按 `PO SL PRICE` 解析。
   - 只有 `PO`（含默认 PO）命令携带 `SL PRICE` 或 `TP PRICE` 且任意一个价格大于 0 时，才启动 PO watcher；watcher 默认等待 60 秒，成交后挂 SL/TP，部分成交时取消剩余并保护已成交数量，超时未成交则取消 entry。`SL 0` / `TP 0` 与省略 `SL/TP` 均表示纯 PO 长挂单，不启动 watcher。
   - 同一 `account + symbol` 同时只允许一个 PO watcher；不同账户或不同 symbol 可以并行。
   - 手动命令事件落盘到 `state/manual_trade/orders/YYYY-MM-DD.jsonl`，不写入策略 live state / strategy audit。
   - bot 启动时会扫描最近手动交易事件；若发现 PO entry 已提交但没有 watcher done 终态，fail-fast 停止启动并要求人工核查交易所挂单。
   - LONG 手动开仓会先按 entry reference price 校验 `SL < entry`、`TP > entry`；`SL 0` / `TP 0` 仍表示跳过对应保护单。
   - PO entry 提交遇到 Binance `-5022` maker reject 时，会重新读取 order book best bid 并重试提交；当前硬编码最多 3 次，只对 `-5022` 类 post-only maker reject 重试。
   - 2026-05-12 已将手动 open 前的账户 position mode 处理改为只读校验：若账户不是 Hedge Mode，fail-fast 提示；不再在每次 open 前自动调用 `futures_change_position_mode`，避免已有 open orders 时触发 Binance `-4067`。
   - 2026-05-24 手动 open 前的 symbol preparation 增加 exchange activity 只读检查：若该账户该 symbol 已有 position 或 open orders，则跳过 `change_margin_type`，避免已有持仓/挂单时触发 Binance `-4067`；`change_leverage` 仍按当前 bot 配置值执行，避免交易所残留旧杠杆导致新开仓触发 `-2027` 最大可持仓限制。若无活动仓位/挂单，则仍按当前 symbol/leverage 设置流程执行。
8. `/trade close` 默认使用 `/set s` 维护的当前 symbol，也支持在 action 后显式携带 `SYMBOL`；支持 `/trade close [SYMBOL] ACCOUNT[ | ACCOUNT...] [PO] [PCT%]`、`/trade close [SYMBOL] ACCOUNT[ | ACCOUNT...] M|PO [PCT%]`、`/trade close [SYMBOL] ACCOUNT[ | ACCOUNT...] PRICE [PCT%]` 与 `/trade close [SYMBOL] ACCOUNT[ | ACCOUNT...] L PRICE [PCT%]` 命令式 LONG 平仓入口；省略 `M/PO/L` 时默认 `PO`：
   - `M` 对每个指定账户查询该 symbol 的 LONG position qty，并提交 `MANUAL_CLOSE` market reduce 平仓。
   - `PO` 对每个指定账户查询该 symbol 的 LONG position qty，读取 order book best ask，并提交 `LIMIT + GTX` maker 平仓单。
   - `PRICE` 直接输入数字时会读取当前价自动分类：若 `PRICE > current`，按普通 `LIMIT + GTC` 限价止盈；若 `PRICE < current`，按条件 SL 止损；若价格与当前价过近则 fail-fast，提示改用 `M` / `PO` 或输入更明确的价格。bot 回复中显示 `ref` 与 `classified=LIMIT/SL`。
   - `L PRICE` 保留兼容，对每个指定账户查询该 symbol 的 LONG position qty，并按指定 price 提交普通 `LIMIT + GTC` 平仓单。
   - 命令末尾可选 `PCT%` 表示按当前 LONG 持仓比例平仓；不填表示 100% 全部平仓。
   - 多账户用 `|` 分隔，逐账户顺序执行；某个账户失败不阻断后续账户。
   - 2026-06-08 起，`PO` / `L` close 下新单前会先确认该账户该 symbol 仍有 LONG 仓位，再撤销同账户同 symbol 的现有 LONG `SELL` 平仓/止盈类挂单（`LIMIT` / `TAKE_PROFIT*`），然后提交新的平仓单；不会因为存在 SL 条件单而撤销 SL。若撤旧单失败，该账户不继续挂新单。`M` market close 与旧按钮式 market close 不预撤挂单。
   - PO close 提交遇到 Binance `-5022` maker reject 时，会重新读取 order book best ask 并重试提交；当前硬编码最多 3 次。
9. `/trade pending [SYMBOL] ACCOUNT[ | ACCOUNT...]` 命令式挂单查询入口：默认使用当前 symbol，也可显式携带 symbol；列出每个指定账户该 symbol 的 LONG open orders；多账户用 `|` 分隔，逐账户顺序查询，某个账户失败不阻断后续账户。
10. `/trade cancel [SYMBOL] ACCOUNT[ | ACCOUNT...]` 命令式撤单入口：默认使用当前 symbol，也可显式携带 symbol；对每个指定账户撤销该 symbol 的全部 open orders；多账户用 `|` 分隔，逐账户顺序执行，某个账户失败不阻断后续账户。兼容用户常见拼写 `/trade cancle ...`。
11. `/trade sl [SYMBOL] ACCOUNT[ | ACCOUNT...] PRICE [PCT%]` 命令式 LONG 止损挂单入口保留兼容；新命令建议优先使用 `/trade close [SYMBOL] ACCOUNT PRICE [PCT%]` 由价格关系自动分类。默认使用当前 symbol，也可显式携带 symbol。
   - 不填比例时提交全仓 `closePosition=true` SL。
   - 末尾 `PCT%` 表示按当前 LONG 持仓比例提交指定数量 SL，例如 `50%` 只保护当前 LONG 数量的一半。
   - 多账户用 `|` 分隔，逐账户顺序执行；某个账户失败不阻断后续账户。
   - 2026-06-08 起，提交新 SL 前会先确认该账户该 symbol 仍有 LONG 仓位，再撤销同账户同 symbol 的现有 LONG `SELL` 条件退出单（`STOP*` / `TAKE_PROFIT*` / trailing stop），避免 Binance `-4130` closePosition 同方向条件单冲突；普通 `LIMIT` 平仓/止盈单不因单独更新 SL 被撤销。旧按钮式 `/stop_market` 入口同样执行该替换式清理。
12. `/fav` 新增手动交易命令收藏维护，收藏落盘到 `state/manual_trade_command_shortcuts.json`，只保存 `/trade` 参数文本，不接触交易所、不写策略 state。收藏名支持大小写字母、常用汉字、数字、`_`、`-`，落盘与查找时英文字母统一转为小写，并限制为 1-32 个字符且不超过 48 个 UTF-8 bytes。支持 `/fav save NAME TRADE_ARGS`、`/fav show NAME`、`/fav del NAME`、`/fav run NAME`；交易入口支持 `/trade @NAME` 或 `/trade fav NAME` 展开收藏后复用原 `/trade` 解析与 LONG-only 执行路径。菜单展示按收藏命令 action 分类：`/trade_open` 只展示 `open`，`/trade_close` 展示 `close` / `sl`，`/trade_other` 展示 `pending` / `cancel` / `cancle`；旧 `/trade` 不带参数时仍以 `Trade: SYMBOL LEVERAGE` 标题展示全部收藏作为兼容入口。收藏按钮列表末尾固定追加 `Abort`。按钮参数标记使用 `(N p)`；若末尾存在可省略的独立 `?%` 参数，则显示为 `(required-total p)`，例如 `L ? ?%` 显示 `(1-2 p)`；点选收藏后原收藏列表消息会被改写为下一步提示或 `Run: /trade ...`，从而清掉按钮避免误触；若命令中没有 `?` 占位符则直接执行，若包含 `?` 则只提示命令模板与 `Send values separated by spaces.`，输入参数后按顺序替换，支持 token 内占位符，例如 `SL ? TP ?` 点选后输入 `55.392 61.233`，或 `close deepa999 M ?%` 点选后输入 `50` 得到 `50%`。所有 `/trade close` 收藏末尾的独立 `?%` 仓位比例均可省略：例如 `close chen912 | junjie2026 M ?%` 不输入比例时按默认 100% 平仓；`close chen912 | junjie2026 L ? ?%` 输入 `1.25` 会展开为 `close ... L 1.25` 并同样按默认 100% 平仓。
13. 2026-05-28 新增独立手动 `/hedge_short` overlay，作为账户级管理员门户例外，不属于 Spring / Snapback / SWR / TVR 策略 alpha，不写策略 state。语义边界：`PROJECT_BASELINE.md` 已改为“策略 LONG-only”，手动 hedge short 必须独立 namespace、独立白名单、独立 current symbol、独立 client id 前缀 `HSH`、独立 audit/event 落盘。文件为 `state/hedge_short_symbols.json`、`state/hedge_short_current_symbol.json`、`state/hedge_short_command_shortcuts.json`、`state/hedge_short/orders/YYYY-MM-DD.jsonl`。`hedge_short_current_symbol.json` 允许为 `null`，此时未显式携带 symbol 的 `/hedge_short open/close/sl/cancel/pending` 均 fail-fast；即使 symbol 在白名单里，只要 current 为 `null`，默认入口也不可用。`/hs_edit_symbols` 维护白名单，支持 `ADD SYMBOL LEVERAGE`、`DEL SYMBOL`、`LIST`、`DONE`；`/hs_set_s` 从白名单选择当前做空 symbol，并提供 `OFF`。`/hedge_short` 支持在 action 后显式携带 `SYMBOL`，显式 symbol 必须存在于 `hedge_short_symbols.json`，其杠杆从该白名单读取；支持 `open [SYMBOL] ACCOUNT NOTIONAL[ | ACCOUNT NOTIONAL...] [SL PRICE] [TP PRICE]`，省略 `M/PO/L` 时默认 `PO`；也支持 `open [SYMBOL] ... M|PO [SL PRICE] [TP PRICE]`、`open [SYMBOL] ... L PRICE`、`close [SYMBOL] ACCOUNT[ | ACCOUNT...] [PO] [PCT%]`、`close [SYMBOL] ... M|PO [PCT%]`、`close [SYMBOL] ... PRICE [PCT%]`、`close [SYMBOL] ... L PRICE [PCT%]`、`sl [SYMBOL] ACCOUNT[ | ACCOUNT...] PRICE [PCT%]`、`pending [SYMBOL] ACCOUNT[ | ACCOUNT...]`、`cancel [SYMBOL] ACCOUNT[ | ACCOUNT...]`；`close` 省略 `M/PO/L` 时同样默认 `PO`。2026-06-13 起，hedge-short `M` / `PO` open 可选携带 `SL PRICE` 或 `TP PRICE`，两者可只输入一个；不填或对应价格为 0 表示跳过该保护，PO 不启动 watcher；任一保护价格大于 0 时会按 SHORT 方向校验 `SL > entry_ref`、`TP < entry_ref`，M 成交后按成交数量挂保护单，PO 启动 60 秒 watcher，成交后挂保护单，未成交则撤销 entry。2026-06-13 起，hedge-short `close PRICE` 会读取当前价自动分类：若 `PRICE < current`，按普通 `LIMIT + GTC` 限价止盈；若 `PRICE > current`，按条件 SL 止损；若价格与当前价过近则 fail-fast，提示改用 `M` / `PO` 或输入更明确的价格，bot 回复中显示 `ref` 与 `classified=LIMIT/SL`。旧 `sl` 与 `L PRICE` 入口保留兼容。2026-06-08 起，hedge-short `PO` / `L` close 下新单前会先确认该账户该 symbol 仍有 SHORT 仓位，再撤销同账户同 symbol 的现有 SHORT `BUY` 平仓/止盈类挂单（`LIMIT` / `TAKE_PROFIT*`），然后提交新的平空单；不会因为存在 SHORT SL 条件单而撤销 SL。hedge-short `M` market close 不预撤挂单。hedge-short `sl` 提交前会撤销同账户同 symbol 的现有 SHORT `BUY` 条件退出单，避免 closePosition 同方向条件单冲突；若撤旧单失败，该账户不继续挂新单。`/hs_fav` 维护独立收藏，只保存 hedge-short 参数文本（可含显式 symbol），支持 `?` 占位符；`/hedge_short @NAME` 或菜单按钮执行收藏，菜单展示按 `open -> close/sl -> pending -> cancel/cancle` 排序。`/hedge_short cancel` 只撤该 current symbol 的 SHORT open orders，不撤 LONG 挂单。
   - 2026-05-31 修复 `/trade` 与 `/hedge_short` 收藏占位输入串线：设置 `pending_trade_shortcut` 时会清除 `pending_hedge_short_shortcut`，设置 hedge-short pending 时会清除 trade pending，确保 `/trade_open` 后输入参数不会被旧 hedge-short 状态抢走。随后将 bot 临时输入状态收束为“每个 user/chat 只保留一个活跃命令现场”：新的 slash 命令进入时清除旧 transient context，收藏占位输入按 `active_command_context` 路由，并在终态、取消、Abort 后清理现场；所有文本输入型 conversation 入口增加 stale context guard，若旧 ConversationHandler 抢到文本，会先转交当前 active context，再结束旧会话。后续新增需要等待用户输入的命令应复用 `_start_command_context` / `_clear_command_context`，避免跨命令串线。
14. `/account_detail`、`/view_history`、`/pending_orders` 不再依赖 `current_account`；用户点击命令后先弹出账户列表，点选账户后对该账户执行查询。`/account_detail` 结果页内的 Pending / History 按钮会携带本次选择的账户继续查询。2026-05-28 修正 `/account_detail` 展示口径：账户详情读取账户内 LONG + SHORT 当前持仓，SHORT 行以红色标记且持仓金额按负数展示，尾部与 LONG 一样显示止盈挂单百分比；`/trade` 与旧手动交易入口仍保持 LONG-only。2026-06-02 `/pending_orders` 展示账户内 LONG + SHORT 全部当前挂单，并在消息过长时拆分发送，避免静默截断。
15. `/rebate_report GROUP START_DATE END_DATE` 是只读 API 返佣区间报表，标题固定“API返佣报表”。报表从 `mybwin139` 的本地 `income[API_REBATE]` 账本读取返佣流水，用 `symbol + trade_id` 反查系统内账户 `trades`，再从对应 `secrets_{account}.json` 顶层 `rebate_group` 读取账户所属分组。查询结果按 `account + masked_email` 汇总，金额为 USDT 数量。底层按北京时间自然日生成日报缓存，路径为 `state/exchange_history/reports/api_rebate_daily/mybwin139/YYYY-MM-DD.json`；仅当日期已闭合且无未匹配、无多账户冲突、无缺失 `rebate_group` 时持久化缓存。当天或不完整日期每次实时重算。`/rebate_report` 无参数时进入向导：admin 先选择 group、再选择起始日期，group viewer 直接选择起始日期；向导的截止日期固定为查询当天北京时间日期，命令式完整参数入口仍保留。`permissions.json` 可配置 `group_viewers` 只读角色，格式为 `"telegram_user_id": {"group": "Chen", "name": "..."}`；group viewer 只能查询绑定 group，`/start` 也只下发 `rebate_report` 菜单。

2026-05-15 已修补 `/view_history` 的 symbol discovery：最近 24h 历史查询除手动 symbol、当前持仓和当前挂单外，还会从 `state/manual_trade/orders/YYYY-MM-DD.jsonl` 与 `state/live_audit/*_{account}.YYYY-MM-DD.jsonl` 的真实交易生命周期事件中补充 symbol，避免已离场且不在手动列表中的品种被漏查。当前 `/view_history` 的“历史委托”对应 Binance order history / `get_all_orders` 的已成交 LONG order；2026-05-28 起也展示账户级手动 `hedge_short` SHORT order；`get_account_trades` 仅用于按 order id 补充 realized PnL，不是独立的“历史成交”列表。

2026-05-15 已新增 `docs/EXCHANGE_HISTORY_SYNC_SPEC.md` 与 `core/exchange_history_sync.py` 初始模块，明确后续用独立同步层同步账户侧交易所历史事实，定时/增量落盘 orders、trades、income、transfers；admin 门户查询只读本地账本，REST 按 symbol 补查仅作为同步层职责，不再让 Telegram 查询实时扫描大量 symbol。每个账户可在 `secrets_{account}.json` 顶层配置 `exchange_history_start_time` 作为最早追溯边界，格式为带时区 ISO 时间字符串。模块支持 `python -m core.exchange_history_sync --account ACCOUNT --loop --interval-secs 300` 常驻增量运行。

2026-05-21 对 Binance 官方导出与服务器 `state/exchange_history/mybwin139` 做对比：服务器已落盘的 `85` 个 order_id 与 `79` 个 trade_id 均能在官方导出中命中，说明已覆盖子集的数据一致；但官方导出有历史委托 `774`、历史成交 `725`、交易流水 `1620`，服务器当前仅有 orders `85`、trades `79`、income `293`、transfers `0`，缺口来自同步窗口和 symbol universe 覆盖不足。已将 `core/exchange_history_sync.py` 增强为：首次无 source 进度时从 `exchange_history_start_time` 起步；新增一次性 `--bootstrap` 历史回填模式，可忽略已有 cursor 从起始时间重扫；新增 `--symbol-file` 显式历史 symbol universe 入口。`--bootstrap` 必须有 `exchange_history_start_time`，且不得与 `--loop` 同用。

2026-05-21 服务器执行 `mybwin139` bootstrap 后确认 Binance `orders` / `trades` 历史接口对大跨度窗口返回 `APIError(code=-4165): Maximum time interval is 7 days.`；`income` 虽未报错但单次返回命中 `limit=1000`，不能视为完整。已将 exchange history sync 改为窗口化查询：orders/trades 按不超过 6 天切片，income 按 1 天切片；失败窗口不会把 per-source cursor 推进到未成功覆盖的 end_ms，后续 bootstrap 可继续重试补齐。

2026-05-21 exchange history sync 已降为低优先级后台账本任务：历史 orders/trades/income 请求显式使用 Binance REST `LOW` priority，并支持 `--request-sleep-secs` 在每次请求后主动慢跑；CLI 支持重复传入 `--account`，同一进程内按账户串行执行，账户之间用 `--account-sleep-secs` 间隔，避免每账户一个进程并发回填挤压 live 策略 REST quota。

2026-05-21 第一刀推进 exchange history sync 的 symbol universe 简化：日常同步改为 `income-first active symbols`。每轮先同步账户级 `income/transfers`，从本轮 income 查询窗口返回的非空 `income.symbol` 提取 `active_sync_symbols`，再只对这些 symbol 同步 `orders/trades`。`state/exchange_history/{account}/symbols.json` 只作为历史出现过的 symbol 索引，不再作为每轮 orders/trades API 扫描输入；`--symbol` / `--symbol-file` 仅保留为人工补查入口。零成交 `CANCELED/EXPIRED` 订单无 income 事实，当前不纳入完整交易账本的完整性要求。

2026-05-21 第二刀推进 exchange history sync 的余额快照落盘：每轮同步先读取 Binance futures account，并按资产写入 `state/exchange_history/{account}/balance_snapshots/YYYY-MM-DD.jsonl`。每个资产一行，规范化字段中的 `wallet_balance` 表示该资产钱包余额数量，不是折算美元价值；`raw` 保留 Binance account `assets[]` 原始行。余额快照是本轮同步必备事实，失败时本轮返回非 ok，且不继续推进 `income/orders/trades` cursor。后续余额连续性审计按 `wallet_end(asset) = wallet_start(asset) + sum(income.amount by asset)`，`trades.realized_pnl/commission` 只用于和 income 的 `REALIZED_PNL/COMMISSION` 交叉核查，避免重复计算。

2026-05-21 第三刀推进 exchange history sync 的 `positions` 派生落盘：每轮在 `orders/trades` 同步成功后，从已落盘 LONG `trades` 派生闭合仓位生命周期，写入 `state/exchange_history/{account}/positions/YYYY-MM-DD.jsonl`。正常闭合仓位状态为 `CLOSED`；若平仓成交缺少对应开仓事实，则写 `status=INCOMPLETE` 与 `incomplete_reason`，不得伪造 entry price / open time；仍未闭合的 active position 不硬造历史行，只在结果中计入 `open_positions_skipped`。2026-05-22 已将派生仓位补充 `net_pnl`：`realized_pnl` 保留为成交裸盈亏，`net_pnl` 按 Binance 仓位历史口径汇总该仓位相关 `REALIZED_PNL + COMMISSION + 持仓期间 FUNDING_FEE`。2026-05-28 已扩展为同时支持账户级手动 `hedge_short` SHORT 历史派生：LONG 仍按 BUY 开仓 / SELL 平仓，SHORT 按 SELL 开仓 / BUY 平仓；若发现非 LONG/SHORT 方向或无法解释的成交顺序冲突，仍返回非 ok。

2026-05-21 第四刀推进 exchange history 只读审计工具：新增 `audit_tools/exchange_history/audit_exchange_history_continuity.py`，读取本地 `state/exchange_history/{account}`，不访问 Binance、不修改 state。主审计按 `wallet_end(asset) = wallet_start(asset) + sum(income.amount by asset)` 检查相邻 `balance_snapshots` 区间；交叉核查 `sum(trades.realized_pnl) ≈ sum(income[REALIZED_PNL])` 与 `sum(trades.commission) ≈ -sum(income[COMMISSION])`。余额不连续或交叉核查不一致返回非零退出码；资产快照不足只给 warning。

2026-05-21 已将 `core/manual_trade_bot.py` 的 `/view_history` 改为只读本地 `state/exchange_history/{account}` 落盘账本，不再在 Telegram 查询路径实时调用 Binance 历史接口，并在标题区显示本地账本最近同步时间，提醒用户该视图允许分钟级延迟；“历史委托”读 `orders`，“仓位历史”读 `positions` 派生视图并展示盈亏、开仓价、平仓价、开仓时间、平仓时间、持仓时间、最高 O、已平仓量，“转账流水”读 `transfers`，资金费汇总读本地 `income`。2026-05-22 已将“仓位历史”盈亏改为优先展示 Binance 仓位历史口径 `net_pnl`；旧 positions 行没有 `net_pnl` 时，查询时从本地 `income` 按仓位 trade ids 与持仓期间资金费即时计算。开仓时间、平仓时间显示到秒。2026-05-28 `/view_history` 已兼容手动 `hedge_short` 历史：历史委托显示 `开空` / `平空`，仓位历史显示 `SHORT` 方向。2026-06-02 `/view_history` 支持带参数查询：`/view_history [ACCOUNT] [SYMBOL] [DATE_BEGIN] [DATE_END]`，第一个参数匹配已配置账户时直接查询该账户；不填账户则仍弹出账户列表，不填 `SYMBOL` 表示全部品种，不填 `DATE_END` 表示北京时间当日。2026-06-03 不带日期参数时默认查询最近 24 小时。2026-06-23 `/view_history` 日期参数兼容 `YYYYMMDD` 紧凑格式；当使用 `SYMBOL + DATE_BEGIN + DATE_END` 或 `ACCOUNT + SYMBOL + DATE_BEGIN + DATE_END` 精确查询时只输出历史委托和仓位历史，不输出转账流水、汇总统计、净入金。

2026-06-18 修复 exchange_history `orders` 状态快照落盘语义：历史委托依赖 `orders.status in FILLED/PARTIALLY_FILLED`，但 Binance 同一 `order_id` 可能先被同步为 `NEW`，稍后成交才变为 `FILLED`；旧 append-only 去重会保留早期 `NEW` 快照，导致 `/view_history` 漏显示实际已成交的平仓委托。本轮将 `orders` 写入改为按 `dedupe_key` upsert，后续同 `order_id` 状态覆盖旧快照；`trades` / `income` / `transfers` 仍保持流水级追加去重。

2026-06-18 `/view_history` 历史委托新增订单来源图标，只改变 Telegram 展示，不改变 exchange_history 账本与策略执行语义。识别规则基于 `client_order_id`：`SNP` 显示 `🦅`、`SPR` 显示 `🌱`、`SWR` 显示 `📈`、`CAL` 显示 `⚓`、本 bot 手动/API 管理员入口（`MAN` 以及 hedge-short `HSH` 等本 broker 非策略码）显示 `🧰`、非本 broker 或无系统 client id 的 Binance App/Web 官方渠道手动单显示 `🟨`。

2026-06-23 `/view_history` 的“仓位历史”标题增加当前查询范围内仓位净盈亏合计，格式为 `📌 仓位历史: (盈亏合计 **** USDT)`；合计口径使用每条仓位展示同源的 `net_pnl` / 即时补算净盈亏。2026-06-25 `/view_history` 日期参数继续兼容 `YYYY-MM-DD` 与 `YYYYMMDD`，并新增 `YYYYMMDDHHMM` 分钟级时间 token；自然日 token 仍按北京时间整天查询，分钟级 token 按该分钟起止边界查询，例如 `202606181435` 表示北京时间 `2026-06-18 14:35`。

2026-06-25 `/view_history` 仓位历史行优化 Telegram 展示：正常 `CLOSED` 仓位不再显示“完全平仓”，首行行首改为 `🟢盈利` / `🔴亏损` / `⚪盈亏` 加净盈亏金额，再显示 `symbol + side`；开仓价/平仓价压缩为 `O` / `C`，且按价格档位最多显示 3/5/8 位小数；开仓时间/平仓时间压缩为同一行 `T: open -> close`；持仓时间、最高 open qty、已平仓量压缩为一行 `duration max_open/closed_qty`。该变更只影响 bot 展示，不改变 `exchange_history` positions 派生账本。

2026-05-21 chen912 配置 `exchange_history_start_time=2026-05-01T00:00:00+08:00` 后尝试单账户 bootstrap，发现该账户某些 1 天 income 窗口会命中 Binance `limit=1000`，同步器按完整性规则 fail-fast。已将 `core/exchange_history_sync.py` 的 income 同步改为自适应拆分：1 天窗口命中 `limit=1000` 时递归拆为更小窗口，直到低于 limit；若达到最小 1 小时窗口仍命中 limit，继续 fail-fast，避免把截断流水当作完整历史。

2026-05-23 SWR sim/live 一致性审计 checkpoint：mybwin139 最新 live 样本窗口为 `2026-05-11 13:30:00+08:00` 至 `2026-05-22 13:00:00+08:00`，对应 sim run `SWR_SmokeTest_V1_0523T1152`。已确认 live projection anchor 无错位，70 个 sim signal 均能在 live 按 `symbol + signal_time` 命中，匹配信号的 H/gamma/B/C 结构锚点一致；live-only 21 个信号中，20 个对应 sim `cooldown_active`，来自执行状态分歧，1 个 `MLNUSDT 2026-05-15 04:14+08:00` 待查，当前事实指向 sim 本地 1m 落盘数据缺失。live trades 共 64 笔，其中 3 笔 `UNKNOWN_EXIT` 待进一步核查：`ESPORTSUSDT 2026-05-13 01:58+08:00`、`GUAUSDT 2026-05-16 14:44+08:00`、`EDENUSDT 2026-05-20 22:25+08:00`。

2026-05-23 SWR 待查项已有归因：`MLNUSDT` live-only 是 sim 本地 `data/klines_1m/MLNUSDT/2026-05.parquet` 只覆盖到 `2026-05-09 10:21:00+08:00`，而 live hub 在 `2026-05-15 04:13:00+08:00` 有完整结构并 emit signal；这不是 SWR 结构语义漂移。3 笔 `UNKNOWN_EXIT` 均为同账户同 symbol 多策略净仓位合并导致的退出归因缺口：SWR 先开仓后，snapback 在数秒内对同 symbol 追加开仓，随后 snapback 的 market flatten/TP limit 关闭了账户 LONG 净仓位中的剩余数量，SWR 自身 TP/SL 均为未成交 `EXPIRED`，因此 `core/live/execution_runner.py` 只能按自身 time_stop/tp/sl order 状态推断并落为 `UNKNOWN_EXIT`。逐笔事实：`ESPORTSUSDT` SWR 买 17，snapback 买 17 后先卖 17、再由 snapback TP 卖 17；`GUAUSDT` SWR 买 6，snapback 买 7 后先卖 7、再由 snapback TP 卖 6；`EDENUSDT` SWR 买 126，snapback 买 127 后先卖 127、再由 snapback TP 卖 126。

Pending：MLNUSDT 1m 落盘缺尾暂不立即补数据或 patch，先作为 SWR 审计遗留项保留；待 snapback / spring / SWR 三组 sim/live 审计都完成后，统一决定是否先补齐历史 parquet 并重跑 SWR，或增加 backtest preflight 数据完整性 fail-fast 检查。该问题应与 live 执行隔离问题分开处理，避免把数据源完整性修复与策略执行语义修复混在同一 patch。

2026-05-23 Spring-SABC sim/live 一致性审计 checkpoint：mybwin139 live run `SPRINGLIVE_MYBWIN139_20260511T052927Z` 对齐 sim run `Spring_SmokeTest_V1_0523T1239`，审计窗口 `2026-05-11 13:30:00+08:00` 至 `2026-05-22 13:00:00+08:00`。live projection 与 sim decision audit 均为 15811 个窗口内分钟点，live `c_bar_ts + 60000 == signal_time_ts` 无错位。sim signals 84，live signals 98，matched 81，sim-only 3，live-only 17。81 个 matched signal 的 A/B/C/gamma/pattern_start/ab/bc/bars_ac 锚点全部一致，价格类字段在合理浮点容差内一致；26 个 rank/score/order 小差异集中在 `AIGENSYNUSDT`、`IRYSUSDT`、`PIEVERSEUSDT`、`ESPORTSUSDT`、`PLAYUSDT`，未改变 matched signal 的结构锚点。live trades 86，与 `consumed_open_confirmed` 一一对应，无 missing trade；exit reason 为 `STOP_LOSS=41`、`TAKE_PROFIT=41`、`TIME_STOP=4`，无 `UNKNOWN_EXIT`。

Spring-SABC 当前 pending：sim-only 3 分别为 `USELESSUSDT 2026-05-13 00:18`（live audit 为 `cooldown_active`）、`CFXUSDT 2026-05-15 12:34`（live audit 为 `score_not_in_top_n`）、`AVNTUSDT 2026-05-21 22:24`（live audit 为 `spring_structure_not_found`）。live-only 17 中，12 个在 sim audit 已 `structure_pass` 但 `signal_fail_reason=cooldown_active`，属于执行/冷却状态分歧；2 个 `MLNUSDT 2026-05-14 16:27`、`2026-05-15 11:32` 在 sim decision audit 缺失，和已知 `MLNUSDT` 1m parquet 缺尾一致；1 个 `UBUSDT 2026-05-11 14:57` sim 为 `baseline_window_insufficient_bars`，倾向回测窗口起点 warm-up/历史传入差异；2 个 `STORJUSDT 2026-05-16 08:55`、`PROVEUSDT 2026-05-21 16:00` sim 为 `spring_structure_not_found`，本地 2026-05 parquet 覆盖完整，需后续继续查 live hub 与 sim parquet 数据/结构扫描差异。

2026-05-23 Snapback sim/live 一致性审计 checkpoint：mybwin139 live run `SNAPBACKLIVE_MYBWIN139_20260511T052921Z` 对齐 sim run `Snapback_SmokeTest_0523T1300`。sim 输出为 `output/SmokeTest/sim_signals.Snapback_SmokeTest_0523T1300.jsonl`、`sim_trades.Snapback_SmokeTest_0523T1300.jsonl`、`sim_summary.Snapback_SmokeTest_0523T1300.json`、`snapback_candidate_pool_audit.Snapback_SmokeTest_0523T1300.jsonl`；live 输出为 `output/live_projection/live_signals.SNAPBACKLIVE_MYBWIN139_20260511T052921Z.jsonl` 与 `live_trades.SNAPBACKLIVE_MYBWIN139_20260511T052921Z.jsonl`，live stage4/stage5 审计来自 `state/live_audit/stage_audit/snapback_mybwin139.stage{4,5}_*.2026-05-*.jsonl`。sim signals/trades/candidate 均为 151；live signals 157，live trades 144。sim signal 窗口为 `2026-05-11 16:37:00+08:00` 至 `2026-05-22 12:21:00+08:00`，live signal 窗口为 `2026-05-11 14:37:00+08:00` 至 `2026-05-22 12:21:00+08:00`；两侧 signal 均满足 `c_time + 60000 == signal_time`，未发现 anchor 错位。按 `(symbol, signal_time)` 对齐：matched 145，sim-only 6，live-only 12。145 个 matched signal 的 `s/a/b/c_time`、`ab_bars/bc_bars`、`current_price/sl_price/tp_price`、`b_index_price/c_index_price`、`drop_pct/drop_window_chg/vol_ratio/rebound_ratio/basis_b_pct/basis_c_pct` 在当前容差内一致，未发现结构硬字段漂移。

Snapback 当前 pending：sim-only 6 中，`HYPEUSDT 2026-05-15 20:54` 与 `BCHUSDT 2026-05-19 22:47` 未进入 live stage4/stage5，且二者位于 `market_data_hub_config.json` 的 live hub `exclude_symbols`；`TACUSDT 2026-05-15 09:46` live stage5 记录 `active_symbol_skip`；`UBUSDT 2026-05-16 18:44` 对应 live 此前 `17:29` 同 symbol 信号后的 live cooldown；`NAORISUSDT 2026-05-15 22:20` 与 `22:21` live stage5 均为 `audit_selected=true` 但 `logic_selected=false`，到 `22:22` 才 `logic_selected=true` 并 emit live signal，随后 `22:23` 起因同 symbol open trade 记录 `active_symbol_skip`；`AIGENSYNUSDT 2026-05-16 12:22` 对应 live finalized summary 已包含 symbol passed，但该分钟 `SKYAIUSDT` time-stop 维护中先出现 TP/SL cancel `-2011 Unknown order sent`，随后写 `signal_scan_skipped_reconcile_query_error`，因此整轮新 scan 被跳过，下一分钟 `AIGENSYNUSDT 12:23` 正常 emit live signal。live-only 12 中，`BUSDT 2026-05-11 21:54`、`SAHARAUSDT 2026-05-14 02:15`、`LABUSDT 2026-05-14 20:19`、`NAORISUSDT 2026-05-15 22:22`、`AIGENSYNUSDT 2026-05-16 12:23`、`UBUSDT 2026-05-16 17:29` 可由 sim 侧先前同 symbol signal/cooldown 或 active trade 解释；`MLNUSDT 2026-05-15 06:14` 与 `SYSUSDT 2026-05-17 10:04` 对应本地 sim parquet 缺尾，分别只覆盖到 `2026-05-09 10:21:00+08:00` 与 `2026-05-09 10:29:00+08:00`；`JELLYJELLYUSDT 2026-05-11 14:37` 位于 sim 首个信号前早期窗口，倾向回测起点 / warm-up 差异；`STARUSDT 2026-05-15 16:36/16:50/16:51` 本地 contract parquet 覆盖完整，但目标窗口 `close_idx/high_idx/low_idx` 全为 `NaN`，live hub 当时具备 idx 字段且 stage5 均选中，live 执行层因 `APIError(code=-4028): Leverage 5 is not valid` 未成交。`STARUSDT` 属于本地 sim idx 数据缺口，不是已证实的 Snapback 结构语义漂移。

Snapback live lifecycle 审计事实：live trades 144，exit reason 分布为 `TAKE_PROFIT=59`、`STOP_LOSS=41`、`TIME_STOP=39`、`SL_SUBMIT_FAILED_FLATTEN=3`、`UNKNOWN_EXIT=2`。13 个 live signal 无 live trade projection，其中 8 个是 `pre_entry_price_guard_skip`（`BUSDT 2026-05-11 21:52`、`PLAYUSDT 2026-05-13 19:51`、`SAHARAUSDT 2026-05-14 02:14`、`LABUSDT 2026-05-14 20:18`、`SIRENUSDT 2026-05-15 11:35`、`UBUSDT 2026-05-15 14:20`、`UBUSDT 2026-05-16 14:18`、`SWARMSUSDT 2026-05-22 11:46`），2 个是非 ASCII symbol 下单前 `precheck_skip`（`币安人生USDT 2026-05-13 12:56`、`2026-05-19 22:11`），3 个是 `STARUSDT` leverage ensure failed（`2026-05-15 16:36/16:50/16:51`，交易所返回 `APIError(code=-4028): Leverage 5 is not valid`）。2 个 `UNKNOWN_EXIT` 为 `STORJUSDT 2026-05-16 08:02` 与 `NAORISUSDT 2026-05-16 15:15`：`STORJUSDT` 已确认与 SWR 同分钟同 symbol 并发有关，SWR `08:02:26` 先提交 entry/SL/TP，Snapback `08:02:29` 再提交 entry，Snapback SL 提交被交易所拒绝 `-4130`（同方向已有 GTE closePosition 条件单），随后 protective flatten 又被拒绝 `-2022 ReduceOnly Order is rejected`，`08:03` 仓位已空且 Snapback 自身 TP/SL/TS 身份缺失，故落 `UNKNOWN_EXIT`；`NAORISUSDT` 目前未在 Spring/SWR 同日审计中发现同 symbol 并发，事实为 Snapback entry 243 张、SL NEW、TP NEW 后，`15:17` 账户 LONG 仓位为空、SL 为 `EXPIRED`、TP 按 order query 仍为 `NEW` 但 open orders snapshot 为空，因此仍是外部/交易所事实归因缺口。当前未执行任何代码 patch；待 snapback / spring / SWR 三组审计统一收敛后，再拆分本地数据完整性、live exclude、同 symbol 跨策略执行隔离/归因，以及 `-2011/-4130/-2022` lifecycle 容错问题。

2026-05-23 后续推进 pending 计划：先把三策略 sim/live 一致性审计闭环收尾，再按单问题拆 patch。第一阶段只归类，不改代码：1）数据完整性：`MLNUSDT` / `SYSUSDT` parquet 缺尾、`STARUSDT` 本地 contract parquet 有数据但目标窗口 idx 字段为 `NaN`；2）live hub universe / exclude：Snapback `HYPEUSDT` / `BCHUSDT` sim-only 来自 live hub exclude；3）多策略同账户同 symbol 执行隔离与 exit 归因：SWR 三笔 `UNKNOWN_EXIT`、Snapback `STORJUSDT UNKNOWN_EXIT` 已指向净仓位合并 / 保护单冲突；4）live-only 执行保护：pre-entry guard、非 ASCII symbol precheck、`STARUSDT -4028 Leverage 5 is not valid`；5）cooldown / active state 分歧：只记录为执行状态差异，不直接判为策略结构不一致。第一刀倾向做数据完整性 `fail-fast / preflight`，因为它同时影响 Spring/SWR/Snapback 审计可信度，并且与执行隔离问题解耦。后续单独拆：live execution 同 symbol 多策略隔离 / 归因 patch、live hub exclude 与 sim universe 对齐策略、leverage invalid 的配置或交易所能力校验。每刀必须按项目 patch 纪律推进：一次只处理一个主问题，先锁 MD5，分类为 `LOGIC_ONLY` 或 `ARCH_ONLY`，最小修改，跑最小验证，并更新 `CURRENT_STATE.md`。

2026-05-23 第一刀数据完整性 patch 已落到 `strategies/run_backtest.py`，分类 `LOGIC_ONLY`，编辑前 MD5 为 `0c20d557520ca5c1f47f12b46c39f887`。变更只在 sim 取证模式触发：当命令同时传入 `--audit-start-bj`、`--audit-end-bj`、`--audit-symbols` 时，先对审计目标 symbol 做本地 parquet preflight，再初始化 `CrossSectionalFeeder`。preflight 要求目标 symbol 有 parquet、覆盖 `audit_start - runtime.max_history_window_mins` 到 `audit_end`、1m 时间戳连续、基础 OHLCV 字段可数值化且非 NaN；Snapback 额外要求 `high_idx/low_idx/close_idx` 存在且在审计窗口内为正数。这样 `MLNUSDT` / `SYSUSDT` 缺尾、`STARUSDT` idx 为 `NaN` 会在审计启动阶段 fail-fast，而不是沉默表现为 sim 侧缺候选或结构未选中。普通非取证 backtest 暂不受影响，避免把退市/缺尾目录一次性扩大成全量历史回测阻断。最小验证：`python3 -m py_compile strategies/run_backtest.py` 通过；用 `/private/tmp` 临时 parquet fixture 验证完整数据通过、缺尾返回 `trailing coverage missing`、Snapback idx 缺失返回 `invalid Snapback idx columns`。本地缺 `mplfinance`，直接 import `run_backtest.py` 时会被既有可视化顶层依赖挡住，因此 fixture 验证用 stub 绕过 `core.analysis.visualizer`，未改变生产代码导入路径。

2026-05-23 第二刀同账户同 symbol 多策略隔离 patch 已落到 `core/live/live_state.py`、`core/live/execution_plan.py`、`strategies/snapback/current_ledger.py`、`strategies/snapback/trade_consumer.py`，分类 `LOGIC_ONLY`，编辑前 MD5 分别为 `e4696888eacc86dbc1b88cdfc2e508b5`、`2e4af3657cb949351b551dda3ad5d5f3`、`f4f8219646c8ad85948c377608867a9b`、`3a074ba006f9bc337d91c7272476d120`。新增 `collect_account_symbol_strategy_activity(account, symbol, exclude_strategy_name=...)` 读取 `state/live/*_{account}.state.json`，只把其它策略同一 symbol 的 `pending_entry_order` / `open_trade` 作为本地事实 blockers，不读取交易所、不修改 state。Spring/SWR 公共 dry-run plan 新增 `precheck.cross_strategy`，若其它策略同 symbol active，则 `ok_to_execute=false` 并返回 `cross_strategy_pending_entry_order` / `cross_strategy_open_trade`；Snapback scan gate 将其它策略同 symbol active 加入 `active_symbols`，consumer 下单前也会二次 precheck，命中时落 `skipped_cross_strategy_active_symbol`。该 patch 只处理同账户同 symbol 跨策略本地 active 隔离，不处理账户级全局互斥，也不改变 `UNKNOWN_EXIT` 的历史归因逻辑。最小验证：`python3 -m py_compile core/live/live_state.py core/live/execution_plan.py strategies/snapback/current_ledger.py strategies/snapback/trade_consumer.py` 与 `git diff --check` 通过；用 `BN_STATE_DIR=/private/tmp/bn_state_cross_strategy_test` 临时 state fixture 验证 sweep-reclaim `STORJUSDT open_trade` 会让 Spring dry-run `ok_to_execute=False` 且 blocker 为 `cross_strategy_open_trade`，并让 Snapback reconcile/active symbols 包含 `STORJUSDT`。

2026-05-23 第三刀 Snapback universe exclude 对齐 patch 已落到 `core/config_loader.py`、`strategies/snapback/logic.py`、`strategies/snapback/config.sim.json`、`strategies/snapback/config.highfreq.json`、`strategies/snapback/config.profit.json`，分类 `LOGIC_ONLY`，编辑前 MD5 分别为 `9ee8b7c2f1668f82550ed6cde494eb17`、`7996c9bb36ae0c14fd740c7558569302`、`1b479bcd42f660404c61452e2e368976`、`8e4602d6a57d6604bc8acb697c2a269b`、`480ea298fbda3a1367339883072bd20d`。Snapback config schema 现在显式要求 `universe.exclude_symbols`，策略逻辑在 universe 过滤与 `audit_symbols_at_kline_close` 中同口径排除这些 symbol；三份 Snapback 策略配置补入当前 live_config 使用的 exclude 列表，包含 `HYPEUSDT` 与 `BCHUSDT`。这会让此前 Snapback sim-only 的 `HYPEUSDT 2026-05-15 20:54`、`BCHUSDT 2026-05-19 22:47` 在 sim 侧同样落为 `symbol_in_exclude_symbols`，不再作为结构语义差异。最小验证：`python3 -m py_compile core/config_loader.py strategies/snapback/logic.py`、三份 Snapback config `StrategyConfig.load(...)`、以及最小 `HYPEUSDT` audit fixture 均通过，fixture 返回 `symbol_in_exclude_symbols`。

2026-05-23 用户确认 `STARUSDT -4028 Leverage 5 is not valid` 这类交易所杠杆能力问题暂不做 patch：实盘遇到时直接跳过交易并保留现有审计记录即可。该类问题归入 live-only execution protection，不再作为当前三策略 sim/live 语义一致性审计的待修复项；后续只有在需要减少 skip 噪音或做交易所能力预筛时，再单独按一刀 patch 处理。

2026-05-23 第四刀 `UNKNOWN_EXIT` 归因增强 patch 已进入本地代码，分类 `LOGIC_ONLY`，目标是只增强审计/投影落盘，不改变实盘下单、撤单、平仓、state 清理或 exit_reason 判定。新增公共 helper `core/live/exit_attribution.py`，Spring/SWR 公共 `core/live/execution_runner.py` 与 Snapback 私有 `strategies/snapback/trade_consumer.py` 在 `exit_reason=UNKNOWN_EXIT` 时写入同口径 `exit_attribution_detail`：记录 `position_flat`、open orders snapshot 是否为空、三条自有退出腿的 query status、cleanup status、`own_open_status_legs`、`open_query_but_absent_from_snapshot` 与归因标签。典型 NAORISUSDT 类现场会保留 `exit_reason=UNKNOWN_EXIT`，同时把“仓位已空、自有 TP/SL/TS 未见 FILLED、TP query 仍可能是 NEW 但 open orders snapshot 为空”归为 `external_or_exchange_inconsistent`，便于后续复盘，不伪造成 TAKE_PROFIT / STOP_LOSS / FOREIGN_EXIT。最小验证：`python3 -m py_compile core/live/exit_attribution.py core/live/execution_runner.py strategies/snapback/trade_consumer.py` 通过；用本地 fixture 模拟 `position_flat=true`、`tp_query_status=NEW`、`sl_query_status=EXPIRED`、open orders snapshot 为空，确认 `exit_attribution_detail.attribution=external_or_exchange_inconsistent`；`git diff --check` 通过。

2026-05-23 当前三项明确 pending：
1. 三策略 sim/live 审计复跑收尾：优先基于现有 live 落盘事实重跑/复核 SWR、Spring-SABC、Snapback 对应窗口的 sim/backtest，对比新 sim 输出与既有 live projection/audit，确认数据 preflight、跨策略同 symbol 隔离、Snapback exclude 对齐与 UNKNOWN_EXIT 归因增强后的 mismatch 分布是否收敛；该步骤不要求先产生新的 live 样本。
2. Spring-SABC 两个结构差异样本继续查证：`STORJUSDT 2026-05-16 08:55`、`PROVEUSDT 2026-05-21 16:00`。已归因为 sim feeder float32 降精度导致边界 `rebound_ratio` 低于阈值，当前已进入 `core/engine/data_feeder.py` 的 `LOGIC_ONLY` 补丁；后续需在 commit / push 后重跑 Spring 对应审计窗口，确认这两例从 pending mismatch 中移除。
3. 本地 1m / idx 数据缺口实际修复：先对全量 `data/klines_1m` 做服务器审计，再修复可从交易所 idx 源补齐的缺口；不可补齐项只做事实归因和审计基线决策，不伪造 idx 数据。该问题应与 live execution 归因、Spring 结构差异分开处理。

2026-05-23 服务器已完成一次全量 `data/klines_1m` / idx 数据质量审计与补数修复。审计输出位于服务器 `output/state/1m_idx_full_20260523T161412/`，覆盖 555 个本地 symbol；contract 连续性结果为 0 个 contract gap、0 个 duplicate timestamp、0 个 non-monotonic timestamp。修复前 idx 状态为 `FULL=552`、`PARTIAL_MISSING=3`，缺口 symbol 为 `MEGAUSDT`、`PHAROSUSDT`、`STARUSDT`，全量 `idx_completeness` 缺失三列同空行数为 30133，占 0.1372%。已在服务器执行 `augment-idx` 修复，输出位于 `output/state/idx_fix_20260523T161717/`；修复后全量 recheck 为 `COMPLETE/FULL=554`、`PARTIAL_GAP/PARTIAL_MISSING=1`，缺失三列同空行数降为 8824，占 0.0402%。`STARUSDT` 已从 `2026-05-14 13:30` 起补齐 idx，`PHAROSUSDT` 已从 `2026-05-14 13:15` 起补齐 idx；这两项不再作为本地 idx 缺口 pending。

2026-05-23 本轮 1m / idx 审计对原三策略审计相关 symbol 的归因结论：`MLNUSDT` 与 `SYSUSDT` 现有本地行 idx 均为 `FULL`，并非 idx 缺口；二者 contract/idx 尾部分别停在 `2026-05-09 10:21:00+08:00` 与 `2026-05-09 10:29:00+08:00`，且当前 live exchangeInfo 不再包含，脚本自动判为 suspected/confirmed delisted，属于退市后本地历史尾部事实，不应按普通可交易 symbol 补尾。`MEGAUSDT` 是本轮全量审计新暴露的唯一剩余 idx 前缀缺口：contract 从 `2026-04-24 15:16:00+08:00` 起有 1m 行，但 Binance `/fapi/v1/indexPriceKlines` 在 `2026-04-30 18:20:00+08:00` 前对 `MEGAUSDT` 返回 0 行，`2026-04-30 18:20:00+08:00` 起才有 idx 行；因此该缺口不是补数脚本漏补，而是交易所 index 历史源前缀不可得。下一步若要让全量审计不再把 `MEGAUSDT` 视为 warning，应单独处理 pre-market-no-idx 基线/审计策略，不伪造 idx 数据。

2026-05-23 Spring-SABC 两个结构差异样本已完成归因并进入本地补丁，分类 `LOGIC_ONLY`，目标文件 `core/engine/data_feeder.py`，编辑前 MD5 为 `42e43829ad7d2f3a9b97c697cd12c7ab`。根因不是本地 parquet 缺失，也不是 live hub 独有数据，而是 `CrossSectionalFeeder` 统一把 float64 列 downcast 为 float32，改变了 Spring 边界结构判定：`STORJUSDT 2026-05-16 08:55` 与 `PROVEUSDT 2026-05-21 16:00` 的 `rebound_ratio` 都卡在阈值 `0.5` 附近，float64 / live 侧通过，float32 后分别变为约 `0.4999975165` 与 `0.4999985099`，从而被 sim 判为 `spring_structure_not_found`。补丁移除 feeder 的 float32 downcast，保留 parquet 原始数值精度作为策略语义输入；不改变 Spring 结构规则、阈值或 live 执行行为。服务器只用 `/tmp/data_feeder_patched.py` 做非生产验证，未改服务器 repo：patched feeder 对两例复算均为 `structure_pass`，A/B/C 与 live 分别对齐为 `STORJUSDT A=08:41 B=08:46 C=08:54`、`PROVEUSDT A=15:51 B=15:54 C=15:59`。本地最小验证：`python3 -m py_compile core/engine/data_feeder.py` 与 `git diff --check` 通过。

2026-06-12 CAL H anchor 刷新参数已调整：三份实盘 decision config（`stark21`、`chen912`、`junjie2026`）的 `data.h_anchor_refresh_secs` 从 `3600` 改为 `60`。语义保持 `include_current_bar=true`，即 48h 高点继续包含当前未闭合 1h bar；刷新间隔缩短到 60 秒，用于降低当前小时内先创新 48h 高点、随后快速回撤触发 P1 时因旧 H 缓存而漏信号的风险。该变更只调整 CAL H anchor 参数，不改变入场/止盈/仓位管理代码路径。

2026-06-12 CAL TP 补单 post-only 重试补丁：生产发现 `chen912 / SPCXUSDT` 一笔 entry 已 `FILLED`、交易所 LONG `36.31`、open orders 为空、TP client id 不存在，但策略固定用原始 TP 价 `169.37` 重挂 GTX SELL；当时盘口约 `175.95/175.96`，原始 TP 价已低于 best bid，Binance 持续返回 `-5022`，导致裸仓无 TP 且每 10 秒重复 `tp_submit_failed`。补丁落在 `strategies/cal/live_trader.py`：TP 补单时取最新 order book，把实际挂单价设为 `max(original_tp_price, best_ask)`，若仍因 post-only 被拒绝则刷新盘口继续重试；成功后将 pending entry 转为 open lot，并清理 `tp_submit_failed` pause 标记。该变更只影响 entry 成交后的 TP maker 补单路径，不改变信号、entry、止盈比例或 open lot reconcile 语义。
```
