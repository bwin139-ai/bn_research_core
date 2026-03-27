# bn_research_core / Snapback-ABC_BINDEX
## sim/live 一致性代码审计（基于上传 zip 的代码事实）

审计范围：
- `strategies/run_backtest.py`
- `strategies/snapback/logic.py`
- `core/engine/data_feeder.py`
- `core/engine/broker.py`
- `strategies/snapback/run_live.py`
- `core/live/market_data.py`
- `core/live/binance_exec.py`
- `core/live/live_state.py`

结论口径：
- 只写代码中**已被证实**的事实。
- 不对 zip 之外的数据构建流程做猜测。
- “偏离”分成两类：
  1. **确定偏离**：sim 与 live 代码行为已经不同。
  2. **高风险未共用实现**：两边走的不是同一实现链路，数值一致性在这份 zip 内无法被证明。

---

## 一、sim 侧基线链路（代码事实）

### 1. sim 主循环
- `strategies/run_backtest.py:367-397`
- 顺序是：
  1. `broker.on_kline_close(ts, cross_section)` 先处理已有订单/持仓；
  2. 取 `active_symbols = active_orders ∪ active_positions`；
  3. `strategy.on_kline_close(...)` 产出信号；
  4. 若有信号，则创建 **LIMIT entry order**，并写入 `broker.active_orders`。

### 2. sim 数据输入
- `core/engine/data_feeder.py:44-126`
- sim 直接从本地 parquet 读 1m 数据。
- `chg_24h` = `close / close.shift(1440) - 1`：`data_feeder.py:88-91`
- `vol_24h` = `quote_asset_volume` 的 1440 根 rolling sum：`data_feeder.py:93-98`
- `high_idx / low_idx / close_idx` 直接从 parquet 中读取；若缺列仅补 NaN：`data_feeder.py:77-81`

### 3. sim Snapback 信号逻辑
- `strategies/snapback/logic.py:65-352`
- 关键信号链：
  - 24h 过滤：`76-82`
  - active_symbols/cooldown 过滤：`86-90`
  - S->C 窗口与 drop_window_chg：`120-138`
  - 第一象限过滤：`140-145`
  - A/C/drop_pct：`147-167`
  - vol_ratio：`169-177`
  - B / basis / ab_bars / bc_bars / rebound_ratio：`189-228`
  - 多候选排序后仅取 top1：`275-277`
  - limit/tp/sl 生成：`282-286`
  - **信号产生时即写入策略内 cooldown**：`288`

### 4. sim 撮合与退出
- `core/engine/broker.py:65-166`
- 入场：
  - 挂单是 LIMIT，超时撤单：`97-104`
  - 命中逻辑：`low <= limit_price`；成交价 = `min(limit_price, open_price)`：`113-116`
- 持仓退出：
  - time stop 先判断：`75-83`
  - SL / TP 再判断：`84-90`
  - TIME_STOP 平仓价用当前 bar `close`：`78-80`
  - TP / SL 平仓价用阈值价本身：`85-89`

---

## 二、live 侧链路（代码事实）

### 1. live 主循环
- `strategies/snapback/run_live.py:2297-2960`
- 顺序是：
  1. 取 candidate symbols：`2307`
  2. 采集 exchange activity snapshot：`2308-2311`
  3. 构造 candidate / extra 的 live inputs：`2313-2316`
  4. 先做 pending/open_trade reconcile：`2348-2366`
  5. orphan / reconcile / missing data / active_state error 多层守门：`2380-2475`
  6. **每轮重新实例化** `WashoutSnapbackStrategy(strategy_cfg)`：`2479`
  7. `strategy.on_kline_close(...)`：`2500`
  8. 信号后再做 cooldown/precheck/下单：`2525-2960`

### 2. live 数据输入
- `core/live/market_data.py:284-370`
- 数据来源：
  - 候选符号列表来自 Binance `futures_exchange_info`：`77-94`
  - 24h 指标来自 Binance `futures_ticker()`：`97-105`
  - K 线来自 Binance `futures_klines()`：`193-195`
  - benchmark/index 在 live 侧现场重建：`237-281`

### 3. live 当前已有落盘快照
- `core/live/market_data.py`
  - `stage2_universe`：`147-188`
  - `stage3_bars parquet`：`340-342`
- `strategies/snapback/run_live.py`
  - `stage4_input_snapshot`：`2482-2499`
  - `stage6_signal`：`2515-2520`, `2613-2622`
  - `stage7_precheck`：`2555-2565`, `2570-2580`
  - `stage8_exec`：`2629-2638`, `2666-2678`

---

## 三、已证实的 sim/live 偏离

### 偏离 1：live 每轮重建 strategy 对象，导致 logic 内部 cooldown 状态不持久
**证据**
- sim：strategy 在主循环外只创建一次：`strategies/run_backtest.py:337-345`
- live：strategy 在 `_run_once()` 内每轮都新建：`strategies/snapback/run_live.py:2479`
- sim 的 strategy 内部 cooldown 在发信号时写入：`strategies/snapback/logic.py:288`

**结果**
- sim：某 symbol 一旦发出信号，会在 logic 入口阶段被 `self.cooldown_until` 拦住：`logic.py:89-90`
- live：由于 strategy 每轮重建，`self.cooldown_until` 每轮清空，logic 阶段**不会复用上轮 cooldown**。

**这会造成的具体偏离**
- sim：处于 cooldown 的 symbol 不会进入候选排序。
- live：处于 cooldown 的 symbol 仍可能被选为 top1，直到 signal 产出后才在 `run_live.py:2533-2538` 被拦下。
- 一旦 top1 被 cooldown 拦下，live **不会回退选择第二名候选**，而 sim 会直接在逻辑阶段跳过该 symbol，可能选到别的 symbol。

这是一个**确定偏离**，而且是信号选择层面的核心偏离。

---

### 偏离 2：sim 是 LIMIT+timeout 入场；live 是 MARKET 立即入场
**证据**
- sim 生成 `limit_price` 与 `timeout_sec`：`strategies/snapback/logic.py:282-321`
- sim 将信号桥接成 `Order(limit_price, timeout_sec, ...)`：`strategies/run_backtest.py:386-397`
- sim broker 挂 LIMIT 单并在超时时间后撤销：`core/engine/broker.py:97-104`
- sim 入场成交逻辑是 `low <= limit_price`，成交价 `min(limit_price, open_price)`：`core/engine/broker.py:113-116`

- live 的 entry API 类型固定为 `MARKET`：`core/live/binance_exec.py:11-15, 309-360`
- live 下 entry 时并未使用 `signal['limit_price']` 或 `signal['params']['timeout_sec']`：`strategies/snapback/run_live.py:2624`

**结果**
- sim 与 live 的入场模型不是同一模型。
- sim 允许“挂单未成交 / 超时撤销”；live 入口不存在同构的 entry timeout 语义。

这是**确定偏离**。

---

### 偏离 3：sim 的 24h 指标来自 bar 序列；live 的 24h 指标来自 ticker API
**证据**
- sim：
  - `chg_24h` = `close / close.shift(1440) - 1`：`core/engine/data_feeder.py:88-91`
  - `vol_24h` = rolling 1440 根 quote volume：`core/engine/data_feeder.py:93-98`
- live：
  - ticker 来自 `client.futures_ticker()`：`core/live/market_data.py:97-105`
  - universe 过滤使用 `quoteVolume` 与 `priceChangePercent`：`129-190`
  - 写入 cross_section 的 `chg_24h` / `vol_24h` 也来自 ticker：`222-234`

**结果**
- sim 与 live 的 24h 指标不是同一来源，也不是同一计算路径。
- live 的 24h 指标还不是“锁定在 latest_closed_bar_ts 的 bar-close 值”，而是 API 调用瞬间的 ticker 值；`latest_closed_bar_ts` 在 `43-45` 先算出，ticker 则在后续独立请求中再取：`297-305`。

这是**确定偏离**。

---

### 偏离 4：live 先做 universe 预过滤，再把结果喂给 strategy；sim 不做这层前置 universe 裁剪
**证据**
- sim：把完整 `cross_section` 直接传给 strategy：`strategies/run_backtest.py:367-380`
- live：`build_live_inputs()` 里先用 24h ticker 做 `stage2_universe` 过滤，只对 eligible symbols 拉取/保留历史：`core/live/market_data.py:299-345`
- 随后 strategy 只看到 `candidate_cross_section`：`strategies/snapback/run_live.py:2477-2500`

**结果**
- live 有一层 sim 没有的“前置 universe 守门”。
- 因为这层守门又使用的是 ticker API 指标（不是 sim 的 bar-based 指标），所以 live 可能在 strategy 运行前就排除掉 sim 本可参与比较的 symbol。

这是**确定偏离**。

---

### 偏离 5：live 的候选 universe 额外受 `exclude_symbols` 和交易所当前状态约束；sim 无同构机制
**证据**
- live 候选列表来自 `futures_exchange_info()`，且只保留 `TRADING + PERPETUAL + USDT`：`core/live/market_data.py:77-94`
- live 还会应用 `exclude_symbols`：`core/live/market_data.py:80, 2307`
- sim feeder 直接扫描 `data/klines_1m` 目录：`core/engine/data_feeder.py:37-42`

**结果**
- live 与 sim 的可扫描 symbol 集合不是同一集合定义。
- 这不仅是运行环境差异，也是候选空间差异。

这是**确定偏离**。

---

### 偏离 6：live 在 signal 之前有多层“全局阻断守门”，sim 没有同构流程
**证据**
- live 在 signal scan 前会因为以下条件直接 return：
  - orphan exchange activity：`run_live.py:2380-2401`
  - reconcile query error：`2403-2415`
  - missing reconcile data：`2417-2432`
  - no candidate payload：`2434-2445`
  - exchange activity query error：`2447-2461`
  - active state error：`2463-2475`
- sim 主循环不存在这些守门层，只有 broker 处理已有订单/持仓，然后直接跑 strategy：`run_backtest.py:367-380`

**结果**
- 即便同一根 bar、同一份行情，live 也可能在 signal 之前整体跳过扫描。
- 这类偏离不是 strategy 逻辑本身，而是 live 额外状态机带来的流程偏离。

这是**确定偏离**。

---

### 偏离 7：cooldown 的生效时点不同
**证据**
- sim：在 strategy 发信号当下写入 cooldown：`strategies/snapback/logic.py:288`
- live：
  - signal 之后才检查 state cooldown：`run_live.py:2533-2538`
  - entry cooldown 在“仓位确认后”才刷新：`2932-2953`
  - exit cooldown 在退出检测后刷新：`1723-1727`

**结果**
- sim 的 cooldown 是“信号层 cooldown”。
- live 的 cooldown 更接近“已建仓/已离场后的 state cooldown”。
- 两者生效时机不同，会影响 top1 选择和同 bar 是否继续尝试其它机会。

这是**确定偏离**。

---

### 偏离 8：time stop 执行模型不同
**证据**
- sim：达到 `max_hold_mins` 且利润不足时，按当前 bar `close` 直接平仓：`core/engine/broker.py:75-83`
- live：达到条件后先撤 TP/SL，再提交一个 `MARKET` time-stop order：`strategies/snapback/run_live.py:2000-2220`
- live 的 `TIME_STOP_ORDER_TYPE = "MARKET"`：`core/live/binance_exec.py:13-15, 479-529`

**结果**
- sim 的 time stop 是“bar close price close-out”。
- live 的 time stop 是“交易所市场单退出”。

这是**确定偏离**。

---

## 四、高风险未共用实现（这份 zip 内无法证明数值相等）

### 风险点 1：benchmark / `*_idx` 实现链路不共用
**证据**
- sim：`high_idx / low_idx / close_idx` 从 parquet 直接读取：`core/engine/data_feeder.py:77-81`
- live：benchmark/index 在 `core/live/market_data.py:237-281` 现场重建
- live benchmark weights 还是硬编码：
  - `BTC 0.56 / ETH 0.24 / BNB 0.12 / SOL 0.08`：`core/live/market_data.py:240-245`
- 而策略配置文件里 benchmark 是另一套配置：
  - `BTC 0.6 / ETH 0.3 / BNB 0.1`：`strategies/snapback/config.json`

**能确定的事实**
- live 没有读取 `strategy_cfg['benchmark']['index_weights']`。
- sim 与 live 没有共用同一套 benchmark/index 生成实现。

**不能在这份 zip 内证明的事**
- sim parquet 中的 `*_idx` 究竟是按什么权重、什么归一化口径生成的。

因此，这一项在本轮审计中应判为：**高风险未共用实现**。

---

## 五、当前 live 落盘快照覆盖不足的地方

### 1. 缺 stage1：候选 universe 原始清单快照
当前只有 `stage2_universe`，没有把 `list_candidate_symbols()` 的原始候选全集单独落盘。
证据：`core/live/market_data.py` 中没有 stage1 写盘；stage 搜索结果也无 stage1。

### 2. 缺 stage5：strategy 内部逐阶段过滤快照
当前只有：
- stage4 输入快照：`run_live.py:2482-2499`
- stage6 最终 signal：`2515-2520`, `2613-2622`

中间缺失的关键事实包括：
- 哪些 symbol 在 logic 内因 `active_symbols` 被过滤；
- 哪些 symbol 因 cooldown 被过滤；
- `drop_window_chg`、`drop_pct`、`vol_ratio`、`basis_b_pct`、`ab_bars`、`bc_bars`、`rebound_ratio` 的逐 symbol 计算值；
- 每一步 fail reason；
- 候选列表排序前的全量候选集与排序结果。

这意味着：
- 当前 live 快照**还不够支撑**“逐阶段与 sim 对表”的全面审计。

### 3. stage8_exec 只覆盖执行摘要，不是完整 sim 桥接对表
目前 stage8 记录的是 entry/tp/sl 提交结果摘要：`2629-2638`, `2666-2678`
但没有一个“sim bridge 对照快照”，去明确表达：
- sim order model = LIMIT/timeout
- live exec model = MARKET/no-timeout
- 本轮 signal 的 `limit_price` / `timeout_sec` 在 live 被忽略

---

## 六、审计结论（本轮）

### A. 已确认的核心偏离（优先级最高）
1. **strategy 对象生命周期不同，导致 logic cooldown 失真**
2. **sim LIMIT+timeout 入场 vs live MARKET 入场**
3. **24h 指标来源不同：sim=bar rolling，live=ticker API**
4. **live 有 strategy 前置 universe 过滤，sim 没有这层**
5. **cooldown 生效时点不同**
6. **live 存在 signal 前全局阻断守门层**
7. **time stop 执行模型不同**

### B. 高风险但本轮无法闭环证明的点
1. **benchmark / `*_idx` 是否与 sim 完全一致**

### C. 快照能力结论
- 当前 live 已有 stage2/3/4/6/7/8。
- **最关键的缺口是 stage5（logic 内部逐步筛选快照）**。
- 在没有 stage5 之前，无法把“live 为什么没出信号 / 为什么选中了这个 symbol”与 sim 做逐阶段硬对表。

---

## 七、下一步建议（不含代码，只给实施顺序）

### 第一步：先补 stage5，且只做审计，不改交易语义
建议把 `WashoutSnapbackStrategy.on_kline_close()` 拆成“可审计阶段快照”形式，至少落盘：
- stage5_1_active_cooldown
- stage5_2_sc_drop
- stage5_3_vol_climax
- stage5_4_basis_rebound
- stage5_5_candidates_ranked

每条记录至少含：
- `bar_ts / bar_bj / symbol`
- 当前阶段输入值
- 当前阶段 pass/fail
- fail_reason
- 若进入候选，则保留排序关键字段（`drop_pct`, `rebound_ratio`, `basis_b_pct` 等）

### 第二步：做“同 bar 对表器”
拿同一个 `bar_ts + symbol`，把 sim 与 live 的下列字段逐项对比：
- close / vol_24h / chg_24h
- high_idx / low_idx / close_idx
- drop_window_chg / drop_pct / vol_ratio
- b_index_price / basis_b_pct / ab_bars / bc_bars / rebound_ratio
- signal selected / not selected / fail reason

### 第三步：在审计完成前，不要急着改 live 交易语义
先把偏离点全部可视化、可落盘、可对表；再决定修哪个偏离。

