# TVR 项目语义基线

本文档是 TradFi Value Reclaim（简称 `TVR`）的唯一活跃语义基线。

若本文档与项目公共基线冲突，以 `docs/PROJECT_BASELINE.md` 为准。

## 1. 策略定位

1. `TVR` 是 Binance USD-M TradFi 永续合约的 LONG-only 策略路线。
2. `TVR` 的交易对象是 TradFi 映射合约，例如黄金、白银、原油、天然气、股票或指数类 USDT 永续。
3. Binance USD-M 当前 TradFi 合约元数据事实为 `contractType=TRADIFI_PERPETUAL` 且 `underlyingSubType` 包含 `TradFi`。
4. `TVR` 不属于山寨币短周期结构策略，不复用 Snapback / Spring / Sweep-Reclaim 的结构语义。
5. `TVR` 的核心前提是 TradFi 标的存在相对明确的价值锚，允许在低杠杆、小仓位、账户级限仓前提下做价值回归。
6. `TVR` 当前仍受项目 LONG-only 总规则约束，不定义 SHORT、对冲或双向 CTA 语义。

## 2. live-first 边界

1. `TVR` 不做山寨币结构策略式传统 sim 作为第一阶段准入条件。
2. `TVR` 采用 live-first 路线，以 live 交易所事实、live funding、live 盘口与真实 maker 成交作为事实源。
3. live-first 不等于无验证；第一阶段必须先建设数据端，长期采集并落盘事实。
4. 策略交易端可以频繁重启、调参、暂停；`TVR data_hub` 应稳定运行并持续积累数据。
5. `TVR` 必须建设 percentile reclaim backtest，用完整历史 1m 数据验证各入场分位与固定 TP 的命中率和耗时。

## 3. data_hub 语义

`TVR data_hub` 第一版只采集和落盘事实，不下单，不写 live state，不生成交易 intent。

第一版事实流分为：

1. `universe`
   - 只采集 live 当前 Binance exchangeInfo / ticker 事实。
   - 不回填历史 universe。
   - TradFi universe 必须由交易所当前合约元数据识别；识别不到时 fail-fast。
   - TVR data_hub 只采集显式配置的 live 交易白名单 symbol；白名单为空、重复或包含当前 TradFi exchangeInfo 不存在的品种时必须 fail-fast。

2. `funding`
   - 当前 funding 使用 Binance `/fapi/v1/premiumIndex` live fact。
   - 历史 funding 可使用 Binance `/fapi/v1/fundingRate` bootstrap。
   - 历史 funding 只用于研究、审计和分布分析，不作为 live 入场门禁依赖。

3. `rolling_24h`
   - 使用 TVR data_hub 自有的原始 1m contract kline store 计算 rolling 24h return 分布。
   - 原始 1m price history 必须支持按 symbol cursor 增量补齐；TVR data_hub 启动时不得一刀切重拉完整决策窗口。
   - live 决策参数可以参考最近窗口统计，但第一版不让统计模块自动改写交易参数。
   - 原始价格事实、cursor state、统计结果和超过决策窗口的历史归档必须落盘，供后续人工审计。

`TVR` 价格历史必须分为两类语义：

```text
live decision store:
  只服务当前实盘决策，例如最近 30 天。
  要求轻量、稳定、可增量、可被 data_hub loop 持续维护。

research history store:
  服务 TVR backtest / 参数研究。
  必须尽量保存所有 TradFi 品种上市以来的完整 1m contract kline。
  必须支持断点续传、按 symbol cursor 增量补齐、长期归档和审计。
```

## 4. 入场门禁草案

当前已实现第一版策略侧 `decision_audit`，只读取 TVR data_hub facts 并落盘 audit-only intent，不下单，不写交易 live state。后续真实交易端必须继续遵守以下入场语义：

1. 只允许 LONG。
2. 只允许 `POST_ONLY` maker 买入。
3. 只允许 `POST_ONLY` maker 止盈卖出。
4. 不设置价格止损。
5. 必须设置账户级风险边界，包括单品种最大暴露和 TVR 总暴露。
6. 必须设置 `funding_rate_entry_max`：
   - 若入场时当前 funding rate 大于该阈值，禁止新开仓。
   - 若 funding rate 缺失、不可读或字段异常，fail-fast，不入场。
   - 入场后 funding 变化只落盘审计，不作为第一版持仓退出条件。
7. 目标入场语义必须使用当前滚动 24h return 与历史低位分位比较：
   - `current_24h_return = priceChangePercent / 100`
   - 等价于 Binance `/fapi/v1/ticker/24hr` 的 `lastPrice / openPrice - 1`
   - `selected_percentile_return` 必须来自历史 rolling 24h return 的 `p1/p5/p10/p20/p50` 之一。
   - 触发条件为 `current_24h_return <= selected_percentile_return`。
8. 第一版 `decision_audit` 必须要求 `history_sufficient=true`，否则该 symbol 禁入并落盘原因。
9. 第一版 `decision_audit` 必须由 JSON 显式配置 `tradable_symbols` 白名单：
   - DataHub 只采集其显式配置的 live 交易白名单品种。
   - 策略侧只允许 `tradable_symbols` 白名单品种产生候选 intent。
   - `tradable_symbols` 必须被 DataHub universe 覆盖；缺失时必须 fail-fast。
   - 白名单为空、重复或包含 DataHub universe 不存在的品种时必须 fail-fast。
10. 第一版 `decision_audit` 只生成 `POST_ONLY_MAKER_*_AUDIT_ONLY` intent；真实 Binance 下单能力由 `live_trader` 承担。

## 4.1 实盘执行第一版

当前已实现第一版 `live_trader` 小资金实盘执行层：

1. 每轮通过 `decision_audit` 同源构建函数读取全局 data_hub facts，生成本账户 audit-only selected intents；不依赖单独常驻 `decision_audit` 进程。
2. 必须显式配置 `allow_live_order=true` 才允许提交真实订单。
3. 只支持 `LONG` / hedge mode / crossed margin。
4. 入场只允许 `LIMIT + GTX` post-only maker BUY。
5. 入场成交后只允许 `LIMIT + GTX` post-only maker SELL 止盈。
6. 第一版不设置价格止损，不做 SHORT，不做对冲，不自动加仓。
7. 第一版只允许小仓位 smoke notional，并要求本地 state / 交易所 symbol 维度无 pending、无 open position、无 open orders 后才提交新 entry。
8. 若 entry 部分成交，第一版会尝试撤销剩余 entry，并对已成交数量挂 TP。
9. 目标实盘入场价格不应由历史目标价长期挂单等待；触发后应读取实时盘口，使用 best bid 或 best bid 减一 tick 提交 `BUY LIMIT GTX`，以降低动态撤单重挂复杂度。
10. 若 `BUY LIMIT GTX` 因盘口下移或 post-only 约束直接 `EXPIRED/REJECTED`，该结果视为有利方向上的 maker 重定价机会；在本次 entry attempt window 内只重读 best bid 并重试，不重新判断 `current_24h_return`。
11. entry attempt 必须有显式生命周期和次数上限，防止无限重试和 API 额度失控。
12. TVR 实盘下单必须复用公共 Binance execution / Gateway 体系，保留统一 quota、ban guard、BN_EXEC 日志和 bot 执行通知，不得在 `live_trader` 私有绕过公共执行入口直接调用 Binance 下单。
13. open trade 必须进入完整生命周期 reconcile：同时查询 TP 订单与 LONG position；TP 成交时清理本策略 state 并输出 EXIT；position 已关闭但 TP 未成交时按外部 `POSITION_CLOSED` 清理并取消残留 TP；position 仍存在但 TP 查询失败、缺失或终态未成交时必须 CRITICAL + fail-fast。`opened_utc_ms` 不是 entry 挂单创建时间，而是 entry 已成交或部分成交并处理剩余后、TP 提交完成并建立 open_trade 的时间；EXIT 日志、bot 与 closed trade audit 必须包含基于 `opened_utc_ms` / `closed_utc_ms` 计算出的持仓时长。
14. live stdout 只输出真实动作、异常和显式周期 heartbeat；普通 wait/skip 事件必须继续落 audit，但不得每轮刷 INFO 日志。
15. live_trader 在已有 pending/open 时必须先过滤本策略 active symbols；持仓期间新增候选 decision 可以按显式 `active_decision_interval_secs` 降频，但已有仓位 lifecycle reconcile 不得降频。

## 4.2 recovery ladder 计划语义

`TVR` 后续可以增加 recovery ladder，用于处理单笔 TVR 仓位长时间未触达 TP 后的低位继续回收交易。

该能力尚未进入当前 live 代码；实现前必须先满足以下约束：

1. 不改变 LONG-only、maker-only、funding gate 和 `current_24h_return <= selected_percentile_return` 入场触发语义。
2. 不使用账户聚合持仓均价作为 recovery 锚点。
3. 不用当前市值作为仓位上限，避免越跌越释放额度。
4. 必须以本金成本口径限制每个 symbol 的最大投入：

```json
"execution": {
  "symbol_notional_usdt": {
    "CLUSDT": 10,
    "XAUUSDT": 10
  },
  "max_symbol_entry_notional_usdt": {
    "CLUSDT": 30,
    "XAUUSDT": 30
  }
}
```

`max_symbol_entry_notional_usdt` 的语义固定为：

```text
sum(open lots entry_notional_usdt for symbol) + next_entry_notional_usdt
<= max_symbol_entry_notional_usdt[symbol]
```

该字段表达“最多投入多少本金成本”，不是当前 mark price 下的 position notional。

recovery ladder 的第一版规则固定为：

```text
允许 recovery 新增一笔，当且仅当：

1. recovery.enabled = true。
2. 当前 symbol 已有未平 TVR lot。
3. 当前 symbol 没有 pending entry。
4. 当前 24h return 仍满足同一 entry_percentile，例如 p10。
5. 当前 funding 仍满足 funding_rate_entry_max。
6. 当前 symbol open lots 的 entry_notional_usdt 总和 + 本次 entry notional
   不超过 max_symbol_entry_notional_usdt[symbol]。
7. 距离该 symbol 最近一次 entry 至少 recovery.min_spacing_hours。
8. 当前价格满足最高价锚点的固定间距门槛：

   anchor_price = max(open_lots.entry_price)
   open_lot_count = len(open_lots)
   required_drop_pct = recovery.grid_step_pct * open_lot_count
   current_price <= anchor_price * (1 - required_drop_pct)
```

若 `grid_step_pct=0.05`，且第一笔 entry price 为 `100`，则：

```text
第二层 recovery 门槛：current_price <= 95，且距离上一笔 entry >= min_spacing_hours。
第三层 recovery 门槛：current_price <= 90，且距离上一笔 entry >= min_spacing_hours。
```

初始建议配置：

```json
"recovery": {
  "enabled": true,
  "anchor": "HIGHEST_OPEN_ENTRY",
  "grid_step_pct": 0.05,
  "min_spacing_hours": 24
}
```

实现边界：

1. recovery 不是 martingale；每笔 recovery 的 notional 仍来自 `symbol_notional_usdt[symbol]`。
2. 每个 lot 必须独立记录 `lot_id`、entry order、entry price、entry qty、entry notional、TP order 和状态。
3. 每个 lot 必须独立挂 `entry_price * (1 + take_profit_pct)` 的 maker TP。
4. 多 lot 不能继续复用单个 `open_trade` 语义；必须升级为 `open_lots` / `lots` 结构。
5. reconcile 必须能按 TP order 归属清理对应 lot；不能只依赖 Binance 聚合 LONG position 均价。
6. bot / log / audit 必须区分 `BASE` 与 `RECOVERY` entry。
7. recovery dry-run 与审计应先于真实下单实现。

## 5. rolling 24h 统计

`rolling_24h_return` 定义为：

```text
current_price / price_24h_ago - 1
```

它不是自然日涨跌幅，而是每个采样点相对于 24 小时前同一采样点的滚动收益。

第一版统计窗口：

```text
decision_window_days = 30
minimum_history_days = 14
initial_sync_lookback_days = 30
rolling_window_hours = 24
archive_after_days = 30
```

只有 `decision_window_days` 内的数据参与当前 rolling 24h 统计和后续 TVR live 入场阈值校准；archive 数据只用于人工复盘、研究和审计，不参与当下决策。

统计项至少包含：

```text
min / max / mean / median / p1 / p5 / p10 / p20 / p50 / sample_count
```

其中 `p1/p5/p10/p20` 表示 rolling 24h return 的低位百分位，用于人工校准 `entry_percentile`；`p50` 只用于 smoke / 观察期提高触发频率，不作为默认生产档位。

后续字段命名应避免使用孤立 `latest` 表达涨跌幅语义，优先使用：

```text
rolling_24h_return_latest
rolling_24h_return_p1
rolling_24h_return_p5
rolling_24h_return_p10
rolling_24h_return_p20
rolling_24h_return_p50
```

## 5.1 percentile reclaim backtest 语义

`TVR` backtest 不是山寨币结构策略 sim，而是 percentile reclaim backtest。

第一版 backtest 必须回答：

```text
1. p1 / p5 / p10 / p20 / p50 触发后，固定 TP 0.5% / 1% 的命中率。
2. 达到 TP 的最短时间、最长时间、平均时间和中位时间。
3. max_hold 窗口内未达到 TP 的样本数和比例。
4. 不同 symbol、不同分位、不同 TP 的横向比较。
```

第一版 backtest 口径：

```text
1. 每个时刻只能使用该时刻之前的历史样本计算 p1/p5/p10/p20/p50，禁止未来函数。
2. 触发条件为 current_24h_return <= selected_percentile_return。
3. 第一版 entry_price 可使用当前 1m close，并明确标记为 signal-level backtest。
4. TP 判定使用后续 K 线 high 是否触达 entry_price * (1 + take_profit_pct)。
5. 同一 symbol 在一笔样本结束前不得重复入场。
6. 样本结束条件为达到 TP 或超过 max_hold。
```

## 6. 共享基础设施边界

`TVR` 可以复用当前项目的公共基础设施：

```text
Binance REST client
Binance REST Gateway
REST quota / ban guard
REST usage ledger
北京时间转换
JSONL audit 落盘
Telegram 消息推送
后续公共 Binance execution / state / reconcile 框架
```

`TVR` 第一版不得复用或污染：

```text
现有 market_data_hub 的 HBs/finalized payload 语义
Snapback / Spring / Sweep-Reclaim 的结构信号逻辑
山寨币 universe 过滤规则
任何 SHORT 或对冲语义
```

## 7. 当前第一阶段目标

```text
实现 TVR data_hub：
1. 采集当前 TradFi universe snapshot。
2. 采集当前 funding snapshot。
3. 可选 bootstrap 历史 funding。
4. 增量补齐 TVR 自有原始 1m price history store，并计算 decision window 内 rolling 24h 分布。
5. 超过 decision window 的原始 price history 可归入 archive，不参与 live 决策统计。
6. 按 TVR 独立目录落盘 audit facts。
7. 不下单，不写 live state，不影响现有三套策略。
```

## 8. 当前三件事与 patch 顺序

当前 TVR 后续工程固定为三件事：

```text
1. 完整历史数据落盘：
   对所有 TradFi 品种尽量补齐上市以来完整 1m contract kline，写入 research history store。

2. 完整 TVR percentile reclaim backtest：
   读取 research history store，评估 p1/p5/p10/p20/p50 与 TP 0.5%/1% 的命中率和耗时。

3. 完整 TVR 实盘逻辑：
   根据 backtest 选择 entry_percentile；live 决策使用 current_24h_return <= selected_percentile_return；
   触发后按实时盘口 best bid 侧提交 post-only maker entry，成交后固定 TP。
```

代码 patch 顺序必须优先按 `3 -> 2 -> 1` 的工程依赖倒序推进：

```text
第一刀：完整历史数据落盘。
第二刀：TVR percentile reclaim backtest。
第三刀：按 backtest 语义修正 TVR decision_audit / live_trader 实盘入场逻辑。
```

第三刀 live 逻辑的固定语义：

```text
1. selected_percentile_return 低频刷新，默认 percentile_refresh_secs = 3600。
2. 当前价格/24h return 高频监控，默认 decision_audit.collection.interval_secs = 2。
3. current_24h_return 使用 Binance 24h ticker 的 priceChangePercent / 100。
4. 触发条件为 current_24h_return <= selected_percentile_return。
5. 触发后 live_trader 读取 best bid 并提交 BUY LIMIT GTX。
6. entry attempt window 内若 post-only 失败，重读 best bid 并继续重试，不重新判断 24h return。
7. entry attempt window 默认 30 秒，最大 30 次尝试。
8. entry pending order 默认 TTL 为 120 秒，超时未成交则撤单并清理 pending。
```

## 9. TVR live 多账户目标架构

TVR live 后续生产形态固定为：

```text
全局唯一 TVR data_hub -> 每账户一个 TVR live_trader
```

目标边界：

1. `TVR data_hub` 是全局公共事实源，只启动一个进程。
2. `TVR data_hub` 可以配置 `gateway_account` 用于 Binance REST Gateway 调用身份，但落盘 facts 不属于任何交易账户。
3. `TVR live_trader` 每个账户只启动一个进程；实盘常驻时不应再要求单独启动 `decision_audit` 进程。
4. `decision_audit.py` 可保留为只读审计/debug 工具，但 live 交易主路径应由 `live_trader` 内部构建本账户 decision。
5. 不同账户必须复用同一套 TVR 入场/离场逻辑；账户差异只能来自显式配置，例如账户名、是否允许实盘、每 symbol 开仓金额和账户级最大敞口。
6. 开仓金额必须支持按 symbol 显式配置，不得用单一账户级金额隐式套用全部 TradFi 品种。

后续 patch 顺序：

```text
第一刀 ARCH_ONLY:
  将 TVR data_hub 改为全局公共 facts source；
  移除 decision_audit 对 data_hub account 的强绑定；
  不合并 decision/live，不改下单金额语义。

第二刀 ARCH_ONLY:
  将 decision 构建能力并入 live_trader 常驻主路径；
  live_trader 每轮直接读取全局 data_hub facts 并构建本账户 intent；
  decision_audit.py 仅作为审计/debug 工具保留。

第三刀 LOGIC_ONLY:
  将 fixed order_notional_usdt 改为 per-symbol notional；
  同步配置 p10 + TP 0.5%；
  保持 LONG-only、maker-only 和账户级敞口上限。
```

当前三刀均已进入代码；后续多账户配置只应改显式 JSON，不应复制策略逻辑。

第一刀 history backfill 的固定语义：

```text
1. 独立脚本：strategies/tvr/history_backfill.py。
2. 独立配置：strategies/tvr/config.history_backfill.json。
3. 只写 research history store，不修改 live decision store。
4. 只采集 contract 1m klines，不采集 index klines。
5. 当前 universe 来自 Binance futures exchangeInfo 的 TradFi active symbols。
6. 每个 symbol 的起点使用 exchangeInfo.onboardDate。
7. 每个 symbol 必须维护 cursor，重启后从 last_open_time_ms + interval 继续。
8. 所有 Binance 请求必须通过 Binance REST Gateway，并使用 LOW/NORMAL 优先级。
9. 落盘路径为 state/research/tvr/klines_1m/{symbol}/{YYYY-MM}.parquet。
10. cursor 路径为 state/research/tvr/history_backfill_state.json。
11. audit 路径为 state/research/tvr/audit/history_backfill/YYYY-MM-DD/tvr_history_backfill.jsonl。
```

第二刀 percentile reclaim backtest 的固定语义：

```text
1. 独立脚本：strategies/tvr/percentile_tp_backtest.py。
2. 独立配置：strategies/tvr/config.percentile_backtest.json。
3. 只读取 research history store，不访问 Binance API。
4. 每个时刻只能使用该时刻之前的 rolling 24h return 样本计算 p1/p5/p10/p20/p50。
5. 触发条件为 current_24h_return <= selected_percentile_return。
6. 第一版 entry_price 使用当前 1m close，并标记为 signal-level backtest。
7. TP 判定使用后续 K 线 high 是否触达 entry_price * (1 + take_profit_pct)。
8. 同一 symbol 在一笔样本 TP 或 max_hold 结束前不得重复入场。
9. summary CSV 路径为 state/research/tvr/backtest/percentile_reclaim/YYYY-MM-DD/{run_id}.summary.csv。
10. sample CSV 路径为 state/research/tvr/backtest/percentile_reclaim/YYYY-MM-DD/{run_id}.samples.csv。
11. audit 路径为 state/research/tvr/backtest/percentile_reclaim/YYYY-MM-DD/tvr_percentile_reclaim_backtest.jsonl。
```

## 9. 已实现组件目标

```text
实现 TVR decision_audit：
1. 读取最新 universe / funding / rolling_24h_stats 全局 data_hub facts，并按白名单 symbol 高频读取 live 24h ticker。
2. 校验 data_hub facts 新鲜度、`data_scope=global`、producer、symbol 覆盖和字段可读性。
3. 应用 LONG-only、history_sufficient、funding_rate_entry_max、entry_percentile 分位阈值、risk notional cap。
4. 应用 JSON `tradable_symbols` 白名单，非白名单品种只落盘拒绝原因。
5. 生成 audit-only maker LONG intent，不提交订单。
6. 按 TVR 独立 decision audit 目录落盘候选、拒绝原因和 selected_intents。
7. 不查询 Binance 账户，不接执行层，不写交易 live state。
```

```text
实现 TVR live_trader：
1. 每轮内部构建本账户 decision_audit selected_intents，并按原 decision audit 路径落盘审计。
2. 校验 account、audit-only intent 与 notional 一致性。
3. 查询本地 TVR live state 与交易所 symbol position/open orders，防止重复开仓。
4. 通过公共 BN_EXEC / Binance REST Gateway 提交 post-only maker entry。
5. entry 成交后通过公共 BN_EXEC / Binance REST Gateway 提交 post-only maker TP。
6. 写入 TVR 独立 live audit 与 TVR live state。
7. 对 open_trade 执行 TP order + position 双事实 reconcile，完成 OPEN / EXIT / CRITICAL 生命周期日志与 bot 输出。
8. 通过 `logging.summary_interval_secs` 聚合普通循环观测，避免 `open_trade_wait` / `entry_skipped_local_active_symbol` 每 2 秒刷屏。
9. 持仓期间通过 `collection.active_decision_interval_secs` 降低新增候选扫描频率，并只对非 active tradable symbols 请求 live 24h ticker。
10. pending entry 若出现 `PARTIALLY_FILLED`，先按显式 `execution.partial_fill_wait_secs` 等待继续成交；等待到期或 entry TTL 到期后撤剩余挂单，并按已成交数量挂 maker TP。
```
