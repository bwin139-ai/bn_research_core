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

1. `TVR` 不做传统历史回测 sim 作为第一阶段准入条件。
2. `TVR` 采用 live-first 路线，以 live 交易所事实、live funding、live 盘口与真实 maker 成交作为事实源。
3. live-first 不等于无验证；第一阶段必须先建设数据端，长期采集并落盘事实。
4. 策略交易端可以频繁重启、调参、暂停；`TVR data_hub` 应稳定运行并持续积累数据。

## 3. data_hub 语义

`TVR data_hub` 第一版只采集和落盘事实，不下单，不写 live state，不生成交易 intent。

第一版事实流分为：

1. `universe`
   - 只采集 live 当前 Binance exchangeInfo / ticker 事实。
   - 不回填历史 universe。
   - TradFi universe 必须由交易所当前合约元数据识别；识别不到时 fail-fast。

2. `funding`
   - 当前 funding 使用 Binance `/fapi/v1/premiumIndex` live fact。
   - 历史 funding 可使用 Binance `/fapi/v1/fundingRate` bootstrap。
   - 历史 funding 只用于研究、审计和分布分析，不作为 live 入场门禁依赖。

3. `rolling_24h`
   - 使用 TVR data_hub 自有的原始 1m contract kline store 计算 rolling 24h return 分布。
   - 原始 1m price history 必须支持按 symbol cursor 增量补齐；TVR data_hub 启动时不得一刀切重拉完整决策窗口。
   - live 决策参数可以参考最近窗口统计，但第一版不让统计模块自动改写交易参数。
   - 原始价格事实、cursor state、统计结果和超过决策窗口的历史归档必须落盘，供后续人工审计。

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
7. 第一版 `decision_audit` 使用 data_hub 的 `rolling_24h.latest` 与显式配置的 `entry_drop_pct` 判断当前跌幅是否触发。
8. 第一版 `decision_audit` 必须要求 `history_sufficient=true`，否则该 symbol 禁入并落盘原因。
9. 第一版 `decision_audit` 必须由 JSON 显式配置 `tradable_symbols` 白名单：
   - DataHub 继续采集全部 TradFi universe。
   - 策略侧只允许白名单品种产生候选 intent。
   - 非白名单品种必须落盘 `symbol_not_in_tradable_symbols` 拒绝原因。
   - 白名单为空、重复或包含 DataHub universe 不存在的品种时必须 fail-fast。
10. 第一版 `decision_audit` 只生成 `POST_ONLY_MAKER_*_AUDIT_ONLY` intent；真实 Binance 下单能力必须另起后续 patch 接入。

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
min / max / mean / median / p1 / p5 / p10 / p20 / sample_count
```

其中 `p1/p5/p10/p20` 表示 rolling 24h return 的低位百分位，用于人工校准 `entry_drop_pct`。

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

## 8. 策略侧第一刀目标

```text
实现 TVR decision_audit：
1. 读取最新 universe / funding / price_24h / rolling_24h_stats data_hub facts。
2. 校验 data_hub facts 新鲜度、account、symbol 覆盖和字段可读性。
3. 应用 LONG-only、history_sufficient、funding_rate_entry_max、entry_drop_pct、risk notional cap。
4. 应用 JSON `tradable_symbols` 白名单，非白名单品种只落盘拒绝原因。
5. 生成 audit-only maker LONG intent，不提交订单。
6. 按 TVR 独立 decision audit 目录落盘候选、拒绝原因和 selected_intents。
7. 不查询 Binance 账户，不接执行层，不写交易 live state。
```
