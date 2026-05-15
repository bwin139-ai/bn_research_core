# Exchange History Sync 规格

## 0. 定位

`core/exchange_history_sync.py` 是账户级交易所历史事实同步模块。

它服务管理员门户，不服务策略信号生产。它的职责是把 Binance 账户侧历史事实定时、增量、可恢复地同步到本地落盘文件，使 `/view_history` 等前端查询只读本地账本，允许分钟级延迟。

该模块不改变 LONG-only 策略基线，不提供 SHORT 执行能力。

## 1. 数据范围

同步范围覆盖账户侧交易所事实：

1. `orders`：历史委托，对应 Binance order history。
2. `trades`：历史成交，对应 Binance account trade list。
3. `income`：资金流水，包括 realized PnL、commission、funding fee、transfer 等 income 事实。
4. `transfers`：从 `income` 中按 `income_type=TRANSFER` 派生出的出入金/划转视图。

未来可新增：

1. `positions`：由 orders / trades / income 聚合出的仓位生命周期视图，对应 Binance App 的仓位历史。
2. `short_anomaly`：只读 SHORT 异常审计视图。

## 2. 落盘路径

所有文件落在 runtime state 目录下：

```text
state/exchange_history/{account}/orders/YYYY-MM-DD.jsonl
state/exchange_history/{account}/trades/YYYY-MM-DD.jsonl
state/exchange_history/{account}/income/YYYY-MM-DD.jsonl
state/exchange_history/{account}/transfers/YYYY-MM-DD.jsonl
state/exchange_history/{account}/sync_state.json
state/exchange_history/{account}/symbols.json
```

JSONL 每行必须包含：

```text
source
account
symbol
event_time_ms
event_day_bj
sync_time_ms
sync_time_bj
dedupe_key
raw
```

`raw` 保存当前代码 normalize 后的交易所返回事实。派生字段只用于查询便利，不替代 raw。

## 3. Symbol 发现

Binance USD-M REST 的 `allOrders` / `userTrades` 是按 symbol 查询的接口，不能在管理员门户请求时扫描全市场。

日常同步只扫描已知相关 symbol：

1. 显式命令行传入的 symbol。
2. 当前持仓 symbol。
3. 当前 open orders symbol。
4. `manual_trade_symbols.json`。
5. `state/manual_trade/orders/*.jsonl`。
6. `state/live_audit/*_{account}.YYYY-MM-DD.jsonl` 中真实交易生命周期事件出现过的 symbol。
7. 已落盘 `state/exchange_history/{account}/symbols.json`。

全市场补齐不得作为日常 Telegram 查询路径。若需要全量历史，应走 Binance 异步导出接口并作为单独低频审计任务。

## 4. 增量规则

同步按 account / source / symbol / time window 执行。

1. 默认窗口为最近 24 小时。
2. 每个账户可在 `secrets_{account}.json` 顶层显式配置 `exchange_history_start_time`，格式必须是带时区 ISO 时间字符串，例如 `2026-05-15T00:00:00+08:00`。
3. 若配置了 `exchange_history_start_time`，同步窗口不得早于该时间；该字段是账户历史同步的最早追溯边界，避免追到远古交易。
4. 每次同步带 overlap，避免交易所延迟导致尾部漏记。
5. 写入前按 `dedupe_key` 去重。
6. `sync_state.json` 记录每个 source / symbol 的最近同步窗口。
7. 接口失败必须记录错误并返回非零同步结果；不得伪造空成功。

## 5. 查询语义

管理员门户查询类功能应读本地 exchange history：

1. `/view_history` 的“历史委托”读 `orders`。
2. 后续“历史成交”读 `trades`。
3. 资金费 / 转账读 `income` / `transfers`。
4. 仓位历史读未来 `positions` 聚合视图。

正常视图只展示 LONG-only 事实。若发现 `position_side=SHORT` 或交易所返回方向与 LONG-only 基线冲突，必须进入异常审计区，不得混入正常 LONG 历史。
