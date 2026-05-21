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
5. `balance_snapshots`：账户资产钱包余额快照，对应 Binance futures account 的 per-asset wallet balance。
6. `positions`：由 `trades` 派生出的 LONG 仓位生命周期视图，对应 Binance App 的仓位历史。

未来可新增：

1. `short_anomaly`：只读 SHORT 异常审计视图。

## 2. 落盘路径

所有文件落在 runtime state 目录下：

```text
state/exchange_history/{account}/orders/YYYY-MM-DD.jsonl
state/exchange_history/{account}/trades/YYYY-MM-DD.jsonl
state/exchange_history/{account}/income/YYYY-MM-DD.jsonl
state/exchange_history/{account}/transfers/YYYY-MM-DD.jsonl
state/exchange_history/{account}/balance_snapshots/YYYY-MM-DD.jsonl
state/exchange_history/{account}/positions/YYYY-MM-DD.jsonl
state/exchange_history/{account}/sync_state.json
state/exchange_history/{account}/symbols.json
```

JSONL 每行必须包含：

```text
source
account
symbol
asset
event_time_ms
event_day_bj
sync_time_ms
sync_time_bj
dedupe_key
raw
```

`raw` 保存当前代码 normalize 后的交易所返回事实。派生字段只用于查询便利，不替代 raw。

`balance_snapshots` 每个资产一行，`asset` 表示资产币种，`raw` 保存 Binance account `assets[]` 中该资产的原始行。规范化字段中的 `wallet_balance` 是该资产的钱包余额数量，例如 USDT 数量，不是折算美元价值。

`positions` 是派生视图，不替代交易所原始事实。正常行只包含 `position_side=LONG` 的闭合仓位，状态为 `CLOSED`；若存在平仓成交但本地 trades 缺少对应开仓事实，必须写 `status=INCOMPLETE` 与 `incomplete_reason`，不得伪造 entry price / open time。

## 3. Symbol 发现

Binance USD-M REST 的 `allOrders` / `userTrades` 是按 symbol 查询的接口，不能在管理员门户请求时扫描全市场。

日常同步采用 `income-first active symbols`：

1. 每轮先同步账户级 `income` / `transfers`。
2. 从本轮 income 查询窗口内返回的非空 `income.symbol` 提取 `active_sync_symbols`。
3. 本轮只对 `active_sync_symbols` 拉取 `orders` / `trades`。
4. `state/exchange_history/{account}/symbols.json` 只作为历史出现过的 symbol 索引，只增量维护，不作为每轮 `orders` / `trades` 的 API 扫描输入。
5. 显式命令行 `--symbol` / `--symbol-file` 只作为人工补查入口，可临时追加到本轮 `active_sync_symbols`；日常 loop 不依赖它。

`income` 是账户级接口，不需要按 symbol 查询，覆盖系统内订单与系统外订单产生的账户资金事实。零成交的 `CANCELED` / `EXPIRED` 订单没有 income 事实，不纳入当前“完整交易账本”的完整性要求。

全市场补齐不得作为日常 Telegram 查询路径。若需要全量历史，应走 Binance 异步导出接口并作为单独低频审计任务。

## 4. 增量规则

同步按 account / source / symbol / time window 执行；每轮执行顺序固定为先写入 `balance_snapshots`，再同步 `income/transfers`，再由本轮 income 的 `active_sync_symbols` 同步 `orders/trades`，最后从已落盘 `trades` 派生 `positions`。

1. 默认窗口为最近 24 小时。
2. 每个账户可在 `secrets_{account}.json` 顶层显式配置 `exchange_history_start_time`，格式必须是带时区 ISO 时间字符串，例如 `2026-05-15T00:00:00+08:00`。
3. 若配置了 `exchange_history_start_time`，同步窗口不得早于该时间；该字段是账户历史同步的最早追溯边界，避免追到远古交易。
4. 若账户尚无任何 `sync_state` source 进度，首次同步必须从 `exchange_history_start_time` 起步，而不是只拉默认 24 小时。
5. `--bootstrap` 是一次性历史回填模式：必须配置 `exchange_history_start_time`，并忽略已有 per-source cursor，从该起点重新扫描；写入仍按 `dedupe_key` 去重。该模式不得与 `--loop` 同用。
6. Binance `orders` / `trades` 历史接口存在最大查询窗口限制；同步层必须切成小窗口执行，不得把大跨度历史一次性提交给交易所。
7. `income` 同步也必须切成小窗口执行，避免单次 `limit=1000` 截断污染完整性；若 1 天窗口命中 `limit=1000`，同步层必须自动继续拆分为更小窗口，直到窗口低于 limit 或达到最小拆分窗口。达到最小拆分窗口仍命中 limit 时必须 fail-fast，不得视为完整结果。
8. 每次同步带 overlap，避免交易所延迟导致尾部漏记。
9. 写入前按 `dedupe_key` 去重。
10. 每轮必须先成功写入 `balance_snapshots`；若余额快照失败，本轮不得继续推进 `income/orders/trades` cursor。
11. 若 `income` 同步失败，本轮不得继续用不完整 symbol 集合同步 `orders/trades`。
12. `sync_state.json` 记录每个 source / symbol 的最近同步窗口；失败窗口不得把 cursor 推进到未成功覆盖的 `end_ms`。
13. 接口失败必须记录错误并返回非零同步结果；不得伪造空成功。
14. `positions` 只从已落盘 `trades` 派生，不推进交易所 cursor；若派生发现 SHORT 或无法解释的成交顺序冲突，必须返回非 ok。

## 5. 余额审计语义

余额连续性审计以 `balance_snapshots` 与 `income` 为事实源：

```text
wallet_end(asset) = wallet_start(asset) + sum(income.amount by asset)
```

`income` 已包含 `REALIZED_PNL`、`COMMISSION`、`FUNDING_FEE`、`TRANSFER`、`API_REBATE`、`REFERRAL_KICKBACK` 等资金变化事实。审计时不得再额外叠加 `trades.realized_pnl`，避免重复计算；`trades` 只用于交叉核查：

```text
sum(trades.realized_pnl) ≈ sum(income[REALIZED_PNL])
sum(trades.commission) ≈ -sum(income[COMMISSION])
```

审计工具入口：

```text
python audit_tools/exchange_history/audit_exchange_history_continuity.py --account mybwin139 --pretty
```

该工具只读本地 `state/exchange_history/{account}`，不访问 Binance，不修改 state。余额连续性不匹配、realized PnL 交叉核查不匹配、commission 交叉核查不匹配均返回非零退出码；资产余额快照数量不足只作为 warning。

## 6. 查询语义

管理员门户查询类功能应读本地 exchange history：

1. `/view_history` 的“历史委托”读 `orders`。
2. 后续“历史成交”读 `trades`。
3. 资金费 / 转账读 `income` / `transfers`。
4. 仓位历史读 `positions` 派生视图。
5. `/view_history` 必须显示本地账本的最近同步时间，提醒用户该视图允许分钟级延迟。

正常视图只展示 LONG-only 事实。若发现 `position_side=SHORT` 或交易所返回方向与 LONG-only 基线冲突，必须进入异常审计区，不得混入正常 LONG 历史。

## 7. 运行方式

同步模块支持单次运行、常驻 loop 与多账户串行运行：

```text
python -m core.exchange_history_sync --account mybwin139
python -m core.exchange_history_sync --account mybwin139 --loop --interval-secs 300
python -m core.exchange_history_sync --account mybwin139 --bootstrap
python -m core.exchange_history_sync --account mybwin139 --account chen912 --loop --interval-secs 300
```

`--symbol` 可重复传入，用于人工补查额外 symbol；`--symbol-file` 可重复传入，用于人工补查一批额外 symbol。二者未传入时，本轮 `orders` / `trades` 只使用本轮 income 返回的 `active_sync_symbols`。

该模块对 Binance 历史接口使用 `LOW` priority，并在每次历史请求后按 `--request-sleep-secs` 主动 sleep，默认允许延迟以减少对 live 策略接口额度的挤压。多个 `--account` 会在同一进程内按顺序执行，账户之间按 `--account-sleep-secs` sleep，禁止用多个 bootstrap 进程并发抢 REST quota。

常驻模式每轮仍走同一增量与去重逻辑，输出一行 JSON 结果，适合由 `nohup` / systemd / supervisor 管理日志。
