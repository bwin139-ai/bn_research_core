# Core Anchor Ladder 项目语义基线

本文档是 Core Anchor Ladder（中文名：锚梯策略，简称 `CAL`）的唯一活跃语义基线。

若本文档与 `docs/PROJECT_BASELINE.md` 冲突，以 `docs/PROJECT_BASELINE.md` 为准。

## 1. 策略定位

1. `CAL` 是 Binance USD-M 核心资产永续合约的 LONG-only 策略路线。
2. `CAL` 面向黄金、原油、优质美股映射合约等核心资产，例如 `XAUUSDT`、`CLUSDT`、`BZUSDT`、`MUUSDT`、`NVDAUSDT`、`GOOGLUSDT`。
3. `CAL` 不属于 Snapback / Spring / Sweep-Reclaim 山寨币短周期结构策略，不复用其结构语义、候选池或止损/持仓时间语义。
4. `CAL` 的核心前提是标的存在基本面价值锚，允许在低杠杆、显式本金上限、maker-only、无价格止损前提下做分段式高抛低吸。
5. `CAL` 当前仍受项目 LONG-only 总规则约束，不定义 SHORT、对冲或双向 CTA 语义。

## 2. P0 / 策略 lot 边界

`CAL` 必须区分外部底仓 `P0` 与策略自动 lot `P1/P2/P3`。

### 2.1 P0

1. `P0` 是外部手动底仓。
2. `P0` 可以来自 Binance App / Web，也可以来自本项目账户级管理员门户的手动 LONG 入口。
3. `P0` 不写入 `CAL` strategy state。
4. `CAL` 不为 `P0` 挂 TP，不主动平 `P0`，不使用 `P0` 成本、数量或盈亏计算策略 lot 的入场和止盈。
5. 交易所层面同一 symbol 的 LONG position 会聚合；因此 `CAL` 只能通过本地 lot state、策略专属 `client_order_id`、订单数量与 TP 归属来保证不主动影响 `P0`。

### 2.2 P1 / P2 / P3

1. `P1/P2/P3` 是 `CAL` 自动策略 lot。
2. 每个策略 lot 必须有唯一 `lot_id`，并记录：
   - `ladder_id`
   - `level`
   - `entry_order`
   - `entry_price`
   - `entry_qty`
   - `entry_notional_usdt`
   - `tp_price`
   - `tp_order`
   - `opened_utc_ms`
   - `status`
3. 所有 `CAL` entry / TP order 必须使用策略专属 `client_order_id` 前缀。当前建议前缀为 `CAL`。
4. Reconcile 只能把带有 `CAL` 策略身份且已写入本地 state 的订单归入 `P1/P2/P3`。
5. 任何非 `CAL` client order id 的订单、成交或持仓只能作为外部账户事实，不得自动归入策略 lot。

## 3. Ladder 与锚点

每个账户、每个 symbol 同一时间最多允许一个 active ladder。

### 3.1 H 锚点

1. 当 symbol 没有 active `P1` 时，`CAL` 使用最近 `data.h_anchor_lookback_hours` 根 1h contract bars 的最高价作为 `H`；当前实盘配置为 `24`，即 24H 高点。
2. `CAL` 的 `H` 允许包含当前未闭合 1h bar。该字段只作为核心资产 ladder 的执行锚点，不属于 Snapback / Spring / SWR 的 1m 结构信号语义。
3. `P1` 入场触发条件：

```text
current_price <= H * (1 - p1_drop_pct)
```

4. `current_price` 属于执行生命周期事实，应来自 live 盘口或交易所当前价格，不得写入策略结构 HBs 语义。

### 3.2 P1 entry 锚点

1. 一旦 `P1` 建立并处于 active ladder，该 ladder 的后续加仓锚点固定为 `P1.entry_price`。
2. `P2` / `P3` 不再使用最新 `H` 作为锚点。
3. `P2` / `P3` 入场触发条件：

```text
P2: current_price <= P1.entry_price * (1 - p2_effective_drop_pct)
P3: current_price <= P1.entry_price * (1 - p3_effective_drop_pct)
```

4. 同一时刻每个 level 最多允许一个 active lot。
5. `P1` 是当前 ladder 的主锚点；`P1` active 期间不允许重复开 `P1`。
6. `P2/P3` 是围绕 `P1.entry_price` 的可重复回补 lot；当某个 `P2/P3` lot 已 TP 关闭后，只要 `P1` 仍 active，且当前价格再次满足该 level 的 trigger，允许再次开同一 level。
7. 只有 `P1` 已关闭且该 ladder 的全部策略 lot 都已关闭后，才允许按 `data.h_anchor_lookback_hours` 重新读取最新 `H` 并开启下一轮 `P1`。

### 3.3 P2+ 重复触发下移语义

1. 在同一轮 `P1` active ladder 内，`P2/P3` 每次 `entry -> TAKE_PROFIT` 完成后，必须递增该 level 的 `repeat_count`。
2. `repeat_count` 只在对应 level 的 TP 成交后递增；未成交、未入场、外部平仓、异常关闭不递增。
3. `P1` 关闭且当前 ladder 全部策略 lot 关闭后，所有 `repeat_count` 清零。
4. 每个 level 的 `repeat_drop_step_pct` 以小数比例配置；`0.01` 表示每次重复止盈后后续触发回撤增加 1 个百分点。
5. 某 level 的有效回撤为：自身基础 `drop_pct` 加上从 `P2` 到该 level 的所有累计下移：

```text
P2_effective_drop_pct = P2.drop_pct
                      + P2.repeat_count * P2.repeat_drop_step_pct

P3_effective_drop_pct = P3.drop_pct
                      + P2.repeat_count * P2.repeat_drop_step_pct
                      + P3.repeat_count * P3.repeat_drop_step_pct
```

6. 因此 `P2_repeat_count` 会影响 `P2/P3`，`P3_repeat_count` 会影响 `P3`；未来若显式扩展 `P4/P5`，同理向更深 level 累加。
7. 该语义用于表达同一低吸区域被反复触碰并止盈后，后续补仓网格应逐步下移，避免在被反复打穿的位置机械重复接货。

## 4. 配置语义

第一版 ladder 配置应显式表达每个 level 的入场回撤和本金：

```json
{
  "data": {
    "h_anchor_lookback_hours": 24,
    "h_anchor_refresh_secs": 60
  },
  "ladder": {
    "levels": [
      {"level": "P1", "drop_pct": 0.02, "notional_usdt": 10},
      {"level": "P2", "drop_pct": 0.01, "notional_usdt": 12, "repeat_drop_step_pct": 0.01},
      {"level": "P3", "drop_pct": 0.025, "notional_usdt": 15, "repeat_drop_step_pct": 0.01}
    ],
    "symbol_levels": {
      "SKHYNIXUSDT": [
        {"level": "P1", "drop_pct": 0.02, "notional_usdt": 15},
        {"level": "P2", "drop_pct": 0.01, "notional_usdt": 20, "repeat_drop_step_pct": 0.01},
        {"level": "P3", "drop_pct": 0.025, "notional_usdt": 25, "repeat_drop_step_pct": 0.01}
      ]
    }
  },
  "exit_policy": {
    "symbol_take_profit_pct": {
      "MUUSDT": 0.03,
      "SKHYNIXUSDT": 0.01
    }
  },
  "execution": {
    "symbol_leverage": {
      "MUUSDT": 25,
      "SKHYNIXUSDT": 25
    }
  }
}
```

配置要求：

1. `levels` 必须非空，level 名称不得重复。
2. 第一版固定支持 `P1/P2/P3`，不得隐式扩展到未配置 level。
3. `P1` 必须存在。
4. `P1.drop_pct` 作用于 `data.h_anchor_lookback_hours` 定义的 `H` 锚点；`P2/P3.drop_pct` 作用于 `P1.entry_price` 锚点，因此 `P2.drop_pct` 不要求大于 `P1.drop_pct`。
5. 若同时存在 `P2/P3`，必须满足 `P3.drop_pct > P2.drop_pct`。
6. 每个 level 的 `notional_usdt` 必须显式配置，不允许默认值。
6.1 `P2/P3` 的 `repeat_drop_step_pct` 必须显式配置且为正数；`P1` 不支持该字段。
7. 每个 symbol 必须有显式最大策略本金上限：

```text
sum(open CAL lots entry_notional_usdt for symbol) + next_entry_notional_usdt
<= max_symbol_strategy_notional_usdt[symbol]
```

8. `P0` 本金不计入 `max_symbol_strategy_notional_usdt`，但 live audit 必须记录交易所 LONG position 中存在外部数量的事实。
9. 不同核心资产可以通过 `ladder.symbol_levels` 显式覆盖自己的 ladder 参数。
10. 每个核心资产必须通过 `exit_policy.symbol_take_profit_pct` 显式配置自己的 TP 参数，键必须与 `tradable_symbols` 完全一致。
11. 每个核心资产必须通过 `execution.symbol_leverage` 显式配置自己的杠杆，键必须与 `tradable_symbols` 完全一致；同一账户不同 symbol 允许使用不同杠杆。

## 5. 止盈语义

1. 每个策略 lot 独立止盈。
2. 每个 lot 的 TP 价格固定为：

```text
tp_price = entry_price * (1 + take_profit_pct)
```

3. `P1/P2/P3` 的 TP order 必须各自独立，不能使用账户聚合 LONG position 均价。
4. TP 只允许 `POST_ONLY` maker SELL，当前 Binance USD-M 语义为 `LIMIT + GTX`。
5. TP SELL 数量必须等于对应策略 lot 的剩余数量，不得按账户聚合 position 全量卖出。
6. 同一 ladder 内已存在的 TP 价格必须满足：

```text
P3.tp_price < P2.tp_price < P1.tp_price
```

7. 若某个 level 不存在，只校验已存在 levels 的 TP 单调关系。

## 6. Maker-only 执行

1. 所有 `CAL` BUY entry 必须使用 `POST_ONLY` maker order。
2. 所有 `CAL` SELL TP 必须使用 `POST_ONLY` maker order。
3. 当前 Binance USD-M 对应 `LIMIT + GTX`。
4. 若 entry 因 post-only 约束被拒绝、`EXPIRED` 或 `REJECTED`，必须重新读取 best bid 后继续重试，直到成功挂出 maker entry。
5. 若 TP 因 post-only 约束无法建立，必须进入异常状态，不得裸奔持有策略 lot。
6. 第一版不设置价格止损，不设置 time stop，不做 market flatten。
7. 若 `POST_ONLY` BUY entry 只有部分成交，不撤销剩余 entry，不为部分成交数量提前挂 TP；必须继续等待该 entry 全部成交后，再一次性建立对应策略 lot 与 TP。
8. 若 entry 在 TTL 内完全未成交，可以撤销并清理 pending entry。

## 6.1 实盘 smoke 第一版

第一版 live trader 固定为小账户小资金 smoke：

1. 配置入口：

```text
strategies/cal/config.decision_audit.json
strategies/cal/config.live_trader.stark21.json
```

2. 当前账户为 `stark21`。
3. 当前交易 symbol 为 `MUUSDT` 与 `SKHYNIXUSDT`。
4. 当前默认 ladder notional 为：

```text
P1 = 10 USDT
P2 = 12 USDT
P3 = 15 USDT
```

5. `MUUSDT` 使用默认 ladder，TP 为 `0.03`。
6. `SKHYNIXUSDT` 使用自定义 ladder drop `0.02/0.01/0.025`，notional 为 `15/20/25`，TP 为 `0.01`。

7. 当前 leverage 通过 `execution.symbol_leverage` 按 symbol 显式配置，`stark21` 的 `MUUSDT` 与 `SKHYNIXUSDT` 均为 `25`。
8. live trader 必须显式 `allow_live_order=true` 才允许真实下单。
9. live trader 必须在每轮先 reconcile pending / open lots，再构建新 decision。
10. live trader 信号、入场、开仓、离场必须写 audit、写 stdout log，并推送 bot 消息。
9. entry 因 maker-only 约束挂单失败时，必须重读 best bid 并重试，直到交易所接受 maker entry；非 maker 约束类错误仍必须记录并中断本次 entry。

## 7. 异常与暂停

`CAL` 暂停策略不等于杀进程。进程必须持续运行，继续 reconcile、继续记录事实、继续推送异常，但禁止新 BUY。

触发以下任一情况时，策略必须进入：

```text
PAUSED_BY_INVARIANT_VIOLATION
```

触发条件：

1. 同一 ladder 内 TP 价格顺序不满足 `P3.tp_price < P2.tp_price < P1.tp_price`。
2. `P1` TP 已成交，但仍存在未关闭的 `P2/P3` 策略 lot。
3. `P2` TP 已成交，但仍存在未关闭的 `P3` 策略 lot。
4. 策略 lot 的 TP 订单丢失、被撤销、终态异常或数量不匹配。
5. 本地 state 中存在重复 `lot_id`、重复 active level 或无法归属的 `CAL` client order id。
6. 交易所 LONG position 事实与本地 `CAL` open lots 的最小可解释数量冲突，且无法由外部 `P0` 解释。

进入暂停后：

1. 禁止提交任何新的 `CAL` BUY。
2. 禁止开启新的 ladder。
3. 已有 lot 只允许继续 reconcile。
4. 必须写 live audit / error state。
5. 必须推送 bot `CRITICAL` 消息。
6. 进程必须继续运行，除非遇到项目级 fail-fast 且无法安全读取账户事实。

## 8. P0 共存 precheck

第一版 `CAL` 不要求 symbol flat。

允许的账户事实：

1. 同一 symbol 已有外部 LONG position。
2. 同一 symbol 存在非 `CAL` 历史成交。
3. 同一 symbol 存在非 `CAL` open order，但该订单必须能被解释为外部 `P0` 管理行为，且不会占用或覆盖 `CAL` 自己的 lot / TP 数量。
4. 非 `CAL` `BUY/LONG` open order 视为外部 `P0` 入场，不阻断 `CAL`。
5. 非 `CAL` `SELL/LONG` open order 只在剩余卖出数量不超过估算外部 `P0` 数量时允许共存；若该订单剩余数量会吃到 `CAL` open lots，必须阻断新的 `CAL` BUY。

禁止或需阻断新开仓的账户事实：

1. 同一 symbol 存在无法识别归属、方向不明、`closePosition`，或剩余数量可能影响 `CAL` TP / entry 数量判断的 open order。
2. 同一 symbol 存在其它自动策略 active lot，除非后续文档显式允许共存。
3. 同一 symbol 存在 `CAL` state 外的 `CAL` client order id open order。
4. 账户 position mode、margin type、leverage 不满足显式配置。

## 9. live-first 与验证

1. `CAL` 第一阶段不以历史 backtest 作为参数准入条件。
2. 参数由用户基于核心资产基本面、估值重定价和个人经验显式配置。
3. 不做 backtest 不等于无验证；第一阶段必须先实现 dry-run / audit-only。
4. dry-run / audit-only 必须落盘：
   - 按 `data.h_anchor_lookback_hours` 计算的 `H`
   - `P1.entry_price` anchor
   - 每个 level 的 trigger price
   - 当前 price
   - ready / blocked reason
   - 外部 `P0` position fact
   - `CAL` open lots
   - risk cap usage
5. live 下单前必须先经过 dry-run 同源 decision 构建。
6. 后续若增加 replay / scenario test，只能用于验证状态机和风险边界，不自动改写用户配置参数。

## 9.1 轮询频率

1. `CAL` 不绑定每分钟开头运行。
2. 第一版默认可以按 `collection.interval_secs=10` 高频轮询。
3. 高频轮询的前提是显式核心资产白名单很小，通常只监控 1-2 个 symbol。
4. REST 请求必须继续走 Binance REST Gateway，并保留 quota / ban guard。
5. `H` 锚点默认每 60 秒刷新一次，由 `data.h_anchor_refresh_secs` 显式配置；H 的 1h bar 回看根数由 `data.h_anchor_lookback_hours` 显式配置；不应在每个 10 秒循环中重复拉取 1h bars。
6. 每 10 秒循环只需要刷新盘口、账户 position / open orders、本地 state 与触发判断。
7. 若后续扩大 universe，必须先重新评估 API 消耗，不得静默扩大扫描面。

## 10. 第一阶段工程目标

第一阶段已从 dry-run / audit-only 推进到小账户 live smoke。固定边界如下：

1. 新增 `CAL` 独立配置。
2. 新增 `CAL` dry-run / audit-only decision。
3. 新增 `CAL` maker-only live trader。
4. 读取交易所账户 LONG position / open orders，用于识别外部 `P0` 与阻断异常状态。
5. 判断 `P1/P2/P3` 是否 ready。
6. live trader 只允许 `POST_ONLY` BUY entry 与 `POST_ONLY` SELL TP。
7. 不修改 Snapback / Spring / Sweep-Reclaim / TVR 现有交易语义。

后续 live trader 必须复用公共 Binance execution / Gateway / BN_EXEC 能力，不得私有绕过公共下单入口。
