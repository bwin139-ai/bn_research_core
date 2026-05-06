Spring-SABC 项目语义基线
0. 策略定位

spring-sabc 是一套 LONG-only、1m级别、顺势延续型结构策略。
它不做市场恐慌后的错杀修复，不做 idx 偏离修复，只做 强势合约在上升过程中的洗盘-收回-再延续 机会。

1. 价格语义

spring-sabc 只使用 contract 价格与 contract bars。
不使用 idx，不使用 idx_bars，不讨论 basis。

2. 公共时间语义

spring-sabc 完全继承项目公共语义：

HBs：全部已收盘历史 bars
CB：当前 bar
signal_time = entry_time = CB

spring-sabc 的结构锚点属于策略内部时间锚点，必须全部落在 HBs 中。

3. 结构锚点语义

spring-sabc 使用 S / A / B / C 四个结构锚点。

其中：

A → B：连续洗盘段
B → C：快速收回段
C = HBs[0]
CB = C + 1

因此：

C 是结构完成点
CB 是观察点
CB 也是信号点与入场点
4. Universe 语义

spring-sabc 的 universe 先筛强势票，再做结构检查。
它不再只看 24h_chg_top1，而是从强势候选池中选票。

4.1 Universe 硬条件

候选币必须满足至少以下条件：

24h_chg_pct >= min_24h_chg_pct
24h_quote_volume >= min_24h_quote_volume
symbol 不在 exclude_symbols 中

exclude_symbols 的职责是排除大而老的币种，避免它们长期占据高成交额排名并污染候选池。

4.2 Score 排名语义

对通过硬条件的候选币，定义：

score = rank(chg_24h) + rank(vol_24h)

按 score 从小到大排序，取前 score_top_n 名进入 structure 检查。

注意：

rank(chg_24h) 越大代表涨幅越强，名次值越小
rank(vol_24h) 越大代表成交额越强，名次值越小
score 越小越优

spring-sabc 的候选池不是“唯一龙一”，而是 score 前 N 名强势候选池。

审计字段语义：

```text
rank_chg_24h：通过硬 universe 条件后，按 24h 涨幅降序得到的名次，数值越小越强。
rank_vol_24h：通过硬 universe 条件后，按 24h 成交额降序得到的名次，数值越小越强。
score：rank_chg_24h + rank_vol_24h，是综合分数，不是名次。
score_rank_all：通过硬 universe 条件后的全候选综合排序名次，数值越小越靠前。
selected_score_order：进入 score_top_n 后的 topN 内顺序；未进入 topN 时为 null。
score_order：历史字段，当前语义等同 selected_score_order，不表示全候选排名。
selected_for_structure：是否进入 score_top_n 并被送入 structure 检查。
universe_hard_gate_pass：是否通过 24h 涨幅、24h 成交额、exclude_symbols 等 universe 硬条件。
```

因此：

```text
score=19 不表示第 19 名。
score_order=null 不表示排序失败，只表示没有进入 score_top_n。
判断是否靠前应看 score_rank_all；判断是否进入结构检查应看 selected_score_order / selected_for_structure。
```

5. Structure 总语义

spring-sabc 的核心任务，是在强势候选池中识别 高质量洗盘后快速收回 的微观 1m 结构。

它要找的不是超跌反抽，而是：

强势运行中
主动洗盘
快速收回
准备继续向上延续
6. Structure 观察窗口语义

structure.pattern_window_mins 定义 spring-sabc 允许向前观察和识别结构的最大窗口。
A / B / C 必须在该窗口覆盖的 HBs 中识别。

它属于策略观察窗口，不是运行时投喂窗口。

pre-A 不使用 structure.pattern_window_mins 的左边界作为 S。
pre-A 使用独立固定窗口：

```text
structure.pre_a.window_mins
```

S = A 向左固定 window_mins 后的 pre-A 起点。
同一组 A/B 在不同 C 上被复核时，pre-A 观察区间必须保持一致，不允许随 C 右移导致 pattern window 左边界漂移而改变 pre-A 判定。

7. A→B 连续洗盘语义

A → B 必须是一段 严格连续下跌的洗盘段。

其成立条件至少包括：

ab.chg_pct >= ab.chg_pct_min
连续下跌 bars 数量 >= ab.consecutive_down_bars_min

这里的核心不是 AB 总跨度，而是：

必须连续下跌
必须干脆、干净
不接受中间夹杂反弹 bar 的松散回撤

A → B 表示一次有辨识度的主动洗盘，而不是普通震荡。

8. AB 放量语义

spring-sabc 要求 A → B 洗盘段必须放量。

量能比较不使用 S → A 作为基线，使用固定背景窗口。
定义：

vol_climax.baseline_window_mins：背景量能窗口长度
vol_climax.ratio_min：AB 放量阈值

比较口径为：

AB 段平均每 bar 成交量 / 背景窗口平均每 bar 成交量

要求该比值不低于 vol_climax.ratio_min。

这样做的目的，是让 AB 放量判断具备稳定、统一、可回测的统计口径，不受 S → A 长度不固定的影响。

10. A→C 收回量能确认语义

spring-sabc 除了要求 A→B 洗盘段自身放量，还要求 A→C 收回段相对 A 前等长背景段具备量能确认。

定义：

```text
bars_ac = C 与 A 的 1m bar 距离
γ = A 向左距离 bars_ac 的对称点
vol_gamma_A = γ→A 区间成交额
vol_AC = A→C 区间成交额
gamma_ac_vol_ratio = vol_AC / vol_gamma_A
```

区间口径：

```text
vol_gamma_A = sum quote_asset_volume of (γ, A]
vol_AC = sum quote_asset_volume of (A, C]
```

成立条件：

```text
gamma_ac_vol_ratio >= vol_climax.gamma_ac_vol_ratio_min
```

业务语义：

理想的 Spring 洗盘收回结构中，A→C 收回段应体现资金重新承接。因此 A→C 收回段成交额必须高于 A 前等长背景段成交额。

该字段回答的问题是：

```text
洗盘后的收回，是否比洗盘前等长背景段更有资金参与？
```

它与 AB 爆量互补：

```text
vol_climax.ratio_min：确认 AB 洗盘有强度
gamma_ac_vol_ratio_min：确认 A→C 收回有承接
```

当前基础语义只要求 `gamma_ac_vol_ratio >= 1.0`。后续若要测试更细区间，例如 `1~1.5`、`1.5~2`、`>=2`，必须作为单独参数实验处理，不得把审计分组直接偷换成默认策略语义。

9. B→C 快速收回语义

B → C 必须是一段 短时间内快速收回 AB 跌幅 的修复段。

其成立条件至少包括：

rebound.ratio >= rebound.ratio_min
bc_bars / ab_bars <= rebound.bc_over_ab_bars_max

其中：

rebound.ratio 表示 BC 对 AB 跌幅的收回比例
bc_bars / ab_bars 表示 BC 收回速度相对 AB 洗盘速度的约束

这保证 spring-sabc 找到的是：

洗盘后迅速恢复强势的结构

而不是：

下跌后缓慢、疲弱、拖沓的修复
11. C 点成立语义

当 A → B 连续洗盘、AB 放量、B → C 快速收回全部成立时，
并且 C = HBs[0]，则认为当前 spring-sabc 结构完成。

此时：

C 是最近一个已收盘 bar
结构已在历史中完成
策略在 CB 时刻进行观察并决定是否入场
12. 入场语义

spring-sabc 在 CB 时刻观察 HBs。
若 C = HBs[0] 且完整结构成立，则在 CB 产生信号并执行入场。

因此：

不允许用未来 bars 确认结构
不允许把 C 放在 CB
不允许信号点与入场点脱离 CB
13. 止损语义

spring-sabc 的止损锚点固定为：

stop_loss_price = b_close

不使用 b_low 作为主止损锚点。

这条语义的含义是：

B close 是洗盘结构是否仍被守住的关键价格
若连 b_close 都守不住，则该次 spring 结构大概率失效
b_low 更容易受到瞬时插针噪音影响，不适合作为该策略的主止损锚点

13.1 BREAKEVEN_GUARD 与持仓检查优先级

BREAKEVEN_GUARD 是 Spring-SABC 的保护性离场类型。

其用途不是替代原始 STOP_LOSS，也不是替代 TAKE_PROFIT，而是在交易已经走对一段距离后，将有效止损上移到保本或锁盈位置。

定义：

```text
risk_distance = entry_price - original_sl_price
breakeven_trigger_price = entry_price + risk_distance * trigger_r
breakeven_sl_price = entry_price + risk_distance * floor_r
```

默认语义：

```text
trigger_r = 0.5
floor_r = 0.0
```

即：当持仓曾经达到 +0.5R 后，保护止损上移到 entry_price。若后续触发该保护止损，则离场类型记为：

```text
BREAKEVEN_GUARD
```

13.1.1 live 侧语义

live 主入口是：

```text
on_kline_close(...)
```

因此持仓阶段与信号阶段一致，都是在 CB 时刻观察已经闭合的 HBs。

在 live 场景中，原始 SL / TP 在 signal_time 入场后即已创建为交易所条件单，后续由交易所实时自动触发。

live execution 配置中的并发语义必须显式化：

```text
strategy_concurrency_scope = account:
  同账户同策略只允许一笔 pending/open trade；若已有任意 Spring pending/open trade，
  Live Signal Gate 必须在 Strategy Signal Logic 前阻断新 signal。

strategy_concurrency_scope = symbol:
  同账户同策略只阻断同 symbol 重复 pending/open trade；不同 symbol 可并发。
```

该字段表达策略自身并发约束，不得与 `precheck_scope` 混用。`precheck_scope` 只表达交易所下单前检查范围。

BREAKEVEN_GUARD 则不同：它是在 `on_kline_close(...)` 中，于 CB 时刻观察最近闭合的 HBs[1] 后才执行的保护动作。

若 HBs[1].high >= breakeven_trigger_price，则在当前 CB 执行：

```text
1. 撤销原 STOP_MARKET SL
2. 新建保护性 STOP_MARKET SL
3. 标记 breakeven_guard_armed = true
```

新保护 SL 不允许倒回作用于刚刚用于 armed 判断的 HBs[1]。

13.1.2 sim 侧持仓检查优先级

sim 必须按 live 可执行时序模拟持仓检查。

持仓检查优先级固定为：

```text
第一优先：STOP_LOSS
第二优先：TAKE_PROFIT
第三优先：BREAKEVEN_GUARD armed / BREAKEVEN_GUARD exit
第四优先：TIME_STOP
```

若同一根 bar 同时命中 SL 与 TP，sim 固定采用 STOP_LOSS 优先，作为保守口径。

若同一根 bar 同时满足 SL / TP / BREAKEVEN_GUARD armed 条件，则必须先处理 SL / TP。

原因是：在 live 中，SL / TP 已经真实挂在交易所并可能实时触发；BREAKEVEN_GUARD 的撤旧 SL / 挂新 SL 动作只能等 bar 闭合后的 CB 才发生。若 SL / TP 已经触发，到了 CB 时这笔持仓已经不存在，BREAKEVEN_GUARD 没有执行对象。

13.1.3 禁止同 bar arm + exit

BREAKEVEN_GUARD 不允许在同一根 bar 内先 armed 再触发离场。

某一根 bar 若满足：

```text
high >= breakeven_trigger_price
```

则该 bar 只能将：

```text
breakeven_guard_armed = true
```

从下一根 bar 开始，`breakeven_sl_price` 才能参与 BREAKEVEN_GUARD 离场判断。

这条规则用于保证 sim 与 live 的执行时序一致，避免把“未来在 CB 才会创建的新保护 SL”倒灌到已经闭合的同一根 HBs 中。

13.1.4 当前阶段性结论

截至当前版本，BREAKEVEN_GUARD 的代码语义已经理顺，但阶段性实验结论是：

```text
1. trigger_r = 0.5 明显过早，伤害收益
2. trigger_r = 0.8 较 0.5 明显改善，但整体仍弱于完全关闭 BREAKEVEN_GUARD 的主基线
3. 当前 Spring-SABC 主基线应保持：gamma_ac_vol_ratio_min = 1.0，BREAKEVEN_GUARD disabled
4. 暂停 BREAKEVEN_GUARD 的含义是：保留代码，配置关闭；不要求回退代码实现
```

后续若重新启用 BREAKEVEN_GUARD，只能在本文件已定义的 sim/live 时序语义上继续实验，禁止退回到旧的同 bar arm + exit 错误语义。

13.1.5 TIME_STOP live 执行语义

TIME_STOP 是 Spring-SABC 的持仓阶段保护性离场类型，优先级低于交易所已挂出的 STOP_LOSS / TAKE_PROFIT。

live 侧在每轮 `on_kline_close(...)` 对齐的 CB 时刻，使用最新闭合历史 bar 的 close 作为 TIME_STOP 检查价格。不得使用未来 bar，也不得用未闭合 tick 替代该语义价格。

触发条件固定为：

```text
held_mins >= max_hold_mins
current_profit_pct < time_stop_min_profit_pct
```

其中：

```text
held_mins = floor((current_time_ms - entry_ts) / 60000)
current_profit_pct = latest_closed_close / entry_price - 1
```

若达到 `max_hold_mins` 但 `current_profit_pct >= time_stop_min_profit_pct`，live 只记录检查结果并继续持仓。

若达到 `max_hold_mins` 且收益不足，live 必须按以下顺序执行：

```text
1. 基于交易所事实确认 LONG position 仍存在
2. 撤销本策略持有的 TP / SL 剩余挂单
3. 若撤单过程中发现 TP / SL 已成交，则不得提交 TIME_STOP
4. 撤单成功后提交 LONG market flatten，离场类型记为 TIME_STOP
5. 后续通过公共 reconcile 读取交易所 TS/TP/SL 事实并清理本地 state
```

TIME_STOP 不负责替代 STOP_LOSS / TAKE_PROFIT 的实时保护。入场后的 SL / TP 仍必须优先挂在交易所，由交易所实时触发。

13.2 风险距离与动态开仓金额语义

Spring-SABC 不使用 `risk_pct` 作为信号硬过滤条件。

定义：

```text
risk_pct = (entry_price - stop_loss_price) / entry_price
```

`risk_pct` 表示该笔结构从入场价到原始止损价的价格风险距离。

`entry_price` 属于执行时态，不属于 ABC 结构识别时态。Spring-SABC 的策略结构逻辑只使用 HBs 中已经闭合的 A/B/C 与 universe 指标；不得在 `logic.py` 中用 C_open、C_close 或其它 HBs 字段伪造 `signal.current_price`。价格时态边界固定为：

```text
Strategy Signal Logic:
  产出 A/B/C、SL、take_profit_mode、base_order_notional_usdt、full_notional_risk_pct。
  不产出 signal.current_price，不产出最终 tp_price，不产出基于执行价的 sizing。

sim:
  signal_time = CB，执行参考价为 CB open。

live:
  entry 前即时读取 live pre_entry_price，并落盘 price_source / exchange_snapshot。
  市价 entry 成交后使用真实 entry fill price 作为最终 risk_reward_1r 基线。
```

`take_profit_pct = -1` 时，TP 语义为 `risk_reward_1r`，最终计算必须锚定真实执行入场价：

```text
resolved_tp_price = entry_price + (entry_price - stop_loss_price)
```

对于 LONG，执行层必须保证 `resolved_tp_price > entry_price`。若该条件无法满足，不得提交低于或等于 entry 的 TP 单。

Spring-SABC 的资金管理使用两个显式配置字段：

```text
risk_controls.base_order_notional_usdt
risk_controls.full_notional_risk_pct
```

含义：

```text
base_order_notional_usdt：满额开仓名义金额
full_notional_risk_pct：满额开仓对应的风险距离预算
```

开仓金额计算公式固定为：

```text
sizing_ratio = min(1.0, full_notional_risk_pct / risk_pct)
position_notional_usdt = base_order_notional_usdt * sizing_ratio
planned_sl_loss_usdt = position_notional_usdt * risk_pct
```

因此：

```text
若 risk_pct <= full_notional_risk_pct，则按 base_order_notional_usdt 满额开仓。
若 risk_pct > full_notional_risk_pct，则按比例降低开仓金额。
```

该规则的目的，是保留高波动 SABC 结构的信号机会，同时约束单笔原始 STOP_LOSS 的计划亏损金额。

旧字段 `risk_controls.max_risk_pct` 不再属于 Spring-SABC 活跃语义：

```text
1. 它不再作为信号过滤条件。
2. 它不再作为配置字段出现。
3. 后续不得用 0.99 等参数让旧过滤逻辑“事实失效”。
```

sim 侧交易流水必须落盘动态 sizing 字段，至少包括：

```text
base_order_notional_usdt
full_notional_risk_pct / risk_budget_pct
signal_risk_pct
sizing_ratio
position_notional_usdt
planned_sl_loss_usdt
```

sim 侧绩效统计必须优先使用 `position_notional_usdt` 计算实际 USDT 盈亏。
如果未来 live 启用 Spring-SABC，live 下单数量必须按同一套 `position_notional_usdt / entry_price` 口径对齐。

14. Runtime 与 Structure 的关系

runtime.max_history_window_mins 的职责是：

给 logic 提供足够长的历史 HBs 数据投喂窗口

它不是结构语义本身。

structure.pattern_window_mins、structure.pre_a.window_mins 与 structure.vol_climax.baseline_window_mins 属于策略语义字段。
因此必须满足：

runtime.max_history_window_mins >= max(
  structure.pattern_window_mins + structure.pre_a.window_mins,
  structure.vol_climax.baseline_window_mins
)

若不满足，则属于数据投喂不足，必须 fail-fast。

15. 与 Snapback-SABC 的边界

spring-sabc 与 snapback-sabc 共享同一套项目公共语义与结构型策略骨架，包括：

HBs / CB
结构锚点属于 HBs
signal_time = entry_time = CB
runtime / universe / structure / exit_policy / risk_controls 分层
1m 数据投喂与按 bar 推进方式

但二者的策略世界观完全不同：

snapback-sabc
超跌
恐慌
修复
逆势反抽
使用 idx
spring-sabc
强势
洗盘
收回
顺势延续
只使用 contract

因此二者只共享公共语法与代码骨架，不共享具体结构语义与判定逻辑。



16. ABC 精确搜索算法

本节为 spring-sabc 的唯一 ABC 结构定义，原 `Spring-SABC_ABC结构定义.md` 的内容已并入本文件；旧入口已归档到 `docs/archive/legacy/`，不再作为活跃语义文件。

16.1 C 固定语义

C 固定为 HBs[0]。

16.2 B 搜索语义

从 C 向左，在 `structure.pattern_window_mins` 覆盖的历史窗口内逐根搜索 B。

B 初筛条件只有一个：

C_close > B_close

不使用 `C_low > B_low` 作为硬条件。

16.3 A-B 连跌识别

找到待定 B 后，向左识别 B 所属的 close 严格连续下跌段：

A = 该连续下跌段的最早起点

A 不要求是局部高点。

AB 连跌只比较 close，不比较 low。

16.4 AB bars 约束

AB 必须满足：

ab_bars >= max(
    consecutive_down_bars_min,
    ceil(bc_bars / bc_over_ab_bars_max)
)

16.5 B 低点确认

B_low 必须等于 A-C 区间最低 low。

即 B 必须是从 A 到 C 这段完整洗盘-收回结构的真实最低点。

若 B 之后、C 之前出现任何低于 B_low 的 X 点，则待定 B 失效；算法必须继续向左搜索其它 B，若无其它合法 B，则本轮不产生信号。

该规则用于排除“前面已经砸出深坑，后面只是阴跌但没有再创新低”的伪洗盘结构。

16.6 AB 跌幅

(A_close - B_close) / A_close >= ab.chg_pct_min

16.7 BC 收回

(C_close - B_close) / (A_close - B_close) >= rebound.ratio_min

16.8 AB 爆量

AB 平均成交量 / baseline_window 平均成交量 >= vol_climax.ratio_min

baseline_window 由 `structure.vol_climax.baseline_window_mins` 定义，不使用 S-A 作为量能基线。

16.9 pre-A 固定锚定语义

pre-A 是 A 之前的固定背景观察区间，不是当前 C 的滚动 pattern window 左边界。

```text
S = A 左侧固定 structure.pre_a.window_mins 的起点
pre_a_bars = structure.pre_a.window_mins
pre_a_chg_pct = (A_close - S_close) / S_close
```

pre-A 的 high / low / range / up-down bars / quote volume 等统计，均在该固定 S→A 区间内计算。

该规则用于避免同一组 A/B 结构因为 C 不断右移、pattern window 左边界漂移，而使 pre-A 条件从不合格变为合格。若历史数据不足以提供完整 fixed pre-A window，必须 fail-fast 为 `pre_a_window_insufficient_bars`，不得缩短窗口或回退到 pattern window 左边界。

16.10 A-C 收回量能确认

bars_ac = C 与 A 的 1m bar 距离。

γ = A 向左距离 bars_ac 的对称点。

vol_gamma_A = γ→A 区间成交额。

vol_AC = A→C 区间成交额。

gamma_ac_vol_ratio = vol_AC / vol_gamma_A。

必须满足：

```text
gamma_ac_vol_ratio >= vol_climax.gamma_ac_vol_ratio_min
```

16.10 唯一结构选择

B 从近到远扫描。

第一组完整满足条件的 A-B-C 即为唯一结构。

找到后立即停止，不再比较更远处结构。

17. 1m 落盘数据结构

本节记录 `bn_research_core` 当前 1m K线落盘结构。后续凡涉及 1m contract / idx 数据读取、审计脚本、复盘脚本，不应再猜路径，应以本节为准，或直接复用 `CrossSectionalFeeder`。

17.1 contract 1m 数据

根目录：

```text
data/klines_1m
```

目录结构：

```text
data/klines_1m/{SYMBOL}/{YYYY-MM}.parquet
```

示例：

```text
data/klines_1m/UMAUSDT/2025-05.parquet
```

单个 parquet 文件结构：

```text
index   : RangeIndex
columns : open_time_ms, open, high, low, close, quote_asset_volume, high_idx, low_idx, close_idx
```

字段语义：

```text
open_time_ms        1m bar 开始时间，毫秒时间戳
open/high/low/close contract OHLC
quote_asset_volume  contract quote 成交额
high_idx/low_idx/close_idx 同一 bar 对应的 index price 字段；仅供需要 idx 的策略或审计使用
```

spring-sabc 只使用 contract 字段：

```text
open/high/low/close/quote_asset_volume
```

不使用：

```text
high_idx/low_idx/close_idx
```

17.2 index 1m 数据

根目录：

```text
data/index_klines_1m
```

目录结构：

```text
data/index_klines_1m/{SYMBOL}/{YYYY-MM}.parquet
```

示例：

```text
data/index_klines_1m/1000PEPEUSDT/2025-05.parquet
```

单个 parquet 文件结构：

```text
index   : RangeIndex
columns : open_time_ms, open, high, low, close
```

字段语义：

```text
open_time_ms        1m index bar 开始时间，毫秒时间戳
open/high/low/close index OHLC
```

17.3 读取纪律

独立审计脚本若要读取 1m 数据，优先复用：

```text
core.engine.data_feeder.CrossSectionalFeeder
```

若必须直接读 parquet，则必须按本节目录结构读取：

```text
data/klines_1m/{SYMBOL}/{YYYY-MM}.parquet
data/index_klines_1m/{SYMBOL}/{YYYY-MM}.parquet
```

禁止继续假设以下旧路径：

```text
data/klines_1m/{SYMBOL}.parquet
data/klines_1m/{SYMBOL}/1m.parquet
data/klines_1m/{SYMBOL}/{SYMBOL}.parquet
```

17.4 与 Spring-SABC 的关系

spring-sabc 的价格语义是 contract-only。

因此 spring-sabc 的 sim / audit / visualizer / pre-A 审计默认只读取 contract 1m 数据。

index 数据结构记录在本文件中，是为了避免后续其他策略或历史审计再次重复确认落盘格式，不代表 spring-sabc 使用 idx。

一句话总定义

spring-sabc 是一套在强势候选池中，基于 1m contract bars 识别“连续洗盘 AB + AB 放量 + A-C 收回量能确认 + 快速收回 BC”，并在 C = HBs[0]、CB 时刻入场的顺势延续型 LONG-only 结构策略。
