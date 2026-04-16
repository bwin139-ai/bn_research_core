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

5. Structure 总语义

spring-sabc 的核心任务，是在强势候选池中识别 高质量洗盘后快速收回 的微观 1m 结构。

它要找的不是超跌反抽，而是：

强势运行中
主动洗盘
快速收回
准备继续向上延续
6. Structure 观察窗口语义

structure.pattern_window_mins 定义 spring-sabc 允许向前观察和识别结构的最大窗口。
S / A / B / C 必须在该窗口覆盖的 HBs 中识别。

它属于策略观察窗口，不是运行时投喂窗口。

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
14. Runtime 与 Structure 的关系

runtime.max_history_window_mins 的职责是：

给 logic 提供足够长的历史 HBs 数据投喂窗口

它不是结构语义本身。

structure.pattern_window_mins 与 structure.vol_climax.baseline_window_mins 属于策略语义字段。
因此必须满足：

runtime.max_history_window_mins >= max(structure.pattern_window_mins, structure.vol_climax.baseline_window_mins)

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

本节为 spring-sabc 的唯一 ABC 结构定义，原 `Spring-SABC_ABC结构定义.md` 的内容已并入本文件。

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

B_low 必须等于 A-B 区间最低 low。

即 B 必须是这段洗盘的真实最低点。

该规则用于排除“前面已经砸出深坑，后面只是阴跌但没有再创新低”的伪洗盘结构。

16.6 AB 跌幅

(A_close - B_close) / A_close >= ab.chg_pct_min

16.7 BC 收回

(C_close - B_close) / (A_close - B_close) >= rebound.ratio_min

16.8 AB 爆量

AB 平均成交量 / baseline_window 平均成交量 >= vol_climax.ratio_min

baseline_window 由 `structure.vol_climax.baseline_window_mins` 定义，不使用 S-A 作为量能基线。

16.9 A-C 收回量能确认

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