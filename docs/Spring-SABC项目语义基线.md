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
10. C 点成立语义

当 A → B 连续洗盘、AB 放量、B → C 快速收回全部成立时，
并且 C = HBs[0]，则认为当前 spring-sabc 结构完成。

此时：

C 是最近一个已收盘 bar
结构已在历史中完成
策略在 CB 时刻进行观察并决定是否入场
11. 入场语义

spring-sabc 在 CB 时刻观察 HBs。
若 C = HBs[0] 且完整结构成立，则在 CB 产生信号并执行入场。

因此：

不允许用未来 bars 确认结构
不允许把 C 放在 CB
不允许信号点与入场点脱离 CB
12. 止损语义

spring-sabc 的止损锚点固定为：

stop_loss_price = b_close

不使用 b_low 作为主止损锚点。

这条语义的含义是：

B close 是洗盘结构是否仍被守住的关键价格
若连 b_close 都守不住，则该次 spring 结构大概率失效
b_low 更容易受到瞬时插针噪音影响，不适合作为该策略的主止损锚点
13. Runtime 与 Structure 的关系

runtime.max_history_window_mins 的职责是：

给 logic 提供足够长的历史 HBs 数据投喂窗口

它不是结构语义本身。

structure.pattern_window_mins 与 structure.vol_climax.baseline_window_mins 属于策略语义字段。
因此必须满足：

runtime.max_history_window_mins >= max(structure.pattern_window_mins, structure.vol_climax.baseline_window_mins)

若不满足，则属于数据投喂不足，必须 fail-fast。

14. 与 Snapback-SABC 的边界

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

一句话总定义

spring-sabc 是一套在强势候选池中，基于 1m contract bars 识别“连续洗盘 AB + 放量 + 快速收回 BC”，并在 C = HBs[0]、CB 时刻入场的顺势延续型 LONG-only 结构策略。