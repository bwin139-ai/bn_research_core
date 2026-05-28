# 项目基线（宪法）
（`PROJECT_BASELINE.md`）

## 0. 总则
0.1 本基线适用于本项目内全部策略、全部 `sim / live / audit / patch / review / 对话协作`。  
0.2 若某策略需要例外，必须显式写入该策略文档；**未写明即禁止例外**。  
0.3 凡本基线未显式允许的行为，一律视为禁止。  

## 1. 公共语义

### 1.1 K线语义
1.1.1 全部 K 线统一分为：  
- `CB = Current Bar = 当前 bar = 编号 0`  
- `HBs = History Bars = 全部已收盘 bars = 编号 1..N`  

1.1.2 `CB` 永远只有唯一一根，OHLCV 会变化。  
1.1.3 `HBs` 全部已收盘，OHLCV 已固化不变。  
1.1.4 所有策略都必须在 **CB 时刻** 观察 **HBs**。  
1.1.5 所有策略的 `logic.py` / signal 生产层只能读取 HBs 数据事实；不得读取或消费 CB 的 OHLCV、24h 指标、排名、结构字段或其它未闭合时态数据。  
1.1.6 sim / live 上游投喂给策略逻辑的数据必须同样只包含 HBs；CB 数据只允许进入信号之后的执行、撮合、entry price / pre-entry price / 最终 TP 解析等执行生命周期。  
1.1.7 live `data_hub` 生产策略信号输入时，所有进入策略语义的 per-symbol 24h 指标、排名与结构字段必须锚定同一个最新闭合 bar `C=HBs[0]`。
1.1.8 live `data_hub` 的公共候选初筛允许使用 Binance futures 24h ticker 的 `quoteVolume` 作为工程限流事实；该事实只决定是否构建 HBs payload，不得写入策略 `logic.py` 的 per-symbol 24h 指标、排名或结构字段。
1.1.9 Snapback live `market_total_24h_vol` 使用 Binance futures 24h ticker API 计算，属于 live-only 市场总量 gate；该字段不再承诺与 sim 严格一致，sim/live 一致性审计必须将其标记为已知 live-source 例外，而不是 C-anchor 硬字段。

### 1.2 时间字段语义
1.2.1 `signal_time` 只能表示信号生产时间。  
1.2.2 `signal_time` 必须严格等于 `CB` 的时间。  
1.2.3 `entry_time` 只能表示进场时间。  
1.2.4 `entry_time` 必须严格等于 `signal_time`。  
1.2.5 对本项目全部策略，`signal_time = entry_time = CB`。  

### 1.3 策略内部时间锚点
1.3.1 某些策略可能定义自己的内部结构时间锚点，例如 `A / B / C / S`。  
1.3.2 内部结构时间锚点（例如 `A / B / C / S`）必须全部归属于 HBs，即全部属于过去时。
1.3.3 这类内部时间锚点**不是**项目公共语义，禁止默认推广到全部策略。  
1.3.4 若某策略存在内部时间锚点，必须在该策略文档中显式定义其名称、含义、对应 bar 和对应字段。  
1.3.5 未在策略文档中显式定义的内部时间锚点，不得假定存在。  

### 1.4 价格语义
1.4.1 价格统一区分为：`contract` 与 `idx`。  
1.4.2 凡涉及价格字段，必须明确属于哪一类。  
1.4.3 禁止混用或偷换 `contract` 与 `idx`。  

### 1.5 配置语义
1.5.1 所有策略配置统一采用“配置段（section）”概念。  
1.5.2 当前公共配置段为：  
- `strategy_name`  
- `runtime`  
- `universe`  
- `structure`  
- `exit_policy`  
- `risk_controls`  

1.5.3 新增配置段时，必须显式说明职责；禁止隐式扩展旧段语义。  

### 1.6 sim / live 关系
1.6.1 `sim` 是策略语义基线。  
1.6.2 `live` 必须向 `sim` 对齐。  
1.6.3 若发现 `sim` 语义错误，必须先修 `sim`，再重跑验证，再要求 `live` 对齐。  
1.6.4 禁止用 `live` 现实限制掩盖 `sim` 语义错误。  

### 1.7 策略方向语义
1.7.1 本项目策略 alpha 统一只允许 LONG，不允许 SHORT。
1.7.2 Spring / Snapback / SWR / TVR 等策略不定义 SHORT alpha 语义，不实现 SHORT signal / entry / exit / reconcile / state 路径，不接受以 SHORT 策略化为目标的 patch、review、audit、sim、live 设计。
1.7.3 凡涉及策略方向的分析、对话、方案、代码实现、审计与交付，只讨论 LONG。
1.7.4 未显式声明为非策略管理员门户 hedge overlay 例外的字段、分支、行为，若引入 SHORT 语义，一律视为违规。
1.7.5 禁止以“多空对称”“通用化”“未来扩展”为理由向既有策略路径引入 SHORT 相关语义、字段、分支或实现。

### 1.8 live 分层语义
1.8.1 live 侧公共架构分为四段：`Live Data Gate -> Signal Gate -> Strategy Signal Logic -> Execution Lifecycle`。
1.8.2 `Live Data Gate` 是信号生成前的公共数据门禁，负责 loop 调度锚点、expected C / signal_time 推导、hub finalized payload anchor 校验、deadline / stale 防护，以及构造策略可消费的数据事实。
1.8.3 `Signal Gate` 是策略信号逻辑前的公共 live 门禁，负责按 `strategy_name + account + symbol` 维度汇总命令行 active symbols、本策略 pending/open symbols 与本策略 cooldown symbols，并在策略识别信号前阻断这些 symbol。
1.8.4 `Strategy Signal Logic` 是策略个性化信号生成层，只负责读取 HBs/已闭合 bar 数据事实，并运行策略自身的结构识别、过滤、评分、选币与 signal 输出；不得读取 CB/未闭合数据，不得伪造 `current_price`、最终 TP 或其它执行时态价格。
1.8.5 `Execution Lifecycle` 是信号后的公共交易执行生命周期，负责 execution intent、dry-run plan、exchange/local precheck、live pre-entry price、entry / SL / TP、reconcile、time-stop、repair、flatten、projection、cooldown、state 与 audit；对于基于入场价的 TP 语义，必须在真实 entry fill 后用真实 entry price 解析最终 TP。
1.8.6 除策略自身信号生成逻辑外，Snapback live 侧已实现且具备公共语义的 live 能力，必须逐步沉淀为公共模块并供 Spring 及后续 LONG 策略复用；禁止第三、第四策略复制私有 live 闭环。
1.8.7 `signal` 只能表示策略计算后的信号结果；不得用 `Signal Input` 等术语指代信号生成前的数据输入层。

### 1.9 管理员门户语义
1.9.1 `run_manual_trade_bot.py` / `core/manual_trade_bot.py` 当前文件名保留历史命名，但语义定位是账户级管理员门户，不是仅覆盖手动 API 订单的窄义 bot。
1.9.2 管理员门户必须以 Binance 账户事实为主，覆盖 API 手动订单、API 自动策略订单，以及通过 Binance App / Web 产生的订单、成交、持仓、挂单与资金流水。
1.9.3 管理员门户的查询类能力（例如 status / pending orders / history）不得只按本地手动事件或策略 state 判断账户事实；必须优先读取交易所事实，并用本地 state / audit / manual event 只作为 symbol discovery、可读性补充或交叉核查。
1.9.4 管理员门户仍必须遵守项目 LONG-only 基线；若查询到非 LONG 方向或与 LONG-only 冲突的交易所事实，必须显式暴露为异常事实，不得混入正常 LONG 历史或静默忽略。
1.9.5 管理员门户允许存在独立、手动、账户级 `hedge_short` overlay 例外；该例外只服务账户风险对冲，不属于策略 alpha，不得反向调用 Spring / Snapback / SWR / TVR 策略逻辑，也不得写入任何策略 state。
1.9.6 `hedge_short` overlay 必须默认关闭，必须使用独立命令 namespace、独立白名单、独立 current symbol、独立 client order id 前缀、独立 audit/event 落盘；不得复用 `/trade` 的 LONG-only action 分支，不得复用 LONG 当前 symbol。
1.9.7 `hedge_short` overlay 必须同时经过白名单与 current symbol 双闸：白名单决定理论允许做空的 symbol，current symbol 决定当前实际开放的手动做空入口；current symbol 为 `null` / 未设置时，所有做空执行入口必须 fail-fast。
1.9.8 `hedge_short` overlay 第一阶段只允许手动触发，不允许自动策略触发；后续若考虑自动化，必须先另行更新基线与审计边界。

## 2. 行动纪律（铁律）

### 2.1 事实优先
2.1.1 代码、日志、落盘文件、运行结果才是事实。
2.1.2 一切分析、结论、方案必须基于事实。  
2.1.3 禁止臆测。  
2.1.4 缺少事实时，必须先补齐事实，再下结论。    

### 2.2 fail-fast
2.2.1 必须坚持 `fail_fast`。  
2.2.2 禁止带病运行。  
2.2.3 配置缺失、字段缺失、语义冲突、数据异常时，必须中断并报错。  

### 2.3 禁止兜底 / 禁止硬编码 / 禁止伪兼容
2.3.1 禁止默认值兜底掩盖错误。  
2.3.2 配置必须显式提供；缺失就是错误。  
2.3.3 禁止业务语义硬编码。  
2.3.4 临时硬编码调试完成后必须删除。  
2.3.5 禁止向下兼容破坏当前业务语义。  
2.3.6 严禁兼容旧字段、旧字段残留、多套语义并存。  
2.3.7 一条语义只能对应一套字段、一套实现。  

### 2.4 patch 协作
2.4.1 正式 patch 的进入条件、分类约束、交付顺序、最小修改原则，统一严格服从 `STANDARD_PATCH_FRAMEWORK.md`。
2.4.2 若 `STANDARD_PATCH_FRAMEWORK.md` 与本基线冲突，以本基线为准。

### 2.5 表达纪律
2.5.1 提问和回答必须精准、简练。  
2.5.2 不遗漏关键线索。  
2.5.3 不发散、不啰嗦。  
2.5.4 先统一语义，再审代码。  

## 3. 审计纪律

### 3.1 审计工具原则
3.1.1 审计工具严禁猜字段。  
3.1.2 审计工具严禁兼容多个字段名。  
3.1.3 每条语义必须严格命中唯一字段。  
3.1.4 找不到唯一字段时，必须报错。  

### 3.2 审计基准
3.2.1 审计优先看落盘事实。  
3.2.2 单笔审计主键优先使用语义主键。  
3.2.3 审计必须区分：  
- 输入差异  
- 结构差异  
- 执行差异  
- 记账差异  

### 3.3 记账与逻辑分离
3.3.1 必须区分策略逻辑错误与记账字段错误。  
3.3.2 记账字段错误不等于策略逻辑错误。  
3.3.3 记账错误也必须修复，因为它会污染审计与判断。  

## 4. 代码与交付纪律

### 4.1 脚本交付
4.1.1 禁止在聊天窗口输出整段脚本。  
4.1.2 所有脚本、patch、diff 一律走附件。  

### 4.2 patch 脚本约束
4.2.1 patch 脚本不得包含 `/mnt/data/...`。  
4.2.2 patch 脚本必须基于已锁定指纹工作。  
4.2.3 patch 脚本必须在运行时校验基线指纹。  
4.2.4 指纹不一致时必须中断。  
4.2.5 文件唯一身份以 MD5 为准。
4.2.6 Lines 仅作为辅助诊断信息，不作为硬身份条件。

### 4.3 版本与验证
4.3.1 每次 patch 后必须验证：  
- 指纹  
- 编译 / 语法  
- 现场运行结果  

4.3.2 未验证通过，不得进入下一步。  
4.3.3 正式 patch 必须同时具备对齐输入与对齐输出。
4.3.4 未完成对齐输入或对齐输出，不算完整交付。

## 5. 判定优先级
5.1 实现便利性永远不能压过语义正确性。
5.1.1 当前阶段目标只能在不违反公共语义、fail-fast 与事实证据的前提下成立。
5.2 出现冲突时，优先级如下：  
- 公共语义  
- fail-fast  
- 事实证据  
- 当前阶段目标  
- 实现便利性  
  
