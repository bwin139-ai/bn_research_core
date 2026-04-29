# 当前项目状态
（`CURRENT_STATE.md`）

更新时间：2026-04-29

## 0. 文档定位

本文件只记录 **当前现场状态**。它回答：

```text
我们当前正在做什么、已经做到哪里、下一步做什么。
```

本文件不是项目宪法，也不是 patch 交付框架。若发生冲突，优先级固定为：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`

详细阶段流水账见：

```text
docs/archive/reports/2026-04-28_codex_5day_progress_summary.md
```

---

## 1. 当前项目定位

### 1.1 当前仓库

```text
bn_research_core
```

### 1.2 当前活跃主线

当前不是单一策略开发现场，而是以下几条主线并行：

1. `data_hub` 与 `live` 协同稳定化。
2. `snapback-sabc` 的 live 观察与 sim/live/bn 审计闭环。
3. `spring-sabc` 的主基线、结构过滤与审计工具完善。
4. 1m / idx 数据质量、hub-vs-klines 对表与基础设施审计。
5. Codex 多线程交接文档体系。

### 1.3 当前阶段目标

```text
让 live 数据链路、hub 共享数据、策略信号、交易执行、审计落盘与文档交接都进入可复核、可续接、可长期维护状态。
```

---

## 2. 当前固定入口与现场路径

### 2.1 文档入口

```text
AGENTS.md
docs/README.md
docs/PROJECT_BASELINE.md
docs/STANDARD_PATCH_FRAMEWORK.md
docs/CURRENT_STATE.md
```

新线程用户侧可复制模板：

```text
docs/新Codex线程开场白.txt
```

### 2.2 配置入口

```text
market_data_hub_config.json
strategies/snapback/config.highfreq.json
strategies/spring/config.json
```

### 2.3 live / audit 现场

```text
state/live/snapback_mybwin139.state.json
state/live_audit/snapback_mybwin139.jsonl
output/live_projection
```

---

## 3. 最近 5 天总账结论

本节从 commit `b2b3e88dc95d8192682fb45c79d15bd329a1d94f` 之后到当前 HEAD `a6608e2` 的本地 git 历史归纳。

### 3.1 Codex 协作体系

已完成：

1. 新增 Codex 协作文档体系。
2. 新增根入口 `AGENTS.md`。
3. 精简 `docs/README.md` 与 `docs/新聊天开场白.md`。
4. 将旧协作文档、旧入口、旧审计报告归档到 `docs/archive/`。
5. 明确新线程启动纪律：不依赖旧聊天记忆，先读仓库文档与现场事实。

当前结论：

```text
线程可以切换；项目状态必须沉淀在仓库文档、代码、日志、state、落盘输出和 git 历史中。
```

### 3.2 data_hub / live 协同

已完成：

1. `market_data_hub_config.json` 改为显式 schema，缺少关键字段必须 fail-fast。
2. `data_hub` 补齐完整 json 字段，并增加配置快照发布能力。
3. hub snapshot 由 shared hub 集中管理。
4. live 侧从 hub bars 获取 universe 指标，降低 live 与 hub 指标源漂移。
5. 增加 finalized symbols 读取能力，审计工具可读取 shared hub finalized 结果。
6. full universe rollsum refresh、shared bars 增量 refill、部分 rollsum window refill 已推进。
7. 增加 finalize quality stats 与 hub health stats，可用于现场观察。
8. 增加 Binance REST quota / ban window / API stats 相关保护与观测。

当前配置事实：

```text
market_data_hub_config.json:
- enabled = true
- min_24h_quote_volume = 30000000
- history_window_mins = 180
- rollsum_refresh_batch_size = 80
- exclude_symbols 已显式配置，包含大市值币与 TradFi 品种
```

当前 pending：

1. 继续观察是否长期无 `-1003` / ban window 风险。
2. 继续确认 live 消费 hub 数据后，sim/live 指标口径是否仍有残余偏离。
3. 继续补齐人工审计可读性，例如 prefilter 原始名单与 snapshot 字段解释。

### 3.3 1m / idx 数据质量

已完成：

1. 修复 `klines_1m_store.py` backfill 中 `end_ms` 未定义问题。
2. contract 写入时保持 idx 字段为空，避免伪 idx。
3. klines rewrite 时保留真实 idx 字段，避免覆盖已有真实 idx。
4. static index price 400 场景 fail-fast。
5. 新增/增强 delisted confirmed symbols 记录。
6. 新增 1m 数据质量与 idx completeness 审计能力。
7. hub-vs-klines 审计改为 overlap only，并对 cache window / finalized symbols 做了对齐。
8. 增加 augment idx 增量同步能力。

当前 pending：

1. data_hub、klines_1m、idx 字段三者仍需持续审计对表。
2. 若再改 1m 写入链路，必须明确 contract 与 idx 字段责任，禁止伪兼容。

### 3.4 snapback-sabc live / audit

已完成：

1. `config.highfreq.json` 已进入 live 测试参数状态。
2. live `max_history_window_mins`、`market_total_24h_vol_min` 等参数多轮调整。
3. live 侧已对齐 snapback market vol source。
4. stage5 audit 的 market vol gate 与 pandas import 问题已修复。
5. live bot message 增加完整日志。
6. live projection / live_signals / live_trades / bn truth / triplet audit 主线已经打通。
7. ENTRY 后 SL / TP 建立顺序与 SL 保护失败 fail-fast 风险修补已完成。
8. 实盘账户 live config 已新增/调整。
9. snapback sim 配置新增显式 `risk_controls.base_order_notional_usdt`，sim 信号与交易流水不再依赖后处理默认 100U。

当前配置事实：

```text
strategies/snapback/config.highfreq.json:
- runtime.max_history_window_mins = 150
- universe.24h_quote_volume_min = 30000000
- universe.market_total_24h_vol_min = 20000000000
- structure.s_to_c_window.mins = 60
- structure.election_rule = drop_pct_top1
- exit_policy.time_stop.max_hold_mins = 4
- risk_controls.base_order_notional_usdt = 100
```

当前 pending：

1. 持续做 snapback sim/live 一致性验证。
2. 继续明确 live 使用 hub 后与 sim feeder 的 universe / 24h_vol 口径差异。
3. 继续明确 snapback sim `base_order_notional_usdt` 与 live `entry_notional_usdt` 的账户资金口径关系。
4. 是否为 bn truth 增加条件委托 / algo 父单独立真相层，尚未决定。
5. triplet audit 是否显式解释父单 ID 与基础子单 ID 差异，尚未决定。

### 3.5 Spring-SABC

已完成：

1. Spring-SABC 主语义基线已集中到 `docs/Spring-SABC项目语义基线.md`。
2. sim decision audit context 已落盘，支持 pre-A / gamma volume 等审计工具读取。
3. 增加 rebound ratio 上限过滤。
4. 增加 pre-A structure filters。
5. pre-A trend 当前要求非负。
6. Spring 配置曾对齐 1924 基线，并多轮调整 `max_risk_pct`；回测事实显示硬过滤收紧到 `0.12/0.10/0.08` 没有改善综合表现。
7. 当前配置中 BREAKEVEN_GUARD 关闭，保留代码语义但不作为当前主基线启用。
8. `max_risk_pct` 已从 Spring 活跃语义中删除；风险距离改为动态开仓金额计算依据。
9. Spring live 侧启动第一刀架构边界：新增公共 LONG-only live execution intent contract，并新增 Spring signal -> execution intent adapter。
10. 新增 Spring observe-only live runner：只读取 hub finalized candidate inputs，调用 Spring sim 同源逻辑，校验 execution intent 并落盘观察 projection；不触交易所、不下单。
11. Spring observe-only live runner 增加正式 loop 模式：支持按分钟边界运行、限制迭代次数、写 heartbeat；仍然不触交易所、不下单、不维护订单生命周期。
12. 新增 `strategies/spring/config.observer_loose.json`，仅用于 observe-only 链路压测和尽快覆盖 signal -> execution intent 路径；不得作为 Spring 策略基线或绩效结论。
13. 新增公共 dry-run execution plan：`core/live/execution_plan.py` 消费 LONG-only execution intent，产出 orphan/local/exchange precheck、quantity、client order id、SL/TP/time-stop plan 与 state transition plan；不调用交易所、不写 live state。
14. Spring observer 支持可选只读 exchange verified dry-run：`--dry-run-verify-exchange` 会读取交易所 positions/open orders 与本地 live state，用于验证 orphan/precheck；仍然不下单、不写 live state。
15. 新增公共 live execution runner：`core/live/execution_runner.py` 消费已验证 LONG intent + execution plan + 外部 live execution JSON；显式执行 entry MARKET、SL-first、TP-after-SL、state/audit/cooldown，SL 提交失败时按配置提交 market flatten。
16. Spring runner 增加显式 `--execute-live` + `--live-execution-config` 一次性实盘入口；默认仍不下单，且 `--execute-live` 当前只支持 once 模式，不支持 loop 常驻。
17. 新增 `strategies/spring/config.live_smoke_10u.json` 与 `strategies/spring/live_execution.smoke_10u.json`，用于 10U 小仓位实盘 smoke；所有实盘执行参数从 JSON 读取，代码不内置 10U、杠杆、重试、冷却等测试参数。
18. 2026-04-29 15:05 BJ，已在阿里云 `mybwin139` 执行一次 Spring 10U / 5x 实盘 smoke：`SKYAIUSDT` entry 成交，Spring `SPR_SL` 与 `SPR_TP` 保护单建立成功。
19. 2026-04-29 15:09-15:10 BJ，确认 Spring smoke 发生跨策略串线 incident：Snapback live 捕获并维护了 Spring open_trade，取消 Spring `SPR_TP/SPR_SL`，提交 Snapback `SNP_TS` time-stop 并完成离场。
20. 2026-04-29 已提交、推送并部署 Spring/Snapback live state ownership 隔离 patch：`11d1b22 live: isolate strategy state ownership`。
21. 2026-04-29 20:17 BJ，重启 3 个 Snapback live 进程后执行 Spring 10U / 5x live smoke：`SKYAIUSDT` entry 成交，`SPR_SL` 与 `SPR_TP` 提交成功；随后交易所真相显示 `SPR_TP` 立即成交、`SPR_SL` 自动 EXPIRED，仓位与挂单为空。未复现 Snapback 串线，但暴露 Spring state 缺少 post-entry reconcile / exit monitor，Spring state 仍记录 `OPEN`。
22. 本地已补 Spring live once 的公共 post-entry reconcile：`core/live/execution_runner.py` 在 entry/SL/TP 建立后立即查询 LONG position 与 symbol open orders；若交易所仓位和挂单均为空，则查询 TP/SL/TS 订单事实，推断 exit reason，写 Spring audit event，并清理 strategy-specific `open_trade`。

当前配置事实：

```text
strategies/spring/config.json:
- strategy_name = spring-sabc
- max_history_window_mins = 130
- min_24h_chg_pct = 30
- min_24h_quote_volume = 50000000
- score_top_n = 3
- gamma_ac_vol_ratio_min = 1.0
- rebound.ratio_min = 0.75
- rebound.ratio_max = 1.2
- pre_a.chg_pct_min = 0.0
- take_profit_pct = -1
- max_hold_mins = 60
- breakeven_guard.enabled = false
- base_order_notional_usdt = 100
- full_notional_risk_pct = 0.99
```

当前 pending：

1. 基于 `SPRING_V1_30D_P6_0427T1606_ALL` 作为结构毕业候选，重跑动态 sizing 后的正式 sim。
2. 继续审计 Spring-SABC 坏月份 / 坏 regime，尤其 2026-04。
3. 若再调整 Spring 结构过滤或 sizing 参数，必须同步评估审计工具是否需要扩展。
4. Spring/Snapback live state ownership 隔离 patch 已提交并部署，20:17 smoke 未复现 Snapback 接管。
5. Spring live 后续如要常驻实盘，仍需补循环式 open_trade reconcile / exit monitor / time-stop monitor；当前本地 post-entry reconcile 只覆盖 live once 入场后的一次即时对账。
6. Snapback live 不得维护、取消、离场或写入非 `SNP` 策略的 open_trade；Spring live 不得写入 Snapback state 文件。
7. 在本地 post-entry reconcile 完成部署验证前，不建议继续做新的 Spring 实盘 smoke；否则交易所已平而 Spring state 仍可能保留 `OPEN`。

已确认 incident：

```text
2026-04-29 Spring/Snapback live state ownership 串线

现场事实：
- Spring 真实开仓：
  - symbol = SKYAIUSDT
  - entry_client_order_id = x-7Qv8Kw2S_SPR_EN_0429150516_95e6a6
  - sl_client_order_id = x-7Qv8Kw2S_SPR_SL_0429150516_95e6a6
  - tp_client_order_id = x-7Qv8Kw2S_SPR_TP_0429150516_95e6a6
  - Spring audit 文件 = state/live_audit/spring_sabc_mybwin139.jsonl
- Snapback live 后续事件：
  - 15:09:11 BJ: time_stop_cancel_tp_ok 取消 x-7Qv8Kw2S_SPR_TP_0429150516_95e6a6
  - 15:09:11 BJ: time_stop_cancel_sl_ok 取消 x-7Qv8Kw2S_SPR_SL_0429150516_95e6a6
  - 15:09:12 BJ: time_stop_submitted 提交 x-7Qv8Kw2S_SNP_TS_0429150516_95e6a6
  - 15:10:07 BJ: position_closed_detected 记录 exit_reason = TIME_STOP
- 复查时交易所 positions/open orders 均为空，state 中 SKYAIUSDT open_trade 已清空。

初步根因：
- core/live/live_state.py 当前文件名固定为 live/snapback_{account}.state.json。
- Spring execution_runner 复用 load_live_state/save_symbol_state/set_open_trade，导致 Spring open_trade 写入 Snapback state namespace。
- Snapback live maintain loop 读取同一个 state namespace，未按 strategy_code/client_order_id 策略归属过滤 open_trade，于是把 SPR open_trade 当作 Snapback 仓位维护。

影响：
- Spring 实盘 smoke 的离场由 Snapback live 执行和记录。
- 离场 time-stop client id 使用 SNP，而 entry/SL/TP 使用 SPR。
- 当前 2026-04-29 这笔 smoke 不再是纯 Spring live execution lifecycle 样本，只能作为跨策略串线 incident 样本。

incident 记录时的下一刀建议：
1. live_state namespace 必须 strategy-specific，例如 spring_sabc_{account}.state.json / snapback_{account}.state.json。
2. public live execution contract 必须显式携带 strategy_name/strategy_code/state_namespace。
3. Snapback maintain/reconcile 必须拒绝维护非 SNP open_trade/order_root/client_order_id。
4. Spring 在拥有独立 state 与 reconcile/exit monitor 前，不再执行新的实盘下单。
```

当前 ownership 修复状态：

```text
Patch 分类：LOGIC_ONLY

已提交并部署：
- commit = 11d1b22 live: isolate strategy state ownership
- core/live/live_state.py
  - live state 文件名改为 strategy-specific。
  - 默认 snapback 仍写 live/snapback_{account}.state.json。
  - Spring 显式写 live/spring_sabc_{account}.state.json。
- core/live/execution_runner.py
  - Spring live execution state 写入使用 intent.strategy_name。
  - open_trade / pending_entry payload 写入 strategy_name 与 strategy_code。
- strategies/spring/run_live_observer.py
  - dry-run local_state_snapshot 读取 spring-sabc namespace。
- strategies/snapback/trade_consumer.py
  - Snapback pending/open_trade 写入 SNP 归属字段。
  - Snapback reconcile 发现非 SNP 或未知归属 payload 时阻断并写 audit event，不取消、不平仓、不接管。

本地验证：
- python3 -m py_compile core/live/live_state.py core/live/execution_runner.py strategies/spring/run_live_observer.py strategies/snapback/trade_consumer.py
- live_state 临时目录写入验证：snapback_acct.state.json 与 spring_sabc_acct.state.json 分离。
- Snapback ownership helper 验证：SNP=true，SPR/spring-sabc/unknown/mixed=false。

服务器部署验证：
- 阿里云 `/root/bn_research_core` HEAD = 11d1b22。
- 3 个 Snapback live 进程已于 2026-04-29 20:14 BJ 重启并加载新代码。
- 服务器 py_compile 通过。
```

20:17 Spring live smoke 事实：

```text
run_id = SPRING_SMOKE_LIVE_0429T2017
symbol = SKYAIUSDT
signal_time_bj = 2026-04-29 20:17:00
order_root = 0429201742_c1b964
entry_client_order_id = x-7Qv8Kw2S_SPR_EN_0429201742_c1b964
sl_client_order_id = x-7Qv8Kw2S_SPR_SL_0429201742_c1b964
tp_client_order_id = x-7Qv8Kw2S_SPR_TP_0429201742_c1b964

交易所真相：
- ENTRY: FILLED, avg_price = 0.27252, qty = 37
- TP: FILLED, avg_price = 0.27277, qty = 37, realized_pnl = 0.00925
- SL: EXPIRED
- 复查 positions/open orders 均为空。

state / audit 结论：
- state/live/spring_sabc_mybwin139.state.json 中 SKYAIUSDT open_trade.strategy_code = SPR，status = OPEN。
- state/live/snapback_mybwin139.state.json 中 SKYAIUSDT 无 open_trade。
- Snapback audit 未出现取消本次 SPR_SL/SPR_TP 或提交 SNP_TS 的记录。

当前风险：
- Spring one-shot execution runner 已补一次即时 post-entry reconcile，但尚无循环式 Spring reconcile / exit monitor。
- 若 TP/SL 在即时 reconcile 之后才成交或失效，Spring state 仍不会自动从 OPEN 同步为 CLOSED。
- 下一刀若推进常驻实盘，应补公共循环式 reconcile / exit monitor / time-stop monitor，再做新的 Spring live smoke 或常驻实盘。
```

当前 Spring live 架构事实：

```text
总体架构边界：
- Snapback 当前仍是老结构：信号识别、下单、持仓维护、reconcile、离场落盘集中在策略自己的 live/consumer 代码中。
- Spring live 正在走新结构：策略层只负责 signal -> ValidatedLiveExecutionIntent adapter；交易生命周期能力沉到公共 LONG-only live execution lifecycle。
- `core/live/execution_intent.py` 只是公共 contract 入口，不承载全部生命周期逻辑。
- 公共 live execution lifecycle 的完整目标是：
  signal adapter -> ValidatedLiveExecutionIntent -> execution_plan -> entry/SL/TP -> strategy-specific state/audit -> open_trade reconcile -> TP/SL/TS exit_reason -> state close -> live_trades/projection -> cooldown。
- 未来第三、第四套 LONG 策略应只新增自己的 signal adapter、strategy_name/strategy_code/config，复用公共 live execution lifecycle；不得复制 Snapback 老式策略私有交易生命周期。
- 后续 Spring post-entry reconcile / exit monitor / time-stop monitor 应继续补在公共 LONG-only live execution lifecycle 中，而不是写成 Spring 私有闭环。
- Snapback 若未来迁移到公共层，必须单独拆刀；当前不得在 Spring 修复刀中混改 Snapback 架构。

core/live/execution_intent.py:
- 定义 ValidatedLiveExecutionIntent
- 只允许 LONG
- fail-fast 校验 strategy/account/symbol/time/price/notional/SL/TP/hold/time-stop/signal_snapshot

core/live/execution_plan.py:
- 定义 dry-run execution plan
- 输入 ValidatedLiveExecutionIntent
- 输出 local/exchange precheck、quantity、client order ids、entry/SL/TP/time-stop plan、state transition plan
- exchange snapshot 未提供时标记 exchange_precheck_not_verified，计划不可执行但仍可审计
- 不调用 Binance、不写 live state

core/live/execution_runner.py:
- 定义配置驱动 live execution runner
- 输入 ValidatedLiveExecutionIntent + execution plan + 外部 live execution JSON
- 要求 execution config 与 intent 的 strategy/account/side/mode 对齐
- 要求 local/exchange precheck verified，支持 account_flat 级别 orphan 阻断
- 调用 Binance 执行 entry MARKET、SL STOP_MARKET closePosition、TP LIMIT
- 入场后先建 SL，SL 成功后才建 TP
- SL 建立失败时按 JSON 中 `stop_loss_failure_action=submit_market_flatten` 提交 market flatten
- 写 live state pending/open_trade/cooldown/error 与 live audit event
- 当前覆盖 entry、保护单建立、live once 入场后的即时 post-entry reconcile；尚未覆盖循环式 open_trade reconcile / exit monitor / time-stop monitor

core/live/audit_log.py:
- 保留既有 snapback audit 写入入口
- 新增 strategy-specific audit 写入入口，Spring live execution event 写入 `spring_sabc_{account}.jsonl`

strategies/spring/live_execution.py:
- 定义 SPRING_LIVE_STRATEGY_CODE = SPR
- 将 Spring-SABC signal 显式转换为公共 execution intent
- 要求 signal.action = BUY
- 使用 signal.position_notional_usdt 作为 live 下单名义金额来源

strategies/spring/run_live_observer.py:
- Spring live runner 入口
- 读取 shared hub finalized_candidate_inputs
- 调用 SpringSABCStrategy.on_kline_close(...)
- signal 存在时生成并校验公共 execution intent
- signal 存在时生成 dry_run_execution_plan 并落盘
- 支持 `--dry-run-verify-exchange` 读取只读交易所快照与本地 live state 快照
- 支持显式 `--execute-live --live-execution-config ...` 一次性真实下单
- 写入 output/live_projection/spring_observer.{run_id}.jsonl
- 支持 `--loop`、`--max-iterations`、`--signal-check-second`
- 写入 output/live_projection/spring_observer_heartbeat.{run_id}.json
- 默认不下单；只有 `--execute-live` 与外部 live execution JSON 同时满足时才会触发真实交易

strategies/spring/config.observer_loose.json:
- observe-only 专用 loose 配置
- 主用途是放宽 universe/structure 门槛，尽快产生 signal 样本以验证 execution intent 落盘路径
- 不得用于正式 sim/live 策略基线判断

strategies/spring/config.live_smoke_10u.json:
- 小仓位实盘 smoke 专用 Spring 策略配置
- 继承 loose signal 门槛以尽快出信号
- `base_order_notional_usdt = 10`
- 不得作为 Spring 策略基线或绩效结论

strategies/spring/live_execution.smoke_10u.json:
- 小仓位实盘 smoke 专用 live execution contract
- `execution_mode = live_once`
- `allow_live_order = true`
- `precheck_scope = account_flat`
- `max_position_notional_usdt = 10.0`
- `leverage = 5`
- 要求 local/exchange/symbol filters 均 verified
```

### 3.6 audit tools / 目录治理

已完成：

1. audit scripts 已按 data_quality / spring / snapback / maintenance 等方向整理。
2. `make_md5_line_suffix_copies.py` 等工具增强。
3. 常用审计命令与常用命令文件持续维护。
4. 2026-04-28 已执行一次服务器磁盘清理：删除 mybwin139 旧 stage3 parquet、旧 stage3 enriched 日文件，并清理 Spring 回测审计中除 `SPRING_V1_30D_P6_0427T1606` 外的历史 decision audit。清理后服务器可用空间从约 14G 提升到约 43G。

当前服务器清理纪律：

```text
清理历史 state/audit/output 前必须做二次快照稳定性检查：

1. 第一次记录候选文件 path / size / mtime / inode / run_id。
2. 间隔至少 10 秒后再次记录同一候选集合。
3. 只有两次 path / size / mtime / inode 完全一致，且无活跃进程或 open file handle，才可删除。
4. 当前日期、当前 run_id、仍在增长的文件、live/data_hub/backtest 活跃产物，默认不删。
5. 若发现关联长跑进程，必须先报告进程与文件关系，等待用户明确授权后才能 stop/kill/restart。
6. 删除后必须复查磁盘空间、剩余文件和是否仍有同 run_id 新文件生成。
```

当前保留事实：

```text
output/state/spring_decision_audit.SPRING_V1_30D_P6_0427T1606*.jsonl
```

当前注意：

```text
当前仍有未提交本地改动；新线程开始时必须先看 git status，不要误把它当成已提交事实：

- tools/常用命令
- tools/常用命令-过去.txt
- docs/新Codex线程开场白.txt
```

---

## 4. 当前明确不做

1. 不引入 SHORT 语义、字段、分支或实现。
2. 不把旧归档报告反向覆盖当前活跃文档。
3. 不再依赖旧聊天记忆推进长任务。
4. 不在未锁基线时进入正式 patch。
5. 不在一个 patch 中混合性能、结构、逻辑多个目标。
6. 未经批准，不 `git push`，不碰生产发布。

---

## 5. 下一步建议顺序

### 5.1 data_hub / live 协同

```text
1. 读取 hub snapshot / market snapshot / live logs，确认当前最新现场。
2. 对齐 live 消费 hub bars 后的 universe / 24h_vol 与 sim feeder 口径。
3. 继续观察 REST quota / ban window 状态。
4. 必要时补充 snapshot 可读性，不先改策略逻辑。
```

### 5.2 snapback-sabc sim/live 一致性验证

```text
1. 锁定一个 bar_ts / symbol 样本。
2. 对齐 sim 输入、hub 输入、live stage 输入。
3. 对比 24h_vol / market_total_24h_vol / stage5 fail reason / signal。
4. 只在偏离事实明确后进入单问题 patch。
```

### 5.3 Spring-SABC

```text
1. 固定当前 config 事实。
2. 用动态开仓金额语义重跑 `0427T1606` 候选基线，确认收益、回撤与 2026-04 表现。
3. 若要改 pre-A / rebound / sizing 参数，先形成语义说明，再做单问题 patch。
```

### 5.4 文档

```text
1. 每个长任务结束前判断是否更新 CURRENT_STATE.md。
2. 阶段性审计结论写入 docs/archive/reports/。
3. 新线程必须从 AGENTS.md + docs/README.md + CURRENT_STATE.md 恢复现场。
```
