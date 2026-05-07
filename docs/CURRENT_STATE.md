# 当前项目状态
（`CURRENT_STATE.md`）

更新时间：2026-05-08

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
4. `sweep-reclaim` 的新策略语义基线与参数骨架。
5. 1m / idx 数据质量、hub-vs-klines 对表与基础设施审计。
6. Codex 多线程交接文档体系。

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
docs/SNAPBACK_SIM_LIVE_AUDIT_SPEC.md
docs/Sweep-Reclaim项目语义基线.md
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
strategies/sweep_reclaim/config.json
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
9. `market_data_hub` 的 candidate / finalized / market snapshot 已按账户与北京时间日期落盘到 `state/live_audit/market_data_hub/{account}/daily/YYYY-MM-DD/`，当前无需纳入 live audit 主文件分片 patch。
10. 2026-05-08 修复严格 C-anchor 后的策略依赖分层：`data_hub` 不再用 Snapback 专属的 `market_total_24h_vol` market-wide ready gate 阻断 Spring / SWR 共享 finalized candidate 输入；当 market-wide rollsum 仍 warming 时，hub 不用 realtime ticker 24h 裁剪策略可见 universe，而是直接用交易所 TRADING universe 生产锚定 C 的已闭合 HBs `cross_section/full_df/finalize_summary`，由策略 `logic.py` 执行自身 24h 与结构过滤。
11. 2026-05-08 修复 `rollsum_refresh_batch_size=80` 与 strict C-anchor market-wide ready gate 的不可收敛问题：该 batch size 只作为 hub-owned rollsum 后台刷新节流参数，不再作为 Snapback `market_total_24h_vol` 的唯一 ready 路径。若 rollsum 未 ready，hub 从 finalized C-anchor HBs `cross_section.vol_24h` 聚合得到 `market_total_24h_vol_source=candidate_cross_section_hbs` / `status=ready_candidate_cross_section_hbs`，Snapback live 延后到读取 finalized payload 后再执行 market_total gate；DataHub 的 market_total / Binance REST quota 30 轮统计在 warming/not-ready 时也继续采样推送，并携带 status/source 与 missing/partial/stale/newly-listed counts。

当前配置事实：

```text
market_data_hub_config.json:
- enabled = true
- min_24h_quote_volume = 30000000
- history_window_mins = 180
- rollsum_refresh_batch_size = 80
- rollsum_refresh_batch_size 仅为 hub-owned rollsum 后台刷新节流参数，不是全市场同锚 ready 语义保证
- exclude_symbols 已显式配置，包含大市值币与 TradFi 品种；market-wide rollsum 未 ready 时，hub shared payload 会使用交易所 TRADING universe 生产 C-anchor HBs 输入，再由策略自身过滤
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
9. `klines_1m_store.py` 默认交易所 universe 已从“排除 USDC”修正为仅接受 `quoteAsset=USDT` 且 symbol 以 `USDT` 结尾；手工 symbols、force include、confirmed delisted 与本地 shard 目录发现非 USDT symbol 时 fail-fast。

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
10. 2026-04-29 已完成 `Snapback_SmokeTest_0429T2229` 与 mybwin139 live 重叠窗口一致性审计：sim 16 笔信号全部在 live 中按 `(symbol, c_time)` 匹配；2 笔 live-only（`IRUSDT 15:48 C`、`DAMUSDT 15:49 C`）已确认为 17:00 BJ 交易所 delist 前后的已解释样本，不继续追查。
11. live audit 主事件与 stage audit 已改为按北京时间日期分片落盘：`state/live_audit/snapback_{account}.YYYY-MM-DD.jsonl`、`state/live_audit/{strategy}_{account}.YYYY-MM-DD.jsonl`、`state/live_audit/stage_audit/snapback_{account}.{stage}.YYYY-MM-DD.jsonl`，便于后续按日期做 retention 清理。
12. 2026-04-30 已完成 `c8d8689 live: wait snapback finalized payload anchor` 部署后验证：北京时间 15:00 后三账户共 9 笔 snapback live 信号（`mybwin139` 5 笔、`junjie2026` 2 笔、`chen912` 2 笔）全部满足 `bar_bj = c_bar_bj + 1min`，未再出现旧问题中的 `C+2m` 消费；9 笔均为 `candidate_payload_wait_ok=true`，且 `expected_latest_closed_bar_ts / expected_signal_time_ts` 与 finalized payload 实际 anchor 匹配。当前观测表明该 patch 已把 live 消费约束回正确的 finalized payload anchor；实际 `signal_detected / entry_submitted` 仍发生在 `bar_bj` 后约 33-42 秒，这是等待 hub finalize 完成后的预期时序，不是旧的一分钟漂移。
13. 2026-04-30 已定位 RAVEUSDT 17:59 BJ 在 `junjie2026` / `chen912` 的 live 离场未输出问题：交易所 truth 显示两账户 `RAVEUSDT` LONG position 已归零、open orders 为空、TP order 为 `FILLED`、SL algo order 为 `EXPIRED`，但本地 `state/live/snapback_{account}.state.json` 仍保留 `symbols.RAVEUSDT.open_trade.status=OPEN`。根因是 `run_live._run_once()` 在 `market_total_24h_vol < market_total_24h_vol_min` 时早退，早退点位于 `build_consumer_reconcile_plan()` / `prepare_consumer_loop_gate()` / open_trade reconcile 之前；18:31 后两账户每分钟只记录 `market_total_24h_vol_below_min_skip`，导致已有仓位 TP/SL 同步被阻断。已将该 gate 改为“reconcile 后阻断新扫描”：低于市场总量阈值时仍先维护已有 pending/open trade，再跳过新信号扫描。
14. 2026-04-30 已复盘 `HUSDT 18:15 BJ` 在 `junjie2026` / `chen912` 的 `SL fail -> TIME_STOP` 表象：两账户 entry 后提交 SL 均被 Binance 拒绝 `APIError(code=-2021): Order would immediately trigger.`，随后代码进入 `entry_sl_fail_flatten` 风险分支。现场价格对比：C bar=`2026-04-30 18:14 BJ`，C close=`0.17015`，`C+1m` open=`0.17016`，signal current_price=`0.17015`，resolved SL=`0.16837356`，C close 到 SL 仍有约 `1.06%` 空间；但实际 entry 发生在 `18:15:37 BJ` 左右，`junjie2026` entry=`0.1681068`、`chen912` entry=`0.1679185`，分别已低于 SL 约 `0.16%` / `0.27%`，因此 SL submit 必然被交易所视为立即触发风险。已完成两步修补：一是新增 live pre-entry price guard，在提交市价 entry 前即时读取并落盘 `pre_entry_price`，价格源与 SL `workingType=CONTRACT_PRICE` 对齐；若 `(pre_entry_price - resolved_sl_price) / resolved_sl_price` 小于显式配置阈值，则跳过 entry 并写 audit/stage 记录。二是将 SL 提交失败后的应急平仓从正常 `TIME_STOP` 语义中拆出：client order id leg 使用 `SNP_SF`，BN exec / audit / live trade reason 使用 `SL_SUBMIT_FAILED_FLATTEN`，同时保留原 `time_stop_*` 订单身份字段用于既有 reconcile 查询，避免已有状态机路径失联。
15. 2026-05-01 已优化 Snapback live Telegram bot 消息的可读格式，不改变交易语义、下单顺序、state/audit 字段或执行风控。消息统一为多行头 `[HH:MM:SS 🦅 SNP] {account}`，其中时间取具体业务事件时间：signal 使用 `signal_time`，ENTRY/SL/TP 使用交易所订单创建/更新时间，离场使用 `trade_row.exit_time`。雷达、ENTRY、SL、TP、开仓、离场消息均保留原核心字段，但拆为多行展示；Snapback 使用 `SNP`，公共 BN_EXEC 通知会从 client order id 识别 `SNP`/`SPR`，并在第二行追加 `【BN_EXEC】` 标记以区分公共执行层消息与策略侧消息。
16. 2026-05-06 已增强 Snapback live stage2 universe 审计可观测性：`contract_24h_metric_empty` 等 24h metric 缺口不再只落归一化 fail reason，stage2 现在同时记录 `metric_frame_present`、`metric_frame_empty`、`metric_frame_rows`、`metric_frame_min/max_ts/bj`、`metric_frame_contains_c_bar`、`contract_metric_reason` 与 `contract_metric_prefetch_error`。该 patch 不改变 universe 过滤语义，仅用于后续 sim/live 一致性审计还原 per-symbol 合约 K 线 metric prefetch / frame 构建失败原因。
17. 2026-05-06 已增强 Snapback live `finalized full_df -> candidate_cross_section -> stage4/stage5` 审计链：finalize summary 增加 `full_df_only_symbol_count/full_df_only_symbols`；当 finalize refresh 的 `full_df` 已有 symbol 但 `cross_section` 缺 symbol 时，写 `c_bar_finalize_cross_section_missing` event；进入策略逻辑前若仍存在 `full_df` 有、`cross_section` 无的 symbol，写 `candidate_cross_section_missing_after_finalize` event，并在 `stage4_input_snapshot` 与 `stage5_structure_audit` 中为该 symbol 落 `input_pass_to_logic=false`、`fail_reason=missing_from_cross_section_after_finalize`、C bar full_df 快照、candidate error/stale reason 与 finalize passed/delayed 标记。该 patch 不改变 Snapback 信号或交易语义，只用于避免 AIOTUSDT 这类 sim 有信号、live 无 stage4/stage5 现场时无法定位缺席边界。
18. 2026-05-06 已将 data hub `finalized_candidate_inputs` 的 candidate finalize probe 间隔从 2 秒加固为 3 秒，仍保持两轮快照一致视为闭合、deadline 为 `signal_time+50s`。原因是 SKYAIUSDT 暴露出 `B=C` 同 bar 时 `c_index_low/b_index_price` 伪闭合会直接改变 `current_price > b_index_price`、`rebound_ratio` 与 SL，影响策略事实；本刀先用最小方式降低 index low 伪闭合概率，后续若继续复现再升级到三次一致或 near-deadline 关键字段复核。
19. 2026-05-06 新增 `docs/SNAPBACK_SIM_LIVE_AUDIT_SPEC.md` 作为 Snapback sim/live 一致性审计长期规格文档。后续新线程做 Snapback sim/live 审计时，应按该文档统一审计窗口、输入文件、signal/ABC 匹配、硬字段与降权字段、live-only/sim-only 分类、stage audit 定位顺序与 live trading lifecycle 检查口径；其中 `c_index_close` 默认记录但降权，`c_index_low/b_index_price` 在 `B=C` 同 bar 时为核心硬字段。

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
live_config.*.json:
- pre_entry_min_sl_distance_pct = 0.003
- `pre_entry_min_sl_distance_pct` 属于 live 执行风控配置，已由 `run_live.py` / `run_consumer.py` 的 live config loader 校验；`core/config_loader.py` 仅校验策略语义配置，不承接该字段。
```

当前 pending：

1. 持续做 snapback sim/live 一致性验证。
2. `Snapback_SmokeTest_0429T2229` 的 4 笔历史 C 点 `close_idx / basis_c_pct` 偏差审计已形成结论：`IRUSDT 2026-04-29 04:42 C` 与 `LYNUSDT 2026-04-29 16:37 C` 是 candidate 初始 index 快照即与事后 Binance 历史值不同，finalize round 1 连续两次相同后毕业；`AIOTUSDT 2026-04-29 07:32 C` 与 `BROCCOLI714USDT 2026-04-29 20:06 C` 在 finalize probe 中发生过改写，但最终毕业值仍与事后 Binance 历史值不同。4 笔均确认 candidate/finalize 阶段为 index cache miss，即当时重新请求了 Binance `/fapi/v1/indexPriceKlines`；当前交易所历史值与本地 parquet / sim 输入一致，不与 live 当时 hub 值一致。结论：snapback 结构逻辑与 `klines_1m` 不是第一嫌疑，偏差来自 hub 对 index C bar 的工程近似判定，即连续两次 index 快照相同就视为 finalized；Binance API 当前没有直接提供“index C bar 已最终稳定”的确定事实。现阶段不改逻辑，继续跟踪该类早期/未稳定 index 快照复现概率。
3. 继续明确 snapback sim `base_order_notional_usdt` 与 live `entry_notional_usdt` 的账户资金口径关系。
4. 是否为 bn truth 增加条件委托 / algo 父单独立真相层，尚未决定。
5. triplet audit 是否显式解释父单 ID 与基础子单 ID 差异，尚未决定。
6. 部署 market-total gate 顺序 patch 后，需要重启 snapback live 进程并确认 `junjie2026` / `chen912` 的 `RAVEUSDT` stale open_trade 能在下一轮 reconcile 中投影为 `TAKE_PROFIT` 离场并清空 state。
7. 部署 live pre-entry SL distance guard 与 `SL_SUBMIT_FAILED_FLATTEN` 独立离场语义后，观察是否出现 `pre_entry_price_guard_skip`；若仍发生 SL submit fail，应确认落盘 reason / custom id / 通知不再混入正常 `TIME_STOP`。

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
10. 新增 Spring projection-only live runner：只读取 hub finalized candidate inputs，调用 Spring sim 同源逻辑，校验 execution intent 并落盘观察 projection；不触交易所、不下单。
11. Spring projection-only live runner 增加正式 loop 模式：支持按分钟边界运行、限制迭代次数、写 heartbeat；仍然不触交易所、不下单、不维护订单生命周期。
12. 新增 `strategies/spring/config.live_loose.json`，仅用于 projection-only 链路压测和尽快覆盖 signal -> execution intent 路径；不得作为 Spring 策略基线或绩效结论。
13. 新增公共 dry-run execution plan：`core/live/execution_plan.py` 消费 LONG-only execution intent，产出 orphan/local/exchange precheck、quantity、client order id、SL/TP/time-stop plan 与 state transition plan；不调用交易所、不写 live state。
14. Spring live runner 支持可选只读 exchange verified dry-run：`--dry-run-verify-exchange` 会读取交易所 positions/open orders 与本地 live state，用于验证 orphan/precheck；仍然不下单、不写 live state。
15. 新增公共 live execution runner：`core/live/execution_runner.py` 消费已验证 LONG intent + execution plan + 外部 live execution JSON；显式执行 entry MARKET、SL-first、TP-after-SL、state/audit/cooldown，SL 提交失败时按配置提交 market flatten。
16. Spring runner 增加显式 `--execute-live` + `--live-execution-config` 一次性实盘入口；默认仍不下单，且 `--execute-live` 当前只支持 once 模式，不支持 loop 常驻。
17. 新增 `strategies/spring/config.live_smoke_10u.json` 与 `strategies/spring/live_execution.smoke_10u.json`，用于 10U 小仓位实盘 smoke；所有实盘执行参数从 JSON 读取，代码不内置 10U、杠杆、重试、冷却等测试参数。
18. 2026-04-29 15:05 BJ，已在阿里云 `mybwin139` 执行一次 Spring 10U / 5x 实盘 smoke：`SKYAIUSDT` entry 成交，Spring `SPR_SL` 与 `SPR_TP` 保护单建立成功。
19. 2026-04-29 15:09-15:10 BJ，确认 Spring smoke 发生跨策略串线 incident：Snapback live 捕获并维护了 Spring open_trade，取消 Spring `SPR_TP/SPR_SL`，提交 Snapback `SNP_TS` time-stop 并完成离场。
20. 2026-04-29 已提交、推送并部署 Spring/Snapback live state ownership 隔离 patch：`11d1b22 live: isolate strategy state ownership`。
21. 2026-04-29 20:17 BJ，重启 3 个 Snapback live 进程后执行 Spring 10U / 5x live smoke：`SKYAIUSDT` entry 成交，`SPR_SL` 与 `SPR_TP` 提交成功；随后交易所真相显示 `SPR_TP` 立即成交、`SPR_SL` 自动 EXPIRED，仓位与挂单为空。未复现 Snapback 串线，但暴露 Spring state 缺少 post-entry reconcile / exit monitor，Spring state 仍记录 `OPEN`。
22. 2026-04-29 已提交、推送并部署 Spring live once 公共 post-entry reconcile patch：`d56d5b9 live: reconcile spring post-entry exits`。`core/live/execution_runner.py` 在 entry/SL/TP 建立后立即查询 LONG position 与 symbol open orders；若交易所仓位和挂单均为空，则查询 TP/SL/TS 订单事实，推断 exit reason，写 Spring audit event，并清理 strategy-specific `open_trade`。
23. 2026-04-29 21:10 BJ，服务器 `/root/bn_research_core` 已拉取 `d56d5b9` 并用 `/root/service_env/bin/python` 完成 py_compile。随后用新 post-entry reconcile 逻辑处理 20:17 Spring smoke 残留：交易所 position/open orders 为空，`SPR_TP` 为 `FILLED`、`SPR_SL` 为 `EXPIRED`，Spring audit 写入 `spring_position_closed_detected` 与 `spring_state_cleared_after_exit`，`state/live/spring_sabc_mybwin139.state.json` 中 `SKYAIUSDT.open_trade` 已清空。
24. 2026-04-29 21:11 BJ，Spring 10U smoke dry-run 仍有 `SKYAIUSDT` 信号，交易所 precheck 为空仓无挂单，但 local precheck 因 `cooldown_until_bj = 2026-04-30 00:17:00` 返回 `local_cooldown_active`，因此未继续执行新的实盘下单。
25. 2026-04-29 21:18 BJ，服务器启动 Spring 10U smoke watcher：`pid=4138786`，脚本 `output/live_projection/spring_smoke_live_watch.sh`，日志 `output/live_projection/spring_smoke_live_watch.20260429T131846Z.log`。watcher 每分钟先 dry-run，只有 `ok_to_execute=true` 才调用 `--execute-live`；当前首轮仍只有 `SKYAIUSDT` signal，因 `local_cooldown_active` 等待，未触发 live 下单。
26. 2026-04-29 21:29 BJ，watcher 发现 `AIOTUSDT` 可执行并完成 10U live smoke：ENTRY filled qty=101 avg=0.09517，SL submitted stop=0.09216，TP submitted price=0.10495。后续交易所事实显示 SL filled avg=0.0921878、TP expired；21:38 BJ 手动调用 post-entry reconcile 后，Spring state 清理，exit_reason=`STOP_LOSS`。
27. 2026-04-29 21:35 BJ，watcher 又发现 `TACUSDT` 可执行并完成 10U live smoke：ENTRY filled qty=612 avg=0.0163321，SL submitted stop=0.016236，TP submitted price=0.016418。后续交易所事实显示 TP filled avg=0.016418、SL expired；21:38 BJ 手动调用 post-entry reconcile 后，Spring state 清理，exit_reason=`TAKE_PROFIT`。
28. 2026-04-29 21:38 BJ，因 smoke 目标已覆盖且 watcher 已连续开出两笔真实交易，已停止临时 watcher `pid=4138786`。复查交易所 positions/open orders 为空；Spring state 中 `AIOTUSDT`、`TACUSDT`、`SKYAIUSDT` 均无 open_trade / pending_entry_order，仅保留各自 cooldown。
29. 2026-04-29 已提交、推送并部署 Spring live loop patch：`15eecc6 live: run spring execution loop`。该 patch 允许 `--loop --execute-live`，每轮先对 Spring state 全部 open_trade 做交易所事实 reconcile，再执行 signal scan；普通 precheck blocker 记录为 `execution_blocked_by_precheck`，不再让 loop 崩溃；新增账户级 Spring local active gate，防止其它 symbol stale/open state 时继续开下一笔。
30. 2026-04-29 21:59 BJ，服务器用 `/root/service_env/bin/python` 跑新版本 `--loop --execute-live --max-iterations 1` 验证通过：本轮 signal=`SKYAIUSDT`，`dry_run_execution_plan.ok_to_execute=false`，blocker=`local_cooldown_active`；`lifecycle_reconcile` 返回无剩余 open/pending，`account_local_precheck` 为空，`live_execution_result.outcome=execution_blocked_by_precheck`。交易所 positions/open orders 仍为空；Spring state 无 open_trade/pending_entry，仅保留 `AIOTUSDT/SKYAIUSDT/TACUSDT` cooldown。
31. 本地已推进 Spring active time-stop patch：`core/live/execution_runner.py` 的 loop reconcile 在 LONG position 仍存在时，会使用 Spring live runner 从 hub `full_df` 提取的最新闭合 C close 检查 `max_hold_mins / time_stop_min_profit_pct`；到期且收益不足时先撤 TP/SL，再提交 `SPR_TS` market flatten，并设置 `exit_submit_inflight`，后续仍由同一公共 reconcile 根据 TP/SL/TS 交易所事实清理 state。
32. 本地已推进 Spring open_trade bracket verify/repair patch：loop reconcile 发现 LONG position 仍 open 且未处于 `exit_submit_inflight` 时，会校验本策略 TP/SL 是否仍存在于交易所 open orders；若缺失则按 `open_trade.tp_price / sl_trigger_price` 与当前 position qty 补挂，补挂后再次查询 open orders 验证。补挂或验证失败会写 state error / audit 并 fail-fast 保留 open_trade，账户级 local active gate 会继续阻止新开仓。
33. 2026-05-01 文档 checkpoint：下一刀将把 Spring 的 SL submit failed emergency flatten 从普通 TIME_STOP 语义拆出，对齐 Snapback 的独立 protective flatten 语义。目标字段/事件：exit reason=`SL_SUBMIT_FAILED_FLATTEN`，custom id leg=`SPR_SF`，BN exec order_role=`SL_SUBMIT_FAILED_FLATTEN`，audit event=`spring_sl_submit_failed_flatten_submitted` / `spring_sl_submit_failed_flatten_filled`，并在后续 terminal projection 中保留 `protective_flatten_*` 字段。
34. 本地已推进 Spring SL submit failed protective flatten patch：SL 提交失败后的应急平仓使用 `SPR_SF` client id leg 与 `SL_SUBMIT_FAILED_FLATTEN` BN exec order_role；open_trade 同时写 `time_stop_exit_reason` 与 `protective_flatten_*` 字段；后续 reconcile 若该 flatten 订单成交，exit_reason 落为 `SL_SUBMIT_FAILED_FLATTEN` 并写 `spring_sl_submit_failed_flatten_filled` audit。
35. 2026-05-01 已按 Snapback Telegram 新标准补齐 Spring live 策略侧消息格式，不改变交易语义、下单顺序、state/audit 字段或执行风控。Spring 策略侧消息使用多行头 `[HH:MM:SS 🌱 SPR] {account}`；signal 使用 `signal_time`，开仓确认使用 entry 交易所订单事件时间，离场确认使用 exit 交易所订单事件时间。公共 BN_EXEC ENTRY/SL/TP 消息已由 `core/live/binance_exec.py` 从 `SPR` client order id 识别并在第二行追加 `【BN_EXEC】`。Spring live execution config 显式新增 `notify_enabled` / `notify_on_signal_locked` / `notify_on_order_submit` / `notify_on_exit_detected` / `notify_on_order_error`。
36. 2026-05-01 本地已推进 Spring live lifecycle 对齐 Snapback 基线 patch：`strategies/spring/run_live.py` 将 hub payload anchor 校验收紧为 `signal_time_ts == latest_closed_bar_ts + 60000`，锁死 Spring `CB=C+1m`；`core/live/execution_runner.py` 补齐 Spring pending entry terminal/recovery reconcile、flat 但仍有残余 open orders 时的 exit 推断与清理、TIME_STOP submit failed 后 bracket repair、TS inflight 终态但 LONG position 仍 open 时的 reset+repair、terminal exit 后 live trade projection 落盘与 exit cooldown 刷新。该 patch 仍保持 Spring 走公共 LONG-only lifecycle，不复制 Snapback 私有 consumer 架构。
37. 2026-05-01 本地已推进 Spring live 正式入口命名 patch：正式入口统一为 `strategies/spring/run_live.py`，旧过渡入口从源码树删除，不保留 wrapper、alias 或兼容路径；projection / heartbeat / run_id / row metadata 统一使用 `spring_live`、`spring_live_heartbeat`、`SPRINGLIVE` 与 `run_mode=live`；loose 压测配置改名为 `strategies/spring/config.live_loose.json`。后续 Spring live 审计从 `run_live.py` 与公共 `core/live/execution_runner.py` 开始。
38. 2026-05-01 本地已推进 Spring loop finalized payload anchor wait patch：`strategies/spring/run_live.py` 的 loop 不再按 `--hub-max-age-secs` 单次读取任意 fresh finalized payload；每轮从 scheduled signal check epoch 推导 `expected_signal_time_ts` 与 `expected_latest_closed_bar_ts`，按 1s 轮询等待 finalized payload 精确匹配当前 C anchor 且包含 `finalize_summary`，deadline 对齐 Snapback 为 `expected_signal_time_ts + 50s`。deadline 前未等到时，本轮写 `finalized_candidate_payload_not_ready` projection/heartbeat 并继续下一轮，不消费旧 payload、不下单。`--signal-check-second` 默认值从 2 改为 5，对齐 Snapback live 起始检查秒。
39. 2026-05-01 本地已推进 live 三段架构边界 patch：项目正式采用 `Live Data Gate -> Signal Generation -> Execution Lifecycle` 术语；新增 `core/live/live_data_gate.py` 承接信号生成前的公共 finalized payload anchor gate，Spring `strategies/spring/run_live.py` 改为复用该公共模块。`signal` 仍只表示策略计算后的信号结果，信号生成前的数据输入层统一称为 `Live Data Gate`，不得称为 `Signal Input`。
40. 2026-05-06 已补 Spring sim/live 决策审计排名字段：策略逻辑同源产出 `rank_chg_24h`、`rank_vol_24h`、`score_rank_all`、`selected_score_order`、`score_top_n`、`selected_for_structure`、`universe_hard_gate_pass`；sim `spring_decision_audit` 新增 `decision_scoreboard`，live `spring_live` projection 新增完整 `decision_audit` 与同口径 `decision_scoreboard`。`score` 是综合分数不是名次，历史 `score_order` 仅表示 topN 内顺序；后续 Spring sim/live 一致性审计应以 `score_rank_all` 判断全候选排名、以 `selected_score_order/selected_for_structure` 判断是否进入 structure 检查。
41. 2026-05-07 Spring smoke `Spring_SmokeTest_V1_0507T1944` 与 `mybwin139` live 重叠审计确认：11/11 信号按 `(symbol, signal_time)` 匹配，结构字段一致；此前看到的 `chg_24h / vol_24h / rank / score` 差异来自 sim signal 文件记录了 CB cross_section，而 live 严格使用 `C=HBs[0]` finalized payload。进一步对表显示 live 24h 指标逐笔匹配 sim decision audit 的 C 行 scoreboard，不匹配 sim CB 行指标。
42. 2026-05-07 已将公共语义明确为：所有策略的 `logic.py` / signal 生产层只能消费 HBs 数据，CB 数据只允许进入 signal 之后的执行撮合、entry price / pre-entry price 与最终 TP 解析。共享回测 runner 已修正 Spring/SWR 的策略逻辑投喂：`strategy.on_kline_close(signal_time=CB, cross_section=C)`，同时保留 CB cross_section 用于 sim 执行价注入和撮合。Snapback sim 已检查，其 logic 当前以 `current_time_ms=C` 运行并自行产出 `signal_time=C+1m`，本环节未发现同类 CB 投喂偏差。
43. 2026-05-07 Snapback smoke `Snapback_SmokeTest_0507T2043` 与 `mybwin139` live 重叠审计确认：共同 16 条信号的单币 `vol_24h` 在 live stage4 输入与 sim parquet rolling 口径基本一致，但 `market_total_24h_vol` 16/16 存在差异。偏差源头是 live `hub_owned_1m_rollsum` 读取全市场总量时混用了不同 symbol 的不同 `latest_bar_ts`，没有证明同锚到 `C = HBs[0]`。公共 hub rollsum 读取已修正为 C-anchor；随后确认 `rollsum_refresh_batch_size=80` 与“全市场每分钟同 C ready”不可收敛，因此 Snapback live 的 `market_total_24h_vol` 可由同一 finalized C-anchor HBs payload 的 `cross_section.vol_24h` 聚合得到，`hub_owned_1m_rollsum` 保留为后台刷新/观测源。Spring / SWR 只继承单币 24h / cross_section 的 C-anchor HBs 约束，不继承 Snapback 专属的 market-wide `market_total_24h_vol` ready gate。

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
5. Spring live loop patch 已部署并完成 1 轮服务器验证；后续如要继续 smoke，应优先使用内置 `--loop --execute-live`，不再启动临时外层 watcher。
6. Snapback live 不得维护、取消、离场或写入非 `SNP` 策略的 open_trade；Spring live 不得写入 Snapback state 文件。
7. 下一次 Spring 实盘 smoke 不应绕过 cooldown；若信号仍为 `SKYAIUSDT`，需等 `2026-04-30 00:17:00 BJ` 之后或等无 cooldown 的新标的信号。
8. 当前 Spring smoke watcher 已停止；正式常驻前必须使用 Python 内置 loop lifecycle，不再依赖临时外层 shell watcher。
9. 21:29 与 21:35 两笔 smoke 说明：live once 即时 post-entry reconcile 只能捕获“执行返回前已经离场”的情况；若 TP/SL 在返回后触发，需要后续 loop reconcile / exit monitor 清理。
10. 已补账户级 Spring local active gate 与每轮 open_trade reconcile；后续再评估是否继续补 time-stop 主动提交/撤单能力。

### 3.6 Sweep-Reclaim

已完成：

1. 新增 Sweep-Reclaim / SWR 策略语义基线：`docs/Sweep-Reclaim项目语义基线.md`。
2. 新增策略参数骨架：`strategies/sweep_reclaim/config.json`。
3. 新增策略包目录：`strategies/sweep_reclaim/`。
4. 新增 SWR sim 结构识别逻辑：`strategies/sweep_reclaim/logic.py`。
5. `core/config_loader.py` 已新增 `sweep-reclaim` fail-fast 配置校验。
6. `strategies/run_backtest.py` 已支持 `--strategy sweep-reclaim`，首版复用 Spring 强势 TopN decision audit 写法并输出 `sweep_reclaim_decision_audit.{run_id}.jsonl`。
7. `core/engine/broker.py` 已允许 SWR 使用 Spring 同形的 sim exit policy 段：`max_hold_mins` / `time_stop_min_profit_pct` / `breakeven_guard`。
8. `core/analysis/visualizer.py` 已将 SWR 复盘图文件名前缀改为 `SWR_`，Snapback 仍保持 `SNAP_`。
9. `strategies/schedule_backtests.py` 已支持 `--strategy sweep-reclaim` 全量并行调度，并在 post-merge 阶段合并 `sweep_reclaim_decision_audit.{run_id}.jsonl` 为 `sweep_reclaim_decision_audit.{runset}_ALL.jsonl`。
10. 本地已完成 SWR scheduler `--dry-run` 验证，能按 batch 生成 `strategies/run_backtest.py --strategy sweep-reclaim` 命令。
11. 2026-05-07 已新增 SWR live 侧代码入口：
    - `strategies/sweep_reclaim/live_execution.py`
    - `strategies/sweep_reclaim/run_live.py`
    - `strategies/sweep_reclaim/live_execution.smoke_10u.json`
12. 2026-05-07 公共 live execution intent 已支持 `risk_reward_r_multiple` / `take_profit_r_multiple`，用于表达 SWR 的 R 倍数止盈语义；Spring 原 `risk_reward_1r` / `fixed_pct` 语义保持不变。

当前语义事实：

```text
strategy_name = sweep-reclaim
strategy_code = SWR
LONG-only
1m contract bars only
C = HBs[0]
CB = C + 1
signal_time = entry_time = CB
H -> gamma -> B -> C -> CB
B = support_window 内最低 low，当前参数 support_window_mins = 180
H = B 左侧最高 close 点
hb_drop = (h_close - b_low) / h_close
bc_rebound = (c_close - b_low) / (h_close - b_low)
gamma = B - bars_bc
vol_climax = avg_quote_volume(gamma, C] / avg_quote_volume(H, gamma]
SL = b_close
TP = entry_price + risk_distance * take_profit_r_multiple
```

当前配置事实：

```text
strategies/sweep_reclaim/config.json:
- runtime.bar_interval = 1m
- runtime.max_history_window_mins = 300
- universe.min_24h_chg_pct = 30
- universe.min_24h_quote_volume = 50000000
- universe.score_top_n = 3
- structure.support_window_mins = 180
- structure.hb_drop.min = 0.06
- structure.rebound.bc_rebound_min = 0.2
- structure.rebound.bc_rebound_max = 0.4
- structure.rebound.hb_bars_min = 3
- structure.rebound.bc_bars_min = 1
- structure.rebound.bc_bars_max = 30
- structure.rebound.bc_over_hb_bars_max = 0.3
- structure.vol_climax.ratio_min = 3.0
- exit_policy.stop_loss_anchor = b_close
- exit_policy.take_profit_r_multiple = 1.0
```

当前 performance baseline：

```text
run_id = SWR_V1_30D_P6_0506T2125
strategy = sweep-reclaim
period = 2025-04-18T00:00:00+08:00 -> 2026-05-06T10:00:00+08:00
scheduler = 30D / P6
success_count = 13
failed_count = 0
trades = 405
signals = 407
skipped_signals = 2
decision_audit_rows = 458822
viz_png = 405
return_simple_net_pct = 185.04
return_compound_net_pct = 428.33
max_drawdown_simple_net = 23.15U / 19.38%
max_drawdown_compound_net = 52.40U / 17.36%
```

当前 pending：

1. `SWR_V1_30D_P6_0506T2125` 已作为当前 performance baseline。
2. SWR live 侧已完成代码入口与本地 intent / dry-run plan 小样本验证，但尚未在服务器读取 hub payload 跑真实 live projection dry-run。
3. SWR live 尚未启动真实下单；首次使用 `--execute-live` 前必须先做服务器 dry-run projection 与 exchange precheck 验证。
4. 后续若调整 SWR 参数，必须同步更新语义文档或明确为实验配置。

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
- strategies/spring/run_live.py
  - dry-run local_state_snapshot 读取 spring-sabc namespace。
- strategies/snapback/trade_consumer.py
  - Snapback pending/open_trade 写入 SNP 归属字段。
  - Snapback reconcile 发现非 SNP 或未知归属 payload 时阻断并写 audit event，不取消、不平仓、不接管。

本地验证：
- python3 -m py_compile core/live/live_state.py core/live/execution_runner.py strategies/spring/run_live.py strategies/snapback/trade_consumer.py
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
- 20:17 smoke 后，state/live/spring_sabc_mybwin139.state.json 中 SKYAIUSDT open_trade.strategy_code = SPR，status = OPEN。
- 21:10 部署 `d56d5b9` 后，manual post-entry reconcile 已根据交易所 TP FILLED / SL EXPIRED 事实清空 Spring `open_trade`。
- state/live/snapback_mybwin139.state.json 中 SKYAIUSDT 无 open_trade。
- Snapback audit 未出现取消本次 SPR_SL/SPR_TP 或提交 SNP_TS 的记录。

该 smoke 暴露的历史风险：
- 当时 Spring one-shot execution runner 只补了一次即时 post-entry reconcile，尚无循环式 Spring reconcile / exit monitor。
- 后续 Spring live loop、active time-stop、bracket repair、protective flatten 与 lifecycle 对齐 patch 已把这些能力补到公共 LONG-only runner；现阶段仍需要用服务器 live smoke / projection 继续确认真实交易所路径。
```

当前 Spring live 架构事实：

```text
总体架构边界：
- Snapback 当前仍是老结构：信号识别、下单、持仓维护、reconcile、离场落盘集中在策略自己的 live/consumer 代码中。
- 新 live 结构正式分四段：`Live Data Gate -> Signal Gate -> Strategy Signal Logic -> Execution Lifecycle`。
- Spring live 正在走新结构：`Live Data Gate`、`Signal Gate` 与 `Execution Lifecycle` 逐步沉到公共 LONG-only live 模块；策略层只负责 `Strategy Signal Logic` 与 signal -> ValidatedLiveExecutionIntent adapter。
- `core/live/execution_intent.py` 只是公共 contract 入口，不承载全部生命周期逻辑。
- `core/live/live_data_gate.py` 承载信号生成前的公共 live 数据门禁：expected C / signal_time 推导、hub finalized payload anchor wait、deadline / stale payload 防护。
- `core/live/signal_gate.py` 承载策略信号逻辑前的公共 live signal gate：汇总命令行 active symbols、本策略 pending/open symbols 与本策略 cooldown symbols；cooldown 默认按 `strategy_name + account + symbol` 维度隔离。
- 公共 live execution lifecycle 的完整目标是：
  signal adapter -> ValidatedLiveExecutionIntent -> execution_plan -> entry/SL/TP -> strategy-specific state/audit -> open_trade reconcile -> TP/SL/TS exit_reason -> state close -> live_trades/projection -> cooldown。
- 未来第三、第四套 LONG 策略应只新增自己的 strategy signal logic / signal adapter / strategy_name / strategy_code / config，复用公共 Live Data Gate、Signal Gate 与 Execution Lifecycle；不得复制 Snapback 老式策略私有 live 闭环。
- 后续新增 Spring live 生命周期能力应继续补在公共 LONG-only live execution lifecycle 中，而不是写成 Spring 私有闭环。
- Snapback 若未来迁移到公共层，必须单独拆刀；当前不得在 Spring 修复刀中混改 Snapback 架构。

core/live/execution_intent.py:
- 定义 ValidatedLiveExecutionIntent
- 只允许 LONG
- fail-fast 校验 strategy/account/symbol/time/price/notional/SL/TP/hold/time-stop/signal_snapshot

core/live/live_data_gate.py:
- 定义公共 Live Data Gate 边界
- 提供 `expected_snapshot_from_signal_check_epoch(...)`
- 提供 `wait_finalized_candidate_inputs_for_snapshot(...)`
- 要求 finalized payload 精确匹配本轮 `expected_latest_closed_bar_ts / expected_signal_time_ts`
- 要求 payload 包含 `finalize_summary`
- deadline 默认对齐 Snapback：`expected_signal_time_ts + 50s`
- deadline 未等到时返回 not_ready diagnostics，由策略 runner 写 projection/heartbeat 并跳过本轮交易

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
- 当前覆盖 entry、保护单建立、live once 入场后的即时 post-entry reconcile
- 本地 loop patch 新增 strategy-level open_trade reconcile 与 account local active precheck：每轮可清理已由 TP/SL/TS 离场的 stale open_trade；若仍有 open/pending，则阻断新的 live entry
- 本地 active time-stop patch 新增到期检查：使用最新闭合 C close 计算收益；若 `held_mins >= max_hold_mins` 且收益低于 `time_stop_min_profit_pct`，先取消 TP/SL，再提交 TS market flatten，并等待后续 reconcile 清理 state
- 本地 bracket verify/repair patch 新增持仓保护单维护：position 仍 open 且未处于 `exit_submit_inflight` 时，校验 TP/SL open order 绑定；缺失则按 open_trade 记录补挂，补挂后再次验证；失败写 error/audit 并保留 open_trade
- 本地 SL submit failed protective flatten patch 新增入场保护失败独立离场语义：SL 提交失败后的应急 market flatten 不再复用 `SPR_TS` / `TIME_STOP`，改用 `SPR_SF` / `SL_SUBMIT_FAILED_FLATTEN`，并写 `protective_flatten_client_order_id`、`protective_flatten_exchange_order_id`、`protective_flatten_exit_reason`
- 本地 finalized payload anchor wait patch 新增 loop 侧当前轮 payload 等待：每轮按 scheduled signal check epoch 推导 expected C / signal_time，等待 hub finalized payload 精确匹配后才允许 signal scan / execution；超时记录 `finalized_candidate_payload_not_ready` 并进入下一轮，不消费旧 payload。
- 对照 Snapback live，Spring 公共 lifecycle 本地已补齐 pending entry terminal/recovery reconcile、time-stop submit failed 后保护单修复、inflight TS 终态但 position 仍 open 的修复、terminal exit 的 live trade projection 专用落盘与 exit cooldown 刷新；下一步需要用本地最小测试与后续服务器 live smoke / projection 继续确认真实交易所路径。

core/live/audit_log.py:
- 保留既有 snapback audit 写入入口
- 新增 strategy-specific audit 写入入口，Spring live execution event 写入 `spring_sabc_{account}.jsonl`

strategies/spring/live_execution.py:
- 定义 SPRING_LIVE_STRATEGY_CODE = SPR
- 将 Spring-SABC signal 显式转换为公共 execution intent
- 要求 signal.action = BUY
- 使用 signal.position_notional_usdt 作为 live 下单名义金额来源

strategies/spring/run_live.py:
- Spring live runner 入口
- 通过 `core/live/live_data_gate.py` 等待并读取当前轮 shared hub finalized_candidate_inputs
- 调用 SpringSABCStrategy.on_kline_close(...)
- signal 存在时生成并校验公共 execution intent
- signal 存在时生成 dry_run_execution_plan 并落盘
- 支持 `--dry-run-verify-exchange` 读取只读交易所快照与本地 live state 快照
- 支持显式 `--execute-live --live-execution-config ...` 真实下单
- 写入 output/live_projection/spring_live.{run_id}.jsonl
- 支持 `--loop`、`--execute-live`、`--max-iterations`、`--signal-check-second`
- loop 模式下等待当前轮 expected finalized payload，deadline=`signal_time+50s`；超时只写 not_ready projection/heartbeat，不交易
- 写入 output/live_projection/spring_live_heartbeat.{run_id}.json
- 默认不下单；只有 `--execute-live`、外部 live execution JSON、account local gate 与 exchange/local precheck 同时满足时才会触发真实交易

strategies/spring/config.live_loose.json:
- projection-only 专用 loose 配置
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
- `precheck_scope = symbol`
- `strategy_concurrency_scope = account`
- `pre_entry_min_sl_distance_pct = 0.003`
- `max_position_notional_usdt = 11.0`
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

### 5.3 Spring-SABC live lifecycle

```text
当前审计起点：

1. Spring live 正式入口是 `strategies/spring/run_live.py`。
2. 旧过渡入口已从源码树删除，不保留 wrapper、alias 或兼容路径。
3. 运行产物命名收敛为 `spring_live.{run_id}.jsonl` 与 `spring_live_heartbeat.{run_id}.json`，默认 run_id 前缀为 `SPRINGLIVE_`。
4. loop 消费 finalized_candidate_inputs 时必须匹配本轮 expected C anchor，deadline 为 `signal_time+50s`；不得用 fresh 但非当前轮的 payload 产生信号或交易。
5. Spring live 在参数与 live execution config 校验通过后会立即写 `[Spring-Live] runner started | account=... | run_id=... | mode=...` 日志；当 `--execute-live` 且 live execution config `notify_enabled=true` 时，同步写入 `spring` PUSH 队列，使 `nohup` 日志不必等第一轮 projection 才能看到启动时间。
6. Spring live 常规每分钟 projection 结果只落盘到 `spring_live.{run_id}.jsonl` 与 heartbeat，不再写 `wrote projection` INFO 日志刷屏。
7. Spring live 已复用公共 `core/live/signal_gate.py`：本策略 pending/open symbol 会在信号生成前并入 active symbols，本策略 cooldown map 会在信号生成前灌入策略，使持仓或 cooldown 期间同一 symbol 不再每分钟重复打印 `Spring雷达锁定`；projection 同时保留 `configured_active_symbols`、实际 `active_symbols`、`live_state_active_symbols`、`cooldown_symbols` 与 `signal_gate` 供审计。
8. Spring live execution config 显式区分 `precheck_scope` 与 `strategy_concurrency_scope`：`precheck_scope` 只表达交易所下单前检查范围（`symbol` / `account_flat`）；`strategy_concurrency_scope` 表达同账户同策略并发约束（`symbol` / `account`）。当前 smoke 配置为 `strategy_concurrency_scope=account`，当本策略 state 已有任意 pending/open trade 时，Signal Gate 会在 Strategy Signal Logic 前阻断新 signal，避免账户级 hard gate 只在信号产生后才拦截。
9. 公共 BN_EXEC 事件支持按调用方传入 `notify_label`；Spring execution runner 传 `spring`，避免 Spring 的 ENTRY/SL/TP/CANCEL 执行通知落到 `snapback` 队列。
10. Spring pre-A 语义已从 pattern window 左边界漂移改为 A 点前固定窗口：`structure.pre_a.window_mins=60`。`pre_a_chg_pct`、pre-A range、high-to-A-close、close position、up/down 统计均锚定该固定 S→A 区间；`runtime.max_history_window_mins` 必须覆盖 `structure.pattern_window_mins + structure.pre_a.window_mins`，否则 fail-fast。
11. Spring B 低点确认已从 A-B 区间最低 low 收紧为 A-C 区间最低 low：若 B 之后、C 之前出现任何低于 B_low 的 X 点，待定 B 失效，算法继续搜索其它 B；若无其它合法 B，本轮不产生信号。
12. Spring 价格时态已拆开：`strategies/spring/logic.py` 不再从 HBs/cross_section 产出 `signal.current_price` 或最终 `tp_price`；sim 侧策略逻辑只消费 `C=HBs[0]` 的 cross_section，并在 `signal_time=CB` 用 CB open 注入可复现执行价；live 侧在公共 execution lifecycle 中 entry 前读取并落盘 `pre_entry_price`，真实 entry fill 后再按 `risk_reward_1r` 重算 TP。执行层必须保证 LONG 的最终 TP 高于真实 entry，避免 BUSDT 23:21 这类 C_open 被误当 current price 后提交低于 entry 的 TP。
13. live_trades 闭仓 projection 必须保留 entry 审计字段：`pre_entry_price`、`pre_entry_price_source`、`resolved_tp_price_source`，用于复盘真实 entry 前价格、真实 fill 与最终 TP 计算来源。
14. 后续若继续推进 Spring live 逻辑 patch，仍需按单问题框架重新锁定 `strategies/spring/run_live.py` 与 `core/live/execution_runner.py` 基线。
```

### 5.4 Spring-SABC sim / 参数

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
