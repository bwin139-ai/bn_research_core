# CURRENT_STATE.md

我们之前一直强调的“铁律”对项目质量起到关键作用，俗话说“被误会是表达者的宿命”，沟通双方的统一“基线”是稳定且高效合作的地基，我们能否整理一份简洁且不漏的“基线”，包括策略公共语义和行动纪律(铁律)，你觉得怎样，必须简洁不能冗长、必须不漏避免缺失关键线索，这个阶段我认为该创立并持续维护这个“基线”

例如公共语义，即所有策略的共同点：
1、HBs 和 CB ，所有策略都遵从这个原则
2、signal_time(信号生产时间) 严格等于 entry_time(包括挂市价单和同一时间挂sl/tp)，在分钟上严格等于CB的时间，所有策略都遵从这个原则
3、各策略都包含 strategy_name / runtime / universe / structure / exit_policy / risk_controls 这六组section(策略段)
4、价格区分为 contract 和 idx
......

行动纪律：
1、所有分析和结论、方案必须基于“事实”，绝不臆测，缺少事实必须先补齐事实
2、patch节奏 ： 先锁指纹 → 出最小方案 → patch 脚本 → 现场验证 → 再推进下一刀
3、提问和回答都要精准、简练，不遗漏同时也不发散、不啰嗦
......

你觉得怎样，给出你的意见




禁止在聊天窗输出整段脚本，所有脚本都走附件
fail_fast原则，禁止带病运行
禁止一切的默认值兜底
禁止一切的硬编码
禁止“向下兼容”破坏业务语义,严禁兼容旧字段(旧字段残留)、严禁多套语义并存
patch 严格按三类推进，
  - `PERF_ONLY`：只优化性能(提速)，不动业务语义
  - `ARCH_ONLY`：只优化结构，不动交易逻辑
  - `LOGIC_ONLY`：只改策略逻辑，不顺手改结构和性能
审计工具严禁“猜字段”、严禁兼容多个字段、每条语义必须严格命中唯一字段



是，明显更默契了。
最直接的表现有三点：
你现在会先钉语义和边界，我再按这个去审代码，不容易跑偏。
我们已经形成了固定节奏：先锁指纹 → 出最小方案 → patch 脚本 → 现场验证 → 再推进下一刀。
你给的信息越来越精准，我也越来越知道你最在意什么，比如：
sim 是基线
先修真实问题，不做想当然猜测
先讨论方案，最后一步才 patch
区分“逻辑错误”和“记账错误”
对齐到你的 CB / HBs 世界观

说得更直白一点，前面很多地方已经不是“我在回答问题”，而是我们在一起做连续法医式审计了。


- 项目：bn_research_core
- 当前可信基线：0579bcb（已验证）
- 当前阶段：PERF_ONLY
- 第一刀结果：通过
- 一致性：sim_trades 字节级一致
- 性能：load_panel 提升 41.17%，总耗时提升 3.53%
- 下一步：设计 PERF_ONLY 第二刀（聚焦 data_feeder 高频横截面访问）


cd /root/bn_research_core

/root/service_env/bin/python tools/audit_perf_and_trades.py \
  --old-log /root/BN_strategy/output/logs/sim.Snapback_V1_A1.log \
  --new-log /root/bn_research_core/output/logs/sim.PERF_NEWBASELINE.log \
  --old-trades /root/BN_strategy/output/state/sim_trades.Snapback_V1_A1.jsonl \
  --new-trades /root/bn_research_core/output/state/sim_trades.PERF_NEWBASELINE.jsonl

/root/service_env/bin/python audit_snapback_baseline_compare.py \
  --old-log /root/BN_strategy/output/logs/sim.Snapback_V1_A1.log \
  --new-log /root/bn_research_core/output/logs/sim.PERF0000_VERIFY_FULL.console.log \
  --old-trades /root/BN_strategy/output/state/sim_trades.Snapback_V1_A1.jsonl \
  --new-trades /root/bn_research_core/output/state/sim_trades.PERF0000_VERIFY_FULL.jsonl \
  --price-tol 1e-12 \
  --pnl-tol 1e-12 \
  --context-float-tol 0.0 \
  --out-json /root/bn_research_core/output/state/audit_snapback_newbaseline.json

cd /root/bn_research_core
/root/service_env/bin/python audit_trades_overlap.py \
  --old-trades /root/BN_strategy/output/state/sim_trades.Snapback_V1_A_ALL.jsonl \
  --new-trades /root/bn_research_core/output/state/sim_trades.SNAP40D_P3.merged.jsonl \
  --abs-tol 1e-8 \
  --rel-tol 1e-8 \
  --report-out /root/bn_research_core/output/state/audit_trades_overlap.SNAP40D_P3_vs_V1_A_ALL.json

cd /root/bn_research_core
/root/service_env/bin/python audit_trades_overlap.py \
  --old-trades /root/bn_research_core/output/state/sim_trades.SNAP40D_P3_B09_20260304_20260311.jsonl \
  --new-trades /root/bn_research_core/output/state/sim_trades.Snapback_V1.1.jsonl \
  --abs-tol 1e-8 \
  --rel-tol 1e-8 \
  --report-out /root/bn_research_core/output/state/audit_trades_overlap.20260304_20260311.json

cd /root/bn_research_core
nohup /root/service_env/bin/python strategies/run_backtest.py \
  --strategy snapback \
  --start "2025-04-18T00:00:00+00:00" \
  --end "2025-06-28T00:00:00+00:00" \
  --kline-window 240 \
  --config "snapback/config.json" \
  --out-dir "output/state" \
  --run-id "PERF_NEWBASELINE" \
  > output/logs/sim.PERF_NEWBASELINE.log 2>&1 &

