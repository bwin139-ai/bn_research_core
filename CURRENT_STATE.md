# CURRENT_STATE.md

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