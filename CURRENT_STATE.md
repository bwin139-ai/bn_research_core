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
  --new-trades /root/bn_research_core/output/state/sim_trades.SNAP40D_P3.merged.jsonl \
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






  现在 schedule_backtests.py 的三条线，每条线运行的每个批次之间有没有空闲时间，例如A线第一批运行结束后是否紧接着就运行第二批？
我看主进程下面这部分输出只打印了内容，是否把时间也打印出来，就是START、DONE之前把当前时间打印一下
```bash
START batch=01 pid=5185 run_id=SNAP40D_P3_B01_20250418_20250528
START batch=02 pid=5186 run_id=SNAP40D_P3_B02_20250528_20250707
START batch=03 pid=5187 run_id=SNAP40D_P3_B03_20250707_20250816
DONE batch=02 rc=0 elapsed=1270.1s run_id=SNAP40D_P3_B02_20250528_20250707
START batch=04 pid=5513 run_id=SNAP40D_P3_B04_20250816_20250925
DONE batch=01 rc=0 elapsed=1302.1s run_id=SNAP40D_P3_B01_20250418_20250528
START batch=05 pid=5538 run_id=SNAP40D_P3_B05_20250925_20251104
DONE batch=03 rc=0 elapsed=1446.1s run_id=SNAP40D_P3_B03_20250707_20250816
START batch=06 pid=5568 run_id=SNAP40D_P3_B06_20251104_20251214
DONE batch=04 rc=0 elapsed=1370.1s run_id=SNAP40D_P3_B04_20250816_20250925
START batch=07 pid=6495 run_id=SNAP40D_P3_B07_20251214_20260123
DONE batch=06 rc=0 elapsed=1292.1s run_id=SNAP40D_P3_B06_20251104_20251214
START batch=08 pid=6521 run_id=SNAP40D_P3_B08_20260123_20260304
DONE batch=05 rc=0 elapsed=1584.1s run_id=SNAP40D_P3_B05_20250925_20251104
START batch=09 pid=6544 run_id=SNAP40D_P3_B09_20260304_20260311
DONE batch=09 rc=0 elapsed=176.0s run_id=SNAP40D_P3_B09_20260304_20260311
DONE batch=07 rc=0 elapsed=1116.1s run_id=SNAP40D_P3_B07_20251214_20260123
DONE batch=08 rc=0 elapsed=1116.1s run_id=SNAP40D_P3_B08_20260123_20260304
SUMMARY success=9 failed=0 wall_clock=3854.295s
Wrote summary: output/state/scheduler_SNAP40D_P3.summary.json
```
另外在output/state/scheduler_SNAP40D_P3.summary.json中是否也把每个批次的运行开始时间/运行完成时间打印出来