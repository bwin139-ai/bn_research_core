# BN Backtest Core

这是根据当前全量回测启动脚本反向提取出的 **最小可运行仿真/回测核心项目**。

## 本次提取依据
主链来自 `run_full_backtest.sh` / `tools/run_full_backtest.sh`：

1. `tools/run_full_backtest.sh`
2. `strategies/run_backtest.py`
3. `core/config_loader.py`
4. `core/engine/broker.py`
5. `core/engine/data_feeder.py`
6. `strategies/snapback/logic.py`
7. `strategies/top1_hunter/logic.py`
8. `core/analysis/analyzer.py`
9. `core/analysis/visualizer.py`
10. `core/analysis/top1_equity_curve.py`
11. `tools/ai_feature_extractor.py`

## 故意没有带入的内容
以下内容属于旧项目历史包袱、实盘/机器人/杂项脚本，本次没有纳入：
- Telegram / monitor / hedge / viewer / alpha_* / deploy 等模块
- 与当前回测链无关的旧策略文件
- 历史实验脚本、临时诊断脚本

## 目录说明
- `strategies/`：策略入口与策略逻辑
- `core/engine/`：撮合/持仓/数据投喂
- `core/analysis/`：绩效分析、K线复盘图、资金曲线
- `tools/`：批量回测与 AI 特征提取
- `data/klines_1m/`：1分钟K线根目录（需要你自行放入真实数据）
- `output/`：运行输出

## 运行方式
### 单批回测
```bash
python strategies/run_backtest.py \
  --strategy snapback \
  --start 2025-04-18T00:00:00+00:00 \
  --end   2025-06-28T00:00:00+00:00 \
  --kline-window 240 \
  --config snapback/config.json \
  --out-dir output/state \
  --run-id Snapback_V2_A1
```

### 全量批跑
先按需修改 `run_full_backtest.sh` 或 `tools/run_full_backtest.sh` 中的：
- `STRATEGY_NAME`
- `PREFIX`
- `SERIES`

然后执行：
```bash
bash run_full_backtest.sh
```

## 仍需你自行确认的隐式依赖
本项目已经覆盖命令链上的直接依赖，但以下事项仍要在你的真实环境里核对：
- `data/klines_1m/<SYMBOL>/*.parquet` 数据是否完整
- Python 解释器路径是否仍是 `/root/service_env/bin/python`
- `pyarrow` / `mplfinance` 等三方库是否已安装
- 输出路径规范是否与你现有自动化脚本一致

## 下一步建议
1. 先把这个新仓库跑通一次 snapback 单批回测
2. 再做 PERF_ONLY 提速
3. 再做 ARCH_ONLY 整理
4. LOGIC_ONLY 最后做
