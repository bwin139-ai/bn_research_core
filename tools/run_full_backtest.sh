#!/bin/bash

# ==========================================
# 🎛️ 回测控制台 (仅需修改以下三个变量)
# ==========================================
# 1. 选择策略引擎大脑 (top1 或 snapback)
STRATEGY_NAME="snapback"

# 2. 策略大版本 (姓氏 - 代表代码逻辑)
PREFIX="Snapback_V1"

# 3. 参数测试批次 (名字 - 代表 JSON 参数组合，如 B, C, D)
SERIES="A"
# ==========================================

# 确保输出目录存在
mkdir -p output/state output/logs

# 定义带时间戳的打印函数
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1"
}

run_batch() {
    start_date=$1
    end_date=$2
    run_id="${PREFIX}_$3"

    log "====================================================="
    log "🚀 启动防御装甲批次: $run_id ($start_date 至 $end_date)"
    log "====================================================="

    if [ "${STRATEGY_NAME}" = "top1" ]; then
        CONFIG_PATH="config.json"
    else
        CONFIG_PATH="../${STRATEGY_NAME}/config.json"
    fi

    /root/service_env/bin/python strategies/top1_hunter/run_backtest.py \
      --strategy "${STRATEGY_NAME}" \
      --start "${start_date}T00:00:00+00:00" \
      --end "${end_date}T00:00:00+00:00" \
      --kline-window 240 \
      --config "${CONFIG_PATH}" \
      --out-dir "output/state" \
      --run-id "$run_id" \
      > "output/logs/sim.${run_id}.log" 2>&1

    log "✅ 批次 $run_id 运行完毕！"
}

log "🟢 开始全量回测 | 大版本: ${PREFIX} | 参数组: ${SERIES}"

# 极致压榨算力：分 5 轮吃掉将近 11 个月数据！
run_batch "2025-04-18" "2025-06-28" "${SERIES}1"
run_batch "2025-06-28" "2025-09-20" "${SERIES}2"
run_batch "2025-09-20" "2025-11-15" "${SERIES}3"
run_batch "2025-11-15" "2026-01-05" "${SERIES}4"
run_batch "2026-01-05" "2026-03-04" "${SERIES}5"

log "🎉 ${PREFIX} 的 ${SERIES} 系列参数测试已全部跑完！"
log "🔄 正在处理数据合并与链接..."

# 1. 自动合并流水和信号 (使用动态的 SERIES 变量)
cat output/state/sim_trades.${PREFIX}_${SERIES}*.jsonl > "output/state/sim_trades.${PREFIX}_${SERIES}_ALL.jsonl" 2>/dev/null
cat output/state/sim_signals.${PREFIX}_${SERIES}*.jsonl > "output/state/sim_signals.${PREFIX}_${SERIES}_ALL.jsonl" 2>/dev/null

# 2. 复制该系列的第一批次摘要作为 ALL 的配置来源
cp "output/state/sim_summary.${PREFIX}_${SERIES}1.json" "output/state/sim_summary.${PREFIX}_${SERIES}_ALL.json" 2>/dev/null

# ==========================================
# 🖼️ 自动合并可视化复盘图并清理现场
# ==========================================
ALL_VIZ_DIR="output/state/sim_viz_${PREFIX}_${SERIES}_ALL"
mkdir -p "$ALL_VIZ_DIR"
# 将所有子批次的 png 移动到 ALL 目录
mv output/state/sim_viz_${PREFIX}_${SERIES}[1-9]/*.png "$ALL_VIZ_DIR"/ 2>/dev/null
# 尝试删除已经掏空的子批次目录
rmdir output/state/sim_viz_${PREFIX}_${SERIES}[1-9] 2>/dev/null

# ==========================================
# 📈 自动生成全量资金曲线图
# ==========================================
log "📈 正在生成全局资金曲线 (Equity Curve)..."
/root/service_env/bin/python core/analysis/top1_equity_curve.py \
  --trades "output/state/sim_trades.${PREFIX}_${SERIES}_ALL.jsonl" \
  --kline-root data/klines_1m \
  --initial-equity 100 \
  --fee-side 0.0005 \
  --out "output/state/sim_curve.${PREFIX}_${SERIES}_ALL.png" \
  --summary-out "output/state/sim_equity.${PREFIX}_${SERIES}_ALL.json" \
  > "output/logs/equity_${PREFIX}_${SERIES}_ALL.log" 2>&1

log "✅ ${SERIES} 系列全部处理完毕！"
log "   - 资金曲线: output/state/sim_curve.${PREFIX}_${SERIES}_ALL.png"
log "   - 图片目录: $ALL_VIZ_DIR"
log "   - AI 提取:  /root/service_env/bin/python tools/ai_feature_extractor.py --run-id ${PREFIX}_${SERIES}_ALL"
log "====================================================="
