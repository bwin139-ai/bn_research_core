#!/bin/bash

# ==========================================
# 🎛️ 本地拉取配置 (与云端发车引擎对齐)
# ==========================================
# 1. 选择策略引擎大脑 (top1 或 snapback)
STRATEGY_NAME="snapback"

# 2. 策略大版本 (姓氏 - 代表代码逻辑)
PREFIX="Snapback_V2"

SERVER="root@8.218.96.252"
REMOTE_DIR="/root/BN_strategy/output/state"
LOCAL_DIR="$HOME/ai_projects/bn_strategy/state/${STRATEGY_NAME}"

# 检查是否传入了 SERIES 参数
if [ -z "$1" ]; then
    echo "❌ 错误: 请提供要拉取的参数批次 (SERIES)！"
    echo "💡 用法: sh $0 <SERIES>"
    echo "📌 示例: sh $0 C5"
    exit 1
fi

SERIES=$1
RUN_ID="${PREFIX}_${SERIES}"

echo "====================================================="
echo "📥 正在从服务器极速拉取全量复盘数据: ${RUN_ID}_ALL"
echo "====================================================="

# 确保本地存放目录存在
mkdir -p "${LOCAL_DIR}"

# 使用 scp -r 配合远程通配符，一次性把所有 ALL 结尾的文件和文件夹全拉回来
scp -r "${SERVER}:${REMOTE_DIR}/*${RUN_ID}_ALL*" "${LOCAL_DIR}/"

echo "====================================================="
echo "✅ 拉取成功！文件已落地至: ${LOCAL_DIR}"
echo "📂 本次同步的文件及目录如下："
ls -lh "${LOCAL_DIR}" | grep "${RUN_ID}_ALL"
echo "====================================================="
