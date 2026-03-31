# 当前项目状态
（`CURRENT_STATE.md`）

## 0. 文档定位

本文件只用于记录 **当前现场状态**。  
它回答的是：

**我们当前正在做什么、已经做到哪里、下一步做什么。**

本文件不是：

- `PROJECT_BASELINE.md`
- `STANDARD_PATCH_FRAMEWORK.md`

三者分工固定为：

- `PROJECT_BASELINE.md`：原则、边界、禁令、优先级
- `STANDARD_PATCH_FRAMEWORK.md`：Patch 协作规则与交付结构
- `CURRENT_STATE.md`：当前现场快照

若本文件与前两者冲突，优先级固定为：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`

---

## 1. 当前项目定位

### 1.1 当前仓库
```text
bn_research_core
```

### 1.2 当前主线
```text
strategies/snapback
```

### 1.3 当前阶段
```text
审计 / 投影主线首轮联调验证阶段
```

### 1.4 当前阶段目标
```text
把 live_signals / live_trades / bn 真相层 串成稳定、可复核、可长期使用的审计基础设施，并完成 sim / live / bn 首轮真实样本闭环验证。
```

---

## 2. 固定现场路径

```text
- live state：state/live/snapback_mybwin139.state.json
- audit jsonl：state/live_audit/snapback_mybwin139.jsonl
- live projection dir：output/live_projection
```

---

## 3. 当前唯一主线

```text
当前唯一主线：
继续推进并验证 live projection + bn truth + triplet audit 这条审计闭环主线。

当前优先级：
1. 巩固三方审计闭环（sim / live / bn）
2. 修补审计字段完整性缺口
3. 在不扩功能、不改策略逻辑前提下，增强当前基础设施稳定性
```

---

## 4. 最近已完成

1. `live_signals.<run_id>.jsonl` 与 `live_trades.<run_id>.jsonl` 已落地。
2. `bn_sync` 已可按样本 symbol 同步 `bn_orders / bn_fills / bn_income`。
3. `audit_trade_triplet_diff.py` 已能对单笔样本完成 sim / live / bn 联调审计。
4. `binance_exec.py` 已补充交易动作日志与 Telegram Bot 推送。
5. live 侧已补充：
   - 雷达锁定消息推送
   - 离场日志与 Bot 推送
   - 离场消息持仓时间显示
6. 已完成 3 笔真实 live 样本的首轮 triplet audit。

---

## 5. 当前已验证通过

1. live projection 主线可用：`live_signal` 与 `live_trade` 均可稳定落盘。
2. bn truth 主线可用：`bn_orders / bn_fills / bn_income` 可围绕指定 symbol 成功补齐。
3. triplet audit 主线可用：已成功审计 3 笔真实样本。
4. 三笔样本的离场原因三方一致：
   - ONUSDT：`STOP_LOSS`
   - AIAUSDT：`STOP_LOSS`
   - BRUSDT：`TAKE_PROFIT`
5. live 与 bn 的入场价格已对齐。
6. live 与 bn 的离场价格已实质对齐；少量差异属于浮点精度差异。
7. 当前审计主锚点可优先使用：
   - `client_order_id`
   - `order_root`
   - `leg`
8. 条件单场景下，已确认：
   - live 侧记录的 `exit_order_exchange_id` 可能是条件单 / algo 父单 ID
   - bn `exchange_order_id` 记录的是最终基础成交子单 ID
   - 父单 / 子单当前样本中保留相同 custom_id，可用于稳定认亲

---

## 6. 当前 pending

1. `live_trade.selected_tp_pct` 在部分链路中丢失，需修补字段完整性。
2. 是否为 bn truth 增加“条件委托 / algo 父单”独立真相层，尚未决定。
3. triplet audit 后续是否要把“SL 父单 ID ≠ 基础子单 ID”显式纳入报告解释层，尚未决定。
4. 需要形成当前阶段的正式结论归档（验证通过项 / 待改进项）。

---

## 7. 当前明确不做

- 不扩新功能
- 不主动改策略逻辑
- 不主动重构 `bn_sync`
- 不回到旧主线问题
- 不重复已完成并已 push 的 patch
- 不把当前阶段变成大规模结构重构

---

## 8. 下一步

```text
下一步顺序：
1. 打完 selected_tp_pct 字段完整性这小刀 patch
2. 形成当前阶段结论归档（验证通过项 / 待改进项）
```

---

## 9. 当前协作提醒

1. 当前阶段优先“验证与收束”，不是继续发散扩展。
2. 审计主键优先使用 `custom_id / order_root / leg`，不要硬要求父单 ID 与子单 ID 相等。
3. 正式 Patch 必须继续遵守：
   - 先锁基线
   - 固定输出顺序
   - 对齐输入 + 对齐输出
4. 文件唯一身份以 `MD5` 为准；`Lines` 只作辅助诊断。
