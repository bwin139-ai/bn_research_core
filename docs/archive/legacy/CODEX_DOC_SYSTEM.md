# Codex 视角下的仓库文档体系

本文件只回答一件事：

Codex 在 `bn_research_core` 中协作时，应该按什么层级理解和使用仓库文档。

本文件不新增业务语义，不替代：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`

若本文件与上述文档冲突，优先级固定为：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`
4. `CODEX_DOC_SYSTEM.md`

## 1. 文档分层

### 1.1 宪法层

文档：

- `PROJECT_BASELINE.md`

职责：

- 定义全项目公共语义
- 定义 fail-fast、事实优先、禁止兜底、禁止伪兼容等铁律
- 定义 sim / live / audit / patch / review / 对话协作的最高边界

Codex 规则：

- 任何任务开始前，先以此文档校准语义。
- 若某想法、补丁、推断未被宪法显式允许，默认视为禁止。

### 1.2 Patch 协作层

文档：

- `STANDARD_PATCH_FRAMEWORK.md`

职责：

- 规定正式 Patch 的唯一标准流程
- 规定 Patch 分类唯一、先锁基线、最小外科式修改、固定交付顺序

Codex 规则：

- 只要进入正式源码 Patch，就必须遵守该文档。
- 不能把多问题混成一刀，不能未锁基线直接改。

### 1.3 现场层

文档：

- `CURRENT_STATE.md`
- `新聊天开场白.md`

职责：

- 说明当前项目主线、阶段目标、已完成事实、pending、明确不做
- 帮助新线程避免误回旧主线

Codex 规则：

- 新线程先读开场白，再用 `CURRENT_STATE.md` 锁当前现场。
- 如果现场状态与旧认知冲突，以当前文档为准，不靠隐式记忆。

### 1.4 策略语义层

文档：

- `Spring-SABC项目语义基线.md`
- 未来各策略自己的“项目语义基线”文档

职责：

- 定义某个策略独有的内部结构语义
- 说明该策略的结构锚点、配置语义、入场离场语义、限制条件

Codex 规则：

- 公共语义之外的策略内部锚点，只能在这里找定义。
- 未显式写入策略文档的内部时间锚点，禁止假定存在。

### 1.5 实现理解与归档层

文档：

- `snapback-sabc_代码流程.md`
- `snapback_sim_live_audit_report.md`
- 其他代码流程、审计报告、结论归档

职责：

- 帮助理解当前实现和阶段性结论
- 保留审计结果、分析报告、代码流程图

Codex 规则：

- 这些文档可用于理解和补事实。
- 它们不是宪法，不应反向覆盖主文档语义。

## 2. Codex 读取顺序

### 2.1 新线程默认读取顺序

1. `新聊天开场白.md`
2. `PROJECT_BASELINE.md`
3. `STANDARD_PATCH_FRAMEWORK.md`
4. `CURRENT_STATE.md`
5. 本线程涉及的策略语义文档
6. 必要的代码流程文档、审计报告、源码事实

### 2.2 任务类型对应读取路径

#### 语义讨论

先读：

1. `PROJECT_BASELINE.md`
2. `CURRENT_STATE.md`
3. 对应策略语义文档

#### 正式 Patch

先读：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`
4. 对应策略语义文档
5. 目标代码与现场事实

#### 审计 / review

先读：

1. `PROJECT_BASELINE.md`
2. `CURRENT_STATE.md`
3. 对应策略语义文档
4. 落盘事实、日志、报告、源码

## 3. Codex 协作边界

1. 不靠聊天历史残留记忆替代仓库文档。
2. 不把旧阶段结论默认为当前主线，除非 `CURRENT_STATE.md` 仍然明确保留。
3. 不把策略内部语义偷换成项目公共语义。
4. 不在未统一语义时直接进入代码修改。
5. 不在未获批准时执行 `git push`。
6. 不碰生产发布。

## 4. 文档维护原则

1. 主文档负责边界，策略文档负责特例，归档文档负责事实沉淀。
2. 新增语义时，优先补主文档或策略语义文档，不把关键语义塞进聊天记录。
3. 新阶段切换时，优先更新 `CURRENT_STATE.md` 与 `新聊天开场白.md`。
4. 若新增 Codex 协作规则，应先确认不与宪法和 Patch 框架冲突。
