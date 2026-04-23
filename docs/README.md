# bn_research_core 文档导航

本目录用于承载本仓库的唯一语义基线、Patch 协作规则、当前现场状态、策略语义定义，以及面向 Codex 的协作入口。

当前文档优先级固定为：

1. `PROJECT_BASELINE.md`
2. `STANDARD_PATCH_FRAMEWORK.md`
3. `CURRENT_STATE.md`
4. 具体策略语义文档
5. 协作文档、归档报告、流程说明

## 1. 必读主文档

### 1.1 宪法层

- `PROJECT_BASELINE.md`
  - 项目最高优先级语义与禁令。
  - 适用于 `sim / live / audit / patch / review / 对话协作`。

### 1.2 Patch 协作层

- `STANDARD_PATCH_FRAMEWORK.md`
  - 正式 Patch 的进入条件、分类约束、交付顺序、最小修改原则。
  - 若与宪法冲突，以宪法为准。

### 1.3 现场状态层

- `CURRENT_STATE.md`
  - 当前主线、已完成事实、pending、明确不做、下一步顺序。
  - 只记录当前现场，不重写宪法和 Patch 框架。

## 2. Codex 协作入口

- `新聊天开场白.md`
  - 新线程启动入口。
  - 用于告诉 Codex 当前线程应该先读什么、当前主线是什么、哪些旧主线不要误拉回。

- `CODEX_DOC_SYSTEM.md`
  - 面向 Codex 的仓库文档分层说明。
  - 回答“遇到一个任务时，应该先查哪层文档”。

- `CODEX_COLLAB_WORKFLOW.md`
  - 面向 Codex 的标准协作流程。
  - 回答“从统一语义到执行 patch 与验证，应该按什么顺序做”。

## 3. 策略语义层

- `Spring-SABC项目语义基线.md`
  - Spring-SABC 的唯一策略语义基线。

- `Spring-SABC_ABC结构定义.md`
  - 旧引用入口。
  - 当前应以 `Spring-SABC项目语义基线.md` 为准。

## 4. 代码理解与归档层

- `snapback-sabc_代码流程.md`
  - 面向当前代码实现的流程理解材料。

- `snapback_sim_live_audit_report.md`
  - 审计结论与归档报告类材料。

## 5. 使用原则

1. 先读主文档，再读策略语义，再读实现与报告。
2. 先统一语义，再进入审代码、patch、验证。
3. 当前线程若需要正式 Patch，必须继续遵守：
   - 先锁基线
   - 一次只处理一个主问题
   - Patch 分类唯一
   - 输入一致 + 输出一致
4. 未经批准，不做 `git push`，不碰生产发布。
