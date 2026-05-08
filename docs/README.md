# bn_research_core 文档导航

本目录只保留当前协作需要的活跃文档。新线程不要靠旧聊天记忆恢复项目状态，应先读根目录 `AGENTS.md`，再按本文档进入具体材料。

## 必读入口

1. `../AGENTS.md`
   - Codex / AI agent 的根工作规则。
   - 定义启动检查、文档优先级、patch 纪律、归档边界。

2. `PROJECT_BASELINE.md`
   - 项目宪法。
   - 定义公共语义、LONG-only、fail-fast、事实优先、禁止伪兼容等最高规则。

3. `STANDARD_PATCH_FRAMEWORK.md`
   - 正式 patch 协作框架。
   - 定义单问题、单分类、锁基线、最小修改、验证与交付要求。

4. `CURRENT_STATE.md`
   - 当前现场快照。
   - 只记录当前正在做什么、做到哪里、下一步是什么。

## 策略语义

- `Spring-SABC项目语义基线.md`
  - Spring-SABC 的唯一活跃语义基线。
  - 涉及 Spring-SABC 时必须阅读。

- `Sweep-Reclaim项目语义基线.md`
  - Sweep-Reclaim / SWR 的唯一活跃语义基线。
  - 涉及 Sweep-Reclaim 时必须阅读。

- `TVR项目语义基线.md`
  - TradFi Value Reclaim / TVR 的唯一活跃语义基线。
  - 涉及 TVR data_hub、TradFi funding、rolling 24h 或后续 TVR live 时必须阅读。

## 代码理解材料

- `snapback-sabc_代码流程.md`
  - Snapback/SABC live/sim 相关术语和流程速查。
  - 只作为理解辅助，不覆盖主文档语义。

## 新线程开场白

- `新聊天开场白.md`
  - 可直接复制给新 Codex 线程的短启动提示。
  - 只负责指向活跃文档，不再复制大段历史内容。

- `新Codex线程开场白.txt`
  - 用户侧新线程模板。
  - 适合每次新开 Codex 线程时直接复制发送，并在末尾填写本轮任务。

## 归档区

- `archive/legacy/`
  - 旧版协作文档、旧入口、旧引用文件。

- `archive/reports/`
  - 阶段性审计报告和历史结论。
  - 当前 5 天推进总账：`archive/reports/2026-04-28_codex_5day_progress_summary.md`。

- `archive/scratch/`
  - 临时笔记、系统残留文件、一次性现场材料。

归档文件只提供历史参考。若归档内容与活跃文档冲突，一律以活跃文档为准。
