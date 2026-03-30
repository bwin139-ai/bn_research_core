# 标准化 Patch 协作框架 v1

## 0. 文档定位
本文件用于规范 **每一次 patch 任务的发起方式、约束方式、交付方式**。  
它不是项目语义宪法，不替代 `PROJECT_BASELINE_v1.md`。  

两者关系如下：

- `PROJECT_BASELINE_v1.md`：规定 **原则、边界、禁令、优先级**
- `STANDARD_PATCH_FRAMEWORK_v1.md`：规定 **patch 任务怎样发起、怎样回答、怎样交付**

换句话说：

- 宪法回答的是：**什么允许，什么禁止**
- 本文件回答的是：**一次 patch 任务应该怎样标准化推进**

---

## 1. 使用范围
本框架适用于本项目内全部：

- patch 请求
- patch 方案输出
- patch 脚本交付
- patch 命令交付
- 指纹锁定与验证
- 新聊天中 patch 任务的开场白

若无显式例外，默认全部 patch 协作都遵守本文件。

---

## 2. 基本原则
每次 patch 必须遵守以下原则：

### 2.1 单目标原则
一次 patch 只处理一个主问题。  
禁止把多个无关问题混在一刀里。

### 2.2 先锁现场，再动代码
必须先提供：

- 目标问题
- 已知事实
- 当前不处理项
- 锁定指纹

没有锁定指纹，不进入 patch 阶段。

### 2.3 patch 分类必须明确
每次 patch 必须显式声明且只能属于以下之一：

- `PERF_ONLY`
- `ARCH_ONLY`
- `LOGIC_ONLY`

禁止混改。  
如果确需跨类，必须拆刀顺序推进。

### 2.4 最小外科式修改
只改当前问题所必需的文件与代码。  
禁止顺手重构。  
禁止顺手清理无关代码。  
禁止顺手改日志、命名、风格，除非它就是本刀目标的一部分。

### 2.5 基于事实，不做猜测
patch 方案必须基于：

- 代码事实
- 日志事实
- state 文件事实
- audit 文件事实
- 运行结果事实

缺少事实时，先补事实，不得直接臆测。

---

## 3. 标准 patch 请求结构
每次新聊天或每次发起新 patch，请尽量按以下结构组织输入。

### 3.1 patch目标
用一句话写清这次只处理什么问题：

```text
本次只处理一个问题：
<一句话描述当前唯一目标>
```

示例：

```text
本次只处理一个问题：
修复已平仓后 exit reason inference 被 time-stop 空身份误伤，导致本地 open_trade 残留的问题。
```

### 3.2 patch分类
必须显式说明：

```text
本次 patch 分类：
LOGIC_ONLY
```

只能三选一：

- `PERF_ONLY`
- `ARCH_ONLY`
- `LOGIC_ONLY`

### 3.3 当前已确认事实
把已经确认过的事实写清楚，避免 AI 回到旧分支。

标准格式：

```text
当前已确认事实：
1. <事实1>
2. <事实2>
3. <事实3>
```

示例：

```text
当前已确认事实：
1. 4USDT 的开仓成功、TP/SL 挂单成功。
2. 4USDT 后续平仓后，本地 state 残留 open_trade + last_error。
3. startup blocked 的直接原因是 active_state_errors，不是 orphan gate 太严格。
```

### 3.4 当前不处理项
把本刀明确排除的内容写出来，防止发散：

```text
当前不处理：
- <不处理项1>
- <不处理项2>
```

示例：

```text
当前不处理：
- startup gate 放宽
- ratio / _pct 语义治理
- 其他非 exit reason 主线问题
```

### 3.5 优先审查文件
明确告诉 AI 本刀优先看哪些文件：

```text
优先审查文件：
- <file1>
- <file2>
```

如无必要，不扩展到其他文件。

### 3.6 锁定指纹
进入 patch 前必须锁定基线文件指纹。

标准格式：

```text
对齐指纹和副本：
<file_path_1>
  MD5  : <md5>
  Lines: <lines>
  Copy : <copy_name>

<file_path_2>
  MD5  : <md5>
  Lines: <lines>
  Copy : <copy_name>
```

要求：

- 指纹不一致必须中断
- 不得基于未知版本 patch
- 不得默认“应该差不多”

---

## 4. 标准 patch 交付顺序
每次要求 AI 输出 patch 时，固定要求以下顺序：

1. `patch方案`
2. `patch脚本附件`
3. `patch执行命令行`
4. `预期新指纹`
5. `一行 git add`
6. `一行 git commit`

可选增加：
7. `风险说明`
8. `验证步骤`

---

## 5. 标准 patch 回答结构
建议 AI 每次 patch 回复按以下格式输出。

### 5.1 patch分类与变更范围
```text
这刀继续是 <PATCH_CLASS>
只动 <N> 个文件：
- <file1>
- <file2>

不动：
- <file3>
- <file4>
```

目的：

- 先把边界说死
- 让本刀范围清晰
- 防止混改

### 5.2 这刀的依据
```text
这刀的依据很清楚：
<简洁说明当前 patch 依据>
```

要求：

- 必须基于代码事实 / 日志事实 / state 事实
- 不要空泛描述
- 不要复读无关背景

### 5.3 patch方案
```text
所以这刀的 patch 方案是：
1. <改动点1>
2. <改动点2>
3. <明确不改什么>
```

要求：

- 改动点要能落到代码行为
- 不要只写“优化逻辑”“增强健壮性”这种空话
- 必须说明不改什么

### 5.4 patch脚本附件
必须给出附件，不在聊天框直接输出整段脚本：

```text
patch脚本附件：
<filename.py>
```

### 5.5 打完预期新指纹
如能提前推演，建议一起给出：

```text
打完预期新指纹：
- <file1>
  - MD5: ...
  - Lines: ...
- <file2>
  - MD5: ...
  - Lines: ...
```

注意：

- 如果只是预期，必须基于实际 patch 内容推演
- 不要伪造

### 5.6 patch执行命令行
命令要尽量标准化，通常包含：

- 运行 patch
- py_compile
- 生成 md5/line 副本

示例格式：

```bash
python3 <patch_script>.py && python3 -m py_compile <file1> <file2> && python3 tools/make_md5_line_suffix_copies.py <file1> <file2>
```

### 5.7 一行 git add
只 add 本刀涉及文件：

```bash
git add <file1> <file2>
```

### 5.8 一行 git commit
commit message 必须：

- 简洁
- 只描述本刀目标
- 不混入其他问题

示例：

```bash
git commit -m "snapback: ignore missing time-stop identity in exit reason inference"
```

---

## 6. 一次性脚本与正式 patch 的区分
并不是所有文件都要 commit。

### 6.1 一次性脚本
如果脚本是为了：

- 清理脏 state
- 导出一次性现场
- 修复某次事故后的临时处置
- 审计现场辅助查看

且未来不准备复用，那么它属于 **一次性脚本**。

处理方式：

- 可以直接给附件
- 不要求 commit
- 但必须说明它是一次性的

### 6.2 正式 patch
如果脚本是为了：

- 修改项目源码
- 形成长期保留行为
- 修复长期 bug
- 调整正式逻辑/结构/性能

则属于 **正式 patch**。

处理方式：

- 必须按标准 patch 流程交付
- 必须给 `git add`
- 必须给 `git commit`

---

## 7. 何时先清现场，何时先打 patch
### 7.1 先清现场
如果当前问题表现为：

- live 已无法启动
- gate 被旧脏状态阻断
- 当前 state 明显失真
- 不清现场就无法继续观察新样本

则应先：

1. 确认交易所真实现场
2. 备份 state
3. 一次性清理当前失真状态
4. 恢复 live 可运行性

这不属于掩盖 bug，而属于恢复现场可用性。

### 7.2 先打 patch
如果当前：

- 现场还能继续运行
- 不存在脏状态阻断
- 已经有足够事实定位根因

则应优先直接 patch。

---

## 8. 新聊天推荐开场白模板
以下模板可直接复制到新聊天。

```text
我们继续推进 bn_research_core / strategies/snapback。

本次只处理一个问题：
<一句话目标>

本次 patch 分类：
<PERF_ONLY / ARCH_ONLY / LOGIC_ONLY>

当前已确认事实：
1. <事实1>
2. <事实2>
3. <事实3>

当前不处理：
- <不处理项1>
- <不处理项2>

优先审查文件：
- <file1>
- <file2>

固定现场路径：
- live state：state/live/snapback_mybwin139.state.json
- audit jsonl：state/live_audit/snapback_mybwin139.jsonl

请按以下顺序输出：
1. patch方案
2. patch脚本附件
3. patch执行命令行
4. 一行 git add
5. 一行 git commit

输出必须基于以下锁定指纹，不一致必须中断：

<file_path_1>
  MD5  : <md5>
  Lines: <lines>
  Copy : <copy_name>

<file_path_2>
  MD5  : <md5>
  Lines: <lines>
  Copy : <copy_name>
```

---

## 9. 与《项目基线（宪法）v1》的关系
本文件必须服从：

- `PROJECT_BASELINE_v1.md`

当本文件与宪法发生冲突时，优先级如下：

1. `PROJECT_BASELINE_v1.md`
2. `STANDARD_PATCH_FRAMEWORK_v1.md`
3. 单次聊天中的 patch 请求说明

---

## 10. 最终目标
使用本文件的目的不是让回复“看起来更整齐”，而是为了：

- 降低重复沟通成本
- 降低跑偏概率
- 降低混改概率
- 让 patch 更可审计
- 让新聊天能稳定续接旧现场

如果未来实践中发现某些环节还不够稳，可以继续迭代到 `STANDARD_PATCH_FRAMEWORK_v2.md`。
