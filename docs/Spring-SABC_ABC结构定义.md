# Spring-SABC ABC结构定义

本文档内容已合并进：

```text
docs/Spring-SABC项目语义基线.md
```

后续以 `Spring-SABC项目语义基线.md` 为 Spring-SABC 的唯一语义文件。

保留本文件仅作为旧引用入口，避免新聊天或历史记录找不到原文档。

## 合并后的核心结论

1. C 固定为 HBs[0]。
2. 从 C 向左在 `pattern_window_mins` 内逐根搜索 B。
3. B 初筛条件：`C_close > B_close`。
4. 找到待定 B 后，向左识别 B 所属的 close 严格连续下跌段，A 为该连续下跌段最早起点，A 不要求是局部高点。
5. AB 连跌只比较 close，不比较 low。
6. AB 必须满足：`ab_bars >= max(consecutive_down_bars_min, ceil(bc_bars / bc_over_ab_bars_max))`。
7. B_low 必须等于 A-B 区间最低 low。
8. AB 跌幅必须满足：`(A_close - B_close) / A_close >= ab.chg_pct_min`。
9. BC 收回必须满足：`(C_close - B_close) / (A_close - B_close) >= rebound.ratio_min`。
10. AB 爆量使用固定 baseline_window，不使用 S-A 作为量能基线。
11. B 从近到远扫描，第一组完整满足条件的 A-B-C 即为唯一结构，找到后立即停止。
