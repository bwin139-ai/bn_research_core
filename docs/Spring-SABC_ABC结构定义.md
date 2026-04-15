1. C 固定为 HBs[0]。

2. 从 C 向左在 pattern_window_mins 内逐根搜索 B。

3. B 初筛条件：
   C_close > B_close。

4. 找到待定 B 后，向左识别 B 所属的 close 严格连续下跌段。
   A = 该连续下跌段的最早起点。
   A 不要求是局部高点。

5. AB 必须满足：
   ab_bars >= max(
       consecutive_down_bars_min,
       ceil(bc_bars / bc_over_ab_bars_max)
   )

6. AB 连跌只比较 close，不比较 low。

7. AB 低点确认：
   B_low 必须等于 A-B 区间最低 low。
   即 B 是这段洗盘的真实最低点。

8. AB 跌幅：
   (A_close - B_close) / A_close >= ab.chg_pct_min

9. BC 收回：
   (C_close - B_close) / (A_close - B_close) >= rebound.ratio_min

10. AB 爆量：
   AB 平均成交量 / baseline_window 平均成交量 >= vol_climax.ratio_min

11. 从近到远扫描 B。
    第一组完整满足条件的 A-B-C 即为唯一结构。
    找到后立即停止，不再比较更远处结构。