from pathlib import Path
from datetime import datetime, timezone, timedelta

stage_dir = Path("state/live_audit/stage_audit")
bj = timezone(timedelta(hours=8))

# 今天是北京时间“现在”，保留今天和昨天 => 删除 < 今天00:00 - 1天
now_bj = datetime.now(timezone.utc).astimezone(bj)
today_bj = now_bj.replace(hour=0, minute=0, second=0, microsecond=0)
cutoff_bj = today_bj - timedelta(days=1)

print("now_bj   :", now_bj.strftime("%Y-%m-%d %H:%M:%S"))
print("cutoff_bj:", cutoff_bj.strftime("%Y-%m-%d %H:%M:%S"))
print("rule     : delete stage3 data with BJ time < cutoff_bj")
print()

for p in sorted(stage_dir.glob("snapback_*.stage3_enriched.*.jsonl")):
    # 只匹配分日文件：...stage3_enriched.YYYY-MM-DD.jsonl
    parts = p.name.split(".")
    if len(parts) < 4:
        continue
    day_str = parts[-2]
    try:
        day_bj = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=bj)
    except Exception:
        continue
    if day_bj < cutoff_bj:
        print("DELETE JSONL :", p)

for p in sorted(stage_dir.glob("snapback_*.stage3_enriched.jsonl")):
    print("DELETE LEGACY:", p)

for p in sorted(stage_dir.glob("snapback_*.stage3_bars.*.parquet")):
    parts = p.name.split(".")
    if len(parts) < 5:
        continue
    try:
        bar_ts = int(parts[-2])
    except Exception:
        continue
    bar_bj = datetime.fromtimestamp(bar_ts / 1000.0, tz=timezone.utc).astimezone(bj)
    if bar_bj < cutoff_bj:
        print("DELETE PARQUET:", p)