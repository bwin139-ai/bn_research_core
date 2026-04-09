# /root/service_env/bin/python tools/cleanup_expired_stage3_run.py
# 运行前后执行命令记录现场
# find state/live_audit/stage_audit -type f -name "snapback_*.stage3_bars.*.parquet" | wc -l && find state/live_audit/stage_audit -type f -name "snapback_*.stage3_bars.*.parquet" -print0 | du --files0-from=- -ch | tail -n 1

from pathlib import Path
from datetime import datetime, timezone, timedelta

stage_dir = Path("state/live_audit/stage_audit")
bj = timezone(timedelta(hours=8))

now_bj = datetime.now(timezone.utc).astimezone(bj)
today_bj = now_bj.replace(hour=0, minute=0, second=0, microsecond=0)
cutoff_bj = today_bj - timedelta(days=1)

print("now_bj   :", now_bj.strftime("%Y-%m-%d %H:%M:%S"))
print("cutoff_bj:", cutoff_bj.strftime("%Y-%m-%d %H:%M:%S"))
print("APPLY    : delete stage3 data with BJ time < cutoff_bj")
print()

deleted = 0

for p in sorted(stage_dir.glob("snapback_*.stage3_enriched.*.jsonl")):
    parts = p.name.split(".")
    if len(parts) < 4:
        continue
    day_str = parts[-2]
    try:
        day_bj = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=bj)
    except Exception:
        continue
    if day_bj < cutoff_bj:
        p.unlink(missing_ok=True)
        print("deleted jsonl :", p)
        deleted += 1

for p in sorted(stage_dir.glob("snapback_*.stage3_enriched.jsonl")):
    p.unlink(missing_ok=True)
    print("deleted legacy:", p)
    deleted += 1

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
        p.unlink(missing_ok=True)
        print("deleted parquet:", p)
        deleted += 1

print()
print("total_deleted:", deleted)