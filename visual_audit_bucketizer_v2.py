#!/usr/bin/env python3
import argparse, csv, json, os, re, shutil
from collections import defaultdict
from datetime import datetime, timezone, timedelta

BJ = timezone(timedelta(hours=8))
REASON_MAP = {"TAKE_PROFIT":"TP", "TIME_STOP":"TS", "STOP_LOSS":"SL"}
PNG_RE = re.compile(r'^SNAP_(\d{8})_(\d{4})_(.+?)_(TP|TS|SL)\.png$')

AB_BUCKETS = [(-10**9,2,'<=2'),(2,4,'(2,4]'),(4,6,'(4,6]'),(6,10,'(6,10]'),(10,10**9,'>10')]
RB_BUCKETS = [(-10**9,0.15,'<=0.15'),(0.15,0.30,'(0.15,0.30]'),(0.30,0.50,'(0.30,0.50]'),(0.50,0.70,'(0.50,0.70]'),(0.70,10**9,'>0.70')]

def ab_bucket(v):
    for lo,hi,label in AB_BUCKETS:
        if v <= hi and v > lo:
            return label
    return 'UNKNOWN'

def rb_bucket(v):
    for lo,hi,label in RB_BUCKETS:
        if v <= hi and v > lo:
            return label
    return 'UNKNOWN'

def profit_bucket(p):
    if p > 0: return 'profit'
    if p < 0: return 'loss'
    return 'flat'

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def compute_geom(ctx):
    a,b,c = ctx.get('a_time'), ctx.get('b_time'), ctx.get('c_time')
    if a is None or b is None or c is None:
        return None,None,None
    if not (a <= b < c):
        return None,None,None
    if ((b-a)%60000)!=0 or ((c-b)%60000)!=0:
        return None,None,None
    ab = int((b-a)//60000)
    bc = int((c-b)//60000)
    ratio = (bc/ab) if ab>0 else None
    return ab,bc,ratio

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--png-dir', required=True)
    ap.add_argument('--trades-jsonl', required=True)
    ap.add_argument('--out-dir', required=True)
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--copy-mode', choices=['copy','symlink'], default='copy')
    args = ap.parse_args()

    reports_dir = os.path.join(args.out_dir, 'reports')
    ensure_dir(reports_dir)

    trades = []
    by_key = {}
    unmatched_trades = []
    with open(args.trades_jsonl, 'r', encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line: continue
            t = json.loads(line)
            ctx = t.get('context', {}) or {}
            ab, bc, ratio = compute_geom(ctx)
            if ab is None:
                unmatched_trades.append({
                    'symbol': t.get('symbol',''),
                    'signal_time_bj': t.get('signal_time_bj',''),
                    'reason': t.get('reason',''),
                    'why': 'missing_or_invalid_geometry_times'
                })
                continue
            symbol = t.get('symbol','')
            signal_time_bj = t.get('signal_time_bj','')
            reason_short = REASON_MAP.get(t.get('reason',''), '')
            minute_key = ''
            if signal_time_bj:
                # expects 'YYYY-MM-DD HH:MM'
                minute_key = signal_time_bj.replace('-','').replace(':','').replace(' ','_')
            key = (minute_key, symbol, reason_short)
            rec = {
                'symbol': symbol,
                'signal_time_bj': signal_time_bj,
                'signal_time': t.get('signal_time'),
                'entry_time': t.get('entry_time'),
                'exit_time': t.get('exit_time'),
                'pnl_pct': safe_float(t.get('pnl_pct')),
                'reason': t.get('reason',''),
                'ab_bars': ab,
                'bc_bars': bc,
                'bc_ab_ratio': ratio,
                'drop_pct': safe_float(ctx.get('drop_pct')),
                'rebound_ratio': safe_float(ctx.get('rebound_ratio')),
                'drop_window_chg': safe_float(ctx.get('drop_window_chg')),
                'vol_ratio': safe_float(ctx.get('vol_ratio')),
                'png_filename': '',
                'cluster_id': '',
                'bucket_path': '',
            }
            by_key[key] = rec
            trades.append(rec)

    matched = 0
    unmatched_png = []
    cluster_rows = []
    cluster_stats = defaultdict(list)

    for name in sorted(os.listdir(args.png_dir)):
        m = PNG_RE.match(name)
        if not m:
            unmatched_png.append({'png_filename': name, 'why': 'filename_pattern_mismatch'})
            continue
        datepart, timepart, symbol, reason_short = m.groups()
        minute_key = f'{datepart}_{timepart}'
        key = (minute_key, symbol, reason_short)
        rec = by_key.get(key)
        if rec is None:
            unmatched_png.append({'png_filename': name, 'why': 'no_matching_trade'})
            continue
        matched += 1
        rec = rec.copy()
        rec['png_filename'] = name
        cid = f"ab_{ab_bucket(rec['ab_bars'])}__rb_{rb_bucket(rec['rebound_ratio'])}"
        outcome = profit_bucket(rec['pnl_pct'] if rec['pnl_pct'] is not None else 0)
        bucket_path = os.path.join(args.out_dir, cid, outcome)
        ensure_dir(bucket_path)
        src = os.path.join(args.png_dir, name)
        dst = os.path.join(bucket_path, name)
        if os.path.lexists(dst):
            os.remove(dst)
        if args.copy_mode == 'copy':
            shutil.copy2(src, dst)
        else:
            os.symlink(src, dst)
        rec['cluster_id'] = cid
        rec['bucket_path'] = bucket_path
        cluster_rows.append(rec)
        cluster_stats[cid].append(rec)

    cluster_index = os.path.join(reports_dir, 'cluster_index.csv')
    with open(cluster_index, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['cluster_id','bucket_path','png_filename','symbol','signal_time_bj','signal_time','entry_time','exit_time','pnl_pct','reason','ab_bars','bc_bars','bc_ab_ratio','drop_pct','rebound_ratio','drop_window_chg','vol_ratio'])
        w.writeheader(); w.writerows(cluster_rows)

    cluster_summary_path = os.path.join(reports_dir, 'cluster_summary.csv')
    with open(cluster_summary_path, 'w', newline='', encoding='utf-8') as f:
        fields = ['cluster_id','count','profit_count','loss_count','flat_count','avg_pnl_pct','median_pnl_pct','ab_bars_median','bc_bars_median','bc_ab_ratio_median','drop_pct_median','rebound_ratio_median','drop_window_chg_median']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for cid, rows in sorted(cluster_stats.items()):
            pnls = sorted([r['pnl_pct'] for r in rows if r['pnl_pct'] is not None])
            def med(vals):
                vals = sorted([v for v in vals if v is not None])
                if not vals: return ''
                n=len(vals)
                return vals[n//2] if n%2 else (vals[n//2-1]+vals[n//2])/2
            w.writerow({
                'cluster_id': cid,
                'count': len(rows),
                'profit_count': sum(1 for r in rows if (r['pnl_pct'] or 0) > 0),
                'loss_count': sum(1 for r in rows if (r['pnl_pct'] or 0) < 0),
                'flat_count': sum(1 for r in rows if (r['pnl_pct'] or 0) == 0),
                'avg_pnl_pct': sum(pnls)/len(pnls) if pnls else '',
                'median_pnl_pct': med([r['pnl_pct'] for r in rows]),
                'ab_bars_median': med([r['ab_bars'] for r in rows]),
                'bc_bars_median': med([r['bc_bars'] for r in rows]),
                'bc_ab_ratio_median': med([r['bc_ab_ratio'] for r in rows]),
                'drop_pct_median': med([r['drop_pct'] for r in rows]),
                'rebound_ratio_median': med([r['rebound_ratio'] for r in rows]),
                'drop_window_chg_median': med([r['drop_window_chg'] for r in rows]),
            })

    for filename, rows in [('unmatched_png.csv', unmatched_png), ('unmatched_trades.csv', unmatched_trades)]:
        path = os.path.join(reports_dir, filename)
        keys = sorted({k for r in rows for k in r.keys()}) if rows else ['why']
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader(); w.writerows(rows)

    summary = {
        'run_id': args.run_id,
        'png_dir': args.png_dir,
        'trades_jsonl': args.trades_jsonl,
        'out_dir': args.out_dir,
        'matched_png': matched,
        'unmatched_png': len(unmatched_png),
        'unmatched_trade': len(unmatched_trades),
        'clusters': len(cluster_stats),
    }
    with open(os.path.join(reports_dir,'summary.json'),'w',encoding='utf-8') as f:
        json.dump(summary,f,ensure_ascii=False,indent=2)

    print('=== visual_audit_bucketizer 完成 ===')
    print('run_id         :', args.run_id)
    print('png dir        :', args.png_dir)
    print('trades jsonl   :', args.trades_jsonl)
    print('out dir        :', args.out_dir)
    print('matched png    :', matched)
    print('unmatched png  :', len(unmatched_png))
    print('unmatched trade:', len(unmatched_trades))
    print('clusters       :', len(cluster_stats))

if __name__ == '__main__':
    main()
