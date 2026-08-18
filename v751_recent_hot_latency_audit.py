#!/usr/bin/env python3
"""MEMECOIN LAB — RECENT HOT LATENCY AUDIT V7.5.1

Read-only audit for the active V7.5.0 acquisition trace.
Focuses on recent completed HOT events so inherited startup debt does not pollute
latency estimates after the queue has drained.

Reports:
- current queue health from the latest queue_sample;
- recent-N HOT enqueue->claim / HTTP / enqueue->store p50/p90/p95/max;
- time-window HOT latency for the last 30s / 60s / 120s;
- verdict for whether the *current* acquisition path is stable enough to support
  a fresh future-only arena.

Research/infrastructure diagnostics only. Read-only. Never signs/submits transactions.
"""
from __future__ import annotations

import sqlite3, statistics, time
from pathlib import Path

ROOT = Path.home()/"memecoin_lab"
TRACE = ROOT/'v750_acquisition_trace.db'
RECENT_N = 300


def ro():
    d = sqlite3.connect(f'file:{TRACE}?mode=ro', uri=True, timeout=30)
    d.row_factory = sqlite3.Row
    d.execute('PRAGMA query_only=ON')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def pct(xs, q):
    if not xs:
        return None
    ys = sorted(float(x) for x in xs)
    p = (len(ys)-1)*q
    lo = int(p); hi = min(len(ys)-1, lo+1); f = p-lo
    return ys[lo] + (ys[hi]-ys[lo])*f


def fmt(x):
    return 'NA' if x is None else f'{x:.3f}'


def summarize(rows):
    claim=[]; http=[]; total=[]
    for r in rows:
        if r['enqueue_at'] is not None and r['first_claim_at'] is not None:
            claim.append(max(0.0,float(r['first_claim_at'])-float(r['enqueue_at'])))
        if r['http_start_at'] is not None and r['http_end_at'] is not None:
            http.append(max(0.0,float(r['http_end_at'])-float(r['http_start_at'])))
        if r['enqueue_at'] is not None and r['raw_store_at'] is not None:
            total.append(max(0.0,float(r['raw_store_at'])-float(r['enqueue_at'])))
    def stats(xs):
        return {
            'n':len(xs), 'p50':pct(xs,.5), 'p90':pct(xs,.9), 'p95':pct(xs,.95),
            'max':max(xs) if xs else None,
        }
    return stats(claim),stats(http),stats(total)


def line(label,s):
    print(f"{label:<26} n={s['n']:4d} p50={fmt(s['p50']):>7}s p90={fmt(s['p90']):>7}s p95={fmt(s['p95']):>7}s max={fmt(s['max']):>7}s")


def main():
    if not TRACE.exists():
        raise SystemExit('Missing v750_acquisition_trace.db; run V7.5.0 first')
    d=ro()
    e=d.execute("SELECT epoch_id,MAX(updated_at) newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1").fetchone()
    if not e:
        d.close(); raise SystemExit('No V750 epoch found')
    epoch=str(e['epoch_id'])
    q=d.execute("SELECT * FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1",(epoch,)).fetchone()
    recent=d.execute("""SELECT enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at
        FROM trace WHERE epoch_id=? AND kind='HOT' AND raw_store_at IS NOT NULL
        ORDER BY raw_store_at DESC LIMIT ?""",(epoch,RECENT_N)).fetchall()

    print('='*178)
    print('MEMECOIN LAB — RECENT HOT LATENCY AUDIT V7.5.1')
    print('='*178)
    print(f'epoch={epoch} | recent_N={RECENT_N} | READ-ONLY')
    if q:
        age=max(0.0,time.time()-float(q['sampled_at']))
        print(f"QUEUE now pending={int(q['pending_total'])} HOT={int(q['pending_hot'])} CREATE={int(q['pending_create'])} fetching={int(q['fetching'])} oldest={float(q['oldest_pending_age_s']):.2f}s rps={float(q['current_rps']):.1f} sample_age={age:.1f}s")
    else:
        print('QUEUE now unavailable')

    c,h,t=summarize(recent)
    print('\nRECENT-N HOT')
    line('enqueue -> claim',c)
    line('HTTP start -> end',h)
    line('enqueue -> raw store',t)

    now=time.time()
    windows={}
    for sec in (30,60,120):
        rows=d.execute("""SELECT enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at
            FROM trace WHERE epoch_id=? AND kind='HOT' AND raw_store_at IS NOT NULL AND raw_store_at>=?
            ORDER BY raw_store_at""",(epoch,now-sec)).fetchall()
        windows[sec]=summarize(rows)

    print('\nTIME WINDOWS HOT')
    for sec,(cc,hh,tt) in windows.items():
        print(f'last_{sec:03d}s completed={tt["n"]:4d} | claim p90={fmt(cc["p90"]):>7}s | HTTP p90={fmt(hh["p90"]):>7}s | total p50/p90/p95={fmt(tt["p50"])}/{fmt(tt["p90"])}/{fmt(tt["p95"])}s')

    d.close()

    # Strict current-health verdict uses recent windows, not startup history.
    q_ok=bool(q and int(q['pending_total'])==0 and float(q['oldest_pending_age_s'])<2.0)
    # prefer 60s if enough rows, otherwise recent-N
    tt60=windows[60][2]
    source=tt60 if tt60['n']>=50 else t
    latency_ok=(source['n']>=50 and source['p90'] is not None and source['p90']<=2.0 and source['p95'] is not None and source['p95']<=3.0)
    verdict='CURRENT_PATH_STABLE' if q_ok and latency_ok else 'NOT_YET_STABLE'

    print('\nVERDICT')
    print(f'  {verdict}')
    print(f'  queue_ok={q_ok} | latency_sample_n={source["n"]} p90={fmt(source["p90"])}s p95={fmt(source["p95"])}s')
    print('  Criteria: pending=0, oldest<2s, >=50 recent HOT, p90<=2s, p95<=3s.')
    print('  If stable, keep V750 running and wait for at least one fresh burst; do not reuse V749 evidence.')
    print('  A final post-burst stable window is required before opening the next future-only arena.')

if __name__=='__main__':
    main()
