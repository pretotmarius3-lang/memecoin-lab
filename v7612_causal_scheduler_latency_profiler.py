#!/usr/bin/env python3
"""MEMECOIN LAB — CAUSAL SCHEDULER LATENCY PROFILER V7.6.1.2

READ-ONLY profiler for V7611 causal snapshots. Decomposes build_lag into:
- first canonical transport lag: first_observed_at - first_ts
- stage due -> first input availability
- stage due -> built_at (existing build_lag)
- last required input observed_at -> built_at (scheduler/service residual)

No writes, no strategy logic, no retuning.
"""
from __future__ import annotations
import sqlite3,time,statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
DB=ROOT/"v52_features.db"
WINDOW_S=300


def pct(xs,q):
    if not xs:return None
    ys=sorted(float(x) for x in xs)
    p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f

def fmt(xs):
    if not xs:return "n=0"
    return f"n={len(xs):4d} p50={pct(xs,.5):7.3f}s p90={pct(xs,.9):7.3f}s p95={pct(xs,.95):7.3f}s max={max(xs):7.3f}s"

def main():
    d=sqlite3.connect(f"file:{DB}?mode=ro",uri=True)
    d.row_factory=sqlite3.Row
    now=time.time()
    rows=d.execute('''
      SELECT c.token_mint,c.stage_s,c.first_ts,c.first_observed_at,c.cutoff_ts,c.built_at,c.build_lag_s,
             (SELECT MAX(s.observed_at) FROM v52_swaps s
               WHERE s.token_mint=c.token_mint AND s.timestamp<=c.cutoff_ts) AS last_input_observed_at,
             (SELECT MIN(s.observed_at) FROM v52_swaps s
               WHERE s.token_mint=c.token_mint) AS actual_first_observed_at
      FROM v7611_causal_snapshots c
      WHERE c.built_at>=? AND c.stage_s IN (20,30)
      ORDER BY c.built_at
    ''',(now-WINDOW_S,)).fetchall()
    print('='*150)
    print('MEMECOIN LAB — CAUSAL SCHEDULER LATENCY PROFILER V7.6.1.2')
    print('='*150)
    print(f'window={WINDOW_S}s rows={len(rows)} READ-ONLY')
    if not rows:
        print('No recent T+20/T+30 causal rows yet.')
        return
    transport=[]; build=[]; late_input=[]; residual=[]; first_obs_residual=[]
    by_stage={20:[],30:[]}
    for r in rows:
        first_ts=float(r['first_ts']); first_obs=float(r['first_observed_at']); cut=float(r['cutoff_ts']); built=float(r['built_at'])
        li=r['last_input_observed_at']
        transport.append(max(0.0,first_obs-first_ts))
        build.append(max(0.0,built-cut))
        if li is not None:
            li=float(li)
            late_input.append(max(0.0,li-cut))
            residual.append(max(0.0,built-max(cut,li)))
        first_obs_residual.append(max(0.0,built-max(cut,first_obs)))
        by_stage[int(r['stage_s'])].append(max(0.0,built-cut))
    print('\nDECOMPOSITION')
    print('first chain ts -> first canonical observed '.ljust(42),fmt(transport))
    print('stage due -> causal built '.ljust(42),fmt(build))
    print('stage due -> last input observed '.ljust(42),fmt(late_input))
    print('last input available -> causal built '.ljust(42),fmt(residual))
    print('max(stage due, first obs) -> built '.ljust(42),fmt(first_obs_residual))
    print('\nBY STAGE')
    for st in (20,30):
        print(f'T+{st} build lag'.ljust(42),fmt(by_stage[st]))
    tp90=pct(transport,.9) or 0; rp90=pct(residual,.9) or 0; bp90=pct(build,.9) or 0; lip90=pct(late_input,.9) or 0
    if tp90>=max(5.0,rp90*2): verdict='UPSTREAM_ACQUISITION_LATENCY_DOMINANT'
    elif lip90>=max(3.0,rp90*2): verdict='LATE_INPUT_ARRIVAL_DOMINANT'
    elif rp90>=3.0: verdict='SCHEDULER_SERVICE_LATENCY_DOMINANT'
    else: verdict='SCHEDULER_FAST_BUILD_LAG_MOSTLY_EXTERNAL'
    print('\nVERDICT')
    print(f'  {verdict}')
    print(f'  transport_p90={tp90:.3f}s late_input_p90={lip90:.3f}s scheduler_residual_p90={rp90:.3f}s total_build_p90={bp90:.3f}s')
    print('\nGuardrail: read-only timing diagnosis; do not reuse V7611 rows as strategy evidence.')

if __name__=='__main__':main()
