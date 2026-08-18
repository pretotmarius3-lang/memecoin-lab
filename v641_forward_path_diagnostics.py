#!/usr/bin/env python3
"""V6.4.1 — read-only diagnostics for V6.4 forward failures.

Does NOT modify V6.4 events, frozen rules, snapshots, swaps, or thresholds.
Explains NO_FILL / SPARSE_PATH / ANOMALY using the exact frozen V6.4 definitions.
"""
from __future__ import annotations
import math, sqlite3
from pathlib import Path
import v41_core as core
import v60_economic_edge_discovery_engine as v60

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/"v52_features.db"

def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def main():
    rdb=core.open_research(); rdb.row_factory=sqlite3.Row
    rule=rdb.execute("SELECT * FROM v64_frozen_rule LIMIT 1").fetchone()
    if not rule:
        print("No frozen V6.4 rule."); return
    rows=rdb.execute("""SELECT * FROM v64_forward_events
      WHERE rule_id=? AND state IN ('NO_FILL','SPARSE_PATH','ANOMALY')
      ORDER BY cutoff_ts,token_mint""",(rule['rule_id'],)).fetchall()
    vdb=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=30); vdb.row_factory=sqlite3.Row
    print('='*150)
    print('MEMECOIN LAB — V6.4.1 FORWARD PATH DIAGNOSTICS (READ ONLY)')
    print('='*150)
    print(f"rule={rule['rule_id']} feature={rule['feature']} stage={rule['stage_s']} h={rule['horizon_s']} fill<={rule['fill_window_s']:.0f}s")
    print(f"anomaly limits: abs_step>{v60.MAX_ABS_STEP_PCT:.0f}% OR abs_path_return>{v60.MAX_ABS_PATH_RETURN_PCT:.0f}% | min_path_points={v60.MIN_PATH_POINTS}\n")
    agg={'NO_FILL':0,'SPARSE_PATH':0,'ANOMALY':0}; causes={}
    details=[]
    for e in rows:
        st=e['state']; agg[st]+=1; token=e['token_mint']; cutoff=float(e['cutoff_ts'])
        fill_deadline=cutoff+float(rule['fill_window_s'])
        if st=='NO_FILL':
            a=vdb.execute("SELECT COUNT(*),MIN(timestamp),MAX(timestamp) FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0",(token,cutoff,fill_deadline)).fetchone()
            b=vdb.execute("SELECT MIN(timestamp) FROM v52_swaps WHERE token_mint=? AND timestamp>? AND price_sol IS NOT NULL AND price_sol>0",(token,fill_deadline)).fetchone()
            late=(sf(b[0]) - cutoff) if b and sf(b[0]) is not None else None
            cause='NO_PRICE_0_15S' if int(a[0] or 0)==0 else 'OTHER_NO_FILL'
            causes[cause]=causes.get(cause,0)+1
            details.append((token[:10],st,cause,int(a[0] or 0),late,None,None,None))
            continue
        fill_ts=sf(e['fill_ts']); entry=sf(e['fill_price'])
        if fill_ts is None or entry is None or entry<=0:
            cause='MISSING_FILL_FIELDS'; causes[cause]=causes.get(cause,0)+1
            details.append((token[:10],st,cause,0,None,None,None,None)); continue
        end=fill_ts+int(rule['horizon_s'])
        ps=vdb.execute("SELECT timestamp,price_sol FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp",(token,fill_ts,end)).fetchall()
        prices=[float(x['price_sol']) for x in ps]; n=len(prices)
        if st=='SPARSE_PATH':
            cause=f'PATH_POINTS_{n}_LT_{v60.MIN_PATH_POINTS}'; causes[cause]=causes.get(cause,0)+1
            last_gap=(end-float(ps[-1]['timestamp'])) if ps else float(rule['horizon_s'])
            details.append((token[:10],st,cause,n,None,last_gap,None,None)); continue
        allp=[entry]+prices
        steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]
        rets=[100*(p/entry-1) for p in prices]
        maxstep=max((abs(z) for z in steps),default=0.0); maxret=max((abs(z) for z in rets),default=0.0)
        step_bad=maxstep>v60.MAX_ABS_STEP_PCT; ret_bad=maxret>v60.MAX_ABS_PATH_RETURN_PCT
        cause='STEP_AND_PATH' if step_bad and ret_bad else ('STEP_SPIKE' if step_bad else ('PATH_RETURN' if ret_bad else 'UNRESOLVED_ANOMALY'))
        causes[cause]=causes.get(cause,0)+1
        details.append((token[:10],st,cause,n,None,None,maxstep,maxret))
    print('COUNTS:', ' | '.join(f'{k}={v}' for k,v in agg.items()), '| total=',len(rows))
    print('CAUSES:', ' | '.join(f'{k}={v}' for k,v in sorted(causes.items(),key=lambda x:(-x[1],x[0]))))
    print('\nTOKEN      STATE        CAUSE                 PTS  LATE_FILL  END_GAP   MAX_STEP%   MAX_PATH%')
    print('-'*120)
    for token,st,cause,n,late,gap,ms,mr in details:
        print(f"{token:<10} {st:<12} {cause:<21} {n:>3}  {('-' if late is None else f'{late:.1f}s'):>9}  {('-' if gap is None else f'{gap:.1f}s'):>7}  {('-' if ms is None else f'{ms:.1f}'):>10}  {('-' if mr is None else f'{mr:.1f}'):>10}")
    print('\nInterpretation: this script diagnoses only; it never reclassifies observations or changes the frozen arena.')
    vdb.close(); rdb.close()

if __name__=='__main__': main()
