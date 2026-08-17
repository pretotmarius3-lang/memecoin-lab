#!/usr/bin/env python3
"""V6.2.1 read-only forward-gate diagnostics.

Explains why frozen V6.2 future signals have not become scored trades.
Does not modify champions, thresholds, forward trades, or any research state.
"""
from __future__ import annotations

import math, sqlite3, time
from collections import Counter

import v41_core as core
import v60_economic_edge_discovery_engine as v60
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/"v52_features.db"


def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d


def open_v52():
    d=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute("PRAGMA busy_timeout=30000")
    return d


def diagnose(c,db):
    rows=db.execute(f"""SELECT token_mint,cutoff_ts,{c['feature']} AS feature
        FROM v52_snapshots
        WHERE stage_s=? AND cutoff_ts>? AND {c['feature']} IS NOT NULL
        ORDER BY cutoff_ts,token_mint""",
        (int(c['stage_s']),float(c['frozen_max_cutoff_ts']))).fetchall()
    counts=Counter(); gaps=[]; pathn=[]; examples=[]; now=time.time()
    for r in rows:
        x=sf(r['feature'])
        if x is None: continue
        counts['eligible']+=1
        if float(c['direction'])*x<float(c['threshold']):
            counts['rule_miss']+=1; continue
        counts['signals']+=1
        cutoff=float(r['cutoff_ts']); end=cutoff+int(c['horizon_s'])
        if now<end:
            counts['immature']+=1; continue
        counts['mature']+=1
        q=db.execute("""SELECT price_sol,timestamp FROM v52_swaps
            WHERE token_mint=? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0
            ORDER BY timestamp DESC LIMIT 1""",(r['token_mint'],cutoff)).fetchone()
        if not q:
            counts['no_entry_quote']+=1
            if len(examples)<8: examples.append((r['token_mint'][:10],'NO_ENTRY_QUOTE',cutoff,None,None))
            continue
        gap=cutoff-float(q['timestamp']); gaps.append(gap)
        if gap>v60.MAX_ENTRY_GAP_S:
            counts['stale_entry']+=1
            if len(examples)<8: examples.append((r['token_mint'][:10],'STALE_ENTRY',cutoff,gap,None))
            continue
        counts['entry_ok']+=1
        rs=db.execute("""SELECT price_sol,timestamp FROM v52_swaps
            WHERE token_mint=? AND timestamp>? AND timestamp<=?
              AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp""",
            (r['token_mint'],cutoff,end)).fetchall()
        pathn.append(len(rs))
        if len(rs)<v60.MIN_PATH_POINTS:
            counts['sparse_path']+=1
            if len(examples)<8: examples.append((r['token_mint'][:10],'SPARSE_PATH',cutoff,gap,len(rs)))
            continue
        entry=float(q['price_sol']); prices=[float(z['price_sol']) for z in rs]; allp=[entry]+prices
        steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]
        rets=[100*(p/entry-1) for p in prices]
        if any(abs(z)>v60.MAX_ABS_STEP_PCT for z in steps) or any(abs(z)>v60.MAX_ABS_PATH_RETURN_PCT for z in rets):
            counts['anomaly']+=1
            if len(examples)<8: examples.append((r['token_mint'][:10],'ANOMALY',cutoff,gap,len(rs)))
            continue
        counts['scorable']+=1
    return counts,gaps,pathn,examples


def median(xs):
    if not xs:return None
    xs=sorted(xs); n=len(xs); return xs[n//2] if n%2 else (xs[n//2-1]+xs[n//2])/2


def main():
    rd=core.open_research(); champs=[dict(r) for r in rd.execute("SELECT * FROM v62_champions ORDER BY source_expectancy DESC").fetchall()]; rd.close()
    db=open_v52()
    print('='*160)
    print('MEMECOIN LAB — V6.2.1 FORWARD GATE DIAGNOSTICS (READ ONLY)')
    print('='*160)
    print(f'entry_gap_max={v60.MAX_ENTRY_GAP_S}s | min_path_points={v60.MIN_PATH_POINTS} | step_cap={v60.MAX_ABS_STEP_PCT}% | return_cap={v60.MAX_ABS_PATH_RETURN_PCT}%\n')
    for i,c in enumerate(champs,1):
        ct,gaps,pathn,examples=diagnose(c,db)
        print(f"#{i} {c['family']} feature={c['feature']} stage={c['stage_s']} h={c['horizon_s']}")
        print('   ' + ' | '.join(f'{k}={ct.get(k,0)}' for k in ['eligible','signals','immature','mature','no_entry_quote','stale_entry','entry_ok','sparse_path','anomaly','scorable']))
        print(f"   entry_gap median={median(gaps) if gaps else None}s | path_points median={median(pathn) if pathn else None}")
        for ex in examples:
            print(f"      example token={ex[0]} reason={ex[1]} entry_gap={ex[3]} path_n={ex[4]}")
        print()
    db.close()
    print('Interpretation: SCORABLE>0 with V6.2 done=0 => arena bug. SCORABLE=0 identifies the data/execution gate currently blocking forward trades.')

if __name__=='__main__':main()
