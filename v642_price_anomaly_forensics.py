#!/usr/bin/env python3
"""V6.4.2 — read-only price anomaly forensics.

For each V6.4 ANOMALY event, locate the largest absolute price step inside the
frozen post-fill path and print the two adjacent swaps with fields useful for
root-cause analysis. No scientific state is mutated.
"""
from __future__ import annotations
import math, sqlite3
from pathlib import Path
import v41_core as core

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/"v52_features.db"

def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def decade_hint(ratio):
    if ratio is None or ratio<=0:return '-'
    vals=[1e-6,1e-5,1e-4,1e-3,1e-2,1e-1,10,100,1000,10000,1e5,1e6]
    best=min(vals,key=lambda x:abs(math.log10(ratio)-math.log10(x)))
    err=abs(math.log10(ratio)-math.log10(best))
    return f'~{best:g}x' if err<0.06 else '-'

def fmt(x,n=6):
    if x is None:return '-'
    try:return f'{float(x):.{n}g}'
    except:return str(x)

def main():
    r=core.open_research(); r.row_factory=sqlite3.Row
    rule=r.execute("SELECT * FROM v64_frozen_rule LIMIT 1").fetchone()
    if not rule:
        print('No frozen V6.4 rule.'); return
    evs=r.execute("SELECT * FROM v64_forward_events WHERE rule_id=? AND state='ANOMALY' ORDER BY cutoff_ts,token_mint",(rule['rule_id'],)).fetchall()
    d=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=30); d.row_factory=sqlite3.Row
    print('='*180)
    print('MEMECOIN LAB — V6.4.2 PRICE ANOMALY FORENSICS (READ ONLY)')
    print('='*180)
    print(f"rule={rule['rule_id']} h={rule['horizon_s']} anomalies={len(evs)}")
    print('Largest adjacent price jump per anomalous token; no rows are modified.\n')
    for e in evs:
        token=str(e['token_mint']); fill_ts=sf(e['fill_ts']); fill_price=sf(e['fill_price'])
        if fill_ts is None or fill_price is None or fill_price<=0:
            print(token[:12], 'missing fill fields'); continue
        end=fill_ts+int(rule['horizon_s'])
        rs=[dict(x) for x in d.execute("""SELECT signature,timestamp,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence
          FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp,signature""",(token,fill_ts,end)).fetchall()]
        prev={'signature':'<FILL>','timestamp':fill_ts,'source_program':'FILL','wallet':'-','side':'-','token_amount':None,'quote_sol':None,'price_sol':fill_price,'confidence':'-'}
        best=None
        for cur in rs:
            p0=sf(prev.get('price_sol')); p1=sf(cur.get('price_sol'))
            if p0 and p1 and p0>0:
                step=100*(p1/p0-1); score=abs(step)
                if best is None or score>best[0]:best=(score,step,dict(prev),dict(cur))
            prev=cur
        print('-'*180)
        print(f"TOKEN {token} | path_points={len(rs)} | feature={fmt(e['feature_value'])} | fill={fmt(fill_price)} @ {fill_ts:.0f}")
        if not best:
            print('No adjacent priced pair found.'); continue
        _,step,a,b=best
        ratio=sf(b.get('price_sol'))/sf(a.get('price_sol')) if sf(a.get('price_sol')) and sf(b.get('price_sol')) else None
        print(f"MAX STEP = {step:+.2f}% | ratio={fmt(ratio)} | decade_hint={decade_hint(ratio)} | source_change={a.get('source_program')} -> {b.get('source_program')}")
        for label,x in [('BEFORE',a),('AFTER ',b)]:
            print(f"{label} sig={str(x.get('signature'))[:18]:<18} ts={fmt(x.get('timestamp')):<12} src={str(x.get('source_program')):<10} side={str(x.get('side')):<4} conf={str(x.get('confidence')):<7} price={fmt(x.get('price_sol')):<14} quote={fmt(x.get('quote_sol')):<12} token_amt={fmt(x.get('token_amount')):<14} wallet={str(x.get('wallet'))[:12]}")
        qa,qb=sf(a.get('quote_sol')),sf(b.get('quote_sol')); ta,tb=sf(a.get('token_amount')),sf(b.get('token_amount'))
        if all(v is not None and v>0 for v in (qa,qb,ta,tb)):
            print(f"RATIOS quote_after/before={fmt(qb/qa)} ({decade_hint(qb/qa)}) | token_after/before={fmt(tb/ta)} ({decade_hint(tb/ta)})")
        # local neighborhood around the spike
        sigb=b.get('signature'); idx=next((i for i,x in enumerate(rs) if x['signature']==sigb),None)
        if idx is not None:
            lo=max(0,idx-2); hi=min(len(rs),idx+3)
            print('NEIGHBORHOOD:')
            for x in rs[lo:hi]:
                print(f"  ts={fmt(x['timestamp']):<12} side={x['side']:<4} src={x['source_program']:<10} price={fmt(x['price_sol']):<14} quote={fmt(x['quote_sol']):<12} token_amt={fmt(x['token_amount']):<14} sig={x['signature'][:14]}")
    print('\nInterpretation aids: a decade_hint near 10x/100x/1000x suggests scale/decimal mismatch; source_change suggests pool/migration discontinuity; stable quote with inverse token_amount jump points to amount normalization.')
    d.close(); r.close()

if __name__=='__main__':main()
