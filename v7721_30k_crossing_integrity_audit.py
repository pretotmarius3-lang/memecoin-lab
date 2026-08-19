#!/usr/bin/env python3
"""MEMECOIN LAB — 30K CROSSING INTEGRITY AUDIT V7.7.2.1

READ-ONLY diagnostic before any migration-alpha discovery.
The first-crossing proxy in V7.7.1/V7.7.2 produced very large overshoots and
large immediate reversals. This audit tests whether candidate 30k crossings are
single-trade price spikes or persistent local regime transitions.

No strategy evidence. No alpha threshold selection.
"""
from __future__ import annotations
import argparse, math, os, sqlite3, statistics
from collections import defaultdict
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/"v52_features.db"
DEFAULT_SUPPLY=float(os.environ.get("MEMECOIN_V7721_TOKEN_SUPPLY","1000000000"))
DEFAULT_MC=float(os.environ.get("MEMECOIN_V7721_MC_USD","30000"))


def sf(x):
    try:
        z=float(x); return z if math.isfinite(z) else None
    except Exception:return None

def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f

def ro():
    d=sqlite3.connect(f"file:{FEATURE}?mode=ro",uri=True,timeout=30)
    d.row_factory=sqlite3.Row; d.execute("PRAGMA query_only=ON"); d.execute("PRAGMA busy_timeout=30000")
    return d

def f(x): return "None" if x is None else f"{x:.2f}"


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--sol-usd',type=float,required=True)
    ap.add_argument('--supply',type=float,default=DEFAULT_SUPPLY)
    ap.add_argument('--mc',type=float,default=DEFAULT_MC)
    ap.add_argument('--window',type=float,default=15.0)
    ap.add_argument('--confirm-trades',type=int,default=3)
    a=ap.parse_args()
    th=a.mc/(a.supply*a.sol_usd)
    print('='*140)
    print('MEMECOIN LAB — 30K CROSSING INTEGRITY AUDIT V7.7.2.1')
    print('='*140)
    print(f'READ-ONLY | MC={a.mc:.0f} supply={a.supply:.0f} SOL_USD={a.sol_usd:.2f} threshold={th:.12g}')
    print(f'confirmation diagnostic: next {a.confirm_trades} priced trades and {a.window:g}s local window; NO alpha tuning')

    d=ro()
    rows=d.execute('''SELECT token_mint,timestamp,observed_at,price_sol,signature
                      FROM v52_swaps WHERE price_sol IS NOT NULL AND price_sol>0
                      ORDER BY token_mint,timestamp,signature''').fetchall(); d.close()
    by=defaultdict(list)
    for r in rows:by[str(r['token_mint'])].append(r)

    cand=[]
    for mint,rr in by.items():
        seen=False
        for i,r in enumerate(rr):
            p=float(r['price_sol'])
            if p<th:
                seen=True; continue
            if seen:
                cand.append((mint,i,rr)); break
            # left-censored: do not use
            break

    overs=[]; next1=[]; med3=[]; winmed=[]; persist3=[]; persistwin=[]; gap_next=[]
    confirmed=[]
    for mint,i,rr in cand:
        cr=rr[i]; cp=float(cr['price_sol']); ts=float(cr['timestamp'])
        overs.append((cp/th-1)*100)
        nxt=rr[i+1:i+1+a.confirm_trades]
        if nxt:
            next1.append((float(nxt[0]['price_sol'])/cp-1)*100)
            gap_next.append(float(nxt[0]['timestamp'])-ts)
        if len(nxt)>=a.confirm_trades:
            ps=[float(x['price_sol']) for x in nxt[:a.confirm_trades]]
            m=statistics.median(ps); med3.append((m/cp-1)*100)
            ok3=all(p>=th for p in ps)
            persist3.append(int(ok3))
        else: ok3=False
        local=[float(x['price_sol']) for x in rr[i+1:] if 0 < float(x['timestamp'])-ts <= a.window]
        if local:
            m=statistics.median(local); winmed.append((m/cp-1)*100)
            okw=m>=th; persistwin.append(int(okw))
        else: okw=False
        if ok3 and okw: confirmed.append(mint)

    print(f'\nSTRICT FIRST-CROSSING CANDIDATES n={len(cand)}')
    print(f'crossing overshoot %% p50/p90/p95={f(pct(overs,.5))}/{f(pct(overs,.9))}/{f(pct(overs,.95))}')
    print(f'next trade vs crossing %% p50/p90={f(pct(next1,.5))}/{f(pct(next1,.9))} | next gap_s p50/p90={f(pct(gap_next,.5))}/{f(pct(gap_next,.9))}')
    print(f'next {a.confirm_trades}-trade median vs crossing %% p50/p90={f(pct(med3,.5))}/{f(pct(med3,.9))}')
    print(f'<= {a.window:g}s local median vs crossing %% p50/p90={f(pct(winmed,.5))}/{f(pct(winmed,.9))}')
    print('\nPERSISTENCE DIAGNOSTICS')
    print(f'next_{a.confirm_trades}_all_above_30k={sum(persist3)}/{len(persist3)} ({100*sum(persist3)/max(1,len(persist3)):.1f}%)')
    print(f'local_window_median_above_30k={sum(persistwin)}/{len(persistwin)} ({100*sum(persistwin)/max(1,len(persistwin)):.1f}%)')
    print(f'both_persistence_checks={len(confirmed)}/{len(cand)} ({100*len(confirmed)/max(1,len(cand)):.1f}%)')

    # Overshoot strata are diagnostic only: if huge overshoot strata immediately mean-revert,
    # first crossing is likely contaminated by isolated price estimates.
    strata=[(0,25),(25,100),(100,1000),(1000,float('inf'))]
    print('\nOVERSHOOT STRATA — diagnostic only')
    vals=[]
    for mint,i,rr in cand:
        cp=float(rr[i]['price_sol']); ov=(cp/th-1)*100
        nxt=rr[i+1] if i+1<len(rr) else None
        rev=(float(nxt['price_sol'])/cp-1)*100 if nxt else None
        vals.append((ov,rev))
    for lo,hi in strata:
        xs=[r for o,r in vals if o>=lo and o<hi and r is not None]
        print(f'overshoot {lo:g}..{"inf" if math.isinf(hi) else f"{hi:g}"}% n={len(xs):4d} next_trade_return med={f(statistics.median(xs) if xs else None)}%')

    print('\nINTEGRITY GATE')
    rate=len(confirmed)/max(1,len(cand))
    if rate>=.50 and pct(overs,.95) is not None and pct(overs,.95)<500:
        print('STATUS=FIRST_CROSSING_PROXY_REASONABLY_PERSISTENT')
        print('Proceed to PRE-state alpha discovery, retaining persistence flags as sensitivity checks.')
    elif len(confirmed)>=100:
        print('STATUS=FIRST_CROSSING_NOISY_BUT_PERSISTENT_SUBCOHORT_EXISTS')
        print('Do NOT use raw first crossings as migration truth. Reconstruct V7.7.3 on the persistence-confirmed subcohort and compare sensitivity.')
    else:
        print('STATUS=FIRST_CROSSING_PROXY_NOT_TRUSTWORTHY')
        print('Do not run migration alpha discovery yet; identify a better migration-state marker / curve-completion signal.')
    print('\nGuardrail: persistence criteria above are infrastructure diagnostics, not trading thresholds.')

if __name__=='__main__':main()
