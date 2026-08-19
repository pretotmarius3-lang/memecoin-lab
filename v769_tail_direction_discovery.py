#!/usr/bin/env python3
"""MEMECOIN LAB — TAIL DIRECTION DISCOVERY V7.6.9

Discovery-only analysis of the FINAL V7.6.8.5 selected population.
Goal: among tokens already flagged as explosive by the frozen tail filter,
look for causal T+10/T+20/T+30 differences between future winners and crashes.

IMPORTANT:
- Uses V7.6.8.5 future_obs only to define the discovery cohort/labels.
- Features are taken only from causal snapshots available by T+30.
- This is NOT confirmation evidence and MUST NOT directly change capital rules.
- Any candidate must later pass temporal robustness and a brand-new future-only freeze.
"""
from __future__ import annotations

import math
import sqlite3
import statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
VAL=ROOT/'v7685_final_tail_alpha_validation.db'
WINNER=25.0
CRASH=-50.0

FEATURES=(
    'return_pct','range_pct','gross_sol','net_sol','buy_ratio',
    'unique_wallets','repeat_wallet_ratio','wallet_hhi','wallet_top1_share',
    'avg_trade_sol','max_trade_sol','trade_hhi','top1_trade_share',
    'flow_velocity','flow_acceleration','buy_ratio_delta','price_velocity',
    'swaps','buys','sells'
)


def sf(x):
    try:
        z=float(x)
        return z if math.isfinite(z) else None
    except Exception:
        return None


def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def med(xs):
    return statistics.median(xs) if xs else None


def mean(xs):
    return statistics.mean(xs) if xs else None


def cohort():
    d=ro(VAL)
    rows=[dict(r) for r in d.execute('SELECT * FROM future_obs WHERE selected=1 ORDER BY t30,token_mint').fetchall()]
    d.close()
    out=[]
    for r in rows:
        f=sf(r.get('future'))
        if f is None: continue
        label='WIN' if f>=WINNER else ('CRASH' if f<=CRASH else 'MID')
        out.append({'mint':str(r['token_mint']),'future':f,'label':label,'t30':float(r['t30'])})
    return out


def snapshots(mints):
    if not mints:return {}
    d=ro(FEATURE)
    qs=','.join('?' for _ in mints)
    cols=','.join(FEATURES)
    rows=d.execute(f'''SELECT token_mint,stage_s,first_ts,first_observed_at,{cols}
                       FROM v7611_causal_snapshots
                       WHERE token_mint IN ({qs}) AND stage_s IN (10,20,30)
                       ORDER BY first_ts,token_mint,stage_s''',mints).fetchall()
    d.close()
    by={}
    for r in rows:
        by.setdefault(str(r['token_mint']),{})[int(r['stage_s'])]=dict(r)
    return by


def build_features(s):
    z={}
    for st in (10,20,30):
        if st not in s: continue
        for f in FEATURES:
            v=sf(s[st].get(f))
            if v is not None:z[f'T{st}_{f}']=v
    # causal deltas/path features through T+30
    for a,b in ((10,20),(20,30),(10,30)):
        if a not in s or b not in s: continue
        for f in FEATURES:
            x,y=sf(s[a].get(f)),sf(s[b].get(f))
            if x is not None and y is not None:
                z[f'D{a}_{b}_{f}']=y-x
    # acceleration / persistence contrasts where meaningful
    for f in ('return_pct','gross_sol','net_sol','buy_ratio','unique_wallets','wallet_hhi','wallet_top1_share','flow_velocity','price_velocity'):
        if all(st in s and sf(s[st].get(f)) is not None for st in (10,20,30)):
            a,b,c=(sf(s[st].get(f)) for st in (10,20,30))
            z[f'ACC_{f}']=(c-b)-(b-a)
    return z


def sep_score(win,crash):
    if len(win)<2 or len(crash)<2:return None
    mw,mc=med(win),med(crash)
    allx=win+crash
    q25,q75=pct(allx,.25),pct(allx,.75)
    scale=(q75-q25) if q75 is not None and q25 is not None else 0
    if not scale or abs(scale)<1e-12:
        scale=statistics.pstdev(allx) if len(allx)>1 else 0
    if not scale or abs(scale)<1e-12:return 0.0
    return (mw-mc)/scale


def main():
    c=cohort()
    wins=[x for x in c if x['label']=='WIN']; crashes=[x for x in c if x['label']=='CRASH']; mids=[x for x in c if x['label']=='MID']
    print('='*136)
    print('MEMECOIN LAB — TAIL DIRECTION DISCOVERY V7.6.9')
    print('='*136)
    print(f'selected_cohort={len(c)} WIN={len(wins)} CRASH={len(crashes)} MID={len(mids)} | labels fixed: WIN>={WINNER:+.0f}% CRASH<={CRASH:+.0f}%')
    print('DISCOVERY ONLY — features causal through T+30; V7.6.8.5 outcomes are NOT confirmation evidence')
    if len(wins)<3 or len(crashes)<3:
        print('STATUS=INSUFFICIENT_TAIL_CASES_FOR_DIRECTION_DISCOVERY')
        return
    by=snapshots([x['mint'] for x in c])
    rows=[]
    for x in c:
        if x['mint'] not in by:continue
        rows.append({**x,'features':build_features(by[x['mint']])})
    keys=sorted({k for r in rows for k in r['features']})
    ranked=[]
    for k in keys:
        w=[r['features'][k] for r in rows if r['label']=='WIN' and k in r['features']]
        d=[r['features'][k] for r in rows if r['label']=='CRASH' and k in r['features']]
        if len(w)<3 or len(d)<3:continue
        sc=sep_score(w,d)
        if sc is None:continue
        ranked.append((abs(sc),sc,k,len(w),len(d),mean(w),med(w),mean(d),med(d)))
    ranked.sort(reverse=True)
    print('\nTOP CAUSAL SEPARATORS — WIN vs CRASH')
    for _,sc,k,nw,nd,mw,mdw,mc,mdc in ranked[:20]:
        direction='HIGHER_IN_WIN' if sc>0 else 'LOWER_IN_WIN'
        print(f'{k:<42} score={sc:+6.2f} {direction:<14} n={nw}/{nd} | WIN mean/med={mw:+10.4g}/{mdw:+10.4g} | CRASH={mc:+10.4g}/{mdc:+10.4g}')

    # Temporal half split: a discovery stability sanity check, not OOS confirmation.
    tails=[r for r in rows if r['label'] in ('WIN','CRASH')]
    tails.sort(key=lambda r:(r['t30'],r['mint']))
    cut=max(1,len(tails)//2)
    halves=(tails[:cut],tails[cut:])
    print('\nTEMPORAL SIGN STABILITY — top 12 separators')
    for _,sc,k,*_ in ranked[:12]:
        vals=[]
        for h in halves:
            w=[r['features'][k] for r in h if r['label']=='WIN' and k in r['features']]
            d=[r['features'][k] for r in h if r['label']=='CRASH' and k in r['features']]
            vals.append(sep_score(w,d) if len(w)>=2 and len(d)>=2 else None)
        stable=(vals[0] is not None and vals[1] is not None and vals[0]*vals[1]>0)
        print(f'{k:<42} full={sc:+6.2f} half1={str(None if vals[0] is None else round(vals[0],2)):>6} half2={str(None if vals[1] is None else round(vals[1],2)):>6} stable={stable}')

    print('\nNEXT GATE')
    print('Do NOT freeze from this table alone. Prefer a small, interpretable feature that keeps the same sign across temporal halves, then run a dedicated robustness test before any new future-only validator.')

if __name__=='__main__':main()
