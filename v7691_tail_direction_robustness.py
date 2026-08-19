#!/usr/bin/env python3
"""MEMECOIN LAB — TAIL DIRECTION ROBUSTNESS V7.6.9.1

Robustness-only follow-up to V7.6.9 discovery.
Uses the SAME V7.6.8.5 selected cohort, therefore this remains DISCOVERY evidence.
No threshold from this script is confirmation evidence.

Tests only a small predeclared set of interpretable V7.6.9 stable separators:
  T30_trade_hhi                 expected HIGHER in WIN
  D10_30_sells                  expected LOWER in WIN
  T30_top1_trade_share          expected HIGHER in WIN
  T20_trade_hhi                 expected HIGHER in WIN
  T10_price_velocity            expected LOWER in WIN
  D10_20_swaps                  expected LOWER in WIN
  D20_30_top1_trade_share       expected HIGHER in WIN
  T10_buy_ratio_delta           expected LOWER in WIN
  D20_30_trade_hhi              expected HIGHER in WIN

Checks:
- oriented rank AUC (WIN vs CRASH)
- exact-label permutation p-value (Monte Carlo)
- bootstrap AUC interval / P(AUC>0.5)
- leave-one-tail-out AUC stability
- chronological 3-block sign/AUC stability where sample sizes permit

Research only. No capital decision.
"""
from __future__ import annotations

import math, random, sqlite3, statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
VAL=ROOT/'v7685_final_tail_alpha_validation.db'
WINNER=25.0
CRASH=-50.0
BOOT=4000
PERMS=8000
SEED=7691

CANDIDATES=(
    ('T30_trade_hhi', +1),
    ('D10_30_sells', -1),
    ('T30_top1_trade_share', +1),
    ('T20_trade_hhi', +1),
    ('T10_price_velocity', -1),
    ('D10_20_swaps', -1),
    ('D20_30_top1_trade_share', +1),
    ('T10_buy_ratio_delta', -1),
    ('D20_30_trade_hhi', +1),
)

BASE_FEATURES=('trade_hhi','top1_trade_share','sells','swaps','price_velocity','buy_ratio_delta')


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


def cohort():
    d=ro(VAL)
    rows=[dict(r) for r in d.execute('SELECT token_mint,t30,future FROM future_obs WHERE selected=1 ORDER BY t30,token_mint')]
    d.close()
    out=[]
    for r in rows:
        f=sf(r['future'])
        if f is None: continue
        if f>=WINNER: lab='WIN'
        elif f<=CRASH: lab='CRASH'
        else: continue
        out.append({'mint':str(r['token_mint']),'t30':float(r['t30']),'label':lab,'future':f})
    return out


def snapshots(mints):
    if not mints:return {}
    d=ro(FEATURE)
    q=','.join('?' for _ in mints)
    cols=','.join(BASE_FEATURES)
    rows=d.execute(f'''SELECT token_mint,stage_s,{cols} FROM v7611_causal_snapshots
                       WHERE token_mint IN ({q}) AND stage_s IN (10,20,30)
                       ORDER BY token_mint,stage_s''',mints).fetchall()
    d.close()
    by={}
    for r in rows:by.setdefault(str(r['token_mint']),{})[int(r['stage_s'])]=dict(r)
    return by


def features(s):
    z={}
    for st in (10,20,30):
        if st not in s:continue
        for f in BASE_FEATURES:
            v=sf(s[st].get(f))
            if v is not None:z[f'T{st}_{f}']=v
    for a,b in ((10,20),(20,30),(10,30)):
        if a not in s or b not in s:continue
        for f in BASE_FEATURES:
            x,y=sf(s[a].get(f)),sf(s[b].get(f))
            if x is not None and y is not None:z[f'D{a}_{b}_{f}']=y-x
    return z


def auc(w,c,orient=1):
    if not w or not c:return None
    wins=ties=0
    for a in w:
        for b in c:
            da=orient*a; db=orient*b
            if da>db:wins+=1
            elif da==db:ties+=1
    return (wins+0.5*ties)/(len(w)*len(c))


def quantile(xs,q):
    if not xs:return None
    ys=sorted(xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def perm_p(vals,labels,orient,obs,rng):
    nwin=sum(1 for x in labels if x=='WIN')
    ge=0
    for _ in range(PERMS):
        idx=list(range(len(vals)));rng.shuffle(idx);wi=set(idx[:nwin])
        w=[vals[i] for i in range(len(vals)) if i in wi]
        c=[vals[i] for i in range(len(vals)) if i not in wi]
        a=auc(w,c,orient)
        if a is not None and a>=obs-1e-12:ge+=1
    return (ge+1)/(PERMS+1)


def bootstrap(w,c,orient,rng):
    xs=[]
    for _ in range(BOOT):
        wb=[w[rng.randrange(len(w))] for _ in range(len(w))]
        cb=[c[rng.randrange(len(c))] for _ in range(len(c))]
        xs.append(auc(wb,cb,orient))
    return quantile(xs,.025),statistics.median(xs),quantile(xs,.975),sum(x>.5 for x in xs)/len(xs)


def loo(w,c,orient):
    xs=[]
    for i in range(len(w)):
        a=auc(w[:i]+w[i+1:],c,orient)
        if a is not None:xs.append(a)
    for i in range(len(c)):
        a=auc(w,c[:i]+c[i+1:],orient)
        if a is not None:xs.append(a)
    return (min(xs),statistics.median(xs),max(xs),sum(x>.5 for x in xs)/len(xs)) if xs else (None,None,None,None)


def main():
    rng=random.Random(SEED)
    tails=cohort(); by=snapshots([x['mint'] for x in tails])
    rows=[]
    for x in tails:
        if x['mint'] in by:rows.append({**x,'f':features(by[x['mint']])})
    nw=sum(r['label']=='WIN' for r in rows); nc=sum(r['label']=='CRASH' for r in rows)
    print('='*144)
    print('MEMECOIN LAB — TAIL DIRECTION ROBUSTNESS V7.6.9.1')
    print('='*144)
    print(f'tail_cases={len(rows)} WIN={nw} CRASH={nc} bootstrap={BOOT} perms={PERMS} | SAME discovery cohort — NOT confirmation evidence')
    print('Candidate directions fixed from V7.6.9 before this robustness pass. No threshold optimization.\n')
    survivors=[]
    rows.sort(key=lambda r:(r['t30'],r['mint']))
    for k,orient in CANDIDATES:
        usable=[r for r in rows if k in r['f']]
        w=[r['f'][k] for r in usable if r['label']=='WIN']; c=[r['f'][k] for r in usable if r['label']=='CRASH']
        if len(w)<3 or len(c)<3:
            print(f'[{k}] insufficient n={len(w)}/{len(c)}')
            continue
        a=auc(w,c,orient); vals=[r['f'][k] for r in usable]; labs=[r['label'] for r in usable]
        p=perm_p(vals,labs,orient,a,rng); lo,md,hi,pgt=bootstrap(w,c,orient,rng); lmin,lmed,lmax,lpos=loo(w,c,orient)
        # chronological 3 blocks, discovery stability only
        blocks=[]
        for bi in range(3):
            st=round(len(usable)*bi/3); en=round(len(usable)*(bi+1)/3); b=usable[st:en]
            bw=[r['f'][k] for r in b if r['label']=='WIN']; bc=[r['f'][k] for r in b if r['label']=='CRASH']
            blocks.append(auc(bw,bc,orient) if len(bw)>=1 and len(bc)>=1 else None)
        valid=[x for x in blocks if x is not None]
        block_pos=sum(x>.5 for x in valid)
        robust=(a>=.75 and lo>.5 and lmin>.5 and p<=.10 and len(valid)>=2 and block_pos==len(valid))
        if robust:survivors.append((a,k,orient))
        arrow='HIGHER_WIN' if orient>0 else 'LOWER_WIN'
        print(f'[{k}] {arrow} n={len(w)}/{len(c)} AUC={a:.3f} perm_p={p:.4f}')
        print(f'  bootstrap95={lo:.3f}..{hi:.3f} med={md:.3f} P(AUC>.5)={100*pgt:.1f}%')
        print(f'  LOO min/med/max={lmin:.3f}/{lmed:.3f}/{lmax:.3f} positive={100*lpos:.1f}% | chrono3={blocks} robust={robust}')
    print('\nROBUSTNESS GATE')
    if survivors:
        survivors.sort(reverse=True)
        print('SURVIVORS (discovery robustness only):')
        for a,k,o in survivors:print(f'  {k} direction={"HIGH" if o>0 else "LOW"} AUC={a:.3f}')
        print('Next: choose at most ONE simplest survivor, freeze one past-derived threshold, then open a brand-new future-only validator.')
    else:
        print('NO ROBUST SURVIVOR. Do not freeze a direction rule. Accumulate more tail cases or reject this branch.')

if __name__=='__main__':main()
