#!/usr/bin/env python3
"""MEMECOIN LAB — MULTIVARIATE CAUSAL DISCOVERY V7.6.7

Discovery-only scanner over the clean V7611 causal cohort.
Tests a SMALL, predeclared set of interpretable interactions between:
- early price path
- capital flow
- flow dynamics
- wallet structure

No threshold is frozen here. No prospective confirmation claim.
The goal is to identify only interactions that survive a chronological split
and robust summaries before any future-only preregistration.
"""
from __future__ import annotations
import math, sqlite3, statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
DB=ROOT/'v52_features.db'
TRAIN_FRAC=0.60
MIN_TOTAL=60
MIN_TEST_SELECTED=6


def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
 d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def med(xs): return statistics.median(xs) if xs else None

def qv(xs,q):
 s=sorted(xs);p=(len(s)-1)*q;lo=int(p);hi=min(len(s)-1,lo+1);f=p-lo
 return s[lo]+(s[hi]-s[lo])*f

def trimmean(xs,p=.1):
 if not xs:return None
 s=sorted(xs);k=int(len(s)*p)
 if 2*k>=len(s):return statistics.mean(s)
 return statistics.mean(s[k:len(s)-k])

def capmean(xs,cap=100.0):
 return statistics.mean([max(-cap,min(cap,x)) for x in xs]) if xs else None

def corr(a,b):
 if len(a)<3:return None
 ma=statistics.mean(a);mb=statistics.mean(b)
 sa=sum((x-ma)**2 for x in a);sb=sum((y-mb)**2 for y in b)
 if sa<=0 or sb<=0:return None
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb)

def load_samples():
 d=db();act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(act[0]) if act else None
 if activation is None: raise SystemExit('V7611 activation unavailable')
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,return_pct,gross_sol,net_sol,buy_ratio,buy_ratio_delta,
 flow_velocity,flow_acceleration,repeat_wallet_ratio,wallet_hhi,wallet_top1_share,unique_wallets
 FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s''',(activation,)).fetchall();d.close()
 by={}
 for r in rows: by.setdefault(str(r['token_mint']),{})[int(r['stage_s'])]=dict(r)
 out=[]
 for mint,s in by.items():
  if not all(k in s for k in (5,10,20,30)):continue
  r5,r10,r20,r30=[sf(s[k]['return_pct']) for k in (5,10,20,30)]
  if None in (r5,r10,r20,r30):continue
  fut=None
  for h in (120,60):
   if h in s and sf(s[h]['return_pct']) is not None:
    fut=sf(s[h]['return_pct'])-r30;break
  if fut is None:continue
  def val(st,c):return sf(s[st].get(c)) if st in s else None
  gross10,gross20=val(10,'gross_sol'),val(20,'gross_sol')
  net10,net20=val(10,'net_sol'),val(20,'net_sol')
  br10,br20=val(10,'buy_ratio'),val(20,'buy_ratio')
  rep20=val(20,'repeat_wallet_ratio');hhi20=val(20,'wallet_hhi');top20=val(20,'wallet_top1_share');uw20=val(20,'unique_wallets')
  fv20=val(20,'flow_velocity');fa20=val(20,'flow_acceleration');brd20=val(20,'buy_ratio_delta')
  slope=(r20-r5)/15.0
  price_accel=(r20-r10)-(r10-r5)
  gross_growth=(gross20-gross10) if None not in (gross10,gross20) else None
  net_growth=(net20-net10) if None not in (net10,net20) else None
  buy_shift=(br20-br10) if None not in (br10,br20) else brd20
  out.append({'mint':mint,'t':float(s[30]['first_ts'])+30.0,'future':fut,'slope':slope,'price_accel':price_accel,
              'gross_growth':gross_growth,'net_growth':net_growth,'buy_shift':buy_shift,'flow_velocity':fv20,'flow_acceleration':fa20,
              'repeat_wallet_ratio':rep20,'wallet_hhi':hhi20,'wallet_top1_share':top20,'unique_wallets':uw20})
 out.sort(key=lambda z:(z['t'],z['mint']))
 return activation,out

# Six deliberately limited, interpretable hypotheses. Each selector's thresholds are learned on TRAIN only.
HYPOTHESES=[
 ('PRICE+CAPITAL','high slope + positive gross growth', [('slope','hi',.70),('gross_growth','hi',.50)]),
 ('PRICE+NETFLOW','high slope + positive net growth', [('slope','hi',.70),('net_growth','hi',.50)]),
 ('PRICE+BUYSHIFT','high slope + positive buy shift', [('slope','hi',.70),('buy_shift','hi',.50)]),
 ('PRICE+WALLET_BREADTH','high slope + broad wallets', [('slope','hi',.70),('wallet_hhi','lo',.50),('unique_wallets','hi',.50)]),
 ('FLOW+WALLET_QUALITY','positive flow accel + broad wallets', [('flow_acceleration','hi',.60),('wallet_hhi','lo',.50)]),
 ('CAPITAL+LOW_CONCENTRATION','gross growth + low top1 share', [('gross_growth','hi',.60),('wallet_top1_share','lo',.50)]),
]

def thresholds(train,conds):
 th={}
 for f,side,q in conds:
  xs=[z[f] for z in train if z.get(f) is not None]
  if len(xs)<12:return None
  th[f]=(side,qv(xs,q))
 return th

def select(rows,th):
 out=[]
 for z in rows:
  ok=True
  for f,(side,t) in th.items():
   x=z.get(f)
   if x is None:ok=False;break
   if side=='hi' and not x>=t:ok=False;break
   if side=='lo' and not x<=t:ok=False;break
  if ok:out.append(z)
 return out

def summary(xs):
 ys=[z['future'] for z in xs]
 if not ys:return None
 return {'n':len(ys),'mean':statistics.mean(ys),'med':med(ys),'trim':trimmean(ys),'cap':capmean(ys),'hit':sum(y>0 for y in ys)/len(ys)}

def main():
 activation,samples=load_samples();N=len(samples)
 print('='*140);print('MEMECOIN LAB — MULTIVARIATE CAUSAL DISCOVERY V7.6.7');print('='*140)
 print(f'activation>{activation:.3f} N={N} | predeclared_interactions={len(HYPOTHESES)} | DISCOVERY ONLY')
 if N<MIN_TOTAL:
  print(f'WAITING: need >= {MIN_TOTAL} complete causal paths');return
 split=max(1,min(N-1,int(N*TRAIN_FRAC)));train=samples[:split];test=samples[split:]
 base=summary(test)
 print(f'train={len(train)} test={len(test)} | TEST baseline mean/med/cap/hit={base["mean"]:+.2f}/{base["med"]:+.2f}/{base["cap"]:+.2f}/{100*base["hit"]:.1f}%')
 print('\nPREDECLARED INTERACTIONS — thresholds learned on TRAIN only')
 survivors=[]
 for name,desc,conds in HYPOTHESES:
  th=thresholds(train,conds)
  if th is None:
   print(f'[{name}] insufficient feature coverage');continue
  tr=select(train,th);te=select(test,th);s=summary(te)
  if not s:
   print(f'[{name}] TEST selected=0 | {desc}');continue
  dmean=s['mean']-base['mean'];dmed=s['med']-base['med'];dcap=s['cap']-base['cap'];dhit=s['hit']-base['hit']
  tstr=' '.join(f'{f}{">=" if side=="hi" else "<="}{t:.4g}' for f,(side,t) in th.items())
  robust=(s['n']>=MIN_TEST_SELECTED and dmed>0 and dcap>0 and dhit>=0)
  print(f'[{name}] {desc}')
  print(f'  TRAIN selected={len(tr):2d} | TEST selected={s["n"]:2d} | {tstr}')
  print(f'  TEST mean/med/trim/cap/hit={s["mean"]:+8.2f}/{s["med"]:+8.2f}/{s["trim"]:+8.2f}/{s["cap"]:+8.2f}/{100*s["hit"]:5.1f}%')
  print(f'  UPLIFT vs TEST base mean/med/cap/hit={dmean:+8.2f}/{dmed:+8.2f}/{dcap:+8.2f}/{100*dhit:+5.1f}pp | robust={robust}')
  if robust:survivors.append((name,s['n'],dmed,dcap,dhit,th,desc))
 print('\nRANKING / FREEZE GATE')
 if not survivors:
  print(' NO ROBUST SURVIVOR. Do not preregister a multivariate rule yet; keep accumulating or reject this branch.')
 else:
  survivors.sort(key=lambda x:(x[2]+x[3],x[4],x[1]),reverse=True)
  for i,x in enumerate(survivors,1):
   print(f' {i}. {x[0]} test_n={x[1]} med_uplift={x[2]:+.2f}% cap_uplift={x[3]:+.2f}% hit_uplift={100*x[4]:+.1f}pp')
  print(' Candidate(s) survive chronological TEST. Next step is ONE fixed future-only preregistration; no threshold retuning after this output.')
 print('\nGuardrail: discovery only; historical and current outcomes are not capital evidence.')

if __name__=='__main__':main()
