#!/usr/bin/env python3
"""MEMECOIN LAB — VARIANCE PATH DISCOVERY V7.6.5

Read-only discovery companion for the clean V7611 causal cohort.
Measures how price-return variance evolves across causal stages and ranks
variance-path shapes against later causal returns. Discovery only: this file
MUST NOT be treated as prospective confirmation evidence.
"""
from __future__ import annotations
import math, sqlite3, statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
DB=ROOT/'v52_features.db'
STAGES=(5,10,20,30,60,120)
MIN_N=12

def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def corr(a,b):
 if len(a)<3:return None
 ma=statistics.mean(a);mb=statistics.mean(b);sa=sum((x-ma)**2 for x in a);sb=sum((y-mb)**2 for y in b)
 if sa<=0 or sb<=0:return None
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb)

def main():
 d=db()
 try: act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone()
 except sqlite3.OperationalError: act=None
 activation=sf(act[0]) if act else None
 if activation is None: raise SystemExit('V7611 activation unavailable')
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,return_pct,range_pct,flow_velocity,flow_acceleration,buy_ratio_delta
 FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY token_mint,stage_s''',(activation,)).fetchall()
 by={}
 for r in rows: by.setdefault(r['token_mint'],{})[int(r['stage_s'])]=dict(r)
 samples=[]
 for mint,s in by.items():
  if 20 not in s or 30 not in s: continue
  r20=sf(s[20]['return_pct']);r30=sf(s[30]['return_pct'])
  if r20 is None or r30 is None: continue
  early=[sf(s[k]['return_pct']) for k in (5,10,20) if k in s];early=[x for x in early if x is not None]
  if len(early)<2: continue
  var=statistics.pvariance(early);rv=statistics.pvariance([0.0]+early)
  slope=(r20-(sf(s.get(5,{}).get('return_pct')) or 0.0))/15.0
  curvature=r30-2*r20+(sf(s.get(10,{}).get('return_pct')) or 0.0)
  future=None
  for h in (120,60):
   if h in s and sf(s[h]['return_pct']) is not None:
    future=sf(s[h]['return_pct'])-r30;break
  if future is None: continue
  samples.append((mint,var,rv,slope,curvature,future))
 print('='*128);print('MEMECOIN LAB — VARIANCE PATH DISCOVERY V7.6.5');print('='*128)
 print(f'activation>{activation:.3f} samples={len(samples)} | DISCOVERY ONLY — NOT CONFIRMATION EVIDENCE')
 if len(samples)<MIN_N:
  print(f'WAITING: need >= {MIN_N} complete causal paths.');return
 names=('path_var','origin_var','return_slope','curvature')
 for j,name in enumerate(names,1):
  xs=[z[j] for z in samples];ys=[z[5] for z in samples];c=corr(xs,ys)
  order=sorted(range(len(xs)),key=lambda i:xs[i]);q=max(1,len(order)//4);lo=[ys[i] for i in order[:q]];hi=[ys[i] for i in order[-q:]]
  print(f'{name:<16} corr_future={c if c is not None else 0:+.3f} | lowQ future={statistics.mean(lo):+.3f}% highQ future={statistics.mean(hi):+.3f}% spread={statistics.mean(hi)-statistics.mean(lo):+.3f}%')
 print('\nNext gate: freeze only a simple direction/threshold after adequate discovery N, then test it in a brand-new future-only arena.')
 d.close()
if __name__=='__main__':main()
