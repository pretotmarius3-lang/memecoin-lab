#!/usr/bin/env python3
"""MEMECOIN LAB — TEMPORAL ROBUSTNESS VALIDATION V7.6.5.2

Discovery-only temporal validation for variance-path signals. Uses the clean
V7611 causal cohort, splits samples chronologically, derives candidate thresholds
on the earlier segment only, and evaluates them on later samples. Also reports
rank correlation, sign hit rates, capped-return robustness, and a permutation
p-value for the primary return_slope feature.

This is NOT prospective confirmation evidence and must not be used as such.
"""
from __future__ import annotations
import math, random, sqlite3, statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
DB=ROOT/'v52_features.db'
SEED=7652
PERMS=5000
TRAIN_FRAC=0.60
CAP=100.0


def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def qtile(xs,q):
 ys=sorted(xs)
 if not ys:return None
 p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def rank(xs):
 order=sorted(range(len(xs)),key=lambda i:xs[i]);r=[0.0]*len(xs);i=0
 while i<len(order):
  j=i+1
  while j<len(order) and xs[order[j]]==xs[order[i]]:j+=1
  avg=(i+j-1)/2+1
  for k in range(i,j):r[order[k]]=avg
  i=j
 return r

def corr(a,b):
 if len(a)<3:return None
 ma=statistics.mean(a);mb=statistics.mean(b);sa=sum((x-ma)**2 for x in a);sb=sum((y-mb)**2 for y in b)
 if sa<=0 or sb<=0:return None
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb)

def spear(a,b): return corr(rank(a),rank(b))
def mean(xs): return statistics.mean(xs) if xs else None
def med(xs): return statistics.median(xs) if xs else None

def collect():
 d=db();act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(act[0]) if act else None
 if activation is None: raise SystemExit('V7611 activation unavailable')
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,return_pct FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s''',(activation,)).fetchall();d.close()
 by={}
 for r in rows:
  x=by.setdefault(r['token_mint'],{'first_ts':float(r['first_ts']),'s':{}});x['s'][int(r['stage_s'])]=sf(r['return_pct'])
 out=[]
 for mint,x in by.items():
  s=x['s']
  if any(k not in s or s[k] is None for k in (5,10,20,30)):continue
  fut=None
  for h in (120,60):
   if h in s and s[h] is not None:fut=s[h]-s[30];break
  if fut is None:continue
  early=[s[5],s[10],s[20]]
  path_var=statistics.pvariance(early);origin_var=statistics.pvariance([0.0]+early)
  slope=(s[20]-s[5])/15.0
  curvature=s[30]-2*s[20]+s[10]
  out.append({'mint':mint,'ts':x['first_ts'],'return_slope':slope,'origin_var':origin_var,'path_var':path_var,'curvature':curvature,'future':fut})
 return activation,sorted(out,key=lambda z:(z['ts'],z['mint']))

def eval_rule(train,test,feat,q):
 th=qtile([z[feat] for z in train],q);sel=[z for z in test if z[feat]>=th];base=[z['future'] for z in test];ys=[z['future'] for z in sel]
 cap=lambda x:max(-CAP,min(CAP,x))
 return th,len(sel),mean(ys),med(ys),mean([cap(x) for x in ys]),sum(x>0 for x in ys)/len(ys) if ys else None,mean(base),med(base)

def main():
 activation,rows=collect();n=len(rows);cut=max(1,int(n*TRAIN_FRAC));train=rows[:cut];test=rows[cut:]
 print('='*132);print('MEMECOIN LAB — TEMPORAL ROBUSTNESS VALIDATION V7.6.5.2');print('='*132)
 print(f'activation>{activation:.3f} N={n} train={len(train)} test={len(test)} split={TRAIN_FRAC:.0%}/{1-TRAIN_FRAC:.0%} | DISCOVERY ONLY')
 if len(train)<20 or len(test)<12:
  print('WAITING: need >=20 train and >=12 temporal test samples.');return
 for feat in ('return_slope','origin_var','path_var','curvature'):
  xs=[z[feat] for z in rows];ys=[z['future'] for z in rows]
  print(f'\n[{feat}] pearson={corr(xs,ys):+.3f} spearman={spear(xs,ys):+.3f}')
  for q in (.60,.70,.80):
   th,k,m,md,capm,hit,bm,bmd=eval_rule(train,test,feat,q)
   print(f' trainQ{int(q*100)} th={th:.6g} | TEST n={k:2d}/{len(test)} mean={m if m is not None else 0:+7.2f}% med={md if md is not None else 0:+7.2f}% cappedMean={capm if capm is not None else 0:+7.2f}% hit={100*(hit or 0):5.1f}% | testBase mean/med={bm:+.2f}/{bmd:+.2f}%')
 # permutation test for primary feature using top-30% vs rest on all discovery rows
 feat='return_slope';th=qtile([z[feat] for z in rows],.70);hi=[z['future'] for z in rows if z[feat]>=th];lo=[z['future'] for z in rows if z[feat]<th];obs=mean(hi)-mean(lo)
 vals=[z['future'] for z in rows];flags=[z[feat]>=th for z in rows];rnd=random.Random(SEED);ge=0
 for _ in range(PERMS):
  vv=vals[:];rnd.shuffle(vv);a=[v for v,f in zip(vv,flags) if f];b=[v for v,f in zip(vv,flags) if not f]
  if abs(mean(a)-mean(b))>=abs(obs):ge+=1
 p=(ge+1)/(PERMS+1)
 print(f'\nPRIMARY return_slope Q70 permutation: observed spread={obs:+.3f}% two-sided p≈{p:.4f} ({PERMS} perms)')
 print('\nFREEZE GATE')
 print('Freeze only if a train-derived threshold has positive TEST median, positive capped mean, sensible hit-rate uplift, and the effect is not just one outlier. Otherwise keep accumulating causal samples.')
if __name__=='__main__':main()
