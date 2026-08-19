#!/usr/bin/env python3
"""MEMECOIN LAB — REGIME STABILITY MAP V7.6.5.3

Discovery-only diagnostic for whether early variance/slope effects are stable or
flip sign across time. Uses only the clean V7611 causal cohort. No prospective
confirmation claim, no trading, no threshold freeze.
"""
from __future__ import annotations
import math, sqlite3, statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"; DB=ROOT/'v52_features.db'
FEATURES=('return_slope','origin_var','path_var','curvature')

def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def corr(xs,ys):
 if len(xs)<3:return None
 mx,my=statistics.mean(xs),statistics.mean(ys);sx=sum((x-mx)**2 for x in xs);sy=sum((y-my)**2 for y in ys)
 if sx<=0 or sy<=0:return None
 return sum((x-mx)*(y-my) for x,y in zip(xs,ys))/math.sqrt(sx*sy)

def qtile(xs,q):
 ys=sorted(xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo;return ys[lo]+(ys[hi]-ys[lo])*f

def build_samples():
 d=db();act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(act[0]) if act else None
 if activation is None: raise SystemExit('V7611 activation unavailable')
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,return_pct FROM v7611_causal_snapshots
 WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s''',(activation,)).fetchall();d.close()
 by={}
 for r in rows: by.setdefault(r['token_mint'],{})[int(r['stage_s'])]=dict(r)
 out=[]
 for mint,s in by.items():
  if 20 not in s or 30 not in s: continue
  r5=sf(s.get(5,{}).get('return_pct'));r10=sf(s.get(10,{}).get('return_pct'));r20=sf(s[20]['return_pct']);r30=sf(s[30]['return_pct'])
  if r20 is None or r30 is None: continue
  early=[x for x in (r5,r10,r20) if x is not None]
  if len(early)<2: continue
  future=None;fh=None
  for h in (120,60):
   if h in s and sf(s[h]['return_pct']) is not None: future=sf(s[h]['return_pct'])-r30;fh=h;break
  if future is None: continue
  slope=(r20-(r5 or 0.0))/15.0
  origin_var=statistics.pvariance([0.0]+early);path_var=statistics.pvariance(early)
  curvature=r30-2*r20+(r10 or 0.0)
  out.append({'mint':mint,'ts':float(s[20]['first_ts']),'return_slope':slope,'origin_var':origin_var,'path_var':path_var,'curvature':curvature,'future':future,'fh':fh})
 return activation,sorted(out,key=lambda z:(z['ts'],z['mint']))

def block_stats(rows,feature):
 xs=[r[feature] for r in rows];ys=[r['future'] for r in rows];c=corr(xs,ys)
 med=qtile(xs,.5);lo=[r['future'] for r in rows if r[feature]<=med];hi=[r['future'] for r in rows if r[feature]>med]
 return c,statistics.mean(hi)-statistics.mean(lo),statistics.median(hi)-statistics.median(lo)

def main():
 activation,s=build_samples();n=len(s)
 print('='*132);print('MEMECOIN LAB — REGIME STABILITY MAP V7.6.5.3');print('='*132)
 print(f'activation>{activation:.3f} N={n} | DISCOVERY ONLY — SIGN-STABILITY DIAGNOSTIC')
 if n<40: print('WAITING: need >=40 complete paths.');return
 # Equal chronological blocks, at least 10 rows each.
 k=4 if n>=56 else 3;cuts=[round(i*n/k) for i in range(k+1)]
 for f in FEATURES:
  print(f'\n[{f}]')
  signs=[]
  for i in range(k):
   b=s[cuts[i]:cuts[i+1]]
   c,ms,md=block_stats(b,f);signs.append(0 if c is None else (1 if c>0 else -1))
   print(f' block{i+1} n={len(b):2d} corr={0 if c is None else c:+.3f} meanSpreadHi-Lo={ms:+8.2f}% medSpread={md:+8.2f}% futureMed={statistics.median(r["future"] for r in b):+7.2f}%')
  print(f' sign_consistency={max(signs.count(1),signs.count(-1))}/{len(signs)} dominant={"POS" if signs.count(1)>signs.count(-1) else "NEG" if signs.count(-1)>signs.count(1) else "MIXED"}')

 print('\nEXPANDING WALK-FORWARD — RETURN_SLOPE')
 # At each cut, derive Q30/Q70 from all prior rows and score only next block.
 for i in range(1,k):
  train=s[:cuts[i]];test=s[cuts[i]:cuts[i+1]];xs=[r['return_slope'] for r in train];q30=qtile(xs,.30);q70=qtile(xs,.70)
  low=[r['future'] for r in test if r['return_slope']<=q30];high=[r['future'] for r in test if r['return_slope']>=q70]
  def fmt(a):
   return 'n=0' if not a else f'n={len(a)} mean={statistics.mean(a):+.2f}% med={statistics.median(a):+.2f}% hit={100*sum(x>0 for x in a)/len(a):.1f}%'
  print(f' step{i} train={len(train):2d} test={len(test):2d} q30={q30:.6g} q70={q70:.6g} | LOW {fmt(low)} | HIGH {fmt(high)}')

 print('\nINTERPRETATION GATE')
 print(' - Stable same-sign blocks => candidate structural effect worth freezing later.')
 print(' - Repeated sign flips => regime-dependent/nonstationary effect; do NOT freeze a global threshold.')
 print(' - If LOW consistently beats HIGH out-of-time, investigate reversal rather than continuation in a separate discovery branch.')

if __name__=='__main__': main()
