#!/usr/bin/env python3
"""MEMECOIN LAB — RELATIVE / REGIME EDGE SCANNER V7.6.5.4
Discovery only. Tests whether early return_slope is a relative ranking signal once broad market regime is controlled.
No prospective confirmation claims.
"""
from __future__ import annotations
import math,sqlite3,statistics
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"; DB=ROOT/'v52_features.db'

def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def med(xs): return statistics.median(xs) if xs else None

def corr(a,b):
 if len(a)<3:return None
 ma=statistics.mean(a);mb=statistics.mean(b);sa=sum((x-ma)**2 for x in a);sb=sum((y-mb)**2 for y in b)
 if sa<=0 or sb<=0:return None
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb)

def qv(xs,q):
 s=sorted(xs);p=(len(s)-1)*q;lo=int(p);hi=min(len(s)-1,lo+1);f=p-lo;return s[lo]+(s[hi]-s[lo])*f

def main():
 d=db();act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(act[0]) if act else None
 if activation is None: raise SystemExit('V7611 activation unavailable')
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,return_pct FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s''',(activation,)).fetchall();d.close()
 by={}
 for r in rows: by.setdefault(r['token_mint'],{})[int(r['stage_s'])]=dict(r)
 samples=[]
 for mint,s in by.items():
  if not all(k in s for k in (5,20,30)):continue
  r5,r20,r30=sf(s[5]['return_pct']),sf(s[20]['return_pct']),sf(s[30]['return_pct'])
  if None in (r5,r20,r30):continue
  fut=None
  for h in (120,60):
   if h in s and sf(s[h]['return_pct']) is not None: fut=sf(s[h]['return_pct'])-r30;break
  if fut is None:continue
  slope=(r20-r5)/15.0;samples.append({'t':float(s[30]['first_ts'])+30,'slope':slope,'future':fut})
 samples.sort(key=lambda z:z['t']);N=len(samples)
 print('='*132);print('MEMECOIN LAB — RELATIVE / REGIME EDGE V7.6.5.4');print('='*132);print(f'activation>{activation:.3f} N={N} | DISCOVERY ONLY')
 if N<40: print('WAITING: need >=40 complete paths');return
 # rolling observable regime proxy: median slope of preceding 20 completed decisions
 enriched=[]
 for i,z in enumerate(samples):
  hist=samples[max(0,i-20):i]
  if len(hist)<10:continue
  regime=med([h['slope'] for h in hist]);enriched.append({**z,'regime':regime})
 # relative future within chronological blocks to remove broad market drift
 B=4;chunk=max(1,len(enriched)//B);rel=[]
 for b in range(B):
  block=enriched[b*chunk:(b+1)*chunk if b<B-1 else len(enriched)]
  if not block:continue
  base=med([x['future'] for x in block])
  for x in block: rel.append({**x,'rel_future':x['future']-base,'block':b+1})
 print('\nBLOCK-NEUTRAL RELATIVE EDGE — slope ranking')
 for b in range(1,B+1):
  x=[z for z in rel if z['block']==b]
  if len(x)<6:continue
  th=qv([z['slope'] for z in x],.7);hi=[z['rel_future'] for z in x if z['slope']>=th];lo=[z['rel_future'] for z in x if z['slope']<th]
  print(f' block{b} n={len(x):2d} high_n={len(hi):2d} relMeanHi={statistics.mean(hi):+7.2f}% relMedHi={med(hi):+7.2f}% vsRestMean={statistics.mean(lo):+7.2f}%')
 print('\nREGIME INTERACTION — regime proxy uses only preceding slopes')
 regs=[z['regime'] for z in rel];rmed=med(regs)
 for label,pred in [('LOW_REGIME',lambda z:z['regime']<rmed),('HIGH_REGIME',lambda z:z['regime']>=rmed)]:
  x=[z for z in rel if pred(z)]
  if len(x)<8:continue
  th=qv([z['slope'] for z in x],.7);hi=[z for z in x if z['slope']>=th];rest=[z for z in x if z['slope']<th]
  print(f' {label:<11} n={len(x):2d} th={th:+.4f} | HIGH future mean/med={statistics.mean([z["future"] for z in hi]):+7.2f}/{med([z["future"] for z in hi]):+7.2f}% rel={statistics.mean([z["rel_future"] for z in hi]):+7.2f}% | REST rel={statistics.mean([z["rel_future"] for z in rest]):+7.2f}%')
 print('\nGLOBAL')
 print(f' corr(slope, raw_future)={corr([z["slope"] for z in rel],[z["future"] for z in rel]):+.3f}')
 print(f' corr(slope, block_neutral_future)={corr([z["slope"] for z in rel],[z["rel_future"] for z in rel]):+.3f}')
 print('\nINTERPRETATION: if raw edge dies but block-neutral edge survives, slope is cross-sectional ranking alpha, not absolute long alpha. If only one regime survives, freeze a regime-conditional hypothesis later.')
if __name__=='__main__':main()
