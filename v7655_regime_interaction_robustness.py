#!/usr/bin/env python3
"""MEMECOIN LAB — REGIME INTERACTION ROBUSTNESS V7.6.5.5

Discovery-only validation of the specific hypothesis discovered in V7654:
when the rolling prior-slope regime is LOW, high current return_slope may have
better subsequent outcomes than the rest. All regime proxies and thresholds are
formed from information available before each test observation.

No strategy freeze. No prospective confirmation claim.
"""
from __future__ import annotations
import math,random,sqlite3,statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"; DB=ROOT/'v52_features.db'
SEED=7655; BOOT=3000

def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def med(xs): return statistics.median(xs) if xs else None

def qv(xs,q):
 s=sorted(xs);p=(len(s)-1)*q;lo=int(p);hi=min(len(s)-1,lo+1);f=p-lo;return s[lo]+(s[hi]-s[lo])*f

def trimmean(xs,p=.1):
 if not xs:return None
 s=sorted(xs);k=int(len(s)*p)
 if 2*k>=len(s):return statistics.mean(s)
 return statistics.mean(s[k:len(s)-k])

def winsmean(xs,cap=100.0): return statistics.mean([max(-cap,min(cap,x)) for x in xs]) if xs else None

def bootstrap_spread(a,b,n=BOOT):
 rng=random.Random(SEED)
 if not a or not b:return (None,None,None)
 vals=[]
 for _ in range(n):
  aa=[a[rng.randrange(len(a))] for __ in range(len(a))];bb=[b[rng.randrange(len(b))] for __ in range(len(b))]
  vals.append(statistics.mean(aa)-statistics.mean(bb))
 vals.sort();return vals[int(.025*(n-1))],vals[int(.975*(n-1))],sum(v>0 for v in vals)/n

def main():
 d=db();act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(act[0]) if act else None
 if activation is None:raise SystemExit('V7611 activation unavailable')
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,return_pct FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s''',(activation,)).fetchall();d.close()
 by={}
 for r in rows:by.setdefault(r['token_mint'],{})[int(r['stage_s'])]=dict(r)
 samples=[]
 for mint,s in by.items():
  if not all(k in s for k in (5,20,30)):continue
  r5,r20,r30=sf(s[5]['return_pct']),sf(s[20]['return_pct']),sf(s[30]['return_pct'])
  if None in (r5,r20,r30):continue
  fut=None
  for h in (120,60):
   if h in s and sf(s[h]['return_pct']) is not None:fut=sf(s[h]['return_pct'])-r30;break
  if fut is None:continue
  samples.append({'t':float(s[30]['first_ts'])+30,'slope':(r20-r5)/15.0,'future':fut})
 samples.sort(key=lambda z:z['t'])
 enriched=[]
 for i,z in enumerate(samples):
  hist=samples[max(0,i-20):i]
  if len(hist)<10:continue
  prior=[h['slope'] for h in hist]
  regime=med(prior)
  regime_cut=med([med([x['slope'] for x in samples[max(0,j-20):j]]) for j in range(10,i) if len(samples[max(0,j-20):j])>=10]) if i>10 else regime
  if regime_cut is None:regime_cut=regime
  slope_th=qv(prior,.70)
  enriched.append({**z,'regime':regime,'regime_cut':regime_cut,'low_regime':regime<regime_cut,'slope_th':slope_th,'high':z['slope']>=slope_th})
 print('='*132);print('MEMECOIN LAB — REGIME INTERACTION ROBUSTNESS V7.6.5.5');print('='*132)
 print(f'activation>{activation:.3f} rawN={len(samples)} eligibleN={len(enriched)} bootstrap={BOOT} | DISCOVERY ONLY')
 low=[z for z in enriched if z['low_regime']]; highreg=[z for z in enriched if not z['low_regime']]
 print(f'causal regime counts: LOW={len(low)} HIGH={len(highreg)}')
 for label,x in [('LOW_REGIME',low),('HIGH_REGIME',highreg)]:
  hi=[z['future'] for z in x if z['high']];rest=[z['future'] for z in x if not z['high']]
  print(f'\n[{label}] n={len(x)} high={len(hi)} rest={len(rest)}')
  if len(hi)<3 or len(rest)<3:
   print(' insufficient');continue
  mean=statistics.mean(hi)-statistics.mean(rest);median=med(hi)-med(rest);trim=trimmean(hi)-trimmean(rest);win=winsmean(hi)-winsmean(rest)
  lo95,hi95,p=bootstrap_spread(hi,rest)
  loo=[]
  for k in range(len(hi)):
   hh=hi[:k]+hi[k+1:]
   if hh:loo.append(statistics.mean(hh)-statistics.mean(rest))
  print(f' meanSpread={mean:+8.2f}% medianSpread={median:+8.2f}% trimmed={trim:+8.2f}% winsor100={win:+8.2f}%')
  print(f' HIGH mean/med={statistics.mean(hi):+8.2f}/{med(hi):+8.2f}% hit={100*sum(v>0 for v in hi)/len(hi):5.1f}% | REST mean/med={statistics.mean(rest):+8.2f}/{med(rest):+8.2f}% hit={100*sum(v>0 for v in rest)/len(rest):5.1f}%')
  print(f' bootstrap meanSpread 95% CI={lo95:+.2f}..{hi95:+.2f}% P(>0)={100*p:.1f}%')
  if loo:print(f' leave-one-out HIGH spread min/med/max={min(loo):+.2f}/{med(loo):+.2f}/{max(loo):+.2f}% positive={100*sum(v>0 for v in loo)/len(loo):.1f}%')
 print('\nCHRONOLOGICAL WALK-FORWARD — fully past-derived regime + Q70 slope')
 # Report consecutive test thirds after a minimum warmup, using only each observation's precomputed past-derived flags.
 e=enriched;start=max(12,len(e)//3);rem=e[start:];chunk=max(1,len(rem)//3)
 for b in range(3):
  x=rem[b*chunk:(b+1)*chunk if b<2 else len(rem)];lx=[z for z in x if z['low_regime']];hi=[z['future'] for z in lx if z['high']];rest=[z['future'] for z in lx if not z['high']]
  if not lx:print(f' step{b+1} test={len(x)} LOW=0');continue
  if hi and rest:print(f' step{b+1} test={len(x):2d} LOW={len(lx):2d} high={len(hi):2d} spreadMean={statistics.mean(hi)-statistics.mean(rest):+8.2f}% spreadMed={med(hi)-med(rest):+8.2f}% highHit={100*sum(v>0 for v in hi)/len(hi):5.1f}%')
  else:print(f' step{b+1} test={len(x):2d} LOW={len(lx):2d} high={len(hi):2d} insufficient split')
 print('\nFREEZE GATE')
 print(' Freeze later only if LOW_REGIME high-vs-rest remains positive across robust stats and chronological slices; otherwise reject this interaction and keep accumulating.')
if __name__=='__main__':main()
