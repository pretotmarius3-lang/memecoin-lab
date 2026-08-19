#!/usr/bin/env python3
"""MEMECOIN LAB — VARIANCE PATH ROBUSTNESS SCANNER V7.6.5.1

Read-only robustness analysis for V765 discovery. Uses the same clean V7611
causal cohort, but stress-tests the apparent effect with medians, trimmed means,
winsorization, hit-rates, leave-one-out stability, bootstrap CIs and simple
quantile thresholds. DISCOVERY ONLY. Never prospective confirmation evidence.
"""
from __future__ import annotations
import math,random,sqlite3,statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
DB=ROOT/'v52_features.db'
BOOT=2000
SEED=7651
MIN_N=30


def db():
 d=sqlite3.connect(f'file:{DB}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def pct(xs,q):
 ys=sorted(xs)
 if not ys:return None
 p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def trimmed_mean(xs,frac=.10):
 ys=sorted(xs);k=int(len(ys)*frac)
 if len(ys)-2*k<=0:return statistics.mean(ys)
 return statistics.mean(ys[k:len(ys)-k])

def winsorized(xs,frac=.10):
 ys=sorted(xs);n=len(ys);k=int(n*frac)
 if not n:return []
 lo=ys[min(k,n-1)];hi=ys[max(0,n-k-1)]
 return [min(hi,max(lo,x)) for x in xs]

def corr(a,b):
 if len(a)<3:return None
 ma=statistics.mean(a);mb=statistics.mean(b);sa=sum((x-ma)**2 for x in a);sb=sum((y-mb)**2 for y in b)
 if sa<=0 or sb<=0:return None
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb)

def feature_rows():
 d=db();act=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(act[0]) if act else None
 if activation is None:raise SystemExit('V7611 activation unavailable')
 rs=d.execute('''SELECT token_mint,stage_s,return_pct FROM v7611_causal_snapshots
  WHERE first_observed_at>? ORDER BY token_mint,stage_s''',(activation,)).fetchall();d.close()
 by={}
 for r in rs:by.setdefault(r['token_mint'],{})[int(r['stage_s'])]=sf(r['return_pct'])
 out=[]
 for mint,s in by.items():
  if any(k not in s or s[k] is None for k in (5,10,20,30)):continue
  future=None;h=None
  for hh in (120,60):
   if hh in s and s[hh] is not None:future=s[hh]-s[30];h=hh;break
  if future is None:continue
  early=[s[5],s[10],s[20]]
  path_var=statistics.pvariance(early)
  origin_var=statistics.pvariance([0.0]+early)
  return_slope=(s[20]-s[5])/15.0
  curvature=s[30]-2*s[20]+s[10]
  out.append({'mint':mint,'path_var':path_var,'origin_var':origin_var,'return_slope':return_slope,'curvature':curvature,'future':future,'h':h})
 return activation,out

def spread(rows,feature,q=.25):
 a=sorted(rows,key=lambda z:z[feature]);k=max(1,int(len(a)*q));lo=a[:k];hi=a[-k:]
 ly=[z['future'] for z in lo];hy=[z['future'] for z in hi]
 return {
  'nq':k,'lo_mean':statistics.mean(ly),'hi_mean':statistics.mean(hy),
  'mean_spread':statistics.mean(hy)-statistics.mean(ly),
  'lo_med':statistics.median(ly),'hi_med':statistics.median(hy),
  'med_spread':statistics.median(hy)-statistics.median(ly),
  'lo_hit':sum(x>0 for x in ly)/len(ly),'hi_hit':sum(x>0 for x in hy)/len(hy),
  'trim_spread':trimmed_mean(hy)-trimmed_mean(ly),
  'win_spread':statistics.mean(winsorized(hy))-statistics.mean(winsorized(ly)),
 }

def bootstrap_spread(rows,feature,b=BOOT):
 rng=random.Random(SEED+sum(ord(c) for c in feature));vals=[];n=len(rows)
 for _ in range(b):
  s=[rows[rng.randrange(n)] for __ in range(n)];vals.append(spread(s,feature)['mean_spread'])
 return pct(vals,.025),statistics.median(vals),pct(vals,.975),sum(v>0 for v in vals)/len(vals)

def loo(rows,feature):
 vals=[]
 for i in range(len(rows)):
  vals.append(spread(rows[:i]+rows[i+1:],feature)['mean_spread'])
 return min(vals),statistics.median(vals),max(vals),sum(v>0 for v in vals)/len(vals)

def thresholds(rows,feature):
 xs=[r[feature] for r in rows];qs=(.50,.60,.70,.75,.80)
 ans=[]
 for q in qs:
  th=pct(xs,q);sel=[r for r in rows if r[feature]>=th];ys=[r['future'] for r in sel]
  if len(ys)<5:continue
  ans.append((q,th,len(ys),statistics.mean(ys),statistics.median(ys),trimmed_mean(ys),sum(y>0 for y in ys)/len(ys)))
 return ans

def main():
 activation,rows=feature_rows();print('='*132);print('MEMECOIN LAB — VARIANCE PATH ROBUSTNESS V7.6.5.1');print('='*132)
 print(f'activation>{activation:.3f} samples={len(rows)} bootstrap={BOOT} | DISCOVERY ONLY — NOT CONFIRMATION EVIDENCE')
 if len(rows)<MIN_N:print(f'WAITING: need >= {MIN_N} complete paths');return
 future=[r['future'] for r in rows]
 print(f'future distribution: mean={statistics.mean(future):+.3f}% median={statistics.median(future):+.3f}% p10/p90={pct(future,.10):+.3f}/{pct(future,.90):+.3f}% max={max(future):+.3f}%')
 print()
 for f in ('return_slope','origin_var','path_var','curvature'):
  xs=[r[f] for r in rows];ys=future;c=corr(xs,ys);s=spread(rows,f);ci=bootstrap_spread(rows,f);l=loo(rows,f)
  print(f'[{f}] corr={c if c is not None else 0:+.3f} quartile_n={s["nq"]}')
  print(f' mean spread={s["mean_spread"]:+.3f}% | median spread={s["med_spread"]:+.3f}% | trimmed={s["trim_spread"]:+.3f}% | winsor={s["win_spread"]:+.3f}%')
  print(f' hit-rate low/high={100*s["lo_hit"]:5.1f}%/{100*s["hi_hit"]:5.1f}%')
  print(f' bootstrap mean-spread 95% CI={ci[0]:+.3f}..{ci[2]:+.3f}% median={ci[1]:+.3f}% P(spread>0)={100*ci[3]:.1f}%')
  print(f' leave-one-out spread min/med/max={l[0]:+.3f}/{l[1]:+.3f}/{l[2]:+.3f}% positive={100*l[3]:.1f}%')
  if f!='curvature':
   for q,th,n,mean,med,trim,hit in thresholds(rows,f):print(f'   Q{int(q*100):02d}+ th={th:.6g} n={n:2d} future mean/med/trim={mean:+.2f}/{med:+.2f}/{trim:+.2f}% hit={100*hit:4.1f}%')
  print()
 print('FREEZE GATE')
 print('  Prefer RETURN_SLOPE only if median/trimmed/LOO/bootstrap all retain positive separation.')
 print('  Do not pick a threshold from prospective data after freezing. If robust, freeze one simple quantile-derived threshold now, then open a brand-new future-only arena.')
if __name__=='__main__':main()
