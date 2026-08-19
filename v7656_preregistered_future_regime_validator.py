#!/usr/bin/env python3
"""MEMECOIN LAB — PREREGISTERED FUTURE REGIME VALIDATOR V7.6.5.6

Future-only validator for the fixed hypothesis:
LOW rolling prior-slope regime AND current return_slope >= rolling Q70.
Historical rows are rolling context only, never validation outcomes.
"""
from __future__ import annotations
import math, sqlite3, statistics, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
OUT=ROOT/'v7656_future_regime_validation.db'
MIN_HISTORY=20; ROLL=20; Q=0.70; TARGET_LOW=20; TARGET_HIGH=10

def ro():
 d=sqlite3.connect(f'file:{FEATURE}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
 d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def odb():
 d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
 d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=FULL');d.execute('PRAGMA busy_timeout=30000');return d

def sf(x):
 try:
  z=float(x);return z if math.isfinite(z) else None
 except:return None

def med(xs): return statistics.median(xs) if xs else None

def qv(xs,q):
 s=sorted(xs);p=(len(s)-1)*q;lo=int(p);hi=min(len(s)-1,lo+1);f=p-lo
 return s[lo]+(s[hi]-s[lo])*f

def init():
 d=odb();d.executescript('''
 CREATE TABLE IF NOT EXISTS run(
  id INTEGER PRIMARY KEY CHECK(id=1),created_at REAL,cutoff_t30 REAL,activation REAL,
  hypothesis TEXT,roll_n INTEGER,q REAL,target_low INTEGER,target_high INTEGER);
 CREATE TABLE IF NOT EXISTS future_obs(
  token_mint TEXT PRIMARY KEY,t30 REAL,slope REAL,regime REAL,regime_cut REAL,slope_th REAL,
  low_regime INTEGER,high_slope INTEGER,future REAL,first_recorded_at REAL);
 ''')
 r=d.execute('SELECT * FROM run WHERE id=1').fetchone()
 if not r:
  x=ro();a=x.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(a[0]) if a else None
  z=x.execute('SELECT MAX(first_ts+30) FROM v7611_causal_snapshots WHERE stage_s=30 AND first_observed_at>?',(activation or 0,)).fetchone();x.close()
  cut=sf(z[0]) if z and z[0] is not None else time.time()
  d.execute('INSERT INTO run VALUES(?,?,?,?,?,?,?,?,?)',(1,time.time(),cut,activation,'LOW rolling prior-slope regime AND current slope >= rolling Q70',ROLL,Q,TARGET_LOW,TARGET_HIGH));d.commit()
 d.close()

def samples():
 x=ro();a=x.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();activation=sf(a[0]) if a else None
 rows=x.execute('SELECT token_mint,stage_s,first_ts,return_pct FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s',(activation or 0,)).fetchall();x.close()
 by={}
 for r in rows:by.setdefault(str(r['token_mint']),{})[int(r['stage_s'])]=dict(r)
 out=[]
 for mint,s in by.items():
  if not all(k in s for k in (5,20,30)):continue
  r5,r20,r30=sf(s[5]['return_pct']),sf(s[20]['return_pct']),sf(s[30]['return_pct'])
  if None in (r5,r20,r30):continue
  fut=None
  for h in (120,60):
   if h in s and sf(s[h]['return_pct']) is not None:
    fut=sf(s[h]['return_pct'])-r30;break
  out.append({'mint':mint,'t30':float(s[30]['first_ts'])+30.0,'slope':(r20-r5)/15.0,'future':fut})
 out.sort(key=lambda z:(z['t30'],z['mint']));return out

def enrich_all(ss):
 enriched=[]
 for i,z in enumerate(ss):
  hist=ss[max(0,i-ROLL):i]
  if len(hist)<MIN_HISTORY:continue
  prior=[h['slope'] for h in hist];regime=med(prior);slope_th=qv(prior,Q)
  prior_regs=[]
  for j in range(MIN_HISTORY,i):
   hh=ss[max(0,j-ROLL):j]
   if len(hh)>=MIN_HISTORY:prior_regs.append(med([u['slope'] for u in hh]))
  regime_cut=med(prior_regs) if prior_regs else regime
  enriched.append({**z,'regime':regime,'regime_cut':regime_cut,'low':int(regime<regime_cut),'high':int(z['slope']>=slope_th),'slope_th':slope_th})
 return enriched

def ingest():
 d=odb();run=d.execute('SELECT * FROM run WHERE id=1').fetchone();cut=float(run['cutoff_t30']);known={r[0] for r in d.execute('SELECT token_mint FROM future_obs').fetchall()}
 made=0
 for z in enrich_all(samples()):
  if z['t30']<=cut or z['future'] is None or z['mint'] in known:continue
  d.execute('INSERT OR IGNORE INTO future_obs VALUES(?,?,?,?,?,?,?,?,?,?)',(z['mint'],z['t30'],z['slope'],z['regime'],z['regime_cut'],z['slope_th'],z['low'],z['high'],z['future'],time.time()))
  made+=d.execute('SELECT changes()').fetchone()[0]
 d.commit();d.close();return made

def stats(xs):
 if not xs:return (None,None,None,None)
 cap=[max(-100,min(100,x)) for x in xs]
 return statistics.mean(xs),med(xs),statistics.mean(cap),sum(x>0 for x in xs)/len(xs)

def display():
 d=odb();r=d.execute('SELECT * FROM run WHERE id=1').fetchone();rows=[dict(z) for z in d.execute('SELECT * FROM future_obs ORDER BY t30,token_mint').fetchall()];d.close()
 # IMPORTANT: DB columns are low_regime/high_slope. The old display used low/high and crashed once rows existed.
 low=[z for z in rows if int(z['low_regime'])==1]
 lh=[z['future'] for z in low if int(z['high_slope'])==1]
 lr=[z['future'] for z in low if int(z['high_slope'])==0]
 highreg=[z for z in rows if int(z['low_regime'])==0]
 print('='*132);print('MEMECOIN LAB — PREREGISTERED FUTURE REGIME VALIDATOR V7.6.5.6');print('='*132)
 print(f"cutoff_t30>{float(r['cutoff_t30']):.3f} total_future={len(rows)} LOW={len(low)} HIGH_REGIME={len(highreg)} | rule FIXED: LOW regime + rolling Q70 slope")
 print('Historical discovery outcomes are not included. Future observations only. No capital decision.')
 print(f'\nLOW_REGIME validation: high={len(lh)} rest={len(lr)} target each >= {int(r["target_high"])} / total LOW >= {int(r["target_low"])}')
 if lh and lr:
  a=stats(lh);b=stats(lr)
  print(f' HIGH mean/med/capped/hit={a[0]:+8.2f}/{a[1]:+8.2f}/{a[2]:+8.2f}/{100*a[3]:5.1f}%')
  print(f' REST mean/med/capped/hit={b[0]:+8.2f}/{b[1]:+8.2f}/{b[2]:+8.2f}/{100*b[3]:5.1f}%')
  print(f' SPREAD mean/med/capped={(a[0]-b[0]):+8.2f}/{(a[1]-b[1]):+8.2f}/{(a[2]-b[2]):+8.2f}%')
 enough=len(low)>=int(r['target_low']) and len(lh)>=int(r['target_high']) and len(lr)>=int(r['target_high'])
 if not enough:print('\nSTATUS=ACCUMULATING_FUTURE_EVIDENCE')
 else:
  a=stats(lh);b=stats(lr);passed=(a[1]>b[1] and a[2]>b[2] and a[3]>=b[3])
  print(f'\nSTATUS={"FUTURE_SIGNAL_SURVIVES" if passed else "FUTURE_SIGNAL_FAILS"}')
  print('Gate: median + capped mean + hit-rate must not deteriorate versus rest.')

def main():
 init();made=ingest();print(f'new_future_rows={made}');display()
if __name__=='__main__':main()
