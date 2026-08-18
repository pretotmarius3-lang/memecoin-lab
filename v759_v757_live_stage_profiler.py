#!/usr/bin/env python3
"""MEMECOIN LAB — V7.5.9 LIVE V757 STAGE PROFILER

Read-only profiler for the live V7.5.7/7.5.7.1 pipeline.
It does not mutate strategy rules or causal evidence.

Measures, on recent swaps/snapshots:
- RAW observed_at -> v52_processed.processed_at
- v52_processed -> v52_swap observed_at/timestamp availability proxy
- due_time(first_ts+stage) -> causal snapshot built_at
- source_max_processed_at -> causal built_at
- current raw backlog

Goal: identify whether decode, backlog, or causal scheduler/materialization dominates.
"""
from __future__ import annotations
import sqlite3,time,statistics
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
V5=ROOT/'v5_raw_events.db'
V52=ROOT/'v52_features.db'
WINDOW=180.0

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
 d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def pct(xs,q):
 if not xs:return None
 ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def fmt(x):return 'NA' if x is None else f'{x:.3f}s'

def stats(name,xs):
 if not xs:
  print(f'{name:<34} n=0');return
 print(f'{name:<34} n={len(xs):5d} p50={fmt(pct(xs,.5))} p90={fmt(pct(xs,.9))} p95={fmt(pct(xs,.95))} max={fmt(max(xs))}')

def main():
 now=time.time();f=ro(V52);r=ro(V5)
 try:
  # current decoder backlog
  f.execute('ATTACH DATABASE ? AS raw',(str(V5),))
  pending=f.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions x LEFT JOIN main.v52_processed p ON p.signature=x.signature WHERE p.signature IS NULL''').fetchone()[0]
  print('='*150)
  print('MEMECOIN LAB — LIVE V757 STAGE PROFILER V7.5.9')
  print('='*150)
  print(f'window={WINDOW:.0f}s raw_pending={pending} READ-ONLY')

  # raw -> processed for rows processed recently
  rows=f.execute('''SELECT p.signature,p.processed_at,x.observed_at
                    FROM v52_processed p JOIN raw.v5_raw_transactions x USING(signature)
                    WHERE p.processed_at>=? AND x.observed_at IS NOT NULL''',(now-WINDOW,)).fetchall()
  raw_to_proc=[max(0,float(z['processed_at'])-float(z['observed_at'])) for z in rows]
  stats('RAW observed -> processed',raw_to_proc)

  # recent swaps: raw store -> processed and chain timestamp -> observed_at
  sw=f.execute('''SELECT s.signature,s.timestamp,s.observed_at,p.processed_at
                  FROM v52_swaps s LEFT JOIN v52_processed p USING(signature)
                  WHERE COALESCE(p.processed_at,s.observed_at)>=?''',(now-WINDOW,)).fetchall()
  proc_after_obs=[max(0,float(z['processed_at'])-float(z['observed_at'])) for z in sw if z['processed_at'] is not None and z['observed_at'] is not None]
  chain_to_raw=[max(0,float(z['observed_at'])-float(z['timestamp'])) for z in sw if z['observed_at'] is not None and z['timestamp'] is not None]
  stats('swap observed -> processed',proc_after_obs)
  stats('chain ts -> raw observed',chain_to_raw)

  # V757 causal rows
  tbl=f.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='v757_causal_snapshots'").fetchone()
  if not tbl:
   print('v757_causal_snapshots: MISSING');return
  cs=f.execute('''SELECT stage_s,cutoff_ts,built_at,build_lag_s,source_max_raw_observed_at,source_max_processed_at
                  FROM v757_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)''',(now-WINDOW,)).fetchall()
  build_lag=[float(z['build_lag_s']) for z in cs]
  proc_to_build=[max(0,float(z['built_at'])-float(z['source_max_processed_at'])) for z in cs if z['source_max_processed_at'] is not None]
  raw_to_build=[max(0,float(z['built_at'])-float(z['source_max_raw_observed_at'])) for z in cs if z['source_max_raw_observed_at'] is not None]
  stats('stage due -> causal built',build_lag)
  stats('source processed -> causal built',proc_to_build)
  stats('source raw observed -> causal built',raw_to_build)

  for st in (20,30):
   xs=[float(z['build_lag_s']) for z in cs if int(z['stage_s'])==st]
   stats(f'T+{st} build lag',xs)

  # Diagnosis heuristic
  rp90=pct(raw_to_proc,.9);bp90=pct(build_lag,.9);pp90=pct(proc_to_build,.9)
  print('\nDIAGNOSIS')
  if pending>200 and rp90 is not None and rp90>5:
   verdict='DECODER_BACKLOG_DOMINANT'
  elif pp90 is not None and pp90>5:
   verdict='CAUSAL_SCHEDULER_OR_SQLITE_DOMINANT'
  elif bp90 is not None and bp90>5:
   verdict='UPSTREAM_AVAILABILITY_OR_STAGE_REFERENCE_DOMINANT'
  else:
   verdict='CURRENT_PATH_FAST'
  print(' VERDICT='+verdict)
  print(f' raw_pending={pending} raw->processed_p90={fmt(rp90)} processed->build_p90={fmt(pp90)} stage_build_p90={fmt(bp90)}')
  print('\nGuardrail: profiler only; do not launch V758 until recent causal lag is within frozen limits.')
 finally:
  try:f.close()
  except:pass
  try:r.close()
  except:pass

if __name__=='__main__':main()
