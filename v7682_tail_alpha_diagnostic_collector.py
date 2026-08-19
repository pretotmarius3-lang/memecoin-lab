#!/usr/bin/env python3
"""MEMECOIN LAB — TAIL-ALPHA DIAGNOSTIC COLLECTOR V7.6.8.2

Continuous companion for V768 with source-side diagnostics.
It does not change the preregistered cutoff or rule. It only calls V768.ingest()
and reports why future rows are or are not yet eligible.
"""
from __future__ import annotations
import os, signal, sqlite3, time
from pathlib import Path
import v768_preregistered_tail_alpha_validator as v

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
POLL=float(os.environ.get('MEMECOIN_V7682_POLL_S','5'))
REPORT=float(os.environ.get('MEMECOIN_V7682_REPORT_S','20'))
STOP=False

def stop(*_):
 global STOP; STOP=True

def ro():
 d=sqlite3.connect(f'file:{FEATURE}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
 d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def diag():
 od=v.odb();run=od.execute('SELECT * FROM run WHERE id=1').fetchone();cut=float(run['cutoff_t30']);stored=od.execute('SELECT COUNT(*) FROM future_obs').fetchone()[0];od.close()
 d=ro()
 a=d.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone();act=float(a[0]) if a and a[0] is not None else 0.0
 rows=d.execute('''SELECT token_mint,stage_s,first_ts,gross_sol,wallet_top1_share,return_pct
 FROM v7611_causal_snapshots WHERE first_observed_at>? ORDER BY first_ts,token_mint,stage_s''',(act,)).fetchall();d.close()
 by={}
 for r in rows:by.setdefault(str(r['token_mint']),{})[int(r['stage_s'])]=dict(r)
 post30=mature60=mature120=complete=0
 missing10=missing30=missing_future=missing_metrics=0
 newest_t30=None
 for mint,s in by.items():
  if 30 not in s: continue
  t30=float(s[30]['first_ts'])+30.0
  newest_t30=t30 if newest_t30 is None else max(newest_t30,t30)
  if t30<=cut: continue
  post30+=1
  if 60 in s: mature60+=1
  if 120 in s: mature120+=1
  if 10 not in s:
   missing10+=1; continue
  if 30 not in s:
   missing30+=1; continue
  g10=s[10]['gross_sol'];g30=s[30]['gross_sol'];top1=s[30]['wallet_top1_share'];r30=s[30]['return_pct']
  if None in (g10,g30,top1,r30):
   missing_metrics+=1; continue
  fut=False
  for h in (120,60):
   if h in s and s[h]['return_pct'] is not None:
    fut=True; break
  if not fut:
   missing_future+=1; continue
  complete+=1
 return {'cut':cut,'stored':stored,'post30':post30,'m60':mature60,'m120':mature120,'complete':complete,
         'missing10':missing10,'missing30':missing30,'missing_future':missing_future,'missing_metrics':missing_metrics,
         'newest_t30':newest_t30}

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
 v.init();last=0.0;total_new=0
 print(f'MEMECOIN LAB V7.6.8.2 TAIL-ALPHA DIAGNOSTIC COLLECTOR | poll={POLL:.1f}s',flush=True)
 while not STOP:
  try:
   made=v.ingest();total_new+=made;now=time.time()
   if made or now-last>=REPORT:
    z=diag()
    print('V7682 '
          f'new={made} total_new={total_new} stored={z["stored"]} | '
          f'post_cutoff_T30={z["post30"]} mature_T60={z["m60"]} mature_T120={z["m120"]} complete={z["complete"]} | '
          f'missing_future={z["missing_future"]} missing_metrics={z["missing_metrics"]} missing_T10={z["missing10"]} | '
          f'cutoff={z["cut"]:.3f} newest_T30={z["newest_t30"]}',flush=True)
    v.display();last=now
  except Exception as e:
   print('V7682 error:',repr(e),flush=True)
  time.sleep(POLL)
 print('V7682 stopped cleanly',flush=True)

if __name__=='__main__': main()
