#!/usr/bin/env python3
"""V6.4.3 — read-only PRE/POST V5.2.2 cohort audit.

Separates V6.4 observations by the durable V5.2.2 decoder activation watermark.
No frozen rule, event, swap, snapshot, threshold or outcome is modified.
"""
from __future__ import annotations
import json, math, os, sqlite3, statistics
from pathlib import Path
import v41_core as core

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/"v52_features.db"
ENV_TS=os.environ.get("MEMECOIN_V522_WATERMARK_TS")

def sf(x,d=None):
 try:
  v=float(x); return v if math.isfinite(v) else d
 except Exception:return d

def infer_watermark():
 if ENV_TS:return float(ENV_TS),"env"
 d=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=10)
 try:
  r=d.execute("SELECT value FROM v52_state WHERE key='v522_started_at'").fetchone()
  if r:
   x=json.loads(r[0]);return float(x['ts']),str(x.get('source') or 'v52_state')
  q=d.execute("SELECT MIN(processed_at) FROM v52_processed WHERE status='PRICE_QUARANTINE'").fetchone()
  if q and q[0] is not None:return float(q[0]),"earliest PRICE_QUARANTINE fallback"
 finally:d.close()
 raise SystemExit("No durable V5.2.2 watermark yet. Pull/restart v522 once, or set MEMECOIN_V522_WATERMARK_TS.")

def pf(xs):
 g=sum(x for x in xs if x>0); l=-sum(x for x in xs if x<0)
 return g/l if l>0 else (999.0 if g>0 else 0.0)

def dd(xs):
 eq=peak=0.; worst=0.
 for x in xs:eq+=x;peak=max(peak,eq);worst=min(worst,eq-peak)
 return worst

def report(name,rows):
 states={}
 for r in rows:states[r['state']]=states.get(r['state'],0)+1
 signals=sum(1 for r in rows if r['state']!='NO_SIGNAL')
 done=[r for r in rows if r['state']=='DONE' and sf(r['net_return']) is not None];xs=[float(r['net_return']) for r in done]
 terminal=sum(states.get(k,0) for k in ('NO_FILL','SPARSE_PATH','ANOMALY','DONE'));clean=len(done)/terminal*100 if terminal else 0
 print(f"\n{name}\n"+'-'*100)
 print(f"eligible={len(rows)} signals={signals} done={len(done)} no_signal={states.get('NO_SIGNAL',0)} no_fill={states.get('NO_FILL',0)} sparse={states.get('SPARSE_PATH',0)} anomaly={states.get('ANOMALY',0)} waiting_fill={states.get('WAIT_FILL',0)} waiting_maturity={states.get('WAIT_MATURITY',0)}")
 print(f"terminal_clean_DONE_rate={clean:.1f}%")
 if xs:
  print(f"DONE performance: n={len(xs)} exp={statistics.mean(xs):+.2f}% med={statistics.median(xs):+.2f}% win={100*sum(x>0 for x in xs)/len(xs):.1f}% PF={pf(xs):.2f} DD={dd(xs):+.2f}%")
 else:print("DONE performance: n=0")
 return {'eligible':len(rows),'signals':signals,'done':len(done),'anomaly':states.get('ANOMALY',0),'sparse':states.get('SPARSE_PATH',0),'no_fill':states.get('NO_FILL',0),'clean':clean}

def main():
 wm,src=infer_watermark();d=core.open_research();d.row_factory=sqlite3.Row
 rule=d.execute("SELECT * FROM v64_frozen_rule LIMIT 1").fetchone()
 if not rule:raise SystemExit("No V6.4 frozen rule")
 rows=d.execute("SELECT * FROM v64_forward_events WHERE rule_id=? ORDER BY cutoff_ts,token_mint",(rule['rule_id'],)).fetchall()
 pre=[r for r in rows if float(r['cutoff_ts'])<=wm];post=[r for r in rows if float(r['cutoff_ts'])>wm]
 print('='*140);print('MEMECOIN LAB — V6.4.3 V5.2.2 COHORT AUDIT (READ ONLY)');print('='*140)
 print(f"rule={rule['rule_id']} | V5.2.2 watermark={wm:.3f} ({src})")
 print('POST means snapshot cutoff_ts strictly after this durable watermark; no historical event is rewritten.')
 a=report('PRE-V5.2.2',pre);b=report('POST-V5.2.2',post)
 print('\nDELTA / QUALITY\n'+'-'*100)
 for k in ('eligible','signals','done','no_fill','sparse','anomaly'):print(f"{k:12s}: pre={a[k]:>5} post={b[k]:>5}")
 print(f"clean rate  : pre={a['clean']:.1f}% post={b['clean']:.1f}%")
 if b['signals']<10:print('\nCAUTION: POST cohort is still very small; treat this as instrumentation, not evidence of improvement/decay.')
 elif b['done']<10:print('\nCAUTION: enough POST signals to inspect data quality, but too few DONE for alpha conclusions.')
 else:print('\nPOST cohort is becoming informative; continue accumulating toward the frozen 30-DONE arena threshold.')
 d.close()
if __name__=='__main__':main()
