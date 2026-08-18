#!/usr/bin/env python3
"""MEMECOIN LAB — CAUSAL-MATERIALIZED FRESH FUTURE ARENA V7.5.4

Fresh future-only arena for the exact V7.4.1 CAPITAL/FLOW/WALLET rules.
It consumes only V7.5.3 INSERT-ONLY causal snapshots, never mutable v52_snapshots.

The arena does not start its cutoff until BOTH acquisition (V7.5.0) and feature
materialization (V7.5.3) are healthy. It pauses automatically if either path
leaves the frozen operational regime.

Paper research only. No rule retuning. No old evidence import.
"""
from __future__ import annotations

import signal, sqlite3, time
from pathlib import Path
import v7423_snapshot_first_causal_arena as base

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/'v52_features.db'
TRACE=ROOT/'v750_acquisition_trace.db'
OUT=ROOT/'v754_causal_materialized_future.db'
base.OUT=OUT
base.V52=V52

MIN_ACQ_N=30
MIN_FEATURE_N=12
ACQ_WINDOW=60.0
FEATURE_WINDOW=60.0
MAX_ACQ_P90=2.0
MAX_ACQ_P95=3.0
MAX_FEATURE_P90=2.0
MAX_FEATURE_P95=3.0
MAX_RAW_PENDING=100
MAX_HEARTBEAT_AGE=5.0
STOP=False

def sf(x,d=None):
 try:return float(x) if x is not None else d
 except:return d

def pct(xs,q):
 if not xs:return None
 ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def ro(path):
 d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def acq_health():
 if not TRACE.exists():return {'healthy':False,'reason':'missing_v750'}
 try:
  d=ro(TRACE);now=time.time();ep=d.execute('SELECT epoch_id,MAX(updated_at) newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1').fetchone()
  if not ep:d.close();return {'healthy':False,'reason':'no_acq_epoch'}
  eid=str(ep['epoch_id']);q=d.execute('SELECT * FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1',(eid,)).fetchone()
  rs=d.execute('SELECT enqueue_at,raw_store_at FROM trace WHERE epoch_id=? AND kind="HOT" AND enqueue_at IS NOT NULL AND raw_store_at IS NOT NULL AND raw_store_at>=?',(eid,now-ACQ_WINDOW)).fetchall();d.close()
  l=[max(0,float(x['raw_store_at'])-float(x['enqueue_at'])) for x in rs];p90=pct(l,.9);p95=pct(l,.95)
  pending=int(q['pending_total']) if q else 999999;oldest=float(q['oldest_pending_age_s']) if q else 999999
  healthy=len(l)>=MIN_ACQ_N and p90 is not None and p90<=MAX_ACQ_P90 and p95 is not None and p95<=MAX_ACQ_P95 and pending==0 and oldest<2
  return {'healthy':healthy,'epoch':eid,'n':len(l),'p90':p90,'p95':p95,'pending':pending,'oldest':oldest}
 except Exception as e:return {'healthy':False,'reason':repr(e)}

def feature_health():
 if not V52.exists():return {'healthy':False,'reason':'missing_v52'}
 try:
  d=ro(V52);now=time.time();s=d.execute('SELECT * FROM v753_engine_state WHERE id=1').fetchone()
  if not s:d.close();return {'healthy':False,'reason':'no_v753_state'}
  rs=d.execute('SELECT build_lag_s FROM v753_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-FEATURE_WINDOW,)).fetchall();d.close()
  l=[float(x[0]) for x in rs];p90=pct(l,.9);p95=pct(l,.95);age=max(0,now-float(s['heartbeat_at']));pending=int(s['raw_pending'])
  healthy=len(l)>=MIN_FEATURE_N and p90 is not None and p90<=MAX_FEATURE_P90 and p95 is not None and p95<=MAX_FEATURE_P95 and age<=MAX_HEARTBEAT_AGE and 0<=pending<=MAX_RAW_PENDING
  return {'healthy':healthy,'n':len(l),'p90':p90,'p95':p95,'heartbeat_age':age,'raw_pending':pending}
 except Exception as e:return {'healthy':False,'reason':repr(e)}

def causal_max_cutoff():
 if not V52.exists():return 0.0
 try:
  d=ro(V52);x=d.execute('SELECT MAX(cutoff_ts) FROM v753_causal_snapshots').fetchone()[0];d.close();return sf(x,0.0) or 0.0
 except:return 0.0

# Make the inherited freeze use only the causal-materialized frontier.
base.max_cutoff=causal_max_cutoff

def init_meta():
 d=base.odb();d.executescript('''
 CREATE TABLE IF NOT EXISTS v754_infra_freeze(
  id INTEGER PRIMARY KEY CHECK(id=1),frozen_at REAL,acq_epoch TEXT,
  acq_n INTEGER,acq_p90 REAL,acq_p95 REAL,
  feature_n INTEGER,feature_p90 REAL,feature_p95 REAL,note TEXT);
 CREATE TABLE IF NOT EXISTS v754_pause_log(
  id INTEGER PRIMARY KEY AUTOINCREMENT,observed_at REAL,reason TEXT,
  acq_p90 REAL,acq_p95 REAL,feature_p90 REAL,feature_p95 REAL,raw_pending INTEGER);
 ''');d.commit();d.close()

def freeze_when_healthy():
 d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone();d.close()
 if a:return True
 ah=acq_health();fh=feature_health()
 if not ah.get('healthy') or not fh.get('healthy'):
  print(f'V7.5.4 WAITING_FOR_CLEAN_INFRA | acquisition={ah} | features={fh}',flush=True);return False
 base.freeze_once()
 d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone()
 d.execute("UPDATE arena SET method='V753_INSERT_ONLY_CAUSAL_MATERIALIZATION_V1' WHERE arena_id=?",(a['arena_id'],))
 d.execute('INSERT OR REPLACE INTO v754_infra_freeze VALUES(?,?,?,?,?,?,?,?,?,?)',(1,time.time(),ah['epoch'],ah['n'],ah['p90'],ah['p95'],fh['n'],fh['p90'],fh['p95'],'exact frozen rules; V750+V753 healthy before fresh cutoff'))
 d.commit();d.close();return True

def ingest_causal(a,r):
 try:x=ro(V52)
 except:return 0
 try:
  src=x.execute(f'''SELECT token_mint,cutoff_ts,built_at,{r['feature']} AS val
                    FROM v753_causal_snapshots WHERE stage_s=? AND cutoff_ts>?
                    ORDER BY cutoff_ts,token_mint''',(int(r['stage_s']),float(a['common_cutoff']))).fetchall()
  o=base.odb();now=time.time();made=0
  for z in src:
   token=str(z['token_mint']);cut=float(z['cutoff_ts']);val=base.sf(z['val']);seen=float(z['built_at'])
   if o.execute('SELECT 1 FROM events WHERE rule_id=? AND token_mint=? AND cutoff_ts=?',(r['rule_id'],token,cut)).fetchone():continue
   deadline=cut+float(a['fill_window_s'])
   if val is None:
    fa=0;sig=0;state='FEATURE_UNAVAILABLE';terminal=seen;locked=None;note='NULL in immutable V753 causal snapshot'
   elif seen>deadline:
    fa=1;sig=0;state='LATE_SNAPSHOT';terminal=seen;locked=val;note=f'V753 build after deadline by {seen-deadline:.3f}s'
   else:
    fa=1;locked=val;qual=(float(r['direction'])*val>=float(r['threshold']));sig=int(qual)
    state='SIGNAL_LOCKED' if qual else 'NO_SIGNAL';terminal=None if qual else seen;note=f'V753 immutable feature={val:.12g}, build_lag={seen-cut:.3f}s'
   o.execute('''INSERT OR IGNORE INTO events(rule_id,token_mint,cutoff_ts,first_observed_at,feature_available,locked_feature,
     signal_decision,state,signal_locked_at,terminal_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(r['rule_id'],token,cut,seen,fa,locked,sig,state,seen if state=='SIGNAL_LOCKED' else None,terminal,now))
   if o.execute('SELECT changes()').fetchone()[0]:base.log_transition(o,r,token,cut,'NEW',state,note);made+=1
  o.commit();o.close();return made
 finally:x.close()

def pause(reason,ah,fh):
 d=base.odb();d.execute('INSERT INTO v754_pause_log(observed_at,reason,acq_p90,acq_p95,feature_p90,feature_p95,raw_pending) VALUES(?,?,?,?,?,?,?)',(time.time(),reason,ah.get('p90'),ah.get('p95'),fh.get('p90'),fh.get('p95'),fh.get('raw_pending')));d.commit();d.close()
 print('\n'+'='*170);print('MEMECOIN LAB — CAUSAL-MATERIALIZED FUTURE ARENA V7.5.4');print('='*170)
 print('PAUSED:',reason);print(' acquisition:',ah);print(' features:',fh);print('No new evidence ingested while causal infrastructure is outside gate.',flush=True)

def cycle():
 if not freeze_when_healthy():return
 d=base.odb();a=dict(d.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(x) for x in d.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()];f=d.execute('SELECT * FROM v754_infra_freeze WHERE id=1').fetchone();d.close()
 ah=acq_health();fh=feature_health()
 if ah.get('epoch')!=f['acq_epoch']:pause('ACQUISITION_EPOCH_CHANGED',ah,fh);return
 if not ah.get('healthy') or not fh.get('healthy'):pause('LIVE_CAUSAL_INFRA_UNHEALTHY',ah,fh);return
 for r in rules:
  ingest_causal(a,r);base.progress_signal_locked(a,r);base.progress_fill_locked(a,r);base.audit_invariants(r);base.summarize(r)
 base.display(a,rules)
 print(f"V7.5.4 CAUSAL HEALTH | ACQ n={ah['n']} p90/p95={ah['p90']:.3f}/{ah['p95']:.3f}s pending={ah['pending']} | FEATURE n={fh['n']} p90/p95={fh['p90']:.3f}/{fh['p95']:.3f}s raw_pending={fh['raw_pending']}")
 print('Source=v753_causal_snapshots INSERT-ONLY. Exact rules. Fresh cutoff. No old evidence imported.',flush=True)

def main():
 signal.signal(signal.SIGINT,base.stop);signal.signal(signal.SIGTERM,base.stop)
 base.init_schema();init_meta();print('V7.5.4 boot: waiting for V750 + V753 clean causal regime before freezing...',flush=True)
 while not base.STOP:
  try:cycle()
  except Exception as e:print('V7.5.4 error:',repr(e),flush=True)
  time.sleep(max(.1,base.LOOP))

if __name__=='__main__':main()
