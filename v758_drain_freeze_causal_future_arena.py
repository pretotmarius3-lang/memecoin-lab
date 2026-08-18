#!/usr/bin/env python3
"""MEMECOIN LAB — DRAIN-FREEZE CAUSAL FUTURE ARENA V7.5.8
Consumes only V7.5.7 insert-only snapshots. Starts after V750 acquisition and
V757 causal materialization are both healthy. Exact frozen rules, fresh cutoff.
"""
from __future__ import annotations
import signal,sqlite3,time
from pathlib import Path
import v7423_snapshot_first_causal_arena as base

ROOT=Path.home()/"memecoin_lab"; V52=ROOT/'v52_features.db'; TRACE=ROOT/'v750_acquisition_trace.db'; OUT=ROOT/'v758_drain_freeze_causal_future.db'
base.OUT=OUT;base.V52=V52
WINDOW=60.0;MIN_ACQ_N=50;MIN_FEATURE_N=12;MAX_ACQ_P90=2.;MAX_ACQ_P95=3.;MAX_FEATURE_P90=1.5;MAX_FEATURE_P95=2.;MAX_HEARTBEAT_AGE=4.;MAX_RAW_PENDING=50

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def pct(xs,q):
 if not xs:return None
 ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo;return ys[lo]+(ys[hi]-ys[lo])*f

def acq_health():
 try:
  d=ro(TRACE);now=time.time();ep=d.execute('SELECT epoch_id,MAX(updated_at) newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1').fetchone()
  if not ep:d.close();return {'healthy':False,'reason':'no_epoch'}
  eid=str(ep['epoch_id']);q=d.execute('SELECT * FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1',(eid,)).fetchone();rs=d.execute('SELECT enqueue_at,raw_store_at FROM trace WHERE epoch_id=? AND kind="HOT" AND enqueue_at IS NOT NULL AND raw_store_at IS NOT NULL AND raw_store_at>=?',(eid,now-WINDOW)).fetchall();d.close()
  l=[max(0,float(r['raw_store_at'])-float(r['enqueue_at'])) for r in rs];p90=pct(l,.9);p95=pct(l,.95);pending=int(q['pending_total']) if q else 999999;old=float(q['oldest_pending_age_s']) if q else 999999
  ok=len(l)>=MIN_ACQ_N and p90 is not None and p90<=MAX_ACQ_P90 and p95 is not None and p95<=MAX_ACQ_P95 and pending==0 and old<2
  return {'healthy':ok,'epoch':eid,'n':len(l),'p90':p90,'p95':p95,'pending':pending,'oldest':old}
 except Exception as e:return {'healthy':False,'reason':repr(e)}

def feature_health():
 try:
  d=ro(V52);now=time.time();s=d.execute('SELECT * FROM v757_engine_state WHERE id=1').fetchone()
  if not s:d.close();return {'healthy':False,'reason':'no_v757_state'}
  phase=str(s['phase']);rs=d.execute('SELECT build_lag_s FROM v757_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-WINDOW,)).fetchall();d.close();l=[float(x[0]) for x in rs];p90=pct(l,.9);p95=pct(l,.95);age=max(0,now-float(s['heartbeat_at']));rp=int(s['raw_pending'])
  ok=phase=='LIVE_CAUSAL' and len(l)>=MIN_FEATURE_N and p90 is not None and p90<=MAX_FEATURE_P90 and p95 is not None and p95<=MAX_FEATURE_P95 and age<=MAX_HEARTBEAT_AGE and 0<=rp<=MAX_RAW_PENDING
  return {'healthy':ok,'phase':phase,'n':len(l),'p90':p90,'p95':p95,'heartbeat_age':age,'raw_pending':rp,'activation_observed_at':s['activation_observed_at']}
 except Exception as e:return {'healthy':False,'reason':repr(e)}

def causal_max_cutoff():
 try:d=ro(V52);x=d.execute('SELECT MAX(cutoff_ts) FROM v757_causal_snapshots').fetchone()[0];d.close();return float(x or 0)
 except:return 0.
base.max_cutoff=causal_max_cutoff

def init_meta():
 d=base.odb();d.executescript('''CREATE TABLE IF NOT EXISTS v758_infra_freeze(id INTEGER PRIMARY KEY CHECK(id=1),frozen_at REAL,acq_epoch TEXT,acq_n INTEGER,acq_p90 REAL,acq_p95 REAL,feature_n INTEGER,feature_p90 REAL,feature_p95 REAL,feature_activation_observed_at REAL,note TEXT); CREATE TABLE IF NOT EXISTS v758_pause_log(id INTEGER PRIMARY KEY AUTOINCREMENT,observed_at REAL,reason TEXT,acq_p90 REAL,acq_p95 REAL,feature_p90 REAL,feature_p95 REAL,raw_pending INTEGER);''');d.commit();d.close()

def freeze_when_healthy():
 d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone();d.close()
 if a:return True
 ah=acq_health();fh=feature_health()
 if not ah.get('healthy') or not fh.get('healthy'):
  print(f'V7.5.8 WAITING_FOR_CLEAN_INFRA | acquisition={ah} | features={fh}',flush=True);return False
 base.freeze_once();d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone();d.execute("UPDATE arena SET method='V757_DRAIN_FREEZE_INSERT_ONLY_CAUSAL_V1' WHERE arena_id=?",(a['arena_id'],));d.execute('INSERT OR REPLACE INTO v758_infra_freeze VALUES(?,?,?,?,?,?,?,?,?,?,?)',(1,time.time(),ah['epoch'],ah['n'],ah['p90'],ah['p95'],fh['n'],fh['p90'],fh['p95'],fh['activation_observed_at'],'V750 + V757 healthy after drain/freeze barrier'));d.commit();d.close();return True

def ingest(a,r):
 x=ro(V52);src=x.execute(f'''SELECT token_mint,cutoff_ts,built_at,{r['feature']} val FROM v757_causal_snapshots WHERE stage_s=? AND cutoff_ts>? ORDER BY cutoff_ts,token_mint''',(int(r['stage_s']),float(a['common_cutoff']))).fetchall();o=base.odb();now=time.time();made=0
 for z in src:
  tok=str(z['token_mint']);cut=float(z['cutoff_ts']);seen=float(z['built_at']);val=base.sf(z['val'])
  if o.execute('SELECT 1 FROM events WHERE rule_id=? AND token_mint=? AND cutoff_ts=?',(r['rule_id'],tok,cut)).fetchone():continue
  deadline=cut+float(a['fill_window_s'])
  if val is None:fa=0;sig=0;state='FEATURE_UNAVAILABLE';terminal=seen;locked=None;note='NULL in immutable V757 snapshot'
  elif seen>deadline:fa=1;sig=0;state='LATE_SNAPSHOT';terminal=seen;locked=val;note=f'V757 late by {seen-deadline:.3f}s'
  else:
   fa=1;locked=val;qual=float(r['direction'])*val>=float(r['threshold']);sig=int(qual);state='SIGNAL_LOCKED' if qual else 'NO_SIGNAL';terminal=None if qual else seen;note=f'V757 immutable={val:.12g}; lag={seen-cut:.3f}s'
  o.execute('''INSERT OR IGNORE INTO events(rule_id,token_mint,cutoff_ts,first_observed_at,feature_available,locked_feature,signal_decision,state,signal_locked_at,terminal_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(r['rule_id'],tok,cut,seen,fa,locked,sig,state,seen if state=='SIGNAL_LOCKED' else None,terminal,now))
  if o.execute('SELECT changes()').fetchone()[0]:base.log_transition(o,r,tok,cut,'NEW',state,note);made+=1
 o.commit();o.close();x.close();return made

def pause(reason,ah,fh):
 d=base.odb();d.execute('INSERT INTO v758_pause_log(observed_at,reason,acq_p90,acq_p95,feature_p90,feature_p95,raw_pending) VALUES(?,?,?,?,?,?,?)',(time.time(),reason,ah.get('p90'),ah.get('p95'),fh.get('p90'),fh.get('p95'),fh.get('raw_pending')));d.commit();d.close();print('\n'+'='*160);print('MEMECOIN LAB — DRAIN-FREEZE CAUSAL FUTURE ARENA V7.5.8');print('='*160);print('PAUSED:',reason);print(' acquisition:',ah);print(' features:',fh);print('No evidence ingested outside frozen causal regime.',flush=True)

def cycle():
 if not freeze_when_healthy():return
 d=base.odb();a=dict(d.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(x) for x in d.execute('SELECT * FROM frozen_rule ORDER BY family')];f=d.execute('SELECT * FROM v758_infra_freeze WHERE id=1').fetchone();d.close();ah=acq_health();fh=feature_health()
 if ah.get('epoch')!=f['acq_epoch']:pause('ACQUISITION_EPOCH_CHANGED',ah,fh);return
 if not ah.get('healthy') or not fh.get('healthy'):pause('LIVE_CAUSAL_INFRA_UNHEALTHY',ah,fh);return
 for r in rules:ingest(a,r);base.progress_signal_locked(a,r);base.progress_fill_locked(a,r);base.audit_invariants(r);base.summarize(r)
 base.display(a,rules);print(f"V7.5.8 HEALTH | ACQ n={ah['n']} p90/p95={ah['p90']:.3f}/{ah['p95']:.3f}s | FEATURE n={fh['n']} p90/p95={fh['p90']:.3f}/{fh['p95']:.3f}s raw_pending={fh['raw_pending']}");print('Source=v757_causal_snapshots INSERT-ONLY. Exact rules. Fresh cutoff.',flush=True)

def main():
 signal.signal(signal.SIGINT,base.stop);signal.signal(signal.SIGTERM,base.stop);base.init_schema();init_meta();print('V7.5.8 boot: waiting for clean V750 + drain/freeze V757...',flush=True)
 while not base.STOP:
  try:cycle()
  except Exception as e:print('V7.5.8 error:',repr(e),flush=True)
  time.sleep(max(.1,base.LOOP))
if __name__=='__main__':main()
