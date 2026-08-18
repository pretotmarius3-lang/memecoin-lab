#!/usr/bin/env python3
"""MEMECOIN LAB — TRIPLE IMMUTABLE COMMON FUTURE ARENA V7.4.2

Promotes the exact V7.4.1 proposed specifications into ONE shared future-only
paper arena. No retuning. No live trading.

Scientific invariants:
- first run freezes the three exact V7.4.1 proposals and one common cutoff;
- all snapshots at/before the common cutoff are excluded;
- tokens present in V7.4 discovery before promotion are quarantined;
- NEXT-FILL <=15s, TP/SL/time horizon begin at actual fill;
- same anomaly/sparse-path machinery as V6.4;
- first milestone 30 DONE; monitoring continues beyond confirmation;
- R64 is displayed as external immutable benchmark, never modified here.
"""
from __future__ import annotations
import json, math, os, signal, sqlite3, statistics, time, hashlib
from pathlib import Path
import v41_core as core
import v59_champion_exploitation_engine as v59
import v60_economic_edge_discovery_engine as v60
import v63_next_fill_economic_edge_engine as v63

ROOT=Path.home()/"memecoin_lab"; V52=ROOT/'v52_features.db'; DESIGN=ROOT/'v741_shortlist_design.db'; OUT=ROOT/'v742_common_future.db'
LOOP=float(os.environ.get('MEMECOIN_V742_LOOP_S','10')); CONFIRM=int(os.environ.get('MEMECOIN_V742_CONFIRM','30')); STOP=False

def stop(*_):
 global STOP;STOP=True
def sf(x,d=None):
 try:
  v=float(x);return v if math.isfinite(v) else d
 except:return d
def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d
def db():
 d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=30000');return d
def v52():return ro(V52) if V52.exists() else None

def init():
 d=db();d.executescript('''
 CREATE TABLE IF NOT EXISTS arena(arena_id TEXT PRIMARY KEY,created_at REAL,design_id TEXT,common_cutoff REAL,excluded_tokens_json TEXT,cost_pct REAL,fill_window_s REAL,confirm_done INTEGER);
 CREATE TABLE IF NOT EXISTS frozen_rule(rule_id TEXT PRIMARY KEY,arena_id TEXT,family TEXT,experiment_id TEXT,feature TEXT,stage_s INTEGER,horizon_s INTEGER,tp_pct REAL,sl_pct REAL,direction REAL,threshold REAL,source_ho INTEGER,source_exp REAL,source_pf REAL,source_fill REAL);
 CREATE TABLE IF NOT EXISTS events(rule_id TEXT,token_mint TEXT,cutoff_ts REAL,feature_value REAL,state TEXT,fill_price REAL,fill_ts REAL,fill_delay_s REAL,path_points INTEGER,raw_return REAL,net_return REAL,hit INTEGER,exit_reason TEXT,mfe REAL,mae REAL,updated_at REAL,PRIMARY KEY(rule_id,token_mint,cutoff_ts));
 CREATE TABLE IF NOT EXISTS summary(rule_id TEXT PRIMARY KEY,eligible INTEGER,signals INTEGER,no_signal INTEGER,wait_fill INTEGER,no_fill INTEGER,wait_maturity INTEGER,sparse INTEGER,anomaly INTEGER,done INTEGER,fill_rate REAL,delay_med REAL,expectancy REAL,pf REAL,win_rate REAL,raw_dd REAL,true_dd_050 REAL,status TEXT,updated_at REAL);
 ''');d.commit();d.close()

def max_cut():
 x=v52();
 if not x:return 0.0
 r=x.execute('SELECT MAX(cutoff_ts) FROM v52_snapshots').fetchone();x.close();return sf(r[0],0) or 0

def freeze_once():
 d=db();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone()
 if a:d.close();return
 if not DESIGN.exists():d.close();raise SystemExit('Missing v741_shortlist_design.db')
 s=ro(DESIGN);run=s.execute('SELECT * FROM design_run ORDER BY created_at DESC LIMIT 1').fetchone();rules=s.execute("SELECT * FROM proposed_freeze WHERE design_id=? AND status='PROPOSED_ONLY' ORDER BY family",(run['design_id'],)).fetchall()
 if len(rules)!=3:s.close();d.close();raise SystemExit(f'Expected exactly 3 V7.4.1 proposals, got {len(rules)}')
 cut=max_cut();x=v52();excluded=[]
 if x:
  excluded=[str(r[0]) for r in x.execute('SELECT DISTINCT token_mint FROM v52_snapshots WHERE cutoff_ts<=?',(cut,)).fetchall()];x.close()
 now=time.time();aid='A742_'+hashlib.sha256(f"{run['design_id']}|{cut}".encode()).hexdigest()[:20];cost=float(v59.total_cost_pct());fill=float(v63.MAX_FILL_DELAY_S)
 d.execute('INSERT INTO arena VALUES(?,?,?,?,?,?,?,?)',(aid,now,run['design_id'],cut,core.canonical_json(excluded),cost,fill,CONFIRM))
 for r in rules:
  rid='R742_'+hashlib.sha256(f"{aid}|{r['freeze_id']}".encode()).hexdigest()[:20]
  d.execute('INSERT INTO frozen_rule VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(rid,aid,r['family'],r['experiment_id'],r['feature'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['direction'],r['threshold'],r['selected_holdout'],r['holdout_expectancy'],r['holdout_pf'],r['fill_rate']))
 d.commit();d.close();s.close()

def classify(x,r,token,cutoff,val,now,a):
 if float(r['direction'])*val<float(r['threshold']):return {'state':'NO_SIGNAL'}
 f=x.execute('SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND price_sol>0 ORDER BY timestamp LIMIT 1',(token,cutoff)).fetchone();deadline=cutoff+float(a['fill_window_s'])
 if not f:return {'state':'WAIT_FILL' if now<=deadline else 'NO_FILL'}
 ft=float(f['timestamp']);delay=ft-cutoff
 if delay<0 or delay>float(a['fill_window_s']):return {'state':'NO_FILL','fill_delay_s':delay}
 entry=float(f['price_sol']);end=ft+int(r['horizon_s'])
 if now<end:return {'state':'WAIT_MATURITY','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay}
 rows=x.execute('SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol>0 ORDER BY timestamp',(token,ft,end)).fetchall();n=len(rows)
 if n<int(v63.MIN_PATH_POINTS):return {'state':'SPARSE','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay,'path_points':n}
 prices=[float(z['price_sol']) for z in rows];allp=[entry]+prices;steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))];rets=[100*(p/entry-1) for p in prices]
 if any(abs(z)>v60.MAX_ABS_STEP_PCT for z in steps) or any(abs(z)>v60.MAX_ABS_PATH_RETURN_PCT for z in rets):return {'state':'ANOMALY','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay,'path_points':n}
 raw=rets[-1];reason='TIME_EXIT'
 for z in rets:
  if z>=float(r['tp_pct']):raw=float(r['tp_pct']);reason='TP_FIRST';break
  if z<=-float(r['sl_pct']):raw=-float(r['sl_pct']);reason='SL_FIRST';break
 return {'state':'DONE','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay,'path_points':n,'raw_return':raw,'net_return':raw-float(a['cost_pct']),'hit':int(reason=='TP_FIRST'),'exit_reason':reason,'mfe':max(rets),'mae':min(rets)}

def process(a,r):
 x=v52();
 if not x:return
 excluded=set(json.loads(a['excluded_tokens_json']));rows=x.execute(f"SELECT token_mint,cutoff_ts,{r['feature']} val FROM v52_snapshots WHERE stage_s=? AND cutoff_ts>? AND {r['feature']} IS NOT NULL ORDER BY cutoff_ts,token_mint",(r['stage_s'],a['common_cutoff'])).fetchall();now=time.time();o=db()
 for z in rows:
  token=str(z['token_mint']);val=sf(z['val']);cut=float(z['cutoff_ts'])
  if token in excluded or val is None:continue
  q=classify(x,r,token,cut,val,now,a)
  o.execute('''INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(rule_id,token_mint,cutoff_ts) DO UPDATE SET state=excluded.state,fill_price=excluded.fill_price,fill_ts=excluded.fill_ts,fill_delay_s=excluded.fill_delay_s,path_points=excluded.path_points,raw_return=excluded.raw_return,net_return=excluded.net_return,hit=excluded.hit,exit_reason=excluded.exit_reason,mfe=excluded.mfe,mae=excluded.mae,updated_at=excluded.updated_at''',(r['rule_id'],token,cut,val,q['state'],q.get('fill_price'),q.get('fill_ts'),q.get('fill_delay_s'),q.get('path_points'),q.get('raw_return'),q.get('net_return'),q.get('hit'),q.get('exit_reason'),q.get('mfe'),q.get('mae'),now))
 o.commit();o.close();x.close()
def pf(xs):
 g=sum(z for z in xs if z>0);l=-sum(z for z in xs if z<0);return g/l if l>0 else (999 if g>0 else 0)
def rawdd(xs):
 eq=peak=0.;dd=0.
 for z in xs:eq+=z;peak=max(peak,eq);dd=min(dd,eq-peak)
 return dd
def true_dd(xs,risk=.005):
 eq=peak=1.;dd=0.
 for z in xs:
  unit=z/13.;eq*=max(0.000001,1+risk*unit);peak=max(peak,eq);dd=min(dd,eq/peak-1)
 return 100*dd

def summarize(r):
 o=db();rows=[dict(z) for z in o.execute('SELECT * FROM events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(r['rule_id'],)).fetchall()];c={}
 for z in rows:c[z['state']]=c.get(z['state'],0)+1
 done=[z for z in rows if z['state']=='DONE'];xs=[float(z['net_return']) for z in done];signals=len(rows)-c.get('NO_SIGNAL',0);filled=[z for z in rows if z['fill_ts'] is not None and z['state']!='NO_SIGNAL'];delays=[float(z['fill_delay_s']) for z in filled if z['fill_delay_s'] is not None];n=len(xs);exp=statistics.mean(xs) if xs else None;p=pf(xs) if xs else None;wr=sum(z>0 for z in xs)/n if n else None
 status='WAITING'
 if n>=10:status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
 if n>=CONFIRM:status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
 vals=(r['rule_id'],len(rows),signals,c.get('NO_SIGNAL',0),c.get('WAIT_FILL',0),c.get('NO_FILL',0),c.get('WAIT_MATURITY',0),c.get('SPARSE',0),c.get('ANOMALY',0),n,len(filled)/signals if signals else None,statistics.median(delays) if delays else None,exp,p,wr,rawdd(xs) if xs else None,true_dd(xs) if xs else None,status,time.time())
 o.execute('INSERT OR REPLACE INTO summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals);o.commit();o.close()
def r64():
 try:
  d=core.open_research();z=d.execute("SELECT s.* FROM v64_forward_summary s ORDER BY updated_at DESC LIMIT 1").fetchone();d.close();return dict(z) if z else {}
 except:return {}
def display(a,rules):
 print('\033[2J\033[H',end='');print('='*174);print('MEMECOIN LAB — TRIPLE IMMUTABLE COMMON FUTURE ARENA V7.4.2');print('='*174);print(f"arena={a['arena_id']} common_cutoff>{a['common_cutoff']:.3f} | excluded_pre_cutoff_tokens={len(json.loads(a['excluded_tokens_json']))} | confirm={a['confirm_done']} DONE | NEXT-FILL<={a['fill_window_s']:.0f}s\n")
 b=r64();print(f"EXTERNAL CONTROL R64  DONE={int(b.get('done',0) or 0):3d} exp={sf(b.get('expectancy'),0):+6.2f}% PF={sf(b.get('profit_factor'),0):.2f} fill={100*sf(b.get('fill_rate'),0):5.1f}%")
 o=db()
 for r in rules:
  s=o.execute('SELECT * FROM summary WHERE rule_id=?',(r['rule_id'],)).fetchone();s=dict(s) if s else {}
  print(f"{r['family']:<20} {s.get('status','WAITING'):<14} DONE={int(s.get('done',0) or 0):3d}/{CONFIRM} exp={sf(s.get('expectancy'),0):+6.2f}% PF={sf(s.get('pf'),0):.2f} fill={100*sf(s.get('fill_rate'),0):5.1f}% rawDD={sf(s.get('raw_dd'),0):+7.1f}pts TRUE_DD@0.50={sf(s.get('true_dd_050'),0):+5.2f}%")
  print(f"  {r['feature']} stage={r['stage_s']}s h={r['horizon_s']}s dir={r['direction']:+g} th={r['threshold']:.10g} | source HO={r['source_ho']} exp={r['source_exp']:+.2f}% PF={r['source_pf']:.2f}")
 o.close();print('\nGuardrail: common fresh cohort, immutable rules, paper evidence only. No post-freeze retuning or winner substitution.')
def cycle():
 freeze_once();o=db();a=dict(o.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(z) for z in o.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()];o.close()
 for r in rules:process(a,r);summarize(r)
 display(a,rules)
if __name__=='__main__':
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init()
 while not STOP:
  try:cycle()
  except Exception as e:print('V7.4.2 error:',repr(e),flush=True)
  for _ in range(max(1,int(LOOP*2))):
   if STOP:break
   time.sleep(.5)
