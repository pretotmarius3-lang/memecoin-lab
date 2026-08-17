#!/usr/bin/env python3
"""Memecoin Lab V5.9.1 — Execution Reality Engine.

Audits frozen V5.9 champion policies against actual post-decision swap paths.
No policy is promoted and no V5.9 forward result is rewritten.

Adds:
- execution-quality rejection accounting;
- robust path statistics (median/trimmed mean instead of outlier-led averages);
- chronological first-touch TP/SL simulation;
- fixed-exit baseline;
- score-bucket diagnostics for the frozen signal;
- separate HISTORICAL DIAGNOSTIC and FORWARD PAPER reporting.

Research/paper only. No live orders.
"""
from __future__ import annotations
import json, math, os, signal, sqlite3, statistics, time
from collections import Counter, defaultdict
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
LOOP=float(os.environ.get("MEMECOIN_V591_LOOP_S","8"))
MAX_GAP_S=float(os.environ.get("MEMECOIN_V591_MAX_ENTRY_GAP_S","15"))
MIN_PATH_POINTS=int(os.environ.get("MEMECOIN_V591_MIN_PATH_POINTS","3"))
MAX_ABS_STEP_PCT=float(os.environ.get("MEMECOIN_V591_MAX_ABS_STEP_PCT","500"))
MAX_ABS_PATH_RETURN_PCT=float(os.environ.get("MEMECOIN_V591_MAX_ABS_PATH_RETURN_PCT","10000"))
STOP=False

def stop(*_):
 global STOP; STOP=True

def sf(x,d=None):
 try:
  v=float(x); return v if math.isfinite(v) else d
 except Exception:return d

def open_v52():
 if not V52.exists(): return None
 d=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=30); d.row_factory=sqlite3.Row; d.execute("PRAGMA busy_timeout=30000"); return d

def init():
 d=core.open_research(); d.executescript("""
 CREATE TABLE IF NOT EXISTS v591_policy_audit(
  policy_id TEXT PRIMARY KEY,raw_signals INTEGER NOT NULL,executable INTEGER NOT NULL,
  rejected_no_entry INTEGER NOT NULL,rejected_stale_entry INTEGER NOT NULL,
  rejected_no_path INTEGER NOT NULL,rejected_sparse_path INTEGER NOT NULL,
  rejected_price_anomaly INTEGER NOT NULL,fixed_median_net REAL,fixed_trimmed_mean_net REAL,
  fixed_win_rate REAL,median_mfe REAL,median_mae REAL,best_barrier_label TEXT,
  best_barrier_median_net REAL,best_barrier_win_rate REAL,details_json TEXT NOT NULL,updated_at REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS v591_barrier_results(
  policy_id TEXT NOT NULL,tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,n INTEGER NOT NULL,
  tp_first INTEGER NOT NULL,sl_first INTEGER NOT NULL,time_exit INTEGER NOT NULL,
  avg_net REAL,median_net REAL,win_rate REAL,profit_factor REAL,worst_net REAL,best_net REAL,
  PRIMARY KEY(policy_id,tp_pct,sl_pct));
 CREATE TABLE IF NOT EXISTS v591_score_buckets(
  policy_id TEXT NOT NULL,bucket TEXT NOT NULL,n INTEGER NOT NULL,median_score REAL,
  median_net REAL,win_rate REAL,median_mfe REAL,median_mae REAL,PRIMARY KEY(policy_id,bucket));
 """); d.commit(); d.close()

def trimmed_mean(xs,frac=.05):
 xs=sorted(float(x) for x in xs if sf(x) is not None)
 if not xs:return None
 k=int(len(xs)*frac)
 ys=xs[k:len(xs)-k] if k and len(xs)>2*k else xs
 return statistics.mean(ys) if ys else None

def target_tp(spec):
 t=str(spec.get('target',''))
 for n in (10,20,50):
  if f'hit{n}' in t:return float(n)
 return 10.0

def barrier_grid(spec):
 base=target_tp(spec)
 tps=sorted(set([base,10.0,20.0,50.0]))
 sls=[5.0,10.0,15.0,20.0]
 return [(tp,sl) for tp in tps for sl in sls]

def load_path(token,decision_ts,holding_s):
 db=open_v52()
 if db is None:return None,'NO_DB'
 end=float(decision_ts)+int(holding_s)
 pre=db.execute("SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp DESC LIMIT 1",(token,float(decision_ts))).fetchone()
 if not pre:db.close(); return None,'NO_ENTRY'
 gap=float(decision_ts)-float(pre['timestamp'])
 if gap>MAX_GAP_S:db.close(); return None,'STALE_ENTRY'
 rows=db.execute("SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp",(token,float(decision_ts),end)).fetchall(); db.close()
 if not rows:return None,'NO_PATH'
 if len(rows)<MIN_PATH_POINTS:return None,'SPARSE_PATH'
 entry=float(pre['price_sol']); prices=[float(r['price_sol']) for r in rows]
 if entry<=0 or any(p<=0 or not math.isfinite(p) for p in prices):return None,'PRICE_ANOMALY'
 allp=[entry]+prices
 steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]
 rets=[100*(p/entry-1) for p in prices]
 if any(abs(x)>MAX_ABS_STEP_PCT for x in steps) or any(abs(x)>MAX_ABS_PATH_RETURN_PCT for x in rets):return None,'PRICE_ANOMALY'
 return {'entry':entry,'prices':prices,'rets':rets,'fixed_raw':rets[-1],'mfe':max(rets),'mae':min(rets)},'OK'

def first_touch(path,tp,sl):
 for r in path['rets']:
  if r>=tp:return tp,'TP_FIRST'
  if r<=-sl:return -sl,'SL_FIRST'
 return path['fixed_raw'],'TIME_EXIT'

def profit_factor(net):
 gains=sum(x for x in net if x>0); losses=-sum(x for x in net if x<0)
 if losses>0:return gains/losses
 return 999.0 if gains>0 else None

def quartile_buckets(trades):
 if not trades:return []
 xs=sorted(t['score'] for t in trades)
 def q(p):
  pos=(len(xs)-1)*p; lo=int(math.floor(pos)); hi=int(math.ceil(pos)); w=pos-lo
  return xs[lo]*(1-w)+xs[hi]*w
 q1,q2,q3=q(.25),q(.5),q(.75)
 out=[]
 for t in trades:
  s=t['score']; b='Q1' if s<=q1 else ('Q2' if s<=q2 else ('Q3' if s<=q3 else 'Q4'))
  out.append((b,t))
 return out

def audit_policy(p):
 spec=json.loads(p['spec_json']); rows=v59.score_rows(spec,before_ts=float(p['freeze_cutoff_ts']))
 selected=[]
 for r in rows:
  score=float(p['direction'])*r['feature_value']
  if score>=float(p['threshold']):selected.append((r,score))
 rejects=Counter(); trades=[]
 for r,score in selected:
  path,reason=load_path(r['token_mint'],r['decision_ts'],int(p['holding_s']))
  if reason!='OK':rejects[reason]+=1; continue
  trades.append({'token':r['token_mint'],'score':score,'path':path,'fixed_net':path['fixed_raw']-v59.total_cost_pct()})
 fixed=[t['fixed_net'] for t in trades]; mfes=[t['path']['mfe'] for t in trades]; maes=[t['path']['mae'] for t in trades]
 barriers=[]
 for tp,sl in barrier_grid(spec):
  nets=[]; outcomes=Counter()
  for t in trades:
   raw,outcome=first_touch(t['path'],tp,sl); outcomes[outcome]+=1; nets.append(raw-v59.total_cost_pct())
  barriers.append({'tp':tp,'sl':sl,'n':len(nets),'tp_first':outcomes['TP_FIRST'],'sl_first':outcomes['SL_FIRST'],'time_exit':outcomes['TIME_EXIT'],'avg':trimmed_mean(nets),'median':statistics.median(nets) if nets else None,'win':sum(x>0 for x in nets)/len(nets) if nets else None,'pf':profit_factor(nets),'worst':min(nets) if nets else None,'best':max(nets) if nets else None})
 best=max(barriers,key=lambda x:((x['median'] if x['median'] is not None else -1e99),(x['win'] if x['win'] is not None else -1))) if barriers else None
 d=core.open_research(); d.execute("DELETE FROM v591_barrier_results WHERE policy_id=?",(p['policy_id'],)); d.execute("DELETE FROM v591_score_buckets WHERE policy_id=?",(p['policy_id'],))
 for b in barriers:d.execute("INSERT INTO v591_barrier_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(p['policy_id'],b['tp'],b['sl'],b['n'],b['tp_first'],b['sl_first'],b['time_exit'],b['avg'],b['median'],b['win'],b['pf'],b['worst'],b['best']))
 grouped=defaultdict(list)
 for bucket,t in quartile_buckets(trades):grouped[bucket].append(t)
 for bucket,ts in grouped.items():
  nets=[t['fixed_net'] for t in ts]; scores=[t['score'] for t in ts]; bm=[t['path']['mfe'] for t in ts]; ba=[t['path']['mae'] for t in ts]
  d.execute("INSERT INTO v591_score_buckets VALUES(?,?,?,?,?,?,?,?)",(p['policy_id'],bucket,len(ts),statistics.median(scores),statistics.median(nets),sum(x>0 for x in nets)/len(nets),statistics.median(bm),statistics.median(ba)))
 details={'cost_pct':v59.total_cost_pct(),'quality_gate':{'max_entry_gap_s':MAX_GAP_S,'min_path_points':MIN_PATH_POINTS,'max_abs_step_pct':MAX_ABS_STEP_PCT,'max_abs_path_return_pct':MAX_ABS_PATH_RETURN_PCT},'warning':'Barrier grid is historical diagnostic only; best historical barrier is NOT promoted to the frozen forward policy.'}
 d.execute("""INSERT INTO v591_policy_audit VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(policy_id) DO UPDATE SET raw_signals=excluded.raw_signals,executable=excluded.executable,rejected_no_entry=excluded.rejected_no_entry,rejected_stale_entry=excluded.rejected_stale_entry,rejected_no_path=excluded.rejected_no_path,rejected_sparse_path=excluded.rejected_sparse_path,rejected_price_anomaly=excluded.rejected_price_anomaly,fixed_median_net=excluded.fixed_median_net,fixed_trimmed_mean_net=excluded.fixed_trimmed_mean_net,fixed_win_rate=excluded.fixed_win_rate,median_mfe=excluded.median_mfe,median_mae=excluded.median_mae,best_barrier_label=excluded.best_barrier_label,best_barrier_median_net=excluded.best_barrier_median_net,best_barrier_win_rate=excluded.best_barrier_win_rate,details_json=excluded.details_json,updated_at=excluded.updated_at""",(p['policy_id'],len(selected),len(trades),rejects['NO_ENTRY'],rejects['STALE_ENTRY'],rejects['NO_PATH'],rejects['SPARSE_PATH'],rejects['PRICE_ANOMALY'],statistics.median(fixed) if fixed else None,trimmed_mean(fixed),sum(x>0 for x in fixed)/len(fixed) if fixed else None,statistics.median(mfes) if mfes else None,statistics.median(maes) if maes else None,(f"TP{best['tp']:.0f}/SL{best['sl']:.0f}" if best else None),(best['median'] if best else None),(best['win'] if best else None),core.canonical_json(details),time.time())); d.commit(); d.close()

def display(policies):
 d=core.open_research(); print('\033[2J\033[H',end=''); print('='*170); print('MEMECOIN LAB — EXECUTION REALITY ENGINE V5.9.1'); print('='*170); print(f"QUALITY GATE: entry_gap<={MAX_GAP_S:g}s | path_points>={MIN_PATH_POINTS} | max single step={MAX_ABS_STEP_PCT:g}% | max path return={MAX_ABS_PATH_RETURN_PCT:g}% | cost={v59.total_cost_pct():.2f}%")
 print('Historical barrier comparisons are diagnostics only. They do NOT modify the frozen V5.9 forward policies.\n')
 for i,p in enumerate(policies,1):
  a=d.execute('SELECT * FROM v591_policy_audit WHERE policy_id=?',(p['policy_id'],)).fetchone(); spec=json.loads(p['spec_json'])
  if not a:continue
  print(f"#{i} {p['mutation_label']}  {p['policy_id']}  target={spec.get('target')} hold={p['holding_s']}s")
  print(f"   EXECUTION  raw_signals={a['raw_signals']} executable={a['executable']} | reject no_entry={a['rejected_no_entry']} stale={a['rejected_stale_entry']} no_path={a['rejected_no_path']} sparse={a['rejected_sparse_path']} anomaly={a['rejected_price_anomaly']}")
  print(f"   FIXED EXIT median_net={sf(a['fixed_median_net'],0):+.2f}% trimmed_avg={sf(a['fixed_trimmed_mean_net'],0):+.2f}% win={100*sf(a['fixed_win_rate'],0):.1f}% | MFE_med={sf(a['median_mfe'],0):+.2f}% MAE_med={sf(a['median_mae'],0):+.2f}%")
  print(f"   BEST HIST BARRIER (diagnostic only) {a['best_barrier_label']} median_net={sf(a['best_barrier_median_net'],0):+.2f}% win={100*sf(a['best_barrier_win_rate'],0):.1f}%")
  bs=d.execute('SELECT * FROM v591_barrier_results WHERE policy_id=? ORDER BY median_net DESC LIMIT 4',(p['policy_id'],)).fetchall()
  for b in bs:print(f"      TP{b['tp_pct']:.0f}/SL{b['sl_pct']:.0f}: n={b['n']} TPfirst={b['tp_first']} SLfirst={b['sl_first']} time={b['time_exit']} med={sf(b['median_net'],0):+.2f}% win={100*sf(b['win_rate'],0):.1f}% PF={sf(b['profit_factor'],0):.2f}")
  buckets=d.execute('SELECT * FROM v591_score_buckets WHERE policy_id=? ORDER BY bucket',(p['policy_id'],)).fetchall()
  if buckets:
   print('   SIGNAL STRENGTH → ECONOMICS')
   for b in buckets:print(f"      {b['bucket']} n={b['n']:<4} score_med={sf(b['median_score'],0):.3g} net_med={sf(b['median_net'],0):+.2f}% win={100*sf(b['win_rate'],0):.1f}% MFE={sf(b['median_mfe'],0):+.2f}% MAE={sf(b['median_mae'],0):+.2f}%")
  f=d.execute("SELECT COUNT(*) n,SUM(status='OPEN') o,SUM(status='DONE') done,SUM(status='NO_PRICE') np,AVG(CASE WHEN status='DONE' THEN net_return END) avg FROM v59_forward_signals WHERE policy_id=?",(p['policy_id'],)).fetchone(); done=[float(r[0]) for r in d.execute("SELECT net_return FROM v59_forward_signals WHERE policy_id=? AND status='DONE' AND net_return IS NOT NULL",(p['policy_id'],)).fetchall()]
  print(f"   TRUE FORWARD (unchanged V5.9) signals={int(f['n'] or 0)} open={int(f['o'] or 0)} done={int(f['done'] or 0)} no_price={int(f['np'] or 0)} avg={sf(f['avg'],0):+.2f}% med={(statistics.median(done) if done else 0):+.2f}%")
  print()
 d.close(); print('Guardrail: quality filters remove technically invalid price paths; barrier search is descriptive historical analysis only. No historical winner is silently promoted.')

def cycle():
 v59.init(); policies=v59.refresh()
 for p in policies:audit_policy(p)
 display(policies)

def main():
 signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
 while not STOP:
  try:cycle()
  except Exception as e:print('V5.9.1 error:',repr(e),flush=True)
  time.sleep(LOOP)
if __name__=='__main__':main()
