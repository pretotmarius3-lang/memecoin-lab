#!/usr/bin/env python3
"""Memecoin Lab V6.2 — Future-Only Economic Champion Arena.

Freezes representative V6.0 rules from the strongest V6.1 economic families,
then evaluates only snapshots whose cutoff_ts is strictly after the arena freeze.
No threshold/barrier/horizon retuning is permitted after freeze.
Paper/research only; no signing or live execution.
"""
from __future__ import annotations

import json, math, os, signal, sqlite3, statistics, time
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59
import v60_economic_edge_discovery_engine as v60

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
LOOP=float(os.environ.get("MEMECOIN_V62_LOOP_S","10"))
TOP_N=int(os.environ.get("MEMECOIN_V62_TOP_N","4"))
MIN_CONFIRM_TRADES=int(os.environ.get("MEMECOIN_V62_MIN_CONFIRM_TRADES","30"))
MIN_SURVIVE_TRADES=int(os.environ.get("MEMECOIN_V62_MIN_SURVIVE_TRADES","10"))
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
 CREATE TABLE IF NOT EXISTS v62_champions(
  champion_id TEXT PRIMARY KEY,
  family TEXT NOT NULL UNIQUE,
  source_experiment_id TEXT NOT NULL,
  feature TEXT NOT NULL,
  stage_s INTEGER NOT NULL,
  horizon_s INTEGER NOT NULL,
  tp_pct REAL NOT NULL,
  sl_pct REAL NOT NULL,
  direction REAL NOT NULL,
  threshold REAL NOT NULL,
  frozen_at REAL NOT NULL,
  frozen_max_cutoff_ts REAL NOT NULL,
  source_expectancy REAL,
  source_pf REAL,
  source_win_rate REAL,
  created_at REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS v62_forward_trades(
  champion_id TEXT NOT NULL,
  token_mint TEXT NOT NULL,
  cutoff_ts REAL NOT NULL,
  feature_value REAL NOT NULL,
  entry_price REAL,
  net_return REAL,
  raw_return REAL,
  hit INTEGER,
  reason TEXT,
  mfe REAL,
  mae REAL,
  evaluated_at REAL NOT NULL,
  PRIMARY KEY(champion_id,token_mint,cutoff_ts));
 CREATE TABLE IF NOT EXISTS v62_forward_summary(
  champion_id TEXT PRIMARY KEY,
  family TEXT NOT NULL,
  eligible INTEGER NOT NULL,
  signals INTEGER NOT NULL,
  done INTEGER NOT NULL,
  expectancy REAL,
  median_net REAL,
  win_rate REAL,
  profit_factor REAL,
  hit_rate REAL,
  max_drawdown REAL,
  status TEXT NOT NULL,
  updated_at REAL NOT NULL);
 """); d.commit(); d.close()

def current_max_cutoff():
 d=open_v52()
 if d is None:return 0.0
 r=d.execute("SELECT MAX(cutoff_ts) FROM v52_snapshots").fetchone(); d.close(); return sf(r[0],0.0) or 0.0

def freeze_once():
 d=core.open_research(); n=d.execute("SELECT COUNT(*) FROM v62_champions").fetchone()[0]
 if n: d.close(); return 0
 rows=[dict(r) for r in d.execute("""
 SELECT f.family,f.status,f.median_expectancy,f.median_pf,f.representative_experiment_id,
        e.stage_s,e.horizon_s,e.tp_pct,e.sl_pct,e.feature,
        r.direction,r.threshold,r.holdout_expectancy,r.holdout_profit_factor,r.holdout_win_rate
 FROM v61_family_champions f
 JOIN v60_experiments e ON e.experiment_id=f.representative_experiment_id
 JOIN v60_results r ON r.experiment_id=e.experiment_id
 WHERE f.status IN ('STRONG_REPLICATION','REPLICATED')
 ORDER BY CASE f.status WHEN 'STRONG_REPLICATION' THEN 0 ELSE 1 END,
          f.median_expectancy DESC,f.median_pf DESC
 LIMIT ?
 """,(TOP_N,)).fetchall()]
 if not rows: d.close(); return 0
 freeze_ts=time.time(); max_cut=current_max_cutoff(); made=0
 for r in rows:
  cid='C62_'+core.fingerprint({'family':r['family'],'source':r['representative_experiment_id'],'freeze':freeze_ts},'v62:')[:22]
  d.execute("""INSERT INTO v62_champions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
   (cid,r['family'],r['representative_experiment_id'],r['feature'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['direction'],r['threshold'],freeze_ts,max_cut,r['holdout_expectancy'],r['holdout_profit_factor'],r['holdout_win_rate'],freeze_ts)); made+=1
 d.commit(); d.close(); return made

def entry_quote(db,token,decision_ts):
 r=db.execute("""SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp DESC LIMIT 1""",(token,float(decision_ts))).fetchone()
 if not r:return None
 if float(decision_ts)-float(r['timestamp'])>v60.MAX_ENTRY_GAP_S:return None
 return float(r['price_sol'])

def eval_path(db,token,decision_ts,horizon,tp,sl):
 entry=entry_quote(db,token,decision_ts)
 if entry is None or entry<=0:return None
 end=float(decision_ts)+int(horizon)
 # Do not score an unfinished future horizon.
 if time.time()<end:return None
 rs=db.execute("""SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp""",(token,float(decision_ts),end)).fetchall()
 if len(rs)<v60.MIN_PATH_POINTS:return None
 prices=[float(r['price_sol']) for r in rs]; allp=[entry]+prices
 steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]; rets=[100*(p/entry-1) for p in prices]
 if any(abs(x)>v60.MAX_ABS_STEP_PCT for x in steps) or any(abs(x)>v60.MAX_ABS_PATH_RETURN_PCT for x in rets):return None
 raw=rets[-1]; reason='TIME_EXIT'
 for x in rets:
  if x>=tp: raw=tp; reason='TP_FIRST'; break
  if x<=-sl: raw=-sl; reason='SL_FIRST'; break
 return {'entry':entry,'net':raw-v59.total_cost_pct(),'raw':raw,'hit':int(reason=='TP_FIRST'),'reason':reason,'mfe':max(rets),'mae':min(rets)}

def process_champion(c):
 db=open_v52()
 if db is None:return 0
 rows=db.execute(f"""SELECT token_mint,cutoff_ts,{c['feature']} AS feature FROM v52_snapshots WHERE stage_s=? AND cutoff_ts>? AND {c['feature']} IS NOT NULL ORDER BY cutoff_ts,token_mint""",(int(c['stage_s']),float(c['frozen_max_cutoff_ts']))).fetchall()
 rd=core.open_research(); existing={(r[0],float(r[1])) for r in rd.execute("SELECT token_mint,cutoff_ts FROM v62_forward_trades WHERE champion_id=?",(c['champion_id'],)).fetchall()}; rd.close()
 made=0
 for r in rows:
  key=(str(r['token_mint']),float(r['cutoff_ts']))
  if key in existing:continue
  x=sf(r['feature'])
  if x is None or float(c['direction'])*x<float(c['threshold']):continue
  econ=eval_path(db,key[0],key[1],c['horizon_s'],c['tp_pct'],c['sl_pct'])
  if econ is None:continue
  rd=core.open_research(); rd.execute("""INSERT OR IGNORE INTO v62_forward_trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
   (c['champion_id'],key[0],key[1],x,econ['entry'],econ['net'],econ['raw'],econ['hit'],econ['reason'],econ['mfe'],econ['mae'],time.time())); rd.commit(); rd.close(); made+=1
 db.close(); return made

def pf(xs):
 g=sum(x for x in xs if x>0); l=-sum(x for x in xs if x<0)
 return g/l if l>0 else (999.0 if g>0 else None)

def max_dd(xs):
 eq=0.0; peak=0.0; dd=0.0
 for x in xs:
  eq+=x; peak=max(peak,eq); dd=min(dd,eq-peak)
 return dd

def summarize(c):
 d=core.open_research(); tr=[dict(r) for r in d.execute("SELECT * FROM v62_forward_trades WHERE champion_id=? ORDER BY cutoff_ts,token_mint",(c['champion_id'],)).fetchall()]
 db=open_v52(); eligible=0; signals=0
 if db:
  rr=db.execute(f"""SELECT {c['feature']} FROM v52_snapshots WHERE stage_s=? AND cutoff_ts>? AND {c['feature']} IS NOT NULL""",(int(c['stage_s']),float(c['frozen_max_cutoff_ts']))).fetchall(); eligible=len(rr); signals=sum(1 for r in rr if sf(r[0]) is not None and float(c['direction'])*float(r[0])>=float(c['threshold'])); db.close()
 xs=[float(r['net_return']) for r in tr]; n=len(xs); exp=statistics.mean(xs) if xs else None; med=statistics.median(xs) if xs else None; wr=sum(x>0 for x in xs)/n if n else None; p=pf(xs); hr=sum(int(r['hit']) for r in tr)/n if n else None
 status='WAITING'
 if n>=MIN_SURVIVE_TRADES: status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
 if n>=MIN_CONFIRM_TRADES: status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
 d.execute("""INSERT INTO v62_forward_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(champion_id) DO UPDATE SET family=excluded.family,eligible=excluded.eligible,signals=excluded.signals,done=excluded.done,expectancy=excluded.expectancy,median_net=excluded.median_net,win_rate=excluded.win_rate,profit_factor=excluded.profit_factor,hit_rate=excluded.hit_rate,max_drawdown=excluded.max_drawdown,status=excluded.status,updated_at=excluded.updated_at""",
  (c['champion_id'],c['family'],eligible,signals,n,exp,med,wr,p,hr,max_dd(xs) if xs else None,status,time.time())); d.commit(); d.close()

def display(new):
 d=core.open_research(); rows=[dict(r) for r in d.execute("""SELECT c.*,s.eligible,s.signals,s.done,s.expectancy,s.median_net,s.win_rate,s.profit_factor,s.hit_rate,s.max_drawdown,s.status FROM v62_champions c LEFT JOIN v62_forward_summary s USING(champion_id) ORDER BY source_expectancy DESC""").fetchall()]; d.close()
 print('\033[2J\033[H',end=''); print('='*184); print('MEMECOIN LAB — FUTURE-ONLY ECONOMIC CHAMPION ARENA V6.2'); print('='*184)
 print(f"FROZEN CHAMPIONS={len(rows)} | NEW FORWARD TRADES={new} | confirm_at={MIN_CONFIRM_TRADES} trades | costs={v59.total_cost_pct():.2f}%")
 print('Rules are immutable after freeze. Only snapshots with cutoff_ts strictly after the frozen V5.2 watermark are eligible.\n')
 for i,r in enumerate(rows,1):
  print(f"#{i} {r['family']:<22} {r.get('status') or 'WAITING':<14} feature={r['feature']:<22} stage={r['stage_s']:<3} h={r['horizon_s']:<3} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f}")
  print(f"   FROZEN dir={r['direction']:+.0f} threshold={r['threshold']:.6g} source_exp={sf(r['source_expectancy'],0):+.2f}% source_PF={sf(r['source_pf'],0):.2f}")
  print(f"   FUTURE eligible={r.get('eligible') or 0:<4} signals={r.get('signals') or 0:<4} done={r.get('done') or 0:<3} exp={sf(r.get('expectancy'),0):+.2f}% med={sf(r.get('median_net'),0):+.2f}% win={100*sf(r.get('win_rate'),0):.1f}% PF={sf(r.get('profit_factor'),0):.2f} DD={sf(r.get('max_drawdown'),0):+.2f}%")
 print('\nGuardrail: CONFIRMED is prospective paper evidence after the V6.2 freeze, not authorization for live capital. No rule is retuned from these future outcomes.')

def cycle():
 freeze_once(); d=core.open_research(); champs=[dict(r) for r in d.execute('SELECT * FROM v62_champions ORDER BY source_expectancy DESC').fetchall()]; d.close(); new=0
 for c in champs:new+=process_champion(c); summarize(c)
 display(new)

def main():
 signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
 while not STOP:
  try:cycle()
  except Exception as e:print('V6.2 error:',repr(e),flush=True)
  time.sleep(LOOP)

if __name__=='__main__':main()
