#!/usr/bin/env python3
"""Memecoin Lab V6.1 — Economic Champion Consolidator.

Consumes finished V6.0 results without retuning thresholds.
Reconstructs the exact HOLDOUT token sets selected by each frozen V6.0 rule,
measures overlap/redundancy, groups features into economic families, and scores
replication across distinct stage/horizon/barrier regimes.

No new threshold search. No live trading. No promotion to live strategy.
"""
from __future__ import annotations

import json, math, os, signal, statistics, time
from collections import defaultdict

import v41_core as core
import v60_economic_edge_discovery_engine as v60

LOOP=float(os.environ.get("MEMECOIN_V61_LOOP_S","10"))
MIN_REGIMES=int(os.environ.get("MEMECOIN_V61_MIN_REGIMES","2"))
MIN_UNIQUE_TOKENS=int(os.environ.get("MEMECOIN_V61_MIN_UNIQUE_TOKENS","20"))
MAX_OVERLAP_FOR_INDEPENDENT=float(os.environ.get("MEMECOIN_V61_MAX_OVERLAP","0.70"))
STOP=False

FAMILY={
 "swaps":"ACTIVITY",
 "buy_ratio":"FLOW",
 "gross_sol":"CAPITAL_FLOW",
 "net_sol":"CAPITAL_FLOW",
 "unique_wallets":"WALLET_BREADTH",
 "repeat_wallet_ratio":"WALLET_STRUCTURE",
 "wallet_hhi":"WALLET_CONCENTRATION",
 "wallet_top1_share":"WALLET_CONCENTRATION",
 "avg_trade_sol":"TRADE_SIZE",
 "max_trade_sol":"TRADE_SIZE",
 "trade_hhi":"TRADE_DISTRIBUTION",
 "top1_trade_share":"TRADE_DISTRIBUTION",
 "return_pct":"PRICE_ACTION",
 "range_pct":"PRICE_ACTION",
 "flow_velocity":"FLOW_DYNAMICS",
 "flow_acceleration":"FLOW_DYNAMICS",
 "buy_ratio_delta":"FLOW_DYNAMICS",
 "price_velocity":"PRICE_DYNAMICS",
}

def stop(*_):
 global STOP; STOP=True

def sf(x,d=None):
 try:
  v=float(x); return v if math.isfinite(v) else d
 except Exception:return d

def init():
 d=core.open_research(); d.executescript("""
 CREATE TABLE IF NOT EXISTS v61_edge_instances(
  experiment_id TEXT PRIMARY KEY,
  family TEXT NOT NULL,
  feature TEXT NOT NULL,
  stage_s INTEGER NOT NULL,
  horizon_s INTEGER NOT NULL,
  tp_pct REAL NOT NULL,
  sl_pct REAL NOT NULL,
  direction REAL NOT NULL,
  threshold REAL NOT NULL,
  holdout_selected INTEGER NOT NULL,
  selected_tokens_json TEXT NOT NULL,
  holdout_expectancy REAL,
  holdout_profit_factor REAL,
  holdout_win_rate REAL,
  expectancy_lift REAL,
  hit_rate_lift REAL,
  verdict TEXT NOT NULL,
  updated_at REAL NOT NULL);

 CREATE TABLE IF NOT EXISTS v61_family_champions(
  family TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  promising_instances INTEGER NOT NULL,
  positive_regimes INTEGER NOT NULL,
  independent_regimes INTEGER NOT NULL,
  unique_holdout_tokens INTEGER NOT NULL,
  median_expectancy REAL,
  worst_expectancy REAL,
  median_pf REAL,
  worst_pf REAL,
  median_win_rate REAL,
  median_expectancy_lift REAL,
  median_hit_lift REAL,
  max_pair_overlap REAL,
  representative_experiment_id TEXT,
  evidence_json TEXT NOT NULL,
  updated_at REAL NOT NULL);

 CREATE TABLE IF NOT EXISTS v61_state(
  key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
 """); d.commit(); d.close()

def load_v60():
 d=core.open_research(); rows=[dict(r) for r in d.execute("""
  SELECT e.*,r.* FROM v60_experiments e JOIN v60_results r USING(experiment_id)
  WHERE e.status='DONE' AND r.verdict IN ('PROMISING','WEAK')
 """).fetchall()]; d.close(); return rows

def selected_holdout_tokens(row):
 if row.get('threshold') is None or row.get('direction') is None:return []
 data=v60.dataset(row['stage_s'],row['horizon_s'],row['tp_pct'],row['sl_pct'],row['feature'])
 out=[]; direction=float(row['direction']); th=float(row['threshold'])
 for r in data:
  if not v60.holdout(r['token_mint']):continue
  if direction*float(r['feature'])>=th:out.append(str(r['token_mint']))
 return sorted(set(out))

def jaccard(a,b):
 a=set(a); b=set(b)
 if not a and not b:return 0.0
 u=len(a|b)
 return len(a&b)/u if u else 0.0

def rebuild_instances():
 rows=load_v60(); now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
 try:
  d.execute('DELETE FROM v61_edge_instances')
  for r in rows:
   toks=selected_holdout_tokens(r)
   d.execute("""INSERT INTO v61_edge_instances VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    (r['experiment_id'],FAMILY.get(r['feature'],'OTHER'),r['feature'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],
     r['direction'],r['threshold'],len(toks),core.canonical_json(toks),r['holdout_expectancy'],r['holdout_profit_factor'],
     r['holdout_win_rate'],r['expectancy_lift'],r['hit_rate_lift'],r['verdict'],now))
  d.commit()
 except BaseException:
  d.rollback(); raise
 finally:d.close()
 return len(rows)

def independent_subset(rows):
 # Greedy: strongest first, then reject near-duplicate selected-token sets.
 ordered=sorted(rows,key=lambda r:(sf(r['holdout_expectancy'],-1e9),sf(r['holdout_profit_factor'],0)),reverse=True)
 kept=[]
 for r in ordered:
  toks=json.loads(r['selected_tokens_json'])
  if all(jaccard(toks,json.loads(k['selected_tokens_json']))<MAX_OVERLAP_FOR_INDEPENDENT for k in kept):
   kept.append(r)
 return kept

def rebuild_families():
 d=core.open_research(); rows=[dict(r) for r in d.execute('SELECT * FROM v61_edge_instances').fetchall()]; d.close()
 by=defaultdict(list)
 for r in rows:by[r['family']].append(r)
 now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
 try:
  d.execute('DELETE FROM v61_family_champions')
  for fam,rs in by.items():
   pos=[r for r in rs if sf(r['holdout_expectancy'],-1)<=0 and False]
   # positive economic instances only
   pos=[r for r in rs if sf(r['holdout_expectancy'],-1)>0 and sf(r['holdout_profit_factor'],0)>1 and sf(r['expectancy_lift'],-1)>0]
   indep=independent_subset(pos)
   toks=set();
   for r in indep:toks.update(json.loads(r['selected_tokens_json']))
   exps=[sf(r['holdout_expectancy']) for r in indep if sf(r['holdout_expectancy']) is not None]
   pfs=[sf(r['holdout_profit_factor']) for r in indep if sf(r['holdout_profit_factor']) is not None]
   wins=[sf(r['holdout_win_rate']) for r in indep if sf(r['holdout_win_rate']) is not None]
   lifts=[sf(r['expectancy_lift']) for r in indep if sf(r['expectancy_lift']) is not None]
   hits=[sf(r['hit_rate_lift']) for r in indep if sf(r['hit_rate_lift']) is not None]
   maxov=0.0
   for i in range(len(pos)):
    a=json.loads(pos[i]['selected_tokens_json'])
    for j in range(i+1,len(pos)):
     maxov=max(maxov,jaccard(a,json.loads(pos[j]['selected_tokens_json'])))
   regimes={(r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct']) for r in pos}
   status='DISCOVERY'
   if len(indep)>=MIN_REGIMES and len(toks)>=MIN_UNIQUE_TOKENS and exps and min(exps)>0 and pfs and min(pfs)>1:
    status='REPLICATED'
   if status=='REPLICATED' and len(indep)>=3 and len(toks)>=30 and statistics.median(exps)>=1.0 and statistics.median(pfs)>=1.25:
    status='STRONG_REPLICATION'
   rep=max(pos,key=lambda r:(sf(r['holdout_expectancy'],-1e9),sf(r['holdout_profit_factor'],0)))['experiment_id'] if pos else None
   evidence={'instances':[r['experiment_id'] for r in pos],'independent_instances':[r['experiment_id'] for r in indep],
    'regimes':[list(x) for x in sorted(regimes)],'unique_tokens':len(toks),'max_overlap':maxov,
    'overlap_threshold':MAX_OVERLAP_FOR_INDEPENDENT}
   d.execute("""INSERT INTO v61_family_champions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    (fam,status,len(pos),len(regimes),len(indep),len(toks),statistics.median(exps) if exps else None,min(exps) if exps else None,
     statistics.median(pfs) if pfs else None,min(pfs) if pfs else None,statistics.median(wins) if wins else None,
     statistics.median(lifts) if lifts else None,statistics.median(hits) if hits else None,maxov,rep,core.canonical_json(evidence),now))
  d.commit()
 except BaseException:
  d.rollback(); raise
 finally:d.close()
 return len(by)

def display(ninst,nfam):
 d=core.open_research(); rows=[dict(r) for r in d.execute("""SELECT * FROM v61_family_champions
  ORDER BY CASE status WHEN 'STRONG_REPLICATION' THEN 0 WHEN 'REPLICATED' THEN 1 ELSE 2 END,
  median_expectancy DESC,median_pf DESC""").fetchall()]; d.close()
 counts=defaultdict(int)
 for r in rows:counts[r['status']]+=1
 print('\033[2J\033[H',end=''); print('='*176); print('MEMECOIN LAB — ECONOMIC CHAMPION CONSOLIDATOR V6.1'); print('='*176)
 print(f"EDGE INSTANCES={ninst} | FAMILIES={nfam} | STRONG={counts['STRONG_REPLICATION']} REPLICATED={counts['REPLICATED']} DISCOVERY={counts['DISCOVERY']}")
 print(f"Independence gate: selected-token Jaccard overlap < {MAX_OVERLAP_FOR_INDEPENDENT:.0%} | minimum unique tokens={MIN_UNIQUE_TOKENS} | no threshold retuning\n")
 print('ECONOMIC FAMILY CHAMPIONS')
 for i,r in enumerate(rows,1):
  print(f"#{i:02d} {r['status']:<19} {r['family']:<22} instances={r['promising_instances']:<3} regimes={r['positive_regimes']:<3} independent={r['independent_regimes']:<2} unique_tokens={r['unique_holdout_tokens']:<3}")
  print(f"     exp median={sf(r['median_expectancy'],0):+.2f}% worst={sf(r['worst_expectancy'],0):+.2f}% | PF median={sf(r['median_pf'],0):.2f} worst={sf(r['worst_pf'],0):.2f} | win_med={100*sf(r['median_win_rate'],0):.1f}%")
  print(f"     lift_exp={sf(r['median_expectancy_lift'],0):+.2f}% hit_lift={sf(r['median_hit_lift'],0):.2f}x | max_token_overlap={100*sf(r['max_pair_overlap'],0):.1f}% | rep={r['representative_experiment_id']}")
 print('\nGuardrail: V6.1 consolidates already-consumed V6.0 holdout evidence. REPLICATED means cross-regime historical replication after overlap control, not future-only proof.')

def cycle():
 n=rebuild_instances(); f=rebuild_families(); display(n,f)
 d=core.open_research(); d.execute("""INSERT INTO v61_state(key,value_json,updated_at) VALUES('latest',?,?)
 ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
 (core.canonical_json({'instances':n,'families':f,'updated_at':time.time()}),time.time())); d.commit(); d.close()

def main():
 signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
 while not STOP:
  try:cycle()
  except Exception as e:print('V6.1 error:',repr(e),flush=True)
  time.sleep(LOOP)

if __name__=='__main__':main()
