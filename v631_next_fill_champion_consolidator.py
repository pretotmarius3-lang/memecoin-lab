#!/usr/bin/env python3
"""Memecoin Lab V6.3.1 — NEXT-FILL Champion Consolidator.

Consumes completed V6.3 results without threshold retuning. Reconstructs exact
selected HOLDOUT tokens under the frozen V6.3 rule, measures overlap, fill-rate
stability and replication across stage/horizon/barrier regimes.

No V6.4 freeze is created here. This is a consolidation gate only.
"""
from __future__ import annotations

import json, math, os, signal, statistics, time
from collections import defaultdict

import v41_core as core
import v61_economic_champion_consolidator as v61
import v63_next_fill_economic_edge_engine as v63

LOOP=float(os.environ.get('MEMECOIN_V631_LOOP_S','30'))
MIN_REGIMES=int(os.environ.get('MEMECOIN_V631_MIN_REGIMES','2'))
MIN_UNIQUE_TOKENS=int(os.environ.get('MEMECOIN_V631_MIN_UNIQUE_TOKENS','20'))
MIN_FILL_RATE=float(os.environ.get('MEMECOIN_V631_MIN_FILL_RATE','0.10'))
MAX_OVERLAP=float(os.environ.get('MEMECOIN_V631_MAX_OVERLAP','0.70'))
STOP=False


def stop(*_):
    global STOP; STOP=True


def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d


def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v631_edge_instances(
      experiment_id TEXT PRIMARY KEY,family TEXT NOT NULL,feature TEXT NOT NULL,
      stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,
      direction REAL NOT NULL,threshold REAL NOT NULL,holdout_selected INTEGER NOT NULL,
      selected_tokens_json TEXT NOT NULL,holdout_expectancy REAL,holdout_pf REAL,holdout_win REAL,
      expectancy_lift REAL,hit_lift REAL,fill_rate REAL,median_fill_delay REAL,verdict TEXT NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v631_family_champions(
      family TEXT PRIMARY KEY,status TEXT NOT NULL,positive_instances INTEGER NOT NULL,positive_regimes INTEGER NOT NULL,
      independent_regimes INTEGER NOT NULL,unique_holdout_tokens INTEGER NOT NULL,
      median_expectancy REAL,worst_expectancy REAL,median_pf REAL,worst_pf REAL,median_win REAL,
      median_expectancy_lift REAL,median_hit_lift REAL,median_fill_rate REAL,worst_fill_rate REAL,
      median_fill_delay REAL,max_pair_overlap REAL,representative_experiment_id TEXT,evidence_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()


def load_rows():
    d=core.open_research(); rows=[dict(r) for r in d.execute('''
      SELECT e.*,r.* FROM v63_experiments e JOIN v63_results r USING(experiment_id)
      WHERE e.status='DONE' AND r.verdict IN ('PROMISING','WEAK')
    ''').fetchall()]; d.close(); return rows


def selected_holdout_tokens(r):
    if r.get('threshold') is None or r.get('direction') is None:return []
    data,_=v63.dataset(r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['feature'])
    out=[]; direction=float(r['direction']); th=float(r['threshold'])
    for x in data:
        if not v63.holdout(x['token_mint']):continue
        if direction*float(x['feature'])>=th:out.append(str(x['token_mint']))
    return sorted(set(out))


def jaccard(a,b):
    a=set(a); b=set(b); u=len(a|b)
    return len(a&b)/u if u else 0.0


def rebuild_instances():
    rows=load_rows(); now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        d.execute('DELETE FROM v631_edge_instances')
        for r in rows:
            toks=selected_holdout_tokens(r)
            d.execute('''INSERT INTO v631_edge_instances VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(
              r['experiment_id'],v61.FAMILY.get(r['feature'],'OTHER'),r['feature'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],
              r['direction'],r['threshold'],len(toks),core.canonical_json(toks),r['holdout_expectancy'],r['holdout_pf'],r['holdout_win'],
              r['expectancy_lift'],r['hit_lift'],r['fill_rate'],r['median_fill_delay'],r['verdict'],now))
        d.commit()
    except BaseException:
        d.rollback(); raise
    finally:d.close()
    return len(rows)


def independent_subset(rows):
    ordered=sorted(rows,key=lambda r:(sf(r['holdout_expectancy'],-1e9),sf(r['holdout_pf'],0),sf(r['fill_rate'],0)),reverse=True)
    kept=[]
    for r in ordered:
        toks=json.loads(r['selected_tokens_json'])
        if all(jaccard(toks,json.loads(k['selected_tokens_json']))<MAX_OVERLAP for k in kept):kept.append(r)
    return kept


def rebuild_families():
    d=core.open_research(); rows=[dict(r) for r in d.execute('SELECT * FROM v631_edge_instances').fetchall()]; d.close()
    by=defaultdict(list)
    for r in rows:by[r['family']].append(r)
    now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
      d.execute('DELETE FROM v631_family_champions')
      for fam,rs in by.items():
        pos=[r for r in rs if sf(r['holdout_expectancy'],-1)>0 and sf(r['holdout_pf'],0)>1 and sf(r['expectancy_lift'],-1)>0 and sf(r['fill_rate'],0)>=MIN_FILL_RATE]
        indep=independent_subset(pos); toks=set()
        for r in indep:toks.update(json.loads(r['selected_tokens_json']))
        exps=[sf(r['holdout_expectancy']) for r in indep if sf(r['holdout_expectancy']) is not None]
        pfs=[sf(r['holdout_pf']) for r in indep if sf(r['holdout_pf']) is not None]
        wins=[sf(r['holdout_win']) for r in indep if sf(r['holdout_win']) is not None]
        lifts=[sf(r['expectancy_lift']) for r in indep if sf(r['expectancy_lift']) is not None]
        hits=[sf(r['hit_lift']) for r in indep if sf(r['hit_lift']) is not None]
        fills=[sf(r['fill_rate']) for r in indep if sf(r['fill_rate']) is not None]
        delays=[sf(r['median_fill_delay']) for r in indep if sf(r['median_fill_delay']) is not None]
        regimes={(r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct']) for r in pos}
        maxov=0.0
        for i in range(len(pos)):
          a=json.loads(pos[i]['selected_tokens_json'])
          for j in range(i+1,len(pos)):maxov=max(maxov,jaccard(a,json.loads(pos[j]['selected_tokens_json'])))
        status='DISCOVERY'
        if len(indep)>=MIN_REGIMES and len(toks)>=MIN_UNIQUE_TOKENS and exps and min(exps)>0 and pfs and min(pfs)>1 and fills and min(fills)>=MIN_FILL_RATE:
            status='REPLICATED'
        if status=='REPLICATED' and len(indep)>=3 and len(toks)>=30 and statistics.median(exps)>=1 and statistics.median(pfs)>=1.25 and statistics.median(fills)>=0.15:
            status='STRONG_REPLICATION'
        rep=max(pos,key=lambda r:(sf(r['holdout_expectancy'],-1e9),sf(r['holdout_pf'],0),sf(r['fill_rate'],0)))['experiment_id'] if pos else None
        evidence={'instances':[r['experiment_id'] for r in pos],'independent_instances':[r['experiment_id'] for r in indep],
                  'regimes':[list(x) for x in sorted(regimes)],'unique_tokens':len(toks),'max_overlap':maxov,'min_fill_rate':MIN_FILL_RATE}
        d.execute('''INSERT INTO v631_family_champions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(
          fam,status,len(pos),len(regimes),len(indep),len(toks),statistics.median(exps) if exps else None,min(exps) if exps else None,
          statistics.median(pfs) if pfs else None,min(pfs) if pfs else None,statistics.median(wins) if wins else None,
          statistics.median(lifts) if lifts else None,statistics.median(hits) if hits else None,statistics.median(fills) if fills else None,
          min(fills) if fills else None,statistics.median(delays) if delays else None,maxov,rep,core.canonical_json(evidence),now))
      d.commit()
    except BaseException:
      d.rollback(); raise
    finally:d.close()
    return len(by)


def display(ninst,nfam):
    d=core.open_research(); rows=[dict(r) for r in d.execute('''SELECT * FROM v631_family_champions ORDER BY
      CASE status WHEN 'STRONG_REPLICATION' THEN 0 WHEN 'REPLICATED' THEN 1 ELSE 2 END,
      median_expectancy DESC,median_pf DESC''').fetchall()]; d.close()
    counts=defaultdict(int)
    for r in rows:counts[r['status']]+=1
    print('\033[2J\033[H',end=''); print('='*184); print('MEMECOIN LAB — NEXT-FILL CHAMPION CONSOLIDATOR V6.3.1'); print('='*184)
    print(f"EDGE INSTANCES={ninst} | FAMILIES={nfam} | STRONG={counts['STRONG_REPLICATION']} REPLICATED={counts['REPLICATED']} DISCOVERY={counts['DISCOVERY']}")
    print(f"Gates: overlap<{MAX_OVERLAP:.0%} | unique_tokens>={MIN_UNIQUE_TOKENS} | fill_rate>={MIN_FILL_RATE:.0%} | no threshold retuning\n")
    for i,r in enumerate(rows,1):
      print(f"#{i:02d} {r['status']:<19} {r['family']:<22} instances={r['positive_instances']:<3} regimes={r['positive_regimes']:<3} independent={r['independent_regimes']:<2} unique_tokens={r['unique_holdout_tokens']:<3}")
      print(f"     exp med={sf(r['median_expectancy'],0):+.2f}% worst={sf(r['worst_expectancy'],0):+.2f}% | PF med={sf(r['median_pf'],0):.2f} worst={sf(r['worst_pf'],0):.2f} | fill med={100*sf(r['median_fill_rate'],0):.1f}% worst={100*sf(r['worst_fill_rate'],0):.1f}% delay={sf(r['median_fill_delay'],0):.1f}s")
      print(f"     lift={sf(r['median_expectancy_lift'],0):+.2f}% | overlap={100*sf(r['max_pair_overlap'],0):.1f}% | rep={r['representative_experiment_id']}")
    print('\nGuardrail: V6.3.1 consolidates historical holdout evidence under NEXT-FILL execution. It does not create a future-only champion by itself.')


def cycle():
    n=rebuild_instances(); f=rebuild_families(); display(n,f)


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
      try:cycle()
      except Exception as e:print('V6.3.1 error:',repr(e),flush=True)
      time.sleep(LOOP)

if __name__=='__main__':main()
