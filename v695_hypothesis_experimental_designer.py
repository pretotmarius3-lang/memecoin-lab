#!/usr/bin/env python3
"""MEMECOIN LAB — HYPOTHESIS EXPERIMENTAL DESIGNER V6.9.5

Turns creative hypotheses into immutable experiment PROPOSALS only.
It never launches experiments, changes frozen rules, or writes scientific DBs.
"""
from __future__ import annotations
import hashlib, json, sqlite3, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
CREATIVE=ROOT/'v693_creative.db'
OUT=ROOT/'v695_experimental_design.db'

SCHEMAS={
 'SELECTION_BIAS': ('NULL_SELECTION_AUDIT','Estimate false-discovery pressure from candidate selection.',
  ['freeze candidate universe and scoring procedure','generate permutation/null outcome labels','rerun identical selector without retuning','measure frequency of equal-or-better selected scores'],
  'P(null-selected score >= observed score) and rank distribution'),
 'REGIME_DEPENDENCE': ('PRE_SIGNAL_REGIME_REPLICATION','Test whether edge is conditional on pre-signal context.',
  ['define context variables using pre-signal information only','freeze bins/interactions before outcome evaluation','require sign consistency across train and holdout','send surviving specification to a new future-only freeze'],
  'conditional expectancy/PF with minimum sample counts and sign consistency'),
 'TAIL_RISK': ('TAIL_STRUCTURE_AUDIT','Test whether losses cluster in identifiable pre-signal states.',
  ['freeze loss/tail definitions','audit return distribution, MAE/MFE and loss clustering','search only pre-signal predictors of tail membership','validate any risk split on untouched data'],
  'tail loss rate, CVaR proxy, DD contribution and retained expectancy'),
 'DIVERSIFICATION': ('PORTFOLIO_ORTHOGONALITY_AUDIT','Measure incremental value versus R64 without rescuing a failed edge.',
  ['require challenger standalone positive confirmation first','freeze equal-risk combination rule','measure token/signal/return overlap','evaluate combined portfolio on a separate future-only arena'],
  'portfolio expectancy/PF/DD and marginal DD-adjusted contribution'),
 'EDGE_REPLICATION': ('IMMUTABLE_REPLICATION','Continue untouched prospective replication.',
  ['do not alter threshold/direction/stage/horizon','accumulate to declared DONE threshold','compare against R64 at milestone','only then decide whether a portfolio experiment is warranted'],
  'DONE, expectancy, PF, fill, DD, alpha vs R64'),
 'RISK_STRUCTURE': ('R64_RISK_CLUSTER_AUDIT','Explain control drawdown without modifying the confirmed control.',
  ['predeclare drawdown-cluster definition','construct features available strictly before signal','freeze subgroup hypotheses','test subgroup stability on fresh holdout before any new rule'],
  'cluster frequency, loss concentration, subgroup expectancy/PF/DD'),
 'EXECUTION_BASELINE': ('EXECUTION_QUALITY_BENCHMARK','Use R64 as common execution-quality reference.',
  ['freeze identical timestamp/NEXT-FILL rules','classify no-fill/sparse/anomaly causes identically','compare challengers with R64','do not change strategy verdicts from execution audit alone'],
  'fill rate, delay distribution, no-fill/sparse/anomaly rates')
}

def ro(path):
    return sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=10)

def init():
    c=sqlite3.connect(OUT,timeout=10); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA busy_timeout=10000')
    c.execute('''CREATE TABLE IF NOT EXISTS experiment_proposals(
      proposal_id TEXT PRIMARY KEY, created_at REAL NOT NULL, strategy_id TEXT, label TEXT, hypothesis_type TEXT NOT NULL,
      priority REAL, experiment_class TEXT NOT NULL, objective TEXT NOT NULL, protocol_json TEXT NOT NULL,
      success_metric TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PROPOSED', source_hypothesis_id TEXT, source_snapshot TEXT NOT NULL)''')
    c.commit(); c.close()

def tables(c): return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}

def cols(c,t): return [r[1] for r in c.execute(f'PRAGMA table_info({t})')]

def read_hypotheses():
    c=ro(CREATIVE); ts=tables(c)
    # tolerate evolving creative schemas
    candidates=[t for t in ts if 'hypoth' in t.lower()]
    if not candidates: c.close(); return []
    t=sorted(candidates)[0]; names=cols(c,t); rows=[]
    for raw in c.execute(f'SELECT * FROM {t}'):
        d=dict(zip(names,raw)); typ=str(d.get('hypothesis_type') or d.get('type') or d.get('category') or '').upper()
        if typ not in SCHEMAS: continue
        rows.append({
          'id':str(d.get('hypothesis_id') or d.get('id') or ''), 'strategy_id':str(d.get('strategy_id') or ''),
          'label':str(d.get('label') or d.get('strategy_label') or d.get('strategy_id') or ''), 'type':typ,
          'priority':float(d.get('priority') or d.get('score') or 0), 'hypothesis':str(d.get('hypothesis') or d.get('text') or ''),
          'test':str(d.get('test') or d.get('proposed_test') or '')})
    c.close(); return rows

def build(h):
    cls,obj,steps,metric=SCHEMAS[h['type']]
    snap=json.dumps(h,sort_keys=True,separators=(',',':'))
    pid='P695_'+hashlib.sha1((h['strategy_id']+'|'+h['type']+'|'+snap).encode()).hexdigest()[:20]
    protocol={'authority':'PROPOSAL_ONLY','steps':steps,'forbidden':['retune existing frozen rule','reuse future outcomes as training labels for the same freeze','promote without independent validation']}
    return pid,cls,obj,protocol,metric,snap

def cycle():
    hs=read_hypotheses(); now=time.time(); c=sqlite3.connect(OUT,timeout=10); c.execute('PRAGMA busy_timeout=10000')
    proposals=[]
    for h in hs:
        pid,cls,obj,protocol,metric,snap=build(h)
        c.execute('''INSERT OR IGNORE INTO experiment_proposals(proposal_id,created_at,strategy_id,label,hypothesis_type,priority,experiment_class,objective,protocol_json,success_metric,status,source_hypothesis_id,source_snapshot)
                     VALUES(?,?,?,?,?,?,?,?,?,?,\'PROPOSED\',?,?)''',(pid,now,h['strategy_id'],h['label'],h['type'],h['priority'],cls,obj,json.dumps(protocol),metric,h['id'],snap))
        proposals.append((h,pid,cls,obj,protocol,metric))
    c.commit(); c.close()
    print('\n'+'='*178); print('MEMECOIN LAB — HYPOTHESIS EXPERIMENTAL DESIGNER V6.9.5'); print('='*178)
    print(f'CREATIVE INPUT={CREATIVE} READ-ONLY | DESIGN OUTPUT={OUT} | hypotheses={len(hs)}')
    print('Authority boundary: DESIGN ONLY. No experiment is launched and no scientific/frozen state is modified.\n')
    for h,pid,cls,obj,p,m in sorted(proposals,key=lambda x:-x[0]['priority']):
        print(f"[{h['priority']:4.1f}] {h['label'] or h['strategy_id']} // {h['type']} -> {cls}  id={pid}")
        print(f'  OBJECTIVE: {obj}')
        for i,s in enumerate(p['steps'],1): print(f'  {i}. {s}')
        print(f'  SUCCESS METRIC: {m}')
        print('  STATUS: PROPOSED — requires explicit promotion into a separate predeclared research branch.\n')
    print('Guardrail: hypothesis -> design != evidence. Only a separately frozen experiment can produce evidence.',flush=True)

def main():
    init(); print(f'V6.9.5 started | read={CREATIVE} | write={OUT}',flush=True)
    while True:
        try: cycle()
        except Exception as e: print('V6.9.5 error:',repr(e),flush=True)
        time.sleep(20)
if __name__=='__main__': main()
