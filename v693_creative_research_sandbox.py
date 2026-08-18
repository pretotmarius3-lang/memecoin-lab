#!/usr/bin/env python3
"""Memecoin Lab V6.9.3 — Creative Research Sandbox.

Purpose:
- read scientific verdicts/journal from the isolated V6.9.2 intelligence DB;
- generate falsifiable research hypotheses and counter-explanations;
- NEVER mutate scientific databases, frozen rules, forward events, verdicts, or classifications;
- write only to its own creative DB.

This is an idea generator, not a validator. Any idea promoted from here must start a
new independent research chain, get frozen before seeing future data, and pass a fresh
future-only arena before it can influence scientific conclusions.
"""
from __future__ import annotations
import hashlib, json, math, os, signal, sqlite3, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
SCI_DB=Path(os.environ.get('MEMECOIN_V69_DB',ROOT/'v69_intelligence.db'))
RDB=Path(os.environ.get('MEMECOIN_RESEARCH_V41_DB',ROOT/'research_v4_1.db'))
CREATIVE_DB=Path(os.environ.get('MEMECOIN_V693_DB',ROOT/'v693_creative.db'))
LOOP=float(os.environ.get('MEMECOIN_V693_LOOP_S','30'))
STOP=False

def stop(*_):
    global STOP;STOP=True

def sf(x,d=0.0):
    try:
        v=float(x);return v if math.isfinite(v) else d
    except:return d

def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=5);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=5000');return d

def cw():
    d=sqlite3.connect(CREATIVE_DB,timeout=10);d.row_factory=sqlite3.Row;d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=NORMAL');d.execute('PRAGMA busy_timeout=10000');return d

def init():
    d=cw();d.executescript('''
    CREATE TABLE IF NOT EXISTS creative_hypotheses(
      hypothesis_id TEXT PRIMARY KEY,created_at REAL NOT NULL,updated_at REAL NOT NULL,
      strategy_id TEXT NOT NULL,label TEXT NOT NULL,scientific_evidence TEXT NOT NULL,
      scientific_verdict TEXT NOT NULL,hypothesis_type TEXT NOT NULL,priority REAL NOT NULL,
      hypothesis TEXT NOT NULL,rationale TEXT NOT NULL,falsification_test TEXT NOT NULL,
      proposed_next_experiment TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'OPEN',
      source_snapshot_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS creative_cycles(
      cycle_id TEXT PRIMARY KEY,created_at REAL NOT NULL,scientific_snapshot_hash TEXT NOT NULL,
      hypotheses_generated INTEGER NOT NULL,notes TEXT NOT NULL);
    ''');d.commit();d.close()

def scientific_rows():
    if not SCI_DB.exists():return []
    d=ro(SCI_DB)
    try:xs=[dict(x) for x in d.execute('SELECT * FROM v69_strategy_intelligence ORDER BY role,label').fetchall()]
    except:xs=[]
    d.close();return xs

def mk(row,typ,priority,hyp,rationale,test,next_exp):
    snap={k:row.get(k) for k in ('strategy_id','label','role','evidence','done','signals','expectancy','profit_factor','win_rate','fill_rate','max_drawdown','alpha_vs_r64','pf_vs_r64','token_overlap_r64','signal_overlap_r64','verdict')}
    hid=hashlib.sha256((str(row.get('strategy_id'))+'|'+typ+'|'+hyp).encode()).hexdigest()[:24]
    return {'hypothesis_id':hid,'strategy_id':str(row.get('strategy_id')),'label':str(row.get('label')),'scientific_evidence':str(row.get('evidence')),'scientific_verdict':str(row.get('verdict')),'hypothesis_type':typ,'priority':priority,'hypothesis':hyp,'rationale':rationale,'falsification_test':test,'proposed_next_experiment':next_exp,'snapshot':snap}

def ideas(row):
    out=[];role=str(row.get('role'));label=str(row.get('label'));done=int(row.get('done') or 0);exp=sf(row.get('expectancy'));pf=sf(row.get('profit_factor'));fill=sf(row.get('fill_rate'));dd=sf(row.get('max_drawdown'));tok=sf(row.get('token_overlap_r64'));sig=sf(row.get('signal_overlap_r64'))
    if role=='CONTROL':
        if dd<=-50:
            out.append(mk(row,'RISK_STRUCTURE',8.0,'R64 edge may be real but concentrated in adverse path clusters rather than uniformly risky.','Confirmed positive expectancy coexists with large drawdown.','Predeclare drawdown-cluster features, split only on information available before signal, and verify whether one subgroup retains positive expectancy on a fresh holdout.','New risk-regime discovery branch; R64 itself remains untouched.'))
        if fill>0.70:
            out.append(mk(row,'EXECUTION_BASELINE',5.0,'R64 can serve as the execution-quality control for future strategies.','Its fill rate is materially higher than current challengers.','Compare challenger fill failure causes against R64 using the same timestamp and NEXT-FILL machinery.','Execution diagnostic only; no rule changes.'))
        return out
    if done>=10 and exp<=0:
        if sig<0.25:
            out.append(mk(row,'REGIME_DEPENDENCE',9.0,f'{label} may encode a genuinely different market regime, but the discovered edge may exist only in a narrower pre-signal context.','Signal overlap with R64 is low while forward expectancy is weak/negative.','Define candidate context variables without using future outcomes; require the same sign of conditional effect across train/holdout before any new freeze.','Create a new context-interaction discovery experiment, never retune this frozen challenger.'))
        out.append(mk(row,'SELECTION_BIAS',9.5,f'{label} discovery performance may have been amplified by multiple-testing and small holdout selection.','Strong discovery/holdout evidence did not survive early forward data.','Replay the candidate-selection procedure with permutation/null labels and measure how often equal-or-better holdout scores appear by chance.','Build a multiple-testing / false-discovery audit of V6.7.'))
        if fill<0.25:
            out.append(mk(row,'EXECUTION_FILTERING',8.5,f'{label} may be selecting states that are statistically interesting but structurally hard to fill within 15s.','Forward fill rate is low.','Measure NO_FILL versus feature quantiles and liquidity proxies without changing the frozen rule; require a predeclared new candidate to improve fill and retain alpha.','Separate execution-feasibility discovery branch.'))
        if dd<=-40:
            out.append(mk(row,'TAIL_RISK',7.5,f'{label} may contain asymmetric tail losses that the mean holdout masked.','Forward drawdown is already severe relative to sample size.','Audit return distribution, loss clustering, MFE/MAE and exit reasons; test whether a pre-signal risk feature predicts tail membership out of sample.','Tail-risk diagnostics, then independent candidate generation if warranted.'))
    elif done>=5 and exp>0 and pf>1:
        out.append(mk(row,'EDGE_REPLICATION',7.0,f'{label} may be a distinct positive edge worth continued untouched observation.','Early forward expectancy and PF are positive.','Do nothing to the rule; require 30 DONE and compare overlap-adjusted portfolio contribution to R64.','Continue frozen arena only.'))
    else:
        out.append(mk(row,'INSUFFICIENT_EVIDENCE',3.0,f'{label} should not be explained yet because the forward sample is too small.','Current evidence level is insufficient for a stable causal story.','Wait for the next predefined DONE milestone before generating stronger claims.','No experiment change.'))
    if tok<0.50 and sig<0.50:
        out.append(mk(row,'DIVERSIFICATION',6.5,f'{label} appears behaviorally distinct from R64 even if its standalone alpha is weak.','Token and/or signal overlap with R64 is low.','If it eventually confirms positive, test equal-risk portfolio combinations on a separate frozen portfolio arena; if it fails, do not include it merely for low correlation.','Conditional future V7 portfolio test only after confirmation.'))
    return out

def write_cycle(rows):
    allideas=[]
    for r in rows:allideas.extend(ideas(r))
    snap_hash=hashlib.sha256(json.dumps(rows,sort_keys=True,default=str).encode()).hexdigest()[:24]
    now=time.time();d=cw()
    for x in allideas:
        d.execute('''INSERT INTO creative_hypotheses(hypothesis_id,created_at,updated_at,strategy_id,label,scientific_evidence,scientific_verdict,hypothesis_type,priority,hypothesis,rationale,falsification_test,proposed_next_experiment,status,source_snapshot_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)
          ON CONFLICT(hypothesis_id) DO UPDATE SET updated_at=excluded.updated_at,scientific_evidence=excluded.scientific_evidence,scientific_verdict=excluded.scientific_verdict,priority=excluded.priority,rationale=excluded.rationale,falsification_test=excluded.falsification_test,proposed_next_experiment=excluded.proposed_next_experiment,source_snapshot_json=excluded.source_snapshot_json''',(
          x['hypothesis_id'],now,now,x['strategy_id'],x['label'],x['scientific_evidence'],x['scientific_verdict'],x['hypothesis_type'],x['priority'],x['hypothesis'],x['rationale'],x['falsification_test'],x['proposed_next_experiment'],json.dumps(x['snapshot'],sort_keys=True)))
    cid=hashlib.sha256((snap_hash+'|v693').encode()).hexdigest()[:24]
    d.execute('INSERT OR IGNORE INTO creative_cycles VALUES(?,?,?,?,?)',(cid,now,snap_hash,len(allideas),'Creative-only cycle; no scientific mutation.'))
    d.commit();d.close();return allideas

def display(rows,ideas_):
    print('\033[2J\033[H',end='');print('='*178);print('MEMECOIN LAB — CREATIVE RESEARCH SANDBOX V6.9.3');print('='*178)
    print(f'SCIENTIFIC INPUT={SCI_DB} (READ ONLY) | CREATIVE OUTPUT={CREATIVE_DB}')
    print('Authority boundary: hypotheses can propose NEW experiments only; they cannot alter scientific verdicts or frozen rules.\n')
    for r in rows:
        print(f"{r.get('label','?'):<28} SCIENCE={r.get('evidence','?'):<20} verdict={r.get('verdict','?')} DONE={r.get('done',0)} exp={sf(r.get('expectancy')):+.2f}% PF={sf(r.get('profit_factor')):.2f}")
        hs=sorted([x for x in ideas_ if x['strategy_id']==str(r.get('strategy_id'))],key=lambda z:-z['priority'])
        for x in hs[:4]:
            print(f"  [{x['priority']:>4.1f}] {x['hypothesis_type']}: {x['hypothesis']}")
            print(f"         TEST: {x['falsification_test']}")
    print('\nGuardrail: CREATIVE != EVIDENCE. Promotion requires a new predeclared research chain + fresh future-only validation.')

def main():
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init()
    print(f'V6.9.3 started | science={SCI_DB} | creative={CREATIVE_DB}',flush=True)
    while not STOP:
        try:
            rs=scientific_rows();hs=write_cycle(rs);display(rs,hs)
        except Exception as e:print('V6.9.3 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__':main()
