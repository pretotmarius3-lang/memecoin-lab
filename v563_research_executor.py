#!/usr/bin/env python3
"""Memecoin Lab V5.6.3 — Research Executor.

Consumes V5.6.2 research-portfolio actions and turns them into bounded V4.9 SIDE
experiments. Every batch is synchronously passed through V5.6.1 Diversity Gate
before V4.9 can evaluate it.

Books executed:
- CHAMPION_EXPLOITATION: map nearby stage/horizon around confirmed champions
- FALSIFICATION: sign-flip / target-transfer stress tests around champions
- NOVELTY_SEARCH: seed under-covered feature/stage/horizon/target cells

Guardrails:
- never edits V5.5 frozen rules/evidence
- never promotes directly to PASS
- all generated work enters V4.9 as exploratory SIDE evidence
- V5.6.1 can block redundant READY work immediately
- bounded experiments per action/cycle
- no trading/signing
"""
from __future__ import annotations
import json, os, signal, time
from collections import defaultdict

import v41_core as core
import v49_recursive_lab as v49
import v561_diversity_gate as gate

LOOP=float(os.environ.get('MEMECOIN_V563_LOOP_S','3'))
MAX_ACTIONS=int(os.environ.get('MEMECOIN_V563_MAX_ACTIONS','8'))
MAX_PER_ACTION=int(os.environ.get('MEMECOIN_V563_MAX_PER_ACTION','4'))
STOP=False

FAMILY_FEATURES={
 'ACTIVITY_TRADING':('swaps','unique_wallets','avg_trade_sol','max_trade_sol'),
 'PRICE_MOMENTUM':('return_pct','range_pct','price_velocity'),
 'WALLET_CONCENTRATION':('repeat_wallet_ratio','wallet_hhi','wallet_top1_share','trade_hhi','top1_trade_share','unique_wallets'),
 'FLOW_IMBALANCE':('net_sol','flow_velocity','flow_acceleration','buy_ratio','buy_ratio_delta','gross_sol'),
}

def stop(*_):
    global STOP; STOP=True

def init():
    v49.init_db(); gate.init()
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v563_execution_log(
      execution_id TEXT PRIMARY KEY,
      action_id TEXT NOT NULL,
      experiment_id TEXT,
      book TEXT NOT NULL,
      action_type TEXT NOT NULL,
      status TEXT NOT NULL,
      detail TEXT NOT NULL,
      created_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v563_action ON v563_execution_log(action_id);
    CREATE TABLE IF NOT EXISTS v563_state(
      key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()

def tables(d):
    return {r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}

def log(action,eid,status,detail):
    now=time.time(); xid='EX_'+core.fingerprint({'a':action['action_id'],'e':eid,'s':status,'d':detail},'v563:')[:24]
    d=core.open_research(); d.execute('''INSERT OR IGNORE INTO v563_execution_log(execution_id,action_id,experiment_id,book,action_type,status,detail,created_at)
      VALUES(?,?,?,?,?,?,?,?)''',(xid,action['action_id'],eid,action['book'],action['action_type'],status,detail,now)); d.commit(); d.close()

def existing_identity(kind,spec):
    key=gate.scientific_key(kind,spec); d=core.open_research(); names=tables(d)
    found=False
    if 'v49_side_experiments' in names:
        for r in d.execute("SELECT kind,spec_json FROM v49_side_experiments"):
            try:
                if gate.scientific_key(r['kind'],json.loads(r['spec_json']))==key: found=True; break
            except Exception: pass
    if not found and 'v55_candidates' in names:
        for r in d.execute("SELECT kind,spec_json FROM v55_candidates"):
            try:
                if gate.scientific_key(r['kind'],json.loads(r['spec_json']))==key: found=True; break
            except Exception: pass
    d.close(); return found

def seed(kind,parent,parent_feature,spec,reason,generation=2):
    stage=int(spec.get('stage',spec.get('stage1',10))); hz=int(spec['horizon']); target=str(spec['target'])
    wm=v49.latest_watermark(stage,hz,target)
    if not wm:return None,False,'NO_WATERMARK'
    eid,created=v49.insert_exp(kind,parent,parent_feature,spec,wm,reason,generation)
    return eid,created,'SEEDED' if created else 'EXISTS'

def champion_spec(candidate_id):
    d=core.open_research(); r=d.execute("SELECT kind,spec_json FROM v55_candidates WHERE candidate_id=?",(candidate_id,)).fetchone(); d.close()
    return (r['kind'],json.loads(r['spec_json'])) if r else (None,None)

def execute_champion(action,stress=False):
    _,s=champion_spec(action['subject'])
    if not s:return []
    parent=action['subject']; feature=s.get('feature') or s.get('weak') or ((s.get('features') or ['COMPOSITE'])[0])
    out=[]
    if not stress:
        if 'stage' in s:
            st=int(s['stage'])
            for ns in v49.STAGES:
                if ns==st or abs(ns-st)>50:continue
                spec=dict(s,stage=ns)
                if 'feature' in spec:
                    out.append(('TIME_NEIGHBOR',spec,'champion exploitation: map nearby stage'))
        for nh in v49.HORIZONS:
            if nh!=int(s['horizon']): out.append(('HORIZON_TRANSFER',dict(s,horizon=nh),'champion exploitation: map horizon persistence'))
    else:
        if 'feature' in s: out.append(('SIGN_FLIP',dict(s),'champion falsification: contrarian sign stress'))
        for nt in v49.TARGETS:
            if nt!=str(s['target']): out.append(('TARGET_TRANSFER',dict(s,target=nt),'champion falsification: adverse/alternate outcome transfer'))
    made=[]
    for kind,spec,why in out:
        if len(made)>=MAX_PER_ACTION:break
        eid,created,status=seed(kind,parent,feature,spec,why,3 if stress else 2)
        if eid: made.append((eid,created,status,why))
    return made

def novelty_candidates(family):
    feats=FAMILY_FEATURES.get(family,tuple(v49.FEATURES))
    # deterministic broad grid; existing scientific identities are skipped before insertion
    for target in v49.TARGETS:
      for horizon in v49.HORIZONS:
       for stage in v49.STAGES:
        for feature in feats:
            spec={'stage':stage,'horizon':horizon,'target':target,'feature':feature}
            kind='TIME_NEIGHBOR'  # V4.9 univariate evaluator; lineage records NOVELTY parent
            if not existing_identity(kind,spec): yield kind,spec

def execute_novelty(action):
    family=action['subject']; made=[]
    feats=FAMILY_FEATURES.get(family,('swaps',))
    for kind,spec in novelty_candidates(family):
        if len(made)>=MAX_PER_ACTION:break
        f=spec['feature']; parent=f'NOVELTY:{family}'
        eid,created,status=seed(kind,parent,f,spec,'portfolio novelty search: under-covered scientific cell',1)
        if eid: made.append((eid,created,status,'novel scientific cell'))
    return made

def mark_action(action_id,state,detail):
    d=core.open_research(); d.execute("UPDATE v562_actions SET state=?,updated_at=? WHERE action_id=?",(state,time.time(),action_id)); d.commit(); d.close()

def cycle():
    d=core.open_research(); names=tables(d)
    if 'v562_actions' not in names:d.close(); return {'actions':0,'seeded':0,'blocked':0,'allowed':0,'saturated':0}
    actions=[dict(r) for r in d.execute("SELECT * FROM v562_actions WHERE state='OPEN' ORDER BY priority DESC,created_at LIMIT ?",(MAX_ACTIONS,)).fetchall()]; d.close()
    seeded=0; saturated=0
    for a in actions:
        if a['action_type']=='MAP_CHAMPION_NEIGHBORHOOD': made=execute_champion(a,False)
        elif a['action_type']=='STRESS_CHAMPION': made=execute_champion(a,True)
        elif a['action_type']=='EXPLORE_UNDERCOVERED_FAMILY': made=execute_novelty(a)
        else:
            mark_action(a['action_id'],'UNSUPPORTED','executor has no handler'); log(a,None,'UNSUPPORTED','no handler'); continue
        created=[x for x in made if x[1]]
        for eid,cr,status,why in made: log(a,eid,status,why)
        seeded+=len(created)
        if created: mark_action(a['action_id'],'EXECUTED',f'seeded {len(created)} SIDE experiments')
        else:
            saturated+=1; mark_action(a['action_id'],'SATURATED','no novel executable experiment found')
    # Critical: synchronously diversity-gate every newly READY experiment before V4.9 consumes it.
    g=gate.cycle()
    state={'actions':len(actions),'seeded':seeded,'blocked':g.get('skipped',0),'allowed':g.get('allowed',0),'refresh':g.get('refresh',0),'saturated':saturated,'rows_avoided':g.get('estimated_rows_avoided_total',0)}
    d=core.open_research(); d.execute("INSERT INTO v563_state(key,value_json,updated_at) VALUES('latest',?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",(core.canonical_json(state),time.time())); d.commit(); d.close(); return state

def display(s):
    d=core.open_research();
    counts={r['state']:r['n'] for r in d.execute("SELECT state,COUNT(*) n FROM v562_actions GROUP BY state")} if 'v562_actions' in tables(d) else {}
    recent=[dict(r) for r in d.execute("SELECT * FROM v563_execution_log ORDER BY created_at DESC LIMIT 10").fetchall()]; d.close()
    print('\033[2J\033[H',end=''); print('='*132); print('MEMECOIN LAB — AUTONOMOUS RESEARCH EXECUTOR V5.6.3'); print('='*132)
    print(f"ACTIONS_TAKEN={s['actions']} SEEDED={s['seeded']} | GATE_ALLOWED={s['allowed']} GATE_BLOCKED={s['blocked']} REFRESH={s.get('refresh',0)} SATURATED={s['saturated']}")
    print(f"ACTION_STATES OPEN={counts.get('OPEN',0)} EXECUTED={counts.get('EXECUTED',0)} SATURATED={counts.get('SATURATED',0)} | ROWS_AVOIDED_TOTAL={s.get('rows_avoided',0):,}")
    print('\nRECENT EXECUTIONS')
    for r in recent: print(f"{r['book']:<22} {r['status']:<10} {str(r['experiment_id'] or '—')[:18]:<18} {r['detail'][:72]}")
    print('\nLoop: V5.6.2 decides -> V5.6.3 seeds -> V5.6.1 gates -> V4.9 tests -> V5.0 proposes -> V5.5 freezes future-only.')
    print('Guardrail: executor creates SIDE experiments only; it cannot create PASS or alter frozen evidence.')

def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try: display(cycle())
        except Exception as e: print('V5.6.3 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__': main()
