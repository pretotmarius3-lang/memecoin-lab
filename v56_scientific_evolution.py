#!/usr/bin/env python3
"""Memecoin Lab V5.6 — Scientific Evolution Engine.

Builds a higher-level evolutionary view on top of V4.9/V5.0/V5.5 without ever
changing a frozen prospective test.

Responsibilities:
- reconstruct candidate lineage and generations
- group candidates into scientific families
- measure family prospective productivity (PASS/FAIL/WATCH/WAIT)
- detect exact/near-redundant candidate neighborhoods
- publish conservative research-allocation budgets
- suggest EXPAND / MAINTAIN / COOLDOWN / RETIRE actions only when evidence exists

Important guardrail: V5.6 is a research allocator. It does NOT alter v55 rule_json,
thresholds, cutoffs, observations, beliefs, or historical verdicts. No trading.
"""
from __future__ import annotations

import json, math, os, signal, time
from collections import defaultdict

import v41_core as core

LOOP=float(os.environ.get('MEMECOIN_V56_LOOP_S','5'))
MIN_VERDICTS=int(os.environ.get('MEMECOIN_V56_MIN_VERDICTS','3'))
MIN_FAMILY_FLOOR=float(os.environ.get('MEMECOIN_V56_FAMILY_FLOOR','0.08'))
STOP=False


def stop(*_):
    global STOP; STOP=True


def sf(x,d=0.0):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d


def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v56_lineage_nodes(
      candidate_id TEXT PRIMARY KEY,
      parent_key TEXT,
      generation INTEGER NOT NULL,
      family TEXT NOT NULL,
      kind TEXT NOT NULL,
      scientific_key TEXT NOT NULL,
      state TEXT NOT NULL,
      confidence REAL NOT NULL,
      prospective_rho REAL,
      lift REAL,
      n INTEGER NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v56_redundancy(
      candidate_id TEXT PRIMARY KEY,
      redundancy_key TEXT NOT NULL,
      group_size INTEGER NOT NULL,
      representative_id TEXT NOT NULL,
      role TEXT NOT NULL,
      updated_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v56_redundancy_key ON v56_redundancy(redundancy_key);

    CREATE TABLE IF NOT EXISTS v56_family_evolution(
      family TEXT PRIMARY KEY,
      frozen INTEGER NOT NULL,
      waiting INTEGER NOT NULL,
      watching INTEGER NOT NULL,
      passed INTEGER NOT NULL,
      failed INTEGER NOT NULL,
      verdicts INTEGER NOT NULL,
      pass_rate REAL,
      mean_confidence REAL NOT NULL,
      mean_rho REAL,
      mean_lift REAL,
      side_yield REAL,
      evidence_score REAL NOT NULL,
      allocation REAL NOT NULL,
      action TEXT NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v56_actions(
      action_id TEXT PRIMARY KEY,
      action_type TEXT NOT NULL,
      subject TEXT NOT NULL,
      priority REAL NOT NULL,
      rationale TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      state TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v56_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    '''); d.commit(); d.close()


def feature_family(feature):
    f=str(feature or '')
    if f in ('repeat_wallet_ratio','wallet_hhi','wallet_top1_share','trade_hhi','top1_trade_share','unique_wallets','unique_buyers','unique_sellers'):
        return 'WALLET_CONCENTRATION'
    if f in ('net_sol','flow_velocity','flow_acceleration','buy_ratio','buy_ratio_delta','buy_sol','sell_sol','gross_sol'):
        return 'FLOW_IMBALANCE'
    if f in ('return_pct','range_pct','price_velocity','return_since_entry','mfe_so_far','mae_so_far'):
        return 'PRICE_MOMENTUM'
    return 'ACTIVITY_TRADING'


def candidate_family(kind,spec):
    if kind=='CROSS_FAMILY_BLEND':
        fs=[feature_family(x) for x in spec.get('features',[])]
        return 'CROSS_FAMILY' if len(set(fs))>1 else (fs[0] if fs else 'ACTIVITY_TRADING')
    f=spec.get('feature') or spec.get('weak') or spec.get('gate')
    return feature_family(f)


def scientific_key(kind,s):
    # Coarse identity for redundancy: same hypothesis shape/context, ignoring lineage id.
    core_spec={
      'kind':kind,
      'stage':s.get('stage'), 'stage1':s.get('stage1'), 'stage2':s.get('stage2'),
      'horizon':s.get('horizon'), 'target':s.get('target'),
      'feature':s.get('feature'), 'features':s.get('features'),
      'weak':s.get('weak'), 'gate':s.get('gate')
    }
    return core.fingerprint(core_spec,'v56science:')[:24]


def generation_for(candidate,parent,known):
    # Exact parent candidate gives a true generation increment. Historical/non-candidate
    # parent keys are roots from V4.x and therefore generation 1.
    if parent and parent in known:
        return int(known[parent].get('generation',1))+1
    return 1


def refresh_lineage():
    d=core.open_research()
    if not d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='v55_candidates'").fetchone():
        d.close(); return 0
    rows=[dict(r) for r in d.execute('''
      SELECT c.*, COALESCE(b.state,c.state,'WAITING') belief_state,
             COALESCE(b.confidence,0) confidence,b.prospective_rho,b.lift,COALESCE(b.n,0) n
      FROM v55_candidates c LEFT JOIN v55_beliefs b USING(candidate_id)
      ORDER BY c.frozen_at,c.candidate_id
    ''').fetchall()]
    d.close()
    known={}
    now=time.time(); out=[]
    # A few passes resolve parent chains even when ordering is imperfect.
    pending=list(rows)
    for _ in range(6):
        nextp=[]
        for r in pending:
            parent=r.get('parent_key')
            if parent and str(parent).startswith('F_') and parent not in known:
                nextp.append(r); continue
            spec=json.loads(r['spec_json']); fam=candidate_family(r['kind'],spec)
            gen=generation_for(r['candidate_id'],parent,known)
            node=dict(r); node['generation']=gen; known[r['candidate_id']]=node
            out.append((r['candidate_id'],parent,gen,fam,r['kind'],scientific_key(r['kind'],spec),r['belief_state'],sf(r['confidence']),r['prospective_rho'],r['lift'],int(r['n']),now))
        if not nextp: break
        pending=nextp
    for r in pending:
        spec=json.loads(r['spec_json']); fam=candidate_family(r['kind'],spec)
        out.append((r['candidate_id'],r.get('parent_key'),1,fam,r['kind'],scientific_key(r['kind'],spec),r['belief_state'],sf(r['confidence']),r['prospective_rho'],r['lift'],int(r['n']),now))

    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        d.executemany('''INSERT INTO v56_lineage_nodes(candidate_id,parent_key,generation,family,kind,scientific_key,state,confidence,prospective_rho,lift,n,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(candidate_id) DO UPDATE SET parent_key=excluded.parent_key,generation=excluded.generation,
          family=excluded.family,kind=excluded.kind,scientific_key=excluded.scientific_key,state=excluded.state,
          confidence=excluded.confidence,prospective_rho=excluded.prospective_rho,lift=excluded.lift,n=excluded.n,updated_at=excluded.updated_at''',out)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return len(out)


def refresh_redundancy():
    d=core.open_research(); rows=[dict(r) for r in d.execute('SELECT * FROM v56_lineage_nodes').fetchall()]; d.close()
    groups=defaultdict(list)
    for r in rows:groups[r['scientific_key']].append(r)
    now=time.time(); vals=[]
    for key,g in groups.items():
        # Representative = strongest future evidence, then sample size.
        rep=max(g,key=lambda x:(x['state']=='PASS',x['state']=='WATCH',sf(x['confidence']),int(x['n'])))
        for r in g:
            role='REPRESENTATIVE' if r['candidate_id']==rep['candidate_id'] else ('REDUNDANT' if len(g)>1 else 'UNIQUE')
            vals.append((r['candidate_id'],key,len(g),rep['candidate_id'],role,now))
    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        d.executemany('''INSERT INTO v56_redundancy(candidate_id,redundancy_key,group_size,representative_id,role,updated_at)
          VALUES(?,?,?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET redundancy_key=excluded.redundancy_key,
          group_size=excluded.group_size,representative_id=excluded.representative_id,role=excluded.role,updated_at=excluded.updated_at''',vals)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return sum(1 for x in vals if x[4]=='REDUNDANT')


def side_yields():
    d=core.open_research(); names={r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'v49_side_results' not in names or 'v49_side_experiments' not in names:
        d.close(); return {}
    rows=[dict(r) for r in d.execute('''SELECT r.comparison,e.parent_feature FROM v49_side_results r
      JOIN v49_side_experiments e USING(experiment_id)''').fetchall()]; d.close()
    by=defaultdict(lambda:[0,0])
    for r in rows:
        fam=feature_family(r.get('parent_feature')); by[fam][1]+=1; by[fam][0]+=int(r.get('comparison')=='IMPROVED')
    return {k:a/max(1,n) for k,(a,n) in by.items()}


def refresh_families():
    d=core.open_research(); nodes=[dict(r) for r in d.execute('SELECT * FROM v56_lineage_nodes').fetchall()]; d.close(); sy=side_yields()
    fams=defaultdict(list)
    for r in nodes:fams[r['family']].append(r)
    raw={}; metrics={}
    for fam,rs in fams.items():
        states=defaultdict(int)
        for r in rs:states[r['state']]+=1
        verdicts=states['PASS']+states['FAIL']; pr=states['PASS']/verdicts if verdicts else None
        conf=sum(sf(r['confidence']) for r in rs)/max(1,len(rs))
        rhos=[sf(r['prospective_rho'],None) for r in rs if r['prospective_rho'] is not None]
        lifts=[sf(r['lift'],None) for r in rs if r['lift'] is not None]
        mr=sum(rhos)/len(rhos) if rhos else None; ml=sum(lifts)/len(lifts) if lifts else None
        side=sy.get(fam,0.0)
        # Until verdicts exist, prospective confidence contributes but cannot dominate.
        verdict_signal=0.5 if pr is None else pr
        sample_signal=min(1.0,verdicts/10.0)
        score=.45*(verdict_signal*sample_signal + .35*(1-sample_signal)) + .20*min(1,conf) + .20*min(1,max(0,sf(mr))/0.20) + .15*side
        raw[fam]=max(.001,score)
        metrics[fam]=(len(rs),states,verdicts,pr,conf,mr,ml,side,score)
    total=sum(raw.values()) or 1; k=max(1,len(raw)); floor=min(MIN_FAMILY_FLOOR,0.8/k); free=max(0,1-floor*k)
    now=time.time(); vals=[]
    for fam,m in metrics.items():
        frozen,st,verdicts,pr,conf,mr,ml,side,score=m; alloc=floor+free*raw[fam]/total
        if verdicts<MIN_VERDICTS: action='OBSERVE'
        elif pr is not None and pr>=.55: action='EXPAND'
        elif pr is not None and pr<=.15 and verdicts>=max(5,MIN_VERDICTS): action='RETIRE'
        elif pr is not None and pr<=.30: action='COOLDOWN'
        else: action='MAINTAIN'
        vals.append((fam,frozen,st['WAITING'],st['WATCH'],st['PASS'],st['FAIL'],verdicts,pr,conf,mr,ml,side,score,alloc,action,now))
    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        d.executemany('''INSERT INTO v56_family_evolution(family,frozen,waiting,watching,passed,failed,verdicts,pass_rate,mean_confidence,mean_rho,mean_lift,side_yield,evidence_score,allocation,action,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(family) DO UPDATE SET frozen=excluded.frozen,waiting=excluded.waiting,
          watching=excluded.watching,passed=excluded.passed,failed=excluded.failed,verdicts=excluded.verdicts,pass_rate=excluded.pass_rate,
          mean_confidence=excluded.mean_confidence,mean_rho=excluded.mean_rho,mean_lift=excluded.mean_lift,side_yield=excluded.side_yield,
          evidence_score=excluded.evidence_score,allocation=excluded.allocation,action=excluded.action,updated_at=excluded.updated_at''',vals)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return vals


def publish_actions():
    d=core.open_research(); fams=[dict(r) for r in d.execute('SELECT * FROM v56_family_evolution').fetchall()]; d.close(); now=time.time(); made=0
    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for f in fams:
            if f['action']=='OBSERVE':continue
            aid='E_'+core.fingerprint({'fam':f['family'],'action':f['action']},'v56a:')[:24]
            why=(f"{f['family']} prospective verdicts={f['verdicts']} pass_rate={f['pass_rate']} "
                 f"rho={f['mean_rho']} lift={f['mean_lift']} side_yield={f['side_yield']:.3f}")
            priority=min(1,.4+.6*min(1,int(f['verdicts'])/10))
            payload={'family':f['family'],'allocation':f['allocation'],'verdicts':f['verdicts'],'pass_rate':f['pass_rate']}
            before=d.total_changes
            d.execute('''INSERT INTO v56_actions(action_id,action_type,subject,priority,rationale,payload_json,state,created_at,updated_at)
              VALUES(?,?,?,?,?,?,'OPEN',?,?) ON CONFLICT(action_id) DO UPDATE SET priority=excluded.priority,rationale=excluded.rationale,
              payload_json=excluded.payload_json,updated_at=excluded.updated_at''',(aid,f['action']+'_FAMILY',f['family'],priority,why,core.canonical_json(payload),now,now))
            made+=int(d.total_changes>before)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return made


def display(nodes,redundant,fams,new_actions):
    d=core.open_research()
    states={r['state']:r['n'] for r in d.execute('SELECT state,COUNT(*) n FROM v56_lineage_nodes GROUP BY state')}
    gens=[dict(r) for r in d.execute('SELECT generation,COUNT(*) n FROM v56_lineage_nodes GROUP BY generation ORDER BY generation').fetchall()]
    families=[dict(r) for r in d.execute('SELECT * FROM v56_family_evolution ORDER BY allocation DESC').fetchall()]
    top=[dict(r) for r in d.execute('''SELECT n.*,r.role FROM v56_lineage_nodes n LEFT JOIN v56_redundancy r USING(candidate_id)
      ORDER BY CASE n.state WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 WHEN 'WAITING' THEN 2 ELSE 3 END,
      n.confidence DESC,n.n DESC LIMIT 10''').fetchall()]
    open_actions=d.execute("SELECT COUNT(*) FROM v56_actions WHERE state='OPEN'").fetchone()[0]
    d.close()
    print('\033[2J\033[H',end=''); print('='*132); print('MEMECOIN LAB — SCIENTIFIC EVOLUTION ENGINE V5.6'); print('='*132)
    print(f"LINEAGE_NODES={nodes} | REDUNDANT={redundant} | WAIT={states.get('WAITING',0)} WATCH={states.get('WATCH',0)} PASS={states.get('PASS',0)} FAIL={states.get('FAIL',0)} | NEW_ACTIONS={new_actions} OPEN_ACTIONS={open_actions}")
    print('GENERATIONS  '+'  '.join(f"G{x['generation']}={x['n']}" for x in gens))
    print('\nEVOLUTIONARY RESEARCH BUDGET')
    for f in families:
        pr='—' if f['pass_rate'] is None else f"{100*f['pass_rate']:.1f}%"
        print(f"{f['family']:<24} alloc={100*f['allocation']:5.1f}% {f['action']:<8} frozen={f['frozen']:>4} verdicts={f['verdicts']:>3} pass={pr:>6} conf={f['mean_confidence']:.2f} side={100*sf(f['side_yield']):4.1f}%")
    print('\nTOP LINEAGES')
    for x in top:
        print(f"G{x['generation']} {x['state']:<7} {x['family']:<22} {x['kind']:<20} n={x['n']:<4} rho={sf(x['prospective_rho']):+.3f} lift={sf(x['lift']):.2f} conf={x['confidence']:.2f} {x.get('role') or ''}")
    print('\nGuardrail: V5.6 allocates research attention only. Frozen V5.5 tests and their future-only evidence are immutable.')


def cycle():
    nodes=refresh_lineage(); redundant=refresh_redundancy(); fams=refresh_families(); new_actions=publish_actions();
    now=time.time(); d=core.open_research(); state={'nodes':nodes,'redundant':redundant,'families':len(fams),'new_actions':new_actions};
    d.execute("INSERT INTO v56_state(key,value_json,updated_at) VALUES('latest',?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",(core.canonical_json(state),now)); d.commit(); d.close(); display(nodes,redundant,fams,new_actions)


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:cycle()
        except Exception as e:print('V5.6 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
