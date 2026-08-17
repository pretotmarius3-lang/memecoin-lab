#!/usr/bin/env python3
"""Memecoin Lab V5.6.2 — Champion Registry + Research Portfolio.

Builds the next research-control layer on top of V5.5/V5.6 without changing any
frozen prospective test.

Core ideas
----------
1) A CHAMPION is earned only from V5.5 prospective PASS evidence.
2) Champions remain under continuous observation and can become STABLE,
   DECAYING or RETIRED as fresh future-only evidence accumulates.
3) Research attention is split into three explicit books:
      CHAMPION_EXPLOITATION 45%
      NOVELTY_SEARCH       30%
      FALSIFICATION        25%
   These are research budgets, not capital/trading budgets.
4) Candidate redundancy from V5.6 is used to down-weight crowded ideas.
5) This process publishes actions only; it never edits V5.5 rule_json,
   thresholds, cutoffs, observations or verdict history.

Research-only. No trading/signing.
"""
from __future__ import annotations
import json, math, os, signal, time
from collections import defaultdict
import v41_core as core

LOOP=float(os.environ.get('MEMECOIN_V562_LOOP_S','5'))
BASE_EXPLOIT=float(os.environ.get('MEMECOIN_V562_EXPLOIT','0.45'))
BASE_NOVELTY=float(os.environ.get('MEMECOIN_V562_NOVELTY','0.30'))
BASE_FALSIFY=float(os.environ.get('MEMECOIN_V562_FALSIFY','0.25'))
MIN_STABLE_N=int(os.environ.get('MEMECOIN_V562_STABLE_N','160'))
DECAY_RHO=float(os.environ.get('MEMECOIN_V562_DECAY_RHO','0.03'))
DECAY_LIFT=float(os.environ.get('MEMECOIN_V562_DECAY_LIFT','1.08'))
RETIRE_N=int(os.environ.get('MEMECOIN_V562_RETIRE_N','220'))
STOP=False

def stop(*_):
    global STOP; STOP=True

def sf(x,d=0.0):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v562_champion_registry(
      candidate_id TEXT PRIMARY KEY,
      family TEXT NOT NULL,
      kind TEXT NOT NULL,
      generation INTEGER NOT NULL,
      champion_state TEXT NOT NULL,
      n INTEGER NOT NULL,
      prospective_rho REAL,
      lift REAL,
      precision REAL,
      baseline_rate REAL,
      confidence REAL NOT NULL,
      redundancy_role TEXT,
      entered_at REAL NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v562_research_portfolio(
      book TEXT PRIMARY KEY,
      target_weight REAL NOT NULL,
      adaptive_weight REAL NOT NULL,
      evidence_score REAL NOT NULL,
      active_subjects INTEGER NOT NULL,
      rationale TEXT NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v562_actions(
      action_id TEXT PRIMARY KEY,
      book TEXT NOT NULL,
      action_type TEXT NOT NULL,
      subject TEXT NOT NULL,
      priority REAL NOT NULL,
      payload_json TEXT NOT NULL,
      rationale TEXT NOT NULL,
      state TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v562_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    '''); d.commit(); d.close()

def tables(d):
    return {r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}

def champion_state(state,n,rho,lift):
    # PASS is the admission gate. Later labels describe persistence, never rewrite PASS history.
    if state!='PASS': return None
    if n>=RETIRE_N and (sf(rho,9)<=0.0 or sf(lift,9)<=1.00): return 'RETIRED'
    if n>=MIN_STABLE_N and sf(rho,-9)>=.08 and sf(lift,0)>=1.20: return 'STABLE'
    if n>=MIN_STABLE_N and (sf(rho,9)<DECAY_RHO or sf(lift,9)<DECAY_LIFT): return 'DECAYING'
    return 'CHAMPION'

def refresh_registry():
    d=core.open_research(); names=tables(d)
    if not {'v55_candidates','v55_beliefs','v56_lineage_nodes'} <= names:
        d.close(); return 0
    has_red='v56_redundancy' in names
    redjoin='LEFT JOIN v56_redundancy r USING(candidate_id)' if has_red else ''
    redsel="COALESCE(r.role,'UNKNOWN') redundancy_role" if has_red else "'UNKNOWN' redundancy_role"
    rows=[dict(r) for r in d.execute(f'''
      SELECT c.candidate_id,c.kind,b.state,b.n,b.prospective_rho,b.lift,b.precision,b.baseline_rate,b.confidence,
             l.family,l.generation,{redsel}
      FROM v55_candidates c JOIN v55_beliefs b USING(candidate_id)
      JOIN v56_lineage_nodes l USING(candidate_id)
      {redjoin}
      WHERE b.state='PASS'
    ''').fetchall()]; d.close()
    now=time.time(); made=0; d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for r in rows:
            cs=champion_state(r['state'],int(r['n']),r['prospective_rho'],r['lift'])
            old=d.execute('SELECT entered_at FROM v562_champion_registry WHERE candidate_id=?',(r['candidate_id'],)).fetchone(); entered=float(old[0]) if old else now
            before=d.total_changes
            d.execute('''INSERT INTO v562_champion_registry(candidate_id,family,kind,generation,champion_state,n,prospective_rho,lift,precision,baseline_rate,confidence,redundancy_role,entered_at,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(candidate_id) DO UPDATE SET family=excluded.family,kind=excluded.kind,generation=excluded.generation,
              champion_state=excluded.champion_state,n=excluded.n,prospective_rho=excluded.prospective_rho,lift=excluded.lift,
              precision=excluded.precision,baseline_rate=excluded.baseline_rate,confidence=excluded.confidence,
              redundancy_role=excluded.redundancy_role,updated_at=excluded.updated_at''',
              (r['candidate_id'],r['family'],r['kind'],int(r['generation']),cs,int(r['n']),r['prospective_rho'],r['lift'],r['precision'],r['baseline_rate'],sf(r['confidence']),r['redundancy_role'],entered,now))
            made+=int(d.total_changes>before and old is None)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return made

def evidence():
    d=core.open_research(); names=tables(d)
    champs=[dict(r) for r in d.execute("SELECT * FROM v562_champion_registry WHERE champion_state!='RETIRED'").fetchall()]
    fams=[dict(r) for r in d.execute('SELECT * FROM v56_family_evolution').fetchall()] if 'v56_family_evolution' in names else []
    red=d.execute("SELECT COUNT(*) FROM v56_redundancy WHERE role='REDUNDANT'").fetchone()[0] if 'v56_redundancy' in names else 0
    nodes=d.execute('SELECT COUNT(*) FROM v56_lineage_nodes').fetchone()[0] if 'v56_lineage_nodes' in names else 0
    identities=d.execute('SELECT COUNT(DISTINCT scientific_key) FROM v56_lineage_nodes').fetchone()[0] if 'v56_lineage_nodes' in names else 0
    d.close()
    stable=sum(c['champion_state']=='STABLE' for c in champs); dec=sum(c['champion_state']=='DECAYING' for c in champs)
    mean_conf=sum(sf(c['confidence']) for c in champs)/max(1,len(champs))
    novelty_gap=max(0.0,1.0-identities/max(1,nodes))
    crowded=red/max(1,nodes)
    family_uncertainty=sum(1 for f in fams if int(f.get('verdicts') or 0)<3)/max(1,len(fams)) if fams else 1.0
    return champs,fams,dict(stable=stable,decaying=dec,mean_conf=mean_conf,novelty_gap=novelty_gap,crowded=crowded,family_uncertainty=family_uncertainty,nodes=nodes,identities=identities)

def weights(meta,champs):
    # Start from 45/30/25, then adapt mildly. We never let one book monopolize research.
    exploit=BASE_EXPLOIT + .08*min(1,len(champs)/8) + .05*min(1,meta['stable']/4) - .06*min(1,meta['decaying']/3)
    novelty=BASE_NOVELTY + .10*meta['novelty_gap'] + .05*meta['crowded']
    falsify=BASE_FALSIFY + .08*min(1,meta['decaying']/3) + .04*meta['family_uncertainty']
    raw={'CHAMPION_EXPLOITATION':max(.20,min(.60,exploit)),'NOVELTY_SEARCH':max(.15,min(.50,novelty)),'FALSIFICATION':max(.15,min(.45,falsify))}
    z=sum(raw.values()) or 1
    return {k:v/z for k,v in raw.items()}

def refresh_portfolio():
    champs,fams,meta=evidence(); w=weights(meta,champs); now=time.time()
    # Evidence scores are descriptive, not verdicts.
    scores={
      'CHAMPION_EXPLOITATION':min(1,.15*len(champs)+.35*meta['mean_conf']+.20*meta['stable']),
      'NOVELTY_SEARCH':min(1,.65*meta['novelty_gap']+.35*meta['crowded']),
      'FALSIFICATION':min(1,.45*meta['family_uncertainty']+.35*min(1,meta['decaying']/3)+.20*meta['mean_conf'])
    }
    rationale={
      'CHAMPION_EXPLOITATION':f"{len(champs)} live champions; stable={meta['stable']} decaying={meta['decaying']} mean_conf={meta['mean_conf']:.2f}",
      'NOVELTY_SEARCH':f"scientific identities={meta['identities']}/{meta['nodes']} novelty_gap={meta['novelty_gap']:.1%} crowded={meta['crowded']:.1%}",
      'FALSIFICATION':f"family_uncertainty={meta['family_uncertainty']:.1%}; decaying_champions={meta['decaying']}"
    }
    subjects={'CHAMPION_EXPLOITATION':len(champs),'NOVELTY_SEARCH':meta['identities'],'FALSIFICATION':meta['decaying']+sum(1 for f in fams if int(f.get('verdicts') or 0)<3)}
    base={'CHAMPION_EXPLOITATION':BASE_EXPLOIT,'NOVELTY_SEARCH':BASE_NOVELTY,'FALSIFICATION':BASE_FALSIFY}
    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for k in w:
            d.execute('''INSERT INTO v562_research_portfolio(book,target_weight,adaptive_weight,evidence_score,active_subjects,rationale,updated_at)
              VALUES(?,?,?,?,?,?,?) ON CONFLICT(book) DO UPDATE SET target_weight=excluded.target_weight,adaptive_weight=excluded.adaptive_weight,
              evidence_score=excluded.evidence_score,active_subjects=excluded.active_subjects,rationale=excluded.rationale,updated_at=excluded.updated_at''',
              (k,base[k],w[k],scores[k],int(subjects[k]),rationale[k],now))
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return champs,fams,meta,w

def publish_actions(champs,fams,meta,w):
    now=time.time(); items=[]
    # Champions: exploit and falsify each representative only; redundant champions are not cloned.
    for c in champs:
        if c.get('redundancy_role')=='REDUNDANT': continue
        state=c['champion_state']; basepri=.75 if state=='STABLE' else (.88 if state=='DECAYING' else .68)
        items.append(('CHAMPION_EXPLOITATION','MAP_CHAMPION_NEIGHBORHOOD',c['candidate_id'],basepri,
                      {'family':c['family'],'kind':c['kind'],'n':c['n'],'rho':c['prospective_rho'],'lift':c['lift']},
                      f"{state} champion: map nearby stage/horizon/regime without altering frozen parent."))
        items.append(('FALSIFICATION','STRESS_CHAMPION',c['candidate_id'],min(1,basepri+.06),
                      {'family':c['family'],'n':c['n'],'rho':c['prospective_rho'],'lift':c['lift']},
                      'Actively search for time/regime/source conditions where this champion fails.'))
    # Novelty: prioritize families with low frozen coverage or poor diversity.
    for f in fams:
        scarcity=1-min(1,int(f.get('frozen') or 0)/80)
        pri=.45+.45*scarcity
        items.append(('NOVELTY_SEARCH','EXPLORE_UNDERCOVERED_FAMILY',f['family'],pri,
                      {'family':f['family'],'allocation':f['allocation'],'verdicts':f['verdicts'],'pass_rate':f['pass_rate']},
                      'Search feature/stage/horizon/target combinations distant from existing scientific identities.'))
    d=core.open_research(); d.execute('BEGIN IMMEDIATE'); made=0
    try:
        for book,typ,sub,pri,pay,why in items:
            aid='RP_'+core.fingerprint({'book':book,'type':typ,'subject':sub},'v562a:')[:24]
            before=d.total_changes
            d.execute('''INSERT INTO v562_actions(action_id,book,action_type,subject,priority,payload_json,rationale,state,created_at,updated_at)
              VALUES(?,?,?,?,?,?,?,'OPEN',?,?) ON CONFLICT(action_id) DO UPDATE SET priority=excluded.priority,payload_json=excluded.payload_json,
              rationale=excluded.rationale,updated_at=excluded.updated_at''',(aid,book,typ,sub,pri,core.canonical_json(pay),why,now,now))
            made+=int(d.total_changes>before)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return made

def display(new_champs,meta,w,new_actions):
    d=core.open_research(); champs=[dict(r) for r in d.execute("SELECT * FROM v562_champion_registry ORDER BY CASE champion_state WHEN 'STABLE' THEN 0 WHEN 'CHAMPION' THEN 1 WHEN 'DECAYING' THEN 2 ELSE 3 END,confidence DESC,n DESC LIMIT 12").fetchall()];
    states={r['champion_state']:r['n'] for r in d.execute('SELECT champion_state,COUNT(*) n FROM v562_champion_registry GROUP BY champion_state')}
    books=[dict(r) for r in d.execute('SELECT * FROM v562_research_portfolio ORDER BY adaptive_weight DESC').fetchall()]
    opena=d.execute("SELECT COUNT(*) FROM v562_actions WHERE state='OPEN'").fetchone()[0]; d.close()
    print('\033[2J\033[H',end=''); print('='*134); print('MEMECOIN LAB — CHAMPION REGISTRY + RESEARCH PORTFOLIO V5.6.2'); print('='*134)
    print(f"CHAMPIONS={sum(states.values())} NEW={new_champs} | STABLE={states.get('STABLE',0)} DECAYING={states.get('DECAYING',0)} RETIRED={states.get('RETIRED',0)} | NEW_ACTIONS={new_actions} OPEN_ACTIONS={opena}")
    print(f"SCIENCE SPACE identities={meta['identities']:,}/{meta['nodes']:,} novelty_gap={meta['novelty_gap']:.1%} crowded={meta['crowded']:.1%}")
    print('\nRESEARCH PORTFOLIO')
    for b in books: print(f"{b['book']:<24} target={b['target_weight']*100:5.1f}% adaptive={b['adaptive_weight']*100:5.1f}% evidence={b['evidence_score']:.2f} subjects={b['active_subjects']:>4}")
    print('\nCHAMPION REGISTRY')
    if not champs: print('No prospective PASS yet — registry is armed and waiting.')
    for c in champs: print(f"{c['champion_state']:<9} G{c['generation']} {c['family']:<22} {c['kind']:<20} n={c['n']:<4} rho={sf(c['prospective_rho']):+.3f} lift={sf(c['lift']):.2f} conf={c['confidence']:.2f} {c['redundancy_role']}")
    print('\nGuardrail: champion status is earned only from V5.5 PASS. V5.6.2 can allocate research attention, never rewrite frozen evidence.')

def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:
            newc=refresh_registry(); champs,fams,meta,w=refresh_portfolio(); made=publish_actions(champs,fams,meta,w); display(newc,meta,w,made)
        except Exception as e: print('V5.6.2 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__': main()
