#!/usr/bin/env python3
"""Memecoin Lab V4.8 — meta-learning champion brain.

Runs beside V4.6/V4.7. It does not mutate frozen prospective rules.
It answers a new question: WHICH edges are working NOW, which are decaying,
and where should the research organism spend attention next?

Outputs:
- live champion/contender/retire ranking for single-signal prospective candidates
- trend classification from early vs late prospective behavior
- family research budgets based on forward evidence, stability and redundancy
- research agenda items written from the evidence
- ensemble ranking beside singles

Research-only. No trading/signing. No retrospective threshold changes.
"""
from __future__ import annotations

import json, math, os, signal, sqlite3, statistics, time
from collections import defaultdict

import v41_core as core
import v47_science as v47

LOOP_S=float(os.environ.get("MEMECOIN_V48_LOOP_S","3"))
STOP=False

def stop_handler(*_):
    global STOP; STOP=True

def sf(x,default=0.0):
    try:
        v=float(x); return v if math.isfinite(v) else default
    except Exception: return default

def init_v48():
    db=core.open_research()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v48_signal_rankings(
      candidate_id TEXT PRIMARY KEY,
      feature TEXT NOT NULL,
      family TEXT NOT NULL,
      target TEXT NOT NULL,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      belief_status TEXT NOT NULL,
      audit_class TEXT,
      n INTEGER NOT NULL,
      rho REAL,
      lift REAL,
      precision REAL,
      baseline REAL,
      early_rho REAL,
      late_rho REAL,
      early_lift REAL,
      late_lift REAL,
      stability_score REAL,
      redundancy_penalty REAL NOT NULL,
      trend TEXT NOT NULL,
      live_score REAL NOT NULL,
      role TEXT NOT NULL,
      explanation TEXT NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v48_ensemble_rankings(
      ensemble_id TEXT PRIMARY KEY,
      context_key TEXT NOT NULL,
      status TEXT NOT NULL,
      n INTEGER NOT NULL,
      rho REAL,
      lift REAL,
      precision REAL,
      baseline REAL,
      confidence REAL,
      member_count INTEGER NOT NULL,
      live_score REAL NOT NULL,
      role TEXT NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v48_family_budget(
      family TEXT PRIMARY KEY,
      pass_count INTEGER NOT NULL,
      stable_count INTEGER NOT NULL,
      champion_count INTEGER NOT NULL,
      median_score REAL,
      median_rho REAL,
      median_lift REAL,
      budget_weight REAL NOT NULL,
      action TEXT NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v48_research_agenda(
      agenda_key TEXT PRIMARY KEY,
      priority INTEGER NOT NULL,
      family TEXT,
      agenda_type TEXT NOT NULL,
      subject TEXT NOT NULL,
      rationale TEXT NOT NULL,
      state TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    """)
    db.commit(); db.close()

def redundancy_penalty(cid):
    db=core.open_research()
    rows=db.execute("SELECT redundancy FROM v47_pair_redundancy WHERE candidate_a=? OR candidate_b=?",(cid,cid)).fetchall()
    db.close()
    vals=[sf(r[0],0) for r in rows]
    return max(vals) if vals else 0.0

def trend_from(a):
    er=a.get('early_rho'); lr=a.get('late_rho'); el=a.get('early_lift'); ll=a.get('late_lift')
    if lr is None and ll is None: return 'UNKNOWN'
    dr=sf(lr)-sf(er); dl=sf(ll,1)-sf(el,1)
    if sf(lr) < 0.02 or (ll is not None and sf(ll,1) <= 1.05): return 'BROKEN'
    if dr >= .05 and dl >= .25: return 'ACCELERATING'
    if dr <= -.08 or dl <= -.50: return 'DECAYING'
    return 'STABLE'

def score_signal(c,a,red):
    n=sf(c.get('n')); rho=max(0,sf(c.get('prospective_rho'))); lift=max(1,sf(c.get('lift'),1)); conf=max(0,sf(c.get('belief_confidence')))
    stab=max(0,sf(a.get('stability_score')) if a else 0)
    late_rho=max(0,sf(a.get('late_rho')) if a else rho)
    late_lift=max(1,sf(a.get('late_lift'),lift) if a else lift)
    sample=min(1,n/200.0)
    base=(.18*sample + .18*min(1,rho/.30) + .18*min(1,(lift-1)/3.0) + .14*conf + .16*stab + .08*min(1,late_rho/.25) + .08*min(1,(late_lift-1)/2.5))
    return max(0,min(1,base*(1-.35*min(1,red))))

def refresh_signals():
    db=core.open_research()
    cands=[dict(r) for r in db.execute("""SELECT c.*,b.status belief_status,b.n,b.baseline_rate,b.precision,b.lift,b.prospective_rho,b.confidence belief_confidence
      FROM v46_prospective_candidates c JOIN v46_beliefs b ON b.candidate_id=c.candidate_id
      WHERE b.status IN ('PASS','WATCH','FAIL')""").fetchall()]
    audits={r['candidate_id']:dict(r) for r in db.execute("SELECT * FROM v47_pass_audit").fetchall()}
    db.close(); now=time.time(); rows=[]
    for c in cands:
        a=audits.get(c['candidate_id'],{})
        red=redundancy_penalty(c['candidate_id']); trend=trend_from(a); score=score_signal(c,a,red)
        role='WATCH'
        if c['belief_status']=='FAIL' or trend=='BROKEN': role='RETIRE'
        elif c['belief_status']=='PASS' and score>=.72 and trend in ('STABLE','ACCELERATING') and red<.82: role='CHAMPION'
        elif c['belief_status']=='PASS' and score>=.50: role='CONTENDER'
        elif c['belief_status']=='PASS': role='PASS_WEAK'
        explanation=f"{role}: n={c.get('n')}, rho={c.get('prospective_rho')}, lift={c.get('lift')}, trend={trend}, stability={a.get('stability_score')}, redundancy={red:.3f}"
        rows.append((c,a,red,trend,score,role,explanation))
    db=core.open_research(); db.execute('BEGIN IMMEDIATE')
    try:
        for c,a,red,trend,score,role,explanation in rows:
            db.execute("""INSERT INTO v48_signal_rankings(candidate_id,feature,family,target,stage_s,horizon_s,belief_status,audit_class,n,rho,lift,precision,baseline,early_rho,late_rho,early_lift,late_lift,stability_score,redundancy_penalty,trend,live_score,role,explanation,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(candidate_id) DO UPDATE SET belief_status=excluded.belief_status,audit_class=excluded.audit_class,n=excluded.n,rho=excluded.rho,lift=excluded.lift,precision=excluded.precision,baseline=excluded.baseline,early_rho=excluded.early_rho,late_rho=excluded.late_rho,early_lift=excluded.early_lift,late_lift=excluded.late_lift,stability_score=excluded.stability_score,redundancy_penalty=excluded.redundancy_penalty,trend=excluded.trend,live_score=excluded.live_score,role=excluded.role,explanation=excluded.explanation,updated_at=excluded.updated_at""",
              (c['candidate_id'],c['feature'],v47.family(c['feature']),c['target'],c['stage_s'],c['horizon_s'],c['belief_status'],a.get('audit_class'),int(c.get('n') or 0),c.get('prospective_rho'),c.get('lift'),c.get('precision'),c.get('baseline_rate'),a.get('early_rho'),a.get('late_rho'),a.get('early_lift'),a.get('late_lift'),a.get('stability_score'),red,trend,score,role,explanation,now))
        db.commit()
    except BaseException:
        db.rollback(); raise
    finally: db.close()
    return len(rows)

def refresh_ensembles():
    db=core.open_research()
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='v47_ensemble_beliefs'").fetchone(): db.close(); return 0
    rows=[dict(r) for r in db.execute("""SELECT e.*,b.status belief_status,b.n,b.baseline,b.precision,b.lift,b.vote_rho,b.confidence
      FROM v47_ensembles e JOIN v47_ensemble_beliefs b ON b.ensemble_id=e.ensemble_id""").fetchall()]
    db.close(); now=time.time(); db=core.open_research(); db.execute('BEGIN IMMEDIATE')
    try:
        for r in rows:
            n=sf(r['n']); rho=max(0,sf(r['vote_rho'])); lift=max(1,sf(r['lift'],1)); conf=max(0,sf(r['confidence']))
            sc=min(1,.25*min(1,n/200)+.30*min(1,rho/.30)+.25*min(1,(lift-1)/3)+.20*conf)
            role='ENSEMBLE_WATCH'
            if r['belief_status']=='PASS' and sc>=.65: role='ENSEMBLE_CHAMPION'
            elif r['belief_status']=='FAIL': role='ENSEMBLE_RETIRE'
            db.execute("""INSERT INTO v48_ensemble_rankings(ensemble_id,context_key,status,n,rho,lift,precision,baseline,confidence,member_count,live_score,role,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(ensemble_id) DO UPDATE SET status=excluded.status,n=excluded.n,rho=excluded.rho,lift=excluded.lift,precision=excluded.precision,baseline=excluded.baseline,confidence=excluded.confidence,member_count=excluded.member_count,live_score=excluded.live_score,role=excluded.role,updated_at=excluded.updated_at""",
              (r['ensemble_id'],r['context_key'],r['belief_status'],int(r['n'] or 0),r['vote_rho'],r['lift'],r['precision'],r['baseline'],r['confidence'],int(r['member_count']),sc,role,now))
        db.commit()
    except BaseException: db.rollback(); raise
    finally: db.close()
    return len(rows)

def refresh_family_budget():
    db=core.open_research(); rows=[dict(r) for r in db.execute("SELECT * FROM v48_signal_rankings").fetchall()]; db.close()
    groups=defaultdict(list)
    for r in rows: groups[r['family']].append(r)
    raw={}
    for fam,items in groups.items():
        scores=[sf(x['live_score']) for x in items if x['role']!='RETIRE']; rhos=[sf(x['rho']) for x in items if x['rho'] is not None]; lifts=[sf(x['lift']) for x in items if x['lift'] is not None]
        champion=sum(x['role']=='CHAMPION' for x in items); stable=sum(x['trend'] in ('STABLE','ACCELERATING') for x in items); passes=sum(x['belief_status']=='PASS' for x in items)
        raw[fam]=.55*(statistics.median(scores) if scores else 0)+.25*min(1,champion/2)+.20*min(1,stable/3)
    total=sum(raw.values()) or 1.0; now=time.time(); db=core.open_research(); db.execute('BEGIN IMMEDIATE')
    try:
        for fam,items in groups.items():
            scores=[sf(x['live_score']) for x in items if x['role']!='RETIRE']; rhos=[sf(x['rho']) for x in items if x['rho'] is not None]; lifts=[sf(x['lift']) for x in items if x['lift'] is not None]
            champion=sum(x['role']=='CHAMPION' for x in items); stable=sum(x['trend'] in ('STABLE','ACCELERATING') for x in items); passes=sum(x['belief_status']=='PASS' for x in items); w=raw[fam]/total
            action='EXPAND' if w>=.30 else ('MAINTAIN' if w>=.15 else 'DEPRIORITIZE')
            db.execute("""INSERT INTO v48_family_budget(family,pass_count,stable_count,champion_count,median_score,median_rho,median_lift,budget_weight,action,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(family) DO UPDATE SET pass_count=excluded.pass_count,stable_count=excluded.stable_count,champion_count=excluded.champion_count,median_score=excluded.median_score,median_rho=excluded.median_rho,median_lift=excluded.median_lift,budget_weight=excluded.budget_weight,action=excluded.action,updated_at=excluded.updated_at""",
              (fam,passes,stable,champion,statistics.median(scores) if scores else None,statistics.median(rhos) if rhos else None,statistics.median(lifts) if lifts else None,w,action,now))
        db.commit()
    except BaseException: db.rollback(); raise
    finally: db.close()
    return len(groups)

def refresh_agenda():
    db=core.open_research(); sig=[dict(r) for r in db.execute("SELECT * FROM v48_signal_rankings ORDER BY live_score DESC").fetchall()]; fam=[dict(r) for r in db.execute("SELECT * FROM v48_family_budget ORDER BY budget_weight DESC").fetchall()]; db.close()
    now=time.time(); items=[]
    for r in sig[:12]:
        if r['role']=='CHAMPION': items.append((10,r['family'],'CHAMPION_EXTENSION',r['feature'],f"Extend champion {r['feature']} into adjacent timing/regime questions without changing its frozen prospective rule."))
        elif r['trend']=='DECAYING': items.append((20,r['family'],'DECAY_DIAGNOSIS',r['feature'],f"Diagnose why {r['feature']} is decaying: split by regime, source program, and recent time windows."))
    for f in fam:
        if f['action']=='EXPAND': items.append((15,f['family'],'FAMILY_EXPANSION',f['family'],f"Allocate more exploratory research to {f['family']} because forward evidence is currently strongest."))
        elif f['action']=='DEPRIORITIZE': items.append((60,f['family'],'FAMILY_COOLDOWN',f['family'],f"Reduce new experiments in {f['family']} until new data changes the evidence."))
    db=core.open_research(); db.execute('BEGIN IMMEDIATE')
    try:
        for pri,family,typ,subj,why in items:
            key='A_'+core.fingerprint({'type':typ,'subject':subj},'agenda:')[:20]
            db.execute("""INSERT INTO v48_research_agenda(agenda_key,priority,family,agenda_type,subject,rationale,state,created_at,updated_at)
              VALUES(?,?,?,?,? ,?,'OPEN',?,?) ON CONFLICT(agenda_key) DO UPDATE SET priority=excluded.priority,rationale=excluded.rationale,state='OPEN',updated_at=excluded.updated_at""",
              (key,pri,family,typ,subj,why,now,now))
        db.commit()
    except BaseException: db.rollback(); raise
    finally: db.close()
    return len(items)

def display():
    db=core.open_research(); top=[dict(r) for r in db.execute("SELECT * FROM v48_signal_rankings ORDER BY live_score DESC LIMIT 8").fetchall()]; ens=[dict(r) for r in db.execute("SELECT * FROM v48_ensemble_rankings ORDER BY live_score DESC LIMIT 5").fetchall()]; fam=[dict(r) for r in db.execute("SELECT * FROM v48_family_budget ORDER BY budget_weight DESC").fetchall()]; ag=db.execute("SELECT COUNT(*) FROM v48_research_agenda WHERE state='OPEN'").fetchone()[0]; db.close()
    print('\033[2J\033[H',end=''); print('='*126); print('MEMECOIN LAB — META-LEARNING CHAMPION BRAIN V4.8'); print('='*126)
    print(f"SIGNALS={len(top)} shown | ENSEMBLES={len(ens)} shown | OPEN AGENDA={ag}")
    print('\nWHAT IS WORKING NOW')
    for i,r in enumerate(top,1): print(f"#{i:02d} {r['role']:<11} {r['feature']:<22} {r['target']:<15} score={r['live_score']:.3f} n={r['n']:>4} rho={sf(r['rho']):.3f} lift={sf(r['lift'],1):.2f}x trend={r['trend']:<12} red={r['redundancy_penalty']:.2f}")
    if ens:
        print('\nENSEMBLE FRONTIER')
        for r in ens: print(f"{r['role']:<18} {r['context_key']:<25} score={r['live_score']:.3f} n={r['n']:>4} rho={sf(r['rho']):.3f} lift={sf(r['lift'],1):.2f}x members={r['member_count']}")
    print('\nRESEARCH BUDGET')
    for f in fam: print(f"{f['family']:<24} {100*f['budget_weight']:>5.1f}%  {f['action']:<12} champions={f['champion_count']} stable={f['stable_count']} pass={f['pass_count']}")
    print('\nV4.8 never changes frozen thresholds/cutoffs. It ranks current evidence and directs future exploratory attention only.')

def tick():
    refresh_signals(); refresh_ensembles(); refresh_family_budget(); refresh_agenda()

def main():
    signal.signal(signal.SIGINT,stop_handler); signal.signal(signal.SIGTERM,stop_handler); init_v48()
    while not STOP:
        tick(); display(); time.sleep(LOOP_S)
    print('V4.8 meta brain stopped cleanly')

if __name__=='__main__': main()
