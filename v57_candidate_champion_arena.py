#!/usr/bin/env python3
"""Memecoin Lab V5.7 — Candidate & Champion Arena.

Runs bounded head-to-head shadow tournaments around current V5.5 candidates and
confirmed V5.6.2 champions. The frozen parent is ALWAYS the control and is never
modified. Challengers are emitted only as V4.9 SIDE experiments and must later
re-enter V5.0 -> V5.5 future-only validation before they can become champions.

What V5.7 does:
- selects promising WAIT/WATCH/PASS candidates as arena controls;
- creates bounded challenger variants (stage, horizon, sign, target, sequence);
- compares completed SIDE challenger evidence with the frozen control metrics;
- writes explicit conclusions and next-hypothesis requests;
- exposes compact metrics for dashboard visualization.

Research-only. No trading/signing. No frozen evidence is rewritten.
"""
from __future__ import annotations

import json, math, os, signal, time
from collections import defaultdict

import v41_core as core
import v49_recursive_lab as v49
import v561_diversity_gate as diversity

LOOP=float(os.environ.get('MEMECOIN_V57_LOOP_S','5'))
MAX_CONTROLS=int(os.environ.get('MEMECOIN_V57_MAX_CONTROLS','12'))
MAX_CHALLENGERS=int(os.environ.get('MEMECOIN_V57_MAX_CHALLENGERS','5'))
MIN_CONTROL_N=int(os.environ.get('MEMECOIN_V57_MIN_CONTROL_N','20'))
STOP=False


def stop(*_):
    global STOP; STOP=True


def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d


def init():
    v49.init_db(); diversity.init()
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v57_arenas(
      arena_id TEXT PRIMARY KEY,
      control_candidate_id TEXT NOT NULL,
      control_state TEXT NOT NULL,
      control_kind TEXT NOT NULL,
      control_spec_json TEXT NOT NULL,
      family TEXT,
      control_n INTEGER NOT NULL,
      control_rho REAL,
      control_lift REAL,
      control_confidence REAL NOT NULL,
      state TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v57_challengers(
      challenger_id TEXT PRIMARY KEY,
      arena_id TEXT NOT NULL,
      experiment_id TEXT,
      mutation_kind TEXT NOT NULL,
      spec_json TEXT NOT NULL,
      mutation_label TEXT NOT NULL,
      state TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v57_challenger_arena ON v57_challengers(arena_id);

    CREATE TABLE IF NOT EXISTS v57_duels(
      challenger_id TEXT PRIMARY KEY,
      arena_id TEXT NOT NULL,
      experiment_id TEXT NOT NULL,
      control_rho REAL,
      challenger_rho REAL,
      delta_rho REAL,
      challenger_n INTEGER NOT NULL,
      challenger_verdict TEXT,
      challenger_comparison TEXT,
      outcome TEXT NOT NULL,
      score REAL NOT NULL,
      metrics_json TEXT NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v57_conclusions(
      conclusion_id TEXT PRIMARY KEY,
      arena_id TEXT NOT NULL,
      control_candidate_id TEXT NOT NULL,
      verdict TEXT NOT NULL,
      statement TEXT NOT NULL,
      evidence_json TEXT NOT NULL,
      next_action TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v57_state(
      key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()


def tables(d):
    return {r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def controls():
    d=core.open_research(); names=tables(d)
    if not {'v55_candidates','v55_beliefs'} <= names:
        d.close(); return []
    fam_join='LEFT JOIN v56_lineage_nodes l USING(candidate_id)' if 'v56_lineage_nodes' in names else ''
    fam_sel="COALESCE(l.family,'UNKNOWN') family" if 'v56_lineage_nodes' in names else "'UNKNOWN' family"
    rows=[dict(r) for r in d.execute(f'''
      SELECT c.candidate_id,c.kind,c.spec_json,b.state,b.n,b.prospective_rho,b.lift,b.confidence,{fam_sel}
      FROM v55_candidates c JOIN v55_beliefs b USING(candidate_id)
      {fam_join}
      WHERE b.state IN ('WAITING','WATCH','PASS') AND b.n>=?
      ORDER BY CASE b.state WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 ELSE 2 END,
               b.confidence DESC,b.n DESC
      LIMIT ?
    ''',(MIN_CONTROL_N,MAX_CONTROLS)).fetchall()]; d.close(); return rows


def ensure_arenas():
    rows=controls(); now=time.time(); made=0; d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for r in rows:
            aid='A_'+core.fingerprint({'c':r['candidate_id']},'v57arena:')[:24]
            before=d.total_changes
            d.execute('''INSERT INTO v57_arenas(arena_id,control_candidate_id,control_state,control_kind,control_spec_json,family,
              control_n,control_rho,control_lift,control_confidence,state,created_at,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,?)
              ON CONFLICT(arena_id) DO UPDATE SET control_state=excluded.control_state,control_n=excluded.control_n,
              control_rho=excluded.control_rho,control_lift=excluded.control_lift,control_confidence=excluded.control_confidence,
              family=excluded.family,updated_at=excluded.updated_at''',
              (aid,r['candidate_id'],r['state'],r['kind'],r['spec_json'],r['family'],int(r['n']),r['prospective_rho'],r['lift'],sf(r['confidence'],0),now,now))
            made+=int(d.total_changes>before)
        d.commit()
    except BaseException:d.rollback(); raise
    finally:d.close()
    return made


def challenger_specs(kind,s):
    out=[]
    # Keep the parent frozen; every item below is a separate SIDE child.
    if 'stage' in s:
        st=int(s['stage'])
        for ns in v49.STAGES:
            if ns!=st and abs(ns-st)<=50:
                out.append(('TIME_NEIGHBOR',dict(s,stage=ns),f'stage {st}->{ns}'))
    if 'horizon' in s:
        hz=int(s['horizon'])
        for nh in v49.HORIZONS:
            if nh!=hz:
                out.append(('HORIZON_TRANSFER',dict(s,horizon=nh),f'horizon {hz}->{nh}'))
    if 'feature' in s and kind!='SIGN_FLIP':
        out.append(('SIGN_FLIP',dict(s),'sign flip stress'))
    if 'target' in s:
        tg=str(s['target'])
        for nt in v49.TARGETS:
            if nt!=tg:
                out.append(('TARGET_TRANSFER',dict(s,target=nt),f'target {tg}->{nt}'))
    if 'feature' in s and 'stage' in s:
        st=int(s['stage'])
        later=[x for x in v49.STAGES if x>st]
        if later:
            ns=later[0]
            out.append(('SEQUENCE_DELTA',{'stage1':st,'stage2':ns,'horizon':int(s['horizon']),'target':s['target'],'feature':s['feature']},f'level -> trajectory {st}->{ns}'))
    return out


def seed_challengers():
    d=core.open_research(); arenas=[dict(r) for r in d.execute("SELECT * FROM v57_arenas WHERE state='ACTIVE' ORDER BY control_confidence DESC LIMIT ?",(MAX_CONTROLS,)).fetchall()]; d.close()
    made=0; now=time.time()
    for a in arenas:
        s=json.loads(a['control_spec_json']); existing=core.open_research(); n_existing=existing.execute("SELECT COUNT(*) FROM v57_challengers WHERE arena_id=?",(a['arena_id'],)).fetchone()[0]; existing.close()
        if n_existing>=MAX_CHALLENGERS: continue
        parent=a['control_candidate_id']; feature=s.get('feature') or s.get('weak') or ((s.get('features') or ['COMPOSITE'])[0])
        for mkind,spec,label in challenger_specs(a['control_kind'],s):
            if n_existing>=MAX_CHALLENGERS: break
            stage=int(spec.get('stage',spec.get('stage1',10))); hz=int(spec['horizon']); tg=str(spec['target'])
            wm=v49.latest_watermark(stage,hz,tg)
            if not wm: continue
            cid='C_'+core.fingerprint({'a':a['arena_id'],'k':mkind,'s':spec},'v57chall:')[:24]
            d=core.open_research(); exists=d.execute("SELECT 1 FROM v57_challengers WHERE challenger_id=?",(cid,)).fetchone(); d.close()
            if exists: continue
            eid,created=v49.insert_exp(mkind,parent,feature,spec,wm,'V5.7 arena challenger: '+label,2)
            if not created: continue
            d=core.open_research(); d.execute('''INSERT OR IGNORE INTO v57_challengers(challenger_id,arena_id,experiment_id,mutation_kind,spec_json,mutation_label,state,created_at,updated_at)
              VALUES(?,?,?,?,?,?,'SEEDED',?,?)''',(cid,a['arena_id'],eid,mkind,core.canonical_json(spec),label,now,now)); d.commit(); d.close(); made+=1; n_existing+=1
    # Synchronous gate to avoid duplicate arena work.
    diversity.cycle()
    return made


def refresh_duels():
    d=core.open_research(); names=tables(d)
    if 'v49_side_results' not in names:
        d.close(); return 0
    rows=[dict(r) for r in d.execute('''SELECT ch.*,a.control_rho,a.control_lift,a.control_n,
      r.n challenger_n,r.holdout_rho challenger_rho,r.verdict challenger_verdict,r.comparison challenger_comparison,r.metrics_json
      FROM v57_challengers ch JOIN v57_arenas a USING(arena_id)
      JOIN v49_side_results r ON r.experiment_id=ch.experiment_id
      WHERE ch.state IN ('SEEDED','DONE')''').fetchall()]; d.close(); now=time.time(); made=0
    for r in rows:
        cr=sf(r['control_rho'],0); rr=sf(r['challenger_rho'],0); delta=rr-cr
        # Arena score rewards improvement but softly penalizes tiny challenger samples.
        sample=min(1.0,int(r['challenger_n'])/150.0)
        score=delta*sample
        if delta>=.08 and rr>=.10: outcome='CHALLENGER_WINS'
        elif delta<=-.05: outcome='CONTROL_WINS'
        else: outcome='TIE_OR_SPECIALIST'
        metrics={'control_rho':cr,'control_lift':r['control_lift'],'control_n':r['control_n'],'challenger_rho':rr,'delta_rho':delta,'challenger_n':r['challenger_n'],'side_metrics':json.loads(r['metrics_json']) if r.get('metrics_json') else {}}
        d=core.open_research(); before=d.total_changes
        d.execute('''INSERT INTO v57_duels(challenger_id,arena_id,experiment_id,control_rho,challenger_rho,delta_rho,challenger_n,
          challenger_verdict,challenger_comparison,outcome,score,metrics_json,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(challenger_id) DO UPDATE SET challenger_rho=excluded.challenger_rho,
          delta_rho=excluded.delta_rho,challenger_n=excluded.challenger_n,challenger_verdict=excluded.challenger_verdict,
          challenger_comparison=excluded.challenger_comparison,outcome=excluded.outcome,score=excluded.score,metrics_json=excluded.metrics_json,updated_at=excluded.updated_at''',
          (r['challenger_id'],r['arena_id'],r['experiment_id'],cr,rr,delta,int(r['challenger_n']),r['challenger_verdict'],r['challenger_comparison'],outcome,score,core.canonical_json(metrics),now))
        d.execute("UPDATE v57_challengers SET state='DONE',updated_at=? WHERE challenger_id=?",(now,r['challenger_id']))
        d.commit(); made+=int(d.total_changes>before); d.close()
    return made


def conclusions():
    d=core.open_research(); arenas=[dict(r) for r in d.execute("SELECT * FROM v57_arenas").fetchall()]; d.close(); made=0; now=time.time()
    for a in arenas:
        d=core.open_research(); duels=[dict(r) for r in d.execute("SELECT d.*,c.mutation_label,c.mutation_kind,c.spec_json FROM v57_duels d JOIN v57_challengers c USING(challenger_id) WHERE d.arena_id=? ORDER BY d.score DESC",(a['arena_id'],)).fetchall()]; d.close()
        if not duels: continue
        wins=[x for x in duels if x['outcome']=='CHALLENGER_WINS']; losses=[x for x in duels if x['outcome']=='CONTROL_WINS']; best=duels[0]
        if wins:
            verdict='EVOLVE'; next_action='PROMOTE_BEST_CHALLENGER_TO_NORMAL_SIDE_PIPELINE'
            statement=f"{len(wins)} challenger(s) beat control in SIDE evidence. Best: {best['mutation_label']} delta_rho={sf(best['delta_rho'],0):+.3f}. Parent remains frozen; child still requires V5.0/V5.5 prospective validation."
        elif losses and len(losses)==len(duels):
            verdict='CONTROL_DEFENDS'; next_action='KEEP_CONTROL_AND_TEST_ORTHOGONAL_MUTATION'
            statement=f"Control defended against all {len(duels)} completed challengers. Search a more orthogonal mutation rather than tuning the same neighborhood."
        else:
            verdict='SPECIALIST_MAP'; next_action='TEST_REGIME_SPECIALIZATION'
            statement=f"No universal replacement yet. Best challenger delta_rho={sf(best['delta_rho'],0):+.3f}; investigate whether improvement is conditional on timing/target/regime."
        evidence={'duels':len(duels),'challenger_wins':len(wins),'control_wins':len(losses),'best_challenger':best['challenger_id'],'best_delta_rho':best['delta_rho']}
        cid='K_'+core.fingerprint({'a':a['arena_id'],'v':verdict,'best':best['challenger_id']},'v57conclusion:')[:24]
        d=core.open_research(); before=d.total_changes
        d.execute('''INSERT INTO v57_conclusions(conclusion_id,arena_id,control_candidate_id,verdict,statement,evidence_json,next_action,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(conclusion_id) DO UPDATE SET statement=excluded.statement,evidence_json=excluded.evidence_json,next_action=excluded.next_action,updated_at=excluded.updated_at''',
          (cid,a['arena_id'],a['control_candidate_id'],verdict,statement,core.canonical_json(evidence),next_action,now,now)); d.commit(); made+=int(d.total_changes>before); d.close()
    return made


def display(new_arenas,new_challengers,new_duels,new_conclusions):
    d=core.open_research()
    ac=d.execute("SELECT COUNT(*) FROM v57_arenas").fetchone()[0]
    cc=d.execute("SELECT COUNT(*) FROM v57_challengers").fetchone()[0]
    dc=d.execute("SELECT COUNT(*) FROM v57_duels").fetchone()[0]
    wc=d.execute("SELECT COUNT(*) FROM v57_duels WHERE outcome='CHALLENGER_WINS'").fetchone()[0]
    top=[dict(r) for r in d.execute('''SELECT a.control_state,a.family,a.control_candidate_id,d.outcome,d.delta_rho,d.challenger_rho,c.mutation_label
      FROM v57_duels d JOIN v57_arenas a USING(arena_id) JOIN v57_challengers c USING(challenger_id)
      ORDER BY d.score DESC LIMIT 10''').fetchall()]
    cons=[dict(r) for r in d.execute("SELECT * FROM v57_conclusions ORDER BY updated_at DESC LIMIT 6").fetchall()]; d.close()
    print('\033[2J\033[H',end=''); print('='*136); print('MEMECOIN LAB — CANDIDATE & CHAMPION ARENA V5.7'); print('='*136)
    print(f"ARENAS={ac} NEW={new_arenas} | CHALLENGERS={cc} NEW={new_challengers} | DUELS={dc} NEW={new_duels} | CHALLENGER_WINS={wc} | NEW_CONCLUSIONS={new_conclusions}")
    print('\nHEAD-TO-HEAD LEADERBOARD')
    if not top: print('No completed duel yet — challengers are being evaluated by V4.9 SIDE.')
    for r in top:
        print(f"{r['outcome']:<20} {r['control_state']:<7} {r['family']:<22} delta_rho={sf(r['delta_rho'],0):+.3f} challenger_rho={sf(r['challenger_rho'],0):+.3f} {r['mutation_label'][:44]}")
    print('\nWHAT THE LAB LEARNED')
    if not cons: print('Waiting for completed head-to-heads.')
    for r in cons: print(f"{r['verdict']:<16} {r['statement'][:108]}")
    print('\nGuardrail: CONTROL is immutable. Every challenger remains SIDE evidence until it independently passes the normal freeze + future-only pipeline.')


def cycle():
    a=ensure_arenas(); c=seed_challengers(); d=refresh_duels(); k=conclusions(); display(a,c,d,k)
    state={'arenas_new':a,'challengers_new':c,'duels_new':d,'conclusions_new':k}
    db=core.open_research(); db.execute("INSERT INTO v57_state(key,value_json,updated_at) VALUES('latest',?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",(core.canonical_json(state),time.time())); db.commit(); db.close()


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try: cycle()
        except Exception as e: print('V5.7 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
