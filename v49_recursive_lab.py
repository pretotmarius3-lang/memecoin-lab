#!/usr/bin/env python3
"""Memecoin Lab V4.9 — recursive side-research lab.

Purpose
-------
Push the research organism wider without contaminating frozen prospective evidence.
V4.9 runs *beside* V4.6/V4.7/V4.8 and mines the current evidence for follow-up ideas.
It never rewrites a frozen candidate, threshold, cutoff, or prospective observation.

What it does automatically:
- expands strong signals into nearby stage/horizon/target contexts;
- tests sequence changes (feature at later stage minus earlier stage);
- tests cross-family pair blends around champions/contenders;
- tries to rescue NO_EDGE / contradictory signals inside champion-defined regimes;
- tests sign-flips for persistent negative signals;
- records lineage from parent signal/conclusion to every side experiment;
- compares each child to its parent evidence and labels IMPROVED / SAME / WORSE;
- recycles useful side results into new side hypotheses on later data watermarks.

All V4.9 results are exploratory SIDE evidence only. Promotion still requires a new
immutable freeze and forward-only validation in the prospective lane.
Research-only. No trading/signing.
"""
from __future__ import annotations

import json, math, os, signal, sqlite3, statistics, time
from collections import defaultdict
from pathlib import Path

import v41_core as core
import v41_engine as base
import v44_organism as v44

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
LOOP_S=float(os.environ.get("MEMECOIN_V49_LOOP_S","4"))
MAX_NEW=int(os.environ.get("MEMECOIN_V49_MAX_NEW","36"))
MIN_N=int(os.environ.get("MEMECOIN_V49_MIN_N","90"))
STOP=False

FEATURES=(
 "swaps","buy_ratio","gross_sol","net_sol","unique_wallets","repeat_wallet_ratio",
 "wallet_hhi","wallet_top1_share","avg_trade_sol","max_trade_sol","trade_hhi",
 "top1_trade_share","return_pct","range_pct","flow_velocity","flow_acceleration",
 "buy_ratio_delta","price_velocity")
STAGES=(10,20,30,60,120)
HORIZONS=(120,300,600,900)
TARGETS=("future_hit10","future_hit20","future_hit50","future_death50","future_migration")


def stop_handler(*_):
    global STOP; STOP=True

def sf(x,default=None):
    try:
        v=float(x); return v if math.isfinite(v) else default
    except Exception: return default

def open_v52():
    if not V52.exists(): return None
    db=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=20)
    db.row_factory=sqlite3.Row; db.execute("PRAGMA busy_timeout=20000"); return db

def rank01(vals):
    n=len(vals)
    order=sorted(range(n),key=lambda i:(vals[i],i)); out=[0.0]*n
    for r,i in enumerate(order): out[i]=0.0 if n<=1 else r/(n-1)
    return out

def init_db():
    db=core.open_research(); db.executescript("""
    CREATE TABLE IF NOT EXISTS v49_side_experiments(
      experiment_id TEXT PRIMARY KEY,
      kind TEXT NOT NULL,
      parent_key TEXT,
      parent_feature TEXT,
      spec_json TEXT NOT NULL,
      watermark_n INTEGER NOT NULL,
      status TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v49_side_results(
      experiment_id TEXT PRIMARY KEY,
      kind TEXT NOT NULL,
      n INTEGER NOT NULL,
      discovery_n INTEGER,
      holdout_n INTEGER,
      holdout_rho REAL,
      qdiff_pp REAL,
      verdict TEXT NOT NULL,
      parent_rho REAL,
      delta_rho REAL,
      comparison TEXT NOT NULL,
      metrics_json TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v49_lineage(
      experiment_id TEXT PRIMARY KEY,
      parent_key TEXT,
      reason TEXT NOT NULL,
      generation INTEGER NOT NULL,
      created_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v49_side_memory(
      memory_key TEXT PRIMARY KEY,
      memory_type TEXT NOT NULL,
      subject TEXT NOT NULL,
      statement TEXT NOT NULL,
      evidence_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    """); db.commit(); db.close()

def ready_n(stage,horizon,target):
    db=open_v52()
    if db is None:return 0
    n=db.execute(f"SELECT COUNT(*) FROM v52_outcomes WHERE stage_s=? AND horizon_s=? AND ready=1 AND {target} IS NOT NULL",(stage,horizon)).fetchone()[0]
    db.close(); return int(n)

def latest_watermark(stage,horizon,target):
    n=ready_n(stage,horizon,target)
    if n<MIN_N:return 0
    return n-(n%25)

def dataset_one(stage,horizon,target,feature,limit):
    db=open_v52();
    if db is None:return []
    rows=db.execute(f"""SELECT s.token_mint,s.cutoff_ts,s.{feature} x,o.{target} y
      FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
      WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.{feature} IS NOT NULL AND o.{target} IS NOT NULL
      ORDER BY s.cutoff_ts,s.token_mint LIMIT ?""",(stage,horizon,int(limit))).fetchall(); db.close()
    return [dict(token_mint=str(r['token_mint']),feature=float(r['x']),target=int(r['y'])) for r in rows]

def dataset_pair(stage,horizon,target,a,b,limit):
    db=open_v52();
    if db is None:return []
    rows=db.execute(f"""SELECT s.token_mint,s.cutoff_ts,s.{a} a,s.{b} b,o.{target} y
      FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
      WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.{a} IS NOT NULL AND s.{b} IS NOT NULL AND o.{target} IS NOT NULL
      ORDER BY s.cutoff_ts,s.token_mint LIMIT ?""",(stage,horizon,int(limit))).fetchall(); db.close()
    vals=[dict(r) for r in rows]
    if not vals:return []
    ra=rank01([float(r['a']) for r in vals]); rb=rank01([float(r['b']) for r in vals])
    return [dict(token_mint=str(r['token_mint']),feature=.5*(ra[i]+rb[i]),target=int(r['y'])) for i,r in enumerate(vals)]

def dataset_sequence(stage1,stage2,horizon,target,feature,limit):
    db=open_v52();
    if db is None:return []
    rows=db.execute(f"""SELECT a.token_mint,a.cutoff_ts,a.{feature} x1,b.{feature} x2,o.{target} y
      FROM v52_snapshots a JOIN v52_snapshots b ON b.token_mint=a.token_mint
      JOIN v52_outcomes o ON o.token_mint=a.token_mint AND o.stage_s=a.stage_s
      WHERE a.stage_s=? AND b.stage_s=? AND o.horizon_s=? AND o.ready=1
        AND a.{feature} IS NOT NULL AND b.{feature} IS NOT NULL AND o.{target} IS NOT NULL
      ORDER BY a.cutoff_ts,a.token_mint LIMIT ?""",(stage1,stage2,horizon,int(limit))).fetchall(); db.close()
    return [dict(token_mint=str(r['token_mint']),feature=float(r['x2'])-float(r['x1']),target=int(r['y'])) for r in rows]

def dataset_regime(stage,horizon,target,weak,gate,limit):
    db=open_v52();
    if db is None:return []
    rows=db.execute(f"""SELECT s.token_mint,s.cutoff_ts,s.{weak} weak,s.{gate} gate,o.{target} y
      FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
      WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.{weak} IS NOT NULL AND s.{gate} IS NOT NULL AND o.{target} IS NOT NULL
      ORDER BY s.cutoff_ts,s.token_mint LIMIT ?""",(stage,horizon,int(limit))).fetchall(); db.close()
    vals=[dict(r) for r in rows]
    if len(vals)<MIN_N:return []
    cut=statistics.median(float(r['gate']) for r in vals)
    hi=[r for r in vals if float(r['gate'])>=cut]
    return [dict(token_mint=str(r['token_mint']),feature=float(r['weak']),target=int(r['y'])) for r in hi]

def eval_rows(rows,flip=False):
    if flip:
        rows=[dict(r,feature=-float(r['feature'])) for r in rows]
    if len(rows)<MIN_N:return 'COLLECT_MORE',{'n':len(rows)}
    verdict,m=base.evaluate_univariate(rows,'feature')
    return verdict,m

def parent_rho_for(feature,stage,horizon,target):
    db=core.open_research()
    row=db.execute("""SELECT median_rho FROM v45_conclusions WHERE feature=? AND stage_s=? AND horizon_s=? AND target=? ORDER BY confidence DESC LIMIT 1""",(feature,stage,horizon,target)).fetchone()
    db.close(); return sf(row[0]) if row else None

def insert_exp(kind,parent_key,parent_feature,spec,watermark,reason,generation):
    identity={'kind':kind,'parent_key':parent_key,'spec':spec,'watermark':watermark}
    eid='X_'+core.fingerprint(identity,'v49:')[:24]; now=time.time(); db=core.open_research()
    before=db.total_changes
    db.execute("INSERT OR IGNORE INTO v49_side_experiments(experiment_id,kind,parent_key,parent_feature,spec_json,watermark_n,status,created_at,updated_at) VALUES(?,?,?,?,?,?,'READY',?,?)",
      (eid,kind,parent_key,parent_feature,core.canonical_json(spec),int(watermark),now,now))
    created=db.total_changes>before
    if created: db.execute("INSERT OR IGNORE INTO v49_lineage(experiment_id,parent_key,reason,generation,created_at) VALUES(?,?,?,?,?)",(eid,parent_key,reason,int(generation),now))
    db.commit(); db.close(); return eid,created

def seed():
    db=core.open_research()
    champs=[dict(r) for r in db.execute("SELECT * FROM v48_signal_rankings WHERE role IN ('CHAMPION','CONTENDER') ORDER BY live_score DESC LIMIT 24").fetchall()]
    weak=[dict(r) for r in db.execute("SELECT * FROM v45_conclusions WHERE classification IN ('NO_EDGE','CONTRADICTORY','NEGATIVE_SIGNAL') ORDER BY confidence DESC LIMIT 80").fetchall()]
    db.close(); made=0
    for c in champs:
        st=int(c['stage_s']); hz=int(c['horizon_s']); tg=str(c['target']); f=str(c['feature']); parent=c['candidate_id']
        # Nearby timing/horizon/target transfer.
        for ns in STAGES:
            if abs(ns-st)>50 or ns==st: continue
            wm=latest_watermark(ns,hz,tg)
            if wm:
                _,cr=insert_exp('TIME_NEIGHBOR',parent,f,{'stage':ns,'horizon':hz,'target':tg,'feature':f},wm,'expand strong signal into nearby stage',1); made+=int(cr)
                if made>=MAX_NEW:return made
        for nh in HORIZONS:
            if nh==hz: continue
            wm=latest_watermark(st,nh,tg)
            if wm:
                _,cr=insert_exp('HORIZON_TRANSFER',parent,f,{'stage':st,'horizon':nh,'target':tg,'feature':f},wm,'test persistence across horizon',1); made+=int(cr)
                if made>=MAX_NEW:return made
        for nt in TARGETS:
            if nt==tg: continue
            wm=latest_watermark(st,hz,nt)
            if wm:
                _,cr=insert_exp('TARGET_TRANSFER',parent,f,{'stage':st,'horizon':hz,'target':nt,'feature':f},wm,'test whether champion transfers to another outcome',1); made+=int(cr)
                if made>=MAX_NEW:return made
        # Sequence version around the winning feature.
        for ns in STAGES:
            if ns<=st: continue
            wm=latest_watermark(st,hz,tg)
            if wm:
                _,cr=insert_exp('SEQUENCE_DELTA',parent,f,{'stage1':st,'stage2':ns,'horizon':hz,'target':tg,'feature':f},wm,'test trajectory shape instead of level',2); made+=int(cr)
                if made>=MAX_NEW:return made
        # Cross-family blends with other strong signals in same context.
        for d in champs:
            if d['candidate_id']==parent or int(d['stage_s'])!=st or int(d['horizon_s'])!=hz or str(d['target'])!=tg: continue
            if str(d['family'])==str(c['family']): continue
            wm=latest_watermark(st,hz,tg)
            if wm:
                feats=sorted([f,str(d['feature'])])
                _,cr=insert_exp('CROSS_FAMILY_BLEND',parent,f,{'stage':st,'horizon':hz,'target':tg,'features':feats},wm,'combine independent strong families',2); made+=int(cr)
                if made>=MAX_NEW:return made
        # Rescue weak/contradictory ideas inside champion regime.
        for w in weak:
            if int(w['stage_s'])!=st or int(w['horizon_s'])!=hz or str(w['target'])!=tg: continue
            wf=str(w['feature'])
            if wf==f: continue
            wm=latest_watermark(st,hz,tg)
            if wm:
                _,cr=insert_exp('REGIME_RESCUE',str(w['conclusion_key']),wf,{'stage':st,'horizon':hz,'target':tg,'weak':wf,'gate':f},wm,'retest failed idea only inside champion regime',3); made+=int(cr)
                if made>=MAX_NEW:return made
            if str(w['classification'])=='NEGATIVE_SIGNAL' and wm:
                _,cr=insert_exp('SIGN_FLIP',str(w['conclusion_key']),wf,{'stage':st,'horizon':hz,'target':tg,'feature':wf},wm,'test persistent negative relation as contrarian signal',3); made+=int(cr)
                if made>=MAX_NEW:return made
    return made

def run_ready(limit=48):
    db=core.open_research(); rows=[dict(r) for r in db.execute("SELECT * FROM v49_side_experiments WHERE status='READY' ORDER BY created_at LIMIT ?",(int(limit),)).fetchall()]; db.close(); done=0
    for x in rows:
        spec=json.loads(x['spec_json']); kind=x['kind']; wm=int(x['watermark_n'])
        if kind in ('TIME_NEIGHBOR','HORIZON_TRANSFER','TARGET_TRANSFER'):
            data=dataset_one(spec['stage'],spec['horizon'],spec['target'],spec['feature'],wm); flip=False
        elif kind=='SIGN_FLIP':
            data=dataset_one(spec['stage'],spec['horizon'],spec['target'],spec['feature'],wm); flip=True
        elif kind=='CROSS_FAMILY_BLEND':
            data=dataset_pair(spec['stage'],spec['horizon'],spec['target'],spec['features'][0],spec['features'][1],wm); flip=False
        elif kind=='SEQUENCE_DELTA':
            data=dataset_sequence(spec['stage1'],spec['stage2'],spec['horizon'],spec['target'],spec['feature'],wm); flip=False
        elif kind=='REGIME_RESCUE':
            data=dataset_regime(spec['stage'],spec['horizon'],spec['target'],spec['weak'],spec['gate'],wm); flip=False
        else: continue
        verdict,m=eval_rows(data,flip)
        hr=sf(m.get('holdout_signed_rho')); parent=parent_rho_for(x['parent_feature'],spec.get('stage',spec.get('stage1')),spec['horizon'],spec['target'])
        delta=None if hr is None or parent is None else hr-parent
        comp='UNKNOWN' if delta is None else ('IMPROVED' if delta>=.05 else ('WORSE' if delta<=-.05 else 'SAME'))
        now=time.time(); db=core.open_research(); db.execute('BEGIN IMMEDIATE')
        try:
            db.execute("INSERT OR REPLACE INTO v49_side_results(experiment_id,kind,n,discovery_n,holdout_n,holdout_rho,qdiff_pp,verdict,parent_rho,delta_rho,comparison,metrics_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
              (x['experiment_id'],kind,len(data),m.get('discovery_n'),m.get('holdout_n'),hr,m.get('qdiff_pp'),verdict,parent,delta,comp,core.canonical_json(m),now,now))
            db.execute("UPDATE v49_side_experiments SET status='DONE',updated_at=? WHERE experiment_id=?",(now,x['experiment_id']))
            if comp=='IMPROVED' or (hr is not None and hr>=.15):
                mk='M_'+core.fingerprint({'x':x['experiment_id'],'wm':wm},'v49mem:')[:22]
                statement=f"{kind} improved/extended {x['parent_feature']}: holdout_rho={hr}, parent_rho={parent}, delta={delta}, verdict={verdict}"
                db.execute("INSERT OR REPLACE INTO v49_side_memory(memory_key,memory_type,subject,statement,evidence_json,updated_at) VALUES(?,?,?,?,?,?)",
                  (mk,'SIDE_DISCOVERY',x['experiment_id'],statement,core.canonical_json({'spec':spec,'metrics':m,'comparison':comp}),now))
            db.commit()
        except BaseException:
            db.rollback(); raise
        finally: db.close()
        done+=1
    return done

def display(seeded,ran):
    db=core.open_research()
    counts={r['status']:r['n'] for r in db.execute("SELECT status,COUNT(*) n FROM v49_side_experiments GROUP BY status")}
    kinds=[dict(r) for r in db.execute("SELECT kind,COUNT(*) n,SUM(CASE WHEN comparison='IMPROVED' THEN 1 ELSE 0 END) improved,MAX(holdout_rho) best FROM v49_side_results GROUP BY kind ORDER BY improved DESC,best DESC").fetchall()]
    top=[dict(r) for r in db.execute("SELECT r.*,e.parent_feature,e.spec_json FROM v49_side_results r JOIN v49_side_experiments e USING(experiment_id) WHERE r.holdout_rho IS NOT NULL ORDER BY r.holdout_rho DESC LIMIT 10").fetchall()]
    mem=db.execute("SELECT COUNT(*) FROM v49_side_memory").fetchone()[0]; db.close()
    print('\033[2J\033[H',end=''); print('='*132); print('MEMECOIN LAB — RECURSIVE SIDE-RESEARCH LAB V4.9'); print('='*132)
    print(f"READY={counts.get('READY',0)} | DONE={counts.get('DONE',0)} | NEW SEEDED={seeded} | RAN={ran} | SIDE MEMORIES={mem}")
    print('\nWHERE THE SIDE LAB IS DIGGING')
    for k in kinds: print(f"{k['kind']:<24} N={k['n']:>5} IMPROVED={k['improved'] or 0:>4} BEST_RHO={k['best'] if k['best'] is not None else '—'}")
    print('\nTOP SIDE DISCOVERIES')
    for i,r in enumerate(top,1): print(f"#{i:02d} {r['kind']:<20} parent={r['parent_feature']:<22} rho={r['holdout_rho']:.3f} delta={r['delta_rho'] if r['delta_rho'] is not None else 0:+.3f} {r['comparison']:<9} {r['verdict']}")
    print('\nGuardrail: side results are exploratory only. No frozen/prospective rule is changed here.')

def main():
    signal.signal(signal.SIGINT,stop_handler); signal.signal(signal.SIGTERM,stop_handler); init_db()
    while not STOP:
        seeded=seed(); ran=run_ready(); display(seeded,ran); time.sleep(LOOP_S)
    print('V4.9 side lab stopped cleanly')

if __name__=='__main__': main()
