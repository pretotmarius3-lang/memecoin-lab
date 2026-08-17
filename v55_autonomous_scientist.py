#!/usr/bin/env python3
"""Memecoin Lab V5.5 — Autonomous Scientist.

Closes the scientific loop without contaminating prospective evidence:
  V4.9 exploratory side result -> V5.0 promotion proposal -> immutable V5.5 freeze
  -> future-only observations -> WAITING/WATCH/PASS/FAIL -> lineage + memory
  -> new exploratory mutation requests for the side lab.

V5.5 supports all current V4.9 experiment shapes with transformations calibrated
ONLY on data available at freeze time. Frozen candidate rows are append-only in
meaning: rule_json / data_cutoff / threshold / direction are never updated.
Research-only. No signing. No trading.
"""
from __future__ import annotations
import bisect, json, math, os, signal, sqlite3, statistics, time
from collections import defaultdict
from pathlib import Path

import v41_core as core
import v41_engine as base

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
LOOP=float(os.environ.get("MEMECOIN_V55_LOOP_S","4"))
MAX_FREEZE=int(os.environ.get("MEMECOIN_V55_MAX_FREEZE","24"))
MIN_TRAIN=int(os.environ.get("MEMECOIN_V55_MIN_TRAIN","90"))
MIN_WATCH=int(os.environ.get("MEMECOIN_V55_MIN_WATCH","40"))
PASS_N=int(os.environ.get("MEMECOIN_V55_PASS_N","100"))
FAIL_N=int(os.environ.get("MEMECOIN_V55_FAIL_N","100"))
STOP=False

def stop(*_):
    global STOP; STOP=True

def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def db52():
    if not V52.exists():return None
    c=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=30); c.row_factory=sqlite3.Row; c.execute("PRAGMA busy_timeout=30000"); return c

def percentile(xs,q):
    xs=sorted(float(x) for x in xs if sf(x) is not None)
    if not xs:return None
    if len(xs)==1:return xs[0]
    p=(len(xs)-1)*q; lo=math.floor(p); hi=math.ceil(p)
    return xs[lo] if lo==hi else xs[lo]*(hi-p)+xs[hi]*(p-lo)

def ecdf(sorted_train,x):
    if not sorted_train:return 0.5
    return bisect.bisect_right(sorted_train,float(x))/len(sorted_train)

def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v55_candidates(
      candidate_id TEXT PRIMARY KEY,
      promotion_id TEXT NOT NULL UNIQUE,
      experiment_id TEXT NOT NULL,
      parent_key TEXT,
      kind TEXT NOT NULL,
      spec_json TEXT NOT NULL,
      rule_json TEXT NOT NULL,
      data_cutoff REAL NOT NULL,
      train_n INTEGER NOT NULL,
      train_positive_rate REAL,
      train_rho REAL,
      direction REAL NOT NULL,
      threshold REAL NOT NULL,
      state TEXT NOT NULL,
      frozen_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v55_observations(
      candidate_id TEXT NOT NULL,
      token_mint TEXT NOT NULL,
      cutoff_ts REAL NOT NULL,
      score REAL NOT NULL,
      predicted_positive INTEGER NOT NULL,
      actual INTEGER NOT NULL,
      observed_at REAL NOT NULL,
      PRIMARY KEY(candidate_id,token_mint));
    CREATE INDEX IF NOT EXISTS idx_v55_obs ON v55_observations(candidate_id,cutoff_ts);
    CREATE TABLE IF NOT EXISTS v55_beliefs(
      candidate_id TEXT PRIMARY KEY,
      state TEXT NOT NULL,n INTEGER NOT NULL,positives INTEGER NOT NULL,
      predicted_n INTEGER NOT NULL,predicted_hits INTEGER NOT NULL,
      baseline_rate REAL,precision REAL,lift REAL,prospective_rho REAL,
      confidence REAL NOT NULL,statement TEXT NOT NULL,metrics_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v55_lineage(
      child_key TEXT PRIMARY KEY,parent_key TEXT,edge_type TEXT NOT NULL,
      generation INTEGER NOT NULL,evidence_json TEXT NOT NULL,created_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v55_memory(
      memory_key TEXT PRIMARY KEY,memory_type TEXT NOT NULL,subject TEXT NOT NULL,
      statement TEXT NOT NULL,evidence_json TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v55_mutation_requests(
      request_id TEXT PRIMARY KEY,parent_candidate_id TEXT NOT NULL,reason TEXT NOT NULL,
      mutation_kind TEXT NOT NULL,spec_json TEXT NOT NULL,priority REAL NOT NULL,
      state TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v55_state(key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()

def spec_context(kind,s):
    if kind=='SEQUENCE_DELTA': return int(s['stage1']),int(s['horizon']),str(s['target'])
    return int(s['stage']),int(s['horizon']),str(s['target'])

def historical_rows(kind,s):
    c=db52();
    if c is None:return []
    hz=int(s['horizon']); tg=str(s['target'])
    if kind in ('TIME_NEIGHBOR','HORIZON_TRANSFER','TARGET_TRANSFER','SIGN_FLIP'):
        st=int(s['stage']); f=str(s['feature'])
        q=f'''SELECT s.token_mint,s.cutoff_ts,s.{f} x,o.{tg} y FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.{f} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY s.cutoff_ts,s.token_mint'''
        rs=c.execute(q,(st,hz)).fetchall()
    elif kind=='CROSS_FAMILY_BLEND':
        st=int(s['stage']); a,b=map(str,s['features'])
        q=f'''SELECT s.token_mint,s.cutoff_ts,s.{a} a,s.{b} b,o.{tg} y FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.{a} IS NOT NULL AND s.{b} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY s.cutoff_ts,s.token_mint'''
        rs=c.execute(q,(st,hz)).fetchall()
    elif kind=='SEQUENCE_DELTA':
        s1,s2=int(s['stage1']),int(s['stage2']); f=str(s['feature'])
        q=f'''SELECT a.token_mint,a.cutoff_ts,a.{f} x1,b.{f} x2,o.{tg} y FROM v52_snapshots a JOIN v52_snapshots b ON b.token_mint=a.token_mint JOIN v52_outcomes o ON o.token_mint=a.token_mint AND o.stage_s=a.stage_s WHERE a.stage_s=? AND b.stage_s=? AND o.horizon_s=? AND o.ready=1 AND a.{f} IS NOT NULL AND b.{f} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY a.cutoff_ts,a.token_mint'''
        rs=c.execute(q,(s1,s2,hz)).fetchall()
    elif kind=='REGIME_RESCUE':
        st=int(s['stage']); w,g=str(s['weak']),str(s['gate'])
        q=f'''SELECT s.token_mint,s.cutoff_ts,s.{w} weak,s.{g} gate,o.{tg} y FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.{w} IS NOT NULL AND s.{g} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY s.cutoff_ts,s.token_mint'''
        rs=c.execute(q,(st,hz)).fetchall()
    else: rs=[]
    c.close(); return [dict(r) for r in rs]

def calibrate(kind,s,rows):
    if len(rows)<MIN_TRAIN:return None
    cutoff=max(float(r['cutoff_ts']) for r in rows); y=[int(r['y']) for r in rows]; rule={}
    if kind in ('TIME_NEIGHBOR','HORIZON_TRANSFER','TARGET_TRANSFER','SIGN_FLIP'):
        raw=[float(r['x']) for r in rows]; rho=base.spearman(raw,y); direction=(-1.0 if kind=='SIGN_FLIP' else (1.0 if sf(rho,0)>=0 else -1.0)); score=[direction*x for x in raw]
    elif kind=='SEQUENCE_DELTA':
        raw=[float(r['x2'])-float(r['x1']) for r in rows]; rho=base.spearman(raw,y); direction=1.0 if sf(rho,0)>=0 else -1.0; score=[direction*x for x in raw]
    elif kind=='CROSS_FAMILY_BLEND':
        aa=sorted(float(r['a']) for r in rows); bb=sorted(float(r['b']) for r in rows); rule={'a_train':aa,'b_train':bb}; raw=[.5*(ecdf(aa,r['a'])+ecdf(bb,r['b'])) for r in rows]; rho=base.spearman(raw,y); direction=1.0 if sf(rho,0)>=0 else -1.0; score=[direction*x for x in raw]
    elif kind=='REGIME_RESCUE':
        gate_cut=statistics.median(float(r['gate']) for r in rows); gated=[r for r in rows if float(r['gate'])>=gate_cut]
        if len(gated)<MIN_TRAIN//2:return None
        raw=[float(r['weak']) for r in gated]; yy=[int(r['y']) for r in gated]; rho=base.spearman(raw,yy); direction=1.0 if sf(rho,0)>=0 else -1.0; score=[direction*x for x in raw]; y=yy; rows=gated; rule={'gate_cut':gate_cut}
    else:return None
    threshold=percentile(score,.75)
    if threshold is None:return None
    return {'data_cutoff':cutoff,'train_n':len(rows),'base':sum(y)/len(y),'rho':rho,'direction':direction,'threshold':threshold,'rule':rule}

def freeze_promotions():
    d=core.open_research(); names={r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'v50_promotions' not in names: d.close(); return 0
    rows=[dict(r) for r in d.execute("SELECT * FROM v50_promotions WHERE state='PROPOSED' ORDER BY priority DESC,created_at LIMIT ?",(MAX_FREEZE,)).fetchall()]; d.close(); made=0
    for p in rows:
        s=json.loads(p['spec_json']); kind=str(p['kind']); hist=historical_rows(kind,s); cal=calibrate(kind,s,hist)
        if cal is None:continue
        cid='F_'+core.fingerprint({'promotion':p['promotion_id'],'cut':cal['data_cutoff'],'rule':cal['rule'],'thr':cal['threshold']},'v55:')[:24]; now=time.time()
        rule={'kind':kind,'spec':s,'calibration':cal['rule']}; d=core.open_research(); d.execute('BEGIN IMMEDIATE')
        try:
            before=d.total_changes
            d.execute('''INSERT OR IGNORE INTO v55_candidates(candidate_id,promotion_id,experiment_id,parent_key,kind,spec_json,rule_json,data_cutoff,train_n,train_positive_rate,train_rho,direction,threshold,state,frozen_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'FROZEN',?,?)''',(cid,p['promotion_id'],p['experiment_id'],p['parent_key'],kind,p['spec_json'],core.canonical_json(rule),cal['data_cutoff'],cal['train_n'],cal['base'],cal['rho'],cal['direction'],cal['threshold'],now,now))
            if d.total_changes>before:
                made+=1; d.execute("INSERT OR IGNORE INTO v55_lineage VALUES(?,?,?,?,?,?)",(cid,p['parent_key'],'PROMOTED_AND_REFROZEN',1,core.canonical_json({'promotion_id':p['promotion_id'],'experiment_id':p['experiment_id']}),now))
                d.execute("UPDATE v50_promotions SET state='FROZEN',freeze_cutoff=?,updated_at=? WHERE promotion_id=? AND state='PROPOSED'",(cal['data_cutoff'],now,p['promotion_id']))
            d.commit()
        except BaseException:d.rollback(); raise
        finally:d.close()
    return made

def prospective_rows(c):
    s=json.loads(c['spec_json']); kind=c['kind']; cutoff=float(c['data_cutoff']); hz=int(s['horizon']); tg=str(s['target']); qargs=[]; xexpr=''; joins=''; where=''
    v=db52();
    if v is None:return []
    if kind in ('TIME_NEIGHBOR','HORIZON_TRANSFER','TARGET_TRANSFER','SIGN_FLIP'):
        st=int(s['stage']); f=str(s['feature']); sql=f'''SELECT s.token_mint,s.cutoff_ts,s.{f} x,o.{tg} y FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.cutoff_ts>? AND s.{f} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY s.cutoff_ts'''; rs=v.execute(sql,(st,hz,cutoff)).fetchall()
    elif kind=='CROSS_FAMILY_BLEND':
        st=int(s['stage']); a,b=map(str,s['features']); sql=f'''SELECT s.token_mint,s.cutoff_ts,s.{a} a,s.{b} b,o.{tg} y FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.cutoff_ts>? AND s.{a} IS NOT NULL AND s.{b} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY s.cutoff_ts'''; rs=v.execute(sql,(st,hz,cutoff)).fetchall()
    elif kind=='SEQUENCE_DELTA':
        s1,s2=int(s['stage1']),int(s['stage2']); f=str(s['feature']); sql=f'''SELECT a.token_mint,a.cutoff_ts,a.{f} x1,b.{f} x2,o.{tg} y FROM v52_snapshots a JOIN v52_snapshots b ON b.token_mint=a.token_mint JOIN v52_outcomes o ON o.token_mint=a.token_mint AND o.stage_s=a.stage_s WHERE a.stage_s=? AND b.stage_s=? AND o.horizon_s=? AND o.ready=1 AND a.cutoff_ts>? AND a.{f} IS NOT NULL AND b.{f} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY a.cutoff_ts'''; rs=v.execute(sql,(s1,s2,hz,cutoff)).fetchall()
    elif kind=='REGIME_RESCUE':
        st=int(s['stage']); w,g=str(s['weak']),str(s['gate']); sql=f'''SELECT s.token_mint,s.cutoff_ts,s.{w} weak,s.{g} gate,o.{tg} y FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.cutoff_ts>? AND s.{w} IS NOT NULL AND s.{g} IS NOT NULL AND o.{tg} IS NOT NULL ORDER BY s.cutoff_ts'''; rs=v.execute(sql,(st,hz,cutoff)).fetchall()
    else:rs=[]
    v.close(); return [dict(r) for r in rs]

def frozen_score(c,r):
    rule=json.loads(c['rule_json']); kind=c['kind']; direction=float(c['direction'])
    if kind in ('TIME_NEIGHBOR','HORIZON_TRANSFER','TARGET_TRANSFER','SIGN_FLIP'): raw=float(r['x'])
    elif kind=='SEQUENCE_DELTA': raw=float(r['x2'])-float(r['x1'])
    elif kind=='CROSS_FAMILY_BLEND':
        cr=rule['calibration']; raw=.5*(ecdf(cr['a_train'],r['a'])+ecdf(cr['b_train'],r['b']))
    elif kind=='REGIME_RESCUE':
        if float(r['gate'])<float(rule['calibration']['gate_cut']):return None
        raw=float(r['weak'])
    else:return None
    return direction*raw

def refresh_observations():
    d=core.open_research(); cs=[dict(r) for r in d.execute("SELECT * FROM v55_candidates").fetchall()]; d.close(); inserted=0
    for c in cs:
        rs=prospective_rows(c); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
        try:
            for r in rs:
                sc=frozen_score(c,r)
                if sc is None:continue
                before=d.total_changes; d.execute("INSERT OR IGNORE INTO v55_observations VALUES(?,?,?,?,?,?,?)",(c['candidate_id'],str(r['token_mint']),float(r['cutoff_ts']),sc,int(sc>=float(c['threshold'])),int(r['y']),time.time())); inserted+=d.total_changes-before
            d.commit()
        except BaseException:d.rollback(); raise
        finally:d.close()
    return inserted

def belief(c):
    d=core.open_research(); rs=[dict(r) for r in d.execute("SELECT * FROM v55_observations WHERE candidate_id=? ORDER BY cutoff_ts",(c['candidate_id'],)).fetchall()]; d.close(); n=len(rs); pos=sum(int(r['actual']) for r in rs); pred=[r for r in rs if r['predicted_positive']]; pn=len(pred); ph=sum(int(r['actual']) for r in pred); baseline=pos/n if n else None; precision=ph/pn if pn else None; lift=(precision/baseline) if precision is not None and baseline else None; rho=base.spearman([r['score'] for r in rs],[r['actual'] for r in rs]) if n>=8 else None
    state='WAITING'
    if n>=MIN_WATCH:state='WATCH'
    if n>=PASS_N and sf(rho,-9)>=.08 and sf(lift,0)>=1.20 and pn>=10:state='PASS'
    elif n>=FAIL_N and (sf(rho,9)<=.01 or (lift is not None and lift<=1.05)):state='FAIL'
    sample=min(1,n/max(1,PASS_N)); strength=min(1,max(0,sf(rho,0))/.20); ls=min(1,max(0,sf(lift,1)-1)/.5); conf=.45*sample+.30*strength+.25*ls
    m={'n':n,'positives':pos,'predicted_n':pn,'predicted_hits':ph,'baseline':baseline,'precision':precision,'lift':lift,'rho':rho,'cutoff':c['data_cutoff'],'threshold':c['threshold']}; st=f"{c['kind']} {state}: future-only n={n}, rho={rho}, lift={lift}, precision={precision}, baseline={baseline}"; return state,conf,st,m

def mutation_specs(c,state):
    s=json.loads(c['spec_json']); out=[]
    # These are EXPLORATORY requests only; they can never modify this frozen candidate.
    if state=='FAIL':
        if 'horizon' in s:
            for h in (120,300,600,900):
                if h!=int(s['horizon']): out.append(('HORIZON_TRANSFER',dict(s,horizon=h),'prospective failure: test horizon dependence',.65))
        if 'feature' in s and c['kind']!='SIGN_FLIP': out.append(('SIGN_FLIP',dict(s),'prospective failure: test sign instability/contrarian relation',.55))
    elif state=='PASS':
        if 'stage' in s:
            for st in (10,20,30,60,120):
                if st!=int(s['stage']) and abs(st-int(s['stage']))<=50: out.append(('TIME_NEIGHBOR',dict(s,stage=st),'prospective pass: map timing neighborhood',.75))
    return out[:4]

def refresh_beliefs_and_mutations():
    d=core.open_research(); cs=[dict(r) for r in d.execute("SELECT * FROM v55_candidates").fetchall()]; d.close(); counts=defaultdict(int); newmut=0; now=time.time()
    for c in cs:
        state,conf,statement,m=belief(c); counts[state]+=1; d=core.open_research(); d.execute('BEGIN IMMEDIATE')
        try:
            prev=d.execute("SELECT state FROM v55_beliefs WHERE candidate_id=?",(c['candidate_id'],)).fetchone(); old=prev[0] if prev else None
            d.execute('''INSERT INTO v55_beliefs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_id) DO UPDATE SET state=excluded.state,n=excluded.n,positives=excluded.positives,predicted_n=excluded.predicted_n,predicted_hits=excluded.predicted_hits,baseline_rate=excluded.baseline_rate,precision=excluded.precision,lift=excluded.lift,prospective_rho=excluded.prospective_rho,confidence=excluded.confidence,statement=excluded.statement,metrics_json=excluded.metrics_json,updated_at=excluded.updated_at''',(c['candidate_id'],state,m['n'],m['positives'],m['predicted_n'],m['predicted_hits'],m['baseline'],m['precision'],m['lift'],m['rho'],conf,statement,core.canonical_json(m),now))
            d.execute("UPDATE v55_candidates SET state=?,updated_at=? WHERE candidate_id=?",(state,now,c['candidate_id']))
            if old!=state:
                mk='M_'+core.fingerprint({'c':c['candidate_id'],'state':state},'v55mem:')[:24]; d.execute("INSERT OR IGNORE INTO v55_memory VALUES(?,?,?,?,?,?,?)",(mk,'PROSPECTIVE_STATE',c['candidate_id'],statement,core.canonical_json(m),now,now))
                if state in ('PASS','FAIL'):
                    d.execute("UPDATE v50_promotions SET state=?,updated_at=? WHERE promotion_id=?",(state,now,c['promotion_id']))
                    for mkind,mspec,reason,pri in mutation_specs(c,state):
                        rid='R_'+core.fingerprint({'p':c['candidate_id'],'k':mkind,'s':mspec},'v55mut:')[:24]; before=d.total_changes; d.execute("INSERT OR IGNORE INTO v55_mutation_requests VALUES(?,?,?,?,?,'OPEN',?,?)",(rid,c['candidate_id'],reason,mkind,core.canonical_json(mspec),pri,now,now)); newmut+=int(d.total_changes>before)
            d.commit()
        except BaseException:d.rollback(); raise
        finally:d.close()
    return counts,newmut

def export_mutations_to_v49(limit=24):
    d=core.open_research(); names={r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'v49_side_experiments' not in names:d.close(); return 0
    req=[dict(r) for r in d.execute("SELECT * FROM v55_mutation_requests WHERE state='OPEN' ORDER BY priority DESC,created_at LIMIT ?",(limit,)).fetchall()]; d.close(); made=0
    for r in req:
        s=json.loads(r['spec_json']); kind=r['mutation_kind']; stage=int(s.get('stage',s.get('stage1',10))); hz=int(s['horizon']); tg=s['target']; v=db52()
        if v is None:continue
        n=v.execute(f"SELECT COUNT(*) FROM v52_outcomes WHERE stage_s=? AND horizon_s=? AND ready=1 AND {tg} IS NOT NULL",(stage,hz)).fetchone()[0]; v.close(); wm=int(n-(n%25))
        if wm<MIN_TRAIN:continue
        eid='X_'+core.fingerprint({'kind':kind,'parent':r['parent_candidate_id'],'spec':s,'wm':wm},'v55v49:')[:24]; now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
        try:
            before=d.total_changes; parent_feature=s.get('feature') or s.get('weak') or (s.get('features') or ['COMPOSITE'])[0]
            d.execute("INSERT OR IGNORE INTO v49_side_experiments(experiment_id,kind,parent_key,parent_feature,spec_json,watermark_n,status,created_at,updated_at) VALUES(?,?,?,?,?,?,'READY',?,?)",(eid,kind,r['parent_candidate_id'],parent_feature,core.canonical_json(s),wm,now,now))
            if d.total_changes>before:
                made+=1; d.execute("INSERT OR IGNORE INTO v49_lineage(experiment_id,parent_key,reason,generation,created_at) VALUES(?,?,?,?,?)",(eid,r['parent_candidate_id'],r['reason'],2,now)); d.execute("UPDATE v55_mutation_requests SET state='EXPORTED',updated_at=? WHERE request_id=?",(now,r['request_id']))
            d.commit()
        except BaseException:d.rollback(); raise
        finally:d.close()
    return made

def display(frozen,obs,counts,mut,exported):
    d=core.open_research(); total=d.execute("SELECT COUNT(*) FROM v55_candidates").fetchone()[0]; top=[dict(r) for r in d.execute("SELECT b.*,c.kind FROM v55_beliefs b JOIN v55_candidates c USING(candidate_id) ORDER BY CASE b.state WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 WHEN 'WAITING' THEN 2 ELSE 3 END,b.confidence DESC LIMIT 10").fetchall()]; openm=d.execute("SELECT COUNT(*) FROM v55_mutation_requests WHERE state='OPEN'").fetchone()[0]; d.close()
    print('\033[2J\033[H',end=''); print('='*128); print('MEMECOIN LAB — AUTONOMOUS SCIENTIST V5.5'); print('='*128)
    print(f"FROZEN_TOTAL={total} NEW_FREEZES={frozen} NEW_OBS={obs} | WAIT={counts.get('WAITING',0)} WATCH={counts.get('WATCH',0)} PASS={counts.get('PASS',0)} FAIL={counts.get('FAIL',0)}")
    print(f"MUTATIONS new={mut} exported_to_side_lab={exported} open={openm}")
    print('\nTOP PROSPECTIVE CHILDREN')
    for x in top: print(f"{x['state']:<7} {x['kind']:<20} n={x['n']:<4} rho={sf(x['prospective_rho'],0):+.3f} lift={sf(x['lift'],0):.2f} conf={x['confidence']:.2f}")
    print('\nGuardrail: every score/threshold/transform above is frozen before these observations. Mutations return to SIDE exploration; they never rewrite a frozen test.')

def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:
            frozen=freeze_promotions(); obs=refresh_observations(); counts,mut=refresh_beliefs_and_mutations(); exported=export_mutations_to_v49(); display(frozen,obs,counts,mut,exported)
        except Exception as e: print('V5.5 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__':main()
