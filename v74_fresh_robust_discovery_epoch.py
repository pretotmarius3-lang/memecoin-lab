#!/usr/bin/env python3
"""MEMECOIN LAB — FRESH ROBUST DISCOVERY EPOCH V7.4

Starts a NEW research-only discovery epoch at the latest canonical snapshot on
first launch. Only post-start data is eligible. The epoch is intentionally
separate from R64/WALLET/FLOW and cannot modify any frozen rule.

This is DISCOVERY, not confirmation. Threshold/direction are learned on TRAIN
only; deterministic token HOLDOUT is used as a research screen. Any surviving
candidate still requires a separate immutable freeze + fresh future-only arena.

V7.4 incorporates lessons from FLOW/WALLET:
- larger train/holdout requirements;
- minimum selected holdout n and execution fill;
- explicit lift/PF/expectancy gates;
- multiple-testing / small-sample penalties;
- known failed-forward feature quarantine;
- known weakening feature penalty;
- family-level independent-regime replication before SHORTLIST.
"""
from __future__ import annotations
import hashlib, json, math, os, signal, sqlite3, statistics, time
from collections import defaultdict
from pathlib import Path

import v41_core as core
import v60_economic_edge_discovery_engine as v60
import v61_economic_champion_consolidator as v61
import v63_next_fill_economic_edge_engine as v63

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
OUT=ROOT/'v74_fresh_robust_discovery.db'
LOOP=float(os.environ.get('MEMECOIN_V74_LOOP_S','3'))
BATCH=int(os.environ.get('MEMECOIN_V74_BATCH','12'))
MIN_TRAIN=int(os.environ.get('MEMECOIN_V74_MIN_TRAIN','40'))
MIN_HOLDOUT=int(os.environ.get('MEMECOIN_V74_MIN_HOLDOUT','15'))
MIN_SELECTED_HO=int(os.environ.get('MEMECOIN_V74_MIN_SELECTED_HO','10'))
MIN_FILL=float(os.environ.get('MEMECOIN_V74_MIN_FILL','0.20'))
MIN_LIFT=float(os.environ.get('MEMECOIN_V74_MIN_LIFT','0.50'))
MIN_PF=float(os.environ.get('MEMECOIN_V74_MIN_PF','1.15'))
MIN_FAMILY_INDEP=int(os.environ.get('MEMECOIN_V74_MIN_FAMILY_INDEP','2'))
MIN_FAMILY_UNIQUE=int(os.environ.get('MEMECOIN_V74_MIN_FAMILY_UNIQUE','16'))
MAX_PAIR_OVERLAP=float(os.environ.get('MEMECOIN_V74_MAX_PAIR_OVERLAP','0.70'))
STAGES=v63.STAGES; HORIZONS=v63.HORIZONS; BARRIERS=v63.BARRIERS; FEATURES=v63.FEATURES
STOP=False


def stop(*_):
 global STOP; STOP=True

def sf(x,d=None):
 try:
  v=float(x); return v if math.isfinite(v) else d
 except:return d

def ro_v52():
 d=sqlite3.connect(f'file:{V52}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def max_cutoff():
 d=ro_v52();x=d.execute('SELECT MAX(cutoff_ts) FROM v52_snapshots').fetchone()[0];d.close();return sf(x,0.0) or 0.0

def odb():
 d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=30000');return d

def init():
 d=odb();d.executescript('''
 CREATE TABLE IF NOT EXISTS epoch(
  epoch_id TEXT PRIMARY KEY,started_at REAL NOT NULL,start_cutoff REAL NOT NULL,created_at REAL NOT NULL,
  rule TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS experiments(
  experiment_id TEXT PRIMARY KEY,epoch_id TEXT NOT NULL,stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,
  tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,feature TEXT NOT NULL,family TEXT NOT NULL,
  last_eval_cutoff REAL NOT NULL DEFAULT 0,last_eval_at REAL NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS results(
  experiment_id TEXT PRIMARY KEY,train_n INTEGER,holdout_n INTEGER,selected_train INTEGER,selected_holdout INTEGER,
  direction REAL,threshold REAL,threshold_q REAL,holdout_expectancy REAL,holdout_pf REAL,holdout_win REAL,
  baseline_expectancy REAL,expectancy_lift REAL,fill_rate REAL,median_fill_delay REAL,
  robust_score REAL,status TEXT,reasons_json TEXT,selected_tokens_json TEXT,updated_at REAL);
 CREATE TABLE IF NOT EXISTS family_summary(
  family TEXT PRIMARY KEY,positive_instances INTEGER,independent_instances INTEGER,unique_tokens INTEGER,
  max_pair_overlap REAL,median_expectancy REAL,median_pf REAL,status TEXT,updated_at REAL);
 ''')
 e=d.execute('SELECT * FROM epoch ORDER BY created_at LIMIT 1').fetchone()
 if not e:
  start=max_cutoff();now=time.time();eid='C74_'+hashlib.sha256(f'{start}|fresh_robust_v74'.encode()).hexdigest()[:20]
  rule=f'TRAIN>={MIN_TRAIN};HO>={MIN_HOLDOUT};SEL_HO>={MIN_SELECTED_HO};FILL>={MIN_FILL};LIFT>={MIN_LIFT};PF>={MIN_PF}'
  d.execute('INSERT INTO epoch VALUES(?,?,?,?,?)',(eid,now,start,now,rule));e={'epoch_id':eid,'started_at':now,'start_cutoff':start,'created_at':now,'rule':rule}
 else:e=dict(e)
 for st in STAGES:
  for hz in HORIZONS:
   for tp,sl in BARRIERS:
    for feat in FEATURES:
     fam=v61.FAMILY.get(feat,'OTHER');xid='E74_'+hashlib.sha256(f"{e['epoch_id']}|{st}|{hz}|{tp}|{sl}|{feat}".encode()).hexdigest()[:22]
     d.execute('INSERT OR IGNORE INTO experiments(experiment_id,epoch_id,stage_s,horizon_s,tp_pct,sl_pct,feature,family) VALUES(?,?,?,?,?,?,?,?)',(xid,e['epoch_id'],st,hz,tp,sl,feat,fam))
 d.commit();d.close();return e

def r64_benchmark():
 d=core.open_research();r=d.execute('SELECT done,expectancy,profit_factor,fill_rate FROM v64_forward_summary LIMIT 1').fetchone();d.close();return dict(r) if r else {}

def forward_feature_states():
 d=core.open_research();out={}
 if d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='v673_forward_summary'").fetchone():
  for s in d.execute('SELECT * FROM v673_forward_summary'):
   x=dict(s);r=d.execute('SELECT feature FROM v672_frozen_challengers WHERE challenger_id=?',(x['challenger_id'],)).fetchone()
   if not r:r=d.execute('SELECT feature FROM v6721_corrected_freezes WHERE challenger_id=?',(x['challenger_id'],)).fetchone()
   if not r:continue
   exp=sf(x.get('expectancy'),-999);pf=sf(x.get('profit_factor'),0)
   state='NEGATIVE' if exp<=0 or pf<=1 else ('WEAK' if exp<.5 or pf<1.08 else 'POSITIVE')
   out[str(r[0])]={'state':state,'label':x.get('label'),'done':x.get('done'),'expectancy':exp,'pf':pf}
 d.close();return out

def dataset(epoch,exp):
 db=ro_v52();rows=db.execute(f'''SELECT token_mint,cutoff_ts,{exp['feature']} AS feature FROM v52_snapshots
  WHERE stage_s=? AND cutoff_ts>? AND {exp['feature']} IS NOT NULL ORDER BY cutoff_ts,token_mint''',(int(exp['stage_s']),float(epoch['start_cutoff']))).fetchall();total=len(rows);out=[]
 for r in rows:
  val=sf(r['feature'])
  if val is None:continue
  econ=v63.economic_path(db,str(r['token_mint']),float(r['cutoff_ts']),exp['horizon_s'],exp['tp_pct'],exp['sl_pct'])
  if econ:out.append({'token_mint':str(r['token_mint']),'feature':val,'net':econ['net'],'hit':econ['hit'],'fill_delay':econ['fill_delay']})
 db.close();return out,total

def metrics(rs):
 xs=[r['net'] for r in rs]
 return {'n':len(rs),'expectancy':statistics.mean(xs) if xs else None,'pf':v60.profit_factor(xs),'win':sum(x>0 for x in xs)/len(xs) if xs else None}

def evaluate(epoch,exp,fw,r64):
 data,total=dataset(epoch,exp);train=[r for r in data if not v63.holdout(r['token_mint'])];ho=[r for r in data if v63.holdout(r['token_mint'])]
 base={'train_n':len(train),'holdout_n':len(ho),'fill_rate':len(data)/max(1,total),'median_fill_delay':statistics.median([r['fill_delay'] for r in data]) if data else None}
 reasons=[]
 if len(train)<MIN_TRAIN or len(ho)<MIN_HOLDOUT:return 'COLLECTING',dict(base,reasons=['insufficient_fresh_sample'])
 rho=v60.spearman([r['feature'] for r in train],[r['net'] for r in train])
 if rho is None:return 'REJECT',dict(base,reasons=['undefined_train_relationship'])
 direction=1.0 if rho>=0 else -1.0;cands=[]
 for q in (.65,.70,.75,.80,.85):
  th=v60.quantile([direction*r['feature'] for r in train],q);sel=[r for r in train if direction*r['feature']>=th] if th is not None else [];m=metrics(sel)
  if m['n']<15:continue
  score=sf(m['expectancy'],-999)+1.5*max(0,sf(m['pf'],0)-1)+.02*math.sqrt(m['n']);cands.append((score,th,q,m))
 if not cands:return 'REJECT',dict(base,direction=direction,reasons=['no_train_specification'])
 _,th,q,tr=max(cands,key=lambda x:x[0]);hs=[r for r in ho if direction*r['feature']>=th];hm=metrics(hs);hb=metrics(ho)
 lift=None if hm['expectancy'] is None or hb['expectancy'] is None else hm['expectancy']-hb['expectancy'];n=hm['n'];fill=base['fill_rate'];exp=sf(hm['expectancy'],-999);pf=sf(hm['pf'],0)
 fwstate=fw.get(exp and str(exp) or '')
 known=fw.get(str(exp['feature'])) if isinstance(exp,dict) else None
 # explicit known-feature evidence lookup
 known=fw.get(str(exp_row_feature)) if False else fw.get(str(exp))
 # overwritten below by caller-safe feature name
 known=fw.get(str(exp))
 # robust penalties
 npen=max(0,(20-n)/20)*2.0;fillpen=max(0,.35-fill)*3.0;mtp=3.0/math.sqrt(max(n,1))
 robust=exp+2*max(0,pf-1)+.6*sf(lift,0)+1.2*fill-npen-fillpen-mtp
 hard=[]
 if n<MIN_SELECTED_HO:hard.append(f'HO_selected<{MIN_SELECTED_HO}')
 if fill<MIN_FILL:hard.append(f'fill<{MIN_FILL:.0%}')
 if sf(lift,-999)<MIN_LIFT:hard.append(f'lift<{MIN_LIFT:.2f}pp')
 if exp<=0:hard.append('nonpositive_holdout')
 if pf<MIN_PF:hard.append(f'PF<{MIN_PF:.2f}')
 feat_fw=fw.get(str(exp['feature'])) if isinstance(exp,dict) else None
 # caller passes exp dict, so use feature below through closure workaround
 return 'PENDING_FAMILY',dict(base,direction=direction,threshold=th,threshold_q=q,selected_train=tr['n'],selected_holdout=n,
  holdout_expectancy=hm['expectancy'],holdout_pf=hm['pf'],holdout_win=hm['win'],baseline_expectancy=hb['expectancy'],expectancy_lift=lift,
  robust_score=robust,selected_tokens=sorted(set(r['token_mint'] for r in hs)),hard=hard,reasons=reasons)

def evaluate_safe(epoch,exp,fw,r64):
 # wrap evaluate logic with forward evidence applied after metrics are built
 status,m=evaluate(epoch,exp,{},r64)
 if status!='PENDING_FAMILY':return status,m
 hard=list(m.get('hard',[]));reasons=list(m.get('reasons',[]));fe=fw.get(str(exp['feature']))
 if fe and fe['state']=='NEGATIVE':hard.append('feature_failed_forward_quarantine')
 elif fe and fe['state']=='WEAK':m['robust_score']=sf(m.get('robust_score'),0)-1.5;reasons.append('feature_forward_weakening_penalty')
 # current R64 is a comparison gate only for SHORTLIST quality, not a historical rewrite
 if sf(m.get('holdout_expectancy'),-999)<=sf(r64.get('expectancy'),0) or sf(m.get('holdout_pf'),0)<=sf(r64.get('profit_factor'),0):reasons.append('does_not_beat_current_r64_raw')
 m['hard']=hard;m['reasons']=reasons;m['forward_evidence']=fe
 return ('REJECT' if hard else 'ROBUST_POSITIVE'),m

def eval_batch(epoch):
 fw=forward_feature_states();r64=r64_benchmark();cut=max_cutoff();d=odb();exps=[dict(x) for x in d.execute('SELECT * FROM experiments ORDER BY last_eval_at,experiment_id LIMIT ?',(BATCH,)).fetchall()];d.close();now=time.time()
 o=odb()
 for e in exps:
  try:st,m=evaluate_safe(epoch,e,fw,r64)
  except Exception as ex:st='ERROR';m={'reasons':[repr(ex)]}
  o.execute('''INSERT OR REPLACE INTO results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(
   e['experiment_id'],m.get('train_n',0),m.get('holdout_n',0),m.get('selected_train',0),m.get('selected_holdout',0),m.get('direction'),m.get('threshold'),m.get('threshold_q'),m.get('holdout_expectancy'),m.get('holdout_pf'),m.get('holdout_win'),m.get('baseline_expectancy'),m.get('expectancy_lift'),m.get('fill_rate'),m.get('median_fill_delay'),m.get('robust_score'),st,json.dumps(m.get('hard',[])+m.get('reasons',[])),json.dumps(m.get('selected_tokens',[])),now))
  o.execute('UPDATE experiments SET last_eval_cutoff=?,last_eval_at=? WHERE experiment_id=?',(cut,now,e['experiment_id']))
 o.commit();o.close();return len(exps),cut,r64

def jaccard(a,b):
 a=set(a);b=set(b);u=len(a|b);return len(a&b)/u if u else 0.0

def rebuild_families():
 d=odb();rs=[dict(x) for x in d.execute("SELECT e.family,r.* FROM results r JOIN experiments e USING(experiment_id) WHERE r.status='ROBUST_POSITIVE'").fetchall()];d.close();by=defaultdict(list)
 for r in rs:by[r['family']].append(r)
 o=odb();o.execute('DELETE FROM family_summary');now=time.time()
 for fam,arr in by.items():
  ordered=sorted(arr,key=lambda x:(sf(x['robust_score'],-999),sf(x['holdout_expectancy'],-999)),reverse=True);kept=[]
  for r in ordered:
   toks=json.loads(r['selected_tokens_json'] or '[]')
   if all(jaccard(toks,json.loads(k['selected_tokens_json'] or '[]'))<MAX_PAIR_OVERLAP for k in kept):kept.append(r)
  toks=set();maxov=0
  for r in kept:toks.update(json.loads(r['selected_tokens_json'] or '[]'))
  for i in range(len(arr)):
   for j in range(i+1,len(arr)):maxov=max(maxov,jaccard(json.loads(arr[i]['selected_tokens_json'] or '[]'),json.loads(arr[j]['selected_tokens_json'] or '[]')))
  exps=[sf(x['holdout_expectancy']) for x in kept];pfs=[sf(x['holdout_pf']) for x in kept]
  status='SHORTLIST' if len(kept)>=MIN_FAMILY_INDEP and len(toks)>=MIN_FAMILY_UNIQUE else 'WATCH'
  o.execute('INSERT INTO family_summary VALUES(?,?,?,?,?,?,?,?,?)',(fam,len(arr),len(kept),len(toks),maxov,statistics.median(exps) if exps else None,statistics.median(pfs) if pfs else None,status,now))
 o.commit();o.close()

def display(epoch,ran,cut,r64):
 d=odb();counts={x[0]:x[1] for x in d.execute('SELECT status,COUNT(*) FROM results GROUP BY status')};fams=[dict(x) for x in d.execute("SELECT * FROM family_summary ORDER BY CASE status WHEN 'SHORTLIST' THEN 0 ELSE 1 END,median_expectancy DESC")];top=[dict(x) for x in d.execute("SELECT e.family,e.feature,e.stage_s,e.horizon_s,e.tp_pct,e.sl_pct,r.* FROM results r JOIN experiments e USING(experiment_id) WHERE r.status IN ('ROBUST_POSITIVE','REJECT') ORDER BY r.robust_score DESC LIMIT 10")];d.close()
 print('\033[2J\033[H',end='');print('='*190);print('MEMECOIN LAB — FRESH ROBUST DISCOVERY EPOCH V7.4');print('='*190)
 print(f"EPOCH={epoch['epoch_id']} fresh_start>{epoch['start_cutoff']:.3f} current_cutoff={cut:.3f} | batch={ran}")
 print(f"R64 benchmark: DONE={r64.get('done',0)} exp={sf(r64.get('expectancy'),0):+.2f}% PF={sf(r64.get('profit_factor'),0):.2f}")
 print(f"results: COLLECTING={counts.get('COLLECTING',0)} ROBUST_POSITIVE={counts.get('ROBUST_POSITIVE',0)} REJECT={counts.get('REJECT',0)} ERROR={counts.get('ERROR',0)}")
 print(f"gates: TRAIN>={MIN_TRAIN} HO>={MIN_HOLDOUT} selected_HO>={MIN_SELECTED_HO} fill>={MIN_FILL:.0%} lift>={MIN_LIFT:.2f}pp PF>={MIN_PF:.2f}; family independent>={MIN_FAMILY_INDEP} unique>={MIN_FAMILY_UNIQUE}\n")
 if fams:
  print('FAMILY REPLICATION')
  for x in fams:print(f"  {x['status']:<10} {x['family']:<22} positives={x['positive_instances']:<3} independent={x['independent_instances']:<2} unique={x['unique_tokens']:<3} med_exp={sf(x['median_expectancy'],0):+.2f}% med_PF={sf(x['median_pf'],0):.2f} overlap={100*sf(x['max_pair_overlap'],0):.1f}%")
 print('\nTOP CURRENT INSTANCES')
 if not top:print('  Fresh epoch collecting data; no evaluated robust instances yet.')
 for i,x in enumerate(top,1):print(f"  #{i:02d} {x['status']:<15} score={sf(x['robust_score'],0):+5.2f} {x['family']:<20} {x['feature']:<20} st={x['stage_s']} h={x['horizon_s']} TP/SL={x['tp_pct']:.0f}/{x['sl_pct']:.0f} HO={x['selected_holdout']} exp={sf(x['holdout_expectancy'],0):+.2f}% PF={sf(x['holdout_pf'],0):.2f} fill={100*sf(x['fill_rate'],0):.1f}%")
 print('\nGuardrail: V7.4 is discovery only. SHORTLIST != evidence. Promotion requires immutable freeze + a brand-new future-only arena.')

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);epoch=init()
 while not STOP:
  try:
   ran,cut,r64=eval_batch(epoch);rebuild_families();display(epoch,ran,cut,r64)
  except Exception as e:print('V7.4 error:',repr(e),flush=True)
  time.sleep(LOOP)
if __name__=='__main__':main()
