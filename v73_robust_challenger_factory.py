#!/usr/bin/env python3
"""MEMECOIN LAB — ROBUST CHALLENGER FACTORY V7.3

Ranks historical post-canonical V6.7 challenger instances with stricter gates
learned from the observed FLOW failure and WALLET weakening. This is a selection
factory only: it NEVER freezes or forward-tests a new challenger.

Principles
- no threshold retuning;
- penalize small holdout n, low fill, weak lift, unstable family replication;
- quarantine exact features already carrying negative/weakening forward evidence;
- require family-level independent replication before shortlist status;
- compare against current R64 benchmark but do not rewrite R64.
"""
from __future__ import annotations
import json, math, sqlite3, statistics, time
from pathlib import Path
import v41_core as core

ROOT=Path.home()/"memecoin_lab"
OUT=ROOT/'v73_robust_factory.db'
MIN_HO_N=10
MIN_FILL=.20
MIN_LIFT=.50
MIN_FAMILY_INDEP=2
MIN_UNIQUE=12
MAX_FAMILY_OVERLAP=.85
TOPN=12


def sf(x,d=0.0):
 try:
  v=float(x);return v if math.isfinite(v) else d
 except:return d

def init():
 d=sqlite3.connect(OUT,timeout=10);d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=10000')
 d.execute('''CREATE TABLE IF NOT EXISTS robust_candidates(
  created_at REAL,rank INTEGER,experiment_id TEXT,feature TEXT,family TEXT,stage_s INTEGER,horizon_s INTEGER,tp_pct REAL,sl_pct REAL,
  holdout_n INTEGER,holdout_expectancy REAL,holdout_pf REAL,expectancy_lift REAL,fill_rate REAL,
  family_status TEXT,family_independent INTEGER,family_unique_tokens INTEGER,family_overlap REAL,
  forward_evidence TEXT,robust_score REAL,status TEXT,reasons_json TEXT)''')
 d.commit();d.close()

def current_r64():
 d=core.open_research();r=d.execute('SELECT done,expectancy,profit_factor,fill_rate FROM v64_forward_summary LIMIT 1').fetchone();d.close()
 return dict(r) if r else {}

def forward_feature_states():
 d=core.open_research();out={}
 # map frozen challenger feature -> latest forward summary
 if d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='v673_forward_summary'").fetchone():
  for s in d.execute('SELECT * FROM v673_forward_summary'):
   x=dict(s);cid=x['challenger_id'];feat=None
   r=d.execute('SELECT feature FROM v672_frozen_challengers WHERE challenger_id=?',(cid,)).fetchone()
   if not r:r=d.execute('SELECT feature FROM v6721_corrected_freezes WHERE challenger_id=?',(cid,)).fetchone()
   if r:feat=r[0]
   if feat:
    state='NEGATIVE' if sf(x.get('expectancy'),-1)<=0 or sf(x.get('profit_factor'),0)<=1 else ('WEAK' if sf(x.get('expectancy'))<.5 or sf(x.get('profit_factor'))<1.08 else 'POSITIVE')
    out[str(feat)]={'state':state,'done':x.get('done'),'expectancy':x.get('expectancy'),'pf':x.get('profit_factor'),'label':x.get('label')}
 d.close();return out

def load():
 d=core.open_research();
 if not d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='v671_edge_instances'").fetchone():d.close();raise SystemExit('Run v671_postcanonical_challenger_consolidator.py first')
 inst=[dict(x) for x in d.execute("SELECT * FROM v671_edge_instances WHERE verdict IN ('PROMISING','WEAK')")]
 fam={x['family']:dict(x) for x in d.execute('SELECT * FROM v671_family_champions')}
 d.close();return inst,fam

def score(r,f,forward,r64):
 reasons=[];hard=[]
 n=int(r.get('holdout_selected') or 0);exp=sf(r.get('holdout_expectancy'));pf=sf(r.get('holdout_pf'));lift=sf(r.get('expectancy_lift'));fill=sf(r.get('fill_rate'))
 if n<MIN_HO_N:hard.append(f'HO_n<{MIN_HO_N}')
 if fill<MIN_FILL:hard.append(f'fill<{MIN_FILL:.0%}')
 if lift<MIN_LIFT:hard.append(f'lift<{MIN_LIFT:.2f}pp')
 if exp<=0 or pf<=1:hard.append('nonpositive_holdout')
 indep=int(f.get('independent_regimes') or 0);unique=int(f.get('unique_holdout_tokens') or 0);overlap=sf(f.get('max_pair_overlap'))
 if indep<MIN_FAMILY_INDEP:hard.append('family_not_independently_replicated')
 if unique<MIN_UNIQUE:hard.append('too_few_unique_holdout_tokens')
 if overlap>MAX_FAMILY_OVERLAP:reasons.append('high_family_overlap_penalty')
 fw=forward.get(str(r['feature']))
 if fw and fw['state']=='NEGATIVE':hard.append('feature_failed_forward')
 elif fw and fw['state']=='WEAK':reasons.append('feature_forward_weakening_penalty')
 # anti-selection-bias penalties: small n and huge winner score are discounted
 npen=max(0,(20-n)/20)*2.0
 fillpen=max(0,.35-fill)*3.0
 overlap_pen=max(0,overlap-.60)*2.0
 instability=sf(f.get('threshold_cv'))*.8 + (1-sf(f.get('direction_consistency')))*.8
 multiple_test_pen=1.0/math.sqrt(max(n,1))*3.0
 fwpen=1.5 if fw and fw['state']=='WEAK' else 0
 # robust score rewards edge + PF + lift + fill, penalizes fragility
 sc=exp + 2.0*max(0,pf-1) + .6*lift + 1.2*fill - npen-fillpen-overlap_pen-instability-multiple_test_pen-fwpen
 # modest bonus only if it clears current R64 raw expectancy/PF, never a hard requirement
 if exp>sf(r64.get('expectancy')) and pf>sf(r64.get('profit_factor')):sc+=.5;reasons.append('beats_current_r64_raw')
 return sc,hard,reasons,fw

def main():
 init();inst,fams=load();fw=forward_feature_states();r64=current_r64();cands=[]
 for r in inst:
  f=fams.get(r['family'],{});sc,hard,reasons,fe=score(r,f,fw,r64)
  status='REJECT' if hard else ('SHORTLIST' if sc>=1.0 else 'WATCH')
  cands.append((sc,r,f,fe,status,hard+reasons))
 cands.sort(key=lambda x:x[0],reverse=True)
 now=time.time();d=sqlite3.connect(OUT,timeout=10);d.execute('DELETE FROM robust_candidates')
 for rank,(sc,r,f,fe,status,reasons) in enumerate(cands[:TOPN],1):
  d.execute('INSERT INTO robust_candidates VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(
   now,rank,r['experiment_id'],r['feature'],r['family'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['holdout_selected'],
   r['holdout_expectancy'],r['holdout_pf'],r['expectancy_lift'],r['fill_rate'],f.get('status'),f.get('independent_regimes'),f.get('unique_holdout_tokens'),f.get('max_pair_overlap'),
   json.dumps(fe,sort_keys=True) if fe else 'NONE',sc,status,json.dumps(reasons)))
 d.commit();d.close()
 print('='*188);print('MEMECOIN LAB — ROBUST CHALLENGER FACTORY V7.3');print('='*188)
 print(f"R64 benchmark now: DONE={r64.get('done',0)} exp={sf(r64.get('expectancy')):+.2f}% PF={sf(r64.get('profit_factor')):.2f} fill={100*sf(r64.get('fill_rate')):.1f}%")
 print(f'Hard gates: HO_n>={MIN_HO_N} fill>={MIN_FILL:.0%} lift>={MIN_LIFT:.2f}pp family_independent>={MIN_FAMILY_INDEP} unique>={MIN_UNIQUE}')
 print('Known forward evidence is used only as a quarantine/penalty; no frozen rule is modified.\n')
 for rank,(sc,r,f,fe,status,reasons) in enumerate(cands[:TOPN],1):
  print(f"#{rank:02d} {status:<9} score={sc:+5.2f} {r['family']:<20} {r['feature']:<22} stage={r['stage_s']:<3} h={r['horizon_s']:<3} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f}")
  print(f"     HO_n={r['holdout_selected']:<3} exp={sf(r['holdout_expectancy']):+.2f}% PF={sf(r['holdout_pf']):.2f} lift={sf(r['expectancy_lift']):+.2f} fill={100*sf(r['fill_rate']):.1f}% | family={f.get('status','?')} indep={f.get('independent_regimes',0)} unique={f.get('unique_holdout_tokens',0)}")
  if fe:print(f"     prior forward evidence: {fe['label']} state={fe['state']} DONE={fe['done']} exp={sf(fe['expectancy']):+.2f}% PF={sf(fe['pf']):.2f}")
  print('     reasons:',', '.join(reasons) if reasons else 'passes robust gates')
 print('\nNEXT: only SHORTLIST candidates are eligible for a separate immutable freeze + fresh future-only arena. This factory itself produces no evidence.')

if __name__=='__main__':main()
