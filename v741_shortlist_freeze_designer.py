#!/usr/bin/env python3
"""MEMECOIN LAB — SHORTLIST SELECTION & FREEZE DESIGNER V7.4.1

READS V7.4 discovery output and produces a deterministic DESIGN ONLY shortlist.
It does NOT launch a forward arena and does NOT mutate V7.4, R64, WALLET or FLOW.

Selection philosophy:
- family first, not best-looking individual result;
- reward independent replication, unique tokens, sample size, fill and stability;
- penalize within-family dispersion and overlap;
- select one representative instance per eligible SHORTLIST family;
- cap proposed freezes to MAX_FREEZES (default 3), with family diversity.

A proposed freeze is not evidence. It must be promoted into a separate immutable
future-only arena with a common cutoff before it can confirm or fail.
"""
from __future__ import annotations
import json, math, os, sqlite3, statistics, time, hashlib
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
SRC=ROOT/'v74_fresh_robust_discovery.db'
OUT=ROOT/'v741_shortlist_design.db'
MAX_FREEZES=int(os.environ.get('MEMECOIN_V741_MAX_FREEZES','3'))
MIN_SELECTED_HO=int(os.environ.get('MEMECOIN_V741_MIN_SELECTED_HO','10'))
MIN_FILL=float(os.environ.get('MEMECOIN_V741_MIN_FILL','0.20'))


def sf(x,d=None):
 try:
  v=float(x);return v if math.isfinite(v) else d
 except:return d

def ro(path):
 d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=20);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=20000');return d

def odb():
 d=sqlite3.connect(OUT,timeout=20);d.row_factory=sqlite3.Row;d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=20000');return d

def init():
 d=odb();d.executescript('''
 CREATE TABLE IF NOT EXISTS design_run(
  design_id TEXT PRIMARY KEY,created_at REAL NOT NULL,epoch_id TEXT NOT NULL,source_start_cutoff REAL NOT NULL,
  max_freezes INTEGER NOT NULL,method TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS family_design(
  design_id TEXT NOT NULL,family TEXT NOT NULL,family_status TEXT NOT NULL,positive_instances INTEGER,
  independent_instances INTEGER,unique_tokens INTEGER,max_pair_overlap REAL,median_expectancy REAL,median_pf REAL,
  stability_exp REAL,stability_pf REAL,family_score REAL,rank INTEGER,selected INTEGER NOT NULL,
  PRIMARY KEY(design_id,family));
 CREATE TABLE IF NOT EXISTS proposed_freeze(
  freeze_id TEXT PRIMARY KEY,design_id TEXT NOT NULL,family TEXT NOT NULL,experiment_id TEXT NOT NULL,
  feature TEXT NOT NULL,stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,
  direction REAL NOT NULL,threshold REAL NOT NULL,threshold_q REAL,selected_holdout INTEGER,holdout_expectancy REAL,
  holdout_pf REAL,expectancy_lift REAL,fill_rate REAL,robust_score REAL,representative_score REAL,
  status TEXT NOT NULL,created_at REAL NOT NULL);
 ''');d.commit();d.close()

def main():
 if not SRC.exists():raise SystemExit(f'Missing {SRC}')
 init();s=ro(SRC)
 ep=s.execute('SELECT * FROM epoch ORDER BY created_at LIMIT 1').fetchone()
 if not ep:raise SystemExit('V7.4 epoch missing')
 ep=dict(ep)
 fams=[dict(x) for x in s.execute("SELECT * FROM family_summary WHERE status='SHORTLIST'").fetchall()]
 positives=[dict(x) for x in s.execute("""SELECT e.*,r.* FROM results r JOIN experiments e USING(experiment_id)
  WHERE r.status='ROBUST_POSITIVE'""").fetchall()]
 by={}
 for x in positives:by.setdefault(x['family'],[]).append(x)
 scored=[]
 for f in fams:
  arr=by.get(f['family'],[])
  ex=[sf(x.get('holdout_expectancy')) for x in arr if sf(x.get('holdout_expectancy')) is not None]
  pf=[sf(x.get('holdout_pf')) for x in arr if sf(x.get('holdout_pf')) is not None]
  st_exp=statistics.pstdev(ex) if len(ex)>1 else 0.0
  st_pf=statistics.pstdev(pf) if len(pf)>1 else 0.0
  indep=sf(f.get('independent_instances'),0) or 0;uniq=sf(f.get('unique_tokens'),0) or 0;ov=sf(f.get('max_pair_overlap'),1) or 1
  medexp=sf(f.get('median_expectancy'),0) or 0;medpf=sf(f.get('median_pf'),0) or 0
  # family score deliberately compresses raw edge so giant PF/EXP cannot dominate replication/stability
  score=(2.0*min(indep,4)+0.08*min(uniq,40)+1.5*min(max(medpf-1,0),2)+0.18*min(max(medexp,0),8)
         -1.8*min(ov,1)-0.22*min(st_exp,10)-0.8*min(st_pf,4))
  scored.append((score,f,st_exp,st_pf,arr))
 scored.sort(key=lambda z:z[0],reverse=True)
 chosen=[]
 for rank,(fscore,f,se,sp,arr) in enumerate(scored,1):
  candidates=[]
  medexp=sf(f.get('median_expectancy'),0) or 0;medpf=sf(f.get('median_pf'),1) or 1
  for x in arr:
   n=int(x.get('selected_holdout') or 0);fill=sf(x.get('fill_rate'),0) or 0
   if n<MIN_SELECTED_HO or fill<MIN_FILL:continue
   exp=sf(x.get('holdout_expectancy'),-999);pf=sf(x.get('holdout_pf'),0);lift=sf(x.get('expectancy_lift'),0);rs=sf(x.get('robust_score'),0)
   # representative score favors closeness to family medians, sample/fill, then edge; not the maximum raw winner
   distance=abs(exp-medexp)+2.0*abs(pf-medpf)
   rep=0.35*min(rs,20)+0.08*min(n,30)+2.0*min(fill,0.6)+0.12*min(max(lift,0),8)-0.45*distance
   candidates.append((rep,x))
  candidates.sort(key=lambda z:z[0],reverse=True)
  if candidates and len(chosen)<MAX_FREEZES:chosen.append((rank,fscore,f,se,sp,candidates[0][0],candidates[0][1]))
 now=time.time();design_id='D741_'+hashlib.sha256(f"{ep['epoch_id']}|{ep['start_cutoff']}|{MAX_FREEZES}|family_first_v1".encode()).hexdigest()[:20]
 o=odb();o.execute('DELETE FROM family_design WHERE design_id=?',(design_id,));o.execute('DELETE FROM proposed_freeze WHERE design_id=?',(design_id,))
 o.execute('INSERT OR REPLACE INTO design_run VALUES(?,?,?,?,?,?)',(design_id,now,ep['epoch_id'],ep['start_cutoff'],MAX_FREEZES,'FAMILY_FIRST_REPLICATION_STABILITY_V1'))
 chosen_f={x[2]['family'] for x in chosen}
 for rank,(score,f,se,sp,arr) in enumerate(scored,1):
  o.execute('INSERT OR REPLACE INTO family_design VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(design_id,f['family'],f['status'],f.get('positive_instances'),f.get('independent_instances'),f.get('unique_tokens'),f.get('max_pair_overlap'),f.get('median_expectancy'),f.get('median_pf'),se,sp,score,rank,1 if f['family'] in chosen_f else 0))
 for rank,fscore,f,se,sp,rep,x in chosen:
  fid='F741_'+hashlib.sha256(f"{design_id}|{x['experiment_id']}".encode()).hexdigest()[:20]
  o.execute('INSERT OR REPLACE INTO proposed_freeze VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(fid,design_id,f['family'],x['experiment_id'],x['feature'],x['stage_s'],x['horizon_s'],x['tp_pct'],x['sl_pct'],x['direction'],x['threshold'],x.get('threshold_q'),x.get('selected_holdout'),x.get('holdout_expectancy'),x.get('holdout_pf'),x.get('expectancy_lift'),x.get('fill_rate'),x.get('robust_score'),rep,'PROPOSED_ONLY',now))
 o.commit();o.close();s.close()
 print('='*150);print('MEMECOIN LAB — SHORTLIST SELECTION & FREEZE DESIGNER V7.4.1');print('='*150)
 print(f"design={design_id} | epoch={ep['epoch_id']} | fresh_start>{ep['start_cutoff']:.3f} | shortlist_families={len(scored)} | proposed={len(chosen)}/{MAX_FREEZES}")
 print('Method: FAMILY FIRST — replication + unique tokens + stability + fill/sample; raw best PF/EXP cannot win by itself.\n')
 print('FAMILY RANKING')
 for rank,(score,f,se,sp,arr) in enumerate(scored,1):
  flag='SELECT' if f['family'] in chosen_f else 'WATCH '
  print(f"#{rank:02d} {flag} {f['family']:<22} score={score:+6.2f} robust+={int(f.get('positive_instances') or 0):3d} indep={int(f.get('independent_instances') or 0):2d} unique={int(f.get('unique_tokens') or 0):3d} overlap={100*sf(f.get('max_pair_overlap'),0):5.1f}% medExp={sf(f.get('median_expectancy'),0):+6.2f}% medPF={sf(f.get('median_pf'),0):4.2f} stability(exp/pf)={se:.2f}/{sp:.2f}")
 print('\nPROPOSED IMMUTABLE FREEZES — DESIGN ONLY')
 for _,fscore,f,se,sp,rep,x in chosen:
  print(f"{f['family']:<22} feature={x['feature']:<22} stage={x['stage_s']:>3}s h={x['horizon_s']:>3}s TP/SL={x['tp_pct']:g}/{x['sl_pct']:g} dir={sf(x['direction'],0):+g} th={sf(x['threshold'],0):.10g}")
  print(f"  HO={int(x.get('selected_holdout') or 0)} exp={sf(x.get('holdout_expectancy'),0):+.2f}% PF={sf(x.get('holdout_pf'),0):.2f} lift={sf(x.get('expectancy_lift'),0):+.2f}pp fill={100*sf(x.get('fill_rate'),0):.1f}% robust={sf(x.get('robust_score'),0):.2f} representative={rep:.2f}")
 print('\nNEXT: inspect this design, then explicitly promote these exact specifications into ONE new common-cutoff future-only arena. No retuning after freeze.')
 print('Guardrail: V7.4.1 is selection/design, not evidence and not live-capital authorization.')

if __name__=='__main__':main()
