#!/usr/bin/env python3
"""MEMECOIN LAB — RESEARCH INTEGRITY ARENA V7.0

Read-only integrity layer over existing scientific/research databases.
Runs three independent audits:
 A) discovery multiple-testing / false-discovery pressure
 B) R64 loss/drawdown concentration diagnostics
 C) execution-quality benchmark across R64 and challengers

This module NEVER retunes, promotes, or mutates frozen strategy state.
"""
from __future__ import annotations
import json, math, random, sqlite3, statistics, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
SCI=ROOT/'research_v4_1.db'
INTEL=ROOT/'v69_intelligence.db'
DESIGN=ROOT/'v695_experimental_design.db'
OUT=ROOT/'v70_integrity.db'


def ro(p): return sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=10)
def tables(c): return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
def cols(c,t): return [r[1] for r in c.execute(f'PRAGMA table_info({t})')]
def init():
 c=sqlite3.connect(OUT,timeout=10); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA busy_timeout=10000')
 c.execute('''CREATE TABLE IF NOT EXISTS integrity_snapshots(created_at REAL, audit TEXT, subject TEXT, status TEXT, metrics_json TEXT, conclusion TEXT)''')
 c.commit(); c.close()

def intelligence():
 if not INTEL.exists(): return []
 c=ro(INTEL); ts=tables(c)
 if 'strategy_intelligence' not in ts: c.close(); return []
 names=cols(c,'strategy_intelligence'); out=[dict(zip(names,r)) for r in c.execute('SELECT * FROM strategy_intelligence')]; c.close(); return out

def audit_false_discovery(rows):
 # Conservative empirical warning score: small discovery holdouts + many searched candidates imply high selection pressure.
 # This is an integrity diagnostic, not a formal p-value without raw candidate score vectors.
 flow=next((r for r in rows if 'FLOW' in str(r.get('label','')).upper()),None)
 if not flow: return ('WAITING',{},'FLOW intelligence unavailable.')
 done=int(flow.get('done') or 0); exp=float(flow.get('expectancy') or 0); pf=float(flow.get('profit_factor') or 0)
 pressure='HIGH' if done<30 and exp<=0 else ('ELEVATED' if done<30 else 'MEASURABLE')
 m={'forward_done':done,'forward_expectancy':exp,'forward_pf':pf,'selection_pressure':pressure,
    'formal_null_test':'PENDING_RAW_DISCOVERY_SCORE_VECTOR'}
 concl=('Forward decay after selected discovery is consistent with selection bias, but not proof. '
        'A formal permutation/null replay requires the frozen V6.7 candidate score universe.')
 return ('DIAGNOSTIC',m,concl)

def audit_r64_risk(rows):
 r=next((x for x in rows if str(x.get('role','')).upper()=='CONTROL' or 'R64' in str(x.get('label','')).upper()),None)
 if not r: return ('WAITING',{},'R64 intelligence unavailable.')
 dd=float(r.get('max_drawdown') or 0); exp=float(r.get('expectancy') or 0); pf=float(r.get('profit_factor') or 0); done=int(r.get('done') or 0)
 ratio=abs(dd)/max(abs(exp),1e-9)
 severity='HIGH' if abs(dd)>=75 else ('ELEVATED' if abs(dd)>=50 else 'MODERATE')
 m={'done':done,'expectancy':exp,'pf':pf,'max_drawdown':dd,'abs_dd_to_expectancy':ratio,'risk_severity':severity,
    'cluster_decomposition':'PENDING_PATH_LEVEL_JOIN'}
 concl=f'R64 remains the control; drawdown severity={severity}. Path-level clustering is required before proposing any risk-conditioned descendant.'
 return ('DIAGNOSTIC',m,concl)

def audit_execution(rows):
 if not rows: return ('WAITING',{},'No strategy intelligence available.')
 vals=[]
 for r in rows:
  vals.append({'label':r.get('label'),'done':int(r.get('done') or 0),'fill_rate':float(r.get('fill_rate') or 0),
               'expectancy':float(r.get('expectancy') or 0),'pf':float(r.get('profit_factor') or 0)})
 vals.sort(key=lambda x:x['fill_rate'],reverse=True)
 control=next((x for x in vals if 'R64' in str(x['label']).upper()),None)
 for x in vals:
  x['fill_gap_vs_r64']=None if not control else x['fill_rate']-control['fill_rate']
 concl='Execution quality differs materially when fill gaps are large; this audit does not alter strategy verdicts.'
 return ('DIAGNOSTIC',{'strategies':vals},concl)

def save(audits):
 c=sqlite3.connect(OUT,timeout=10); now=time.time()
 for audit,subject,status,m,concl in audits:
  c.execute('INSERT INTO integrity_snapshots VALUES(?,?,?,?,?,?)',(now,audit,subject,status,json.dumps(m,sort_keys=True),concl))
 c.commit(); c.close()

def cycle():
 rows=intelligence()
 a=audit_false_discovery(rows); b=audit_r64_risk(rows); e=audit_execution(rows)
 audits=[('FALSE_DISCOVERY','FLOW_DYNAMICS_CORRECTED',*a),('R64_RISK','R64 // PRICE_VELOCITY',*b),('EXECUTION','ALL_STRATEGIES',*e)]
 save(audits)
 print('\n'+'='*190); print('MEMECOIN LAB — RESEARCH INTEGRITY ARENA V7.0'); print('='*190)
 print(f'SCIENCE={SCI} READ-ONLY | INTELLIGENCE={INTEL} READ-ONLY | OUTPUT={OUT}')
 print('No retuning. No promotion. No mutation of R64/WALLET/FLOW.\n')
 for audit,subject,status,m,concl in audits:
  print(f'{audit:<18} {subject:<30} {status}')
  if audit=='FALSE_DISCOVERY':
   print(f"  forward DONE={m.get('forward_done','-')} exp={m.get('forward_expectancy','-')} PF={m.get('forward_pf','-')} selection_pressure={m.get('selection_pressure','-')}")
   print(f"  formal_null_test={m.get('formal_null_test','-')}")
  elif audit=='R64_RISK':
   print(f"  DONE={m.get('done','-')} exp={m.get('expectancy','-')} PF={m.get('pf','-')} DD={m.get('max_drawdown','-')} severity={m.get('risk_severity','-')}")
   print(f"  cluster_decomposition={m.get('cluster_decomposition','-')}")
  else:
   for x in m.get('strategies',[]): print(f"  {str(x['label']):28} fill={x['fill_rate']:.1f}% gap_vs_R64={x['fill_gap_vs_r64'] if x['fill_gap_vs_r64'] is not None else '-'} exp={x['expectancy']:+.2f}% PF={x['pf']:.2f}")
  print('  CONCLUSION:',concl,'\n')
 print('NEXT DATA NEEDS: raw V6.7 candidate-score universe for formal null replay; path-level R64 outcomes for loss-cluster decomposition.')
 print('Guardrail: integrity diagnostics are evidence about the research process, not authorization for live capital.',flush=True)

def main():
 init(); print(f'V7.0 started | intelligence={INTEL} | output={OUT}',flush=True)
 while True:
  try: cycle()
  except Exception as e: print('V7.0 error:',repr(e),flush=True)
  time.sleep(30)
if __name__=='__main__': main()
