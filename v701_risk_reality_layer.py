#!/usr/bin/env python3
"""MEMECOIN LAB — RISK REALITY LAYER V7.0.1

Converts arena outcome-unit drawdown into account-risk scenarios without
retuning or modifying any frozen strategy. Reads intelligence DB only.

IMPORTANT: when path-level trade outcomes are unavailable, account DD is an
explicit approximation based on the reported raw DD divided by the frozen
SL+cost outcome unit (13 percentage points for TP/SL 20/10, costs 3%).
"""
from __future__ import annotations
import json, math, sqlite3, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
INTEL=ROOT/'v69_intelligence.db'
OUT=ROOT/'v701_risk_reality.db'
RISK_LEVELS=(0.25,0.50,1.00)
LOSS_UNIT_PCT=13.0  # frozen arenas: SL 10% + 3% costs => -13 outcome points

def ro(p): return sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=10)
def init():
 c=sqlite3.connect(OUT,timeout=10); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA busy_timeout=10000')
 c.execute('''CREATE TABLE IF NOT EXISTS risk_reality(
 created_at REAL,strategy_id TEXT,label TEXT,done INTEGER,raw_dd REAL,loss_equiv REAL,
 risk_pct REAL,linear_dd_est REAL,compound_dd_est REAL,method TEXT,metrics_json TEXT)''')
 c.commit(); c.close()
def rows():
 c=ro(INTEL); names=[r[1] for r in c.execute('PRAGMA table_info(strategy_intelligence)')]
 out=[dict(zip(names,r)) for r in c.execute('SELECT * FROM strategy_intelligence')]; c.close(); return out

def calc(r,risk):
 dd=abs(float(r.get('max_drawdown') or 0)); loss_equiv=dd/LOSS_UNIT_PCT
 linear=loss_equiv*risk
 # Equivalent consecutive full-risk losses. This is NOT the true chronological equity curve.
 compound=(1.0-(1.0-risk/100.0)**loss_equiv)*100.0
 return loss_equiv,linear,compound

def cycle():
 rs=rows(); now=time.time(); out=[]
 c=sqlite3.connect(OUT,timeout=10); c.execute('PRAGMA busy_timeout=10000')
 for r in rs:
  for risk in RISK_LEVELS:
   le,lin,comp=calc(r,risk)
   m={'expectancy':r.get('expectancy'),'profit_factor':r.get('profit_factor'),'win_rate':r.get('win_rate'),
      'fill_rate':r.get('fill_rate'),'signals':r.get('signals'),'loss_unit_pct':LOSS_UNIT_PCT,
      'path_level_available':False}
   c.execute('INSERT INTO risk_reality VALUES(?,?,?,?,?,?,?,?,?,?,?)',(
    now,str(r.get('strategy_id') or ''),str(r.get('label') or ''),int(r.get('done') or 0),float(r.get('max_drawdown') or 0),le,risk,lin,comp,
    'LOSS_EQUIVALENT_APPROXIMATION',json.dumps(m,sort_keys=True)))
   out.append((r,risk,le,lin,comp))
 c.commit(); c.close()
 print('\n'+'='*188); print('MEMECOIN LAB — RISK REALITY LAYER V7.0.1'); print('='*188)
 print(f'INPUT={INTEL} READ-ONLY | OUTPUT={OUT}')
 print('Raw arena DD is outcome-unit drawdown, NOT account drawdown. No strategy rule is changed.')
 print(f'Approximation anchor: one full SL outcome = -{LOSS_UNIT_PCT:.1f} points (10% SL + 3% costs).\n')
 for r in rs:
  label=str(r.get('label') or r.get('strategy_id')); dd=float(r.get('max_drawdown') or 0); done=int(r.get('done') or 0)
  group=[x for x in out if x[0] is r]
  le=group[0][2] if group else 0
  print(f'{label:<30} DONE={done:>3} raw_arena_DD={dd:+7.1f} points  ~= {le:.2f} full-loss equivalents')
  for _,risk,_,lin,comp in group:
   print(f'  risk/SL={risk:>4.2f}%  linear_DD~{lin:>6.2f}%  compounded_equivalent~{comp:>6.2f}%')
  print()
 print('LIMITATION: these are sizing translations, not reconstructed chronological account equity curves.')
 print('NEXT: join exact DONE outcome sequence + timestamps to compute true max consecutive losses, rolling clusters, exact compounded DD and bootstrap path distributions.')
 print('Guardrail: risk translation is paper analysis only; it is not authorization or a recommendation to deploy capital.',flush=True)

def main():
 init(); print(f'V7.0.1 started | read={INTEL} | write={OUT}',flush=True)
 while True:
  try: cycle()
  except Exception as e: print('V7.0.1 error:',repr(e),flush=True)
  time.sleep(30)
if __name__=='__main__': main()
