#!/usr/bin/env python3
"""MEMECOIN LAB — EXACT PATH RISK V7.0.2

Reconstructs the exact chronological DONE outcome sequence for R64, WALLET and
FLOW, then translates frozen arena net returns into account-equity paths at
0.25%, 0.50% and 1.00% risk per full -13-point loss.

No strategy state is modified. Research/paper analysis only.
"""
from __future__ import annotations
import json, math, random, sqlite3, statistics, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
RDB=ROOT/'research_v4_1.db'
OUT=ROOT/'v702_exact_path_risk.db'
RISK_LEVELS=(0.25,0.50,1.00)
LOSS_UNIT=13.0
BOOT_N=2000
SEED=7022026

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=10); d.row_factory=sqlite3.Row; d.execute('PRAGMA query_only=ON'); d.execute('PRAGMA busy_timeout=10000'); return d

def init():
 d=sqlite3.connect(OUT,timeout=10); d.execute('PRAGMA journal_mode=WAL'); d.execute('PRAGMA busy_timeout=10000')
 d.executescript('''
 CREATE TABLE IF NOT EXISTS exact_path_summary(
  created_at REAL,strategy TEXT,risk_pct REAL,n INTEGER,final_equity REAL,total_return_pct REAL,
  max_drawdown_pct REAL,max_loss_streak INTEGER,max_win_streak INTEGER,worst_5trade_return_pct REAL,
  p50_boot_dd REAL,p90_boot_dd REAL,p95_boot_dd REAL,p99_boot_dd REAL,
  prob_dd_gt_5 REAL,prob_dd_gt_10 REAL,prob_dd_gt_15 REAL,source TEXT);
 CREATE TABLE IF NOT EXISTS exact_path_points(
  created_at REAL,strategy TEXT,risk_pct REAL,seq INTEGER,cutoff_ts REAL,token_mint TEXT,
  net_return REAL,equity REAL,drawdown_pct REAL);
 '''); d.commit(); d.close()

def load_sequences():
 d=ro(RDB); out={}
 rr=d.execute('SELECT rule_id FROM v64_frozen_rule LIMIT 1').fetchone()
 if rr:
  out['R64']=[dict(x) for x in d.execute("SELECT cutoff_ts,token_mint,net_return FROM v64_forward_events WHERE rule_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint",(rr['rule_id'],)).fetchall()]
 wr=d.execute("SELECT challenger_id FROM v672_frozen_challengers WHERE family='WALLET_STRUCTURE' LIMIT 1").fetchone()
 if wr:
  out['WALLET']=[dict(x) for x in d.execute("SELECT cutoff_ts,token_mint,net_return FROM v673_forward_events WHERE challenger_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint",(wr['challenger_id'],)).fetchall()]
 fr=d.execute("SELECT challenger_id FROM v6721_corrected_freezes WHERE label='FLOW_DYNAMICS_CORRECTED' LIMIT 1").fetchone()
 if fr:
  out['FLOW']=[dict(x) for x in d.execute("SELECT cutoff_ts,token_mint,net_return FROM v673_forward_events WHERE challenger_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint",(fr['challenger_id'],)).fetchall()]
 d.close(); return out

def step_multiplier(net_return,risk_pct):
 # -13 arena points == exactly -risk_pct of equity. Positive/time-exit returns scale linearly to same loss unit.
 return 1.0 + (risk_pct/100.0)*(float(net_return)/LOSS_UNIT)

def path(seq,risk):
 eq=100.0; peak=100.0; maxdd=0.0; pts=[]; ls=ws=maxls=maxws=0
 rets=[]
 for i,x in enumerate(seq,1):
  net=float(x['net_return']); before=eq; eq*=step_multiplier(net,risk); trade_ret=100*(eq/before-1); rets.append(trade_ret)
  peak=max(peak,eq); dd=100*(eq/peak-1); maxdd=min(maxdd,dd)
  if net<0: ls+=1; ws=0; maxls=max(maxls,ls)
  elif net>0: ws+=1; ls=0; maxws=max(maxws,ws)
  else: ls=ws=0
  pts.append((i,float(x['cutoff_ts']),str(x['token_mint']),net,eq,dd))
 worst5=0.0
 if len(rets)>=1:
  for i in range(len(rets)):
   e=100.0
   for tr in rets[i:i+5]:e*=1+tr/100
   worst5=min(worst5,100*(e/100-1))
 return {'final_equity':eq,'total_return_pct':eq-100,'max_drawdown_pct':maxdd,'max_loss_streak':maxls,'max_win_streak':maxws,'worst_5trade_return_pct':worst5,'points':pts}

def boot_dd(seq,risk,n=BOOT_N):
 if not seq:return []
 rng=random.Random(SEED+int(risk*100)); vals=[]; base=[float(x['net_return']) for x in seq]
 for _ in range(n):
  xs=base[:]; rng.shuffle(xs); eq=peak=100.0; md=0.0
  for net in xs:
   eq*=step_multiplier(net,risk); peak=max(peak,eq); md=min(md,100*(eq/peak-1))
  vals.append(abs(md))
 return sorted(vals)
def q(xs,p):
 if not xs:return None
 i=min(len(xs)-1,max(0,int(round((len(xs)-1)*p))));return xs[i]

def cycle():
 seqs=load_sequences(); now=time.time(); rows=[]; pointrows=[]
 for name,seq in seqs.items():
  for risk in RISK_LEVELS:
   p=path(seq,risk); b=boot_dd(seq,risk)
   row=(now,name,risk,len(seq),p['final_equity'],p['total_return_pct'],p['max_drawdown_pct'],p['max_loss_streak'],p['max_win_streak'],p['worst_5trade_return_pct'],q(b,.50),q(b,.90),q(b,.95),q(b,.99),sum(x>5 for x in b)/len(b) if b else None,sum(x>10 for x in b)/len(b) if b else None,sum(x>15 for x in b)/len(b) if b else None,'EXACT_CHRONOLOGICAL_DONE_SEQUENCE')
   rows.append(row)
   for z in p['points']:pointrows.append((now,name,risk,*z))
 d=sqlite3.connect(OUT,timeout=10);d.execute('PRAGMA busy_timeout=10000');d.executemany('INSERT INTO exact_path_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',rows);d.executemany('INSERT INTO exact_path_points VALUES(?,?,?,?,?,?,?,?,?)',pointrows);d.commit();d.close()
 print('\n'+'='*190);print('MEMECOIN LAB — EXACT PATH RISK V7.0.2');print('='*190)
 print(f'SOURCE={RDB} READ-ONLY | OUTPUT={OUT} | bootstrap permutations={BOOT_N}')
 print(f'Risk convention: -{LOSS_UNIT:.0f} arena points = one full risk unit. Chronological path is exact for recorded DONE outcomes.\n')
 for name in ('R64','WALLET','FLOW'):
  rs=[r for r in rows if r[1]==name]
  if not rs:continue
  print(f'{name}  DONE={rs[0][3]}')
  for r in rs:
   print(f'  risk={r[2]:.2f}% final={r[4]:7.2f} return={r[5]:+7.2f}% TRUE_DD={r[6]:+6.2f}% maxL={r[7]} maxW={r[8]} worst5={r[9]:+6.2f}% | bootDD p50={r[10]:5.2f}% p90={r[11]:5.2f}% p95={r[12]:5.2f}% p99={r[13]:5.2f}% | P(DD>5/10/15)={100*r[14]:4.1f}/{100*r[15]:4.1f}/{100*r[16]:4.1f}%')
  print()
 print('Interpretation: TRUE_DD uses the actual chronological DONE order; bootstrap DD changes only order, not outcomes.')
 print('Guardrail: paper risk analysis only; no capital deployment decision is made by this script.',flush=True)

def main():
 init();print(f'V7.0.2 started | read={RDB} | write={OUT}',flush=True)
 while True:
  try:cycle()
  except Exception as e:print('V7.0.2 error:',repr(e),flush=True)
  time.sleep(60)
if __name__=='__main__':main()
