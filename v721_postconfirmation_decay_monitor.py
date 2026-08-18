#!/usr/bin/env python3
"""MEMECOIN LAB — POST-CONFIRMATION DECAY MONITOR V7.2.1

Read-only surveillance of already frozen strategies after confirmation.
Adds monitoring states only; never changes the scientific source verdict or rule.
"""
from __future__ import annotations
import sqlite3, statistics, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
RDB=ROOT/'research_v4_1.db'
OUT=ROOT/'v721_decay_monitor.db'
CHECKPOINTS=(30,50,75,100)

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=10);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def init():
 d=sqlite3.connect(OUT);d.execute('PRAGMA journal_mode=WAL');d.execute('''CREATE TABLE IF NOT EXISTS decay_state(
 created_at REAL,strategy TEXT,done INTEGER,expectancy REAL,pf REAL,checkpoint INTEGER,state TEXT,reason TEXT)''');d.commit();d.close()

def pf(xs):
 g=sum(x for x in xs if x>0);l=-sum(x for x in xs if x<0);return g/l if l else (999 if g else 0)

def seqs():
 d=ro(RDB);out={}
 rr=d.execute('SELECT rule_id FROM v64_frozen_rule LIMIT 1').fetchone()
 if rr:out['R64']=[float(x[0]) for x in d.execute("SELECT net_return FROM v64_forward_events WHERE rule_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint",(rr['rule_id'],))]
 wr=d.execute("SELECT challenger_id FROM v672_frozen_challengers WHERE family='WALLET_STRUCTURE' LIMIT 1").fetchone()
 if wr:out['WALLET']=[float(x[0]) for x in d.execute("SELECT net_return FROM v673_forward_events WHERE challenger_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint",(wr['challenger_id'],))]
 d.close();return out

def classify(xs):
 n=len(xs);e=statistics.mean(xs) if xs else 0;p=pf(xs) if xs else 0
 cp=max([c for c in CHECKPOINTS if n>=c],default=0)
 if n<30:return e,p,cp,'PRE_CONFIRMATION','Below 30-DONE confirmation threshold.'
 if e>1 and p>=1.15:return e,p,cp,'HEALTHY_CONFIRMED','Positive edge remains materially above zero.'
 if e>0 and p>1:return e,p,cp,'WEAKENING','Still positive, but edge is close to neutral.'
 if e<=0 or p<=1:
  if n>=50:return e,p,cp,'DECAYED','At least 50 DONE and cumulative edge is non-positive / PF<=1.'
  return e,p,cp,'DECAYING','Confirmed edge has crossed neutral before 50 DONE.'
 return e,p,cp,'MONITORING','No stronger classification.'

def cycle():
 now=time.time();o=sqlite3.connect(OUT);rows=[]
 for name,xs in seqs().items():
  e,p,cp,state,reason=classify(xs);row=(now,name,len(xs),e,p,cp,state,reason);rows.append(row);o.execute('INSERT INTO decay_state VALUES(?,?,?,?,?,?,?,?)',row)
 o.commit();o.close()
 print('\n'+'='*150);print('MEMECOIN LAB — POST-CONFIRMATION DECAY MONITOR V7.2.1');print('='*150)
 print('Monitoring checkpoints: 30 / 50 / 75 / 100 DONE. Source rules and verdicts remain untouched.\n')
 for r in rows:print(f'{r[1]:<8} DONE={r[2]:>3} checkpoint={r[5]:>3} exp={r[3]:+6.2f}% PF={r[4]:.2f}  STATE={r[6]}\n  {r[7]}')
 print('\nGuardrail: monitoring state != retuning permission. Any descendant requires a new research chain.',flush=True)

def main():
 init();print(f'V7.2.1 started | read={RDB} | write={OUT}',flush=True)
 while True:
  try:cycle()
  except Exception as e:print('V7.2.1 error:',repr(e),flush=True)
  time.sleep(30)
if __name__=='__main__':main()
