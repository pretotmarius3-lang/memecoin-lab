#!/usr/bin/env python3
"""MEMECOIN LAB V7.1 — R64 + WALLET frozen portfolio arena.

Research-only portfolio experiment. Reads the already frozen R64 and WALLET
rules and evaluates a NEW common future-only cohort after a durable V7.1 cutoff.
No thresholds, directions, stages, horizons or exits are retuned.

Primary allocation is predeclared equal-risk 50/50. Secondary 75/25 is reported
as sensitivity only and cannot replace the primary result after observing data.
"""
from __future__ import annotations
import json, math, os, signal, sqlite3, statistics, time
from pathlib import Path
import v41_core as core
import v64_next_fill_future_only_arena as v64

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
LOOP=float(os.environ.get('MEMECOIN_V71_LOOP_S','10'))
MIN_CONFIRM=int(os.environ.get('MEMECOIN_V71_MIN_CONFIRM','30'))
STOP=False

def stop(*_):
 global STOP;STOP=True
def sf(x,d=None):
 try:
  v=float(x);return v if math.isfinite(v) else d
 except:return d
def v52():
 d=sqlite3.connect(f'file:{V52}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA busy_timeout=30000');return d
def max_cutoff():
 d=v52();x=d.execute('SELECT MAX(cutoff_ts) FROM v52_snapshots').fetchone()[0];d.close();return sf(x,0) or 0

def init():
 d=core.open_research();d.executescript('''
 CREATE TABLE IF NOT EXISTS v71_freeze(
  arena_id TEXT PRIMARY KEY,frozen_at REAL NOT NULL,frozen_cutoff REAL NOT NULL,
  r64_rule_json TEXT NOT NULL,wallet_rule_json TEXT NOT NULL,
  primary_r64_weight REAL NOT NULL,primary_wallet_weight REAL NOT NULL,
  secondary_r64_weight REAL NOT NULL,secondary_wallet_weight REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS v71_events(
  arena_id TEXT NOT NULL,label TEXT NOT NULL,token_mint TEXT NOT NULL,cutoff_ts REAL NOT NULL,
  state TEXT NOT NULL,net_return REAL,fill_ts REAL,fill_delay_s REAL,updated_at REAL NOT NULL,
  PRIMARY KEY(arena_id,label,token_mint,cutoff_ts));
 CREATE TABLE IF NOT EXISTS v71_summary(
  arena_id TEXT NOT NULL,label TEXT NOT NULL,done INTEGER NOT NULL,signals INTEGER NOT NULL,
  expectancy REAL,pf REAL,win_rate REAL,max_drawdown REAL,fill_rate REAL,status TEXT NOT NULL,updated_at REAL NOT NULL,
  PRIMARY KEY(arena_id,label));
 CREATE TABLE IF NOT EXISTS v71_portfolio_summary(
  arena_id TEXT NOT NULL,allocation TEXT NOT NULL,paired_buckets INTEGER NOT NULL,active_events INTEGER NOT NULL,
  expectancy REAL,pf REAL,win_rate REAL,max_drawdown REAL,loss_overlap_rate REAL,return_correlation REAL,
  status TEXT NOT NULL,updated_at REAL NOT NULL,PRIMARY KEY(arena_id,allocation));
 ''')
 if not d.execute('SELECT 1 FROM v71_freeze LIMIT 1').fetchone():
  r=dict(d.execute('SELECT * FROM v64_frozen_rule LIMIT 1').fetchone())
  w=dict(d.execute("SELECT * FROM v672_frozen_challengers WHERE family='WALLET_STRUCTURE' LIMIT 1").fetchone())
  # common cutoff is frozen NOW; old outcomes cannot validate this portfolio experiment
  cut=max_cutoff();now=time.time();aid='P71_'+core.fingerprint({'cutoff':cut,'r64':r['rule_id'],'wallet':w['challenger_id']},'v71:')[:22]
  d.execute('INSERT INTO v71_freeze VALUES(?,?,?,?,?,?,?,?)',(aid,now,cut,core.canonical_json(r),core.canonical_json(w),.5,.5,.75,.25))
 d.commit();d.close()

def freeze():
 d=core.open_research();r=dict(d.execute('SELECT * FROM v71_freeze LIMIT 1').fetchone());d.close();r['r64']=json.loads(r['r64_rule_json']);r['wallet']=json.loads(r['wallet_rule_json']);return r

def process_rule(fr,label,rule):
 db=v52();feature=rule['feature'];excluded=set(json.loads(rule['excluded_tokens_json']))
 rs=db.execute(f'SELECT token_mint,cutoff_ts,{feature} feature FROM v52_snapshots WHERE stage_s=? AND cutoff_ts>? AND {feature} IS NOT NULL ORDER BY cutoff_ts,token_mint',(int(rule['stage_s']),float(fr['frozen_cutoff']))).fetchall();now=time.time();out=[]
 for x in rs:
  tok=str(x['token_mint']);val=sf(x['feature'])
  if tok in excluded or val is None:continue
  z=v64.classify(db,rule,tok,float(x['cutoff_ts']),val,now);out.append((tok,float(x['cutoff_ts']),z))
 db.close();d=core.open_research()
 for tok,cut,z in out:d.execute('''INSERT INTO v71_events VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(arena_id,label,token_mint,cutoff_ts) DO UPDATE SET state=excluded.state,net_return=excluded.net_return,fill_ts=excluded.fill_ts,fill_delay_s=excluded.fill_delay_s,updated_at=excluded.updated_at''',(fr['arena_id'],label,tok,cut,z['state'],z.get('net_return'),z.get('fill_ts'),z.get('fill_delay_s'),time.time()))
 d.commit();d.close()
def pf(xs):
 g=sum(x for x in xs if x>0);l=-sum(x for x in xs if x<0);return g/l if l else (999 if g else 0)
def dd(xs):
 eq=peak=worst=0
 for x in xs:eq+=x;peak=max(peak,eq);worst=min(worst,eq-peak)
 return worst
def summarize(fr,label):
 d=core.open_research();rs=[dict(x) for x in d.execute('SELECT * FROM v71_events WHERE arena_id=? AND label=? ORDER BY cutoff_ts,token_mint',(fr['arena_id'],label))];done=[x for x in rs if x['state']=='DONE' and x['net_return'] is not None];xs=[float(x['net_return']) for x in done];signals=sum(x['state']!='NO_SIGNAL' for x in rs);filled=sum(x['fill_ts'] is not None and x['state']!='NO_SIGNAL' for x in rs);n=len(xs);e=statistics.mean(xs) if xs else None;p=pf(xs) if xs else None;st='WAITING' if n<MIN_CONFIRM else ('CONFIRMED' if sf(e,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD');d.execute('INSERT OR REPLACE INTO v71_summary VALUES(?,?,?,?,?,?,?,?,?,?,?)',(fr['arena_id'],label,n,signals,e,p,sum(x>0 for x in xs)/n if n else None,dd(xs) if xs else None,filled/signals if signals else None,st,time.time()));d.commit();d.close()
def corr(a,b):
 if len(a)<2:return None
 ma=statistics.mean(a);mb=statistics.mean(b);sa=sum((x-ma)**2 for x in a);sb=sum((x-mb)**2 for x in b)
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb) if sa>0 and sb>0 else None
def portfolio(fr,name,wr,ww):
 d=core.open_research();rs=[dict(x) for x in d.execute("SELECT label,token_mint,cutoff_ts,net_return FROM v71_events WHERE arena_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint",(fr['arena_id'],))];d.close()
 # bucket by token + integer signal second: same market opportunity is combined; isolated signals keep their own contribution
 buckets={}
 for x in rs:buckets.setdefault((x['token_mint'],int(float(x['cutoff_ts']))),{})[x['label']]=float(x['net_return'])
 vals=[];pairs=[];bothloss=0
 for _,b in sorted(buckets.items()):
  r=b.get('R64');w=b.get('WALLET')
  if r is not None and w is not None:pairs.append((r,w));bothloss+=int(r<0 and w<0)
  vals.append((wr*r if r is not None else 0)+(ww*w if w is not None else 0))
 e=statistics.mean(vals) if vals else None;p=pf(vals) if vals else None;n=len(vals);st='WAITING' if n<MIN_CONFIRM else ('POSITIVE' if sf(e,-1)>0 and sf(p,0)>1 else 'FAILED')
 d=core.open_research();d.execute('INSERT OR REPLACE INTO v71_portfolio_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(fr['arena_id'],name,len(pairs),n,e,p,sum(x>0 for x in vals)/n if n else None,dd(vals) if vals else None,bothloss/len(pairs) if pairs else None,corr([x[0] for x in pairs],[x[1] for x in pairs]) if pairs else None,st,time.time()));d.commit();d.close()
def display(fr):
 d=core.open_research();s=[dict(x) for x in d.execute('SELECT * FROM v71_summary WHERE arena_id=? ORDER BY label',(fr['arena_id'],))];p=[dict(x) for x in d.execute('SELECT * FROM v71_portfolio_summary WHERE arena_id=? ORDER BY allocation',(fr['arena_id'],))];d.close();print('\033[2J\033[H',end='');print('='*176);print('MEMECOIN LAB — R64 + WALLET FUTURE-ONLY PORTFOLIO ARENA V7.1');print('='*176);print(f"arena={fr['arena_id']} common_cutoff>{fr['frozen_cutoff']:.3f} | PRIMARY=50/50 equal-risk | SECONDARY=75/25 sensitivity\n")
 for x in s:print(f"{x['label']:<8} {x['status']:<14} DONE={x['done']:>3} exp={sf(x['expectancy'],0):+6.2f}% PF={sf(x['pf'],0):.2f} fill={100*sf(x['fill_rate'],0):5.1f}% DD={sf(x['max_drawdown'],0):+7.2f}")
 print()
 for x in p:print(f"PORTFOLIO {x['allocation']:<10} {x['status']:<8} events={x['active_events']:>3} paired={x['paired_buckets']:>3} exp={sf(x['expectancy'],0):+6.2f}% PF={sf(x['pf'],0):.2f} DD={sf(x['max_drawdown'],0):+7.2f} loss_overlap={100*sf(x['loss_overlap_rate'],0):4.1f}% corr={sf(x['return_correlation'],0):+.2f}")
 print('\nGuardrail: new common future-only cohort only. 50/50 is the predeclared primary test; 75/25 cannot be selected post hoc as the winner.')
def cycle():
 fr=freeze();process_rule(fr,'R64',fr['r64']);process_rule(fr,'WALLET',fr['wallet']);summarize(fr,'R64');summarize(fr,'WALLET');portfolio(fr,'PRIMARY_50_50',.5,.5);portfolio(fr,'SECONDARY_75_25',.75,.25);display(fr)
def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init()
 while not STOP:
  try:cycle()
  except Exception as e:print('V7.1 error:',repr(e),flush=True)
  time.sleep(LOOP)
if __name__=='__main__':main()
