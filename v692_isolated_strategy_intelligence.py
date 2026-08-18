#!/usr/bin/env python3
"""Memecoin Lab V6.9.2 — isolated strategy intelligence.
Reads research_v4_1.db strictly read-only and writes derived intelligence only to v69_intelligence.db.
Never mutates R64/V6.7.3 scientific state. Paper/research only.
"""
from __future__ import annotations
import hashlib,json,math,os,signal,sqlite3,time
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get('MEMECOIN_RESEARCH_V41_DB',ROOT/'research_v4_1.db'))
IDB=Path(os.environ.get('MEMECOIN_V69_INTEL_DB',ROOT/'v69_intelligence.db'))
LOOP=float(os.environ.get('MEMECOIN_V692_LOOP_S','20'))
STOP=False

def stop(*_):
 global STOP;STOP=True

def sf(x,d=0.0):
 try:
  v=float(x);return v if math.isfinite(v) else d
 except:return d

def ro():
 d=sqlite3.connect(f'file:{RDB}?mode=ro',uri=True,timeout=5);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=5000');return d

def intel():
 d=sqlite3.connect(IDB,timeout=10);d.row_factory=sqlite3.Row;d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=NORMAL');d.execute('PRAGMA busy_timeout=10000');return d

def init():
 d=intel();d.executescript('''
 CREATE TABLE IF NOT EXISTS strategy_intelligence(
 strategy_id TEXT PRIMARY KEY,label TEXT NOT NULL,role TEXT NOT NULL,evidence TEXT NOT NULL,done INTEGER NOT NULL,signals INTEGER NOT NULL,
 expectancy REAL,profit_factor REAL,win_rate REAL,fill_rate REAL,max_drawdown REAL,alpha_vs_r64 REAL,pf_vs_r64 REAL,
 token_overlap_r64 REAL,signal_overlap_r64 REAL,verdict TEXT NOT NULL,action TEXT NOT NULL,updated_at REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS research_journal(
 event_id TEXT PRIMARY KEY,created_at REAL NOT NULL,strategy_id TEXT NOT NULL,label TEXT NOT NULL,milestone TEXT NOT NULL,
 evidence TEXT NOT NULL,conclusion TEXT NOT NULL,action TEXT NOT NULL,snapshot_json TEXT NOT NULL);
 ''');d.commit();d.close()

def row(d,q,a=()):
 r=d.execute(q,a).fetchone();return dict(r) if r else {}

def rows(d,q,a=()):return [dict(x) for x in d.execute(q,a).fetchall()]

def token_set(d,table,idcol,idv,signal_only=False):
 q=f'SELECT DISTINCT token_mint FROM {table} WHERE {idcol}=?'
 if signal_only:q+=" AND state!='NO_SIGNAL'"
 try:return {str(x[0]) for x in d.execute(q,(idv,)).fetchall()}
 except:return set()

def ov(a,b):
 if not a or not b:return None
 return len(a&b)/min(len(a),len(b))

def evidence(n,role,status):
 if role=='CONTROL':return 'CONFIRMED_CONTROL' if status=='CONFIRMED' else 'CONTROL'
 if n>=30:return 'CONFIRMED_FORWARD' if status=='CONFIRMED' else 'FAILED_FORWARD'
 if n>=20:return 'LATE_FORWARD'
 if n>=10:return 'EARLY_SURVIVING' if status=='SURVIVING' else 'EARLY_DECAYING'
 if n>=5:return 'EARLY_EVIDENCE'
 return 'INSUFFICIENT_DATA'

def decision(role,s,b,to,so):
 n=int(s.get('done') or 0);exp=sf(s.get('expectancy'));pf=sf(s.get('profit_factor'));fill=sf(s.get('fill_rate'));st=str(s.get('status') or '')
 if role=='CONTROL':return 'CONFIRMED BASELINE','Continue immutable control observation.'
 if n<5:return 'TOO EARLY','Accumulate frozen future-only evidence to 5 DONE.'
 if n<30:
  v='AHEAD OF R64, UNCONFIRMED' if exp>sf(b.get('expectancy')) and pf>sf(b.get('profit_factor')) else ('POSITIVE, BELOW/NEAR R64' if exp>0 and pf>1 else 'DECAYING EARLY')
  return v,'No retuning. Continue to 30 DONE.'+(' Execution fill is weak.' if fill<0.20 else '')
 if st=='CONFIRMED':
  div=(to is not None and to<0.5) or (so is not None and so<0.5)
  if exp>sf(b.get('expectancy')) and pf>sf(b.get('profit_factor')):return 'CONFIRMED SUPERIOR'+(' + DIVERSIFYING' if div else ''),'Eligible for V7.0 portfolio arena.'
  return 'CONFIRMED COMPLEMENT'+(' + DIVERSIFYING' if div else ''),'Test portfolio contribution in V7.0.'
 return 'FAILED FORWARD','Do not promote; preserve as negative evidence.'

def milestone(role,s):
 n=int(s.get('done') or 0);st=str(s.get('status') or '')
 if role=='CONTROL':return f'CONTROL_{st}_{(n//10)*10}'
 for x in (30,20,10,5):
  if n>=x:return f'{st}_{x}_DONE'
 return 'STARTED'

def collect():
 d=ro();rr=row(d,'SELECT * FROM v64_frozen_rule LIMIT 1');b=row(d,'SELECT * FROM v64_forward_summary LIMIT 1');rid=str(rr.get('rule_id') or 'R64')
 rt=token_set(d,'v64_forward_events','rule_id',rid);rs=token_set(d,'v64_forward_events','rule_id',rid,True)
 out=[(rr,b,'R64 // PRICE_VELOCITY','CONTROL',None,None)]
 for s in rows(d,'SELECT * FROM v673_forward_summary ORDER BY label'):
  cid=s['challenger_id']
  if s['label']=='WALLET_STRUCTURE':r=row(d,'SELECT * FROM v672_frozen_challengers WHERE challenger_id=?',(cid,))
  else:r=row(d,'SELECT * FROM v6721_corrected_freezes WHERE challenger_id=?',(cid,))
  out.append((r,s,s['label'],'CHALLENGER',ov(token_set(d,'v673_forward_events','challenger_id',cid),rt),ov(token_set(d,'v673_forward_events','challenger_id',cid,True),rs)))
 d.close();return b,out

def cycle():
 b,items=collect();now=time.time();d=intel();d.execute('BEGIN IMMEDIATE')
 for r,s,label,role,to,so in items:
  sid=str(r.get('challenger_id') or r.get('rule_id') or label);n=int(s.get('done') or 0);ev=evidence(n,role,str(s.get('status') or ''));v,a=decision(role,s,b,to,so)
  vals=(sid,label,role,ev,n,int(s.get('signals') or 0),s.get('expectancy'),s.get('profit_factor'),s.get('win_rate'),s.get('fill_rate'),s.get('max_drawdown'),sf(s.get('expectancy'))-sf(b.get('expectancy')) if role!='CONTROL' else 0,sf(s.get('profit_factor'))-sf(b.get('profit_factor')) if role!='CONTROL' else 0,to,so,v,a,now)
  d.execute('''INSERT INTO strategy_intelligence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(strategy_id) DO UPDATE SET label=excluded.label,role=excluded.role,evidence=excluded.evidence,done=excluded.done,signals=excluded.signals,expectancy=excluded.expectancy,profit_factor=excluded.profit_factor,win_rate=excluded.win_rate,fill_rate=excluded.fill_rate,max_drawdown=excluded.max_drawdown,alpha_vs_r64=excluded.alpha_vs_r64,pf_vs_r64=excluded.pf_vs_r64,token_overlap_r64=excluded.token_overlap_r64,signal_overlap_r64=excluded.signal_overlap_r64,verdict=excluded.verdict,action=excluded.action,updated_at=excluded.updated_at''',vals)
  m=milestone(role,s);eid=hashlib.sha256(f'{sid}|{m}'.encode()).hexdigest()[:24];snap=json.dumps({'done':n,'signals':s.get('signals'),'expectancy':s.get('expectancy'),'profit_factor':s.get('profit_factor'),'fill_rate':s.get('fill_rate'),'max_drawdown':s.get('max_drawdown'),'status':s.get('status')},sort_keys=True)
  d.execute('INSERT OR IGNORE INTO research_journal VALUES(?,?,?,?,?,?,?,?,?)',(eid,now,sid,label,m,ev,v,a,snap))
 d.commit();xs=rows(d,'SELECT * FROM strategy_intelligence ORDER BY role,label');js=rows(d,'SELECT * FROM research_journal ORDER BY created_at DESC LIMIT 8');d.close();display(xs,js)

def display(xs,js):
 print('\033[2J\033[H',end='');print('='*170);print('MEMECOIN LAB — ISOLATED STRATEGY INTELLIGENCE V6.9.2');print('='*170)
 for x in xs:
  print(f"{x['label']:<28} {x['evidence']:<20} DONE={x['done']:>3} exp={sf(x['expectancy']):+6.2f}% PF={sf(x['profit_factor']):4.2f} fill={100*sf(x['fill_rate']):5.1f}% DD={sf(x['max_drawdown']):+6.1f}%")
  if x['role']!='CONTROL':print(f"  vs R64 alpha={sf(x['alpha_vs_r64']):+.2f}pp PF={sf(x['pf_vs_r64']):+.2f} token_overlap={100*sf(x['token_overlap_r64']):.1f}% signal_overlap={100*sf(x['signal_overlap_r64']):.1f}%")
  print(f"  CONCLUSION: {x['verdict']} | NEXT: {x['action']}")
 print('\nRESEARCH JOURNAL')
 for x in js:print(f"  {time.strftime('%H:%M:%S',time.localtime(x['created_at']))} {x['label']}: {x['milestone']} -> {x['conclusion']}")
 print(f'\nsource={RDB.name} READ-ONLY | intelligence={IDB.name} isolated WAL')

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init();print(f'V6.9.2 started | read={RDB} | write={IDB}',flush=True)
 while not STOP:
  try:cycle()
  except Exception as e:print('V6.9.2 error:',repr(e),flush=True)
  time.sleep(LOOP)
if __name__=='__main__':main()
