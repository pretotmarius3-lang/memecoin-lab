#!/usr/bin/env python3
"""Memecoin Lab V6.9 — read-only strategy intelligence layer.

Observes R64 + V6.7.3 challengers and writes DERIVED intelligence/journal rows only.
It never mutates frozen rules, forward events, outcomes, thresholds, or classifications.
Research/paper analytics only; no trading/signing.
"""
from __future__ import annotations
import hashlib, json, math, os, signal, time
from pathlib import Path
import v41_core as core

LOOP=float(os.environ.get('MEMECOIN_V69_LOOP_S','20'))
STOP=False

def stop(*_):
 global STOP;STOP=True

def sf(x,d=0.0):
 try:
  v=float(x);return v if math.isfinite(v) else d
 except:return d

def init():
 d=core.open_research();d.executescript('''
 CREATE TABLE IF NOT EXISTS v69_strategy_intelligence(
   strategy_id TEXT PRIMARY KEY,label TEXT NOT NULL,role TEXT NOT NULL,evidence TEXT NOT NULL,
   done INTEGER NOT NULL,signals INTEGER NOT NULL,expectancy REAL,profit_factor REAL,win_rate REAL,
   fill_rate REAL,max_drawdown REAL,alpha_vs_r64 REAL,pf_vs_r64 REAL,
   token_overlap_r64 REAL,signal_overlap_r64 REAL,verdict TEXT NOT NULL,action TEXT NOT NULL,updated_at REAL NOT NULL);
 CREATE TABLE IF NOT EXISTS v69_research_journal(
   event_id TEXT PRIMARY KEY,created_at REAL NOT NULL,strategy_id TEXT NOT NULL,label TEXT NOT NULL,
   milestone TEXT NOT NULL,evidence TEXT NOT NULL,conclusion TEXT NOT NULL,action TEXT NOT NULL,
   snapshot_json TEXT NOT NULL);
 ''');d.commit();d.close()

def r64():
 d=core.open_research();s=d.execute('SELECT * FROM v64_forward_summary LIMIT 1').fetchone();r=d.execute('SELECT * FROM v64_frozen_rule LIMIT 1').fetchone();d.close()
 return (dict(r) if r else {}),(dict(s) if s else {})

def challengers():
 d=core.open_research();out=[]
 try:
  sums=[dict(x) for x in d.execute('SELECT * FROM v673_forward_summary ORDER BY label').fetchall()]
  for s in sums:
   cid=s['challenger_id'];rule={}
   if s['label']=='WALLET_STRUCTURE':x=d.execute('SELECT * FROM v672_frozen_challengers WHERE challenger_id=?',(cid,)).fetchone()
   else:x=d.execute('SELECT * FROM v6721_corrected_freezes WHERE challenger_id=?',(cid,)).fetchone()
   if x:rule=dict(x)
   out.append((rule,s))
 finally:d.close()
 return out

def tokens(table,idcol,idv,signal_only=False):
 d=core.open_research();where=f'{idcol}=?';args=[idv]
 if signal_only:where+=" AND state!='NO_SIGNAL'"
 try:r={str(x[0]) for x in d.execute(f'SELECT DISTINCT token_mint FROM {table} WHERE {where}',args).fetchall()}
 except:r=set()
 d.close();return r

def overlap(a,b):
 if not a or not b:return None
 return len(a&b)/min(len(a),len(b))

def evidence(done,role,status):
 if role=='CONTROL':return 'CONFIRMED_CONTROL' if status=='CONFIRMED' else 'CONTROL'
 if done>=30:return 'CONFIRMED_FORWARD' if status=='CONFIRMED' else 'FAILED_FORWARD'
 if done>=20:return 'LATE_FORWARD'
 if done>=10:return 'EARLY_SURVIVING' if status=='SURVIVING' else 'EARLY_DECAYING'
 if done>=5:return 'EARLY_EVIDENCE'
 return 'INSUFFICIENT_DATA'

def verdict(label,role,s,b,ov,sv):
 n=int(s.get('done') or 0);exp=sf(s.get('expectancy'));pf=sf(s.get('profit_factor'));fill=sf(s.get('fill_rate'));status=str(s.get('status') or '')
 if role=='CONTROL':return 'CONFIRMED BASELINE','Continue immutable control observation.'
 if n<5:return 'TOO EARLY',f'Accumulate frozen future-only evidence; next useful checkpoint is 5 DONE.'
 if n<30:
  if exp>sf(b.get('expectancy')) and pf>sf(b.get('profit_factor')):v='AHEAD OF R64, UNCONFIRMED'
  elif exp>0 and pf>1:v='POSITIVE, BELOW/NEAR R64'
  else:v='DECAYING EARLY'
  extra=' Execution fill is weak.' if fill<0.20 else ''
  return v,f'No retuning. Continue to 30 DONE.{extra}'
 if status=='CONFIRMED':
  div=(ov is not None and ov<0.5) or (sv is not None and sv<0.5)
  if exp>sf(b.get('expectancy')) and pf>sf(b.get('profit_factor')):return 'CONFIRMED SUPERIOR'+(' + DIVERSIFYING' if div else ''),'Eligible for V7.0 portfolio arena; keep rule immutable.'
  return 'CONFIRMED COMPLEMENT'+(' + DIVERSIFYING' if div else ''),'Test portfolio contribution versus R64 in V7.0.'
 return 'FAILED FORWARD','Do not promote; preserve result as negative evidence.'

def milestone(role,s):
 n=int(s.get('done') or 0);status=str(s.get('status') or '')
 if role=='CONTROL':return f'CONTROL_{status}_{n//10*10}'
 for x in (30,20,10,5):
  if n>=x:return f'{status}_{x}_DONE'
 return 'STARTED'

def journal(sid,label,role,s,ev,conclusion,action):
 m=milestone(role,s);snap={'done':s.get('done'),'signals':s.get('signals'),'expectancy':s.get('expectancy'),'profit_factor':s.get('profit_factor'),'fill_rate':s.get('fill_rate'),'max_drawdown':s.get('max_drawdown'),'status':s.get('status')}
 eid=hashlib.sha256(f'{sid}|{m}'.encode()).hexdigest()[:24];d=core.open_research();d.execute('INSERT OR IGNORE INTO v69_research_journal VALUES(?,?,?,?,?,?,?,?,?)',(eid,time.time(),sid,label,m,ev,conclusion,action,json.dumps(snap,sort_keys=True)));d.commit();d.close()

def cycle():
 rr,b=r64();r64id=str(rr.get('rule_id') or 'R64');rt=tokens('v64_forward_events','rule_id',r64id);rs=tokens('v64_forward_events','rule_id',r64id,True)
 allrows=[(rr,b,'R64 // PRICE_VELOCITY','CONTROL')]+[(r,s,s['label'],'CHALLENGER') for r,s in challengers()]
 now=time.time();d=core.open_research()
 for r,s,label,role in allrows:
  sid=str(r.get('challenger_id') or r.get('rule_id') or label);ov=sv=None
  if role!='CONTROL':ov=overlap(tokens('v673_forward_events','challenger_id',sid),rt);sv=overlap(tokens('v673_forward_events','challenger_id',sid,True),rs)
  ev=evidence(int(s.get('done') or 0),role,str(s.get('status') or ''));v,a=verdict(label,role,s,b,ov,sv)
  vals=(sid,label,role,ev,int(s.get('done') or 0),int(s.get('signals') or 0),s.get('expectancy'),s.get('profit_factor'),s.get('win_rate'),s.get('fill_rate'),s.get('max_drawdown'),sf(s.get('expectancy'))-sf(b.get('expectancy')) if role!='CONTROL' else 0,sf(s.get('profit_factor'))-sf(b.get('profit_factor')) if role!='CONTROL' else 0,ov,sv,v,a,now)
  d.execute('''INSERT INTO v69_strategy_intelligence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(strategy_id) DO UPDATE SET label=excluded.label,role=excluded.role,evidence=excluded.evidence,done=excluded.done,signals=excluded.signals,expectancy=excluded.expectancy,profit_factor=excluded.profit_factor,win_rate=excluded.win_rate,fill_rate=excluded.fill_rate,max_drawdown=excluded.max_drawdown,alpha_vs_r64=excluded.alpha_vs_r64,pf_vs_r64=excluded.pf_vs_r64,token_overlap_r64=excluded.token_overlap_r64,signal_overlap_r64=excluded.signal_overlap_r64,verdict=excluded.verdict,action=excluded.action,updated_at=excluded.updated_at''',vals)
  journal(sid,label,role,s,ev,v,a)
 d.commit();d.close();display()

def display():
 d=core.open_research();xs=[dict(x) for x in d.execute('SELECT * FROM v69_strategy_intelligence ORDER BY role,label').fetchall()];j=[dict(x) for x in d.execute('SELECT * FROM v69_research_journal ORDER BY created_at DESC LIMIT 8').fetchall()];d.close()
 print('\033[2J\033[H',end='');print('='*170);print('MEMECOIN LAB — STRATEGY INTELLIGENCE LAYER V6.9');print('='*170)
 for x in xs:
  print(f"{x['label']:<28} {x['evidence']:<20} DONE={x['done']:>3} exp={sf(x['expectancy']):+6.2f}% PF={sf(x['profit_factor']):4.2f} fill={100*sf(x['fill_rate']):5.1f}% DD={sf(x['max_drawdown']):+6.1f}%")
  if x['role']!='CONTROL':print(f"  vs R64: alpha={sf(x['alpha_vs_r64']):+.2f}pp PF={sf(x['pf_vs_r64']):+.2f} | token_overlap={100*sf(x['token_overlap_r64']):.1f}% signal_overlap={100*sf(x['signal_overlap_r64']):.1f}%")
  print(f"  CONCLUSION: {x['verdict']} | NEXT: {x['action']}")
 print('\nRESEARCH JOURNAL');
 for x in j:print(f"  {time.strftime('%H:%M:%S',time.localtime(x['created_at']))} {x['label']}: {x['milestone']} -> {x['conclusion']}")
 print('\nGuardrail: derived intelligence only; frozen experiments are never mutated.')

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init()
 while not STOP:
  try:cycle()
  except Exception as e:print('V6.9 error:',repr(e),flush=True)
  time.sleep(LOOP)
if __name__=='__main__':main()
