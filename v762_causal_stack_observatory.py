#!/usr/bin/env python3
"""MEMECOIN LAB — CAUSAL STACK OBSERVATORY V7.6.2

Read-only presentation layer for V7.5.0 acquisition + V7.5.3 causal features +
V7.5.4 fresh future arena. Reuses the cinematic V7.6.1 UI but corrects the
experiment source/status semantics so '0/30' is not confused with inactivity.
"""
from __future__ import annotations
import os, sqlite3, subprocess, time
from http.server import ThreadingHTTPServer
from pathlib import Path
import v761_cinematic_experiment_observatory as ui

ROOT=Path.home()/"memecoin_lab"
HOST=os.environ.get('MEMECOIN_V762_HOST','127.0.0.1')
PORT=int(os.environ.get('MEMECOIN_V762_PORT','8798'))
ui.DB['arena']=ROOT/'v754_causal_materialized_future.db'
V52=ROOT/'v52_features.db'

def feature_health():
 out={'available':False,'healthy':False}
 if not V52.exists():return out
 try:
  d=ui.ro(V52);now=time.time();s=ui.one(d,'SELECT * FROM v753_engine_state WHERE id=1')
  rs=ui.rows(d,'SELECT build_lag_s FROM v753_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-60,));d.close()
  xs=[float(r['build_lag_s']) for r in rs];p90=ui.pct(xs,.9);p95=ui.pct(xs,.95)
  age=max(0,now-float(s['heartbeat_at'])) if s else 9999;pending=int(s['raw_pending']) if s else -1
  healthy=bool(s and len(xs)>=12 and p90 is not None and p90<=2 and p95 is not None and p95<=3 and age<=5 and 0<=pending<=100)
  return {'available':True,'healthy':healthy,'n':len(xs),'p90':p90,'p95':p95,'heartbeat_age':age,'raw_pending':pending,'activation_cutoff':s['activation_cutoff'] if s else None}
 except Exception as e:return {'available':False,'healthy':False,'error':repr(e)}

def collect_arena754(acq):
 o={'available':False,'rules':[]}
 p=ui.DB['arena']
 if not p.exists():return o
 try:
  d=ui.ro(p);a=ui.one(d,'SELECT * FROM arena LIMIT 1');f=ui.one(d,'SELECT * FROM v754_infra_freeze WHERE id=1')
  rr=ui.rows(d,'SELECT r.family,r.feature,r.stage_s,r.horizon_s,r.direction,r.threshold,s.* FROM frozen_rule r LEFT JOIN summary s USING(rule_id) ORDER BY r.family')
  iv=ui.one(d,'SELECT COUNT(*) n FROM integrity_violation') or {'n':0};tl=ui.one(d,'SELECT COUNT(*) n FROM transition_log') or {'n':0};d.close()
  fh=feature_health();live=bool(acq.get('recent',{}).get('healthy') and fh.get('healthy'))
  return {'available':True,'arena':a,'freeze':f,'rules':rr,'integrity':iv['n'],'transitions':tl['n'],'live_healthy':live,'feature_health':fh}
 except Exception as e:o['error']=repr(e)
 return o

def procs():
 try:
  ls=subprocess.run(['ps','aux'],capture_output=True,text=True,timeout=2).stdout.splitlines();out=[]
  for ln in ls:
   if 'python' not in ln.lower() or 'memecoin_lab' not in ln:continue
   if any(k in ln for k in ('v750_','v753_','v754_','v743_','v721_','v64_','v762_')):
    p=ln.split();out.append({'pid':p[1] if len(p)>1 else '?','cpu':p[2] if len(p)>2 else '?','cmd':' '.join(p[10:])})
  return out
 except:return []

ui.collect_arena=collect_arena754
ui.procs=procs
_orig_collect=ui.collect
def collect():
 d=_orig_collect();fh=feature_health();d['feature_engine']=fh
 # Overall data-stack health requires both acquisition and causal feature materialization.
 if d.get('acq',{}).get('recent') is not None:
  d['acq']['recent']['acquisition_healthy']=bool(d['acq']['recent'].get('healthy'))
  d['acq']['recent']['healthy']=bool(d['acq']['recent'].get('healthy') and fh.get('healthy'))
 return d
ui.collect=collect

# Correct outsider-facing labels without changing the V7.6.1 rendering engine.
ui.HTML=(ui.HTML
 .replace('V7.6.1','V7.6.2')
 .replace('V7.5.2','V7.5.4')
 .replace('DATA ENGINE','CAUSAL DATA STACK')
 .replace('FUTURE ARENA','CAUSAL FUTURE ARENA')
 .replace('The last 60 seconds are inside the frozen low-latency regime.','The last 60 seconds must pass both acquisition and immutable feature-materialization gates.')
 .replace('Purpose: test three rules that were chosen earlier, without changing them after seeing new outcomes.','Purpose: test three frozen rules only on insert-only T+20/T+30 features that were actually available in time.'))

class H(ui.H):
 pass

if __name__=='__main__':
 print(f'MEMECOIN LAB V7.6.2 causal observatory http://{HOST}:{PORT}',flush=True)
 ThreadingHTTPServer((HOST,PORT),H).serve_forever()
