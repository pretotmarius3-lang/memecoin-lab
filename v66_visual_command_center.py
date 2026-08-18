#!/usr/bin/env python3
"""Memecoin Lab V6.6 — read-only live command center.
Adds current Alchemy V5.1.7 HOT telemetry and a detailed V6.4 experiment panel.
No scientific state is mutated. No trading/signing.
"""
from __future__ import annotations
import json, math, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V66_DASH_HOST","127.0.0.1"); PORT=int(os.environ.get("MEMECOIN_V66_DASH_PORT","8793"))
def op(p):
 d=sqlite3.connect(f"file:{p}?mode=ro",uri=True,timeout=3);d.row_factory=sqlite3.Row;d.execute("PRAGMA query_only=ON");d.execute("PRAGMA busy_timeout=3000");return d
def has(d,t):return bool(d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())
def one(d,q,a=(),default=0):
 try:r=d.execute(q,a).fetchone();return default if not r or r[0] is None else r[0]
 except:return default
def row(d,q,a=()):
 try:r=d.execute(q,a).fetchone();return dict(r) if r else {}
 except:return {}
def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []
def collect():
 now=time.time();o={'now':now,'hot':{},'pipe':{},'v64':{},'events':[]}
 try:
  d=op(V5)
  if has(d,'v517_provider_stats'):
   s=row(d,"SELECT * FROM v517_provider_stats ORDER BY started_at DESC LIMIT 1")
   active=one(d,"SELECT COUNT(*) FROM v515_hot_tokens WHERE status='ACTIVE' AND expires_at>?",(now,)) if has(d,'v515_hot_tokens') else 0
   pending=one(d,"SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'") if has(d,'v515_hot_queue') else 0
   fetching=one(d,"SELECT COUNT(*) FROM v515_hot_queue WHERE status='FETCHING'") if has(d,'v515_hot_queue') else 0
   rps=float(s.get('current_rps') or 0);s.update({'active_hot':active,'queue':pending,'fetching':fetching,'debt_s':pending/rps if rps else None})
   ep=row(d,"SELECT * FROM v515_acquisition_epochs WHERE epoch_id=?",(s.get('epoch_id'),)) if has(d,'v515_acquisition_epochs') else {}
   s['epoch']=ep;o['hot']=s
  o['pipe']['raw']=one(d,"SELECT COUNT(*) FROM v5_raw_transactions") if has(d,'v5_raw_transactions') else 0
  d.close()
 except Exception as e:o['hot_error']=repr(e)
 try:
  d=op(V52);o['pipe']['processed']=one(d,"SELECT COUNT(*) FROM v52_processed");o['pipe']['swaps']=one(d,"SELECT COUNT(*) FROM v52_swaps");o['pipe']['snapshots']=one(d,"SELECT COUNT(*) FROM v52_snapshots");o['pipe']['pv20']=one(d,"SELECT COUNT(*) FROM v52_snapshots WHERE stage_s=20 AND price_velocity IS NOT NULL");d.close()
 except:pass
 try:
  d=op(RDB);rule=row(d,"SELECT * FROM v64_frozen_rule LIMIT 1");summ=row(d,"SELECT * FROM v64_forward_summary LIMIT 1");o['v64']={'rule':rule,'summary':summ}
  if rule:
   rid=rule['rule_id'];o['events']=rows(d,"SELECT token_mint,cutoff_ts,feature_value,state,fill_delay_s,path_points,net_return,exit_reason,mfe,mae,updated_at FROM v64_forward_events WHERE rule_id=? ORDER BY cutoff_ts DESC LIMIT 12",(rid,))
   o['v64']['exits']=rows(d,"SELECT COALESCE(exit_reason,'—') reason,COUNT(*) n FROM v64_forward_events WHERE rule_id=? AND state='DONE' GROUP BY exit_reason ORDER BY n DESC",(rid,))
  d.close()
 except Exception as e:o['v64_error']=repr(e)
 return o
HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V6.6</title><style>
:root{--b:#05090d;--p:#09121a;--l:#1b2b37;--t:#e8eef4;--m:#71818e;--c:#54d5e5;--g:#67e699;--a:#efb14c;--r:#ff7185}*{box-sizing:border-box}body{margin:0;background:var(--b);color:var(--t);font-family:Inter,-apple-system,sans-serif}.w{max-width:1800px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;align-items:end}.brand{font-weight:900;letter-spacing:.09em;font-size:20px}.muted{color:var(--m);font:10px ui-monospace}.grid{display:grid;grid-template-columns:1fr 1.35fr;gap:9px}.panel{background:linear-gradient(180deg,#0a131b,#080e14);border:1px solid var(--l);border-radius:10px;padding:12px;margin-top:9px}.title{font:900 10px ui-monospace;letter-spacing:.14em}.k{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:10px}.card{background:#0b161f;border:1px solid #172a36;border-radius:7px;padding:9px}.lab{font:8px ui-monospace;color:var(--m)}.val{font:900 20px ui-monospace;margin-top:3px}.good{color:var(--g)}.warn{color:var(--a)}.bad{color:var(--r)}.bar{height:13px;background:#101d27;border-radius:3px;overflow:hidden;margin-top:5px}.fill{height:100%;background:var(--c)}.row{display:grid;grid-template-columns:150px 1fr 70px;gap:8px;align-items:center;margin:8px 0;font:10px ui-monospace}.rule{font:11px ui-monospace;line-height:1.7;background:#071018;border:1px solid #152733;padding:9px;border-radius:7px;margin-top:9px}.progress{height:18px;background:#101d27;border-radius:4px;overflow:hidden;margin:10px 0}.progress div{height:100%;background:var(--g)}table{width:100%;border-collapse:collapse;margin-top:8px;font:9px ui-monospace}th,td{padding:6px;border-bottom:1px solid #14232d;text-align:right}th:first-child,td:first-child{text-align:left}.state{font-weight:800}.mono{font-family:ui-monospace}@media(max-width:1050px){.grid{grid-template-columns:1fr}.k{grid-template-columns:repeat(2,1fr)}}</style></head><body><div class="w"><div class="top"><div><div class="brand">MEMECOIN LAB // COMMAND CENTER V6.6</div><div class="muted">ALCHEMY HOT LANE + FULL V6.4 EXPERIMENT AUDIT</div></div><div id="clock" class="muted">CONNECTING</div></div><div class="grid"><div><div class="panel"><div class="title">01 / ALCHEMY HOT LANE — CURRENT EPOCH</div><div id="hot"></div></div><div class="panel"><div class="title">02 / PIPELINE</div><div id="pipe" class="k"></div></div></div><div><div class="panel"><div class="title">03 / V6.4 EXPERIMENT — FROZEN RULE + FORWARD EVIDENCE</div><div id="v64"></div></div><div class="panel"><div class="title">04 / LATEST V6.4 OBSERVATIONS</div><div id="events"></div></div></div></div></div><script>
const f=(x,n=1)=>x==null?'—':Number(x).toFixed(n),pct=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%',esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
function card(l,v,c=''){return `<div class="card"><div class="lab">${l}</div><div class="val ${c}">${v}</div></div>`}function bar(l,n,total){let p=total?Math.min(100,100*n/total):0;return `<div class="row"><span>${l}</span><div class="bar"><div class="fill" style="width:${p}%"></div></div><b>${n}</b></div>`}
async function go(){try{let d=await (await fetch('/api')).json();clock.textContent=new Date(d.now*1000).toLocaleTimeString()+' LIVE';let h=d.hot||{},e=h.epoch||{};let debt=Number(h.debt_s||0),q=Number(h.queue||0);hot.innerHTML=`<div class="k">${card('ACTIVE HOT',h.active_hot||0,h.active_hot>=60?'warn':'good')}${card('QUEUE',q,q>500?'bad':q>100?'warn':'good')}${card('DEBT',f(debt,1)+'s',debt>30?'bad':debt>10?'warn':'good')}${card('RPS',f(h.current_rps,1),'good')}${card('CREATES',h.creates_seen||0)}${card('ADMITTED',h.creates_admitted||0)}${card('HOT LOGS',h.hot_logs||0)}${card('ENQUEUED',h.hot_enqueued||0)}${card('REQUESTS',h.requests||0)}${card('INSERTED',h.inserted||0)}${card('429',h.http_429||0,(h.http_429||0)?'bad':'good')}${card('ERRORS',h.errors||0,(h.errors||0)?'warn':'good')}</div><div class="rule">epoch=${esc(h.epoch_id)}<br>base sampling=1/${e.base_sample_mod||'?'} · HOT TTL=${f(e.hot_ttl_s,0)}s · configured RPS=${f(e.base_rps,0)}→${f(e.max_rps,0)} · reconnects=${h.reconnects||0}</div>`;
let p=d.pipe||{};pipe.innerHTML=card('RAW',p.raw||0)+card('PROCESSED',p.processed||0)+card('SWAPS',p.swaps||0)+card('SNAPSHOTS',p.snapshots||0)+card('STAGE20 PV',p.pv20||0);
let v=d.v64||{},r=v.rule||{},s=v.summary||{},done=Number(s.done||0),confirm=30,signals=Number(s.signals||0);v64.innerHTML=`<div class="rule"><b>FROZEN RULE</b><br>source=${esc(r.source_experiment_id)}<br>feature=${esc(r.feature)} · stage=${r.stage_s}s · horizon=${r.horizon_s}s · direction=${f(r.direction,0)} · threshold=${f(r.threshold,6)}<br>TP=${f(r.tp_pct,0)}% · SL=${f(r.sl_pct,0)}% · fill≤${f(r.fill_window_s,0)}s · costs=${f(r.cost_pct,2)}%<br>source holdout: n=${r.source_holdout_n||0} · exp=${f(r.source_expectancy,2)}% · PF=${f(r.source_pf,2)} · fill=${pct(r.source_fill_rate)}<br>frozen=${r.frozen_at?new Date(r.frozen_at*1000).toLocaleString():'—'} · watermark=${r.frozen_max_cutoff_ts?new Date(r.frozen_max_cutoff_ts*1000).toLocaleString():'—'}</div><div class="k">${card('STATUS',esc(s.status||'WAITING'))}${card('DONE',done+'/'+confirm,done>=confirm?'good':'warn')}${card('EXPECTANCY',f(s.expectancy,2)+'%',Number(s.expectancy)>0?'good':'bad')}${card('PF',f(s.profit_factor,2),Number(s.profit_factor)>1?'good':'bad')}${card('WIN RATE',pct(s.win_rate))}${card('FILL RATE',pct(s.fill_rate))}${card('FILL DELAY MED',f(s.median_fill_delay,2)+'s')}${card('MAX DD',f(s.max_drawdown,2)+'%')}</div><div class="progress"><div style="width:${Math.min(100,100*done/confirm)}%"></div></div><div class="muted">CONFIRMATION PROGRESS ${done}/${confirm} DONE — rule remains immutable</div>${bar('ELIGIBLE',s.eligible||0,s.eligible||1)}${bar('SIGNALS',signals,s.eligible||1)}${bar('NO SIGNAL',s.no_signal||0,s.eligible||1)}${bar('WAIT FILL',s.waiting_fill||0,signals||1)}${bar('NO FILL',s.no_fill||0,signals||1)}${bar('WAIT MATURITY',s.waiting_maturity||0,signals||1)}${bar('SPARSE PATH',s.sparse_path||0,signals||1)}${bar('ANOMALY',s.anomaly||0,signals||1)}${bar('DONE',done,signals||1)}<div class="rule"><b>WHAT THIS EXPERIMENT TESTS</b><br>At each new stage-${r.stage_s||20}s snapshot after the frozen watermark, price_velocity is compared with the frozen V6.3 threshold. A signal is paper-filled at the first observed post-signal price within ${f(r.fill_window_s,0)}s. TP/SL and the ${r.horizon_s||120}s clock start at that actual fill. Only DONE paths enter expectancy/PF. NO_FILL, SPARSE_PATH and ANOMALY remain explicit failures/data-quality states and are never silently converted.</div>`;
let es=d.events||[];events.innerHTML=`<table><thead><tr><th>TOKEN</th><th>STATE</th><th>PV</th><th>FILL Δ</th><th>PTS</th><th>NET</th><th>EXIT</th><th>MFE</th><th>MAE</th></tr></thead><tbody>${es.map(x=>`<tr><td>${esc((x.token_mint||'').slice(0,10))}</td><td class="state">${esc(x.state)}</td><td>${f(x.feature_value,2)}</td><td>${f(x.fill_delay_s,1)}s</td><td>${x.path_points??'—'}</td><td>${f(x.net_return,1)}%</td><td>${esc(x.exit_reason)}</td><td>${f(x.mfe,1)}</td><td>${f(x.mae,1)}</td></tr>`).join('')}</tbody></table>`}catch(e){clock.textContent='ERROR '+e}setTimeout(go,2000)}go();</script></body></html>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path=='/api':b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
if __name__=='__main__':
 print(f'MEMECOIN LAB V6.6 dashboard http://{HOST}:{PORT}',flush=True);ThreadingHTTPServer((HOST,PORT),H).serve_forever()
