#!/usr/bin/env python3
"""Memecoin Lab V6.8 — live read-only strategy command center.
Shows R64 champion beside V6.7.3 immutable challengers plus acquisition/pipeline health.
No scientific state mutation. No trading/signing.
"""
from __future__ import annotations
import json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V68_DASH_HOST","127.0.0.1");PORT=int(os.environ.get("MEMECOIN_V68_DASH_PORT","8794"))
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
 now=time.time();o={'now':now,'hot':{},'pipe':{},'r64':{},'challengers':[]}
 try:
  d=op(V5);s=row(d,"SELECT * FROM v517_provider_stats ORDER BY started_at DESC LIMIT 1") if has(d,'v517_provider_stats') else {}
  active=one(d,"SELECT COUNT(*) FROM v515_hot_tokens WHERE status='ACTIVE' AND expires_at>?",(now,)) if has(d,'v515_hot_tokens') else 0
  pending=one(d,"SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'") if has(d,'v515_hot_queue') else 0
  rps=float(s.get('current_rps') or 0);s.update({'active_hot':active,'queue':pending,'debt_s':pending/rps if rps else None});o['hot']=s;d.close()
 except Exception as e:o['hot_error']=repr(e)
 try:
  d=op(V52);o['pipe']={'processed':one(d,"SELECT COUNT(*) FROM v52_processed"),'swaps':one(d,"SELECT COUNT(*) FROM v52_swaps"),'snapshots':one(d,"SELECT COUNT(*) FROM v52_snapshots")};d.close()
 except:pass
 try:
  d=op(RDB);rr=row(d,"SELECT * FROM v64_frozen_rule LIMIT 1");rs=row(d,"SELECT * FROM v64_forward_summary LIMIT 1");o['r64']={'rule':rr,'summary':rs}
  if has(d,'v673_forward_summary'):
   for s in rows(d,"SELECT * FROM v673_forward_summary ORDER BY label"):
    cid=s['challenger_id'];rule={}
    if s['label']=='WALLET_STRUCTURE' and has(d,'v672_frozen_challengers'):rule=row(d,"SELECT * FROM v672_frozen_challengers WHERE challenger_id=?",(cid,))
    elif s['label']=='FLOW_DYNAMICS_CORRECTED' and has(d,'v6721_corrected_freezes'):rule=row(d,"SELECT * FROM v6721_corrected_freezes WHERE challenger_id=?",(cid,))
    o['challengers'].append({'rule':rule,'summary':s})
  d.close()
 except Exception as e:o['research_error']=repr(e)
 return o
HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V6.8</title><style>:root{--b:#05090d;--p:#09121a;--l:#1b2b37;--t:#e8eef4;--m:#71818e;--c:#54d5e5;--g:#67e699;--a:#efb14c;--r:#ff7185}*{box-sizing:border-box}body{margin:0;background:var(--b);color:var(--t);font-family:Inter,-apple-system,sans-serif}.w{max-width:1800px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;align-items:end}.brand{font-weight:900;letter-spacing:.09em;font-size:20px}.muted{color:var(--m);font:10px ui-monospace}.panel{background:linear-gradient(180deg,#0a131b,#080e14);border:1px solid var(--l);border-radius:10px;padding:12px;margin-top:9px}.title{font:900 10px ui-monospace;letter-spacing:.14em}.strategies{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin-top:10px}.strategy{background:#08131b;border:1px solid #19303d;border-radius:9px;padding:12px}.champ{border-color:#2e7b57}.challenge{border-color:#765b2d}.name{font:900 13px ui-monospace}.badge{float:right;font:900 9px ui-monospace;padding:4px 6px;border-radius:4px;background:#11212c}.k{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:10px}.card{background:#0b161f;border:1px solid #172a36;border-radius:7px;padding:8px}.lab{font:8px ui-monospace;color:var(--m)}.val{font:900 18px ui-monospace;margin-top:3px}.good{color:var(--g)}.warn{color:var(--a)}.bad{color:var(--r)}.rule{font:10px ui-monospace;line-height:1.65;background:#071018;border:1px solid #152733;padding:8px;border-radius:7px;margin-top:9px}.progress{height:12px;background:#101d27;border-radius:4px;overflow:hidden;margin:9px 0}.progress div{height:100%;background:var(--g)}.health{display:grid;grid-template-columns:1fr 1fr;gap:9px}@media(max-width:1100px){.strategies{grid-template-columns:1fr}.health{grid-template-columns:1fr}.k{grid-template-columns:repeat(2,1fr)}}</style></head><body><div class="w"><div class="top"><div><div class="brand">MEMECOIN LAB // STRATEGY BOARD V6.8</div><div class="muted">R64 CHAMPION vs FROZEN FUTURE-ONLY CHALLENGERS</div></div><div id="clock" class="muted">CONNECTING</div></div><div class="panel"><div class="title">01 / STRATEGY ARENA — SIDE BY SIDE</div><div id="strategies" class="strategies"></div></div><div class="health"><div class="panel"><div class="title">02 / ALCHEMY HOT LANE</div><div id="hot" class="k"></div></div><div class="panel"><div class="title">03 / FEATURE PIPELINE</div><div id="pipe" class="k"></div></div></div><div class="panel"><div class="title">04 / CURRENT RESEARCH CONCLUSION</div><div id="conclusion" class="rule"></div></div></div><script>const f=(x,n=1)=>x==null?'—':Number(x).toFixed(n),pct=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%',esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));function card(l,v,c=''){return `<div class="card"><div class="lab">${l}</div><div class="val ${c}">${v}</div></div>`}function strategy(label,r,s,champ=false){let done=Number(s.done||0),target=30,exp=Number(s.expectancy||0),pf=Number(s.profit_factor||0);return `<div class="strategy ${champ?'champ':'challenge'}"><div class="name">${esc(label)}<span class="badge">${esc(s.status||'WAITING')}</span></div><div class="rule">feature=${esc(r.feature)}<br>stage=${r.stage_s||'?'}s · h=${r.horizon_s||'?'}s · TP/SL=${f(r.tp_pct,0)}/${f(r.sl_pct,0)} · dir=${f(r.direction,0)}<br>threshold=${f(r.threshold,8)} · fill≤${f(r.fill_window_s,0)}s</div><div class="k">${card('DONE',done+(champ?'':'/'+target),done>=target?'good':'warn')}${card('EXP',f(s.expectancy,2)+'%',exp>0?'good':'bad')}${card('PF',f(s.profit_factor,2),pf>1?'good':'bad')}${card('WIN',pct(s.win_rate))}${card('FILL',pct(s.fill_rate))}${card('DELAY',f(s.median_fill_delay,1)+'s')}${card('DD',f(s.max_drawdown,1)+'%')}${card('SIGNALS',s.signals||0)}</div>${champ?'':`<div class="progress"><div style="width:${Math.min(100,100*done/target)}%"></div></div><div class="muted">future-only confirmation ${done}/${target}</div>`}<div class="rule">SOURCE: n=${r.source_holdout_n||'—'} · exp=${f(r.source_expectancy,2)}% · PF=${f(r.source_pf,2)} · fill=${pct(r.source_fill_rate)}<br>eligible=${s.eligible||0} · no_fill=${s.no_fill||0} · sparse=${s.sparse_path||0} · anomaly=${s.anomaly||0}</div></div>`}async function go(){try{let d=await(await fetch('/api')).json();clock.textContent=new Date(d.now*1000).toLocaleTimeString()+' LIVE';let r=d.r64||{},html=strategy('R64 // PRICE VELOCITY',r.rule||{},r.summary||{},true);for(let c of d.challengers||[])html+=strategy(c.summary.label,c.rule||{},c.summary||{},false);strategies.innerHTML=html;let h=d.hot||{},debt=Number(h.debt_s||0);hot.innerHTML=card('ACTIVE HOT',h.active_hot||0,'good')+card('QUEUE',h.queue||0,(h.queue||0)>500?'bad':'good')+card('DEBT',f(debt,1)+'s',debt>30?'bad':'good')+card('RPS',f(h.current_rps,1),'good')+card('INSERTED',h.inserted||0)+card('429',h.http_429||0,(h.http_429||0)?'bad':'good')+card('ERRORS',h.errors||0,(h.errors||0)?'warn':'good');let p=d.pipe||{};pipe.innerHTML=card('PROCESSED',p.processed||0)+card('SWAPS',p.swaps||0)+card('SNAPSHOTS',p.snapshots||0);let rs=r.summary||{},cs=d.challengers||[],w=cs.find(x=>x.summary.label==='WALLET_STRUCTURE'),fl=cs.find(x=>x.summary.label==='FLOW_DYNAMICS_CORRECTED');conclusion.innerHTML=`R64 remains the confirmed control: ${rs.done||0} DONE · ${f(rs.expectancy,2)}% expectancy · PF ${f(rs.profit_factor,2)}.<br>WALLET_STRUCTURE: ${w?.summary.done||0}/30 DONE · ${f(w?.summary.expectancy,2)}% · PF ${f(w?.summary.profit_factor,2)} · fill ${pct(w?.summary.fill_rate)}.<br>FLOW_DYNAMICS: ${fl?.summary.done||0}/30 DONE · ${f(fl?.summary.expectancy,2)}% · PF ${f(fl?.summary.profit_factor,2)} · fill ${pct(fl?.summary.fill_rate)}.<br><b>No challenger is promoted before its frozen 30-DONE future-only gate.</b>`}catch(e){clock.textContent='ERROR '+e}setTimeout(go,2000)}go();</script></body></html>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path=='/api':b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
if __name__=='__main__':print(f'MEMECOIN LAB V6.8 dashboard http://{HOST}:{PORT}',flush=True);ThreadingHTTPServer((HOST,PORT),H).serve_forever()
