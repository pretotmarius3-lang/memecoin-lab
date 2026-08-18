#!/usr/bin/env python3
"""Memecoin Lab V6.8.2 — unified live command center.
Read-only dashboard over scientific, intelligence, creative, design, integrity and risk DBs.
No scientific state mutation. No trading/signing.
"""
from __future__ import annotations
import json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"));V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"));V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
INTEL=ROOT/'v69_intelligence.db';CREATIVE=ROOT/'v693_creative.db';DESIGN=ROOT/'v695_experimental_design.db';INTEGRITY=ROOT/'v70_integrity.db';RISK=ROOT/'v701_risk_reality.db'
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
 now=time.time();o={'now':now,'hot':{},'pipe':{},'r64':{},'challengers':[],'intel':{},'journal':[],'creative':[],'design':[],'integrity':[],'risk':{}}
 try:
  d=op(V5);s=row(d,"SELECT * FROM v517_provider_stats ORDER BY started_at DESC LIMIT 1") if has(d,'v517_provider_stats') else {};active=one(d,"SELECT COUNT(*) FROM v515_hot_tokens WHERE status='ACTIVE' AND expires_at>?",(now,)) if has(d,'v515_hot_tokens') else 0;pending=one(d,"SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'") if has(d,'v515_hot_queue') else 0;rps=float(s.get('current_rps') or 0);s.update({'active_hot':active,'queue':pending,'debt_s':pending/rps if rps else None});o['hot']=s;d.close()
 except Exception as e:o['hot_error']=repr(e)
 try:d=op(V52);o['pipe']={'processed':one(d,"SELECT COUNT(*) FROM v52_processed"),'swaps':one(d,"SELECT COUNT(*) FROM v52_swaps"),'snapshots':one(d,"SELECT COUNT(*) FROM v52_snapshots")};d.close()
 except:pass
 try:
  d=op(RDB);o['r64']={'rule':row(d,"SELECT * FROM v64_frozen_rule LIMIT 1"),'summary':row(d,"SELECT * FROM v64_forward_summary LIMIT 1")}
  if has(d,'v673_forward_summary'):
   for s in rows(d,"SELECT * FROM v673_forward_summary ORDER BY label"):
    cid=s['challenger_id'];rule={}
    if s['label']=='WALLET_STRUCTURE' and has(d,'v672_frozen_challengers'):rule=row(d,"SELECT * FROM v672_frozen_challengers WHERE challenger_id=?",(cid,))
    elif s['label']=='FLOW_DYNAMICS_CORRECTED' and has(d,'v6721_corrected_freezes'):rule=row(d,"SELECT * FROM v6721_corrected_freezes WHERE challenger_id=?",(cid,))
    o['challengers'].append({'rule':rule,'summary':s})
  d.close()
 except Exception as e:o['research_error']=repr(e)
 try:
  d=op(INTEL)
  if has(d,'strategy_intelligence'):
   for x in rows(d,'SELECT * FROM strategy_intelligence ORDER BY role,label'):o['intel'][x['label']]=x
  if has(d,'research_journal'):o['journal']=rows(d,'SELECT created_at,label,milestone,conclusion,action FROM research_journal ORDER BY created_at DESC LIMIT 10')
  d.close()
 except:pass
 try:
  d=op(CREATIVE)
  if has(d,'creative_hypotheses'):o['creative']=rows(d,"SELECT label,hypothesis_type,priority,hypothesis,falsification_test,status FROM creative_hypotheses WHERE status='OPEN' ORDER BY priority DESC, updated_at DESC LIMIT 10")
  d.close()
 except:pass
 try:
  d=op(DESIGN)
  if has(d,'experiment_proposals'):o['design']=rows(d,"SELECT label,hypothesis_type,priority,experiment_class,objective,success_metric,status,proposal_id FROM experiment_proposals ORDER BY priority DESC,created_at DESC LIMIT 10")
  d.close()
 except:pass
 try:
  d=op(INTEGRITY);latest=one(d,'SELECT MAX(created_at) FROM integrity_snapshots',default=None)
  if latest:o['integrity']=rows(d,'SELECT audit,subject,status,metrics_json,conclusion FROM integrity_snapshots WHERE created_at=? ORDER BY audit',(latest,))
  d.close()
 except:pass
 try:
  d=op(RISK);latest=one(d,'SELECT MAX(created_at) FROM risk_reality',default=None)
  if latest:
   for x in rows(d,'SELECT * FROM risk_reality WHERE created_at=? ORDER BY label,risk_pct',(latest,)):o['risk'].setdefault(x['label'],[]).append(x)
  d.close()
 except:pass
 return o
HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab Command Center</title><style>:root{--b:#05090d;--l:#1b2b37;--t:#e8eef4;--m:#71818e;--g:#67e699;--a:#efb14c;--r:#ff7185;--c:#54d5e5}*{box-sizing:border-box}body{margin:0;background:var(--b);color:var(--t);font-family:Inter,-apple-system,sans-serif}.w{max-width:1850px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;align-items:end}.brand{font-weight:900;letter-spacing:.09em;font-size:20px}.muted{color:var(--m);font:10px ui-monospace}.panel{background:linear-gradient(180deg,#0a131b,#080e14);border:1px solid var(--l);border-radius:10px;padding:12px;margin-top:9px}.title{font:900 10px ui-monospace;letter-spacing:.14em}.strategies{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin-top:10px}.strategy{background:#08131b;border:1px solid #19303d;border-radius:9px;padding:12px}.champ{border-color:#2e7b57}.failed{border-color:#77313b}.survive{border-color:#765b2d}.name{font:900 13px ui-monospace}.badge{float:right;font:900 9px ui-monospace;padding:4px 6px;border-radius:4px;background:#11212c}.k{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:10px}.card{background:#0b161f;border:1px solid #172a36;border-radius:7px;padding:8px}.lab{font:8px ui-monospace;color:var(--m)}.val{font:900 17px ui-monospace;margin-top:3px}.good{color:var(--g)}.warn{color:var(--a)}.bad{color:var(--r)}.rule,.feed{font:10px ui-monospace;line-height:1.65;background:#071018;border:1px solid #152733;padding:8px;border-radius:7px;margin-top:9px}.progress{height:10px;background:#101d27;border-radius:4px;overflow:hidden;margin:9px 0}.progress div{height:100%;background:var(--g)}.cols{display:grid;grid-template-columns:1fr 1fr;gap:9px}.item{padding:8px 0;border-bottom:1px solid #13232e}.item:last-child{border-bottom:0}.prio{color:var(--a);font-weight:900}.cyan{color:var(--c)}@media(max-width:1100px){.strategies,.cols{grid-template-columns:1fr}.k{grid-template-columns:repeat(2,1fr)}}</style></head><body><div class="w"><div class="top"><div><div class="brand">MEMECOIN LAB // LIVE COMMAND CENTER</div><div class="muted">SCIENCE · CONCLUSIONS · CREATIVE RESEARCH · EXPERIMENT DESIGN · INTEGRITY · RISK</div></div><div id="clock" class="muted">CONNECTING</div></div><div class="panel"><div class="title">01 / STRATEGY ARENA</div><div id="strategies" class="strategies"></div></div><div class="cols"><div class="panel"><div class="title">02 / ALCHEMY HOT LANE</div><div id="hot" class="k"></div></div><div class="panel"><div class="title">03 / FEATURE PIPELINE</div><div id="pipe" class="k"></div></div></div><div class="panel"><div class="title">04 / SCIENTIFIC CONCLUSIONS — CURRENT VERDICTS</div><div id="conclusions" class="feed"></div></div><div class="cols"><div class="panel"><div class="title">05 / RESEARCH JOURNAL — WHAT CHANGED</div><div id="journal" class="feed"></div></div><div class="panel"><div class="title">06 / CREATIVE RESEARCH — OPEN HYPOTHESES</div><div id="creative" class="feed"></div></div></div><div class="cols"><div class="panel"><div class="title">07 / EXPERIMENTAL DESIGN — PROPOSED TESTS</div><div id="design" class="feed"></div></div><div class="panel"><div class="title">08 / RESEARCH INTEGRITY — ACTIVE AUDITS</div><div id="integrity" class="feed"></div></div></div></div><script>const f=(x,n=1)=>x==null?'—':Number(x).toFixed(n),pct=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%',esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));function card(l,v,c=''){return `<div class="card"><div class="lab">${l}</div><div class="val ${c}">${v}</div></div>`}function strat(label,r,s,intel,risk){let done=Number(intel?.done??s.done??0),ver=intel?.verdict||s.status||'WAITING',exp=Number(intel?.expectancy??s.expectancy??0),pf=Number(intel?.profit_factor??s.profit_factor??0),cls=ver.includes('FAILED')?'failed':ver.includes('CONFIRMED')?'champ':'survive';let rr={};for(let x of risk||[])rr[Number(x.risk_pct)]=x;let raw=intel?.max_drawdown??s.max_drawdown;return `<div class="strategy ${cls}"><div class="name">${esc(label)}<span class="badge">${esc(ver)}</span></div><div class="rule">feature=${esc(r.feature)} · stage=${r.stage_s||'?'}s · h=${r.horizon_s||'?'}s · TP/SL=${f(r.tp_pct,0)}/${f(r.sl_pct,0)} · dir=${f(r.direction,0)}<br>threshold=${f(r.threshold,8)}</div><div class="k">${card('DONE',done+(label.includes('R64')?'':'/30'),done>=30?'good':'warn')}${card('EXP',f(exp,2)+'%',exp>0?'good':'bad')}${card('PF',f(pf,2),pf>1?'good':'bad')}${card('FILL',pct(intel?.fill_rate??s.fill_rate))}${card('RAW DD',f(raw,1)+' pts','warn')}${card('DD @0.25%',rr[.25]?f(rr[.25].compound_dd_est,2)+'%':'—')}${card('DD @0.50%',rr[.5]?f(rr[.5].compound_dd_est,2)+'%':'—')}${card('DD @1.00%',rr[1]?f(rr[1].compound_dd_est,2)+'%':'—')}</div>${!label.includes('R64')?`<div class="progress"><div style="width:${Math.min(100,100*done/30)}%"></div></div>`:''}</div>`}function it(title,body,extra=''){return `<div class="item"><b>${esc(title)}</b>${extra?` · ${extra}`:''}<br>${body}</div>`}async function go(){try{let d=await(await fetch('/api')).json();clock.textContent=new Date(d.now*1000).toLocaleTimeString()+' LIVE';let r=d.r64||{},html=strat('R64 // PRICE_VELOCITY',r.rule||{},r.summary||{},d.intel?.['R64 // PRICE_VELOCITY'],d.risk?.['R64 // PRICE_VELOCITY']);for(let c of d.challengers||[])html+=strat(c.summary.label,c.rule||{},c.summary||{},d.intel?.[c.summary.label],d.risk?.[c.summary.label]);strategies.innerHTML=html;let h=d.hot||{},debt=Number(h.debt_s||0);hot.innerHTML=card('ACTIVE HOT',h.active_hot||0,'good')+card('QUEUE',h.queue||0,(h.queue||0)>500?'bad':'good')+card('DEBT',f(debt,1)+'s',debt>30?'bad':'good')+card('RPS',f(h.current_rps,1),'good')+card('INSERTED',h.inserted||0)+card('429',h.http_429||0,(h.http_429||0)?'bad':'good')+card('ERRORS',h.errors||0,(h.errors||0)?'warn':'good');let p=d.pipe||{};pipe.innerHTML=card('PROCESSED',p.processed||0)+card('SWAPS',p.swaps||0)+card('SNAPSHOTS',p.snapshots||0);
let ci=Object.values(d.intel||{});conclusions.innerHTML=ci.map(x=>it(x.label,`<span class="${String(x.verdict).includes('FAILED')?'bad':String(x.verdict).includes('CONFIRMED')?'good':'warn'}">${esc(x.verdict)}</span> · DONE ${x.done} · exp ${f(x.expectancy,2)}% · PF ${f(x.profit_factor,2)} · fill ${pct(x.fill_rate)}<br><b>Conclusion:</b> ${esc(x.verdict)}<br><b>Next:</b> ${esc(x.action)}`)).join('')||'No intelligence verdicts yet.';
journal.innerHTML=(d.journal||[]).map(x=>it(new Date(x.created_at*1000).toLocaleTimeString()+' · '+x.label,`${esc(x.milestone)} → <span class="cyan">${esc(x.conclusion)}</span><br>${esc(x.action)}`)).join('')||'No journal events yet.';
creative.innerHTML=(d.creative||[]).map(x=>it(`${x.label} // ${x.hypothesis_type}`,`${esc(x.hypothesis)}<br><b>TEST:</b> ${esc(x.falsification_test)}`,`<span class="prio">P${f(x.priority,1)}</span>`)).join('')||'No open creative hypotheses.';
design.innerHTML=(d.design||[]).map(x=>it(`${x.label} // ${x.experiment_class}`,`${esc(x.objective)}<br><b>Success:</b> ${esc(x.success_metric)}<br><span class="muted">${esc(x.proposal_id)} · ${esc(x.status)}</span>`,`<span class="prio">P${f(x.priority,1)}</span>`)).join('')||'No experiment proposals.';
integrity.innerHTML=(d.integrity||[]).map(x=>{let m={};try{m=JSON.parse(x.metrics_json||'{}')}catch{};return it(`${x.audit} // ${x.subject}`,`${esc(x.conclusion)}<br><span class="muted">${esc(JSON.stringify(m))}</span>`,`<span class="cyan">${esc(x.status)}</span>`) }).join('')||'No integrity audit snapshot yet.';
}catch(e){clock.textContent='ERROR '+e}setTimeout(go,2000)}go();</script></body></html>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path=='/api':b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
if __name__=='__main__':print(f'MEMECOIN LAB V6.8.2 dashboard http://{HOST}:{PORT}',flush=True);ThreadingHTTPServer((HOST,PORT),H).serve_forever()
