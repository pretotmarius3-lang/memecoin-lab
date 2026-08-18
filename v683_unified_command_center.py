#!/usr/bin/env python3
"""Memecoin Lab V6.8.3 — unified command center.
Read-only dashboard over strategy science, exact path risk, V7.1 portfolio,
research intelligence, journal, creative hypotheses, design and integrity audits.
"""
from __future__ import annotations
import json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
RDB=ROOT/'research_v4_1.db'; INTEL=ROOT/'v69_intelligence.db'; EXACT=ROOT/'v702_exact_path_risk.db'; CREATIVE=ROOT/'v693_creative.db'; DESIGN=ROOT/'v695_experimental_design.db'; INTEGRITY=ROOT/'v70_integrity.db'; V5=ROOT/'v5_raw_events.db'; V52=ROOT/'v52_features.db'
HOST=os.environ.get('MEMECOIN_V683_HOST','127.0.0.1'); PORT=int(os.environ.get('MEMECOIN_V683_PORT','8794'))
def op(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=3);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=3000');return d
def has(d,t):return bool(d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())
def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []
def row(d,q,a=()):
 try:x=d.execute(q,a).fetchone();return dict(x) if x else {}
 except:return {}
def one(d,q,a=(),default=None):
 try:x=d.execute(q,a).fetchone();return default if not x or x[0] is None else x[0]
 except:return default
def collect():
 o={'now':time.time(),'strategies':{},'exact':{},'portfolio':[],'journal':[],'creative':[],'design':[],'integrity':[],'hot':{},'pipe':{}}
 try:
  d=op(INTEL)
  if has(d,'strategy_intelligence'):
   for x in rows(d,'SELECT * FROM strategy_intelligence ORDER BY role,label'):o['strategies'][x['label']]=x
  if has(d,'research_journal'):o['journal']=rows(d,'SELECT created_at,label,milestone,conclusion,action FROM research_journal ORDER BY created_at DESC LIMIT 12')
  d.close()
 except:pass
 try:
  d=op(EXACT);latest=one(d,'SELECT MAX(created_at) FROM exact_path_summary')
  if latest:
   for x in rows(d,'SELECT * FROM exact_path_summary WHERE created_at=? ORDER BY strategy,risk_pct',(latest,)):o['exact'].setdefault(x['strategy'],[]).append(x)
  d.close()
 except:pass
 try:
  d=op(RDB)
  if has(d,'v71_portfolio_summary'):
   aid=one(d,'SELECT arena_id FROM v71_freeze LIMIT 1')
   if aid:o['portfolio']=rows(d,'SELECT * FROM v71_portfolio_summary WHERE arena_id=? ORDER BY allocation',(aid,))
  if has(d,'v71_summary'):
   aid=one(d,'SELECT arena_id FROM v71_freeze LIMIT 1')
   if aid:o['portfolio_legs']=rows(d,'SELECT * FROM v71_summary WHERE arena_id=? ORDER BY label',(aid,))
  d.close()
 except:pass
 try:
  d=op(CREATIVE)
  if has(d,'creative_hypotheses'):o['creative']=rows(d,"SELECT label,hypothesis_type,priority,hypothesis,falsification_test FROM creative_hypotheses WHERE status='OPEN' ORDER BY priority DESC LIMIT 8")
  d.close()
 except:pass
 try:
  d=op(DESIGN)
  if has(d,'experiment_proposals'):o['design']=rows(d,'SELECT label,experiment_class,priority,objective,success_metric,status,proposal_id FROM experiment_proposals ORDER BY priority DESC LIMIT 8')
  d.close()
 except:pass
 try:
  d=op(INTEGRITY);latest=one(d,'SELECT MAX(created_at) FROM integrity_snapshots')
  if latest:o['integrity']=rows(d,'SELECT audit,subject,status,metrics_json,conclusion FROM integrity_snapshots WHERE created_at=? ORDER BY audit',(latest,))
  d.close()
 except:pass
 try:
  d=op(V5);s=row(d,'SELECT * FROM v517_provider_stats ORDER BY started_at DESC LIMIT 1') if has(d,'v517_provider_stats') else {};o['hot']=s;d.close()
 except:pass
 try:
  d=op(V52);o['pipe']={'swaps':one(d,'SELECT COUNT(*) FROM v52_swaps',default=0),'snapshots':one(d,'SELECT COUNT(*) FROM v52_snapshots',default=0),'processed':one(d,'SELECT COUNT(*) FROM v52_processed',default=0)};d.close()
 except:pass
 return o
HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab</title><style>:root{--b:#05090d;--p:#09121a;--l:#1b2b37;--t:#e8eef4;--m:#71818e;--g:#67e699;--a:#efb14c;--r:#ff7185;--c:#54d5e5}*{box-sizing:border-box}body{margin:0;background:var(--b);color:var(--t);font-family:Inter,-apple-system,sans-serif}.w{max-width:1850px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;align-items:end}.brand{font-weight:900;letter-spacing:.08em;font-size:20px}.muted{color:var(--m);font:10px ui-monospace}.panel{background:linear-gradient(180deg,#0a131b,#080e14);border:1px solid var(--l);border-radius:10px;padding:12px;margin-top:9px}.title{font:900 10px ui-monospace;letter-spacing:.13em}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:9px}.cols{display:grid;grid-template-columns:1fr 1fr;gap:9px}.strategy{background:#08131b;border:1px solid #1d3340;border-radius:9px;padding:12px}.goodb{border-color:#2d7654}.badb{border-color:#77313b}.name{font:900 13px ui-monospace}.badge{float:right;font:900 9px ui-monospace;padding:4px 6px;border-radius:4px;background:#11212c}.k{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:9px}.card{background:#0b161f;border:1px solid #172a36;border-radius:7px;padding:8px}.lab{font:8px ui-monospace;color:var(--m)}.val{font:900 17px ui-monospace;margin-top:3px}.good{color:var(--g)}.bad{color:var(--r)}.warn{color:var(--a)}.cyan{color:var(--c)}.feed{font:10px ui-monospace;line-height:1.65;background:#071018;border:1px solid #152733;padding:8px;border-radius:7px}.item{padding:7px 0;border-bottom:1px solid #13232e}.item:last-child{border-bottom:0}@media(max-width:1100px){.grid3,.cols{grid-template-columns:1fr}.k{grid-template-columns:repeat(2,1fr)}}</style></head><body><div class="w"><div class="top"><div><div class="brand">MEMECOIN LAB // COMMAND CENTER V6.8.3</div><div class="muted">SCIENCE · EXACT RISK · PORTFOLIO · RESEARCH · CREATIVE · INTEGRITY</div></div><div id="clock" class="muted">CONNECTING</div></div><div class="panel"><div class="title">01 / SCIENTIFIC STRATEGIES + EXACT PATH RISK</div><div id="strategies" class="grid3"></div></div><div class="panel"><div class="title">02 / V7.1 R64 + WALLET PORTFOLIO ARENA</div><div id="portfolio" class="feed"></div></div><div class="panel"><div class="title">03 / RESEARCH CONCLUSIONS</div><div id="conclusions" class="feed"></div></div><div class="cols"><div class="panel"><div class="title">04 / RESEARCH JOURNAL</div><div id="journal" class="feed"></div></div><div class="panel"><div class="title">05 / CREATIVE WORLD</div><div id="creative" class="feed"></div></div></div><div class="cols"><div class="panel"><div class="title">06 / EXPERIMENT DESIGN</div><div id="design" class="feed"></div></div><div class="panel"><div class="title">07 / RESEARCH INTEGRITY</div><div id="integrity" class="feed"></div></div></div><div class="cols"><div class="panel"><div class="title">08 / ALCHEMY</div><div id="hot" class="k"></div></div><div class="panel"><div class="title">09 / PIPELINE</div><div id="pipe" class="k"></div></div></div></div><script>const f=(x,n=2)=>x==null?'—':Number(x).toFixed(n),pct=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%',esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));const card=(l,v,c='')=>`<div class="card"><div class="lab">${l}</div><div class="val ${c}">${v}</div></div>`;const item=(t,b)=>`<div class="item"><b>${esc(t)}</b><br>${b}</div>`;function exactBlock(x){if(!x)return '';return `<div class="k">${card('TRUE DD',f(x.max_drawdown_pct)+'%','warn')}${card('BOOT DD P95',f(x.p95_boot_dd)+'%')}${card('MAX LOSS STREAK',x.max_loss_streak)}${card('P(DD>10%)',pct(x.prob_dd_gt_10))}</div><div class="muted">risk=${f(x.risk_pct,2)}% · return=${f(x.total_return_pct)}% · p99 DD=${f(x.p99_boot_dd)}%</div>`}function strat(label,s,ex){let bad=String(s?.verdict||'').includes('FAILED');let e=(ex||[]).find(x=>Number(x.risk_pct)===0.5);return `<div class="strategy ${bad?'badb':'goodb'}"><div class="name">${esc(label)}<span class="badge">${esc(s?.verdict||s?.evidence||'—')}</span></div><div class="k">${card('DONE',s?.done||0)}${card('EXP',f(s?.expectancy)+'%',Number(s?.expectancy)>0?'good':'bad')}${card('PF',f(s?.profit_factor),Number(s?.profit_factor)>1?'good':'bad')}${card('FILL',pct(s?.fill_rate))}</div>${exactBlock(e)}<div class="feed"><b>Next:</b> ${esc(s?.action||'—')}</div></div>`}async function go(){try{let d=await(await fetch('/api')).json();clock.textContent=new Date(d.now*1000).toLocaleTimeString()+' LIVE';let S=d.strategies||{},E=d.exact||{};strategies.innerHTML=strat('R64 // PRICE_VELOCITY',S['R64 // PRICE_VELOCITY'],E['R64'])+strat('WALLET_STRUCTURE',S['WALLET_STRUCTURE'],E['WALLET'])+strat('FLOW_DYNAMICS_CORRECTED',S['FLOW_DYNAMICS_CORRECTED'],E['FLOW']);let legs=(d.portfolio_legs||[]).map(x=>item('LEG '+x.label,`status=${esc(x.status)} · DONE=${x.done} · exp=${f(x.expectancy)}% · PF=${f(x.pf)} · fill=${pct(x.fill_rate)}`)).join('');let ps=(d.portfolio||[]).map(x=>item(x.allocation,`<span class="${String(x.status).includes('FAIL')?'bad':'cyan'}">${esc(x.status)}</span> · events=${x.active_events} · paired=${x.paired_buckets} · exp=${f(x.expectancy)}% · PF=${f(x.pf)} · DD=${f(x.max_drawdown)} · loss overlap=${pct(x.loss_overlap_rate)} · corr=${f(x.return_correlation)}`)).join('');portfolio.innerHTML=legs+ps||'Waiting for first future-only portfolio events.';conclusions.innerHTML=Object.values(S).map(x=>item(x.label,`<span class="${String(x.verdict).includes('FAILED')?'bad':String(x.verdict).includes('CONFIRMED')?'good':'warn'}">${esc(x.verdict)}</span> · DONE ${x.done} · exp ${f(x.expectancy)}% · PF ${f(x.profit_factor)}<br><b>Action:</b> ${esc(x.action)}`)).join('');journal.innerHTML=(d.journal||[]).map(x=>item(new Date(x.created_at*1000).toLocaleTimeString()+' · '+x.label,`${esc(x.milestone)} → <span class="cyan">${esc(x.conclusion)}</span><br>${esc(x.action)}`)).join('')||'No journal.';creative.innerHTML=(d.creative||[]).map(x=>item(`${x.label} // ${x.hypothesis_type}`,`P${f(x.priority,1)} · ${esc(x.hypothesis)}<br><b>TEST:</b> ${esc(x.falsification_test)}`)).join('')||'No open hypotheses.';design.innerHTML=(d.design||[]).map(x=>item(`${x.label} // ${x.experiment_class}`,`P${f(x.priority,1)} · ${esc(x.objective)}<br><b>Success:</b> ${esc(x.success_metric)}`)).join('')||'No proposed tests.';integrity.innerHTML=(d.integrity||[]).map(x=>item(`${x.audit} // ${x.subject}`,`${esc(x.status)} · ${esc(x.conclusion)}`)).join('')||'No integrity data.';let h=d.hot||{};hot.innerHTML=card('RPS',f(h.current_rps,1))+card('INSERTED',h.inserted||0)+card('429',h.http_429||0)+card('ERRORS',h.errors||0);let p=d.pipe||{};pipe.innerHTML=card('SWAPS',p.swaps||0)+card('SNAPSHOTS',p.snapshots||0)+card('PROCESSED',p.processed||0)}catch(e){clock.textContent='ERROR '+e}setTimeout(go,2000)}go();</script></body></html>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path=='/api':b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
if __name__=='__main__':print(f'MEMECOIN LAB V6.8.3 dashboard http://{HOST}:{PORT}',flush=True);ThreadingHTTPServer((HOST,PORT),H).serve_forever()
