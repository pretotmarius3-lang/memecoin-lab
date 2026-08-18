#!/usr/bin/env python3
"""MEMECOIN LAB — ANIMATED RESEARCH COCKPIT V7.5.2
Read-only visual layer. Never mutates scientific/frozen state.
"""
from __future__ import annotations
import json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
DBS={"intel":ROOT/'v69_intelligence.db',"research":ROOT/'research_v4_1.db',"decay":ROOT/'v721_decay_monitor.db',"factory":ROOT/'v73_robust_factory.db',"portfolio":ROOT/'v72_portfolio_reality.db'}
HOST=os.environ.get('MEMECOIN_V75_HOST','127.0.0.1');PORT=int(os.environ.get('MEMECOIN_V75_PORT','8795'))
def op(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=2);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=2000');return d
def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []
def collect():
 o={'now':time.time(),'strategies':[],'journal':[],'decay':[],'factory':[],'portfolio':[]}
 try:
  d=op(DBS['intel']);o['strategies']=rows(d,'SELECT * FROM strategy_intelligence ORDER BY role,label');o['journal']=rows(d,'SELECT * FROM research_journal ORDER BY created_at DESC LIMIT 16');d.close()
 except:pass
 try:
  d=op(DBS['factory']);o['factory']=rows(d,'SELECT * FROM robust_candidates ORDER BY rank LIMIT 12');d.close()
 except:pass
 try:
  d=op(DBS['portfolio']);o['portfolio']=rows(d,'SELECT * FROM v72_summary ORDER BY created_at DESC LIMIT 1');d.close()
 except:pass
 try:
  d=op(DBS['decay']);tabs=rows(d,"SELECT name FROM sqlite_master WHERE type='table'")
  for t in tabs:
   n=t['name'];cs=[x['name'] for x in rows(d,f'PRAGMA table_info({n})')]
   if any(x in cs for x in ('state','verdict')):o['decay']+=rows(d,f'SELECT * FROM {n} ORDER BY rowid DESC LIMIT 8')
  d.close()
 except:pass
 return o
HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V7.5.2</title><style>
:root{--line:#173142;--text:#e9f4fa;--muted:#6e8491;--green:#58f0a5;--cyan:#51dbea;--amber:#ffbd59;--red:#ff667c}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% -20%,#123044 0,#050a0f 38%,#020507 80%);color:var(--text);font-family:Inter,-apple-system,sans-serif}body:before{content:'';position:fixed;inset:0;background-image:linear-gradient(#ffffff05 1px,transparent 1px),linear-gradient(90deg,#ffffff05 1px,transparent 1px);background-size:42px 42px;pointer-events:none}.wrap{max-width:1900px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between}.brand{font:900 22px ui-monospace;letter-spacing:.11em}.live{font:800 10px ui-monospace;color:var(--green);animation:pulse 1.8s infinite}.sub{font:10px ui-monospace;color:var(--muted)}.panel{background:#081119dd;border:1px solid var(--line);border-radius:14px;padding:14px;margin-top:12px}.title{font:900 10px ui-monospace;letter-spacing:.16em;color:#9cb1bd}.hero{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.node{border:1px solid #1c394a;border-radius:13px;padding:15px;background:#07131b}.node.champ{border-color:#2b7756}.name{font:900 13px ui-monospace}.badge{float:right;font:900 9px ui-monospace}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:13px}.metric,.event{background:#0a1922;border:1px solid #17303e;border-radius:8px;padding:8px}.ml{font:8px ui-monospace;color:var(--muted)}.mv{font:900 18px ui-monospace}.green{color:var(--green)}.red{color:var(--red)}.amber{color:var(--amber)}.cyan{color:var(--cyan)}.flow{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-top:12px}.stage{text-align:center;padding:13px;border:1px solid #183444;border-radius:9px;font:800 9px ui-monospace}.cols{display:grid;grid-template-columns:1.1fr .9fr;gap:12px}.factory{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.cand{padding:10px;border:1px solid #18313f;border-radius:9px;background:#07131b}.rank{font:900 18px ui-monospace;color:#415968}.feed{font:10px ui-monospace;line-height:1.6}.portfolioHero{border:1px solid #27526a;border-radius:10px;padding:12px;background:#07151e;box-shadow:0 0 24px #51dbea0b}.pgrid{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-top:10px}.pbig{font:900 22px ui-monospace}.event{margin-top:7px}@keyframes pulse{50%{opacity:.35}}@media(max-width:1100px){.hero,.cols{grid-template-columns:1fr}.factory{grid-template-columns:repeat(2,1fr)}}
</style></head><body><div class="wrap"><div class="top"><div><div class="brand">MEMECOIN LAB // V7.5.2</div><div class="sub">ANIMATED RESEARCH COCKPIT · READ ONLY · SCIENCE FIRST</div></div><div><div class="live">● LIVE RESEARCH BUS</div><div id="clock" class="sub"></div></div></div><div class="panel"><div class="title">CHAMPION / CHALLENGER FIELD</div><div id="hero" class="hero"></div></div><div class="panel"><div class="title">RESEARCH PIPELINE</div><div class="flow"><div class="stage">RAW EVENTS</div><div class="stage">FEATURES</div><div class="stage">DISCOVERY</div><div class="stage">ROBUST GATES</div><div class="stage">FUTURE ONLY</div><div class="stage">DECAY WATCH</div></div></div><div class="cols"><div class="panel"><div class="title">V7.3 ROBUST CHALLENGER FACTORY</div><div id="factory" class="factory"></div></div><div class="panel"><div class="title">V7.2 PORTFOLIO REALITY + DECAY</div><div id="portfolio-status" class="feed"><div class="event">Loading portfolio reality…</div></div></div></div><div class="panel"><div class="title">RESEARCH BRAIN // CONCLUSIONS & JOURNAL</div><div id="journal" class="feed"></div></div></div><script>
const E=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),F=(x,n=2)=>x==null?'—':Number(x).toFixed(n),P=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%';
const el=id=>document.getElementById(id);
function node(s){let v=String(s.verdict||''),good=v.includes('CONFIRMED')&&!v.includes('FAILED'),bad=v.includes('FAILED');return `<div class="node ${good?'champ':''}"><div class="name">${E(s.label)}<span class="badge ${bad?'red':good?'green':'amber'}">${E(v)}</span></div><div class="metrics"><div class="metric"><div class="ml">DONE</div><div class="mv">${s.done||0}</div></div><div class="metric"><div class="ml">EXP</div><div class="mv ${Number(s.expectancy)>0?'green':'red'}">${F(s.expectancy)}%</div></div><div class="metric"><div class="ml">PF</div><div class="mv">${F(s.profit_factor)}</div></div><div class="metric"><div class="ml">FILL</div><div class="mv cyan">${P(s.fill_rate)}</div></div></div><div class="sub" style="margin-top:10px">${E(s.action)}</div></div>`}
function renderPortfolio(d){const box=el('portfolio-status'),p=(d.portfolio||[])[0];let h='';if(p){h=`<div class="portfolioHero"><div class="name">PRIMARY 50/50 <span class="badge ${p.verdict==='DILUTES_R64'?'red':'green'}">${E(p.verdict)}</span></div><div class="pgrid"><div class="metric"><div class="ml">R64 RETURN / TRUE DD</div><div class="pbig green">+${F(p.r64_return)}% / ${F(p.r64_true_dd)}%</div></div><div class="metric"><div class="ml">50/50 RETURN / TRUE DD</div><div class="pbig amber">+${F(p.portfolio_return)}% / ${F(p.portfolio_true_dd)}%</div></div><div class="metric"><div class="ml">MARGINAL RETURN VS R64</div><div class="pbig ${p.marginal_return_vs_r64>=0?'green':'red'}">${F(p.marginal_return_vs_r64)}pp</div></div><div class="metric"><div class="ml">PAIRS / OVERLAP</div><div class="pbig cyan">${p.paired_temporal} / ${P(p.overlap_rate)}</div></div></div><div class="sub" style="margin-top:9px">corr=${F(p.return_corr,3)} · both loss=${P(p.both_loss_rate)} · R64 DONE=${p.r64_done} · WALLET DONE=${p.wallet_done}</div></div>`}else h='<div class="event red">V7.2 API has no portfolio row.</div>';const decay=(d.decay||[]);if(decay.length){h+='<div class="title" style="margin-top:13px">POST-CONFIRMATION DECAY</div>'+decay.slice(0,6).map(x=>`<div class="event">${E(x.label||x.strategy||x.strategy_id||'DECAY')} · <span class="amber">${E(x.state||x.status||x.verdict||'WATCH')}</span> · DONE ${E(x.done??'—')}</div>`).join('')}else h+='<div class="event"><span class="amber">DECAY:</span> no renderable row yet.</div>';box.innerHTML=h}
async function go(){try{let d=await(await fetch('/api?ts='+Date.now(),{cache:'no-store'})).json();el('clock').textContent=new Date(d.now*1000).toLocaleTimeString();el('hero').innerHTML=(d.strategies||[]).map(node).join('');el('factory').innerHTML=(d.factory||[]).map(x=>`<div class="cand"><span class="rank">#${String(x.rank).padStart(2,'0')}</span> <b>${E(x.status)}</b><br><span class="cyan">${E(x.feature)}</span><br><span class="sub">${E(x.family)} · score ${F(x.robust_score)} · HO ${x.holdout_n} · PF ${F(x.holdout_pf)}</span></div>`).join('');renderPortfolio(d);el('journal').innerHTML=(d.journal||[]).map(x=>`<div class="event"><span class="sub">${new Date(x.created_at*1000).toLocaleTimeString()}</span> · <b>${E(x.label)}</b> · <span class="cyan">${E(x.conclusion)}</span><br>${E(x.action)}</div>`).join('')}catch(e){el('clock').textContent='BUS ERROR';el('portfolio-status').innerHTML='<div class="event red">FRONTEND ERROR: '+E(e.message||e)+'</div>'}setTimeout(go,1800)}go();
</script></body></html>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path.startswith('/api'):b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store, no-cache, must-revalidate, max-age=0');self.send_header('Pragma','no-cache');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
if __name__=='__main__':print(f'MEMECOIN LAB V7.5.2 cockpit http://{HOST}:{PORT}',flush=True);ThreadingHTTPServer((HOST,PORT),H).serve_forever()
