#!/usr/bin/env python3
"""MEMECOIN LAB — ANIMATED RESEARCH COCKPIT V7.5
Read-only visual layer. Never mutates scientific/frozen state.
"""
from __future__ import annotations
import json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"
DBS={"intel":ROOT/'v69_intelligence.db',"risk":ROOT/'v702_exact_path_risk.db',"research":ROOT/'research_v4_1.db',"decay":ROOT/'v721_decay_monitor.db',"factory":ROOT/'v73_robust_factory.db'}
HOST=os.environ.get('MEMECOIN_V75_HOST','127.0.0.1'); PORT=int(os.environ.get('MEMECOIN_V75_PORT','8795'))
def op(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=2);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=2000');return d
def has(d,t):return bool(d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())
def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []
def collect():
 o={'now':time.time(),'strategies':[],'journal':[],'decay':[],'factory':[],'portfolio':[]}
 try:
  d=op(DBS['intel']);o['strategies']=rows(d,'SELECT * FROM strategy_intelligence ORDER BY role,label');o['journal']=rows(d,'SELECT * FROM research_journal ORDER BY created_at DESC LIMIT 16');d.close()
 except:pass
 try:
  d=op(DBS['decay']);
  tabs=rows(d,"SELECT name FROM sqlite_master WHERE type='table'")
  for t in tabs:
   n=t['name']
   if 'summary' in n or 'monitor' in n:o['decay']+=rows(d,f'SELECT * FROM {n} ORDER BY rowid DESC LIMIT 8')
  d.close()
 except:pass
 try:
  d=op(DBS['factory']);o['factory']=rows(d,'SELECT * FROM robust_candidates ORDER BY rank LIMIT 12');d.close()
 except:pass
 try:
  d=op(DBS['research']);
  if has(d,'v71_portfolio_summary'):
   aid=d.execute('SELECT arena_id FROM v71_freeze LIMIT 1').fetchone();
   if aid:o['portfolio']=rows(d,'SELECT * FROM v71_portfolio_summary WHERE arena_id=? ORDER BY allocation',(aid[0],))
  d.close()
 except:pass
 return o
HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V7.5</title><style>
:root{--bg:#03070b;--panel:#081119cc;--line:#173142;--text:#e9f4fa;--muted:#6e8491;--green:#58f0a5;--cyan:#51dbea;--amber:#ffbd59;--red:#ff667c}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% -20%,#123044 0,#050a0f 38%,#020507 80%);color:var(--text);font-family:Inter,-apple-system,sans-serif;overflow-x:hidden}body:before{content:'';position:fixed;inset:0;background-image:linear-gradient(#ffffff05 1px,transparent 1px),linear-gradient(90deg,#ffffff05 1px,transparent 1px);background-size:42px 42px;mask-image:linear-gradient(to bottom,#000,transparent);pointer-events:none}.wrap{max-width:1900px;margin:auto;padding:18px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font:900 22px ui-monospace;letter-spacing:.11em}.live{font:800 10px ui-monospace;color:var(--green);animation:pulse 1.8s infinite}.live:before{content:'● ';}.sub{font:10px ui-monospace;color:var(--muted);letter-spacing:.08em}.panel{background:linear-gradient(180deg,#0b151ed9,#071018d9);border:1px solid var(--line);border-radius:14px;padding:14px;box-shadow:0 18px 60px #0008;backdrop-filter:blur(12px);margin-top:12px}.title{font:900 10px ui-monospace;letter-spacing:.16em;color:#9cb1bd}.hero{display:grid;grid-template-columns:1fr 1.2fr 1fr;gap:12px}.node{position:relative;overflow:hidden;border:1px solid #1c394a;border-radius:13px;padding:15px;background:#07131b;transition:.35s transform,.35s border-color}.node:hover{transform:translateY(-3px);border-color:var(--cyan)}.node.champ{border-color:#2b7756;box-shadow:0 0 40px #3cf29b13}.node:after{content:'';position:absolute;width:160px;height:160px;border:1px solid #ffffff0a;border-radius:50%;right:-70px;top:-70px;animation:spin 12s linear infinite}.name{font:900 13px ui-monospace}.badge{float:right;font:900 9px ui-monospace;padding:4px 7px;border:1px solid #294454;border-radius:20px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:13px}.metric{background:#0a1922;border:1px solid #17303e;border-radius:8px;padding:8px}.ml{font:8px ui-monospace;color:var(--muted)}.mv{font:900 18px ui-monospace;margin-top:3px}.green{color:var(--green)}.red{color:var(--red)}.amber{color:var(--amber)}.cyan{color:var(--cyan)}.flow{display:grid;grid-template-columns:repeat(6,1fr);gap:8px;margin-top:12px}.stage{position:relative;text-align:center;padding:13px 7px;border:1px solid #183444;border-radius:9px;background:#07141c;font:800 9px ui-monospace}.stage:not(:last-child):after{content:'›';position:absolute;right:-8px;top:8px;font-size:22px;color:var(--cyan);z-index:2;animation:arrow 1.4s infinite}.factory{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.cand{padding:10px;border:1px solid #18313f;border-radius:9px;background:#07131b;animation:rise .45s ease both}.rank{font:900 18px ui-monospace;color:#415968}.feed{font:10px ui-monospace;line-height:1.6}.event{border-left:2px solid #1c4355;padding:7px 10px;margin:5px 0;background:#07131a;animation:fade .4s ease}.cols{display:grid;grid-template-columns:1.1fr .9fr;gap:12px}.scan{height:2px;background:linear-gradient(90deg,transparent,var(--cyan),transparent);opacity:.45;animation:scan 4s linear infinite}.empty{color:var(--muted);font:10px ui-monospace;padding:12px}@keyframes pulse{50%{opacity:.35}}@keyframes spin{to{transform:rotate(360deg)}}@keyframes arrow{50%{transform:translateX(3px);opacity:.35}}@keyframes scan{from{transform:translateY(-5px)}to{transform:translateY(120px)}}@keyframes rise{from{opacity:0;transform:translateY(8px)}}@keyframes fade{from{opacity:0}}@media(max-width:1100px){.hero,.cols{grid-template-columns:1fr}.flow{grid-template-columns:repeat(3,1fr)}.factory{grid-template-columns:repeat(2,1fr)}.metrics{grid-template-columns:repeat(2,1fr)}}
</style></head><body><div class="wrap"><div class="top"><div><div class="brand">MEMECOIN LAB // V7.5</div><div class="sub">ANIMATED RESEARCH COCKPIT · READ ONLY · SCIENCE FIRST</div></div><div><div class="live">LIVE RESEARCH BUS</div><div id="clock" class="sub"></div></div></div><div class="panel"><div class="scan"></div><div class="title">CHAMPION / CHALLENGER FIELD</div><div id="hero" class="hero"></div></div><div class="panel"><div class="title">RESEARCH PIPELINE</div><div class="flow"><div class="stage">RAW EVENTS</div><div class="stage">FEATURES</div><div class="stage">DISCOVERY</div><div class="stage">ROBUST GATES</div><div class="stage">FUTURE ONLY</div><div class="stage">DECAY WATCH</div></div></div><div class="cols"><div class="panel"><div class="title">V7.3 ROBUST CHALLENGER FACTORY</div><div id="factory" class="factory"></div></div><div class="panel"><div class="title">PORTFOLIO / DECAY STATUS</div><div id="status" class="feed"></div></div></div><div class="panel"><div class="title">RESEARCH BRAIN // CONCLUSIONS & JOURNAL</div><div id="journal" class="feed"></div></div></div><script>
const esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),f=(x,n=2)=>x==null?'—':Number(x).toFixed(n),pct=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%';
function node(s){let v=String(s.verdict||''),good=v.includes('CONFIRMED')&&!v.includes('FAILED'),bad=v.includes('FAILED');return `<div class="node ${good?'champ':''}"><div class="name">${esc(s.label)}<span class="badge ${bad?'red':good?'green':'amber'}">${esc(v)}</span></div><div class="metrics"><div class="metric"><div class="ml">DONE</div><div class="mv">${s.done||0}</div></div><div class="metric"><div class="ml">EXPECTANCY</div><div class="mv ${Number(s.expectancy)>0?'green':'red'}">${f(s.expectancy)}%</div></div><div class="metric"><div class="ml">PF</div><div class="mv ${Number(s.profit_factor)>1?'green':'red'}">${f(s.profit_factor)}</div></div><div class="metric"><div class="ml">FILL</div><div class="mv cyan">${pct(s.fill_rate)}</div></div></div><div class="sub" style="margin-top:10px">${esc(s.action||'observing')}</div></div>`}
async function go(){try{let d=await(await fetch('/api',{cache:'no-store'})).json();clock.textContent=new Date(d.now*1000).toLocaleTimeString();hero.innerHTML=(d.strategies||[]).map(node).join('')||'<div class="empty">waiting for intelligence</div>';factory.innerHTML=(d.factory||[]).map((x,i)=>`<div class="cand" style="animation-delay:${i*.035}s"><span class="rank">#${String(x.rank).padStart(2,'0')}</span> <b>${esc(x.status)}</b><br><span class="cyan">${esc(x.feature)}</span><br><span class="sub">${esc(x.family)} · score ${f(x.robust_score)} · HO ${x.holdout_n} · PF ${f(x.holdout_pf)}</span></div>`).join('')||'<div class="empty">No V7.3 candidates yet.</div>';let ps=(d.portfolio||[]).map(x=>`<div class="event"><b>${esc(x.allocation)}</b> · ${esc(x.status)} · exp ${f(x.expectancy)}% · PF ${f(x.pf)} · DD ${f(x.max_drawdown)}</div>`).join('');let ds=(d.decay||[]).slice(0,8).map(x=>`<div class="event">${esc(x.label||x.strategy||x.strategy_id||'DECAY')} · <span class="amber">${esc(x.state||x.status||x.verdict||'WATCH')}</span> · DONE ${esc(x.done||'—')}</div>`).join('');status.innerHTML=ps+ds||'<div class="empty">Waiting for portfolio / decay data.</div>';journal.innerHTML=(d.journal||[]).map(x=>`<div class="event"><span class="sub">${new Date(x.created_at*1000).toLocaleTimeString()}</span> · <b>${esc(x.label)}</b> · <span class="cyan">${esc(x.conclusion)}</span><br>${esc(x.action)}</div>`).join('')||'<div class="empty">No journal events.</div>'}catch(e){clock.textContent='BUS ERROR'}setTimeout(go,1800)}go();
</script></body></html>'''
class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path=='/api':b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass
if __name__=='__main__':print(f'MEMECOIN LAB V7.5 animated cockpit http://{HOST}:{PORT}',flush=True);ThreadingHTTPServer((HOST,PORT),H).serve_forever()
