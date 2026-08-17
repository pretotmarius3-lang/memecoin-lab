#!/usr/bin/env python3
"""Memecoin Lab — unified visual command center.

Read-only cockpit for acquisition, prospective science, evolutionary research,
champion registry, diversity gate, autonomous executor and V5.7 arenas.
No trading/signing and no mutation of research state.
"""
from __future__ import annotations
import json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V50_DASH_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V50_DASH_PORT","8788"))

def op(path):
    if not path.exists(): return None
    d=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=10); d.row_factory=sqlite3.Row
    d.execute("PRAGMA busy_timeout=10000"); return d

def has(d,t): return bool(d and d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())
def one(d,q,a=(),default=0):
    try:
        r=d.execute(q,a).fetchone(); return default if not r or r[0] is None else r[0]
    except Exception:return default

def many(d,q,a=()):
    try:return [dict(r) for r in d.execute(q,a).fetchall()]
    except Exception:return []

def collect():
    now=time.time(); o={"now":now,"pipeline":{},"v55":{},"champions":[],"portfolio":[],"families":[],"arenas":[],"challengers":[],"learned":[],"executor":{},"diversity":{},"side":{},"arena_stats":{}}
    v=op(V5)
    if v:
        if has(v,"v51_signature_spool"):
            p=o["pipeline"]; p["spool_total"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool"); p["pending"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='PENDING'"); p["done"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='DONE'"); p["failed"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status IN ('FAILED','DEAD')")
            z=one(v,"SELECT MAX(first_seen) FROM v51_signature_spool",default=None); p["ingest_age"]=None if z is None else max(0,now-float(z))
        if has(v,"v5_raw_transactions"):
            o["pipeline"]["raw"]=one(v,"SELECT COUNT(*) FROM v5_raw_transactions"); z=one(v,"SELECT MAX(observed_at) FROM v5_raw_transactions",default=None); o["pipeline"]["raw_age"]=None if z is None else max(0,now-float(z))
        v.close()
    f=op(V52)
    if f:
        if has(f,"v52_swaps"):
            o["pipeline"]["swaps"]=one(f,"SELECT COUNT(*) FROM v52_swaps"); o["pipeline"]["tokens"]=one(f,"SELECT COUNT(DISTINCT token_mint) FROM v52_swaps"); z=one(f,"SELECT MAX(timestamp) FROM v52_swaps",default=None); o["pipeline"]["decode_age"]=None if z is None else max(0,now-float(z))
        if has(f,"v52_outcomes"):o["pipeline"]["outcomes"]=one(f,"SELECT COUNT(*) FROM v52_outcomes WHERE ready=1")
        f.close()
    r=op(RDB)
    if r:
        if has(r,"v55_beliefs"):
            o["v55"]={x["state"]:x["n"] for x in many(r,"SELECT state,COUNT(*) n FROM v55_beliefs GROUP BY state")}
            o["v55_top"]=many(r,"SELECT b.candidate_id,b.state,b.n,b.prospective_rho,b.lift,b.confidence,c.kind FROM v55_beliefs b JOIN v55_candidates c USING(candidate_id) ORDER BY CASE b.state WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 ELSE 2 END,b.confidence DESC LIMIT 18")
        if has(r,"v562_champion_registry"):
            o["champions"]=many(r,"SELECT candidate_id,family,kind,generation,champion_state,n,prospective_rho,lift,precision,confidence FROM v562_champion_registry ORDER BY CASE champion_state WHEN 'STABLE' THEN 0 WHEN 'CHAMPION' THEN 1 WHEN 'DECAYING' THEN 2 ELSE 3 END,confidence DESC LIMIT 20")
        if has(r,"v562_research_portfolio"):o["portfolio"]=many(r,"SELECT book,target_weight,adaptive_weight,evidence_score,active_subjects FROM v562_research_portfolio ORDER BY adaptive_weight DESC")
        if has(r,"v56_family_evolution"):o["families"]=many(r,"SELECT family,frozen,waiting,watching,passed,failed,verdicts,pass_rate,mean_confidence,mean_rho,mean_lift,allocation,action FROM v56_family_evolution ORDER BY allocation DESC")
        if has(r,"v561_state"):
            s=one(r,"SELECT value_json FROM v561_state WHERE key='latest'",default='{}')
            try:o["diversity"]=json.loads(s)
            except Exception:pass
        if has(r,"v563_state"):
            s=one(r,"SELECT value_json FROM v563_state WHERE key='latest'",default='{}')
            try:o["executor"]=json.loads(s)
            except Exception:pass
        if has(r,"v49_side_results"):o["side"]={x["comparison"]:x["n"] for x in many(r,"SELECT comparison,COUNT(*) n FROM v49_side_results GROUP BY comparison")}
        if has(r,"v57_arenas"):
            o["arenas"]=many(r,"SELECT * FROM v57_arenas ORDER BY control_state DESC,control_rho DESC LIMIT 20")
        if has(r,"v57_challengers"):
            o["challengers"]=many(r,"SELECT c.*,a.control_id,a.control_rho,a.control_lift,a.control_state FROM v57_challengers c JOIN v57_arenas a USING(arena_id) ORDER BY CASE c.comparison WHEN 'CHALLENGER_WINS' THEN 0 WHEN 'CONTROL_DEFENDS' THEN 1 ELSE 2 END,COALESCE(c.delta_rho,-999) DESC LIMIT 30")
            o["arena_stats"]={x["status"]:x["n"] for x in many(r,"SELECT status,COUNT(*) n FROM v57_challengers GROUP BY status")}
        if has(r,"v57_conclusions"):o["learned"]=many(r,"SELECT verdict,observation,hypothesis,next_test,updated_at FROM v57_conclusions ORDER BY updated_at DESC LIMIT 12")
        r.close()
    p=o["pipeline"]; age=max([x for x in (p.get('ingest_age'),p.get('raw_age'),p.get('decode_age')) if x is not None] or [9999]); score=100
    if age>300:score-=35
    elif age>90:score-=15
    if int(p.get('pending') or 0)>50000:score-=25
    if int(p.get('failed') or 0):score-=10
    o["health"]=max(0,score)
    return o

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab // Command Center</title><style>
:root{--bg:#04070a;--p:#091016;--line:#17242e;--txt:#e8f1f5;--mut:#667b88;--c:#36d7e8;--g:#4be38b;--a:#f3b64c;--r:#ff627d;--b:#6c8cff;--v:#a97fff}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1900px;margin:auto;padding:16px}.top{display:flex;justify-content:space-between;align-items:center}.brand{font-weight:900;letter-spacing:.1em}.sub,.tag{color:var(--mut);font-size:10px}.live{font:11px ui-monospace;border:1px solid var(--line);padding:7px 9px;border-radius:7px}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--g);margin-right:7px}.kpis{display:grid;grid-template-columns:repeat(10,1fr);gap:7px;margin-top:10px}.kpi,.panel{background:var(--p);border:1px solid var(--line);border-radius:9px}.kpi{padding:9px}.lab{font-size:8px;color:var(--mut);letter-spacing:.12em}.val{font:800 19px ui-monospace;margin-top:4px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:7px}.grid3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:7px}.panel{padding:10px;margin-top:7px;min-height:245px}.head{display:flex;justify-content:space-between}.title{font-size:10px;font-weight:800;letter-spacing:.1em}.good{color:var(--g)}.warn{color:var(--a)}.bad{color:var(--r)}canvas{width:100%;height:220px;display:block}.rows{font:10px ui-monospace}.row{display:grid;grid-template-columns:1.1fr .75fr .55fr .55fr .55fr 1.2fr;gap:6px;padding:6px 3px;border-bottom:1px solid #111b22}.row.h{font-size:8px;color:var(--mut)}.learn{border-left:2px solid var(--c);padding:7px;background:#080e13;margin:5px 0;font-size:10px}.learn span{display:block;color:var(--mut);margin-top:3px}.footer{color:#4e616c;font:9px ui-monospace;text-align:right;margin-top:9px}@media(max-width:1200px){.kpis{grid-template-columns:repeat(5,1fr)}.grid3,.grid2{grid-template-columns:1fr}}</style></head><body><div class="wrap">
<div class="top"><div><div class="brand">MEMECOIN LAB // MASTER COMMAND</div><div class="sub">data → science → prospective → evolution → champions → arena</div></div><div class="live"><span class="dot"></span><span id="clock">CONNECTING</span></div></div>
<div class="kpis" id="kpis"></div>
<div class="grid3"><div class="panel"><div class="head"><div class="title">01 // PROSPECTIVE FUNNEL</div><div class="tag">V5.5</div></div><canvas id="funnel"></canvas></div><div class="panel"><div class="head"><div class="title">02 // RESEARCH PORTFOLIO</div><div class="tag">exploit / novelty / falsify</div></div><canvas id="portfolio"></canvas></div><div class="panel"><div class="head"><div class="title">03 // SCIENCE DIVERSITY</div><div class="tag">duplicates avoided</div></div><canvas id="diversity"></canvas></div></div>
<div class="grid2"><div class="panel"><div class="head"><div class="title">04 // CHAMPION REGISTRY</div><div class="tag">prospective PASS only</div></div><div class="rows" id="champions"></div></div><div class="panel"><div class="head"><div class="title">05 // FAMILY EVOLUTION</div><div class="tag">allocation + verdicts</div></div><canvas id="families"></canvas></div></div>
<div class="grid2"><div class="panel"><div class="head"><div class="title">06 // CANDIDATE / CHAMPION ARENA</div><div class="tag">control vs challengers</div></div><div class="rows" id="arena"></div></div><div class="panel"><div class="head"><div class="title">07 // WHAT THE LAB LEARNED</div><div class="tag">observation → hypothesis → next test</div></div><div id="learned"></div></div></div>
<div class="grid3"><div class="panel"><div class="head"><div class="title">08 // SIDE LAB OUTCOME</div><div class="tag">recursive research</div></div><canvas id="side"></canvas></div><div class="panel"><div class="head"><div class="title">09 // EXECUTOR</div><div class="tag">V5.6.3</div></div><canvas id="executor"></canvas></div><div class="panel"><div class="head"><div class="title">10 // LIVE DATA</div><div class="tag">pipeline health</div></div><canvas id="pipe"></canvas></div></div>
<div class="footer">read-only · no trading · no frozen-rule mutation · refresh 2s</div></div><script>
const C={c:'#36d7e8',g:'#4be38b',a:'#f3b64c',r:'#ff627d',b:'#6c8cff',v:'#a97fff',grid:'#17242e',mut:'#667b88',txt:'#e8f1f5'};let last=null;
function cv(id){let c=document.getElementById(id),d=devicePixelRatio||1,r=c.getBoundingClientRect();c.width=r.width*d;c.height=r.height*d;let x=c.getContext('2d');x.setTransform(d,0,0,d,0,0);x.clearRect(0,0,r.width,r.height);return[x,r.width,r.height]}
function t(x,s,a,b,col=C.mut,size=9,align='left'){x.fillStyle=col;x.font=`${size}px ui-monospace`;x.textAlign=align;x.fillText(String(s),a,b)}function n(v,d=0){v=Number(v||0);return Number.isFinite(v)?v.toLocaleString(undefined,{maximumFractionDigits:d}):'—'}
function bars(id,arr,label,val,cols){let[x,w,h]=cv(id);if(!arr.length){t(x,'NO DATA',w/2,h/2,C.mut,10,'center');return}let mx=Math.max(1,...arr.map(z=>+z[val]||0)),left=Math.min(170,w*.4),row=(h-18)/arr.length;arr.forEach((z,i)=>{let y=7+i*row,bw=(w-left-25)*(+z[val]||0)/mx,col=cols[i%cols.length];t(x,String(z[label]||'').slice(0,22),left-8,y+10,C.mut,8,'right');x.fillStyle='#111b22';x.fillRect(left,y,w-left-25,8);x.fillStyle=col;x.fillRect(left,y,bw,8);t(x,n(z[val],2),w-5,y+9,col,8,'right')})}
function funnel(d){let[x,w,h]=cv('funnel'),b=d.v55||{},a=[['WAIT',b.WAITING||0,C.b],['WATCH',b.WATCH||0,C.a],['PASS',b.PASS||0,C.g],['FAIL',b.FAIL||0,C.r]],mx=Math.max(1,...a.map(z=>z[1]));a.forEach((z,i)=>{let ww=(w-60)*z[1]/mx,y=18+i*46,xx=(w-ww)/2;x.strokeStyle=z[2];x.strokeRect(xx,y,ww,31);t(x,z[0]+'  '+n(z[1]),w/2,y+20,z[2],11,'center')})}
function diversity(d){let[x,w,h]=cv('diversity'),v=d.diversity||{},ratio=+v.diversity_ratio||0,saved=+v.estimated_rows_avoided_total||0,ids=+v.scientific_identities||0,total=+v.total_experiments||0;x.lineWidth=22;x.strokeStyle='#13202a';x.beginPath();x.arc(w/2,h/2-8,70,0,Math.PI*2);x.stroke();x.strokeStyle=C.c;x.beginPath();x.arc(w/2,h/2-8,70,-Math.PI/2,-Math.PI/2+Math.PI*2*ratio);x.stroke();t(x,(ratio*100).toFixed(1)+'%',w/2,h/2-2,C.c,20,'center');t(x,'DIVERSITY',w/2,h/2+16,C.mut,8,'center');t(x,ids+' / '+total,w/2,h-26,C.txt,9,'center');t(x,'ROWS AVOIDED '+n(saved),w/2,h-11,C.g,9,'center')}
function side(d){let s=d.side||{},arr=[{k:'IMPROVED',v:+s.IMPROVED||0},{k:'SAME',v:+s.SAME||0},{k:'WORSE',v:+s.WORSE||0}];bars('side',arr,'k','v',[C.g,C.a,C.r])}
function executor(d){let e=d.executor||{},arr=[{k:'SEEDED',v:+e.seeded||0},{k:'ALLOWED',v:+e.allowed||0},{k:'BLOCKED',v:+e.blocked||0},{k:'REFRESH',v:+e.refresh||0}];bars('executor',arr,'k','v',[C.c,C.g,C.r,C.a])}
function pipe(d){let p=d.pipeline||{},arr=[{k:'RAW',v:+p.raw||0},{k:'SWAPS',v:+p.swaps||0},{k:'TOKENS',v:+p.tokens||0},{k:'PENDING',v:+p.pending||0},{k:'OUTCOMES',v:+p.outcomes||0}];bars('pipe',arr,'k','v',[C.g,C.c,C.v,C.a,C.b])}
function tables(d){let cs=d.champions||[];document.getElementById('champions').innerHTML='<div class="row h"><div>STATE / FAMILY</div><div>KIND</div><div>N</div><div>RHO</div><div>LIFT</div><div>ID</div></div>'+(cs.length?cs.map(c=>`<div class="row"><div class="${c.champion_state==='DECAYING'?'bad':'good'}"><b>${c.champion_state}</b><br>${c.family}</div><div>${c.kind}</div><div>${n(c.n)}</div><div>${c.prospective_rho==null?'—':(+c.prospective_rho).toFixed(3)}</div><div>${c.lift==null?'—':(+c.lift).toFixed(2)}</div><div>${c.candidate_id.slice(0,14)}</div></div>`).join(''):'<div class="learn">No prospective PASS yet — registry armed.</div>');let ar=d.challengers||[];document.getElementById('arena').innerHTML='<div class="row h"><div>MUTATION</div><div>STATUS</div><div>N</div><div>RHO</div><div>ΔRHO</div><div>RESULT</div></div>'+(ar.length?ar.slice(0,16).map(a=>`<div class="row"><div><b>${a.mutation}</b><br>${a.control_id.slice(0,12)}</div><div>${a.status}</div><div>${n(a.n)}</div><div>${a.rho==null?'—':(+a.rho).toFixed(3)}</div><div class="${(+a.delta_rho||0)>0?'good':'bad'}">${a.delta_rho==null?'—':(+a.delta_rho).toFixed(3)}</div><div>${a.comparison||'PENDING'}</div></div>`).join(''):'<div class="learn">Arena armed — waiting for completed SIDE challengers.</div>');let l=d.learned||[];document.getElementById('learned').innerHTML=l.length?l.map(z=>`<div class="learn"><b>${z.verdict}</b> · ${z.observation}<span>HYPOTHESIS: ${z.hypothesis}</span><span>NEXT: ${z.next_test}</span></div>`).join(''):'<div class="learn">Waiting for completed head-to-heads.</div>'}
function render(d){last=d;let p=d.pipeline||{},v=d.v55||{},ds=d.diversity||{},a=d.arena_stats||{},ch=(d.champions||[]).length,health=+d.health||0;document.getElementById('kpis').innerHTML=[['SYSTEM',health+'/100',health>75?'good':health>50?'warn':'bad'],['RAW',n(p.raw),''],['TOKENS',n(p.tokens),''],['BACKLOG',n(p.pending),+p.pending>50000?'warn':''],['WAIT',n(v.WAITING),''],['WATCH',n(v.WATCH),'warn'],['PASS',n(v.PASS),'good'],['CHAMPIONS',n(ch),'good'],['ARENAS',n((d.arenas||[]).length),''],['CHALLENGERS',n((d.challengers||[]).length),'']].map(z=>`<div class="kpi"><div class="lab">${z[0]}</div><div class="val ${z[2]}">${z[1]}</div></div>`).join('');funnel(d);bars('portfolio',d.portfolio||[],'book','adaptive_weight',[C.g,C.c,C.a]);diversity(d);bars('families',d.families||[],'family','allocation',[C.c,C.g,C.b,C.v,C.a]);side(d);executor(d);pipe(d);tables(d);document.getElementById('clock').textContent=new Date().toLocaleTimeString()+' · LIVE'}
async function tick(){try{let r=await fetch('/api/state',{cache:'no-store'});render(await r.json())}catch(e){document.getElementById('clock').textContent='DISCONNECTED'}}setInterval(tick,2000);tick();window.addEventListener('resize',()=>last&&render(last));
</script></body></html>'''

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/api/state'):
            b=json.dumps(collect(),default=str,separators=(',',':')).encode(); self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b); return
        if self.path not in ('/','/index.html'): self.send_response(404); self.end_headers(); return
        b=HTML.encode(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*_):pass

if __name__=='__main__':
    print('='*96); print('MEMECOIN LAB — MASTER VISUAL COMMAND CENTER'); print('='*96); print(f'Dashboard: http://{HOST}:{PORT}'); print('V5.5/V5.6/V5.7 integrated · read-only · refresh 2s'); ThreadingHTTPServer((HOST,PORT),H).serve_forever()
