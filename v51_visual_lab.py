#!/usr/bin/env python3
"""Memecoin Lab V5.1 — visual-first research lab.
Read-only. No signing/trading/state mutation.
Dense visual command center built with stdlib + browser Canvas only.
"""
from __future__ import annotations
import json, math, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V51_DASH_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V51_DASH_PORT","8791"))

def dbopen(p):
    if not p.exists(): return None
    d=sqlite3.connect(f"file:{p}?mode=ro",uri=True,timeout=10); d.row_factory=sqlite3.Row; d.execute("PRAGMA busy_timeout=10000"); return d

def has(d,t): return bool(d and d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())
def rows(d,sql,args=()):
    try:return [dict(r) for r in d.execute(sql,args).fetchall()]
    except Exception:return []
def one(d,sql,args=(),default=0):
    try:
        r=d.execute(sql,args).fetchone(); return default if not r or r[0] is None else r[0]
    except Exception:return default

def collect():
    now=time.time(); o={"now":now,"pipeline":{},"jobs":{},"beliefs":{},"roles":{},"signals":[],"families":[],"side":{},"side_kind":[],"side_top":[],"ensembles":[],"branches":[],"agenda":[],"workers_live":0}
    v=dbopen(V5)
    if v:
        if has(v,"v51_signature_spool"):
            for st in ("PENDING","FETCHING","DONE","FAILED"):
                o["pipeline"]["spool_"+st.lower()]=one(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status=?",(st,))
            o["pipeline"]["spool_total"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool")
            t=one(v,"SELECT MAX(first_seen) FROM v51_signature_spool",default=None); o["pipeline"]["ingest_age"]=None if t is None else max(0,now-float(t))
        if has(v,"v5_raw_transactions"):
            o["pipeline"]["raw_tx"]=one(v,"SELECT COUNT(*) FROM v5_raw_transactions")
            t=one(v,"SELECT MAX(observed_at) FROM v5_raw_transactions",default=None); o["pipeline"]["raw_age"]=None if t is None else max(0,now-float(t))
        v.close()
    f=dbopen(V52)
    if f:
        if has(f,"v52_swaps"):
            o["pipeline"]["swaps"]=one(f,"SELECT COUNT(*) FROM v52_swaps")
            o["pipeline"]["tokens"]=one(f,"SELECT COUNT(DISTINCT token_mint) FROM v52_swaps")
            t=one(f,"SELECT MAX(timestamp) FROM v52_swaps",default=None); o["pipeline"]["decode_age"]=None if t is None else max(0,now-float(t))
        if has(f,"v52_snapshots"): o["pipeline"]["snapshots"]=one(f,"SELECT COUNT(*) FROM v52_snapshots")
        if has(f,"v52_outcomes"): o["pipeline"]["outcomes"]=one(f,"SELECT COUNT(*) FROM v52_outcomes WHERE ready=1")
        f.close()
    r=dbopen(RDB)
    if r:
        if has(r,"v41_jobs"): o["jobs"]={x["status"]:x["n"] for x in rows(r,"SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
        if has(r,"v41_workers"): o["workers_live"]=one(r,"SELECT COUNT(*) FROM v41_workers WHERE last_heartbeat>?",(now-15,))
        if has(r,"v46_beliefs"): o["beliefs"]={x["status"]:x["n"] for x in rows(r,"SELECT status,COUNT(*) n FROM v46_beliefs GROUP BY status")}
        if has(r,"v48_signal_rankings"):
            o["roles"]={x["role"]:x["n"] for x in rows(r,"SELECT role,COUNT(*) n FROM v48_signal_rankings GROUP BY role")}
            o["signals"]=rows(r,"SELECT * FROM v48_signal_rankings ORDER BY live_score DESC LIMIT 120")
        if has(r,"v48_family_budget"): o["families"]=rows(r,"SELECT * FROM v48_family_budget ORDER BY budget_weight DESC")
        if has(r,"v48_ensemble_rankings"): o["ensembles"]=rows(r,"SELECT * FROM v48_ensemble_rankings ORDER BY live_score DESC LIMIT 40")
        if has(r,"v48_research_agenda"): o["agenda"]=rows(r,"SELECT priority,family,agenda_type,subject,rationale FROM v48_research_agenda WHERE state='OPEN' ORDER BY priority,updated_at DESC LIMIT 12")
        if has(r,"v49_side_results"):
            o["side"]={x["comparison"]:x["n"] for x in rows(r,"SELECT comparison,COUNT(*) n FROM v49_side_results GROUP BY comparison")}
            o["side_kind"]=rows(r,"SELECT kind,COUNT(*) n,SUM(CASE WHEN comparison='IMPROVED' THEN 1 ELSE 0 END) improved,AVG(delta_rho) avg_delta FROM v49_side_results GROUP BY kind ORDER BY improved DESC,n DESC")
            o["side_top"]=rows(r,"SELECT r.kind,r.comparison,r.holdout_rho,r.delta_rho,r.qdiff_pp,e.parent_feature FROM v49_side_results r LEFT JOIN v49_side_experiments e USING(experiment_id) ORDER BY COALESCE(r.delta_rho,-999) DESC LIMIT 20")
        if has(r,"v41_hypotheses") and has(r,"v41_jobs"):
            o["branches"]=rows(r,"SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) hypotheses,SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id GROUP BY h.branch ORDER BY hypotheses DESC")
        r.close()
    p=o["pipeline"]; backlog=int(p.get("spool_pending") or 0); age=float(p.get("decode_age") or 99999); failed=int(o.get("jobs",{}).get("FAILED",0)); score=100
    if age>1800: score-=45
    elif age>300: score-=30
    elif age>90: score-=12
    if backlog>300000: score-=30
    elif backlog>100000: score-=20
    elif backlog>20000: score-=10
    if failed: score-=10
    if o["workers_live"]<=0: score-=20
    o["health_score"]=max(0,score)
    return o

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab // Visual Lab</title>
<style>
:root{--bg:#05080c;--p:#091019;--p2:#0d151f;--line:#1a2733;--txt:#e8eef4;--muted:#748391;--cyan:#55d8e8;--green:#62e69a;--amber:#f1b24d;--red:#ff6d82;--blue:#7992ff;--violet:#a984ff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 72% -20%,#10202d 0,#070b10 34%,#04070b 70%);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1920px;margin:auto;padding:14px}.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px}.brand{font-size:18px;font-weight:850;letter-spacing:.08em}.sub{font-size:10px;color:var(--muted);margin-top:2px}.live{font:700 10px ui-monospace;color:var(--green)}.kpis{display:grid;grid-template-columns:repeat(8,1fr);gap:7px}.kpi,.panel{background:linear-gradient(180deg,#0a1119,#080d13);border:1px solid var(--line);border-radius:9px}.kpi{padding:9px 10px}.lab{font-size:8px;color:var(--muted);letter-spacing:.13em;text-transform:uppercase}.val{font:800 21px ui-monospace;margin-top:4px}.hint{font:9px ui-monospace;color:var(--muted);margin-top:2px}.g2{display:grid;grid-template-columns:1fr 1fr;gap:7px}.g3{display:grid;grid-template-columns:1.15fr 1fr .9fr;gap:7px}.g4{display:grid;grid-template-columns:repeat(4,1fr);gap:7px}.panel{padding:9px;margin-top:7px;min-height:245px}.head{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:4px}.title{font-size:9px;font-weight:850;letter-spacing:.13em;text-transform:uppercase}.tag{font:8px ui-monospace;color:var(--muted)}canvas{width:100%;height:220px;display:block}.tall canvas{height:285px}.short canvas{height:175px}.agenda{display:grid;gap:5px}.ag{padding:6px 7px;background:#0b121a;border-left:2px solid var(--cyan);font-size:9px}.ag b{font:700 9px ui-monospace}.ag span{display:block;color:var(--muted);margin-top:2px}.footer{font:8px ui-monospace;color:#53606a;text-align:right;margin-top:8px}@media(max-width:1250px){.kpis{grid-template-columns:repeat(4,1fr)}.g3,.g4,.g2{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<div class="top"><div><div class="brand">MEMECOIN LAB // VISUAL RESEARCH LAB</div><div class="sub">forward evidence · live pipeline · signal geometry · recursive research · read-only</div></div><div class="live" id="clock">CONNECTING</div></div>
<div class="kpis" id="kpis"></div>
<div class="g3"><div class="panel tall"><div class="head"><div class="title">01 / SIGNAL GALAXY</div><div class="tag">rho × lift · bubble=N · color=role</div></div><canvas id="galaxy"></canvas></div><div class="panel tall"><div class="head"><div class="title">02 / PIPELINE FLOW</div><div class="tag">ingest → raw → decode → outcome → science</div></div><canvas id="pipe"></canvas></div><div class="panel tall"><div class="head"><div class="title">03 / PROSPECTIVE FUNNEL</div><div class="tag">WAIT → WATCH → PASS / FAIL</div></div><canvas id="funnel"></canvas></div></div>
<div class="g2"><div class="panel"><div class="head"><div class="title">04 / LIVE TELEMETRY</div><div class="tag">browser memory · absolute counts</div></div><canvas id="telemetry"></canvas></div><div class="panel"><div class="head"><div class="title">05 / THROUGHPUT VELOCITY</div><div class="tag">Δ every refresh · ingest/raw/swaps/backlog</div></div><canvas id="velocity"></canvas></div></div>
<div class="g3"><div class="panel"><div class="head"><div class="title">06 / CHAMPION LADDER</div><div class="tag">live score · forward-only</div></div><canvas id="ladder"></canvas></div><div class="panel"><div class="head"><div class="title">07 / STABILITY MATRIX</div><div class="tag">current / early / late / redundancy</div></div><canvas id="heat"></canvas></div><div class="panel"><div class="head"><div class="title">08 / FAMILY RADAR</div><div class="tag">budget · pass · stability · rho</div></div><canvas id="radar"></canvas></div></div>
<div class="g3"><div class="panel"><div class="head"><div class="title">09 / RECURSIVE LAB</div><div class="tag">IMPROVED / SAME / WORSE</div></div><canvas id="recursive"></canvas></div><div class="panel"><div class="head"><div class="title">10 / SIDE-RESEARCH YIELD</div><div class="tag">improvement rate by experiment kind</div></div><canvas id="yield"></canvas></div><div class="panel"><div class="head"><div class="title">11 / ENSEMBLE FRONTIER</div><div class="tag">rho × lift · score glow</div></div><canvas id="ens"></canvas></div></div>
<div class="g2"><div class="panel short"><div class="head"><div class="title">12 / RESEARCH BRANCH LOAD</div><div class="tag">done / queued / running</div></div><canvas id="branches"></canvas></div><div class="panel"><div class="head"><div class="title">13 / AUTONOMOUS AGENDA</div><div class="tag">machine-selected priorities</div></div><div id="agenda" class="agenda"></div></div></div>
<div class="footer">refresh 2s · read-only · no trading · /api/state</div></div>
<script>
const C={grid:'#18222b',txt:'#82919d',cyan:'#55d8e8',green:'#62e69a',amber:'#f1b24d',red:'#ff6d82',blue:'#7992ff',violet:'#a984ff',white:'#e8eef4'}; const hist=[]; const vel=[]; let prev=null;
function ctx(id){const cv=document.getElementById(id),d=window.devicePixelRatio||1,r=cv.getBoundingClientRect();cv.width=r.width*d;cv.height=r.height*d;const x=cv.getContext('2d');x.setTransform(d,0,0,d,0,0);return [x,r.width,r.height]}
function clr(x){return x==='CHAMPION'?C.green:x==='CONTENDER'?C.cyan:x==='RETIRE'?C.red:x==='PASS_WEAK'?C.amber:C.blue}
function grid(x,w,h){x.strokeStyle=C.grid;x.lineWidth=1;for(let i=1;i<5;i++){let y=18+i*(h-36)/5;x.beginPath();x.moveTo(44,y);x.lineTo(w-12,y);x.stroke()}}
function fmt(n){n=Number(n||0);return n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(Math.round(n))}
function kpis(d){const p=d.pipeline||{},b=d.beliefs||{},r=d.roles||{},j=d.jobs||{};const vals=[['SYSTEM',d.health_score+'/100'],['BACKLOG',fmt(p.spool_pending)],['RAW TX',fmt(p.raw_tx)],['TOKENS',fmt(p.tokens)],['SWAPS',fmt(p.swaps)],['OUTCOMES',fmt(p.outcomes)],['PASS',b.PASS||0],['CHAMPION',r.CHAMPION||0],['WATCH',b.WATCH||0],['WORKERS',d.workers_live||0],['DONE',fmt(j.DONE)],['DECODE AGE',p.decode_age==null?'—':Math.round(p.decode_age)+'s'],['SIDE +',d.side?.IMPROVED||0],['ENSEMBLES',(d.ensembles||[]).length],['QUEUE',j.QUEUED||0],['FAILED',j.FAILED||0]];document.getElementById('kpis').innerHTML=vals.map(([a,b])=>`<div class=kpi><div class=lab>${a}</div><div class=val>${b}</div></div>`).join('')}
function galaxy(d){const [x,w,h]=ctx('galaxy'),a=(d.signals||[]).filter(s=>s.rho!=null&&s.lift!=null);x.clearRect(0,0,w,h);grid(x,w,h);if(!a.length)return;let xs=a.map(s=>+s.rho),ys=a.map(s=>Math.min(8,+s.lift));let xmin=Math.min(-.05,...xs),xmax=Math.max(.35,...xs),ymax=Math.max(2,...ys);a.forEach(s=>{let px=44+(+s.rho-xmin)/(xmax-xmin)*(w-60),py=h-24-Math.min(8,+s.lift)/ymax*(h-48),r=4+Math.sqrt(+s.n||0)/10;x.globalAlpha=.78;x.fillStyle=clr(s.role);x.beginPath();x.arc(px,py,Math.min(15,r),0,Math.PI*2);x.fill();});x.globalAlpha=1;x.fillStyle=C.txt;x.font='9px ui-monospace';x.fillText('rho →',w-45,h-8);x.fillText('lift ↑',6,14)}
function pipe(d){const [x,w,h]=ctx('pipe'),p=d.pipeline||{};x.clearRect(0,0,w,h);const nodes=[['INGEST',p.spool_total||0,C.blue],['RAW',p.raw_tx||0,C.green],['SWAPS',p.swaps||0,C.cyan],['OUTCOMES',p.outcomes||0,C.amber],['PASS',d.beliefs?.PASS||0,C.green]],mx=Math.max(1,...nodes.map(n=>+n[1]));nodes.forEach((n,i)=>{let yy=22+i*(h-44)/(nodes.length-1),ww=70+((+n[1]/mx)*(w-125));x.fillStyle='#101923';x.fillRect(25,yy-10,w-50,20);x.fillStyle=n[2];x.globalAlpha=.8;x.fillRect(25,yy-10,ww,20);x.globalAlpha=1;x.fillStyle=C.white;x.font='700 10px ui-monospace';x.fillText(n[0],32,yy+4);x.textAlign='right';x.fillText(fmt(n[1]),w-30,yy+4);x.textAlign='left';if(i<nodes.length-1){x.strokeStyle=C.grid;x.beginPath();x.moveTo(w/2,yy+11);x.lineTo(w/2,yy+((h-44)/(nodes.length-1))-11);x.stroke()}});let pend=p.spool_pending||0,tot=p.spool_total||1;x.fillStyle=C.amber;x.font='9px ui-monospace';x.fillText('backlog '+(100*pend/tot).toFixed(1)+'% of ingest',25,h-5)}
function funnel(d){const [x,w,h]=ctx('funnel'),b=d.beliefs||{},a=[['WAIT',b.WAITING||0,C.blue],['WATCH',b.WATCH||0,C.amber],['PASS',b.PASS||0,C.green],['FAIL',b.FAIL||0,C.red]],mx=Math.max(1,...a.map(z=>z[1]));x.clearRect(0,0,w,h);a.forEach((z,i)=>{let bw=50+(z[1]/mx)*(w-90),yy=28+i*(h-52)/4;x.fillStyle=z[2]+'33';x.strokeStyle=z[2];x.fillRect((w-bw)/2,yy,bw,35);x.strokeRect((w-bw)/2,yy,bw,35);x.fillStyle=C.white;x.textAlign='center';x.font='700 11px ui-monospace';x.fillText(z[0]+'  '+z[1],w/2,yy+21)});x.textAlign='left'}
function lines(id,data,keys,colors){const [x,w,h]=ctx(id);x.clearRect(0,0,w,h);grid(x,w,h);if(data.length<2)return;let vals=[];data.forEach(r=>keys.forEach(k=>vals.push(+r[k]||0)));let mn=Math.min(...vals),mx=Math.max(...vals);if(mx===mn)mx=mn+1;keys.forEach((k,ki)=>{x.strokeStyle=colors[ki];x.lineWidth=1.8;x.beginPath();data.forEach((r,i)=>{let px=44+i/(data.length-1)*(w-58),py=h-20-((+r[k]||0)-mn)/(mx-mn)*(h-38);i?x.lineTo(px,py):x.moveTo(px,py)});x.stroke()});}
function ladder(d){const [x,w,h]=ctx('ladder'),a=(d.signals||[]).slice(0,12);x.clearRect(0,0,w,h);a.forEach((s,i)=>{let y=12+i*(h-20)/12,bw=(+s.live_score||0)*(w-190);x.fillStyle='#101923';x.fillRect(160,y,w-175,8);x.fillStyle=clr(s.role);x.fillRect(160,y,bw,8);x.fillStyle=C.txt;x.font='9px ui-monospace';x.textAlign='right';x.fillText(s.feature,152,y+7);x.textAlign='left';x.fillText((+s.live_score||0).toFixed(2),w-42,y+7)});x.textAlign='left'}
function heat(d){const [x,w,h]=ctx('heat'),a=(d.signals||[]).slice(0,14),cols=[['rho',.35],['early_rho',.35],['late_rho',.35],['stability_score',1],['redundancy_penalty',1]];x.clearRect(0,0,w,h);let cw=(w-120)/cols.length,ch=(h-22)/Math.max(1,a.length);cols.forEach((c,j)=>{x.fillStyle=C.txt;x.font='8px ui-monospace';x.fillText(c[0].replace('_rho',''),120+j*cw,9)});a.forEach((s,i)=>{x.fillStyle=C.txt;x.font='8px ui-monospace';x.textAlign='right';x.fillText(s.feature,112,19+i*ch);x.textAlign='left';cols.forEach((c,j)=>{let v=Math.max(0,Math.min(1,(+s[c[0]]||0)/c[1])),rr=Math.round(255*(1-v)),gg=Math.round(90+150*v);x.fillStyle=`rgb(${rr},${gg},130)`;x.globalAlpha=.2+.75*v;x.fillRect(120+j*cw,11+i*ch,cw-2,Math.max(4,ch-2));x.globalAlpha=1})})}
function radar(d){const [x,w,h]=ctx('radar'),a=(d.families||[]).slice(0,6);x.clearRect(0,0,w,h);if(!a.length)return;let cx=w/2,cy=h/2,R=Math.min(w,h)*.34;for(let ring=1;ring<=4;ring++){x.strokeStyle=C.grid;x.beginPath();a.forEach((_,i)=>{let ang=-Math.PI/2+i*2*Math.PI/a.length,rr=R*ring/4,px=cx+Math.cos(ang)*rr,py=cy+Math.sin(ang)*rr;i?x.lineTo(px,py):x.moveTo(px,py)});x.closePath();x.stroke()}x.beginPath();a.forEach((f,i)=>{let score=Math.min(1,(+f.budget_weight||0)*2.2),ang=-Math.PI/2+i*2*Math.PI/a.length,px=cx+Math.cos(ang)*R*score,py=cy+Math.sin(ang)*R*score;i?x.lineTo(px,py):x.moveTo(px,py)});x.closePath();x.fillStyle=C.cyan+'22';x.strokeStyle=C.cyan;x.fill();x.stroke();a.forEach((f,i)=>{let ang=-Math.PI/2+i*2*Math.PI/a.length,px=cx+Math.cos(ang)*(R+18),py=cy+Math.sin(ang)*(R+18);x.fillStyle=C.txt;x.font='8px ui-monospace';x.textAlign=px<cx?'right':'left';x.fillText((f.family||'').replace('_','/'),px,py)});x.textAlign='left'}
function donut(d){const [x,w,h]=ctx('recursive'),s=d.side||{},a=[['IMPROVED',s.IMPROVED||0,C.green],['SAME',s.SAME||0,C.amber],['WORSE',s.WORSE||0,C.red]],tot=Math.max(1,a.reduce((q,z)=>q+z[1],0)),cx=w*.42,cy=h*.5,R=Math.min(w,h)*.28;x.clearRect(0,0,w,h);let st=-Math.PI/2;a.forEach(z=>{let en=st+z[1]/tot*Math.PI*2;x.beginPath();x.strokeStyle=z[2];x.lineWidth=24;x.arc(cx,cy,R,st,en);x.stroke();st=en});x.fillStyle=C.white;x.textAlign='center';x.font='800 20px ui-monospace';x.fillText(tot,cx,cy+6);x.textAlign='left';a.forEach((z,i)=>{x.fillStyle=z[2];x.font='9px ui-monospace';x.fillText(z[0]+' '+z[1],w*.72,cy-25+i*24)})}
function yieldChart(d){const [x,w,h]=ctx('yield'),a=d.side_kind||[];x.clearRect(0,0,w,h);a.slice(0,8).forEach((z,i)=>{let rate=z.n?z.improved/z.n:0,y=10+i*(h-18)/8;x.fillStyle='#101923';x.fillRect(145,y,w-165,11);x.fillStyle=C.green;x.fillRect(145,y,(w-165)*rate,11);x.fillStyle=C.txt;x.font='8px ui-monospace';x.textAlign='right';x.fillText(z.kind,138,y+9);x.textAlign='left';x.fillText((rate*100).toFixed(0)+'%',w-33,y+9)});x.textAlign='left'}
function ens(d){const [x,w,h]=ctx('ens'),a=d.ensembles||[];x.clearRect(0,0,w,h);grid(x,w,h);a.forEach(e=>{let rho=Math.max(-.05,Math.min(.5,+e.rho||0)),lift=Math.max(1,Math.min(8,+e.lift||1)),px=44+(rho+.05)/.55*(w-58),py=h-22-(lift-1)/7*(h-40),r=5+10*(+e.live_score||0);x.fillStyle=(e.status==='PASS'?C.green:C.blue);x.globalAlpha=.35+.6*(+e.live_score||0);x.beginPath();x.arc(px,py,r,0,Math.PI*2);x.fill();x.globalAlpha=1})}
function branch(d){const [x,w,h]=ctx('branches'),a=d.branches||[];x.clearRect(0,0,w,h);a.slice(0,8).forEach((b,i)=>{let y=8+i*(h-16)/8,m=Math.max(1,...a.map(z=>+z.done||0)),bw=(+b.done||0)/m*(w-190);x.fillStyle='#101923';x.fillRect(145,y,w-160,10);x.fillStyle=C.blue;x.fillRect(145,y,bw,10);x.fillStyle=C.txt;x.font='8px ui-monospace';x.textAlign='right';x.fillText(b.branch,138,y+9);x.textAlign='left';x.fillText(fmt(b.done),150+bw,y+9)});x.textAlign='left'}
function agenda(d){document.getElementById('agenda').innerHTML=(d.agenda||[]).map(a=>`<div class=ag><b>P${a.priority} · ${a.agenda_type}</b><span>${a.subject} — ${a.rationale}</span></div>`).join('')||'<div class=ag>No open agenda</div>'}
async function refresh(){try{let d=await fetch('/api/state',{cache:'no-store'}).then(r=>r.json());document.getElementById('clock').textContent='LIVE '+new Date().toLocaleTimeString();kpis(d);hist.push({t:d.now,swaps:d.pipeline?.swaps||0,raw:d.pipeline?.raw_tx||0,back:d.pipeline?.spool_pending||0,ing:d.pipeline?.spool_total||0});if(hist.length>180)hist.shift();if(prev){vel.push({ing:(d.pipeline?.spool_total||0)-(prev.pipeline?.spool_total||0),raw:(d.pipeline?.raw_tx||0)-(prev.pipeline?.raw_tx||0),swaps:(d.pipeline?.swaps||0)-(prev.pipeline?.swaps||0),back:(d.pipeline?.spool_pending||0)-(prev.pipeline?.spool_pending||0)});if(vel.length>180)vel.shift()}prev=d;galaxy(d);pipe(d);funnel(d);lines('telemetry',hist,['swaps','raw','back'],[C.cyan,C.green,C.amber]);lines('velocity',vel,['ing','raw','swaps','back'],[C.blue,C.green,C.cyan,C.amber]);ladder(d);heat(d);radar(d);donut(d);yieldChart(d);ens(d);branch(d);agenda(d)}catch(e){document.getElementById('clock').textContent='ERROR'}}
setInterval(refresh,2000);refresh();addEventListener('resize',()=>{if(prev){galaxy(prev);pipe(prev);funnel(prev);lines('telemetry',hist,['swaps','raw','back'],[C.cyan,C.green,C.amber]);lines('velocity',vel,['ing','raw','swaps','back'],[C.blue,C.green,C.cyan,C.amber]);ladder(prev);heat(prev);radar(prev);donut(prev);yieldChart(prev);ens(prev);branch(prev)}});
</script></body></html>'''

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=="/api/state":
            b=json.dumps(collect(),default=str).encode(); self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(b))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(b); return
        if self.path not in ("/","/index.html"): self.send_response(404); self.end_headers(); return
        b=HTML.encode(); self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*_): pass

if __name__=="__main__":
    print("="*96); print("MEMECOIN LAB — V5.1 VISUAL RESEARCH LAB"); print("="*96); print(f"Dashboard: http://{HOST}:{PORT}"); print("Read-only. CTRL+C stops dashboard only."); ThreadingHTTPServer((HOST,PORT),H).serve_forever()
