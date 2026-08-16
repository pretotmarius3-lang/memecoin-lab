#!/usr/bin/env python3
"""Memecoin Lab V5.0 — visual-first master command center.

Read-only dashboard. No trading/signing. No mutation of research state.
Uses only Python stdlib + browser Canvas/SVG; no CDN dependency.

Focus: charts first, tables second.
- rolling live telemetry retained in browser memory
- pipeline throughput / backlog charts
- prospective funnel
- signal rho vs lift scatter
- champion score bars
- family budget radial bars
- research side-lab outcome distribution
- trend/role heatmap
- frozen ensemble frontier
- research branch activity
"""
from __future__ import annotations
import json, math, os, sqlite3, time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V50_DASH_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V50_DASH_PORT","8788"))

def dbopen(path):
    if not path.exists(): return None
    d=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=10)
    d.row_factory=sqlite3.Row; d.execute("PRAGMA busy_timeout=10000"); return d

def table(d,name):
    return bool(d and d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone())

def q1(d,sql,args=(),default=0):
    try:
        r=d.execute(sql,args).fetchone(); return default if not r or r[0] is None else r[0]
    except Exception: return default

def rows(d,sql,args=()):
    try:return [dict(r) for r in d.execute(sql,args).fetchall()]
    except Exception:return []

def collect():
    now=time.time(); o={"now":now,"pipeline":{},"beliefs":{},"roles":{},"families":[],"signals":[],"ensembles":[],"side":{},"side_top":[],"branches":[],"agenda":[]}

    v=dbopen(V5)
    if v:
        if table(v,"v51_signature_spool"):
            o["pipeline"]["spool_total"]=q1(v,"SELECT COUNT(*) FROM v51_signature_spool")
            o["pipeline"]["spool_pending"]=q1(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='PENDING'")
            o["pipeline"]["spool_fetching"]=q1(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='FETCHING'")
            o["pipeline"]["spool_done"]=q1(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='DONE'")
            o["pipeline"]["spool_failed"]=q1(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status IN ('FAILED','DEAD')")
            last=q1(v,"SELECT MAX(first_seen) FROM v51_signature_spool",default=None)
            o["pipeline"]["ingest_age"]=None if last is None else max(0,now-float(last))
        if table(v,"v5_raw_transactions"):
            o["pipeline"]["raw_tx"]=q1(v,"SELECT COUNT(*) FROM v5_raw_transactions")
            last=q1(v,"SELECT MAX(observed_at) FROM v5_raw_transactions",default=None)
            o["pipeline"]["raw_age"]=None if last is None else max(0,now-float(last))
        v.close()

    f=dbopen(V52)
    if f:
        if table(f,"v52_swaps"):
            o["pipeline"]["swaps"]=q1(f,"SELECT COUNT(*) FROM v52_swaps")
            o["pipeline"]["tokens"]=q1(f,"SELECT COUNT(DISTINCT token_mint) FROM v52_swaps")
            last=q1(f,"SELECT MAX(timestamp) FROM v52_swaps",default=None)
            o["pipeline"]["decode_age"]=None if last is None else max(0,now-float(last))
        if table(f,"v52_snapshots"): o["pipeline"]["snapshots"]=q1(f,"SELECT COUNT(*) FROM v52_snapshots")
        if table(f,"v52_outcomes"): o["pipeline"]["outcomes"]=q1(f,"SELECT COUNT(*) FROM v52_outcomes WHERE ready=1")
        f.close()

    r=dbopen(RDB)
    if r:
        if table(r,"v41_jobs"):
            o["jobs"]={x["status"]:x["n"] for x in rows(r,"SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
        else:o["jobs"]={}
        if table(r,"v41_workers"):
            o["workers_live"]=q1(r,"SELECT COUNT(*) FROM v41_workers WHERE last_heartbeat>?",(now-15,))
        else:o["workers_live"]=0
        if table(r,"v46_beliefs"):
            o["beliefs"]={x["status"]:x["n"] for x in rows(r,"SELECT status,COUNT(*) n FROM v46_beliefs GROUP BY status")}
        if table(r,"v48_signal_rankings"):
            o["roles"]={x["role"]:x["n"] for x in rows(r,"SELECT role,COUNT(*) n FROM v48_signal_rankings GROUP BY role")}
            o["signals"]=rows(r,"""SELECT candidate_id,feature,family,target,stage_s,horizon_s,belief_status,audit_class,n,rho,lift,precision,baseline,
                early_rho,late_rho,early_lift,late_lift,stability_score,redundancy_penalty,trend,live_score,role
                FROM v48_signal_rankings ORDER BY live_score DESC LIMIT 80""")
        if table(r,"v48_family_budget"):
            o["families"]=rows(r,"SELECT * FROM v48_family_budget ORDER BY budget_weight DESC")
        if table(r,"v48_research_agenda"):
            o["agenda"]=rows(r,"SELECT priority,family,agenda_type,subject,rationale,state FROM v48_research_agenda WHERE state='OPEN' ORDER BY priority LIMIT 12")
        if table(r,"v47_ensemble_rankings"):
            pass
        if table(r,"v48_ensemble_rankings"):
            o["ensembles"]=rows(r,"SELECT * FROM v48_ensemble_rankings ORDER BY live_score DESC LIMIT 30")
        if table(r,"v49_side_results"):
            o["side"]={x["comparison"]:x["n"] for x in rows(r,"SELECT comparison,COUNT(*) n FROM v49_side_results GROUP BY comparison")}
            o["side_top"]=rows(r,"""SELECT r.experiment_id,r.kind,r.verdict,r.comparison,r.holdout_rho,r.delta_rho,r.qdiff_pp,e.parent_feature
                FROM v49_side_results r LEFT JOIN v49_side_experiments e ON e.experiment_id=r.experiment_id
                ORDER BY COALESCE(r.delta_rho,-999) DESC LIMIT 20""")
        if table(r,"v49_side_experiments"):
            o["side_status"]={x["status"]:x["n"] for x in rows(r,"SELECT status,COUNT(*) n FROM v49_side_experiments GROUP BY status")}
        if table(r,"v41_hypotheses") and table(r,"v41_jobs"):
            o["branches"]=rows(r,"""SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) hypotheses,
              SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
              SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
              SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
              SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed
              FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id GROUP BY h.branch ORDER BY hypotheses DESC""")
        r.close()

    # health score
    ages=[x for x in (o['pipeline'].get('ingest_age'),o['pipeline'].get('raw_age'),o['pipeline'].get('decode_age')) if x is not None]
    age=max(ages) if ages else 9999
    backlog=int(o['pipeline'].get('spool_pending') or 0)
    failed=int(o.get('jobs',{}).get('FAILED',0))
    score=100
    if age>300: score-=35
    elif age>90: score-=15
    if backlog>50000:score-=25
    elif backlog>10000:score-=10
    if failed:score-=10
    if not o.get('workers_live'):score-=20
    o['health_score']=max(0,score)
    return o

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab // Visual Command</title>
<style>
:root{--bg:#05070a;--p:#0a0f14;--p2:#0d141b;--line:#18222d;--txt:#e7edf3;--muted:#657584;--cyan:#36d7e8;--green:#4be38b;--amber:#f3b64c;--red:#ff627d;--blue:#6c8cff;--violet:#a97fff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1900px;margin:auto;padding:18px}.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}.brand{font-size:17px;font-weight:800;letter-spacing:.08em}.sub{font-size:11px;color:var(--muted);margin-top:3px}.live{font:700 11px ui-monospace;padding:6px 9px;border:1px solid var(--line);border-radius:7px}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--green);margin-right:7px}.kpis{display:grid;grid-template-columns:repeat(8,1fr);gap:7px;margin-bottom:7px}.kpi,.panel{background:var(--p);border:1px solid var(--line);border-radius:8px}.kpi{padding:10px 11px}.lab{font-size:9px;color:var(--muted);letter-spacing:.11em;text-transform:uppercase}.val{font:800 21px ui-monospace;margin-top:4px}.hint{font:10px ui-monospace;color:var(--muted);margin-top:2px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:7px}.grid3{display:grid;grid-template-columns:1.25fr 1fr .8fr;gap:7px}.panel{padding:10px;margin-top:7px;min-height:260px}.head{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}.title{font-size:10px;font-weight:800;letter-spacing:.11em;text-transform:uppercase}.tag{font:9px ui-monospace;color:var(--muted)}canvas{width:100%;height:230px;display:block}.small canvas{height:190px}.tall canvas{height:300px}.legend{display:flex;gap:12px;flex-wrap:wrap;font-size:9px;color:var(--muted)}.lg i{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:4px}.rows{font:10px ui-monospace}.row{display:grid;grid-template-columns:24px 1.4fr .8fr .6fr .6fr .7fr;gap:8px;padding:6px 3px;border-bottom:1px solid #111922;align-items:center}.row.h{color:var(--muted);font-size:8px}.good{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}.cyan{color:var(--cyan)}.bar{height:4px;background:#111a22;border-radius:4px;overflow:hidden}.bar i{display:block;height:100%;background:var(--cyan)}.agenda{display:grid;gap:5px}.ag{padding:7px;border-left:2px solid var(--cyan);background:#090f15;font-size:10px}.ag b{font:700 9px ui-monospace}.ag span{display:block;color:var(--muted);margin-top:2px;font-size:9px}.footer{font:9px ui-monospace;color:#4d5c68;margin:10px 0 3px;text-align:right}@media(max-width:1200px){.kpis{grid-template-columns:repeat(4,1fr)}.grid3{grid-template-columns:1fr}.grid2{grid-template-columns:1fr}}
</style></head><body><div class="wrap">
<div class="top"><div><div class="brand">MEMECOIN LAB // VISUAL COMMAND</div><div class="sub">forward evidence · acquisition · science · recursive research · zero decorative noise</div></div><div class="live"><span class="dot"></span><span id="clock">CONNECTING</span></div></div>
<div class="kpis" id="kpis"></div>
<div class="grid3">
 <div class="panel tall"><div class="head"><div class="title">01 // Signal map</div><div class="tag">x = rho · y = lift · radius = N · color = role</div></div><canvas id="scatter"></canvas></div>
 <div class="panel tall"><div class="head"><div class="title">02 // Live telemetry</div><div class="tag">rolling browser memory · 2s</div></div><canvas id="telemetry"></canvas><div class="legend"><span class="lg"><i style="background:var(--cyan)"></i>swaps</span><span class="lg"><i style="background:var(--green)"></i>raw tx</span><span class="lg"><i style="background:var(--amber)"></i>pending spool</span></div></div>
 <div class="panel tall"><div class="head"><div class="title">03 // Prospective funnel</div><div class="tag">WAIT → WATCH → PASS / FAIL</div></div><canvas id="funnel"></canvas></div>
</div>
<div class="grid2">
 <div class="panel"><div class="head"><div class="title">04 // Champion board</div><div class="tag">live score + forward metrics</div></div><canvas id="champions"></canvas></div>
 <div class="panel"><div class="head"><div class="title">05 // Research allocation</div><div class="tag">family budget</div></div><canvas id="families"></canvas></div>
</div>
<div class="grid3">
 <div class="panel"><div class="head"><div class="title">06 // Recursive lab outcome</div><div class="tag">did child beat parent?</div></div><canvas id="side"></canvas></div>
 <div class="panel"><div class="head"><div class="title">07 // Ensemble frontier</div><div class="tag">rho × lift × score</div></div><canvas id="ensembles"></canvas></div>
 <div class="panel"><div class="head"><div class="title">08 // Research branches</div><div class="tag">done / queued / running</div></div><canvas id="branches"></canvas></div>
</div>
<div class="grid2">
 <div class="panel"><div class="head"><div class="title">09 // Best recursive discoveries</div><div class="tag">delta rho vs parent</div></div><div class="rows" id="discoveries"></div></div>
 <div class="panel"><div class="head"><div class="title">10 // Autonomous agenda</div><div class="tag">current machine priorities</div></div><div class="agenda" id="agenda"></div></div>
</div>
<div class="footer">read-only · no trading · no threshold mutation · /api/state</div>
</div>
<script>
const C={bg:'#05070a',grid:'#17212a',txt:'#9aabb9',cyan:'#36d7e8',green:'#4be38b',amber:'#f3b64c',red:'#ff627d',blue:'#6c8cff',violet:'#a97fff'};
const hist=[]; let last=null;
function canvas(id){const c=document.getElementById(id),d=window.devicePixelRatio||1,r=c.getBoundingClientRect();c.width=r.width*d;c.height=r.height*d;const x=c.getContext('2d');x.setTransform(d,0,0,d,0,0);return [x,r.width,r.height]}
function clear(x,w,h){x.clearRect(0,0,w,h);x.fillStyle='#0a0f14';x.fillRect(0,0,w,h)}
function text(x,s,a,b,col=C.txt,size=10,align='left'){x.fillStyle=col;x.font=`${size}px ui-monospace,SFMono-Regular,Menlo,monospace`;x.textAlign=align;x.fillText(s,a,b)}
function grid(x,w,h,l=42,t=12,r=12,b=28){x.strokeStyle=C.grid;x.lineWidth=1;for(let i=0;i<5;i++){let y=t+(h-t-b)*i/4;x.beginPath();x.moveTo(l,y);x.lineTo(w-r,y);x.stroke()}return {l,t,r,b,cw:w-l-r,ch:h-t-b}}
function n(v,d=0){v=Number(v||0);return Number.isFinite(v)?v.toLocaleString(undefined,{maximumFractionDigits:d}):'—'}
function kpi(label,value,hint,cls=''){return `<div class="kpi"><div class="lab">${label}</div><div class="val ${cls}">${value}</div><div class="hint">${hint||''}</div></div>`}
function roleColor(r){if(r==='CHAMPION'||r==='ENSEMBLE_CHAMPION')return C.green;if(r==='CONTENDER'||r==='PASS_WEAK')return C.cyan;if(r==='RETIRE'||r==='FAIL')return C.red;return C.amber}
function drawScatter(d){let [x,w,h]=canvas('scatter');clear(x,w,h);let g=grid(x,w,h),ss=(d.signals||[]).filter(s=>s.rho!=null&&s.lift!=null);if(!ss.length){text(x,'NO SIGNAL DATA',w/2,h/2,C.txt,11,'center');return}let xmin=Math.min(-.05,...ss.map(s=>+s.rho)),xmax=Math.max(.35,...ss.map(s=>+s.rho)),ymin=.8,ymax=Math.max(2,...ss.map(s=>Math.min(8,+s.lift||0)));function X(v){return g.l+(v-xmin)/(xmax-xmin)*g.cw}function Y(v){return g.t+g.ch-(Math.min(ymax,Math.max(ymin,v))-ymin)/(ymax-ymin)*g.ch}for(let i=0;i<5;i++){let v=xmin+(xmax-xmin)*i/4;text(x,v.toFixed(2),X(v),h-8,C.txt,9,'center')}for(let i=0;i<5;i++){let v=ymin+(ymax-ymin)*(4-i)/4;text(x,v.toFixed(1)+'x',g.l-6,g.t+g.ch*i/4+3,C.txt,9,'right')}ss.forEach(s=>{let rr=3+Math.min(9,Math.sqrt(+s.n||0)/4);x.globalAlpha=.72;x.fillStyle=roleColor(s.role);x.beginPath();x.arc(X(+s.rho),Y(+s.lift),rr,0,Math.PI*2);x.fill();x.globalAlpha=1});let top=ss.slice(0,5);top.forEach((s,i)=>text(x,(i+1)+'. '+s.feature,8,14+i*13,roleColor(s.role),9))}
function drawTelemetry(d){let [x,w,h]=canvas('telemetry');clear(x,w,h);let g=grid(x,w,h),p=d.pipeline||{};hist.push({t:Date.now(),sw:+p.swaps||0,raw:+p.raw_tx||0,pending:+p.spool_pending||0});if(hist.length>120)hist.shift();let arr=hist;if(arr.length<2){text(x,'COLLECTING LIVE HISTORY…',w/2,h/2,C.txt,10,'center');return}let series=[['sw',C.cyan],['raw',C.green],['pending',C.amber]];series.forEach(([key,col])=>{let vals=arr.map(a=>a[key]),mn=Math.min(...vals),mx=Math.max(...vals);if(mx===mn)mx=mn+1;x.strokeStyle=col;x.lineWidth=1.7;x.beginPath();arr.forEach((a,i)=>{let xx=g.l+g.cw*i/(arr.length-1),yy=g.t+g.ch-(a[key]-mn)/(mx-mn)*g.ch;i?x.lineTo(xx,yy):x.moveTo(xx,yy)});x.stroke();text(x,key.toUpperCase()+' '+n(vals.at(-1)),g.l+5,g.t+14+series.findIndex(s=>s[0]===key)*13,col,9)})}
function drawFunnel(d){let [x,w,h]=canvas('funnel');clear(x,w,h);let b=d.beliefs||{},a=[['WAIT',+b.WAITING||0,C.blue],['WATCH',+b.WATCH||0,C.amber],['PASS',+b.PASS||0,C.green],['FAIL',+b.FAIL||0,C.red]],mx=Math.max(1,...a.map(z=>z[1]));let cy=32; a.forEach((z,i)=>{let ww=(w-70)*(z[1]/mx),hh=38,xx=(w-ww)/2;x.fillStyle=z[2]+'33';x.strokeStyle=z[2];x.strokeRect(xx,cy,ww,hh);x.fillRect(xx,cy,ww,hh);text(x,z[0],w/2,cy+15,z[2],10,'center');text(x,n(z[1]),w/2,cy+29,'#e7edf3',13,'center');cy+=50})}
function drawBars(id,items,labelKey,valueKey,colorFn,limit=10,suffix=''){let [x,w,h]=canvas(id);clear(x,w,h);items=(items||[]).slice(0,limit);if(!items.length){text(x,'NO DATA',w/2,h/2,C.txt,10,'center');return}let max=Math.max(1,...items.map(a=>+a[valueKey]||0)),left=Math.min(170,w*.38),top=14,row=(h-top-14)/items.length;items.forEach((a,i)=>{let y=top+i*row+row*.2,bw=(w-left-30)*(+a[valueKey]||0)/max,col=colorFn(a,i);text(x,String(a[labelKey]||'').slice(0,22),left-8,y+row*.35,C.txt,9,'right');x.fillStyle='#111a22';x.fillRect(left,y,w-left-30,row*.45);x.fillStyle=col;x.fillRect(left,y,bw,row*.45);text(x,n(a[valueKey],2)+suffix,w-6,y+row*.35,col,9,'right')})}
function drawChampions(d){drawBars('champions',d.signals||[],'feature','live_score',a=>roleColor(a.role),12,'')}
function drawFamilies(d){drawBars('families',d.families||[],'family','budget_weight',(a,i)=>[C.cyan,C.green,C.blue,C.violet,C.amber][i%5],8,'')}
function drawSide(d){let [x,w,h]=canvas('side');clear(x,w,h);let s=d.side||{},arr=[['IMPROVED',+s.IMPROVED||0,C.green],['SAME',+s.SAME||0,C.amber],['WORSE',+s.WORSE||0,C.red]],tot=arr.reduce((q,z)=>q+z[1],0)||1,cy=h/2,r=Math.min(w,h)*.31,start=-Math.PI/2;arr.forEach(z=>{let a=2*Math.PI*z[1]/tot;x.beginPath();x.strokeStyle=z[2];x.lineWidth=24;x.arc(w/2,cy,r,start,start+a);x.stroke();start+=a});text(x,n(tot),w/2,cy-1,'#e7edf3',20,'center');text(x,'SIDE TESTS',w/2,cy+16,C.txt,8,'center');arr.forEach((z,i)=>text(x,z[0]+' '+n(z[1]),9,18+i*14,z[2],9))}
function drawEnsembles(d){let es=d.ensembles||[];drawBars('ensembles',es,'context_key','live_score',a=>roleColor(a.role),10,'')}
function drawBranches(d){let bs=(d.branches||[]).slice(0,10);let [x,w,h]=canvas('branches');clear(x,w,h);if(!bs.length){text(x,'NO BRANCH DATA',w/2,h/2,C.txt,10,'center');return}let mx=Math.max(1,...bs.map(a=>+a.done||0)),left=120,row=(h-18)/bs.length;bs.forEach((a,i)=>{let y=8+i*row,textv=String(a.branch||'').slice(0,18);text(x,textv,left-7,y+10,C.txt,8,'right');let avail=w-left-35,bw=avail*(+a.done||0)/mx;x.fillStyle='#14202a';x.fillRect(left,y,bw,8);x.fillStyle=C.cyan;x.fillRect(left,y,bw,8);if(+a.queued||+a.running){x.fillStyle=C.amber;x.fillRect(left+Math.max(0,bw-4),y,4,8)}text(x,n(a.done),w-5,y+8,C.txt,8,'right')})}
function drawRows(d){let s=(d.side_top||[]).slice(0,12);document.getElementById('discoveries').innerHTML='<div class="row h"><div>#</div><div>KIND / FEATURE</div><div>VERDICT</div><div>RHO</div><div>ΔRHO</div><div>COMPARE</div></div>'+s.map((a,i)=>`<div class="row"><div>${String(i+1).padStart(2,'0')}</div><div><b>${a.kind||''}</b><br><span style="color:#657584">${a.parent_feature||'—'}</span></div><div>${a.verdict||'—'}</div><div>${a.holdout_rho==null?'—':(+a.holdout_rho).toFixed(3)}</div><div class="${(+a.delta_rho||0)>=0?'good':'bad'}">${a.delta_rho==null?'—':(+a.delta_rho).toFixed(3)}</div><div>${a.comparison||'—'}</div></div>`).join('');let ag=d.agenda||[];document.getElementById('agenda').innerHTML=ag.length?ag.map(a=>`<div class="ag"><b>P${a.priority} · ${a.agenda_type}</b>${a.subject}<span>${a.rationale}</span></div>`).join(''):'<div class="ag">NO OPEN AGENDA</div>'}
function render(d){last=d;let p=d.pipeline||{},j=d.jobs||{},b=d.beliefs||{},r=d.roles||{};let health=+d.health_score||0,hc=health>=80?'good':health>=55?'warn':'bad';document.getElementById('kpis').innerHTML=[kpi('SYSTEM',health+' / 100','health',hc),kpi('INGEST',n(p.spool_total),'signatures'),kpi('BACKLOG',n(p.spool_pending),'pending spool',(+p.spool_pending>10000?'warn':'')),kpi('RAW TX',n(p.raw_tx),'enriched'),kpi('TOKENS',n(p.tokens),'decoded universe'),kpi('SWAPS',n(p.swaps),'feature feed'),kpi('PASS',n(b.PASS),'prospective','good'),kpi('CHAMPION',n(r.CHAMPION),'working now','good'),kpi('WATCH',n(b.WATCH),'forward tests','warn'),kpi('OUTCOMES',n(p.outcomes),'mature'),kpi('WORKERS',n(d.workers_live),'heartbeat <15s'),kpi('QUEUE',n(j.QUEUED),'research jobs'),kpi('RUNNING',n(j.RUNNING),'research jobs'),kpi('DONE',n(j.DONE),'all jobs'),kpi('DECODE AGE',p.decode_age==null?'—':n(p.decode_age)+'s','freshness',p.decode_age<90?'good':p.decode_age<300?'warn':'bad'),kpi('SIDE IMPROVED',n((d.side||{}).IMPROVED),'recursive wins','good')].join('');drawScatter(d);drawTelemetry(d);drawFunnel(d);drawChampions(d);drawFamilies(d);drawSide(d);drawEnsembles(d);drawBranches(d);drawRows(d);document.getElementById('clock').textContent=new Date().toLocaleTimeString()+' · LIVE'}
async function tick(){try{let r=await fetch('/api/state',{cache:'no-store'}),d=await r.json();render(d)}catch(e){document.getElementById('clock').textContent='DISCONNECTED'}}
setInterval(tick,2000);tick();window.addEventListener('resize',()=>last&&render(last));
</script></body></html>'''

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/api/state'):
            b=json.dumps(collect(),default=str,separators=(',',':')).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b);return
        if self.path not in ('/','/index.html'):
            self.send_response(404);self.end_headers();return
        b=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
    def log_message(self,*_):pass

if __name__=='__main__':
    print('='*88);print('MEMECOIN LAB — V5.0 VISUAL COMMAND CENTER');print('='*88);print(f'Dashboard: http://{HOST}:{PORT}');print('Visual-first · read-only · no CDN · refresh 2s · CTRL+C stops dashboard only');ThreadingHTTPServer((HOST,PORT),H).serve_forever()
