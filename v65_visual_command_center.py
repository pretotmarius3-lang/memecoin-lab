#!/usr/bin/env python3
"""MEMECOIN LAB V6.5 — lightweight visual command center.

Read-only dashboard focused on the current V6 pipeline.
- server-side cache; avoids rescanning large SQLite tables every browser refresh
- true throughput rates = delta counter / delta wall-clock seconds
- HOT lane telemetry + queue debt
- V6.4 prospective champion state and data-quality funnel
- separate DATA / SCIENCE / EXECUTION health scores
- legacy V4/V5 research summaries refresh slowly and stay secondary

No trading, no mutation of scientific state.
"""
from __future__ import annotations
import json, math, os, sqlite3, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V5=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V65_DASH_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V65_DASH_PORT","8792"))
FAST_CACHE_S=float(os.environ.get("MEMECOIN_V65_FAST_CACHE_S","2"))
SLOW_CACHE_S=float(os.environ.get("MEMECOIN_V65_SLOW_CACHE_S","30"))

LOCK=threading.Lock(); FAST={"at":0,"data":{}}; SLOW={"at":0,"data":{}}
PREV=None

def dbopen(p):
    if not p.exists(): return None
    d=sqlite3.connect(f"file:{p}?mode=ro",uri=True,timeout=3); d.row_factory=sqlite3.Row
    d.execute("PRAGMA busy_timeout=3000"); d.execute("PRAGMA query_only=ON"); return d

def has(d,t):
    try:return bool(d and d.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())
    except Exception:return False

def one(d,sql,args=(),default=0):
    try:
        r=d.execute(sql,args).fetchone(); return default if not r or r[0] is None else r[0]
    except Exception:return default

def row(d,sql,args=()):
    try:
        r=d.execute(sql,args).fetchone(); return dict(r) if r else {}
    except Exception:return {}
def rows(d,sql,args=()):
    try:return [dict(x) for x in d.execute(sql,args).fetchall()]
    except Exception:return []

def state_json(d,key):
    if not d or not has(d,"v5_collector_state"):return {}
    x=one(d,"SELECT value FROM v5_collector_state WHERE key=?",(key,),default=None)
    if not x:return {}
    try:return json.loads(x)
    except Exception:return {}

def clamp(x,a=0,b=100): return max(a,min(b,x))

def fast_collect():
    global PREV
    now=time.time(); out={"now":now,"pipeline":{},"hot":{},"v64":{},"v62":[],"rates":{},"health":{}}
    v=dbopen(V5)
    if v:
        p=out["pipeline"]
        if has(v,"v51_signature_spool"):
            # indexed status queries; no full spool_total COUNT every refresh
            p["pending"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='PENDING'")
            p["fetching"]=one(v,"SELECT COUNT(*) FROM v51_signature_spool WHERE status='FETCHING'")
            p["spool_latest"]=one(v,"SELECT MAX(first_seen) FROM v51_signature_spool",default=None)
        if has(v,"v5_raw_transactions"):
            p["raw"]=one(v,"SELECT COUNT(*) FROM v5_raw_transactions")
            p["raw_latest"]=one(v,"SELECT MAX(observed_at) FROM v5_raw_transactions",default=None)
        out["hot"]=state_json(v,"v514_hot_lane") or state_json(v,"v511_scheduler")
        v.close()
    f=dbopen(V52)
    if f:
        p=out["pipeline"]
        if has(f,"v52_processed"): p["processed"]=one(f,"SELECT COUNT(*) FROM v52_processed")
        if has(f,"v52_swaps"):
            p["swaps"]=one(f,"SELECT COUNT(*) FROM v52_swaps")
            p["swap_latest"]=one(f,"SELECT MAX(timestamp) FROM v52_swaps",default=None)
        if has(f,"v52_snapshots"):
            p["snapshots"]=one(f,"SELECT COUNT(*) FROM v52_snapshots")
            p["stage20_pv"]=one(f,"SELECT COUNT(*) FROM v52_snapshots WHERE stage_s=20 AND price_velocity IS NOT NULL")
        f.close()
    r=dbopen(RDB)
    if r:
        if has(r,"v64_frozen_rule"):
            rule=row(r,"SELECT * FROM v64_frozen_rule LIMIT 1")
            summ=row(r,"SELECT * FROM v64_forward_summary LIMIT 1") if has(r,"v64_forward_summary") else {}
            out["v64"]={"rule":rule,"summary":summ}
        if has(r,"v62_forward_summary"):
            out["v62"]=rows(r,"SELECT * FROM v62_forward_summary ORDER BY rule_id LIMIT 8")
        r.close()
    p=out["pipeline"]
    p["decode_age"]=None if p.get("swap_latest") is None else max(0,now-float(p["swap_latest"]))
    p["raw_age"]=None if p.get("raw_latest") is None else max(0,now-float(p["raw_latest"]))
    p["ingest_age"]=None if p.get("spool_latest") is None else max(0,now-float(p["spool_latest"]))
    current={"t":now,"pending":float(p.get("pending") or 0),"raw":float(p.get("raw") or 0),"swaps":float(p.get("swaps") or 0),"processed":float(p.get("processed") or 0)}
    if PREV:
        dt=max(.2,now-PREV["t"])
        for k in ("raw","swaps","processed"):
            out["rates"][k+"_s"]=max(0,(current[k]-PREV[k])/dt)
        out["rates"]["backlog_net_s"]=(current["pending"]-PREV["pending"])/dt
    PREV=current
    hot=out["hot"] or {}; q=float(hot.get("queue") or 0); rps=float(hot.get("target_rps") or 0)
    hot["queue_debt_s"]=(q/rps) if rps>0 else None
    # Health is intentionally about live usefulness, not historical backlog size.
    age=float(p.get("decode_age") if p.get("decode_age") is not None else 9999)
    data=100-(45 if age>300 else 25 if age>120 else 10 if age>60 else 0)
    debt=hot.get("queue_debt_s")
    if debt is not None:data-=35 if debt>300 else 20 if debt>120 else 8 if debt>45 else 0
    if float(hot.get("queue_drops") or 0)>0:data-=15
    v64s=(out.get("v64") or {}).get("summary") or {}; done=int(v64s.get("done") or 0)
    science=55+min(30,done*3)
    if int(v64s.get("anomaly") or 0)>int(v64s.get("done") or 0)+3:science-=10
    execution=100
    fr=v64s.get("fill_rate")
    if fr is not None: execution-=25 if float(fr)<.10 else 12 if float(fr)<.20 else 0
    if int(v64s.get("sparse_path") or 0)>max(2,done):execution-=15
    out["health"]={"data":int(clamp(data)),"science":int(clamp(science)),"execution":int(clamp(execution))}
    return out

def slow_collect():
    o={"legacy":{},"families":[],"side":{},"v631":[]}
    r=dbopen(RDB)
    if not r:return o
    if has(r,"v46_beliefs"): o["legacy"]["beliefs"]={x["status"]:x["n"] for x in rows(r,"SELECT status,COUNT(*) n FROM v46_beliefs GROUP BY status")}
    if has(r,"v49_side_results"): o["side"]={x["comparison"]:x["n"] for x in rows(r,"SELECT comparison,COUNT(*) n FROM v49_side_results GROUP BY comparison")}
    if has(r,"v631_family_champions"):o["v631"]=rows(r,"SELECT * FROM v631_family_champions ORDER BY CASE status WHEN 'STRONG_REPLICATION' THEN 0 WHEN 'REPLICATED' THEN 1 ELSE 2 END, median_expectancy DESC LIMIT 12")
    if has(r,"v48_family_budget"):o["families"]=rows(r,"SELECT * FROM v48_family_budget ORDER BY budget_weight DESC LIMIT 12")
    r.close(); return o

def collect():
    now=time.time()
    with LOCK:
        if now-FAST["at"]>=FAST_CACHE_S:
            FAST["data"]=fast_collect(); FAST["at"]=now
        if now-SLOW["at"]>=SLOW_CACHE_S:
            SLOW["data"]=slow_collect(); SLOW["at"]=now
        return {**FAST["data"],**SLOW["data"],"cache":{"fast_age":now-FAST["at"],"slow_age":now-SLOW["at"]}}

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V6.5</title><style>
:root{--bg:#05090d;--panel:#091018;--line:#1b2935;--txt:#e8eef4;--muted:#70808e;--cyan:#54d5e5;--green:#67e699;--amber:#efb14c;--red:#ff7185;--blue:#7893ff}
*{box-sizing:border-box}body{margin:0;background:#05090d;color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1920px;margin:auto;padding:12px}.top{display:flex;justify-content:space-between;align-items:end}.brand{font-size:20px;font-weight:900;letter-spacing:.08em}.sub,.tag{color:var(--muted);font-size:9px}.live{font:800 10px ui-monospace;color:var(--green)}.kpis{display:grid;grid-template-columns:repeat(8,1fr);gap:7px;margin-top:9px}.kpi,.panel{background:linear-gradient(180deg,#0a121a,#080e14);border:1px solid var(--line);border-radius:9px}.kpi{padding:9px}.lab{font:8px ui-monospace;color:var(--muted);letter-spacing:.13em}.val{font:800 20px ui-monospace;margin-top:4px}.hint{font:8px ui-monospace;color:var(--muted);margin-top:2px}.g2{display:grid;grid-template-columns:1fr 1fr;gap:7px}.g3{display:grid;grid-template-columns:1.1fr 1fr 1fr;gap:7px}.panel{padding:10px;margin-top:7px;min-height:230px}.head{display:flex;justify-content:space-between}.title{font-size:9px;font-weight:900;letter-spacing:.13em}.bars{display:grid;gap:9px;margin-top:16px}.barrow{display:grid;grid-template-columns:130px 1fr 75px;gap:10px;align-items:center;font:10px ui-monospace}.track{height:12px;background:#101b24;border-radius:2px;overflow:hidden}.fill{height:100%;background:var(--cyan)}.green{background:var(--green)}.amber{background:var(--amber)}.red{background:var(--red)}.big{font:900 33px ui-monospace}.mono{font-family:ui-monospace}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:12px}.mini{background:#0b151e;padding:8px;border:1px solid #162632;border-radius:6px}.mini b{display:block;font:800 17px ui-monospace}.mini span{font:8px ui-monospace;color:var(--muted)}.progress{height:14px;background:#101b24;margin-top:15px;border-radius:2px;overflow:hidden}.progress>div{height:100%;background:var(--green)}canvas{width:100%;height:200px}.funnel{display:grid;gap:6px;margin-top:12px}.step{display:grid;grid-template-columns:140px 1fr 70px;align-items:center;gap:8px;font:9px ui-monospace}.status{font:800 12px ui-monospace;padding:7px;border:1px solid var(--line);display:inline-block;margin-top:10px}.table{width:100%;border-collapse:collapse;margin-top:10px;font:9px ui-monospace}.table td,.table th{padding:5px;border-bottom:1px solid #15222c;text-align:right}.table td:first-child,.table th:first-child{text-align:left}.good{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}@media(max-width:1200px){.kpis{grid-template-columns:repeat(4,1fr)}.g2,.g3{grid-template-columns:1fr}}
</style></head><body><div class="wrap"><div class="top"><div><div class="brand">MEMECOIN LAB // COMMAND CENTER V6.5</div><div class="sub">live data quality · prospective science · execution reality · read-only</div></div><div class="live" id="clock">CONNECTING</div></div><div class="kpis" id="kpis"></div>
<div class="g3"><div class="panel"><div class="head"><div class="title">01 / LIVE PIPELINE</div><div class="tag">freshness + real rates</div></div><div id="pipeline" class="bars"></div></div><div class="panel"><div class="head"><div class="title">02 / HOT LANE</div><div class="tag">V5.1.4 critical path</div></div><div id="hot"></div></div><div class="panel"><div class="head"><div class="title">03 / HEALTH</div><div class="tag">live usefulness, not backlog size</div></div><div id="health" class="bars"></div></div></div>
<div class="g2"><div class="panel"><div class="head"><div class="title">04 / THROUGHPUT VELOCITY</div><div class="tag">normalized / second · 60 samples</div></div><canvas id="velocity"></canvas></div><div class="panel"><div class="head"><div class="title">05 / V6.4 PROSPECTIVE CHAMPION</div><div class="tag">immutable next-fill rule</div></div><div id="v64"></div></div></div>
<div class="g2"><div class="panel"><div class="head"><div class="title">06 / DATA QUALITY FUNNEL</div><div class="tag">where prospective evidence is lost</div></div><div id="quality" class="funnel"></div></div><div class="panel"><div class="head"><div class="title">07 / V6.2 FORWARD EVIDENCE</div><div class="tag">legacy prospective cohort · still running</div></div><div id="v62"></div></div></div>
<div class="g2"><div class="panel"><div class="head"><div class="title">08 / V6.3.1 FAMILY GATE</div><div class="tag">next-fill replication status</div></div><div id="v631"></div></div><div class="panel"><div class="head"><div class="title">09 / LEGACY RESEARCH</div><div class="tag">slow refresh · secondary</div></div><div id="legacy"></div></div></div></div>
<script>
const H=[];const C={grid:'#18252f',cyan:'#54d5e5',green:'#67e699',amber:'#efb14c',red:'#ff7185',blue:'#7893ff',txt:'#8999a6'};
function fmt(n,d=1){n=Number(n||0);return Math.abs(n)>=1e6?(n/1e6).toFixed(2)+'M':Math.abs(n)>=1e3?(n/1e3).toFixed(1)+'k':n.toFixed? n.toFixed(d):n}
function pct(x){return (100*Number(x||0)).toFixed(1)+'%'}
function cls(n,a,b){return n>=a?'good':n>=b?'warn':'bad'}
function bar(label,v,max,color=''){let w=max?Math.max(0,Math.min(100,100*v/max)):0;return `<div class=barrow><span>${label}</span><div class=track><div class="fill ${color}" style="width:${w}%"></div></div><b>${fmt(v)}</b></div>`}
function kpis(d){let p=d.pipeline||{},h=d.hot||{},v=d.v64?.summary||{},hs=d.health||{};let vals=[['DATA',hs.data+'/100','live data health'],['SCIENCE',hs.science+'/100','prospective evidence'],['EXECUTION',hs.execution+'/100','fills + full path'],['RAW',fmt(p.raw),'enriched tx'],['SWAPS',fmt(p.swaps),'decoded'],['DECODE AGE',p.decode_age==null?'—':Math.round(p.decode_age)+'s','freshness'],['HOT DEBT',h.queue_debt_s==null?'—':Math.round(h.queue_debt_s)+'s','queue / rps'],['V6.4 DONE',(v.done||0)+'/30',v.status||'WAITING'],['V6.4 SIGNALS',v.signals||0,'future-only'],['V6.4 FILL',pct(v.fill_rate),'post-signal'],['V6.4 EXP',v.expectancy==null?'—':(Number(v.expectancy)>=0?'+':'')+Number(v.expectancy).toFixed(2)+'%','net costs'],['V6.4 PF',v.profit_factor==null?'—':Number(v.profit_factor).toFixed(2),'future-only'],['HOT ACTIVE',h.hot_subscribed-h.hot_unsubscribed||0,'session cumulative'],['HOT QUEUE',h.queue||0,'critical HTTP'],['429',h['429']||0,'hot lane'],['PENDING',fmt(p.pending),'historical spool']];document.getElementById('kpis').innerHTML=vals.map(x=>`<div class=kpi><div class=lab>${x[0]}</div><div class=val>${x[1]}</div><div class=hint>${x[2]}</div></div>`).join('')}
function pipeline(d){let p=d.pipeline||{},r=d.rates||{};let mx=Math.max(r.raw_s||0,r.swaps_s||0,r.processed_s||0,1);document.getElementById('pipeline').innerHTML=bar('RAW / s',r.raw_s||0,mx)+bar('SWAPS / s',r.swaps_s||0,mx,'green')+bar('DECODE / s',r.processed_s||0,mx)+bar('BACKLOG net / s',Math.abs(r.backlog_net_s||0),Math.max(Math.abs(r.backlog_net_s||0),mx),'amber')+`<div class=metrics><div class=mini><b>${fmt(p.raw)}</b><span>RAW TOTAL</span></div><div class=mini><b>${fmt(p.swaps)}</b><span>SWAPS</span></div><div class=mini><b>${fmt(p.processed)}</b><span>PROCESSED</span></div><div class=mini><b>${p.decode_age==null?'—':Math.round(p.decode_age)+'s'}</b><span>DECODE AGE</span></div></div>`}
function hot(d){let h=d.hot||{};let debt=h.queue_debt_s;document.getElementById('hot').innerHTML=`<div class=metrics><div class=mini><b>${fmt(h.creates_sampled||0)}</b><span>SAMPLED CREATE</span></div><div class=mini><b>${fmt(h.hot_swaps||0)}</b><span>HOT SWAPS</span></div><div class=mini><b>${fmt(h.fetched||0)}</b><span>FETCHED</span></div><div class=mini><b>${h.queue||0}</b><span>QUEUE</span></div><div class=mini><b>${debt==null?'—':Math.round(debt)+'s'}</b><span>QUEUE DEBT</span></div><div class=mini><b>${h['429']||0}</b><span>429</span></div><div class=mini><b>${h.queue_drops||0}</b><span>DROPS</span></div><div class=mini><b>${h.reconnects||0}</b><span>RECONNECTS</span></div></div><div class="status ${debt!=null&&debt>180?'bad':debt!=null&&debt>60?'warn':'good'}">${debt==null?'NO TELEMETRY':debt>180?'HOT LANE SATURATED':debt>60?'HOT LANE LOADED':'HOT LANE HEALTHY'}</div>`}
function health(d){let h=d.health||{};document.getElementById('health').innerHTML=bar('DATA',h.data||0,100,'green')+bar('SCIENCE',h.science||0,100,'green')+bar('EXECUTION',h.execution||0,100,'green')+`<div class=tag style="margin-top:16px">Historical backlog is intentionally excluded from DATA health unless it harms live freshness.</div>`}
function velocity(d){H.push({t:d.now,...d.rates});if(H.length>60)H.shift();let cv=document.getElementById('velocity'),r=cv.getBoundingClientRect(),z=devicePixelRatio||1;cv.width=r.width*z;cv.height=r.height*z;let x=cv.getContext('2d');x.setTransform(z,0,0,z,0,0);x.clearRect(0,0,r.width,r.height);for(let i=1;i<5;i++){let y=20+i*(r.height-40)/5;x.strokeStyle=C.grid;x.beginPath();x.moveTo(42,y);x.lineTo(r.width-10,y);x.stroke()}let max=Math.max(1,...H.flatMap(a=>[a.raw_s||0,a.swaps_s||0,a.processed_s||0]));function line(key,col){x.strokeStyle=col;x.lineWidth=2;x.beginPath();H.forEach((a,i)=>{let xx=42+i*(r.width-55)/Math.max(1,H.length-1),yy=r.height-20-(a[key]||0)/max*(r.height-45);i?x.lineTo(xx,yy):x.moveTo(xx,yy)});x.stroke()}line('raw_s',C.green);line('swaps_s',C.cyan);line('processed_s',C.blue);x.fillStyle=C.txt;x.font='10px ui-monospace';x.fillText(`max ${max.toFixed(1)}/s`,8,14);x.fillStyle=C.green;x.fillText('RAW/s',50,r.height-5);x.fillStyle=C.cyan;x.fillText('SWAPS/s',105,r.height-5);x.fillStyle=C.blue;x.fillText('DECODE/s',175,r.height-5)}
function v64(d){let v=d.v64||{},s=v.summary||{},r=v.rule||{},done=Number(s.done||0);document.getElementById('v64').innerHTML=`<div class=big>${r.feature||'WAITING'}</div><div class=tag>stage=${r.stage_s||'—'}s · horizon=${r.horizon_s||'—'}s · TP/SL=${r.tp_pct||'—'}/${r.sl_pct||'—'} · threshold=${r.threshold?Number(r.threshold).toFixed(6):'—'}</div><div class=metrics><div class=mini><b>${s.eligible||0}</b><span>ELIGIBLE</span></div><div class=mini><b>${s.signals||0}</b><span>SIGNALS</span></div><div class=mini><b>${s.done||0}</b><span>DONE</span></div><div class=mini><b>${pct(s.fill_rate)}</b><span>FILL</span></div><div class=mini><b>${s.expectancy==null?'—':(Number(s.expectancy)>=0?'+':'')+Number(s.expectancy).toFixed(2)+'%'}</b><span>EXPECTANCY</span></div><div class=mini><b>${s.profit_factor==null?'—':Number(s.profit_factor).toFixed(2)}</b><span>PF</span></div><div class=mini><b>${s.win_rate==null?'—':pct(s.win_rate)}</b><span>WIN</span></div><div class=mini><b>${s.median_fill_delay==null?'—':Number(s.median_fill_delay).toFixed(1)+'s'}</b><span>FILL DELAY</span></div></div><div class=progress><div style="width:${Math.min(100,100*done/30)}%"></div></div><div class=tag>${done}/30 confirmation · status=${s.status||'WAITING'}</div>`}
function quality(d){let s=d.v64?.summary||{};let vals=[['ELIGIBLE',s.eligible||0],['SIGNAL',s.signals||0],['FILLED',Math.max(0,(s.signals||0)-(s.no_fill||0)-(s.waiting_fill||0))],['FULL PATH',(s.done||0)+(s.waiting_maturity||0)],['DONE',s.done||0],['NO FILL',s.no_fill||0],['SPARSE',s.sparse_path||0],['ANOMALY',s.anomaly||0]];let mx=Math.max(1,...vals.slice(0,5).map(x=>x[1]));document.getElementById('quality').innerHTML=vals.map((x,i)=>`<div class=step><span>${x[0]}</span><div class=track><div class="fill ${i>4?'amber':i===4?'green':''}" style="width:${Math.min(100,100*x[1]/mx)}%"></div></div><b>${x[1]}</b></div>`).join('')}
function v62(d){let a=d.v62||[];document.getElementById('v62').innerHTML=a.length?`<table class=table><tr><th>RULE</th><th>DONE</th><th>EXP</th><th>PF</th><th>STATUS</th></tr>${a.map(x=>`<tr><td>${String(x.rule_id||'').slice(0,12)}</td><td>${x.done||0}</td><td>${x.expectancy==null?'—':Number(x.expectancy).toFixed(2)+'%'}</td><td>${x.profit_factor==null?'—':Number(x.profit_factor).toFixed(2)}</td><td>${x.status||'—'}</td></tr>`).join('')}</table>`:'<div class=tag>No V6.2 summary</div>'}
function v631(d){let a=d.v631||[];document.getElementById('v631').innerHTML=a.length?`<table class=table><tr><th>FAMILY</th><th>STATUS</th><th>INDEP</th><th>TOKENS</th><th>EXP</th><th>FILL</th></tr>${a.map(x=>`<tr><td>${x.family}</td><td>${x.status}</td><td>${x.independent_regimes}</td><td>${x.unique_holdout_tokens}</td><td>${x.median_expectancy==null?'—':Number(x.median_expectancy).toFixed(2)+'%'}</td><td>${x.median_fill_rate==null?'—':pct(x.median_fill_rate)}</td></tr>`).join('')}</table>`:'<div class=tag>No V6.3.1 data</div>'}
function legacy(d){let b=d.legacy?.beliefs||{},s=d.side||{};document.getElementById('legacy').innerHTML=`<div class=metrics><div class=mini><b>${b.PASS||0}</b><span>LEGACY PASS</span></div><div class=mini><b>${b.WATCH||0}</b><span>LEGACY WATCH</span></div><div class=mini><b>${s.IMPROVED||0}</b><span>SIDE IMPROVED</span></div><div class=mini><b>${s.WORSE||0}</b><span>SIDE WORSE</span></div></div><div class=tag style="margin-top:16px">Legacy research is deliberately secondary. V6 prospective evidence drives the command center.</div>`}
async function refresh(){try{let d=await fetch('/api/state',{cache:'no-store'}).then(r=>r.json());document.getElementById('clock').textContent='LIVE '+new Date().toLocaleTimeString();kpis(d);pipeline(d);hot(d);health(d);velocity(d);v64(d);quality(d);v62(d);v631(d);legacy(d)}catch(e){document.getElementById('clock').textContent='DISCONNECTED'}}refresh();setInterval(refresh,2000);
</script></body></html>'''

class H(BaseHTTPRequestHandler):
    def log_message(self,*_):pass
    def sendb(self,code,body,ctype):
        b=body.encode(); self.send_response(code); self.send_header("Content-Type",ctype); self.send_header("Content-Length",str(len(b))); self.send_header("Cache-Control","no-store"); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        if self.path.startswith('/api/state'):
            try:self.sendb(200,json.dumps(collect(),separators=(',',':'),default=str),'application/json')
            except Exception as e:self.sendb(500,json.dumps({'error':repr(e)}),'application/json')
        elif self.path=='/' or self.path.startswith('/?'):self.sendb(200,HTML,'text/html; charset=utf-8')
        else:self.sendb(404,'not found','text/plain')

if __name__=='__main__':
    print(f'MEMECOIN LAB V6.5 COMMAND CENTER → http://{HOST}:{PORT}',flush=True)
    print(f'fast_cache={FAST_CACHE_S:.1f}s slow_cache={SLOW_CACHE_S:.0f}s | read-only',flush=True)
    ThreadingHTTPServer((HOST,PORT),H).serve_forever()
