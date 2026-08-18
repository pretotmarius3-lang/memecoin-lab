#!/usr/bin/env python3
"""MEMECOIN LAB — EXPERIMENT OBSERVATORY V7.6.0

Read-only dashboard built for both researchers and outsiders.
It explains what is being tested, why each experiment exists, and whether the
scientific chain is currently healthy. Never mutates any research database.
"""
from __future__ import annotations

import json, math, os, sqlite3, statistics, subprocess, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path.home()/"memecoin_lab"
HOST = os.environ.get("MEMECOIN_V760_HOST", "127.0.0.1")
PORT = int(os.environ.get("MEMECOIN_V760_PORT", "8796"))

DB = {
    "research": ROOT/"research_v4_1.db",
    "discovery": ROOT/"v743_orthogonal_discovery.db",
    "acq": ROOT/"v750_acquisition_trace.db",
    "arena": ROOT/"v752_post_burst_future.db",
    "decay": ROOT/"v721_decay_monitor.db",
}


def ro(path: Path):
    d=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=3)
    d.row_factory=sqlite3.Row
    d.execute("PRAGMA query_only=ON")
    d.execute("PRAGMA busy_timeout=3000")
    return d


def rows(d,q,a=()):
    try:return [dict(x) for x in d.execute(q,a).fetchall()]
    except:return []


def one(d,q,a=()):
    try:
        x=d.execute(q,a).fetchone();return dict(x) if x else None
    except:return None


def pct(xs,q):
    if not xs:return None
    ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def process_list():
    try:
        out=subprocess.run(["ps","aux"],capture_output=True,text=True,timeout=2).stdout.splitlines()
        keep=[]
        for ln in out:
            if "python" not in ln.lower():continue
            if "memecoin_lab" not in ln:continue
            if any(x in ln for x in ("v750_","v752_","v743_","v721_","v64_","v760_")):
                parts=ln.split()
                keep.append({"pid":parts[1] if len(parts)>1 else "?","cpu":parts[2] if len(parts)>2 else "?","command":" ".join(parts[10:])})
        return keep
    except:return []


def collect_r64():
    o={"available":False}
    if not DB["research"].exists():return o
    try:
        d=ro(DB["research"])
        z=one(d,"SELECT * FROM v64_forward_summary ORDER BY updated_at DESC LIMIT 1") or one(d,"SELECT * FROM v64_forward_summary LIMIT 1")
        d.close()
        if z:o={"available":True,**z}
    except:pass
    return o


def collect_discovery():
    o={"available":False,"counts":{},"families":[],"top":[]}
    if not DB["discovery"].exists():return o
    try:
        d=ro(DB["discovery"])
        e=one(d,"SELECT * FROM epoch ORDER BY created_at DESC LIMIT 1")
        cs=rows(d,"SELECT status,COUNT(*) n FROM results GROUP BY status")
        fam=rows(d,"SELECT * FROM family_summary ORDER BY CASE status WHEN 'SHORTLIST' THEN 0 ELSE 1 END, independent_instances DESC, median_expectancy DESC LIMIT 12")
        top=rows(d,"""SELECT e.family,e.feature,e.stage_s,e.horizon_s,r.status,r.selected_holdout,
                         r.holdout_expectancy,r.holdout_pf,r.expectancy_lift,r.fill_rate,r.robust_score,r.updated_at
                  FROM results r JOIN experiments e USING(experiment_id)
                  WHERE r.status IN ('ROBUST_POSITIVE','REJECT')
                  ORDER BY CASE r.status WHEN 'ROBUST_POSITIVE' THEN 0 ELSE 1 END,r.robust_score DESC LIMIT 10""")
        d.close();o={"available":True,"epoch":e,"counts":{x['status']:x['n'] for x in cs},"families":fam,"top":top}
    except Exception as e:o["error"]=repr(e)
    return o


def collect_acq():
    o={"available":False,"series":[],"recent":{}}
    if not DB["acq"].exists():return o
    try:
        d=ro(DB["acq"]);now=time.time()
        ep=one(d,"SELECT epoch_id,MAX(updated_at) newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1")
        if not ep:d.close();return o
        eid=ep['epoch_id']
        q=one(d,"SELECT * FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1",(eid,))
        qs=rows(d,"SELECT sampled_at,pending_total,pending_hot,fetching,oldest_pending_age_s,current_rps FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 90",(eid,))[::-1]
        tr=rows(d,"""SELECT enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at
                   FROM trace WHERE epoch_id=? AND kind='HOT' AND raw_store_at IS NOT NULL AND raw_store_at>=?
                   ORDER BY raw_store_at DESC""",(eid,now-60))
        d.close()
        claim=[];http=[];total=[]
        for r in tr:
            if r['enqueue_at'] is not None and r['first_claim_at'] is not None:claim.append(max(0,float(r['first_claim_at'])-float(r['enqueue_at'])))
            if r['http_start_at'] is not None and r['http_end_at'] is not None:http.append(max(0,float(r['http_end_at'])-float(r['http_start_at'])))
            if r['enqueue_at'] is not None and r['raw_store_at'] is not None:total.append(max(0,float(r['raw_store_at'])-float(r['enqueue_at'])))
        p90=pct(total,.9);p95=pct(total,.95)
        healthy=bool(q and len(total)>=50 and int(q.get('pending_total') or 0)==0 and float(q.get('oldest_pending_age_s') or 0)<2 and p90 is not None and p90<=2 and p95 is not None and p95<=3)
        o={"available":True,"epoch":eid,"queue":q,"series":qs,
           "recent":{"n":len(total),"claim_p90":pct(claim,.9),"http_p90":pct(http,.9),"total_p50":pct(total,.5),"total_p90":p90,"total_p95":p95,"healthy":healthy}}
    except Exception as e:o["error"]=repr(e)
    return o


def collect_arena(acq):
    o={"available":False,"rules":[]}
    if not DB["arena"].exists():return o
    try:
        d=ro(DB["arena"])
        a=one(d,"SELECT * FROM arena LIMIT 1")
        f=one(d,"SELECT * FROM infrastructure_freeze WHERE id=1")
        rr=rows(d,"""SELECT r.family,r.feature,r.stage_s,r.horizon_s,r.direction,r.threshold,
                       s.* FROM frozen_rule r LEFT JOIN summary s USING(rule_id) ORDER BY r.family""")
        iv=one(d,"SELECT COUNT(*) n FROM integrity_violation") or {"n":0}
        tr=one(d,"SELECT COUNT(*) n FROM transition_log") or {"n":0}
        d.close()
        live=bool(acq.get('recent',{}).get('healthy'))
        o={"available":True,"arena":a,"freeze":f,"rules":rr,"integrity":iv['n'],"transitions":tr['n'],"live_healthy":live,"paused":not live}
    except Exception as e:o["error"]=repr(e)
    return o


def collect_decay():
    if not DB['decay'].exists():return []
    try:
        d=ro(DB['decay']);x=rows(d,"SELECT * FROM decay_state ORDER BY rowid DESC LIMIT 12");d.close();return x
    except:return []


def collect():
    acq=collect_acq()
    return {"now":time.time(),"processes":process_list(),"r64":collect_r64(),"discovery":collect_discovery(),"acq":acq,"arena":collect_arena(acq),"decay":collect_decay()}


HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab Observatory</title><style>
:root{--bg:#03070a;--panel:#071017e8;--line:#17303f;--text:#eef8fc;--muted:#7d93a0;--green:#55f0a4;--cyan:#55ddea;--amber:#ffbe5c;--red:#ff647d;--violet:#ab8cff;--blue:#75a7ff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% -10%,#17354b 0,#050b10 34%,#020406 78%);color:var(--text);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh}body:before{content:"";position:fixed;inset:0;pointer-events:none;background-image:linear-gradient(#ffffff04 1px,transparent 1px),linear-gradient(90deg,#ffffff04 1px,transparent 1px);background-size:36px 36px}.wrap{max-width:1840px;margin:auto;padding:20px}.top{display:flex;justify-content:space-between;gap:20px;align-items:flex-start}.brand{font:900 24px ui-monospace;letter-spacing:.12em}.kicker{font:800 10px ui-monospace;letter-spacing:.16em;color:var(--cyan)}.muted,.small{color:var(--muted)}.small{font:10px ui-monospace}.live{font:900 10px ui-monospace;color:var(--green);animation:pulse 1.6s infinite}.panel{position:relative;background:linear-gradient(180deg,#08131bdd,#050d13e8);border:1px solid var(--line);border-radius:16px;padding:15px;margin-top:14px;overflow:hidden}.panel:after{content:"";position:absolute;inset:auto -20% -80% 20%;height:180px;background:radial-gradient(circle,#55ddea0d,transparent 65%);pointer-events:none}.title{font:900 10px ui-monospace;letter-spacing:.16em;color:#a5bac5;margin-bottom:12px}.hero{display:grid;grid-template-columns:1.15fr .85fr;gap:14px}.question{font-size:28px;font-weight:900;line-height:1.08;max-width:850px}.answer{margin-top:10px;color:#9fb3bd;max-width:900px;line-height:1.5}.legend{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.pill{border:1px solid #1c3949;border-radius:12px;padding:10px;background:#08151d}.pill b{display:block;font:900 11px ui-monospace}.pipeline{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;position:relative}.stage{position:relative;border:1px solid #1c3949;background:#07131b;border-radius:12px;padding:14px;text-align:center;min-height:88px}.stage .icon{font-size:23px}.stage b{display:block;font:900 10px ui-monospace;margin-top:7px}.stage span{display:block;font-size:10px;color:var(--muted);margin-top:4px}.stage:not(:last-child):after{content:"→";position:absolute;right:-16px;top:34px;color:#3c6578;font-weight:900;z-index:3}.particle{position:absolute;width:8px;height:8px;border-radius:50%;background:var(--cyan);box-shadow:0 0 16px var(--cyan);top:42px;left:2%;animation:travel 7s linear infinite;z-index:5}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.exp{border:1px solid #1b3746;border-radius:14px;padding:14px;background:#07131b;min-height:205px}.expTop{display:flex;justify-content:space-between;align-items:center}.expName{font:900 13px ui-monospace}.status{font:900 9px ui-monospace;padding:5px 8px;border-radius:999px;border:1px solid currentColor}.green{color:var(--green)}.cyan{color:var(--cyan)}.amber{color:var(--amber)}.red{color:var(--red)}.violet{color:var(--violet)}.blue{color:var(--blue)}.metrics{display:grid;grid-template-columns:repeat(3,1fr);gap:7px;margin-top:12px}.metric{background:#091923;border:1px solid #183543;border-radius:9px;padding:9px}.ml{font:8px ui-monospace;color:var(--muted)}.mv{font:900 18px ui-monospace;margin-top:2px}.explain{font-size:11px;line-height:1.45;color:#a7bac4;margin-top:12px}.bar{height:8px;background:#10202a;border-radius:999px;overflow:hidden;margin-top:8px}.bar>i{display:block;height:100%;background:linear-gradient(90deg,var(--cyan),var(--violet));transition:width .8s}.cols{display:grid;grid-template-columns:1fr 1fr;gap:14px}.canvasBox{height:230px;border:1px solid #183542;border-radius:12px;background:#051018;padding:8px}.canvasBox canvas{width:100%;height:100%}.ruleGrid{display:grid;grid-template-columns:repeat(3,1fr);gap:9px}.rule{border:1px solid #1c3948;border-radius:12px;padding:12px;background:#07141c}.ring{width:78px;height:78px;border-radius:50%;display:grid;place-items:center;background:conic-gradient(var(--cyan) var(--p),#122631 0);margin:10px auto;position:relative}.ring:after{content:"";position:absolute;width:58px;height:58px;border-radius:50%;background:#07141c}.ring span{position:relative;z-index:2;font:900 13px ui-monospace}.families{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.family{display:grid;grid-template-columns:145px 1fr 80px;gap:8px;align-items:center;background:#07141c;border:1px solid #1a3442;border-radius:9px;padding:8px;font:10px ui-monospace}.famBar{height:7px;border-radius:99px;background:#10202a;overflow:hidden}.famBar i{display:block;height:100%;background:linear-gradient(90deg,var(--violet),var(--cyan))}.timeline{display:grid;grid-template-columns:repeat(5,1fr);gap:8px}.memory{border:1px solid #243441;border-radius:10px;padding:10px;background:#071018}.memory b{font:900 10px ui-monospace}.memory p{font-size:10px;line-height:1.4;color:#91a7b2}.proc{font:9px ui-monospace;line-height:1.6;color:#8ea4af;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.hint{border-left:3px solid var(--cyan);padding-left:10px;margin-top:10px;color:#a4b7c1;font-size:11px}.tooltip{position:fixed;pointer-events:none;background:#061019;border:1px solid #23475a;border-radius:8px;padding:8px 10px;font-size:10px;max-width:260px;opacity:0;transform:translateY(6px);transition:.15s;z-index:20}@keyframes pulse{50%{opacity:.3}}@keyframes travel{0%{left:2%;opacity:0}8%{opacity:1}92%{opacity:1}100%{left:97%;opacity:0}}@media(max-width:1100px){.hero,.cols,.grid3{grid-template-columns:1fr}.pipeline{grid-template-columns:repeat(3,1fr)}.ruleGrid{grid-template-columns:1fr}.timeline{grid-template-columns:1fr 1fr}.families{grid-template-columns:1fr}}@media(max-width:650px){.pipeline{grid-template-columns:1fr}.stage:after{display:none}.timeline{grid-template-columns:1fr}.metrics{grid-template-columns:1fr 1fr}.brand{font-size:18px}}
</style></head><body><div class="tooltip" id="tip"></div><div class="wrap">
<div class="top"><div><div class="kicker">MEMECOIN LAB // V7.6.0</div><div class="brand">EXPERIMENT OBSERVATORY</div><div class="small">A visual explanation of what the lab is testing right now — read only.</div></div><div><div class="live">● LIVE SCIENTIFIC BUS</div><div class="small" id="clock"></div></div></div>
<div class="panel hero"><div><div class="title">THE QUESTION</div><div class="question">Can we find a memecoin signal that survives <span class="cyan">future-only testing</span> while the data pipeline is fast enough to trade it?</div><div class="answer">The lab deliberately separates <b>discovery</b>, <b>infrastructure</b>, and <b>validation</b>. A strategy is not trusted because a backtest looks good. It must first be frozen, then face data that did not exist when the rule was chosen, while latency guardrails remain healthy.</div></div><div class="legend"><div class="pill"><b class="violet">DISCOVERY</b><span class="small">Search for new mechanisms. Interesting ≠ proven.</span></div><div class="pill"><b class="cyan">INFRASTRUCTURE</b><span class="small">Can the information arrive fast enough?</span></div><div class="pill"><b class="green">VALIDATION</b><span class="small">Frozen rules face brand-new data.</span></div><div class="pill"><b class="amber">CONTROL</b><span class="small">R64 is the external benchmark.</span></div></div></div>
<div class="panel"><div class="title">HOW ONE TOKEN BECOMES EVIDENCE</div><div class="pipeline"><div class="particle"></div><div class="stage" data-tip="A transaction appears on Solana. Nothing is inferred yet."><div class="icon">◎</div><b>BLOCKCHAIN</b><span>raw event</span></div><div class="stage" data-tip="V7.5.0 receives and enriches HOT events. This is where burst latency is controlled."><div class="icon">⚡</div><b>ACQUISITION</b><span>V7.5.0</span></div><div class="stage" data-tip="Canonical snapshots summarize what was knowable at T+20s or T+30s."><div class="icon">◇</div><b>FEATURES</b><span>causal snapshot</span></div><div class="stage" data-tip="Frozen thresholds decide signal or no signal exactly once."><div class="icon">⌁</div><b>FROZEN RULE</b><span>no retuning</span></div><div class="stage" data-tip="A next fill must exist inside the execution window. Late data cannot rescue a signal."><div class="icon">↯</div><b>EXECUTION</b><span>next fill</span></div><div class="stage" data-tip="Only matured future-only outcomes count as evidence."><div class="icon">✓</div><b>EVIDENCE</b><span>DONE outcome</span></div></div></div>
<div class="panel"><div class="title">EXPERIMENTS RUNNING NOW</div><div class="grid3" id="experiments"></div></div>
<div class="cols"><div class="panel"><div class="title">V7.5.0 // ACQUISITION HEALTH — RECENT WINDOW</div><div class="canvasBox"><canvas id="qchart"></canvas></div><div id="acqText" class="hint"></div></div><div class="panel"><div class="title">V7.5.2 // FROZEN RULES UNDER FUTURE-ONLY TEST</div><div id="rules" class="ruleGrid"></div></div></div>
<div class="cols"><div class="panel"><div class="title">V7.4.3 // ORTHOGONAL DISCOVERY</div><div id="families" class="families"></div></div><div class="panel"><div class="title">RESEARCH CONTROL + PROCESS STATUS</div><div id="control"></div><div class="title" style="margin-top:14px">ACTIVE PYTHON PROCESSES</div><div id="procs"></div></div></div>
<div class="panel"><div class="title">WHY THE SCIENTIFIC CHAIN LOOKS SO STRICT</div><div class="timeline"><div class="memory"><b class="red">V7.4.2</b><p>Rejected as final evidence after mutable feature timing was discovered.</p></div><div class="memory"><b class="red">V7.4.2.2</b><p>Append-only improved causality, but out-of-order snapshots could still be missed.</p></div><div class="memory"><b class="amber">V7.4.2.3</b><p>Snapshot-first fixed ingestion and exposed that most data arrived too late.</p></div><div class="memory"><b class="amber">V7.4.9</b><p>Fresh arena started after latency repair, then a burst showed the infrastructure was not stable enough.</p></div><div class="memory"><b class="green">V7.5.2</b><p>Current design pauses automatically whenever live latency leaves the frozen healthy regime.</p></div></div></div>
</div><script>
const $=id=>document.getElementById(id),E=x=>String(x??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])),N=(x,n=2)=>x==null?'—':Number(x).toFixed(n),P=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%';
function badge(s){let x=String(s||'UNKNOWN'),c=/HEALTHY|CONFIRMED|STABLE|RUNNING/.test(x)?'green':/PAUSED|WAIT|COLLECT/.test(x)?'amber':/FAIL|ERROR|UNHEALTHY/.test(x)?'red':'cyan';return `<span class="status ${c}">${E(x)}</span>`}
function expCards(d){let a=d.acq||{},ar=d.arena||{},di=d.discovery||{},r=d.r64||{};let h=a.recent?.healthy?'LIVE HEALTHY':'LIVE DEGRADED';let arenaState=!ar.available?'OFFLINE':ar.paused?'PAUSED':'COLLECTING';let c=di.counts||{},pos=c.ROBUST_POSITIVE||0,coll=c.COLLECTING||0,short=(di.families||[]).filter(x=>x.status==='SHORTLIST').length;return `
<div class="exp"><div class="expTop"><div class="expName cyan">V7.5.0 · DATA ENGINE</div>${badge(h)}</div><div class="metrics"><div class="metric"><div class="ml">HOT 60s</div><div class="mv">${a.recent?.n||0}</div></div><div class="metric"><div class="ml">P90</div><div class="mv ${a.recent?.healthy?'green':'red'}">${N(a.recent?.total_p90,3)}s</div></div><div class="metric"><div class="ml">PENDING</div><div class="mv">${a.queue?.pending_total??'—'}</div></div></div><div class="explain">Purpose: prove that live blockchain data can survive bursts and still reach the research stack fast enough for 20–30 second signals.</div></div>
<div class="exp"><div class="expTop"><div class="expName green">V7.5.2 · FUTURE ARENA</div>${badge(arenaState)}</div><div class="metrics"><div class="metric"><div class="ml">TRANSITIONS</div><div class="mv">${ar.transitions||0}</div></div><div class="metric"><div class="ml">INTEGRITY</div><div class="mv ${ar.integrity?'red':'green'}">${ar.integrity||0}</div></div><div class="metric"><div class="ml">RULES</div><div class="mv">${(ar.rules||[]).length}</div></div></div><div class="explain">Purpose: test three rules that were chosen earlier, without changing them after seeing new outcomes. It automatically pauses when infrastructure leaves the healthy regime.</div></div>
<div class="exp"><div class="expTop"><div class="expName violet">V7.4.3 · DISCOVERY</div>${badge(di.available?'RUNNING / SCREENING':'OFFLINE')}</div><div class="metrics"><div class="metric"><div class="ml">ROBUST +</div><div class="mv green">${pos}</div></div><div class="metric"><div class="ml">COLLECTING</div><div class="mv cyan">${coll}</div></div><div class="metric"><div class="ml">SHORTLIST</div><div class="mv violet">${short}</div></div></div><div class="explain">Purpose: search for mechanisms different from the current validation rail. Discovery can nominate candidates, but cannot prove them.</div></div>`}
function rules(d){let rs=d.arena?.rules||[];if(!rs.length)return '<div class="small">No current arena rows yet.</div>';return rs.map(r=>{let done=Number(r.done||0),goal=30,pc=Math.min(100,100*done/goal),st=r.status||'WAITING';return `<div class="rule"><div class="expTop"><b class="expName">${E(r.family)}</b>${badge(st)}</div><div class="ring" style="--p:${pc}%"><span>${done}/${goal}</span></div><div class="small">${E(r.feature)} · stage ${r.stage_s}s · horizon ${r.horizon_s}s</div><div class="metrics"><div class="metric"><div class="ml">OBSERVED</div><div class="mv">${r.observed||0}</div></div><div class="metric"><div class="ml">SIGNALS</div><div class="mv">${r.signals||0}</div></div><div class="metric"><div class="ml">LATE</div><div class="mv ${Number(r.late_snapshot||0)>0?'amber':''}">${r.late_snapshot||0}</div></div></div><div class="small" style="margin-top:8px">EXP ${N(r.expectancy)}% · PF ${N(r.pf)} · fill ${P(r.fill_rate)}</div></div>`}).join('')}
function families(d){let fs=d.discovery?.families||[];if(!fs.length)return '<div class="small">Discovery is collecting; no family summary yet.</div>';let mx=Math.max(1,...fs.map(x=>Number(x.independent_instances||0)));return fs.map(x=>`<div class="family"><span>${E(x.family)} <b class="${x.status==='SHORTLIST'?'green':'amber'}">${E(x.status)}</b></span><div class="famBar"><i style="width:${100*Number(x.independent_instances||0)/mx}%"></i></div><span>${x.independent_instances||0} indep</span></div>`).join('')}
function control(d){let r=d.r64||{};let dd=r.true_dd??r.true_drawdown??null;return `<div class="exp"><div class="expTop"><div class="expName amber">R64 · EXTERNAL CONTROL</div>${badge(r.available?'REFERENCE':'OFFLINE')}</div><div class="metrics"><div class="metric"><div class="ml">DONE</div><div class="mv">${r.done??'—'}</div></div><div class="metric"><div class="ml">EXPECTANCY</div><div class="mv">${N(r.expectancy)}%</div></div><div class="metric"><div class="ml">PF</div><div class="mv">${N(r.profit_factor)}</div></div></div><div class="explain">R64 is not being optimized here. It is the benchmark the challenger research must eventually justify replacing or complementing.</div></div>`}
function procs(d){return (d.processes||[]).map(x=>`<div class="proc">PID ${E(x.pid)} · CPU ${E(x.cpu)}% · ${E(x.command)}</div>`).join('')||'<div class="small">No tracked processes found.</div>'}
function drawQueue(series){let c=$('qchart'),ctx=c.getContext('2d'),r=c.getBoundingClientRect(),D=devicePixelRatio||1;c.width=r.width*D;c.height=r.height*D;ctx.scale(D,D);let W=r.width,H=r.height;ctx.clearRect(0,0,W,H);ctx.strokeStyle='#173443';ctx.lineWidth=1;for(let i=1;i<5;i++){let y=H*i/5;ctx.beginPath();ctx.moveTo(0,y);ctx.lineTo(W,y);ctx.stroke()}if(!series?.length){ctx.fillStyle='#78909c';ctx.fillText('Waiting for queue samples…',12,22);return}let max=Math.max(1,...series.map(x=>Number(x.pending_total||0)),...series.map(x=>Number(x.oldest_pending_age_s||0)*10));let line=(key,mul,color)=>{ctx.strokeStyle=color;ctx.lineWidth=2;ctx.beginPath();series.forEach((x,i)=>{let xx=8+(W-16)*i/Math.max(1,series.length-1),yy=H-12-(H-28)*(Number(x[key]||0)*mul/max);i?ctx.lineTo(xx,yy):ctx.moveTo(xx,yy)});ctx.stroke()};line('pending_total',1,'#55ddea');line('oldest_pending_age_s',10,'#ffbe5c');ctx.fillStyle='#55ddea';ctx.fillText('pending',12,16);ctx.fillStyle='#ffbe5c';ctx.fillText('oldest ×10',72,16)}
function acqText(d){let a=d.acq||{},r=a.recent||{},q=a.queue||{};let txt=r.healthy?'The last 60 seconds are inside the frozen low-latency regime.':'The current live window is outside the validation regime; V7.5.2 should pause rather than count evidence.';return `${txt} HOT n=${r.n||0}, p90=${N(r.total_p90,3)}s, p95=${N(r.total_p95,3)}s, queue=${q.pending_total??'—'}, oldest=${N(q.oldest_pending_age_s,1)}s.`}
async function go(){try{let d=await(await fetch('/api?'+Date.now(),{cache:'no-store'})).json();$('clock').textContent=new Date(d.now*1000).toLocaleTimeString();$('experiments').innerHTML=expCards(d);$('rules').innerHTML=rules(d);$('families').innerHTML=families(d);$('control').innerHTML=control(d);$('procs').innerHTML=procs(d);$('acqText').textContent=acqText(d);drawQueue(d.acq?.series||[])}catch(e){$('clock').textContent='BUS ERROR · '+e}setTimeout(go,1800)}
const tip=$('tip');document.addEventListener('mouseover',e=>{let x=e.target.closest('[data-tip]');if(!x)return;tip.textContent=x.dataset.tip;tip.style.opacity=1});document.addEventListener('mousemove',e=>{tip.style.left=(e.clientX+14)+'px';tip.style.top=(e.clientY+14)+'px'});document.addEventListener('mouseout',e=>{if(e.target.closest('[data-tip]'))tip.style.opacity=0});window.addEventListener('resize',()=>{});go();
</script></body></html>'''


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/api'):
            b=json.dumps(collect(),separators=(',',':'),default=str).encode();ct='application/json'
        else:
            b=HTML.encode();ct='text/html; charset=utf-8'
        self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
    def log_message(self,*_):pass


if __name__=='__main__':
    print(f"MEMECOIN LAB V7.6.0 observatory http://{HOST}:{PORT}",flush=True)
    ThreadingHTTPServer((HOST,PORT),H).serve_forever()
