#!/usr/bin/env python3
"""MEMECOIN LAB — CINEMATIC EXPERIMENT OBSERVATORY V7.6.1
Read-only outsider-first dashboard. No scientific state is mutated.
"""
from __future__ import annotations
import json, os, sqlite3, subprocess, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
HOST=os.environ.get("MEMECOIN_V761_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V761_PORT","8797"))
DB={
 "research":ROOT/"research_v4_1.db",
 "arena":ROOT/"v752_post_burst_future.db",
 "acq":ROOT/"v750_acquisition_trace.db",
 "discovery":ROOT/"v743_orthogonal_discovery.db",
 "decay":ROOT/"v721_decay_monitor.db",
}

def ro(p):
 d=sqlite3.connect(f"file:{p}?mode=ro",uri=True,timeout=3);d.row_factory=sqlite3.Row
 d.execute("PRAGMA query_only=ON");d.execute("PRAGMA busy_timeout=3000");return d

def one(d,q,a=()):
 try:
  r=d.execute(q,a).fetchone();return dict(r) if r else None
 except:return None

def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []

def pct(xs,q):
 if not xs:return None
 ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def procs():
 try:
  ls=subprocess.run(["ps","aux"],capture_output=True,text=True,timeout=2).stdout.splitlines();out=[]
  for ln in ls:
   if "python" not in ln.lower() or "memecoin_lab" not in ln:continue
   if any(k in ln for k in ("v750_","v752_","v743_","v721_","v64_","v761_")):
    p=ln.split();out.append({"pid":p[1] if len(p)>1 else "?","cpu":p[2] if len(p)>2 else "?","cmd":" ".join(p[10:])})
  return out
 except:return []

def collect_r64():
 o={"available":False}
 if not DB['research'].exists():return o
 try:
  d=ro(DB['research']);z=one(d,"SELECT * FROM v64_forward_summary ORDER BY updated_at DESC LIMIT 1") or one(d,"SELECT * FROM v64_forward_summary LIMIT 1");d.close()
  if z:o={"available":True,**z}
 except:pass
 return o

def collect_acq():
 o={"available":False,"series":[],"recent":{}}
 if not DB['acq'].exists():return o
 try:
  d=ro(DB['acq']);now=time.time();ep=one(d,"SELECT epoch_id,MAX(updated_at) newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1")
  if not ep:d.close();return o
  eid=ep['epoch_id'];q=one(d,"SELECT * FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1",(eid,))
  s=rows(d,"SELECT sampled_at,pending_total,pending_hot,fetching,oldest_pending_age_s,current_rps FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 120",(eid,))[::-1]
  tr=rows(d,"SELECT enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at FROM trace WHERE epoch_id=? AND kind='HOT' AND raw_store_at>=? ORDER BY raw_store_at",(eid,now-60))
  d.close();claim=[];http=[];total=[]
  for r in tr:
   if r['enqueue_at'] is not None and r['first_claim_at'] is not None:claim.append(max(0,float(r['first_claim_at'])-float(r['enqueue_at'])))
   if r['http_start_at'] is not None and r['http_end_at'] is not None:http.append(max(0,float(r['http_end_at'])-float(r['http_start_at'])))
   if r['enqueue_at'] is not None and r['raw_store_at'] is not None:total.append(max(0,float(r['raw_store_at'])-float(r['enqueue_at'])))
  p90=pct(total,.9);p95=pct(total,.95);pending=int((q or {}).get('pending_total') or 0);old=float((q or {}).get('oldest_pending_age_s') or 0)
  healthy=len(total)>=50 and pending==0 and old<2 and p90 is not None and p90<=2 and p95 is not None and p95<=3
  return {"available":True,"epoch":eid,"queue":q,"series":s,"recent":{"n":len(total),"claim_p90":pct(claim,.9),"http_p90":pct(http,.9),"p50":pct(total,.5),"p90":p90,"p95":p95,"healthy":healthy}}
 except Exception as e:o['error']=repr(e)
 return o

def collect_arena(acq):
 o={"available":False,"rules":[]}
 if not DB['arena'].exists():return o
 try:
  d=ro(DB['arena']);a=one(d,"SELECT * FROM arena LIMIT 1");f=one(d,"SELECT * FROM infrastructure_freeze WHERE id=1")
  rr=rows(d,"SELECT r.family,r.feature,r.stage_s,r.horizon_s,r.direction,r.threshold,s.* FROM frozen_rule r LEFT JOIN summary s USING(rule_id) ORDER BY r.family")
  iv=one(d,"SELECT COUNT(*) n FROM integrity_violation") or {"n":0};tl=one(d,"SELECT COUNT(*) n FROM transition_log") or {"n":0};d.close()
  return {"available":True,"arena":a,"freeze":f,"rules":rr,"integrity":iv['n'],"transitions":tl['n'],"live_healthy":bool(acq.get('recent',{}).get('healthy'))}
 except Exception as e:o['error']=repr(e)
 return o

def collect_discovery():
 o={"available":False,"counts":{},"families":[],"top":[]}
 if not DB['discovery'].exists():return o
 try:
  d=ro(DB['discovery']);e=one(d,"SELECT * FROM epoch ORDER BY created_at DESC LIMIT 1");cs=rows(d,"SELECT status,COUNT(*) n FROM results GROUP BY status")
  fam=rows(d,"SELECT * FROM family_summary ORDER BY CASE status WHEN 'SHORTLIST' THEN 0 ELSE 1 END, independent_instances DESC, median_expectancy DESC LIMIT 14")
  top=rows(d,"SELECT e.family,e.feature,e.stage_s,e.horizon_s,r.status,r.selected_holdout,r.holdout_expectancy,r.holdout_pf,r.expectancy_lift,r.fill_rate,r.robust_score FROM results r JOIN experiments e USING(experiment_id) WHERE r.status IN ('ROBUST_POSITIVE','REJECT') ORDER BY CASE r.status WHEN 'ROBUST_POSITIVE' THEN 0 ELSE 1 END,r.robust_score DESC LIMIT 12");d.close()
  return {"available":True,"epoch":e,"counts":{x['status']:x['n'] for x in cs},"families":fam,"top":top}
 except Exception as e:o['error']=repr(e)
 return o

def collect_decay():
 if not DB['decay'].exists():return []
 try:
  d=ro(DB['decay']);x=rows(d,"SELECT * FROM decay_state ORDER BY rowid DESC LIMIT 10");d.close();return x
 except:return []

def collect():
 acq=collect_acq();arena=collect_arena(acq)
 return {"now":time.time(),"r64":collect_r64(),"acq":acq,"arena":arena,"discovery":collect_discovery(),"decay":collect_decay(),"processes":procs()}

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab // Cinematic Observatory</title><style>
:root{--bg:#020509;--panel:#07121aeb;--line:#173544;--txt:#f0f8fb;--muted:#78909c;--green:#57efa5;--cyan:#53dcea;--violet:#a88bff;--amber:#ffbd59;--red:#ff667d;--blue:#6ea6ff}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(circle at 50% -18%,#15354d 0,#050a0f 33%,#020406 76%);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;overflow-x:hidden}#stars{position:fixed;inset:0;z-index:-2}.scan{position:fixed;inset:0;z-index:-1;pointer-events:none;background:linear-gradient(transparent 49%,#ffffff05 50%,transparent 51%);background-size:100% 5px;opacity:.22}.wrap{max-width:1900px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;gap:20px;align-items:center}.brand{font:900 25px ui-monospace;letter-spacing:.12em}.kicker{font:900 10px ui-monospace;letter-spacing:.2em;color:var(--cyan)}.clock{font:800 10px ui-monospace;color:#91aab6;text-align:right}.live{color:var(--green);animation:pulse 1.4s infinite}.hero{display:grid;grid-template-columns:1.25fr .75fr;gap:16px;margin-top:18px}.panel{position:relative;background:linear-gradient(180deg,#08131bdd,#050d13ee);border:1px solid var(--line);border-radius:18px;padding:16px;overflow:hidden;box-shadow:0 18px 60px #0007}.panel:before{content:"";position:absolute;inset:0;pointer-events:none;background:radial-gradient(circle at 85% 0,#53dcea0b,transparent 30%)}.mega{font-size:36px;font-weight:950;line-height:1.03;max-width:950px}.mega span{background:linear-gradient(90deg,#fff,var(--cyan),var(--violet));-webkit-background-clip:text;color:transparent}.lead{max-width:900px;color:#9db0ba;line-height:1.55;font-size:13px}.mission{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:18px}.mcard{border:1px solid #193746;border-radius:12px;background:#07141c;padding:11px}.ml{font:8px ui-monospace;color:var(--muted);letter-spacing:.08em}.mv{font:900 20px ui-monospace;margin-top:3px}.green{color:var(--green)}.red{color:var(--red)}.amber{color:var(--amber)}.cyan{color:var(--cyan)}.violet{color:var(--violet)}.title{font:900 10px ui-monospace;letter-spacing:.16em;color:#a8bbc4;margin-bottom:12px}.healthOrb{width:180px;height:180px;border-radius:50%;margin:12px auto;position:relative;display:grid;place-items:center;background:conic-gradient(var(--green) var(--hp),#112630 0);filter:drop-shadow(0 0 28px #57efa522);animation:float 4s ease-in-out infinite}.healthOrb:before{content:"";position:absolute;inset:13px;border-radius:50%;background:#061018;border:1px solid #1d4253}.healthOrb:after{content:"";position:absolute;inset:31px;border-radius:50%;border:1px dashed #2f6276;animation:spin 11s linear infinite}.healthOrb .in{position:relative;z-index:2;text-align:center}.healthOrb .score{font:950 36px ui-monospace}.healthOrb .lab{font:800 9px ui-monospace;color:#839ba7}.pipeline{display:grid;grid-template-columns:repeat(7,1fr);gap:9px;position:relative}.stage{position:relative;background:#07141b;border:1px solid #183746;border-radius:13px;padding:13px;text-align:center;min-height:100px}.stage .ico{font-size:24px}.stage b{display:block;font:900 9px ui-monospace;margin-top:8px}.stage small{display:block;color:var(--muted);font-size:9px;margin-top:4px}.stage:not(:last-child):after{content:"→";position:absolute;right:-14px;top:40px;color:#3e697c;font-weight:900}.pulseDot{position:absolute;top:50px;left:1.5%;width:7px;height:7px;border-radius:50%;background:var(--cyan);box-shadow:0 0 18px var(--cyan);animation:travel 6.5s linear infinite;z-index:5}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.exp{position:relative;border:1px solid #1b3948;border-radius:15px;padding:14px;background:#07131b;overflow:hidden;transition:.2s}.exp:hover{transform:translateY(-2px);border-color:#326278}.expTop{display:flex;justify-content:space-between;align-items:center}.expName{font:950 13px ui-monospace}.badge{font:900 8px ui-monospace;padding:5px 8px;border-radius:999px;border:1px solid currentColor}.ring{--p:0deg;width:86px;height:86px;border-radius:50%;display:grid;place-items:center;margin:12px auto;background:conic-gradient(var(--cyan) var(--p),#10232d 0);position:relative}.ring:after{content:"";position:absolute;width:64px;height:64px;border-radius:50%;background:#07131b}.ring span{position:relative;z-index:2;font:900 14px ui-monospace}.mini{display:grid;grid-template-columns:repeat(3,1fr);gap:6px}.box{border:1px solid #173443;background:#091923;border-radius:9px;padding:8px;text-align:center}.box b{display:block;font:900 14px ui-monospace}.box small{font:8px ui-monospace;color:var(--muted)}.story{font-size:11px;line-height:1.45;color:#a6bac4;margin-top:10px}.cols{display:grid;grid-template-columns:1.1fr .9fr;gap:14px}.chart{height:250px;border:1px solid #173544;border-radius:12px;background:#051019;padding:6px}.chart canvas{width:100%;height:100%}.stack{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.kpi{padding:10px;border:1px solid #193746;border-radius:10px;background:#07141c;text-align:center}.kpi b{display:block;font:950 18px ui-monospace}.kpi small{font:8px ui-monospace;color:var(--muted)}.families{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.fam{display:grid;grid-template-columns:145px 1fr 78px;gap:8px;align-items:center;border:1px solid #183645;border-radius:10px;background:#07141b;padding:9px;font:9px ui-monospace}.track{height:7px;border-radius:99px;background:#10222c;overflow:hidden}.track i{display:block;height:100%;background:linear-gradient(90deg,var(--violet),var(--cyan));transition:width .8s}.timeline{display:grid;grid-template-columns:repeat(6,1fr);gap:8px}.event{border:1px solid #1c3441;border-radius:11px;background:#071019;padding:10px;min-height:120px}.event b{font:900 9px ui-monospace}.event p{font-size:9px;color:#8fa4af;line-height:1.45}.brain{border-left:3px solid var(--cyan);padding-left:12px;color:#b8c8cf;line-height:1.5;font-size:12px}.proc{font:9px ui-monospace;line-height:1.6;color:#879faa;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.toast{position:fixed;right:18px;bottom:18px;background:#06121a;border:1px solid #2b5366;border-radius:12px;padding:12px 14px;font:10px ui-monospace;opacity:0;transform:translateY(20px);transition:.25s;z-index:30;max-width:320px}.toast.on{opacity:1;transform:none}.split{display:grid;grid-template-columns:1fr 1fr;gap:10px}.signal{height:110px;border:1px solid #163341;border-radius:11px;background:#061018;padding:10px}.signal canvas{width:100%;height:100%}@keyframes pulse{50%{opacity:.25}}@keyframes spin{to{transform:rotate(360deg)}}@keyframes float{50%{transform:translateY(-5px)}}@keyframes travel{0%{left:1.5%;opacity:0}6%{opacity:1}94%{opacity:1}100%{left:97.5%;opacity:0}}@media(max-width:1200px){.hero,.cols{grid-template-columns:1fr}.grid3{grid-template-columns:1fr}.pipeline{grid-template-columns:repeat(4,1fr)}.timeline{grid-template-columns:repeat(2,1fr)}}
</style></head><body><canvas id="stars"></canvas><div class="scan"></div><div class="wrap"><div class="top"><div><div class="kicker">LIVE SCIENTIFIC SYSTEM</div><div class="brand">MEMECOIN LAB // OBSERVATORY</div></div><div class="clock"><div class="live">● LIVE DATA</div><div id="clock"></div><div>V7.6.1 · READ ONLY</div></div></div>
<div class="hero"><div class="panel"><div class="kicker">WHAT IS THIS LAB TRYING TO PROVE?</div><div class="mega">Find memecoin signals that <span>still work after reality attacks them.</span></div><div class="lead">Every idea is forced through fresh data, execution constraints, future-only validation and decay monitoring. A strategy is not considered real because it looked good in hindsight; it must survive after its rule is frozen.</div><div id="mission" class="mission"></div></div><div class="panel"><div class="title">LAB HEALTH</div><div id="orb"></div><div id="healthText" class="brain"></div></div></div>
<div class="panel"><div class="title">THE SCIENTIFIC PIPELINE — FROM BLOCKCHAIN TO EVIDENCE</div><div class="pipeline"><div class="pulseDot"></div><div class="stage"><div class="ico">⛓</div><b>BLOCKCHAIN</b><small>raw on-chain events</small></div><div class="stage"><div class="ico">📡</div><b>ACQUISITION</b><small>Alchemy + queue</small></div><div class="stage"><div class="ico">🧬</div><b>FEATURES</b><small>20s / 30s snapshots</small></div><div class="stage"><div class="ico">🔬</div><b>DISCOVERY</b><small>candidate mechanisms</small></div><div class="stage"><div class="ico">🔒</div><b>FREEZE</b><small>rule becomes immutable</small></div><div class="stage"><div class="ico">🏟</div><b>FUTURE ARENA</b><small>fresh prospective test</small></div><div class="stage"><div class="ico">🧯</div><b>DECAY WATCH</b><small>edge monitored over time</small></div></div></div>
<div class="panel"><div class="title">ACTIVE EXPERIMENTS — WHAT IS RUNNING RIGHT NOW?</div><div id="experiments" class="grid3"></div></div>
<div class="cols"><div class="panel"><div class="title">INFRASTRUCTURE HEARTBEAT — HOT QUEUE & LATENCY</div><div id="acqKpi" class="stack"></div><div class="chart"><canvas id="infraChart"></canvas></div><div class="split" style="margin-top:10px"><div class="signal"><canvas id="latencyBars"></canvas></div><div id="infraExplain" class="brain"></div></div></div><div class="panel"><div class="title">FUTURE-ONLY ARENA — THREE FROZEN RULES</div><div id="rules" class="grid3"></div></div></div>
<div class="cols"><div class="panel"><div class="title">ORTHOGONAL DISCOVERY — NEW MECHANISMS</div><div id="discoveryKpi" class="stack"></div><div id="families" class="families" style="margin-top:10px"></div></div><div class="panel"><div class="title">BENCHMARK CONTROL — R64</div><div id="r64"></div><div class="title" style="margin-top:16px">PLAIN-ENGLISH INTERPRETATION</div><div id="brain" class="brain"></div></div></div>
<div class="panel"><div class="title">SCIENTIFIC MEMORY — WHY OLD RESULTS ARE NOT SILENTLY REUSED</div><div class="timeline"><div class="event"><b class="amber">V7.4.2</b><p>Initial common future arena. Later audit found feature timing contamination.</p></div><div class="event"><b class="red">V7.4.2.1</b><p>Repair reconstruction. Kept as audit artifact, not promoted as clean evidence.</p></div><div class="event"><b class="red">V7.4.2.2</b><p>Append-only version exposed snapshot ingestion issues and massive NO_FILL distortion.</p></div><div class="event"><b class="cyan">V7.4.2.3</b><p>Snapshot-first causal engine. Proved old raw acquisition was arriving too late.</p></div><div class="event"><b class="violet">V7.5.0</b><p>Burst-resilient acquisition. Queue can now drain bursts and return to sub-second operation.</p></div><div class="event"><b class="green">V7.5.2</b><p>Current clean arena. Automatically pauses whenever infrastructure leaves the frozen regime.</p></div></div></div>
<div class="panel"><div class="title">ACTIVE PROCESSES</div><div id="processes"></div></div></div><div id="toast" class="toast"></div><script>
const $=id=>document.getElementById(id),F=(x,n=2)=>x==null?'—':Number(x).toFixed(n),P=x=>x==null?'—':(100*Number(x)).toFixed(1)+'%',esc=s=>String(s??'—').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));let lastState='';
function stars(){let c=$('stars'),x=c.getContext('2d'),dpr=devicePixelRatio||1;function rs(){c.width=innerWidth*dpr;c.height=innerHeight*dpr;c.style.width=innerWidth+'px';c.style.height=innerHeight+'px'}rs();addEventListener('resize',rs);let pts=Array.from({length:90},()=>({x:Math.random(),y:Math.random(),s:Math.random()*1.5+.2,v:Math.random()*.0003+.00005}));function f(){x.clearRect(0,0,c.width,c.height);for(let p of pts){p.y+=p.v;if(p.y>1)p.y=0;x.fillStyle='rgba(100,190,220,'+(.12+p.s*.12)+')';x.beginPath();x.arc(p.x*c.width,p.y*c.height,p.s*dpr,0,7);x.fill()}requestAnimationFrame(f)}f()}stars();
function drawLine(canvas,data){let c=$(canvas),x=c.getContext('2d'),w=c.clientWidth,h=c.clientHeight,dpr=devicePixelRatio||1;c.width=w*dpr;c.height=h*dpr;x.scale(dpr,dpr);x.clearRect(0,0,w,h);x.strokeStyle='#173544';x.lineWidth=1;for(let i=1;i<5;i++){x.beginPath();x.moveTo(0,h*i/5);x.lineTo(w,h*i/5);x.stroke()}if(!data.length)return;let max=Math.max(10,...data.map(z=>Number(z.pending_total||0)));let pts=data.map((z,i)=>[i/(data.length-1||1)*w,h-(Number(z.pending_total||0)/max)*(h-15)-8]);x.strokeStyle='#53dcea';x.lineWidth=2;x.beginPath();pts.forEach((p,i)=>i?x.lineTo(...p):x.moveTo(...p));x.stroke();x.fillStyle='#53dcea';x.fillText('pending HOT queue',8,14)}
function drawBars(a){let c=$('latencyBars'),x=c.getContext('2d'),w=c.clientWidth,h=c.clientHeight,dpr=devicePixelRatio||1;c.width=w*dpr;c.height=h*dpr;x.scale(dpr,dpr);x.clearRect(0,0,w,h);let vals=[['claim p90',a.claim_p90||0],['HTTP p90',a.http_p90||0],['total p90',a.p90||0],['total p95',a.p95||0]],max=Math.max(3,...vals.map(v=>v[1]));vals.forEach((v,i)=>{let y=12+i*22,bw=(w-110)*(v[1]/max);x.fillStyle='#112530';x.fillRect(92,y,bw||1,10);x.fillStyle=i>1?'#a88bff':'#53dcea';x.fillRect(92,y,bw,10);x.fillStyle='#90a6b1';x.font='9px ui-monospace';x.fillText(v[0],4,y+9);x.fillText(F(v[1],3)+'s',Math.min(w-44,98+bw),y+9)})}
function ruleCard(r){let done=Number(r.done||0),obs=Number(r.observed||0),late=Number(r.late_snapshot||0),fa=Number(r.feature_available||0),fu=Number(r.feature_unavailable||0),p=Math.min(100,done/30*100),st=r.status||'WAITING',col=st.includes('CONFIRMED')?'green':st.includes('FAILED')||st.includes('DECAY')?'red':'amber';let lateP=obs?100*late/obs:0,avail=obs?100*fa/obs:0;return `<div class="exp"><div class="expTop"><div class="expName">${esc(r.family)}</div><div class="badge ${col}">${esc(st)}</div></div><div class="ring" style="--p:${p*3.6}deg"><span>${done}/30</span></div><div class="mini"><div class="box"><small>OBSERVED</small><b>${obs}</b></div><div class="box"><small>LATE</small><b class="${lateP>20?'red':'green'}">${F(lateP,0)}%</b></div><div class="box"><small>FEATURE OK</small><b>${F(avail,0)}%</b></div></div><div class="story">${esc(r.feature)} @ ${r.stage_s}s · horizon ${r.horizon_s}s. The rule is frozen; only future observations may count.</div></div>`}
function expCards(d){let a=d.acq||{},ar=d.arena||{},di=d.discovery||{},pr=d.processes||[],has=n=>pr.some(p=>p.cmd.includes(n));let acq=a.recent||{},c1=acq.healthy?'green':'amber',c2=ar.live_healthy?'green':'red';return `<div class="exp"><div class="expTop"><div class="expName">V7.5.0 ACQUISITION</div><div class="badge ${has('v750_')?'green':'red'}">${has('v750_')?'RUNNING':'OFF'}</div></div><div class="ring" style="--p:${Math.min(360,(acq.p90?Math.max(0,1-acq.p90/2):0)*360)}deg"><span>${F(acq.p90,2)}s</span></div><div class="story">Makes raw HOT events causally available fast enough for 20–30 second strategies. Current recent p90 is the key heartbeat.</div></div><div class="exp"><div class="expTop"><div class="expName">V7.5.2 FUTURE ARENA</div><div class="badge ${c2}">${ar.live_healthy?'LIVE':'PAUSED'}</div></div><div class="ring" style="--p:${(ar.rules||[]).reduce((s,r)=>s+Math.min(30,Number(r.done||0)),0)/90*360}deg"><span>${(ar.rules||[]).reduce((s,r)=>s+Number(r.done||0),0)}</span></div><div class="story">Tests CAPITAL, FLOW and WALLET after freeze. It automatically pauses whenever infrastructure leaves the accepted latency regime.</div></div><div class="exp"><div class="expTop"><div class="expName">V7.4.3 DISCOVERY</div><div class="badge ${has('v743_')?'green':'amber'}">${has('v743_')?'RUNNING':'CHECK'}</div></div><div class="ring" style="--p:${Math.min(360,((di.counts||{}).ROBUST_POSITIVE||0)*30)}deg"><span>${(di.counts||{}).ROBUST_POSITIVE||0}</span></div><div class="story">Searches orthogonal mechanisms while active validation stays untouched. Discovery is screening, never proof.</div></div>`}
function brain(d){let ar=d.arena||{},a=d.acq?.recent||{},rules=ar.rules||[],cap=rules.find(r=>r.family==='CAPITAL_FLOW'),flow=rules.find(r=>r.family==='FLOW_DYNAMICS'),wal=rules.find(r=>r.family==='WALLET_STRUCTURE');let t=[];t.push(a.healthy?'Acquisition is currently inside the frozen latency regime.':'Acquisition is outside the accepted latency regime, so the arena should be paused.');if(cap)t.push(`CAPITAL has observed ${cap.observed||0} cases; ${cap.late_snapshot||0} were late.`);if(flow)t.push(`FLOW has ${flow.feature_available||0} usable features and ${flow.feature_unavailable||0} unavailable-at-first-seen features.`);if(wal)t.push(`WALLET has observed ${wal.observed||0} cases; ${wal.late_snapshot||0} were late.`);t.push('No challenger becomes evidence until it accumulates prospective DONE outcomes under the frozen rule.');return t.join(' ')}
function update(d){$('clock').textContent=new Date(d.now*1000).toLocaleTimeString();let a=d.acq?.recent||{},ar=d.arena||{},r=d.r64||{},di=d.discovery||{},qs=d.acq?.queue||{};let hp=a.healthy?92:ar.live_healthy?78:48;$('orb').innerHTML=`<div class="healthOrb" style="--hp:${hp*3.6}deg"><div class="in"><div class="score ${a.healthy?'green':'amber'}">${hp}</div><div class="lab">SYSTEM SCORE</div></div></div>`;$('healthText').textContent=a.healthy?'Current acquisition path is healthy. Future-only evidence may flow when all arena guardrails agree.':'The lab is protecting evidence by pausing when live infrastructure leaves the frozen regime.';$('mission').innerHTML=`<div class="mcard"><div class="ml">R64 CONTROL</div><div class="mv">${r.done||0} DONE</div></div><div class="mcard"><div class="ml">ARENA INTEGRITY</div><div class="mv ${Number(ar.integrity||0)===0?'green':'red'}">${ar.integrity??'—'}</div></div><div class="mcard"><div class="ml">HOT P90</div><div class="mv cyan">${F(a.p90,3)}s</div></div><div class="mcard"><div class="ml">DISCOVERY ROBUST+</div><div class="mv violet">${(di.counts||{}).ROBUST_POSITIVE||0}</div></div>`;$('experiments').innerHTML=expCards(d);$('acqKpi').innerHTML=`<div class="kpi"><small>PENDING</small><b>${qs.pending_total??'—'}</b></div><div class="kpi"><small>OLDEST</small><b>${F(qs.oldest_pending_age_s,1)}s</b></div><div class="kpi"><small>RPS</small><b>${F(qs.current_rps,0)}</b></div><div class="kpi"><small>RECENT HOT</small><b>${a.n||0}</b></div>`;drawLine('infraChart',d.acq?.series||[]);drawBars(a);$('infraExplain').textContent=a.healthy?'The recent HOT path is within the scientific gate: no backlog, low p90/p95, and enough fresh samples.':'The current window breached at least one health gate. The arena should not count new evidence until the path returns to the frozen regime.';$('rules').innerHTML=(ar.rules||[]).map(ruleCard).join('')||'<div class="story">Arena database unavailable.</div>';let c=di.counts||{};$('discoveryKpi').innerHTML=`<div class="kpi"><small>COLLECTING</small><b class="cyan">${c.COLLECTING||0}</b></div><div class="kpi"><small>ROBUST +</small><b class="green">${c.ROBUST_POSITIVE||0}</b></div><div class="kpi"><small>REJECT</small><b class="red">${c.REJECT||0}</b></div><div class="kpi"><small>ERROR</small><b>${c.ERROR||0}</b></div>`;let fam=di.families||[],mx=Math.max(1,...fam.map(x=>Number(x.independent_instances||0)));$('families').innerHTML=fam.map(x=>`<div class="fam"><b>${esc(x.family)}</b><div class="track"><i style="width:${100*Number(x.independent_instances||0)/mx}%"></i></div><span class="${x.status==='SHORTLIST'?'green':'amber'}">${esc(x.status)}</span></div>`).join('');$('r64').innerHTML=`<div class="mission"><div class="mcard"><div class="ml">DONE</div><div class="mv">${r.done||0}</div></div><div class="mcard"><div class="ml">EXPECTANCY</div><div class="mv ${Number(r.expectancy)>0?'green':'red'}">${F(r.expectancy)}%</div></div><div class="mcard"><div class="ml">PF</div><div class="mv">${F(r.profit_factor)}</div></div><div class="mcard"><div class="ml">FILL</div><div class="mv cyan">${P(r.fill_rate)}</div></div></div>`;$('brain').textContent=brain(d);$('processes').innerHTML=(d.processes||[]).map(p=>`<div class="proc">PID ${esc(p.pid)} · CPU ${esc(p.cpu)}% · ${esc(p.cmd)}</div>`).join('')||'<div class="proc">No matching processes found.</div>';let st=(a.healthy?'HEALTHY':'UNHEALTHY')+'|'+(ar.live_healthy?'LIVE':'PAUSED');if(lastState&&st!==lastState){let t=$('toast');t.textContent='STATE CHANGE → '+st;t.classList.add('on');setTimeout(()=>t.classList.remove('on'),3500)}lastState=st}
async function loop(){try{let d=await(await fetch('/api?'+Date.now(),{cache:'no-store'})).json();update(d)}catch(e){$('clock').textContent='API ERROR'}setTimeout(loop,1800)}loop();
</script></body></html>'''

class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path.startswith('/api'):b=json.dumps(collect(),separators=(',',':')).encode();ct='application/json'
  else:b=HTML.encode();ct='text/html; charset=utf-8'
  self.send_response(200);self.send_header('Content-Type',ct);self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def log_message(self,*a):pass

if __name__=='__main__':
 print(f'MEMECOIN LAB V7.6.1 cinematic observatory http://{HOST}:{PORT}',flush=True)
 ThreadingHTTPServer((HOST,PORT),H).serve_forever()
