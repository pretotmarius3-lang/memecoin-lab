#!/usr/bin/env python3
"""MEMECOIN LAB — RESEARCH MISSION CONTROL V7.6.6
Read-only dashboard for the current clean causal stack and preregistered validator.
No scientific state is mutated.
"""
from __future__ import annotations
import json, os, re, sqlite3, subprocess, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
HOST=os.environ.get('MEMECOIN_V766_HOST','127.0.0.1')
PORT=int(os.environ.get('MEMECOIN_V766_PORT','8801'))
FEATURE=ROOT/'v52_features.db'
VALID=ROOT/'v7656_future_regime_validation.db'
RESEARCH=ROOT/'research_v4_1.db'
LOG764=ROOT/'runtime_logs'/'v764.log'

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=3);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=3000');return d

def one(d,q,a=()):
 try:
  r=d.execute(q,a).fetchone();return dict(r) if r else None
 except:return None

def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []

def tail(path,n=80):
 try:return '\n'.join(path.read_text(errors='ignore').splitlines()[-n:])
 except:return ''

def procmap():
 wanted={'v764':'v764_lean_burst_acquisition.py','v7602':'v7602_rowid_cursor_canonical_decoder.py','v7611':'v7611_lightweight_causal_scheduler.py','v766':'v766_research_mission_control.py'}
 out={k:{'alive':False} for k in wanted}
 try:
  ls=subprocess.run(['ps','aux'],capture_output=True,text=True,timeout=2).stdout.splitlines()
  for ln in ls:
   for k,needle in wanted.items():
    if needle in ln and 'grep' not in ln:
     p=ln.split();out[k]={'alive':True,'pid':p[1] if len(p)>1 else '?','cpu':p[2] if len(p)>2 else '?'}
 except:pass
 return out

def collect_stack():
 out={'decoder':{},'scheduler':{},'activation':None,'causal_recent':0,'lag_p90':None}
 if not FEATURE.exists():return out
 try:
  d=ro(FEATURE);now=time.time();dec=one(d,'SELECT * FROM v7602_decoder_state WHERE id=1') or {};sch=one(d,'SELECT * FROM v7611_scheduler_state WHERE id=1') or {}
  rs=rows(d,'SELECT build_lag_s FROM v7611_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-60,));d.close()
  xs=sorted(float(r['build_lag_s']) for r in rs if r.get('build_lag_s') is not None)
  p90=None
  if xs:
   pos=(len(xs)-1)*.9;lo=int(pos);hi=min(len(xs)-1,lo+1);f=pos-lo;p90=xs[lo]+(xs[hi]-xs[lo])*f
  out={'decoder':dec,'scheduler':sch,'activation':sch.get('activation_observed_at'),'causal_recent':len(xs),'lag_p90':p90}
 except Exception as e:out['error']=repr(e)
 return out

def collect_acq_log():
 txt=tail(LOG764,120);o={'epoch':None,'pending':None,'oldest':None,'rps':None,'err':None,'creates':None,'hot_logs':None,'enq':None,'ok':None}
 me=re.findall(r'epoch=(A517_[^\s]+)',txt)
 if me:o['epoch']=me[-1]
 qs=re.findall(r'rps=([0-9.]+) pending=(\d+).*?oldest=([0-9.]+)s',txt)
 if qs:o['rps']=float(qs[-1][0]);o['pending']=int(qs[-1][1]);o['oldest']=float(qs[-1][2])
 cs=re.findall(r'ALCHEMY517 creates=([\d,]+).*?hot_logs=([\d,]+).*?enq=([\d,]+).*?ok=([\d,]+).*?err=(\d+)',txt)
 if cs:
  a=cs[-1];o.update(creates=int(a[0].replace(',','')),hot_logs=int(a[1].replace(',','')),enq=int(a[2].replace(',','')),ok=int(a[3].replace(',','')),err=int(a[4]))
 return o

def collect_validator():
 out={'available':False,'rows':[]}
 if not VALID.exists():return out
 try:
  d=ro(VALID);run=one(d,'SELECT * FROM run WHERE id=1');obs=rows(d,'SELECT * FROM future_obs ORDER BY t30');d.close()
  low=[z for z in obs if int(z['low_regime'])==1];hi=[z for z in low if int(z['high_slope'])==1];rest=[z for z in low if int(z['high_slope'])==0]
  def st(x):
   if not x:return {'n':0,'mean':None,'median':None,'capped':None,'hit':None}
   vals=[float(z['future']) for z in x];sv=sorted(vals);med=sv[len(sv)//2] if len(sv)%2 else (sv[len(sv)//2-1]+sv[len(sv)//2])/2
   return {'n':len(vals),'mean':sum(vals)/len(vals),'median':med,'capped':sum(max(-100,min(100,v)) for v in vals)/len(vals),'hit':sum(v>0 for v in vals)/len(vals)}
  a,b=st(hi),st(rest);target_low=int(run['target_low']);target_each=int(run['target_high']);enough=len(low)>=target_low and len(hi)>=target_each and len(rest)>=target_each
  status='ACCUMULATING_FUTURE_EVIDENCE'
  if enough:status='FUTURE_SIGNAL_SURVIVES' if (a['median']>b['median'] and a['capped']>b['capped'] and a['hit']>=b['hit']) else 'FUTURE_SIGNAL_FAILS'
  return {'available':True,'run':run,'total':len(obs),'low':len(low),'high_regime':len(obs)-len(low),'high':a,'rest':b,'status':status,'enough':enough,'recent':obs[-12:]}
 except Exception as e:return {'available':False,'error':repr(e)}

def collect_r64():
 if not RESEARCH.exists():return {}
 try:
  d=ro(RESEARCH);r=one(d,'SELECT * FROM v64_forward_summary ORDER BY updated_at DESC LIMIT 1') or {};d.close();return r
 except:return {}

def collect():
 p=procmap();stack=collect_stack();acq=collect_acq_log();val=collect_validator();r64=collect_r64()
 healthy=bool(p['v764']['alive'] and p['v7602']['alive'] and p['v7611']['alive'] and (acq.get('pending') in (0,None)) and int(stack.get('decoder',{}).get('backlog_est') or 0)<=2)
 return {'now':time.time(),'processes':p,'stack':stack,'acq':acq,'validator':val,'r64':r64,'healthy':healthy}

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab // Research Mission Control</title><style>
:root{--bg:#03060a;--p:#07121b;--p2:#0a1822;--line:#183849;--txt:#eef8fb;--muted:#79939f;--g:#54eca2;--c:#50d8e8;--v:#a888ff;--a:#ffbd59;--r:#ff657c}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% -10%,#17354a 0,#050a0e 34%,#020406 72%);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.scan{position:fixed;inset:0;pointer-events:none;background:linear-gradient(transparent 49%,#fff4 50%,transparent 51%);background-size:100% 7px;opacity:.04}.wrap{max-width:1800px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;align-items:center;gap:20px}.brand{font:950 25px ui-monospace;letter-spacing:.12em}.sub{font:800 10px ui-monospace;letter-spacing:.18em;color:var(--c)}.clock{font:800 10px ui-monospace;color:var(--muted);text-align:right}.live{color:var(--g)}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px;margin-top:14px}.panel{background:linear-gradient(180deg,#091721ee,#050d13f4);border:1px solid var(--line);border-radius:17px;padding:15px;box-shadow:0 18px 50px #0007;position:relative;overflow:hidden}.hero{grid-column:span 8}.health{grid-column:span 4}.pipelineP{grid-column:span 12}.validator{grid-column:span 8}.benchmark{grid-column:span 4}.activity{grid-column:span 12}.kicker{font:900 9px ui-monospace;letter-spacing:.15em;color:#91aab5}.mega{font-size:34px;font-weight:950;line-height:1.05;margin-top:8px}.mega span{background:linear-gradient(90deg,#fff,var(--c),var(--v));-webkit-background-clip:text;color:transparent}.lead{color:#9db1ba;max-width:900px;line-height:1.5;font-size:13px}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:15px}.card{border:1px solid #173746;border-radius:11px;background:#07141c;padding:10px}.card small{font:8px ui-monospace;color:var(--muted)}.card b{display:block;font:950 19px ui-monospace;margin-top:4px}.green{color:var(--g)}.cyan{color:var(--c)}.violet{color:var(--v)}.amber{color:var(--a)}.red{color:var(--r)}.orb{--p:100%;width:190px;height:190px;border-radius:50%;margin:12px auto;display:grid;place-items:center;background:conic-gradient(var(--g) var(--p),#102631 0);position:relative;filter:drop-shadow(0 0 25px #54eca222);animation:float 4s ease-in-out infinite}.orb:before{content:"";position:absolute;inset:14px;border-radius:50%;background:#061018;border:1px solid #224b5d}.orb:after{content:"";position:absolute;inset:33px;border-radius:50%;border:1px dashed #3a6c80;animation:spin 12s linear infinite}.orb div{z-index:2;text-align:center}.orb b{display:block;font:950 28px ui-monospace}.orb small{font:800 9px ui-monospace;color:var(--muted)}.pipeline{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;position:relative}.stage{min-height:120px;border:1px solid #183848;border-radius:13px;background:#07141c;padding:13px;text-align:center;position:relative}.stage:not(:last-child):after{content:'→';position:absolute;right:-16px;top:48px;color:#3c7085;font-weight:950}.stage .ico{font-size:27px}.stage b{display:block;font:900 10px ui-monospace;margin-top:9px}.stage em{display:block;font-style:normal;color:var(--muted);font-size:9px;line-height:1.45;margin-top:6px}.dot{width:8px;height:8px;border-radius:50%;background:var(--c);box-shadow:0 0 16px var(--c);position:absolute;top:55px;left:2%;animation:travel 7s linear infinite;z-index:5}.progress{height:11px;background:#102630;border-radius:99px;overflow:hidden;margin-top:10px}.progress i{display:block;height:100%;background:linear-gradient(90deg,var(--v),var(--c),var(--g));transition:width .8s}.twocol{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}.metric{border:1px solid #183746;border-radius:12px;background:#07141c;padding:11px}.metric h3{font:900 10px ui-monospace;margin:0 0 9px;color:#aabcc4}.row{display:flex;justify-content:space-between;gap:12px;padding:5px 0;border-bottom:1px solid #102733;font:10px ui-monospace}.row:last-child{border-bottom:0}.status{display:inline-block;border:1px solid currentColor;border-radius:999px;padding:5px 9px;font:900 8px ui-monospace}.timeline{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.event{border:1px solid #173746;border-radius:11px;background:#07141c;padding:10px;min-height:95px}.event b{font:900 9px ui-monospace}.event p{font-size:9px;color:#8ca2ac;line-height:1.4}.proc{font:9px ui-monospace;color:#8fa8b3;margin-top:6px}.note{border-left:3px solid var(--c);padding-left:10px;font-size:11px;color:#a9bec7;line-height:1.5}.pulse{animation:pulse 1.4s infinite}@keyframes spin{to{transform:rotate(360deg)}}@keyframes float{50%{transform:translateY(-6px)}}@keyframes pulse{50%{opacity:.45}}@keyframes travel{from{left:2%}to{left:96%}}@media(max-width:1000px){.hero,.health,.validator,.benchmark,.pipelineP,.activity{grid-column:span 12}.pipeline{grid-template-columns:1fr}.stage:after,.dot{display:none}.cards,.timeline{grid-template-columns:repeat(2,1fr)}}
</style></head><body><div class="scan"></div><div class="wrap"><div class="top"><div><div class="sub">MEMECOIN LAB // V7.6.6</div><div class="brand">RESEARCH MISSION CONTROL</div></div><div class="clock"><span class="live">● LIVE</span><br><span id="clock">--</span></div></div><div class="grid"><section class="panel hero"><div class="kicker">WHAT IS HAPPENING RIGHT NOW</div><div class="mega">From raw Solana events to a <span>future-only falsifiable alpha test.</span></div><p class="lead">The infrastructure is now separated from the research question. Acquisition, decoding and causal snapshots are monitored independently. The active scientific experiment is preregistered: historical discovery data can no longer rescue or retune the rule.</p><div class="cards"><div class="card"><small>ACQUISITION QUEUE</small><b id="pending">--</b></div><div class="card"><small>DECODER BACKLOG</small><b id="backlog">--</b></div><div class="card"><small>CAUSAL BUILD P90</small><b id="lag">--</b></div><div class="card"><small>FUTURE OBS</small><b id="futureN">--</b></div></div></section><section class="panel health"><div class="kicker">STACK HEALTH</div><div class="orb" id="orb"><div><b id="healthTxt">--</b><small>CAUSAL PIPELINE</small></div></div><div class="proc" id="procs"></div></section><section class="panel pipelineP"><div class="kicker">LIVE DATA JOURNEY</div><div class="pipeline"><div class="dot"></div><div class="stage"><div class="ico">◎</div><b>SOLANA / ALCHEMY</b><em id="acqTxt">live provider feed</em></div><div class="stage"><div class="ico">⇣</div><b>V764 LEAN ACQUISITION</b><em id="v764Txt">trace-free hot path</em></div><div class="stage"><div class="ico">⚙</div><b>V7602 DECODER</b><em id="v7602Txt">rowid cursor canonicalization</em></div><div class="stage"><div class="ico">◈</div><b>V7611 CAUSAL SCHEDULER</b><em id="v7611Txt">T+5 / T+20 / T+30 snapshots</em></div><div class="stage"><div class="ico">⌁</div><b>V7656 FUTURE VALIDATOR</b><em>fixed LOW-regime + rolling Q70 slope rule</em></div></div></section><section class="panel validator"><div class="kicker">ACTIVE PREREGISTERED EXPERIMENT</div><div style="display:flex;justify-content:space-between;gap:10px;align-items:center;margin-top:8px"><div><b style="font:950 17px ui-monospace">LOW REGIME × HIGH RETURN SLOPE</b><div style="color:#8ea5b0;font-size:10px;margin-top:5px">Only post-cutoff observations count. Target: 20 LOW-regime observations, including ≥10 HIGH and ≥10 REST.</div></div><span id="valStatus" class="status amber">ACCUMULATING</span></div><div class="progress"><i id="prog" style="width:0%"></i></div><div class="cards"><div class="card"><small>LOW REGIME</small><b id="lowN">0 / 20</b></div><div class="card"><small>HIGH SLOPE</small><b id="hiN">0 / 10</b></div><div class="card"><small>REST</small><b id="restN">0 / 10</b></div><div class="card"><small>HIGH REGIME</small><b id="highReg">0</b></div></div><div class="twocol"><div class="metric"><h3>HIGH SLOPE — FUTURE ONLY</h3><div class="row"><span>mean</span><b id="hiMean">--</b></div><div class="row"><span>median</span><b id="hiMed">--</b></div><div class="row"><span>capped mean</span><b id="hiCap">--</b></div><div class="row"><span>hit rate</span><b id="hiHit">--</b></div></div><div class="metric"><h3>REST — FUTURE ONLY</h3><div class="row"><span>mean</span><b id="rMean">--</b></div><div class="row"><span>median</span><b id="rMed">--</b></div><div class="row"><span>capped mean</span><b id="rCap">--</b></div><div class="row"><span>hit rate</span><b id="rHit">--</b></div></div></div><p class="note" id="cutoffNote">Waiting for validator state…</p></section><section class="panel benchmark"><div class="kicker">EXTERNAL BENCHMARK</div><div style="font:950 17px ui-monospace;margin-top:9px">R64 FORWARD</div><div class="cards" style="grid-template-columns:1fr 1fr"><div class="card"><small>DONE</small><b id="r64done">--</b></div><div class="card"><small>EXPECTANCY</small><b id="r64exp">--</b></div><div class="card"><small>PF</small><b id="r64pf">--</b></div><div class="card"><small>FILL</small><b id="r64fill">--</b></div></div><p class="note">R64 stays external. It is a benchmark, not an input used to select or rescue the active hypothesis.</p></section><section class="panel activity"><div class="kicker">RESEARCH STORY — OUTSIDER VIEW</div><div class="timeline"><div class="event"><b>1. INFRA FIXED</b><p>Historical 30–60s delays were traced through acquisition and decoding. The current stack now keeps decoder backlog near zero and causal build lag sub-second.</p></div><div class="event"><b>2. VARIANCE PATH DISCOVERY</b><p>Early slope/variance looked powerful in aggregate, but temporal tests showed heavy outlier and regime dependence.</p></div><div class="event"><b>3. REGIME HYPOTHESIS</b><p>The surviving research idea is conditional: high early slope may only matter when the rolling prior-slope regime is LOW.</p></div><div class="event"><b>4. FUTURE-ONLY GATE</b><p>V7656 freezes that structure and accepts only new post-cutoff outcomes. No retuning from historical winners is allowed.</p></div></div></section></div></div><script>
const f=(x,d=2)=>x==null?'--':Number(x).toFixed(d);const pct=x=>x==null?'--':(100*Number(x)).toFixed(1)+'%';
async function load(){try{const d=await fetch('/api').then(r=>r.json());document.getElementById('clock').textContent=new Date(d.now*1000).toLocaleString();const a=d.acq||{},s=d.stack||{},v=d.validator||{},p=d.processes||{};pending.textContent=a.pending==null?'--':a.pending;pending.className=(a.pending===0?'green':'amber');backlog.textContent=s.decoder&&s.decoder.backlog_est!=null?s.decoder.backlog_est:'--';backlog.className=((s.decoder&&Number(s.decoder.backlog_est)<=2)?'green':'amber');lag.textContent=s.lag_p90==null?'--':f(s.lag_p90,3)+'s';lag.className=(s.lag_p90!=null&&s.lag_p90<1?'green':'amber');futureN.textContent=v.total??0;healthTxt.textContent=d.healthy?'HEALTHY':'CHECK';orb.style.setProperty('--p',d.healthy?'100%':'45%');orb.style.background=`conic-gradient(${d.healthy?'var(--g)':'var(--a)'} var(--p),#102631 0)`;procs.innerHTML=['v764','v7602','v7611'].map(k=>`${k.toUpperCase()} ${p[k]&&p[k].alive?'● PID '+p[k].pid:'○ OFF'}`).join('<br>');acqTxt.textContent=`epoch ${a.epoch||'--'} · rps ${a.rps??'--'} · err ${a.err??'--'}`;v764Txt.textContent=`pending ${a.pending??'--'} · oldest ${a.oldest??'--'}s · enq ${a.enq??'--'}`;v7602Txt.textContent=`backlog ${s.decoder&&s.decoder.backlog_est!=null?s.decoder.backlog_est:'--'} · cursor ${s.decoder&&s.decoder.cursor_rowid!=null?s.decoder.cursor_rowid:'--'}`;v7611Txt.textContent=`phase ${s.scheduler&&s.scheduler.phase||'--'} · causal ${s.scheduler&&s.scheduler.causal_inserted!=null?s.scheduler.causal_inserted:'--'} · p90 ${s.lag_p90==null?'--':f(s.lag_p90,3)+'s'}`;lowN.textContent=`${v.low||0} / 20`;hiN.textContent=`${v.high&&v.high.n||0} / 10`;restN.textContent=`${v.rest&&v.rest.n||0} / 10`;highReg.textContent=v.high_regime||0;const prog=Math.min(100,Math.min((v.low||0)/20,(v.high&&v.high.n||0)/10,(v.rest&&v.rest.n||0)/10)*100);document.getElementById('prog').style.width=prog+'%';valStatus.textContent=(v.status||'WAITING').replaceAll('_',' ');valStatus.className='status '+(v.status==='FUTURE_SIGNAL_SURVIVES'?'green':v.status==='FUTURE_SIGNAL_FAILS'?'red':'amber');const H=v.high||{},R=v.rest||{};hiMean.textContent=H.mean==null?'--':f(H.mean,2)+'%';hiMed.textContent=H.median==null?'--':f(H.median,2)+'%';hiCap.textContent=H.capped==null?'--':f(H.capped,2)+'%';hiHit.textContent=pct(H.hit);rMean.textContent=R.mean==null?'--':f(R.mean,2)+'%';rMed.textContent=R.median==null?'--':f(R.median,2)+'%';rCap.textContent=R.capped==null?'--':f(R.capped,2)+'%';rHit.textContent=pct(R.hit);cutoffNote.textContent=v.run?`Frozen cutoff T+30 > ${Number(v.run.cutoff_t30).toFixed(3)}. Historical discovery outcomes are excluded from this scoreboard.`:'Validator DB not available yet.';const r=d.r64||{};r64done.textContent=r.done??'--';r64exp.textContent=r.expectancy==null?'--':f(r.expectancy,2)+'%';r64pf.textContent=r.profit_factor==null?'--':f(r.profit_factor,2);r64fill.textContent=r.fill_rate==null?'--':pct(r.fill_rate);}catch(e){console.error(e)}}load();setInterval(load,2000);
</script></body></html>'''

class H(BaseHTTPRequestHandler):
 def log_message(self,*_):pass
 def do_GET(self):
  if self.path=='/api':
   b=json.dumps(collect(),default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(b);return
  if self.path in ('/','/index.html'):
   b=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(b);return
  self.send_response(404);self.end_headers()

if __name__=='__main__':
 print(f'MEMECOIN LAB V7.6.6 RESEARCH MISSION CONTROL http://{HOST}:{PORT}',flush=True)
 ThreadingHTTPServer((HOST,PORT),H).serve_forever()
