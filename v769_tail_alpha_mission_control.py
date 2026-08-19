#!/usr/bin/env python3
"""MEMECOIN LAB — TAIL-ALPHA MISSION CONTROL V7.6.9
Read-only outsider-first dashboard for the clean causal stack + V768 future-only validator.
"""
from __future__ import annotations
import json, os, re, sqlite3, subprocess, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
HOST=os.environ.get('MEMECOIN_V769_HOST','127.0.0.1')
PORT=int(os.environ.get('MEMECOIN_V769_PORT','8802'))
FEATURE=ROOT/'v52_features.db'; VALID=ROOT/'v768_tail_alpha_validation.db'; OLD=ROOT/'v7656_future_regime_validation.db'; LOG764=ROOT/'runtime_logs'/'v764.log'

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=3);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');return d

def one(d,q,a=()):
 try:
  r=d.execute(q,a).fetchone();return dict(r) if r else None
 except:return None

def rows(d,q,a=()):
 try:return [dict(x) for x in d.execute(q,a).fetchall()]
 except:return []

def tail(path,n=120):
 try:return '\n'.join(path.read_text(errors='ignore').splitlines()[-n:])
 except:return ''

def pct(xs,q):
 if not xs:return None
 s=sorted(xs);p=(len(s)-1)*q;lo=int(p);hi=min(len(s)-1,lo+1);f=p-lo;return s[lo]+(s[hi]-s[lo])*f

def procmap():
 wanted={'v764':'v764_lean_burst_acquisition.py','v7602':'v7602_rowid_cursor_canonical_decoder.py','v7611':'v7611_lightweight_causal_scheduler.py','v769':'v769_tail_alpha_mission_control.py'}
 out={k:{'alive':False} for k in wanted}
 try:
  for ln in subprocess.run(['ps','aux'],capture_output=True,text=True,timeout=2).stdout.splitlines():
   for k,n in wanted.items():
    if n in ln and 'grep' not in ln:
     p=ln.split();out[k]={'alive':True,'pid':p[1],'cpu':p[2]}
 except:pass
 return out

def stack():
 o={'decoder':{},'scheduler':{},'recent_n':0,'p90':None}
 if not FEATURE.exists():return o
 try:
  d=ro(FEATURE);now=time.time();o['decoder']=one(d,'SELECT * FROM v7602_decoder_state WHERE id=1') or {};o['scheduler']=one(d,'SELECT * FROM v7611_scheduler_state WHERE id=1') or {}
  xs=[float(r['build_lag_s']) for r in rows(d,'SELECT build_lag_s FROM v7611_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-60,)) if r.get('build_lag_s') is not None];d.close();o['recent_n']=len(xs);o['p90']=pct(xs,.9)
 except Exception as e:o['error']=repr(e)
 return o

def acq():
 t=tail(LOG764);o={'epoch':None,'pending':None,'oldest':None,'rps':None,'err':None,'hot_logs':None}
 m=re.findall(r'epoch=(A517_[^\s]+)',t)
 if m:o['epoch']=m[-1]
 q=re.findall(r'rps=([0-9.]+) pending=(\d+).*?oldest=([0-9.]+)s',t)
 if q:o.update(rps=float(q[-1][0]),pending=int(q[-1][1]),oldest=float(q[-1][2]))
 z=re.findall(r'hot_logs=([\d,]+).*?err=(\d+)',t)
 if z:o.update(hot_logs=int(z[-1][0].replace(',','')),err=int(z[-1][1]))
 return o

def stats(vals,w,d):
 if not vals:return None
 return {'n':len(vals),'mean':sum(vals)/len(vals),'median':pct(vals,.5),'cap':sum(max(-100,min(100,x)) for x in vals)/len(vals),'hit':sum(x>=w for x in vals)/len(vals),'down':sum(x<=d for x in vals)/len(vals)}

def validator():
 if not VALID.exists():return {'available':False}
 try:
  d=ro(VALID);r=one(d,'SELECT * FROM run WHERE id=1');obs=rows(d,'SELECT * FROM future_obs ORDER BY t30');d.close()
  if not r:return {'available':False}
  sel=[float(x['future']) for x in obs if int(x['selected'])==1];base=[float(x['future']) for x in obs];a=stats(sel,float(r['winner_th']),float(r['downside_th']));b=stats(base,float(r['winner_th']),float(r['downside_th']))
  enough=len(sel)>=int(r['target_selected']) and len(base)>=int(r['target_total']);status='ACCUMULATING_FUTURE_EVIDENCE'
  if enough:status='TAIL_ALPHA_SURVIVES' if (a and b and a['hit']>b['hit'] and a['cap']>b['cap'] and a['down']<=b['down']) else 'TAIL_ALPHA_FAILS'
  return {'available':True,'run':r,'total':len(base),'selected':len(sel),'sel':a,'base':b,'status':status,'recent':obs[-10:]}
 except Exception as e:return {'available':False,'error':repr(e)}

def old_fail():
 if not OLD.exists():return {}
 try:
  d=ro(OLD);obs=rows(d,'SELECT * FROM future_obs');d.close();return {'n':len(obs),'status':'FAILED'}
 except:return {}

def collect():
 p=procmap();s=stack();a=acq();v=validator();healthy=bool(p['v764']['alive'] and p['v7602']['alive'] and p['v7611']['alive'] and int(s.get('decoder',{}).get('backlog_est') or 0)<=2 and a.get('pending') in (0,None))
 return {'now':time.time(),'processes':p,'stack':s,'acq':a,'validator':v,'old':old_fail(),'healthy':healthy}

HTML=r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V7.6.9</title><style>
:root{--bg:#020509;--p:#07131b;--line:#193947;--txt:#eef7fb;--m:#7f98a3;--g:#59efa7;--c:#52d9e9;--v:#aa8dff;--a:#ffbd59;--r:#ff667d}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% -15%,#16374c 0,#050a0f 36%,#020406 75%);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.wrap{max-width:1780px;margin:auto;padding:22px}.top{display:flex;justify-content:space-between;align-items:center}.sub{font:800 10px ui-monospace;color:var(--c);letter-spacing:.18em}.brand{font:950 25px ui-monospace;letter-spacing:.12em}.clock{font:800 10px ui-monospace;color:var(--m);text-align:right}.live{color:var(--g)}.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px;margin-top:15px}.panel{background:linear-gradient(180deg,#091721ef,#050d13f5);border:1px solid var(--line);border-radius:17px;padding:15px;box-shadow:0 18px 55px #0007}.hero{grid-column:span 8}.health{grid-column:span 4}.pipeP{grid-column:span 12}.val{grid-column:span 8}.hist{grid-column:span 4}.kicker{font:900 9px ui-monospace;color:#94aab5;letter-spacing:.15em}.mega{font-size:34px;font-weight:950;line-height:1.05;margin-top:8px}.mega span{background:linear-gradient(90deg,#fff,var(--c),var(--v));-webkit-background-clip:text;color:transparent}.lead{font-size:13px;line-height:1.5;color:#9fb3bc}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}.card,.metric{border:1px solid #183745;border-radius:11px;background:#07141c;padding:10px}.card small,.metric small{font:8px ui-monospace;color:var(--m)}.card b,.metric b{display:block;font:950 19px ui-monospace;margin-top:4px}.green{color:var(--g)}.red{color:var(--r)}.amber{color:var(--a)}.cyan{color:var(--c)}.orb{width:185px;height:185px;border-radius:50%;margin:12px auto;display:grid;place-items:center;background:conic-gradient(var(--g) 100%,#102630 0);position:relative;filter:drop-shadow(0 0 25px #59efa722)}.orb:before{content:"";position:absolute;inset:14px;border-radius:50%;background:#061018;border:1px solid #224b5d}.orb div{z-index:2;text-align:center}.orb b{display:block;font:950 28px ui-monospace}.orb small{font:800 9px ui-monospace;color:var(--m)}.pipeline{display:grid;grid-template-columns:repeat(5,1fr);gap:10px}.stage{border:1px solid #183848;border-radius:13px;background:#07141c;padding:13px;text-align:center;min-height:112px;position:relative}.stage:not(:last-child):after{content:'→';position:absolute;right:-16px;top:45px;color:#3d7085;font-weight:950}.stage .ico{font-size:25px}.stage b{display:block;font:900 10px ui-monospace;margin-top:8px}.stage em{display:block;font-style:normal;color:var(--m);font-size:9px;line-height:1.45;margin-top:5px}.progress{height:11px;background:#102630;border-radius:99px;overflow:hidden;margin:10px 0}.progress i{display:block;height:100%;background:linear-gradient(90deg,var(--v),var(--c),var(--g));transition:width .7s}.twocol{display:grid;grid-template-columns:1fr 1fr;gap:10px}.row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #112933;font:10px ui-monospace}.row:last-child{border:0}.status{display:inline-block;padding:5px 9px;border:1px solid currentColor;border-radius:99px;font:900 8px ui-monospace}.note{border-left:3px solid var(--c);padding-left:10px;color:#a9bdc6;font-size:11px;line-height:1.5}.proc{font:9px ui-monospace;color:#8fa6b2;margin-top:6px}@media(max-width:1000px){.hero,.health,.pipeP,.val,.hist{grid-column:span 12}.pipeline{grid-template-columns:1fr}.stage:after{display:none}.cards{grid-template-columns:repeat(2,1fr)}}</style></head><body><div class="wrap"><div class="top"><div><div class="sub">MEMECOIN LAB // V7.6.9</div><div class="brand">TAIL-ALPHA MISSION CONTROL</div></div><div class="clock"><span class="live">● LIVE</span><br><span id="clock">--</span></div></div><div class="grid"><section class="panel hero"><div class="kicker">ACTIVE SCIENTIFIC QUESTION</div><div class="mega">Can <span>capital growth + low wallet concentration</span> predict the right tail?</div><p class="lead">The prior slope/regime hypothesis failed prospectively. This branch is separate and preregistered. It asks whether one fixed T+30 condition raises the probability of large winners without worsening severe downside.</p><div class="cards"><div class="card"><small>ACQ QUEUE</small><b id="pending">--</b></div><div class="card"><small>DECODER BACKLOG</small><b id="backlog">--</b></div><div class="card"><small>CAUSAL P90</small><b id="lag">--</b></div><div class="card"><small>FUTURE OBS</small><b id="future">--</b></div></div></section><section class="panel health"><div class="kicker">STACK HEALTH</div><div class="orb"><div><b id="health">--</b><small>CAUSAL PIPELINE</small></div></div><div class="proc" id="procs"></div></section><section class="panel pipeP"><div class="kicker">LIVE DATA JOURNEY</div><div class="pipeline"><div class="stage"><div class="ico">◎</div><b>SOLANA / ALCHEMY</b><em id="a1">provider feed</em></div><div class="stage"><div class="ico">⇣</div><b>V764 ACQUISITION</b><em id="a2">lean hot path</em></div><div class="stage"><div class="ico">⚙</div><b>V7602 DECODER</b><em id="a3">rowid cursor</em></div><div class="stage"><div class="ico">◈</div><b>V7611 CAUSAL</b><em id="a4">T+ snapshots</em></div><div class="stage"><div class="ico">⌁</div><b>V768 VALIDATOR</b><em id="a5">future-only tail test</em></div></div></section><section class="panel val"><div class="kicker">PREREGISTERED TAIL-ALPHA VALIDATOR</div><div style="margin-top:9px"><span class="status amber" id="status">--</span></div><p class="note">FIXED RULE: gross_growth ≥ 1.142 AND wallet_top1_share ≤ 0.451. Winner ≥ +25%; severe downside ≤ -50%. Verdict requires ≥30 selected and ≥60 total future observations.</p><div class="cards"><div class="card"><small>TOTAL / 60</small><b id="tot">0</b><div class="progress"><i id="pt"></i></div></div><div class="card"><small>SELECTED / 30</small><b id="sel">0</b><div class="progress"><i id="ps"></i></div></div><div class="card"><small>WINNER UPLIFT</small><b id="wu">--</b></div><div class="card"><small>DOWNSIDE UPLIFT</small><b id="du">--</b></div></div><div class="twocol" style="margin-top:10px"><div class="metric"><small>SELECTED</small><div class="row"><span>capped mean</span><b id="sc">--</b></div><div class="row"><span>winner rate</span><b id="sw">--</b></div><div class="row"><span>downside rate</span><b id="sd">--</b></div></div><div class="metric"><small>BASELINE</small><div class="row"><span>capped mean</span><b id="bc">--</b></div><div class="row"><span>winner rate</span><b id="bw">--</b></div><div class="row"><span>downside rate</span><b id="bd">--</b></div></div></div></section><section class="panel hist"><div class="kicker">RESEARCH MEMORY</div><h3 style="font:950 15px ui-monospace">What already failed?</h3><p class="note">V7656 LOW-regime + Q70 slope failed in future-only validation. That branch stays closed; its data is not reused to rescue the current rule.</p><div class="metric"><small>PRIOR VALIDATOR</small><b class="red">FAILED</b><div class="row"><span>rows</span><span id="oldn">--</span></div></div><h3 style="font:950 15px ui-monospace;margin-top:14px">Why V768 is different</h3><p style="color:#94aab5;font-size:11px;line-height:1.5">The current hypothesis targets the distribution tail, not the median. It is judged by capped mean, +25% winner frequency, and ≤−50% downside frequency on a completely new post-cutoff cohort.</p></section></div></div><script>
const f=(x,n=2)=>x==null?'--':Number(x).toFixed(n),pc=x=>x==null?'--':(100*x).toFixed(1)+'%';
async function go(){try{let d=await (await fetch('/api')).json();clock.textContent=new Date(d.now*1000).toLocaleString();health.textContent=d.healthy?'HEALTHY':'CHECK';health.className=d.healthy?'green':'red';pending.textContent=d.acq.pending??'--';backlog.textContent=d.stack.decoder?.backlog_est??'--';lag.textContent=d.stack.p90==null?'--':f(d.stack.p90,3)+'s';procs.innerHTML=Object.entries(d.processes).map(([k,v])=>`${k.toUpperCase()} ${v.alive?'● PID '+v.pid:'○ OFF'}`).join('<br>');a1.textContent='epoch '+(d.acq.epoch||'--');a2.textContent=`pending=${d.acq.pending??'--'} · rps=${d.acq.rps??'--'}`;a3.textContent=`backlog=${d.stack.decoder?.backlog_est??'--'}`;a4.textContent=`recent=${d.stack.recent_n} · p90=${d.stack.p90==null?'--':f(d.stack.p90,3)+'s'}`;let v=d.validator;if(v.available){future.textContent=v.total;tot.textContent=v.total;sel.textContent=v.selected;pt.style.width=Math.min(100,100*v.total/60)+'%';ps.style.width=Math.min(100,100*v.selected/30)+'%';status.textContent=v.status;status.className='status '+(v.status==='TAIL_ALPHA_SURVIVES'?'green':v.status==='TAIL_ALPHA_FAILS'?'red':'amber');a5.textContent=v.status.replaceAll('_',' ');if(v.sel&&v.base){sc.textContent=f(v.sel.cap)+'%';sw.textContent=pc(v.sel.hit);sd.textContent=pc(v.sel.down);bc.textContent=f(v.base.cap)+'%';bw.textContent=pc(v.base.hit);bd.textContent=pc(v.base.down);wu.textContent=(100*(v.sel.hit-v.base.hit)>=0?'+':'')+f(100*(v.sel.hit-v.base.hit),1)+'pp';du.textContent=(100*(v.sel.down-v.base.down)>=0?'+':'')+f(100*(v.sel.down-v.base.down),1)+'pp'}}else{future.textContent='0';status.textContent='WAITING FOR V768 DB'}oldn.textContent=d.old?.n??'--'}catch(e){health.textContent='API ERR';health.className='red'}}go();setInterval(go,2000);
</script></body></html>'''

class H(BaseHTTPRequestHandler):
 def do_GET(self):
  if self.path.startswith('/api'):
   b=json.dumps(collect(),default=str).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(b);return
  b=HTML.encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.end_headers();self.wfile.write(b)
 def log_message(self,*_):pass

if __name__=='__main__':
 print(f'MEMECOIN LAB V7.6.9 tail-alpha mission control http://{HOST}:{PORT}',flush=True)
 ThreadingHTTPServer((HOST,PORT),H).serve_forever()
