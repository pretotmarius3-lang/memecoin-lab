#!/usr/bin/env python3
"""Memecoin Lab V4.8 — live edge command room.
Read-only neon dashboard for champion ranking, trend, family allocation and research agenda.
"""
from __future__ import annotations
import html, json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
ROOT=Path.home()/"memecoin_lab"; RDB=ROOT/"research_v4_1.db"; V52=ROOT/"v52_features.db"
HOST=os.environ.get('MEMECOIN_V48_DASH_HOST','127.0.0.1'); PORT=int(os.environ.get('MEMECOIN_V48_DASH_PORT','8772'))
def esc(x): return html.escape('—' if x is None else str(x))
def dbopen(p):
    if not p.exists(): return None
    d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=10); d.row_factory=sqlite3.Row; d.execute('PRAGMA busy_timeout=10000'); return d
def pct(x):
    try:return f'{100*float(x):.1f}%'
    except:return '—'
def num(x,d=3):
    try:return f'{float(x):.{d}f}'
    except:return '—'
def collect():
    o={'now':time.time()}; d=dbopen(RDB)
    if d:
        o['signals']=[dict(r) for r in d.execute('SELECT * FROM v48_signal_rankings ORDER BY live_score DESC LIMIT 80')]
        o['ensembles']=[dict(r) for r in d.execute('SELECT * FROM v48_ensemble_rankings ORDER BY live_score DESC LIMIT 40')]
        o['families']=[dict(r) for r in d.execute('SELECT * FROM v48_family_budget ORDER BY budget_weight DESC')]
        o['agenda']=[dict(r) for r in d.execute("SELECT * FROM v48_research_agenda WHERE state='OPEN' ORDER BY priority,updated_at DESC LIMIT 30")]
        o['roles']={r['role']:r['n'] for r in d.execute('SELECT role,COUNT(*) n FROM v48_signal_rankings GROUP BY role')}
        d.close()
    v=dbopen(V52)
    if v:
        o['tokens']=v.execute('SELECT COUNT(DISTINCT token_mint) FROM v52_swaps').fetchone()[0]
        o['swaps']=v.execute('SELECT COUNT(*) FROM v52_swaps').fetchone()[0]
        latest=v.execute('SELECT MAX(timestamp) FROM v52_swaps').fetchone()[0]; o['age']=None if latest is None else max(0,time.time()-float(latest)); v.close()
    return o
CSS='''
:root{--bg:#02050a;--p:#07111d;--l:#14324b;--c:#42f5ff;--g:#4dff9c;--y:#ffc857;--r:#ff5576;--m:#89a6ba;--w:#ecfbff}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 80% -10%,#0a3950 0,#05101a 28%,var(--bg) 66%);color:var(--w);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif}.wrap{padding:18px;max-width:1800px;margin:auto}h1{margin:0;font-size:31px;letter-spacing:.5px;text-shadow:0 0 24px #42f5ff55}.sub{color:var(--m);margin:4px 0 16px}.grid{display:grid;grid-template-columns:repeat(8,1fr);gap:9px}.card,.panel{background:linear-gradient(145deg,#081522ee,#030912f5);border:1px solid var(--l);border-radius:14px}.card{padding:13px}.k{font-size:10px;color:#7ca0b8;letter-spacing:1.2px}.v{font-size:28px;font-weight:850;margin-top:5px}.good{color:var(--g)}.warn{color:var(--y)}.bad{color:var(--r)}.panel{padding:15px;margin-top:11px}.panel h2{font-size:13px;letter-spacing:1.4px;color:#b9d9ea}.rank{display:grid;grid-template-columns:52px 1.3fr 1fr 100px 95px 95px 130px 110px;gap:8px;padding:9px;border-bottom:1px solid #11283a;align-items:center}.rank.head{color:#7897ac;font-size:10px}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.pill{display:inline-block;padding:3px 7px;border-radius:999px;font-size:10px;font-weight:800}.CHAMPION,.ACCELERATING{color:var(--g)}.CONTENDER,.STABLE{color:var(--c)}.DECAYING,.WATCH{color:var(--y)}.BROKEN,.RETIRE{color:var(--r)}.bar{height:5px;background:#10283a;border-radius:5px;overflow:hidden}.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--c),var(--g));box-shadow:0 0 10px var(--g)}.split{display:grid;grid-template-columns:1fr 1fr;gap:11px}.family{padding:11px;border-bottom:1px solid #11283a}.agenda{padding:10px;border-left:3px solid var(--c);background:#07131f;margin:7px 0;border-radius:8px}.small{font-size:11px;color:var(--m)}@media(max-width:1150px){.grid{grid-template-columns:repeat(4,1fr)}.rank{grid-template-columns:40px 1fr 90px 80px 80px}.rank>:nth-child(3),.rank>:nth-child(7),.rank>:nth-child(8){display:none}.split{grid-template-columns:1fr}}
'''
def render():
    d=collect(); s=d.get('signals',[]); roles=d.get('roles',{}); age=d.get('age'); agecls='good' if age is not None and age<90 else ('warn' if age is not None and age<300 else 'bad')
    cards=[('CHAMPIONS',roles.get('CHAMPION',0),'good'),('CONTENDERS',roles.get('CONTENDER',0),'good'),('DECAYING',sum(x.get('trend')=='DECAYING' for x in s),'warn'),('RETIRE',roles.get('RETIRE',0),'bad'),('TOKENS',d.get('tokens',0),''),('SWAPS',d.get('swaps',0),''),('DECODE AGE','—' if age is None else f'{age:.0f}s',agecls),('OPEN AGENDA',len(d.get('agenda',[])),'')]
    cs=''.join(f'<div class=card><div class=k>{esc(k)}</div><div class="v {c}">{esc(v)}</div></div>' for k,v,c in cards)
    rows='<div class="rank head"><div>#</div><div>SIGNAL</div><div>TARGET</div><div>SCORE</div><div>RHO</div><div>LIFT</div><div>TREND</div><div>ROLE</div></div>'
    for i,r in enumerate(s[:30],1):
        rows+=f'<div class=rank><div>{i:02d}</div><div><b class=mono>{esc(r["feature"])}</b><div class=small>{esc(r["family"])}</div></div><div>{esc(r["target"])}</div><div><b>{num(r["live_score"])}</b><div class=bar><i style="width:{100*float(r["live_score"]):.0f}%"></i></div></div><div>{num(r["rho"])}</div><div>{num(r["lift"],2)}x</div><div class={esc(r["trend"])}>{esc(r["trend"])}</div><div class={esc(r["role"])}>{esc(r["role"])}</div></div>'
    fam=''.join(f'<div class=family><b>{esc(x["family"])}</b> <span class=small>{esc(x["action"])}</span><div style="display:flex;justify-content:space-between;margin-top:6px"><span>{pct(x["budget_weight"])} research budget</span><span>{x["champion_count"]} champion · {x["stable_count"]} stable · {x["pass_count"]} pass</span></div><div class=bar><i style="width:{100*float(x["budget_weight"]):.0f}%"></i></div></div>' for x in d.get('families',[]))
    ag=''.join(f'<div class=agenda><b>P{a["priority"]} · {esc(a["agenda_type"])}</b><div class=mono>{esc(a["subject"])}</div><div class=small>{esc(a["rationale"])}</div></div>' for a in d.get('agenda',[])) or '<div class=small>No open agenda.</div>'
    ens=''
    for e in d.get('ensembles',[])[:15]: ens+=f'<div class=rank><div>◆</div><div class=mono>{esc(e["context_key"])}</div><div>{esc(e["status"])}</div><div>{num(e["live_score"])}</div><div>{num(e["rho"])}</div><div>{num(e["lift"],2)}x</div><div>{e["member_count"]} members</div><div class={esc(e["role"])}>{esc(e["role"])}</div></div>'
    return f'<!doctype html><html><head><meta charset=utf-8><meta http-equiv=refresh content=3><title>Memecoin Lab V4.8</title><style>{CSS}</style></head><body><div class=wrap><h1>MEMECOIN LAB // LIVE EDGE COMMAND V4.8</h1><div class=sub>WHAT IS WORKING NOW · forward-only evidence · trend + decay + redundancy · read-only</div><div class=grid>{cs}</div><div class=panel><h2>◆ CHAMPION BOARD — CURRENT FORWARD EVIDENCE</h2>{rows}</div><div class=panel><h2>◆ ENSEMBLE FRONTIER</h2>{ens or "<div class=small>No mature ensembles yet.</div>"}</div><div class=split><div class=panel><h2>◆ RESEARCH CAPITAL ALLOCATION</h2>{fam}</div><div class=panel><h2>◆ AUTONOMOUS RESEARCH AGENDA</h2>{ag}</div></div></div></body></html>'
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=='/api/state':
            b=json.dumps(collect(),default=str).encode(); self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b); return
        if self.path not in ('/','/index.html'): self.send_response(404); self.end_headers(); return
        b=render().encode(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*_): pass
if __name__=='__main__':
    print('='*88); print('MEMECOIN LAB — V4.8 LIVE EDGE COMMAND ROOM'); print('='*88); print(f'Dashboard: http://{HOST}:{PORT}'); print('Read-only. CTRL+C stops dashboard only.'); ThreadingHTTPServer((HOST,PORT),H).serve_forever()
