#!/usr/bin/env python3
"""Memecoin Lab V4.7 — EDGE FORGE dashboard.
Read-only UI for PASS audit, redundancy and frozen prospective ensembles.
"""
from __future__ import annotations
import html, json, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
HOST=os.environ.get("MEMECOIN_V47_EDGE_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V47_EDGE_PORT","8771"))

def esc(x): return html.escape("—" if x is None else str(x))
def num(x,d=3):
    if x is None:return "—"
    try:return f"{float(x):.{d}f}" if not isinstance(x,int) else f"{x:,}"
    except:return esc(x)
def pct(x,d=1):
    try:return f"{100*float(x):.{d}f}%"
    except:return "—"
def dbopen():
    if not RDB.exists(): return None
    db=sqlite3.connect(f"file:{RDB}?mode=ro",uri=True,timeout=10); db.row_factory=sqlite3.Row; db.execute("PRAGMA busy_timeout=10000"); return db
def exists(db,t): return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone())

def collect():
    out={"passes":[],"audit":[],"pairs":[],"ensembles":[],"counts":{},"now":time.time()}; db=dbopen()
    if not db:return out
    if exists(db,"v46_prospective_candidates") and exists(db,"v46_beliefs"):
        out["passes"]=[dict(r) for r in db.execute("""SELECT c.candidate_id,c.feature,c.target,c.stage_s,c.horizon_s,b.n,b.prospective_rho,b.lift,b.precision,b.baseline_rate,b.confidence
          FROM v46_prospective_candidates c JOIN v46_beliefs b ON b.candidate_id=c.candidate_id WHERE b.status='PASS' ORDER BY b.confidence DESC""")]
    if exists(db,"v47_pass_audit"):
        out["audit"]=[dict(r) for r in db.execute("SELECT * FROM v47_pass_audit ORDER BY stability_score DESC,n DESC")]
        out["counts"]={r["audit_class"]:r["n"] for r in db.execute("SELECT audit_class,COUNT(*) n FROM v47_pass_audit GROUP BY audit_class")}
    if exists(db,"v47_pair_redundancy"):
        out["pairs"]=[dict(r) for r in db.execute("SELECT * FROM v47_pair_redundancy ORDER BY redundancy DESC LIMIT 40")]
    if exists(db,"v47_ensembles"):
        out["ensembles"]=[dict(r) for r in db.execute("""SELECT e.*,b.status belief_status,b.n,b.predicted_n,b.baseline,b.precision,b.lift,b.vote_rho,b.confidence,b.statement
          FROM v47_ensembles e LEFT JOIN v47_ensemble_beliefs b ON b.ensemble_id=e.ensemble_id
          ORDER BY CASE b.status WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 WHEN 'WAITING' THEN 2 ELSE 3 END,b.confidence DESC,e.frozen_at DESC""")]
    db.close(); return out

CSS=r'''
:root{--bg:#02040a;--p:#07111d;--l:#16344d;--txt:#ecfaff;--mut:#7192a9;--cyan:#31f4ff;--green:#3dff9b;--gold:#ffc94a;--red:#ff4c70;--vio:#a86cff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 50% -10%,#0c3450 0,#07101a 30%,#02040a 70%);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;min-height:100vh}body:before{content:"";position:fixed;inset:0;pointer-events:none;background:linear-gradient(rgba(49,244,255,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(49,244,255,.035) 1px,transparent 1px);background-size:28px 28px}.wrap{max-width:1900px;margin:auto;padding:18px}.hero{display:flex;justify-content:space-between;align-items:center}.hero h1{margin:0;font-size:31px;letter-spacing:.5px;text-shadow:0 0 25px rgba(49,244,255,.3)}.sub{color:var(--mut);font-size:12px;margin-top:5px}.pulse{padding:9px 14px;border:1px solid #1e5875;border-radius:999px;color:var(--cyan);background:#06121e;box-shadow:0 0 24px rgba(49,244,255,.12)}.grid{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-top:15px}.card,.panel{background:linear-gradient(145deg,rgba(8,19,31,.97),rgba(3,9,16,.98));border:1px solid var(--l);border-radius:14px;box-shadow:0 20px 50px rgba(0,0,0,.2)}.card{padding:14px}.k{font-size:10px;color:#7998ad;letter-spacing:1.2px}.v{font-size:27px;font-weight:900;margin-top:5px}.green{color:var(--green)}.gold{color:var(--gold)}.red{color:var(--red)}.panel{padding:15px;margin-top:12px}.panel h2{margin:0 0 12px;font-size:13px;letter-spacing:1.4px;color:#b8d9eb}.ensgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.ens{border:1px solid rgba(61,255,155,.35);border-radius:14px;padding:14px;background:radial-gradient(circle at 90% 0,rgba(61,255,155,.12),transparent 45%),#06121a}.ens.watch{border-color:rgba(255,201,74,.4);background:radial-gradient(circle at 90% 0,rgba(255,201,74,.11),transparent 45%),#0b1117}.state{display:inline-block;padding:3px 8px;border-radius:999px;font-size:10px;font-weight:900;background:#113a29;color:var(--green)}.state.WATCH{background:#3c3210;color:var(--gold)}.state.WAITING{background:#152231;color:#9bb6c8}.state.FAIL{background:#431625;color:var(--red)}.title{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-weight:850;margin-top:9px}.mini{color:#80a0b5;font-size:11px;margin-top:3px}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:12px}.m{border:1px solid #14334c;background:#050e17;border-radius:9px;padding:8px}.m b{display:block;font-size:16px}.m span{font-size:9px;color:#7593a8}table{width:100%;border-collapse:collapse}th,td{padding:9px 7px;border-bottom:1px solid #142d43;font-size:12px;text-align:left}th{font-size:10px;color:#7795aa;letter-spacing:.8px}.bar{height:5px;background:#102233;border-radius:5px;overflow:hidden;margin-top:10px}.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--cyan),var(--green));box-shadow:0 0 10px var(--green)}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.danger{color:var(--red)}@media(max-width:1200px){.grid{grid-template-columns:repeat(3,1fr)}.ensgrid{grid-template-columns:1fr}}
'''

def render():
    d=collect(); ac=d["counts"]; ens=d["ensembles"]; ec={}
    for e in ens:ec[e.get("belief_status") or "WAITING"]=ec.get(e.get("belief_status") or "WAITING",0)+1
    cards=[("SINGLE PASS",len(d["passes"]),"green"),("ELITE STABLE",ac.get("ELITE_STABLE",0),"green"),("STABLE",ac.get("STABLE",0),"green"),("MIXED",ac.get("MIXED",0),"gold"),("ENSEMBLE WATCH",ec.get("WATCH",0),"gold"),("ENSEMBLE PASS",ec.get("PASS",0),"green")]
    cs=''.join(f'<div class=card><div class=k>{esc(k)}</div><div class="v {c}">{esc(v)}</div></div>' for k,v,c in cards)
    er=''
    for e in ens[:18]:
        st=e.get("belief_status") or "WAITING"; members=json.loads(e["member_features_json"]); fams=json.loads(e["member_families_json"])
        conf=float(e.get("confidence") or 0); er+=f'''<div class="ens {'watch' if st=='WATCH' else ''}"><span class="state {esc(st)}">{esc(st)}</span><div class=title>{esc(e['ensemble_id'])}</div><div class=mini>{esc(e['target'])} · {e['stage_s']}s → {e['horizon_s']}s · {e['member_count']} signals</div><div class=mini>{esc(' + '.join(members))}</div><div class=metrics><div class=m><b>{int(e.get('n') or 0)}</b><span>UNSEEN N</span></div><div class=m><b>{num(e.get('vote_rho'))}</b><span>VOTE RHO</span></div><div class=m><b>{num(e.get('lift'),2)}×</b><span>LIFT</span></div><div class=m><b>{pct(e.get('precision'))}</b><span>PRECISION</span></div></div><div class=bar><i style="width:{min(100,100*conf):.0f}%"></i></div><div class=mini>families: {esc(' · '.join(fams))} · frozen before these observations</div></div>'''
    if not er:er='<div class=mini>No ensemble frozen yet. V4.7 science needs at least two low-redundancy PASS signals in the same context.</div>'
    ar=''
    for a in d["audit"][:25]:
        ar+=f"<tr><td class=mono>{esc(a['feature'])}</td><td>{esc(a['target'])}</td><td>{a['stage_s']}s/{a['horizon_s']}s</td><td>{int(a['n'])}</td><td>{num(a['rho'])}</td><td>{num(a['lift'],2)}×</td><td>{num(a['early_rho'])}</td><td>{num(a['late_rho'])}</td><td>{pct(a['stability_score'])}</td><td>{esc(a['audit_class'])}</td></tr>"
    pr=''
    for p in d["pairs"][:30]:
        pr+=f"<tr><td class=mono>{esc(p['candidate_a'][-8:])}</td><td class=mono>{esc(p['candidate_b'][-8:])}</td><td>{int(p['shared_n'])}</td><td>{num(p['score_rho'])}</td><td>{pct(p['prediction_jaccard'])}</td><td>{pct(p['redundancy'])}</td><td>{pct(p['independence'])}</td></tr>"
    return f'''<!doctype html><html><head><meta charset=utf-8><meta http-equiv=refresh content=3><title>EDGE FORGE V4.7</title><style>{CSS}</style></head><body><div class=wrap><div class=hero><div><h1>MEMECOIN LAB // EDGE FORGE V4.7</h1><div class=sub>PASS AUDIT · SIGNAL INDEPENDENCE · FROZEN ENSEMBLES · FUTURE TOKENS ONLY</div></div><div class=pulse>● PROSPECTIVE SCIENCE ONLINE</div></div><div class=grid>{cs}</div><div class=panel><h2>◆ FROZEN ENSEMBLE VAULT — ZERO POST-FREEZE TUNING</h2><div class=ensgrid>{er}</div></div><div class=panel><h2>◆ PASS STABILITY AUDIT — DOES THE EDGE SURVIVE TIME?</h2><table><tr><th>FEATURE</th><th>TARGET</th><th>CTX</th><th>N</th><th>RHO</th><th>LIFT</th><th>EARLY ρ</th><th>LATE ρ</th><th>STABILITY</th><th>CLASS</th></tr>{ar}</table></div><div class=panel><h2>◆ REDUNDANCY MATRIX — ARE WE COUNTING THE SAME EDGE TWICE?</h2><table><tr><th>A</th><th>B</th><th>SHARED N</th><th>SCORE ρ</th><th>PRED JACCARD</th><th>REDUNDANCY</th><th>INDEPENDENCE</th></tr>{pr}</table></div></div></body></html>'''

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ('/','/index.html'):
            self.send_response(404);self.end_headers();return
        b=render().encode();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
    def log_message(self,*_):pass
if __name__=='__main__':
    print('='*88);print('MEMECOIN LAB — V4.7 EDGE FORGE');print('='*88);print(f'Dashboard: http://{HOST}:{PORT}');print('Read-only dashboard. CTRL+C stops dashboard only.');ThreadingHTTPServer((HOST,PORT),H).serve_forever()
