#!/usr/bin/env python3
"""Memecoin Lab V4.6 Scientific Brain Control Room.
Read-only dashboard for conclusions, prospective candidates, beliefs and live data.
"""
from __future__ import annotations
import html, os, sqlite3, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RDB=Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB",ROOT/"research_v4_1.db"))
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
HOST=os.environ.get("MEMECOIN_V46_DASH_HOST","127.0.0.1")
PORT=int(os.environ.get("MEMECOIN_V46_DASH_PORT","8766"))

def esc(x): return html.escape("—" if x is None else str(x))
def num(x,d=3):
    if x is None: return "—"
    try:
        if isinstance(x,int): return f"{x:,}"
        return f"{float(x):.{d}f}"
    except: return esc(x)
def dbopen(path):
    if not path.exists(): return None
    db=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=10)
    db.row_factory=sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db
def table(db,name):
    return db and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone() is not None

def collect():
    out={"now":time.time()}
    r=dbopen(RDB)
    if r:
        out["jobs"]={x["status"]:x["n"] for x in r.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
        out["workers"]=[dict(x) for x in r.execute("SELECT * FROM v41_workers ORDER BY last_heartbeat DESC LIMIT 20")]
        out["conclusions"]=[dict(x) for x in r.execute("""SELECT * FROM v45_conclusions
            ORDER BY CASE classification WHEN 'CONFIRMED_SIGNAL' THEN 0 WHEN 'CANDIDATE_SIGNAL' THEN 1 WHEN 'CONTRADICTORY' THEN 2 ELSE 3 END,
                     confidence DESC LIMIT 40""")] if table(r,"v45_conclusions") else []
        out["conclusion_counts"]={x["classification"]:x["n"] for x in r.execute("SELECT classification,COUNT(*) n FROM v45_conclusions GROUP BY classification")} if table(r,"v45_conclusions") else {}
        out["candidates"]=[dict(x) for x in r.execute("""SELECT c.*,b.status belief_status,b.n,b.baseline_rate,b.precision,b.lift,b.prospective_rho,b.confidence belief_confidence,b.statement
            FROM v46_prospective_candidates c LEFT JOIN v46_beliefs b ON b.candidate_id=c.candidate_id
            ORDER BY CASE b.status WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 WHEN 'WAITING' THEN 2 WHEN 'FAIL' THEN 3 ELSE 4 END,
                     b.confidence DESC,c.frozen_at DESC LIMIT 50""")] if table(r,"v46_prospective_candidates") else []
        out["belief_counts"]={x["status"]:x["n"] for x in r.execute("SELECT status,COUNT(*) n FROM v46_beliefs GROUP BY status")} if table(r,"v46_beliefs") else {}
        out["branches"]=[dict(x) for x in r.execute("""SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) hyp,
             SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
             SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
             SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
             SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed
             FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id GROUP BY h.branch ORDER BY hyp DESC""")]
        r.close()
    v=dbopen(V52)
    if v:
        out["swaps"]=v.execute("SELECT COUNT(*) FROM v52_swaps").fetchone()[0] if table(v,"v52_swaps") else 0
        out["tokens"]=v.execute("SELECT COUNT(DISTINCT token_mint) FROM v52_swaps").fetchone()[0] if table(v,"v52_swaps") else 0
        out["ready"]=v.execute("SELECT COUNT(*) FROM v52_outcomes WHERE ready=1").fetchone()[0] if table(v,"v52_outcomes") else 0
        latest=v.execute("SELECT MAX(timestamp) FROM v52_swaps").fetchone()[0] if table(v,"v52_swaps") else None
        out["decode_age"]=None if latest is None else max(0,time.time()-float(latest))
        v.close()
    return out

CSS="""body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#080b11;color:#edf2fb;margin:0;padding:20px}
h1{margin:0;font-size:27px}.sub{color:#8e9aaf;margin:4px 0 18px}.grid{display:grid;grid-template-columns:repeat(8,minmax(130px,1fr));gap:10px}
.card,.panel{background:#11161f;border:1px solid #28303d;border-radius:12px;padding:14px}.k{font-size:11px;color:#8d98aa;letter-spacing:1px}.v{font-size:27px;font-weight:750;margin-top:5px}.panel{margin-top:12px}
table{width:100%;border-collapse:collapse}th,td{text-align:left;border-bottom:1px solid #29313f;padding:9px 7px;font-size:13px}th{color:#8d98aa;font-size:11px}
.badge{padding:3px 7px;border-radius:10px;font-weight:700;font-size:11px}.PASS,.CONFIRMED_SIGNAL{background:#133b2c;color:#71e0a9}.WATCH,.CANDIDATE_SIGNAL{background:#3c3413;color:#f7d56b}.FAIL,.NEGATIVE_SIGNAL{background:#431b25;color:#ff8098}.CONTRADICTORY{background:#402b16;color:#ffb66f}.WAITING,.NO_EDGE{background:#222936;color:#aab5c8}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}@media(max-width:1100px){.grid{grid-template-columns:repeat(4,1fr)}}"""

def badge(s):
    s=s or "—"; return f'<span class="badge {esc(s)}">{esc(s)}</span>'

def render():
    d=collect(); j=d.get("jobs",{}); bc=d.get("belief_counts",{}); cc=d.get("conclusion_counts",{})
    cards=[
      ("LIVE WORKERS",sum(1 for w in d.get("workers",[]) if time.time()-float(w["last_heartbeat"])<15)),
      ("QUEUE",j.get("QUEUED",0)),("RUNNING",j.get("RUNNING",0)),("DONE",j.get("DONE",0)),
      ("V52 TOKENS",d.get("tokens",0)),("V52 SWAPS",d.get("swaps",0)),("READY OUTCOMES",d.get("ready",0)),
      ("DECODE AGE", "—" if d.get("decode_age") is None else f"{d['decode_age']:.1f}s"),
      ("CONFIRMED",cc.get("CONFIRMED_SIGNAL",0)),("CANDIDATE",cc.get("CANDIDATE_SIGNAL",0)),
      ("CONTRADICTORY",cc.get("CONTRADICTORY",0)),("NO EDGE",cc.get("NO_EDGE",0)),
      ("PROSPECTIVE PASS",bc.get("PASS",0)),("WATCH",bc.get("WATCH",0)),("WAITING",bc.get("WAITING",0)),("FAIL",bc.get("FAIL",0)),
    ]
    cs="".join(f'<div class="card"><div class="k">{esc(k)}</div><div class="v">{esc(v)}</div></div>' for k,v in cards)
    conrows=""
    for c in d.get("conclusions",[])[:25]:
        conrows+=f"<tr><td>{badge(c['classification'])}</td><td class=mono>{esc(c['feature'])}</td><td>{esc(c['target'])}</td><td>{c['stage_s']}s / {c['horizon_s']}s</td><td>{num(c['median_rho'])}</td><td>{num(c['confidence'])}</td><td>{esc(c['conclusion'])}</td></tr>"
    prows=""
    for c in d.get("candidates",[])[:30]:
        prows+=f"<tr><td>{badge(c.get('belief_status'))}</td><td class=mono>{esc(c['feature'])}</td><td>{esc(c['target'])}</td><td>{c['stage_s']}s/{c['horizon_s']}s</td><td>{num(c.get('n'),0)}</td><td>{num(c.get('prospective_rho'))}</td><td>{num(c.get('lift'))}</td><td>{num(c.get('precision'))}</td><td>{num(c.get('baseline_rate'))}</td></tr>"
    brows=""
    for b in d.get("branches",[]):
        brows+=f"<tr><td><b>{esc(b['branch'])}</b></td><td>{num(b['hyp'],0)}</td><td>{num(b['queued'],0)}</td><td>{num(b['running'],0)}</td><td>{num(b['done'],0)}</td><td>{num(b['failed'],0)}</td></tr>"
    return f"""<!doctype html><html><head><meta charset=utf-8><meta http-equiv=refresh content=3><title>Memecoin Lab V4.6</title><style>{CSS}</style></head>
<body><h1>MEMECOIN LAB — SCIENTIFIC BRAIN V4.6</h1><div class=sub>research-only · conclusions · learned hypotheses · prospective validation · refresh 3s</div>
<div class=grid>{cs}</div>
<div class=panel><h3>WHAT THE LAB HAS LEARNED</h3><table><tr><th>CLASS</th><th>FEATURE</th><th>TARGET</th><th>CONTEXT</th><th>MED RHO</th><th>CONF</th><th>CONCLUSION</th></tr>{conrows}</table></div>
<div class=panel><h3>PROSPECTIVE CANDIDATES — UNSEEN TOKENS ONLY</h3><table><tr><th>STATE</th><th>FEATURE</th><th>TARGET</th><th>CONTEXT</th><th>N</th><th>RHO</th><th>LIFT</th><th>PRECISION</th><th>BASELINE</th></tr>{prows}</table></div>
<div class=panel><h3>RESEARCH ORGANISM</h3><table><tr><th>BRANCH</th><th>HYP</th><th>Q</th><th>RUN</th><th>DONE</th><th>FAIL</th></tr>{brows}</table></div>
</body></html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/","/index.html"):
            self.send_response(404); self.end_headers(); return
        body=render().encode()
        self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self,*_): pass

if __name__=="__main__":
    print("="*86)
    print("MEMECOIN LAB — V4.6 SCIENTIFIC BRAIN CONTROL ROOM")
    print("="*86)
    print(f"Dashboard: http://{HOST}:{PORT}")
    print(f"Research : {RDB}")
    print(f"Features : {V52}")
    print("CTRL+C stops dashboard only")
    ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
