#!/usr/bin/env python3
"""Memecoin Lab V4.7 — Neon Scientific Ops Control Room.

Read-only dashboard layered on top of V4.6/V5.2 state.
Goals:
- make prospective PASS/WATCH candidates the visual center of gravity
- expose lift, precision, baseline, rho, n, threshold and direction at a glance
- show data freshness and research-system health prominently
- group recurring features into coarse signal families to expose redundancy
- keep the dashboard strictly read-only

Research-only. No signing. No trading.
"""
from __future__ import annotations

import html
import json
import math
import os
import sqlite3
import statistics
import time
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
RDB = Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB", ROOT / "research_v4_1.db"))
V52 = Path(os.environ.get("MEMECOIN_V52_DB", ROOT / "v52_features.db"))
HOST = os.environ.get("MEMECOIN_V47_DASH_HOST", "127.0.0.1")
PORT = int(os.environ.get("MEMECOIN_V47_DASH_PORT", "8770"))


def esc(x):
    return html.escape("—" if x is None else str(x))


def num(x, d=3):
    if x is None:
        return "—"
    try:
        if isinstance(x, int):
            return f"{x:,}"
        return f"{float(x):.{d}f}"
    except Exception:
        return esc(x)


def pct(x, d=1):
    if x is None:
        return "—"
    try:
        return f"{100.0*float(x):.{d}f}%"
    except Exception:
        return "—"


def dbopen(path):
    if not path.exists():
        return None
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db


def table(db, name):
    return bool(db and db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())


def feature_family(feature):
    f = (feature or "").lower()
    if any(k in f for k in ("wallet", "hhi", "repeat")):
        return "WALLET / CONCENTRATION"
    if any(k in f for k in ("price", "return", "range")):
        return "PRICE / MOMENTUM"
    if any(k in f for k in ("flow", "buy_ratio", "net_sol", "gross_sol")):
        return "FLOW / IMBALANCE"
    if any(k in f for k in ("swap", "trade", "activity", "velocity")):
        return "ACTIVITY / TRADING"
    return "OTHER"


def health_state(decode_age, live_workers, queue, failed):
    if decode_age is None:
        return "CRITICAL", "NO DECODE FEED"
    if decode_age > 300:
        return "CRITICAL", "DECODE STALE"
    if live_workers <= 0:
        return "CRITICAL", "NO RESEARCH WORKERS"
    if failed > 0:
        return "WARNING", "FAILURES DETECTED"
    if decode_age > 90:
        return "WARNING", "DECODE LAG"
    if queue > 1000:
        return "WARNING", "QUEUE PRESSURE"
    return "ONLINE", "SYSTEM NOMINAL"


def composite_score(c):
    n = float(c.get("n") or 0)
    rho = max(0.0, float(c.get("prospective_rho") or 0))
    lift = max(0.0, float(c.get("lift") or 0) - 1.0)
    conf = max(0.0, float(c.get("belief_confidence") or 0))
    sample = min(1.0, n / 150.0)
    return 0.30 * min(1.0, rho / 0.25) + 0.25 * min(1.0, lift / 0.75) + 0.25 * conf + 0.20 * sample


def collect():
    out = {"now": time.time(), "jobs": {}, "workers": [], "conclusions": [], "candidates": [], "branches": []}
    r = dbopen(RDB)
    if r:
        out["jobs"] = {x["status"]: x["n"] for x in r.execute(
            "SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status"
        )}
        out["workers"] = [dict(x) for x in r.execute(
            "SELECT * FROM v41_workers ORDER BY last_heartbeat DESC LIMIT 40"
        )]
        if table(r, "v45_conclusions"):
            out["conclusion_counts"] = {x["classification"]: x["n"] for x in r.execute(
                "SELECT classification,COUNT(*) n FROM v45_conclusions GROUP BY classification"
            )}
            out["conclusions"] = [dict(x) for x in r.execute("""
                SELECT * FROM v45_conclusions
                ORDER BY CASE classification
                  WHEN 'CONFIRMED_SIGNAL' THEN 0
                  WHEN 'CANDIDATE_SIGNAL' THEN 1
                  WHEN 'CONTRADICTORY' THEN 2
                  ELSE 3 END,
                  confidence DESC, ABS(median_rho) DESC
                LIMIT 120
            """)]
        else:
            out["conclusion_counts"] = {}
        if table(r, "v46_prospective_candidates"):
            out["candidates"] = [dict(x) for x in r.execute("""
                SELECT c.*, b.status belief_status,b.n,b.positives,b.predicted_n,b.predicted_hits,
                       b.baseline_rate,b.precision,b.lift,b.prospective_rho,
                       b.confidence belief_confidence,b.statement,b.metrics_json
                FROM v46_prospective_candidates c
                LEFT JOIN v46_beliefs b ON b.candidate_id=c.candidate_id
                ORDER BY CASE b.status WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 WHEN 'WAITING' THEN 2 WHEN 'FAIL' THEN 3 ELSE 4 END,
                         b.confidence DESC,c.frozen_at DESC
                LIMIT 400
            """)]
            out["belief_counts"] = {x["status"]: x["n"] for x in r.execute(
                "SELECT status,COUNT(*) n FROM v46_beliefs GROUP BY status"
            )} if table(r, "v46_beliefs") else {}
        else:
            out["belief_counts"] = {}
        out["branches"] = [dict(x) for x in r.execute("""
            SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) hyp,
                   SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
                   SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
                   SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
                   SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed
            FROM v41_hypotheses h
            LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
            GROUP BY h.branch ORDER BY hyp DESC
        """)]
        r.close()

    v = dbopen(V52)
    if v:
        out["swaps"] = v.execute("SELECT COUNT(*) FROM v52_swaps").fetchone()[0] if table(v, "v52_swaps") else 0
        out["tokens"] = v.execute("SELECT COUNT(DISTINCT token_mint) FROM v52_swaps").fetchone()[0] if table(v, "v52_swaps") else 0
        out["ready"] = v.execute("SELECT COUNT(*) FROM v52_outcomes WHERE ready=1").fetchone()[0] if table(v, "v52_outcomes") else 0
        latest = v.execute("SELECT MAX(timestamp) FROM v52_swaps").fetchone()[0] if table(v, "v52_swaps") else None
        out["decode_age"] = None if latest is None else max(0.0, time.time() - float(latest))
        v.close()

    now = time.time()
    out["live_workers"] = sum(1 for w in out.get("workers", []) if now - float(w["last_heartbeat"]) < 15)
    out["failed_jobs"] = int(out.get("jobs", {}).get("FAILED", 0))
    out["health"], out["health_text"] = health_state(
        out.get("decode_age"), out["live_workers"], int(out.get("jobs", {}).get("QUEUED", 0)), out["failed_jobs"]
    )

    # Candidate analytics
    for c in out.get("candidates", []):
        c["score"] = composite_score(c)
        p = c.get("precision")
        b = c.get("baseline_rate")
        c["edge_pp"] = None if p is None or b is None else 100.0 * (float(p) - float(b))
        c["family"] = feature_family(c.get("feature"))
    out["passes"] = [c for c in out.get("candidates", []) if c.get("belief_status") == "PASS"]
    out["watch"] = sorted([c for c in out.get("candidates", []) if c.get("belief_status") == "WATCH"], key=lambda x: x["score"], reverse=True)

    family_stats = defaultdict(lambda: {"confirmed": 0, "candidate": 0, "pass": 0, "watch": 0, "rho": []})
    for c in out.get("conclusions", []):
        fam = feature_family(c.get("feature"))
        if c.get("classification") == "CONFIRMED_SIGNAL":
            family_stats[fam]["confirmed"] += 1
        if c.get("classification") == "CANDIDATE_SIGNAL":
            family_stats[fam]["candidate"] += 1
        if c.get("median_rho") is not None:
            family_stats[fam]["rho"].append(abs(float(c["median_rho"])))
    for c in out.get("candidates", []):
        fam = c["family"]
        if c.get("belief_status") == "PASS":
            family_stats[fam]["pass"] += 1
        elif c.get("belief_status") == "WATCH":
            family_stats[fam]["watch"] += 1
    out["families"] = []
    for fam, s in family_stats.items():
        s = dict(s)
        s["family"] = fam
        s["median_abs_rho"] = statistics.median(s["rho"]) if s["rho"] else None
        s.pop("rho", None)
        out["families"].append(s)
    out["families"].sort(key=lambda x: (x["pass"], x["confirmed"], x["watch"]), reverse=True)
    return out


CSS = r"""
:root{--bg:#03070c;--panel:#08111b;--panel2:#0b1724;--line:#193148;--txt:#e8f7ff;--muted:#7392a8;--cyan:#39f2ff;--green:#49ff9a;--lime:#c6ff4a;--orange:#ffb142;--red:#ff4f6d;--purple:#b583ff;--blue:#5d8cff}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 70% -20%,#0b2f47 0,#06121e 28%,var(--bg) 65%);color:var(--txt);font-family:Inter,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;min-height:100vh;overflow-x:hidden}
body:before{content:"";position:fixed;inset:0;pointer-events:none;background:linear-gradient(rgba(57,242,255,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(57,242,255,.025) 1px,transparent 1px);background-size:32px 32px;mask-image:linear-gradient(to bottom,black,transparent 85%)}
body:after{content:"";position:fixed;inset:0;pointer-events:none;background:linear-gradient(to bottom,transparent 0,rgba(57,242,255,.03) 50%,transparent 100%);height:10%;animation:scan 7s linear infinite}@keyframes scan{from{transform:translateY(-120vh)}to{transform:translateY(120vh)}}
.wrap{padding:20px;max-width:1800px;margin:auto}.top{display:flex;justify-content:space-between;gap:20px;align-items:center;margin-bottom:14px}.brand h1{font-size:30px;margin:0;letter-spacing:.4px;text-shadow:0 0 22px rgba(57,242,255,.22)}.brand .sub{color:var(--muted);font-size:13px;margin-top:4px}.status{display:flex;align-items:center;gap:10px;padding:9px 14px;border:1px solid var(--line);background:rgba(8,17,27,.8);border-radius:999px;font-size:12px;font-weight:800;letter-spacing:.8px}.dot{width:9px;height:9px;border-radius:50%;box-shadow:0 0 18px currentColor}.ONLINE{color:var(--green)}.WARNING{color:var(--orange)}.CRITICAL{color:var(--red)}
.grid{display:grid;grid-template-columns:repeat(8,minmax(125px,1fr));gap:10px}.card{position:relative;background:linear-gradient(145deg,rgba(12,25,39,.98),rgba(5,12,20,.98));border:1px solid var(--line);border-radius:14px;padding:14px;overflow:hidden;min-height:91px}.card:before{content:"";position:absolute;left:0;top:0;width:100%;height:2px;background:linear-gradient(90deg,transparent,var(--cyan),transparent);opacity:.28}.k{font-size:10px;letter-spacing:1.3px;color:#7895aa;text-transform:uppercase}.v{font-size:27px;font-weight:850;margin-top:6px}.tiny{font-size:11px;color:var(--muted);margin-top:3px}.v.good{color:var(--green);text-shadow:0 0 18px rgba(73,255,154,.25)}.v.warn{color:var(--orange)}.v.bad{color:var(--red)}
.panel{margin-top:12px;background:linear-gradient(145deg,rgba(8,17,27,.95),rgba(4,10,16,.98));border:1px solid var(--line);border-radius:15px;padding:15px;box-shadow:0 20px 60px rgba(0,0,0,.18)}.panel h2{font-size:13px;letter-spacing:1.3px;margin:0 0 12px;color:#b7d4e7;text-transform:uppercase}.split{display:grid;grid-template-columns:1.3fr .7fr;gap:12px}.passes{display:grid;grid-template-columns:repeat(3,minmax(250px,1fr));gap:10px}.pass{position:relative;padding:15px;border-radius:14px;background:radial-gradient(circle at 90% 0%,rgba(73,255,154,.13),transparent 45%),#07141b;border:1px solid rgba(73,255,154,.32);box-shadow:inset 0 0 35px rgba(73,255,154,.035)}.pass .title{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-weight:800;font-size:14px}.pass .target{font-size:11px;color:#8eafc0;margin-top:3px}.metricrow{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;margin-top:12px}.metric{background:#07101a;border:1px solid #163047;border-radius:9px;padding:8px}.metric b{display:block;font-size:16px}.metric span{font-size:9px;color:#7895aa;letter-spacing:.7px}.scoreline{height:5px;background:#102132;border-radius:5px;overflow:hidden;margin-top:11px}.scoreline i{display:block;height:100%;background:linear-gradient(90deg,var(--cyan),var(--green));box-shadow:0 0 12px var(--green)}
.badge{display:inline-block;padding:3px 7px;border-radius:999px;font-size:9px;font-weight:900;letter-spacing:.6px}.PASS,.CONFIRMED_SIGNAL{background:#0f402d;color:#65ffad;border:1px solid #1d684a}.WATCH,.CANDIDATE_SIGNAL{background:#413313;color:#ffd86b;border:1px solid #70591f}.FAIL,.NEGATIVE_SIGNAL{background:#401722;color:#ff7891;border:1px solid #6c2637}.CONTRADICTORY{background:#472817;color:#ffad66;border:1px solid #74452b}.WAITING,.NO_EDGE{background:#172231;color:#91a9bb;border:1px solid #294158}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:9px 7px;border-bottom:1px solid #13283a;font-size:12px}th{font-size:9px;color:#708ba0;letter-spacing:.9px;text-transform:uppercase}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.positive{color:var(--green);font-weight:800}.negative{color:var(--red);font-weight:800}
.familygrid{display:grid;grid-template-columns:repeat(4,1fr);gap:9px}.family{padding:12px;border:1px solid #1a3348;background:#07111b;border-radius:12px}.family .name{font-size:10px;color:#87a8bd;letter-spacing:.7px}.family .big{font-size:24px;font-weight:900;margin-top:5px}.family .bar{height:4px;background:#112436;border-radius:5px;margin:9px 0 7px;overflow:hidden}.family .bar i{display:block;height:100%;background:linear-gradient(90deg,var(--purple),var(--cyan))}.family .foot{font-size:10px;color:#6f8aa0}
.watchlist{max-height:530px;overflow:auto}.watchitem{display:grid;grid-template-columns:1.3fr .9fr .55fr .55fr .55fr .55fr;gap:7px;align-items:center;padding:8px;border-bottom:1px solid #13283a}.watchitem .score{font-weight:900;color:var(--cyan)}.watchitem:hover{background:#0b1824}.empty{padding:30px;text-align:center;color:#6f8aa0;border:1px dashed #224057;border-radius:12px}
.footer{color:#557287;font-size:10px;margin:15px 3px 5px;display:flex;justify-content:space-between}@media(max-width:1300px){.grid{grid-template-columns:repeat(4,1fr)}.passes{grid-template-columns:1fr 1fr}.split{grid-template-columns:1fr}.familygrid{grid-template-columns:1fr 1fr}}@media(max-width:800px){.grid{grid-template-columns:1fr 1fr}.passes{grid-template-columns:1fr}.familygrid{grid-template-columns:1fr}.metricrow{grid-template-columns:1fr 1fr}.top{align-items:flex-start;flex-direction:column}}
"""


def badge(s):
    s = s or "—"
    return f'<span class="badge {esc(s)}">{esc(s)}</span>'


def kpi(k, v, tiny="", cls=""):
    return f'<div class="card"><div class="k">{esc(k)}</div><div class="v {cls}">{esc(v)}</div><div class="tiny">{esc(tiny)}</div></div>'


def pass_card(c):
    score = int(round(100 * float(c.get("score") or 0)))
    edge = c.get("edge_pp")
    edge_txt = "—" if edge is None else f"{edge:+.1f}pp"
    return f"""
    <div class="pass">
      <div style="display:flex;justify-content:space-between;gap:8px">{badge('PASS')}<span style="color:#68869a;font-size:10px">{esc(c.get('family'))}</span></div>
      <div class="title" style="margin-top:10px">{esc(c.get('feature'))}</div>
      <div class="target">{esc(c.get('target'))} · {esc(c.get('stage_s'))}s → {esc(c.get('horizon_s'))}s</div>
      <div class="metricrow">
        <div class="metric"><b>{num(c.get('n'),0)}</b><span>UNSEEN N</span></div>
        <div class="metric"><b>{num(c.get('prospective_rho'))}</b><span>RHO</span></div>
        <div class="metric"><b>{num(c.get('lift'),2)}×</b><span>LIFT</span></div>
        <div class="metric"><b class="positive">{edge_txt}</b><span>EDGE vs BASE</span></div>
      </div>
      <div class="metricrow">
        <div class="metric"><b>{pct(c.get('precision'))}</b><span>PRECISION</span></div>
        <div class="metric"><b>{pct(c.get('baseline_rate'))}</b><span>BASELINE</span></div>
        <div class="metric"><b>{num(c.get('threshold'))}</b><span>THRESHOLD</span></div>
        <div class="metric"><b>{'↑' if float(c.get('direction') or 1)>0 else '↓'}</b><span>DIRECTION</span></div>
      </div>
      <div class="scoreline"><i style="width:{score}%"></i></div>
      <div class="tiny">SCIENCE SCORE {score}/100 · frozen before these observations</div>
    </div>"""


def render():
    d = collect()
    j = d.get("jobs", {})
    bc = d.get("belief_counts", {})
    cc = d.get("conclusion_counts", {})
    age = d.get("decode_age")
    age_cls = "good" if age is not None and age < 90 else ("warn" if age is not None and age < 300 else "bad")
    cards = "".join([
        kpi("LIVE WORKERS", d.get("live_workers",0), "research organism", "good" if d.get("live_workers",0)>0 else "bad"),
        kpi("QUEUE", j.get("QUEUED",0), "jobs waiting"),
        kpi("DONE", j.get("DONE",0), "completed science"),
        kpi("V52 TOKENS", d.get("tokens",0), "decoded population"),
        kpi("V52 SWAPS", d.get("swaps",0), "decoded trades"),
        kpi("READY OUTCOMES", d.get("ready",0), "mature labels"),
        kpi("DECODE AGE", "—" if age is None else f"{age:.0f}s", "freshness", age_cls),
        kpi("PROSPECTIVE PASS", bc.get("PASS",0), "unseen-token wins", "good" if bc.get("PASS",0)>0 else ""),
        kpi("WATCH", bc.get("WATCH",0), "prospective candidates", "warn"),
        kpi("WAITING", bc.get("WAITING",0), "need more N"),
        kpi("CONFIRMED", cc.get("CONFIRMED_SIGNAL",0), "historical stable"),
        kpi("CANDIDATE", cc.get("CANDIDATE_SIGNAL",0), "still developing"),
        kpi("CONTRADICTORY", cc.get("CONTRADICTORY",0), "regime questions"),
        kpi("NO EDGE", cc.get("NO_EDGE",0), "ideas eliminated"),
        kpi("FAIL", bc.get("FAIL",0), "prospective failures", "bad" if bc.get("FAIL",0)>0 else ""),
        kpi("FAILED JOBS", d.get("failed_jobs",0), "terminal failures", "bad" if d.get("failed_jobs",0)>0 else "good"),
    ])

    passes = d.get("passes", [])
    pass_html = "".join(pass_card(c) for c in passes[:9]) if passes else '<div class="empty">NO PROSPECTIVE PASS YET — KEEP COLLECTING UNSEEN TOKENS</div>'

    watch_html = ""
    for c in d.get("watch", [])[:30]:
        watch_html += f"""<div class="watchitem">
          <div><b class="mono">{esc(c.get('feature'))}</b><div class="tiny">{esc(c.get('target'))} · {c.get('stage_s')}s/{c.get('horizon_s')}s</div></div>
          <div>{esc(c.get('family'))}</div>
          <div>{num(c.get('n'),0)}</div><div>{num(c.get('prospective_rho'))}</div><div>{num(c.get('lift'),2)}×</div><div class="score">{int(round(100*c.get('score',0)))}</div>
        </div>"""
    if not watch_html:
        watch_html = '<div class="empty">NO WATCH CANDIDATES</div>'

    fam_html = ""
    for f in d.get("families", [])[:8]:
        power = min(100, int(10*f["pass"] + 2*f["confirmed"] + f["watch"]))
        fam_html += f"""<div class="family"><div class="name">{esc(f['family'])}</div><div class="big">{f['pass']} PASS</div>
        <div class="bar"><i style="width:{power}%"></i></div><div class="foot">{f['confirmed']} confirmed · {f['watch']} watch · med |ρ| {num(f.get('median_abs_rho'))}</div></div>"""

    concl_rows = ""
    for c in d.get("conclusions", [])[:35]:
        concl_rows += f"""<tr><td>{badge(c.get('classification'))}</td><td class="mono"><b>{esc(c.get('feature'))}</b></td>
        <td>{esc(c.get('target'))}</td><td>{c.get('stage_s')}s/{c.get('horizon_s')}s</td><td>{num(c.get('median_rho'))}</td>
        <td>{num(c.get('confidence'))}</td><td>{esc(feature_family(c.get('feature')))}</td></tr>"""

    branch_rows = "".join(
        f"<tr><td><b>{esc(b['branch'])}</b></td><td>{num(b['hyp'],0)}</td><td>{num(b['queued'],0)}</td><td>{num(b['running'],0)}</td><td>{num(b['done'],0)}</td><td>{num(b['failed'],0)}</td></tr>"
        for b in d.get("branches", [])
    )

    return f"""<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="3"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V4.7</title><style>{CSS}</style></head>
<body><div class="wrap">
<div class="top"><div class="brand"><h1>MEMECOIN LAB // SCIENCE OPS V4.7</h1><div class="sub">PROSPECTIVE EDGE COMMAND · UNSEEN TOKENS ONLY · READ-ONLY CONTROL PLANE · REFRESH 3s</div></div>
<div class="status {d['health']}"><span class="dot"></span>{esc(d['health_text'])}</div></div>
<div class="grid">{cards}</div>

<div class="panel"><h2>◆ PROSPECTIVE PASS VAULT — THE SIGNALS THAT SURVIVED NEW DATA</h2><div class="passes">{pass_html}</div></div>

<div class="split">
  <div class="panel"><h2>◆ WATCH RADAR — CANDIDATES APPROACHING DECISION</h2><div class="watchlist"><div class="watchitem" style="color:#6f8ba0;font-size:9px"><div>SIGNAL</div><div>FAMILY</div><div>N</div><div>RHO</div><div>LIFT</div><div>SCORE</div></div>{watch_html}</div></div>
  <div class="panel"><h2>◆ SIGNAL FAMILY POWER MAP — REDUNDANCY VIEW</h2><div class="familygrid">{fam_html}</div><div class="tiny" style="margin-top:10px">Family grouping is intentionally coarse. It helps spot apparent discoveries that may be variants of the same underlying phenomenon.</div></div>
</div>

<div class="panel"><h2>◆ KNOWLEDGE BASE — WHAT THE LAB CURRENTLY BELIEVES</h2><table><tr><th>CLASS</th><th>FEATURE</th><th>TARGET</th><th>CONTEXT</th><th>MED RHO</th><th>CONF</th><th>FAMILY</th></tr>{concl_rows}</table></div>
<div class="panel"><h2>◆ RESEARCH ORGANISM TELEMETRY</h2><table><tr><th>BRANCH</th><th>HYP</th><th>QUEUE</th><th>RUN</th><th>DONE</th><th>FAIL</th></tr>{branch_rows}</table></div>
<div class="footer"><span>V4.7 SCIENCE OPS · NO EXECUTION · NO AUTO-TRADING</span><span>{time.strftime('%Y-%m-%d %H:%M:%S')}</span></div>
</div></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            body = render().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/api/state":
            body = json.dumps(collect(), default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print("=" * 92)
    print("MEMECOIN LAB — V4.7 NEON SCIENCE OPS CONTROL ROOM")
    print("=" * 92)
    print(f"Dashboard : http://{HOST}:{PORT}")
    print(f"API       : http://{HOST}:{PORT}/api/state")
    print(f"Research  : {RDB}")
    print(f"Features  : {V52}")
    print("READ ONLY · CTRL+C stops dashboard only")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
