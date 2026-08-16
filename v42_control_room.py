#!/usr/bin/env python3
"""Memecoin Lab V4.2 control room.

A safer, focused replacement for the V4.1 dashboard. It keeps the useful V4.1
coverage panels but adds the operational information needed now:
- current vs stale/dead workers
- generation funnel
- stage funnel (DISCOVERY/REFINEMENT/ROBUSTNESS)
- failure root causes
- queue / throughput health

No external dependencies. Research-only.
"""

from __future__ import annotations

import html
import json
import os
import re
import sqlite3
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import v41_core as core

HOST = os.environ.get("MEMECOIN_CONTROL_HOST", "127.0.0.1")
PORT = int(os.environ.get("MEMECOIN_CONTROL_PORT", "8765"))
REFRESH_S = int(os.environ.get("MEMECOIN_CONTROL_REFRESH_S", "3"))
LIVE_S = int(os.environ.get("MEMECOIN_WORKER_LIVE_S", "10"))
STALE_S = int(os.environ.get("MEMECOIN_WORKER_STALE_S", "60"))


def esc(v):
    return html.escape("" if v is None else str(v))


def num(v, d=2):
    if v is None:
        return "—"
    if isinstance(v, int):
        return f"{v:,}"
    try:
        return f"{float(v):,.{d}f}"
    except Exception:
        return esc(v)


def age(seconds):
    if seconds is None:
        return "—"
    x = max(0.0, float(seconds))
    if x < 60:
        return f"{x:.0f}s"
    if x < 3600:
        return f"{x/60:.1f}m"
    return f"{x/3600:.1f}h"


def badge(text, cls="neutral"):
    return f'<span class="badge {cls}">{esc(text)}</span>'


def root_cause(error):
    if not error:
        return "NO_ERROR_TEXT"
    lines = [x.strip() for x in str(error).splitlines() if x.strip()]
    if not lines:
        return "EMPTY_ERROR"
    out = lines[-1]
    out = re.sub(r"0x[0-9a-fA-F]+", "0x…", out)
    out = re.sub(r"\b\d{4,}\b", "N", out)
    return out[:180]


def table_exists(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def snapshot():
    now = time.time()
    rdb = core.open_research()

    jobs = {r["status"]: r["n"] for r in rdb.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    stages = {r["stage"]: r["n"] for r in rdb.execute("SELECT stage,COUNT(*) n FROM v41_results GROUP BY stage")}
    verdicts = {r["verdict"]: r["n"] for r in rdb.execute("SELECT verdict,COUNT(*) n FROM v41_results GROUP BY verdict")}
    generations = [dict(r) for r in rdb.execute("SELECT generation,COUNT(*) n FROM v41_hypotheses GROUP BY generation ORDER BY generation")]
    memory = rdb.execute("SELECT COUNT(*) FROM v41_memory").fetchone()[0]
    frozen = rdb.execute("SELECT COUNT(*) FROM v41_candidates WHERE status='FROZEN'").fetchone()[0]
    hypotheses = rdb.execute("SELECT COUNT(*) FROM v41_hypotheses").fetchone()[0]
    results = rdb.execute("SELECT COUNT(*) FROM v41_results").fetchone()[0]

    workers = []
    for r in rdb.execute(
        """SELECT worker_id,pid,state,current_job_id,jobs_done,jobs_failed,last_heartbeat,started_at
           FROM v41_workers ORDER BY last_heartbeat DESC"""
    ):
        x = dict(r)
        hb_age = now - float(x["last_heartbeat"] or 0)
        if hb_age <= LIVE_S:
            health = "LIVE"
        elif hb_age <= STALE_S:
            health = "STALE"
        else:
            health = "DEAD"
        x["heartbeat_age_s"] = hb_age
        x["health"] = health
        workers.append(x)

    active_run_workers = [w for w in workers if w["health"] == "LIVE"]

    branches = [dict(r) for r in rdb.execute(
        """SELECT h.branch,
                  COUNT(DISTINCT h.hypothesis_id) hypotheses,
                  SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
                  SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
                  SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
                  SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed,
                  MAX(res.primary_metric) best_metric,
                  MAX(res.effect_size) best_effect,
                  MAX(res.created_at) last_result
           FROM v41_hypotheses h
           LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
           LEFT JOIN v41_results res ON res.hypothesis_id=h.hypothesis_id
           GROUP BY h.branch ORDER BY hypotheses DESC"""
    )]

    failed_rows = rdb.execute(
        """SELECT j.error,j.job_type,j.payload_json,h.branch,h.generation
           FROM v41_jobs j JOIN v41_hypotheses h ON h.hypothesis_id=j.hypothesis_id
           WHERE j.status='FAILED'"""
    ).fetchall()
    failure_causes = Counter(root_cause(r["error"]) for r in failed_rows)
    failure_branches = Counter(r["branch"] for r in failed_rows)
    failure_adapters = Counter()
    for r in failed_rows:
        try:
            payload = json.loads(r["payload_json"] or "{}")
        except Exception:
            payload = {}
        failure_adapters[payload.get("adapter", "UNKNOWN")] += 1

    recent_results = [dict(r) for r in rdb.execute(
        """SELECT res.created_at,res.stage,res.verdict,res.primary_metric,res.effect_size,
                  h.branch,h.family,h.generation,h.hypothesis_id
           FROM v41_results res JOIN v41_hypotheses h ON h.hypothesis_id=res.hypothesis_id
           ORDER BY res.created_at DESC LIMIT 25"""
    )]

    rates = {}
    for label, sec in (("1m", 60), ("5m", 300), ("15m", 900), ("1h", 3600)):
        rates[label] = rdb.execute("SELECT COUNT(*) FROM v41_results WHERE created_at>=?", (now-sec,)).fetchone()[0]

    queue = rdb.execute(
        """SELECT COUNT(*) n,MIN(created_at) oldest,AVG(?-created_at) avg_age
           FROM v41_jobs WHERE status='QUEUED'""", (now,)
    ).fetchone()
    rdb.close()

    market = {"tokens": 0, "swaps": 0, "wallets": 0, "migrations": 0, "latest_swap_age_s": None}
    try:
        mdb = core.open_market()
        if table_exists(mdb, "t116_pump_swaps"):
            r = mdb.execute("SELECT COUNT(*),COUNT(DISTINCT token_mint),COUNT(DISTINCT wallet),MAX(timestamp) FROM t116_pump_swaps").fetchone()
            market.update(swaps=r[0] or 0, tokens=r[1] or 0, wallets=r[2] or 0)
            if r[3] is not None:
                market["latest_swap_age_s"] = now - float(r[3])
        if table_exists(mdb, "t101_migrations"):
            market["migrations"] = mdb.execute("SELECT COUNT(DISTINCT token_mint) FROM t101_migrations WHERE token_mint IS NOT NULL").fetchone()[0]
        mdb.close()
    except Exception as exc:
        market["error"] = repr(exc)

    return {
        "now": now,
        "jobs": jobs,
        "stages": stages,
        "verdicts": verdicts,
        "generations": generations,
        "memory": memory,
        "frozen": frozen,
        "hypotheses": hypotheses,
        "results": results,
        "workers": workers,
        "live_workers": active_run_workers,
        "branches": branches,
        "failure_causes": failure_causes.most_common(12),
        "failure_branches": failure_branches.most_common(),
        "failure_adapters": failure_adapters.most_common(),
        "recent_results": recent_results,
        "rates": rates,
        "queue": dict(queue) if queue else {},
        "market": market,
    }


def render():
    s = snapshot()
    j = s["jobs"]
    live = len(s["live_workers"])
    total_current = live
    failed = j.get("FAILED", 0)
    done = j.get("DONE", 0)
    total_terminal = done + failed
    fail_rate = (100*failed/total_terminal) if total_terminal else 0

    gen_rows = "".join(
        f'<div class="gen"><b>G{g["generation"]}</b><span>{num(g["n"])}</span></div>'
        for g in s["generations"]
    ) or '<span class="muted">none</span>'

    worker_rows = "".join(
        f'<tr><td>{esc(w["worker_id"])}</td><td>{num(w["pid"])}</td><td>{badge(w["health"], w["health"].lower())}</td><td>{esc(w["state"])}</td><td>{esc(w["current_job_id"] or "—")}</td><td>{num(w["jobs_done"])}</td><td>{num(w["jobs_failed"])}</td><td>{age(w["heartbeat_age_s"])}</td></tr>'
        for w in s["workers"][:24]
    ) or '<tr><td colspan="8" class="muted">No workers registered</td></tr>'

    branch_rows = "".join(
        f'<tr><td><b>{esc(b["branch"])}</b></td><td>{num(b["hypotheses"])}</td><td>{num(b["queued"] or 0)}</td><td>{num(b["running"] or 0)}</td><td>{num(b["done"] or 0)}</td><td>{num(b["failed"] or 0)}</td><td>{num(b["best_metric"],3)}</td><td>{num(b["best_effect"],2)}</td></tr>'
        for b in s["branches"]
    ) or '<tr><td colspan="8" class="muted">No branches</td></tr>'

    fail_rows = "".join(
        f'<tr><td>{esc(cause)}</td><td>{num(n)}</td><td>{100*n/failed if failed else 0:.1f}%</td></tr>'
        for cause, n in s["failure_causes"]
    ) or '<tr><td colspan="3" class="muted">No failures</td></tr>'

    adapter_rows = "".join(
        f'<tr><td>{esc(name)}</td><td>{num(n)}</td></tr>' for name, n in s["failure_adapters"]
    ) or '<tr><td colspan="2" class="muted">No failures</td></tr>'

    recent_rows = "".join(
        f'<tr><td>{time.strftime("%H:%M:%S", time.localtime(r["created_at"]))}</td><td>G{r["generation"]}</td><td>{esc(r["branch"])}</td><td>{esc(r["family"])}</td><td>{esc(r["stage"])}</td><td>{badge(r["verdict"], "good" if r["verdict"] in ("PROMISING","ROBUST") else "warn" if r["verdict"] in ("WEAK","COLLECT_MORE") else "bad")}</td><td>{num(r["primary_metric"],3)}</td><td>{num(r["effect_size"],2)}</td></tr>'
        for r in s["recent_results"]
    ) or '<tr><td colspan="8" class="muted">No results</td></tr>'

    market = s["market"]
    q_old = None
    if s["queue"].get("oldest"):
        q_old = s["now"] - float(s["queue"]["oldest"])

    return f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Memecoin Lab V4.2</title>
<style>
:root{{--bg:#080b11;--panel:#111620;--line:#263040;--text:#eef3fb;--muted:#8793a7;--green:#66d6a5;--amber:#f4c86a;--red:#ff7373;--blue:#7da7ff}}*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}.wrap{{max-width:1550px;margin:auto;padding:18px}}h1{{margin:0;font-size:24px}}h2{{font-size:12px;letter-spacing:.1em;text-transform:uppercase;color:#bcc7d8}}.muted{{color:var(--muted)}}.cards{{display:grid;grid-template-columns:repeat(8,1fr);gap:10px;margin:16px 0}}.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:12px}}.card{{padding:14px}}.label{{font-size:10px;color:var(--muted);letter-spacing:.1em;text-transform:uppercase}}.value{{font-size:26px;font-weight:750;margin-top:4px}}.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:12px 0}}.panel{{padding:14px;overflow:auto}}table{{width:100%;border-collapse:collapse}}th,td{{padding:8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}th{{color:var(--muted);font-size:10px;text-transform:uppercase}}.badge{{font-size:10px;font-weight:700;padding:3px 7px;border-radius:99px;background:#18202d}}.live,.good{{color:var(--green)}}.stale,.warn{{color:var(--amber)}}.dead,.bad{{color:var(--red)}}.gen{{display:inline-flex;gap:8px;background:#171f2b;border:1px solid var(--line);border-radius:8px;padding:7px 10px;margin:3px}}.gen span{{color:var(--blue)}}@media(max-width:1100px){{.cards{{grid-template-columns:repeat(2,1fr)}}.grid2{{grid-template-columns:1fr}}}}
</style><script>setTimeout(()=>location.reload(),{REFRESH_S*1000})</script></head><body><div class="wrap">
<h1>MEMECOIN LAB — RESEARCH CONTROL ROOM V4.2</h1><div class="muted">research-only · current-run worker health · failure diagnostics · refresh {REFRESH_S}s</div>
<div class="cards">
<div class="card"><div class="label">Live workers</div><div class="value">{live}</div></div><div class="card"><div class="label">Queue</div><div class="value">{num(j.get("QUEUED",0))}</div><div class="muted">oldest {age(q_old)}</div></div><div class="card"><div class="label">Running</div><div class="value">{num(j.get("RUNNING",0))}</div></div><div class="card"><div class="label">Done</div><div class="value">{num(done)}</div></div><div class="card"><div class="label">Failed</div><div class="value">{num(failed)}</div><div class="muted">{fail_rate:.1f}% terminal</div></div><div class="card"><div class="label">Memory</div><div class="value">{num(s["memory"])}</div></div><div class="card"><div class="label">Frozen</div><div class="value">{num(s["frozen"])}</div></div><div class="card"><div class="label">Results / 5m</div><div class="value">{num(s["rates"]["5m"])}</div></div>
</div>
<div class="cards"><div class="card"><div class="label">Tokens</div><div class="value">{num(market["tokens"])}</div></div><div class="card"><div class="label">Swaps</div><div class="value">{num(market["swaps"])}</div></div><div class="card"><div class="label">Wallets</div><div class="value">{num(market["wallets"])}</div></div><div class="card"><div class="label">Migrations</div><div class="value">{num(market["migrations"])}</div></div><div class="card"><div class="label">Newest swap</div><div class="value">{age(market["latest_swap_age_s"])}</div></div><div class="card"><div class="label">Hypotheses</div><div class="value">{num(s["hypotheses"])}</div></div><div class="card"><div class="label">Results</div><div class="value">{num(s["results"])}</div></div><div class="card"><div class="label">Results / 1h</div><div class="value">{num(s["rates"]["1h"])}</div></div></div>
<div class="panel"><h2>Generation funnel</h2>{gen_rows}<div style="margin-top:10px"><b>Stages:</b> {esc(s["stages"])} &nbsp; <b>Verdicts:</b> {esc(s["verdicts"])}</div></div>
<div class="grid2"><div class="panel"><h2>Branches</h2><table><tr><th>Branch</th><th>Hyp</th><th>Q</th><th>Run</th><th>Done</th><th>Fail</th><th>Best metric</th><th>Best effect</th></tr>{branch_rows}</table></div><div class="panel"><h2>Worker health</h2><table><tr><th>Worker</th><th>PID</th><th>Health</th><th>State</th><th>Job</th><th>Done</th><th>Fail</th><th>Heartbeat</th></tr>{worker_rows}</table></div></div>
<div class="grid2"><div class="panel"><h2>Failure root causes</h2><table><tr><th>Cause</th><th>N</th><th>% failures</th></tr>{fail_rows}</table></div><div class="panel"><h2>Failure adapters</h2><table><tr><th>Adapter</th><th>N</th></tr>{adapter_rows}</table><div class="muted" style="margin-top:12px">Failures are diagnosed only; this dashboard never auto-requeues them.</div></div></div>
<div class="panel"><h2>Latest results</h2><table><tr><th>Time</th><th>Gen</th><th>Branch</th><th>Family</th><th>Stage</th><th>Verdict</th><th>Metric</th><th>Effect</th></tr>{recent_rows}</table></div>
</div></body></html>'''


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        return

    def do_GET(self):
        if self.path == "/health":
            body = b"ok"
            self.send_response(200); self.send_header("Content-Type", "text/plain"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body); return
        if self.path == "/api/status":
            body = json.dumps(snapshot(), default=list).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body); return
        body = render().encode()
        self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)


def main():
    core.initialize()
    print("=" * 92)
    print("MEMECOIN LAB — V4.2 CONTROL ROOM")
    print("=" * 92)
    print(f"Dashboard: http://{HOST}:{PORT}")
    print("Current-run worker health + failure diagnostics")
    print("CTRL+C stops dashboard only")
    print("=" * 92)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
