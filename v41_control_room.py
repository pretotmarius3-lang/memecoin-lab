#!/usr/bin/env python3
"""Memecoin Lab V4.1 visual control room.

Zero external dependencies. Serves a local dashboard plus small JSON endpoints that
can later be wrapped by an MCP server without changing the research core.

Run:
    python3 v41_control_room.py

Open:
    http://127.0.0.1:8765
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import v41_core as core

HOST = os.environ.get("MEMECOIN_CONTROL_HOST", "127.0.0.1")
PORT = int(os.environ.get("MEMECOIN_CONTROL_PORT", "8765"))


def _one(db: sqlite3.Connection, sql: str, params=()):
    row = db.execute(sql, params).fetchone()
    return dict(row) if row else None


def _all(db: sqlite3.Connection, sql: str, params=()):
    return [dict(r) for r in db.execute(sql, params).fetchall()]


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def market_snapshot() -> dict:
    out = {
        "market_db": str(core.MARKET_DB),
        "available": False,
        "tokens": None,
        "swaps": None,
        "migrations": None,
        "wallets": None,
        "latest_swap_age_s": None,
    }
    try:
        db = core.open_market()
        out["available"] = True
        if table_exists(db, "t116_pump_swaps"):
            row = _one(
                db,
                """
                SELECT COUNT(*) swaps,
                       COUNT(DISTINCT token_mint) tokens,
                       COUNT(DISTINCT wallet) wallets,
                       MAX(timestamp) latest_ts
                FROM t116_pump_swaps
                """,
            )
            if row:
                out.update({k: row[k] for k in ("swaps", "tokens", "wallets")})
                if row.get("latest_ts") is not None:
                    out["latest_swap_age_s"] = max(0.0, time.time() - float(row["latest_ts"]))
        if table_exists(db, "t101_migrations"):
            row = _one(db, "SELECT COUNT(DISTINCT token_mint) migrations FROM t101_migrations WHERE token_mint IS NOT NULL")
            if row:
                out["migrations"] = row["migrations"]
        db.close()
    except Exception as exc:
        out["error"] = repr(exc)
    return out


def research_snapshot() -> dict:
    core.initialize()
    db = core.open_research()
    now = time.time()

    jobs = {r["status"]: r["n"] for r in _all(db, "SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    hypotheses = {r["status"]: r["n"] for r in _all(db, "SELECT status,COUNT(*) n FROM v41_hypotheses GROUP BY status")}
    branches = _all(
        db,
        """
        SELECT h.branch,
               COUNT(DISTINCT h.hypothesis_id) hypotheses,
               SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
               SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
               SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
               SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed
        FROM v41_hypotheses h
        LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
        GROUP BY h.branch
        ORDER BY running DESC, queued DESC, hypotheses DESC
        """,
    )
    workers = _all(
        db,
        """
        SELECT worker_id,pid,state,current_job_id,jobs_done,jobs_failed,
               (? - last_heartbeat) heartbeat_age_s
        FROM v41_workers
        ORDER BY state='RUNNING' DESC, worker_id
        """,
        (now,),
    )
    recent = _all(
        db,
        """
        SELECT r.created_at,r.verdict,r.stage,r.primary_metric,r.effect_size,r.p_value,
               h.branch,h.family,h.hypothesis_id
        FROM v41_results r
        JOIN v41_hypotheses h ON h.hypothesis_id=r.hypothesis_id
        ORDER BY r.created_at DESC
        LIMIT 25
        """,
    )
    top = _all(
        db,
        """
        SELECT r.created_at,r.verdict,r.stage,r.primary_metric,r.effect_size,r.p_value,
               r.discovery_n,r.holdout_n,r.positives,
               h.branch,h.family,h.hypothesis_id
        FROM v41_results r
        JOIN v41_hypotheses h ON h.hypothesis_id=r.hypothesis_id
        WHERE r.verdict IN ('ROBUST','PROMISING','PASS','PASS_HOLDOUT','WEAK')
        ORDER BY
          CASE r.verdict
            WHEN 'ROBUST' THEN 0 WHEN 'PROMISING' THEN 1 WHEN 'PASS' THEN 2
            WHEN 'PASS_HOLDOUT' THEN 3 ELSE 4 END,
          COALESCE(r.primary_metric,-999) DESC
        LIMIT 20
        """,
    )
    candidates = _all(
        db,
        """
        SELECT candidate_id,hypothesis_id,status,data_cutoff,created_at,updated_at
        FROM v41_candidates ORDER BY updated_at DESC LIMIT 20
        """,
    )
    datasets = _all(
        db,
        "SELECT dataset_id,lane,description,updated_at FROM v41_dataset_registry ORDER BY lane,dataset_id",
    )
    memory = {r["verdict"]: r["n"] for r in _all(db, "SELECT verdict,COUNT(*) n FROM v41_memory GROUP BY verdict")}

    one_minute = now - 60
    rate = _one(
        db,
        "SELECT COUNT(*) n FROM v41_results WHERE created_at>=?",
        (one_minute,),
    )["n"]

    db.close()
    return {
        "timestamp": now,
        "research_db": str(core.RESEARCH_DB),
        "jobs": jobs,
        "hypotheses": hypotheses,
        "branches": branches,
        "workers": workers,
        "recent_results": recent,
        "top_results": top,
        "candidates": candidates,
        "datasets": datasets,
        "memory": memory,
        "results_per_min": rate,
    }


def full_snapshot() -> dict:
    return {
        "research": research_snapshot(),
        "market": market_snapshot(),
    }


def esc(value) -> str:
    text = "" if value is None else str(value)
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;"))


def num(value, digits=2) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return f"{value:,}"
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return esc(value)


def age(value) -> str:
    if value is None:
        return "—"
    value = float(value)
    if value < 60:
        return f"{value:.0f}s"
    if value < 3600:
        return f"{value/60:.1f}m"
    return f"{value/3600:.1f}h"


def badge(status: str) -> str:
    s = (status or "UNKNOWN").upper()
    good = {"RUNNING", "DONE", "ROBUST", "PASS", "PASS_HOLDOUT", "PROMISING", "READY"}
    bad = {"FAILED", "ERROR", "REJECT", "REJECTED"}
    cls = "good" if s in good else "bad" if s in bad else "warn"
    return f'<span class="badge {cls}">{esc(s)}</span>'


def render() -> str:
    snap = full_snapshot()
    r, m = snap["research"], snap["market"]
    j = r["jobs"]
    active_workers = sum(1 for w in r["workers"] if w["state"] in ("RUNNING", "BUSY") and w["heartbeat_age_s"] < 30)
    branch_rows = "".join(
        f"<tr><td>{esc(x['branch'])}</td><td>{num(x['hypotheses'])}</td><td>{num(x['running'] or 0)}</td><td>{num(x['queued'] or 0)}</td><td>{num(x['done'] or 0)}</td><td>{num(x['failed'] or 0)}</td></tr>"
        for x in r["branches"]
    ) or '<tr><td colspan="6" class="muted">No research branches yet</td></tr>'
    worker_rows = "".join(
        f"<tr><td>{esc(w['worker_id'])}</td><td>{badge(w['state'])}</td><td>{esc(w['current_job_id'] or '—')}</td><td>{num(w['jobs_done'])}</td><td>{num(w['jobs_failed'])}</td><td>{age(w['heartbeat_age_s'])}</td></tr>"
        for w in r["workers"]
    ) or '<tr><td colspan="6" class="muted">Worker pool not started yet</td></tr>'
    result_rows = "".join(
        f"<tr><td>{time.strftime('%H:%M:%S', time.localtime(x['created_at']))}</td><td>{esc(x['branch'])}</td><td>{esc(x['family'])}</td><td>{badge(x['verdict'])}</td><td>{num(x['primary_metric'],3)}</td><td>{num(x['effect_size'],3)}</td><td>{num(x['p_value'],4)}</td></tr>"
        for x in r["recent_results"]
    ) or '<tr><td colspan="7" class="muted">No V4.1 results yet</td></tr>'
    candidate_rows = "".join(
        f"<tr><td>{esc(x['candidate_id'])}</td><td>{esc(x['hypothesis_id'])}</td><td>{badge(x['status'])}</td><td>{time.strftime('%Y-%m-%d %H:%M', time.localtime(x['data_cutoff']))}</td></tr>"
        for x in r["candidates"]
    ) or '<tr><td colspan="4" class="muted">No frozen candidates yet</td></tr>'

    return f'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Memecoin Lab V4.1 Control Room</title>
<style>
:root{{--bg:#090b10;--panel:#11151d;--panel2:#171c26;--text:#e9eef7;--muted:#8792a6;--line:#242c39;--green:#42d392;--red:#ff6b6b;--amber:#f7c948;--blue:#6ea8fe}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}.wrap{{max-width:1500px;margin:auto;padding:22px}}h1{{font-size:24px;margin:0}}h2{{font-size:14px;letter-spacing:.08em;text-transform:uppercase;color:#b6c2d5;margin:0 0 14px}}.top{{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px}}.muted{{color:var(--muted)}}.grid{{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:14px}}.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:12px}}.card{{padding:16px}}.k{{font-size:11px;letter-spacing:.08em;color:var(--muted);text-transform:uppercase}}.v{{font-size:26px;font-weight:700;margin-top:5px}}.panels{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}.panel{{padding:16px;overflow:auto}}table{{border-collapse:collapse;width:100%;min-width:550px}}th,td{{padding:9px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}th{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.05em}}.badge{{font-size:10px;font-weight:700;border-radius:999px;padding:3px 7px;background:var(--panel2)}}.good{{color:var(--green)}}.bad{{color:var(--red)}}.warn{{color:var(--amber)}}.dot{{width:8px;height:8px;border-radius:50%;display:inline-block;background:var(--green);margin-right:6px}}code{{color:#a9c7ff}}@media(max-width:1000px){{.grid{{grid-template-columns:repeat(2,1fr)}}.panels{{grid-template-columns:1fr}}}}
</style>
<script>setTimeout(()=>location.reload(),5000)</script></head>
<body><div class="wrap">
<div class="top"><div><h1>MEMECOIN LAB — CONTROL ROOM V4.1</h1><div class="muted">Research-only · auto refresh 5s · <code>/api/status</code> MCP-ready</div></div><div><span class="dot"></span>LOCAL</div></div>
<div class="grid">
<div class="card"><div class="k">Active workers</div><div class="v">{active_workers}</div></div>
<div class="card"><div class="k">Queued</div><div class="v">{num(j.get('QUEUED',0))}</div></div>
<div class="card"><div class="k">Running</div><div class="v">{num(j.get('RUNNING',0))}</div></div>
<div class="card"><div class="k">Done</div><div class="v">{num(j.get('DONE',0))}</div></div>
<div class="card"><div class="k">Results / min</div><div class="v">{num(r['results_per_min'])}</div></div>
<div class="card"><div class="k">Failed</div><div class="v">{num(j.get('FAILED',0))}</div></div>
</div>
<div class="grid">
<div class="card"><div class="k">Observed tokens</div><div class="v">{num(m.get('tokens'))}</div></div>
<div class="card"><div class="k">Observed swaps</div><div class="v">{num(m.get('swaps'))}</div></div>
<div class="card"><div class="k">Wallets</div><div class="v">{num(m.get('wallets'))}</div></div>
<div class="card"><div class="k">Migrations</div><div class="v">{num(m.get('migrations'))}</div></div>
<div class="card"><div class="k">Newest swap age</div><div class="v">{age(m.get('latest_swap_age_s'))}</div></div>
<div class="card"><div class="k">Frozen candidates</div><div class="v">{num(len(r['candidates']))}</div></div>
</div>
<div class="panels">
<div class="panel"><h2>Research branches</h2><table><thead><tr><th>Branch</th><th>Hyp.</th><th>Running</th><th>Queued</th><th>Done</th><th>Failed</th></tr></thead><tbody>{branch_rows}</tbody></table></div>
<div class="panel"><h2>Worker pool</h2><table><thead><tr><th>Worker</th><th>State</th><th>Job</th><th>Done</th><th>Fail</th><th>Heartbeat</th></tr></thead><tbody>{worker_rows}</tbody></table></div>
</div>
<div class="panel" style="margin-bottom:14px"><h2>Latest research results</h2><table><thead><tr><th>Time</th><th>Branch</th><th>Family</th><th>Verdict</th><th>Metric</th><th>Effect</th><th>P</th></tr></thead><tbody>{result_rows}</tbody></table></div>
<div class="panel"><h2>Frozen / prospective candidates</h2><table><thead><tr><th>Candidate</th><th>Hypothesis</th><th>Status</th><th>Data cutoff</th></tr></thead><tbody>{candidate_rows}</tbody></table></div>
</div></body></html>'''


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def _json(self, payload, code=200):
        raw = json.dumps(payload, separators=(",", ":"), default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/status":
                return self._json(full_snapshot())
            if path == "/api/research":
                return self._json(research_snapshot())
            if path == "/api/market":
                return self._json(market_snapshot())
            if path == "/health":
                return self._json({"ok": True, "time": time.time()})
            if path == "/":
                raw = render().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(raw)
                return
            self._json({"error": "not found"}, 404)
        except Exception as exc:
            self._json({"error": repr(exc)}, 500)


def main():
    core.initialize()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print("=" * 90)
    print("MEMECOIN LAB — V4.1 CONTROL ROOM")
    print("=" * 90)
    print(f"Dashboard : http://{HOST}:{PORT}")
    print(f"JSON API  : http://{HOST}:{PORT}/api/status")
    print("Mode      : LOCAL / READ-ONLY CONTROL SURFACE")
    print("MCP       : API contract ready for wrapper")
    print("CTRL+C stops dashboard only")
    print("=" * 90)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
