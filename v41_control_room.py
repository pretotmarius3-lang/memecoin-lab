#!/usr/bin/env python3
"""Memecoin Lab V4.1 detailed visual control room.

Zero external dependencies. Serves a local research cockpit plus JSON endpoints that
can later be wrapped by an MCP server without changing the research core.

Run:
    python3 v41_control_room.py

Open:
    http://127.0.0.1:8765
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import v41_core as core

HOST = os.environ.get("MEMECOIN_CONTROL_HOST", "127.0.0.1")
PORT = int(os.environ.get("MEMECOIN_CONTROL_PORT", "8765"))
REFRESH_S = int(os.environ.get("MEMECOIN_CONTROL_REFRESH_S", "3"))


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


def columns(db: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(db, table):
        return set()
    return {r["name"] for r in db.execute(f"PRAGMA table_info({table})").fetchall()}


def file_size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None


def safe_float(v):
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def market_snapshot() -> dict:
    out = {
        "market_db": str(core.MARKET_DB),
        "db_size_bytes": file_size(core.MARKET_DB),
        "available": False,
        "tokens": 0,
        "swaps": 0,
        "migrations": 0,
        "wallets": 0,
        "multi_token_wallets": 0,
        "latest_swap_age_s": None,
        "latest_migration_age_s": None,
        "swap_coverage": {},
        "duration_coverage": {},
        "field_completeness": {},
        "top_tokens": [],
        "tables": {},
    }
    try:
        db = core.open_market()
        out["available"] = True

        for table in (
            "t116_pump_swaps",
            "t116_pump_events",
            "t116_premigration_dump_events",
            "t101_migrations",
        ):
            if table_exists(db, table):
                out["tables"][table] = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        if table_exists(db, "t116_pump_swaps"):
            c = columns(db, "t116_pump_swaps")
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
                out["swaps"] = row.get("swaps") or 0
                out["tokens"] = row.get("tokens") or 0
                out["wallets"] = row.get("wallets") or 0
                if row.get("latest_ts") is not None:
                    out["latest_swap_age_s"] = max(0.0, time.time() - float(row["latest_ts"]))

            if "wallet" in c:
                out["multi_token_wallets"] = db.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT wallet
                        FROM t116_pump_swaps
                        WHERE wallet IS NOT NULL
                        GROUP BY wallet
                        HAVING COUNT(DISTINCT token_mint)>1
                    )
                    """
                ).fetchone()[0]

            coverage = _one(
                db,
                """
                WITH x AS (
                    SELECT token_mint, COUNT(*) n, MAX(timestamp)-MIN(timestamp) duration_s
                    FROM t116_pump_swaps
                    WHERE token_mint IS NOT NULL AND timestamp IS NOT NULL
                    GROUP BY token_mint
                )
                SELECT
                    SUM(n=1) one_swap,
                    SUM(n BETWEEN 2 AND 5) two_five,
                    SUM(n BETWEEN 6 AND 20) six_twenty,
                    SUM(n BETWEEN 21 AND 100) twentyone_hundred,
                    SUM(n>=101) hundred_plus,
                    SUM(n>=5) ge5,
                    SUM(n>=10) ge10,
                    SUM(n>=20) ge20,
                    SUM(n>=5 AND duration_s>=60) deep_5_60,
                    SUM(n>=10 AND duration_s>=120) deep_10_120,
                    SUM(n>=20 AND duration_s>=300) deep_20_300,
                    SUM(duration_s=0) dur0,
                    SUM(duration_s>0 AND duration_s<10) dur_lt10,
                    SUM(duration_s>=10 AND duration_s<60) dur_10_60,
                    SUM(duration_s>=60 AND duration_s<300) dur_60_300,
                    SUM(duration_s>=300 AND duration_s<1800) dur_300_1800,
                    SUM(duration_s>=1800) dur_1800_plus
                FROM x
                """,
            ) or {}
            out["swap_coverage"] = {
                k: coverage.get(k, 0)
                for k in ("one_swap", "two_five", "six_twenty", "twentyone_hundred", "hundred_plus", "ge5", "ge10", "ge20", "deep_5_60", "deep_10_120", "deep_20_300")
            }
            out["duration_coverage"] = {
                k: coverage.get(k, 0)
                for k in ("dur0", "dur_lt10", "dur_10_60", "dur_60_300", "dur_300_1800", "dur_1800_plus")
            }

            for field in ("wallet", "signature", "raw_price_sol", "side", "sol_delta"):
                if field in c:
                    n = db.execute(f"SELECT COUNT(*) FROM t116_pump_swaps WHERE {field} IS NOT NULL").fetchone()[0]
                    out["field_completeness"][field] = n

            out["top_tokens"] = _all(
                db,
                """
                SELECT token_mint,COUNT(*) swaps,MAX(timestamp)-MIN(timestamp) duration_s,
                       COUNT(DISTINCT wallet) wallets
                FROM t116_pump_swaps
                WHERE token_mint IS NOT NULL
                GROUP BY token_mint
                ORDER BY swaps DESC,duration_s DESC
                LIMIT 12
                """,
            )

        if table_exists(db, "t101_migrations"):
            c = columns(db, "t101_migrations")
            out["migrations"] = db.execute(
                "SELECT COUNT(DISTINCT token_mint) FROM t101_migrations WHERE token_mint IS NOT NULL"
            ).fetchone()[0]
            time_col = next((x for x in ("block_time", "detected_at", "timestamp", "created_at") if x in c), None)
            if time_col:
                newest = db.execute(f"SELECT MAX({time_col}) FROM t101_migrations").fetchone()[0]
                if newest is not None:
                    out["latest_migration_age_s"] = max(0.0, time.time() - float(newest))

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
    verdicts = {r["verdict"]: r["n"] for r in _all(db, "SELECT verdict,COUNT(*) n FROM v41_results GROUP BY verdict")}
    memory = {r["verdict"]: r["n"] for r in _all(db, "SELECT verdict,COUNT(*) n FROM v41_memory GROUP BY verdict")}

    branches = _all(
        db,
        """
        SELECT h.branch,
               COUNT(DISTINCT h.hypothesis_id) hypotheses,
               SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
               SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
               SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
               SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed,
               MAX(r.primary_metric) best_metric,
               MAX(r.effect_size) best_effect,
               MIN(r.p_value) best_p,
               MAX(r.created_at) last_result_at
        FROM v41_hypotheses h
        LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
        LEFT JOIN v41_results r ON r.hypothesis_id=h.hypothesis_id
        GROUP BY h.branch
        ORDER BY running DESC, queued DESC, best_metric DESC, hypotheses DESC
        """,
    )

    workers = _all(
        db,
        """
        SELECT worker_id,pid,state,current_job_id,jobs_done,jobs_failed,
               (? - last_heartbeat) heartbeat_age_s,
               (? - started_at) uptime_s
        FROM v41_workers
        ORDER BY state IN ('RUNNING','BUSY') DESC, worker_id
        """,
        (now, now),
    )

    recent_jobs = _all(
        db,
        """
        SELECT j.job_id,j.job_type,j.priority,j.status,j.worker_id,j.attempts,j.error,
               j.created_at,j.started_at,j.finished_at,j.lease_until,
               h.branch,h.family,h.hypothesis_id
        FROM v41_jobs j
        JOIN v41_hypotheses h ON h.hypothesis_id=j.hypothesis_id
        ORDER BY COALESCE(j.started_at,j.created_at) DESC
        LIMIT 30
        """,
    )
    for x in recent_jobs:
        x["queue_age_s"] = max(0.0, now - x["created_at"]) if x.get("created_at") else None
        x["runtime_s"] = (
            max(0.0, (x.get("finished_at") or now) - x["started_at"])
            if x.get("started_at") else None
        )

    recent = _all(
        db,
        """
        SELECT r.result_id,r.created_at,r.verdict,r.stage,r.primary_metric,r.effect_size,r.p_value,
               r.adjusted_p_value,r.discovery_n,r.holdout_n,r.positives,
               h.branch,h.family,h.hypothesis_id
        FROM v41_results r
        JOIN v41_hypotheses h ON h.hypothesis_id=r.hypothesis_id
        ORDER BY r.created_at DESC
        LIMIT 40
        """,
    )

    top = _all(
        db,
        """
        SELECT r.result_id,r.created_at,r.verdict,r.stage,r.primary_metric,r.effect_size,r.p_value,
               r.adjusted_p_value,r.discovery_n,r.holdout_n,r.positives,
               h.branch,h.family,h.hypothesis_id
        FROM v41_results r
        JOIN v41_hypotheses h ON h.hypothesis_id=r.hypothesis_id
        WHERE r.verdict IN ('ROBUST','PROMISING','PASS','PASS_HOLDOUT','WEAK')
        ORDER BY
          CASE r.verdict
            WHEN 'ROBUST' THEN 0 WHEN 'PROMISING' THEN 1 WHEN 'PASS' THEN 2
            WHEN 'PASS_HOLDOUT' THEN 3 ELSE 4 END,
          COALESCE(r.primary_metric,-999) DESC,
          COALESCE(r.effect_size,-999) DESC
        LIMIT 30
        """,
    )

    candidates = _all(
        db,
        """
        SELECT candidate_id,hypothesis_id,status,data_cutoff,created_at,updated_at
        FROM v41_candidates ORDER BY updated_at DESC LIMIT 30
        """,
    )
    datasets = _all(
        db,
        "SELECT dataset_id,lane,description,requirements_json,snapshot_json,updated_at FROM v41_dataset_registry ORDER BY lane,dataset_id",
    )

    rates = {}
    for label, seconds in (("1m", 60), ("5m", 300), ("15m", 900), ("1h", 3600)):
        n = _one(db, "SELECT COUNT(*) n FROM v41_results WHERE created_at>=?", (now - seconds,))["n"]
        rates[label] = n

    queue_stats = _one(
        db,
        """
        SELECT COUNT(*) queued,
               MIN(created_at) oldest_created,
               AVG(?-created_at) avg_age_s,
               MAX(?-created_at) max_age_s
        FROM v41_jobs WHERE status='QUEUED'
        """,
        (now, now),
    ) or {}
    queue_stats["oldest_age_s"] = max(0.0, now - queue_stats["oldest_created"]) if queue_stats.get("oldest_created") else None

    completed_stats = _one(
        db,
        """
        SELECT COUNT(*) n,
               AVG(finished_at-started_at) avg_runtime_s,
               MIN(finished_at-started_at) min_runtime_s,
               MAX(finished_at-started_at) max_runtime_s
        FROM v41_jobs
        WHERE status='DONE' AND started_at IS NOT NULL AND finished_at IS NOT NULL
        """,
    ) or {}

    totals = _one(
        db,
        """
        SELECT
          (SELECT COUNT(*) FROM v41_hypotheses) hypotheses_total,
          (SELECT COUNT(*) FROM v41_jobs) jobs_total,
          (SELECT COUNT(*) FROM v41_results) results_total,
          (SELECT COUNT(*) FROM v41_candidates) candidates_total,
          (SELECT COUNT(*) FROM v41_memory) memory_total
        """,
    ) or {}

    db.close()
    return {
        "timestamp": now,
        "research_db": str(core.RESEARCH_DB),
        "db_size_bytes": file_size(core.RESEARCH_DB),
        "jobs": jobs,
        "hypotheses": hypotheses,
        "verdicts": verdicts,
        "branches": branches,
        "workers": workers,
        "recent_jobs": recent_jobs,
        "recent_results": recent,
        "top_results": top,
        "candidates": candidates,
        "datasets": datasets,
        "memory": memory,
        "rates": rates,
        "queue_stats": queue_stats,
        "completed_stats": completed_stats,
        "totals": totals,
    }


def full_snapshot() -> dict:
    return {"research": research_snapshot(), "market": market_snapshot(), "server_time": time.time()}


def esc(value) -> str:
    text = "" if value is None else str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def num(value, digits=2) -> str:
    if value is None:
        return "—"
    if isinstance(value, int):
        return f"{value:,}"
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return esc(value)


def pct(value, total, digits=1) -> str:
    if value is None or not total:
        return "—"
    return f"{100.0*float(value)/float(total):.{digits}f}%"


def bytes_text(value) -> str:
    if value is None:
        return "—"
    x = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if x < 1024 or unit == "TB":
            return f"{x:.1f} {unit}"
        x /= 1024
    return "—"


def age(value) -> str:
    if value is None:
        return "—"
    value = float(value)
    if value < 60:
        return f"{value:.0f}s"
    if value < 3600:
        return f"{value/60:.1f}m"
    if value < 86400:
        return f"{value/3600:.1f}h"
    return f"{value/86400:.1f}d"


def date_time(ts) -> str:
    if ts is None:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))


def badge(status: str) -> str:
    s = (status or "UNKNOWN").upper()
    good = {"RUNNING", "BUSY", "DONE", "ROBUST", "PASS", "PASS_HOLDOUT", "PROMISING", "READY", "FROZEN"}
    bad = {"FAILED", "ERROR", "REJECT", "REJECTED", "CANCELLED"}
    cls = "good" if s in good else "bad" if s in bad else "warn"
    return f'<span class="badge {cls}">{esc(s)}</span>'


def bar(value, total, cls="blue") -> str:
    width = 0 if not total else max(0, min(100, 100 * float(value or 0) / float(total)))
    return f'<div class="bar"><i class="{cls}" style="width:{width:.1f}%"></i></div>'


def render() -> str:
    snap = full_snapshot()
    r, m = snap["research"], snap["market"]
    j = r["jobs"]
    total_tokens = m.get("tokens") or 0
    total_swaps = m.get("swaps") or 0
    total_jobs = r["totals"].get("jobs_total") or 0
    done_jobs = j.get("DONE", 0)
    failed_jobs = j.get("FAILED", 0)
    active_workers = sum(
        1 for w in r["workers"]
        if w["state"] in ("RUNNING", "BUSY") and (w["heartbeat_age_s"] or 999999) < 30
    )
    total_workers = len(r["workers"])

    branch_rows = "".join(
        f"<tr><td><b>{esc(x['branch'])}</b></td><td>{num(x['hypotheses'])}</td><td>{num(x['running'] or 0)}</td><td>{num(x['queued'] or 0)}</td><td>{num(x['done'] or 0)}</td><td>{num(x['failed'] or 0)}</td><td>{num(x['best_metric'],3)}</td><td>{num(x['best_effect'],3)}</td><td>{num(x['best_p'],4)}</td><td>{date_time(x['last_result_at'])}</td></tr>"
        for x in r["branches"]
    ) or '<tr><td colspan="10" class="muted">No research branches yet</td></tr>'

    worker_rows = "".join(
        f"<tr><td><b>{esc(w['worker_id'])}</b></td><td>{num(w['pid'])}</td><td>{badge(w['state'])}</td><td class="mono">{esc(w['current_job_id'] or '—')}</td><td>{num(w['jobs_done'])}</td><td>{num(w['jobs_failed'])}</td><td>{age(w['heartbeat_age_s'])}</td><td>{age(w['uptime_s'])}</td></tr>"
        for w in r["workers"]
    ) or '<tr><td colspan="8" class="muted">Worker pool not started yet</td></tr>'

    job_rows = "".join(
        f"<tr><td class="mono">{esc(x['job_id'])}</td><td>{esc(x['branch'])}</td><td>{esc(x['family'])}</td><td>{esc(x['job_type'])}</td><td>{badge(x['status'])}</td><td>{num(x['priority'])}</td><td>{esc(x['worker_id'] or '—')}</td><td>{num(x['attempts'])}</td><td>{age(x['queue_age_s'])}</td><td>{age(x['runtime_s'])}</td><td class="errorcell">{esc((x['error'] or '')[-120:])}</td></tr>"
        for x in r["recent_jobs"]
    ) or '<tr><td colspan="11" class="muted">No V4.1 jobs yet</td></tr>'

    result_rows = "".join(
        f"<tr><td>{date_time(x['created_at'])}</td><td>{esc(x['branch'])}</td><td>{esc(x['family'])}</td><td>{badge(x['verdict'])}</td><td>{esc(x['stage'])}</td><td>{num(x['discovery_n'])}</td><td>{num(x['holdout_n'])}</td><td>{num(x['positives'])}</td><td>{num(x['primary_metric'],3)}</td><td>{num(x['effect_size'],3)}</td><td>{num(x['p_value'],4)}</td><td>{num(x['adjusted_p_value'],4)}</td></tr>"
        for x in r["recent_results"]
    ) or '<tr><td colspan="12" class="muted">No V4.1 results yet</td></tr>'

    top_rows = "".join(
        f"<tr><td>{esc(x['branch'])}</td><td>{esc(x['family'])}</td><td>{badge(x['verdict'])}</td><td>{num(x['primary_metric'],3)}</td><td>{num(x['effect_size'],3)}</td><td>{num(x['p_value'],4)}</td><td>{num(x['adjusted_p_value'],4)}</td><td>{num(x['discovery_n'])}</td><td>{num(x['holdout_n'])}</td><td class="mono">{esc(x['hypothesis_id'])}</td></tr>"
        for x in r["top_results"]
    ) or '<tr><td colspan="10" class="muted">No promoted results yet</td></tr>'

    candidate_rows = "".join(
        f"<tr><td class="mono">{esc(x['candidate_id'])}</td><td class="mono">{esc(x['hypothesis_id'])}</td><td>{badge(x['status'])}</td><td>{date_time(x['data_cutoff'])}</td><td>{date_time(x['updated_at'])}</td></tr>"
        for x in r["candidates"]
    ) or '<tr><td colspan="5" class="muted">No frozen candidates yet</td></tr>'

    top_token_rows = "".join(
        f"<tr><td class="mono">{esc(str(x['token_mint'])[:22])}</td><td>{num(x['swaps'])}</td><td>{num(x['wallets'])}</td><td>{age(x['duration_s'])}</td></tr>"
        for x in m["top_tokens"]
    ) or '<tr><td colspan="4" class="muted">No token coverage data</td></tr>'

    datasets = "".join(
        f"<tr><td class="mono">{esc(x['dataset_id'])}</td><td>{esc(x['lane'])}</td><td>{esc(x['description'])}</td><td>{date_time(x['updated_at'])}</td></tr>"
        for x in r["datasets"]
    ) or '<tr><td colspan="4" class="muted">No datasets registered yet</td></tr>'

    coverage = m["swap_coverage"]
    coverage_rows = "".join([
        f"<div class='metricline'><span>1 swap</span><b>{num(coverage.get('one_swap',0))} <em>{pct(coverage.get('one_swap',0),total_tokens)}</em></b>{bar(coverage.get('one_swap',0),total_tokens,'red')}</div>",
        f"<div class='metricline'><span>2–5 swaps</span><b>{num(coverage.get('two_five',0))} <em>{pct(coverage.get('two_five',0),total_tokens)}</em></b>{bar(coverage.get('two_five',0),total_tokens,'amber')}</div>",
        f"<div class='metricline'><span>6–20 swaps</span><b>{num(coverage.get('six_twenty',0))} <em>{pct(coverage.get('six_twenty',0),total_tokens)}</em></b>{bar(coverage.get('six_twenty',0),total_tokens,'blue')}</div>",
        f"<div class='metricline'><span>21–100 swaps</span><b>{num(coverage.get('twentyone_hundred',0))} <em>{pct(coverage.get('twentyone_hundred',0),total_tokens)}</em></b>{bar(coverage.get('twentyone_hundred',0),total_tokens,'green')}</div>",
        f"<div class='metricline'><span>Deep ≥20 swaps + ≥5m</span><b>{num(coverage.get('deep_20_300',0))} <em>{pct(coverage.get('deep_20_300',0),total_tokens)}</em></b>{bar(coverage.get('deep_20_300',0),total_tokens,'green')}</div>",
    ])

    fields = m["field_completeness"]
    field_rows = "".join(
        f"<div class='metricline'><span>{esc(name)}</span><b>{num(n)} <em>{pct(n,total_swaps)}</em></b>{bar(n,total_swaps,'green')}</div>"
        for name, n in fields.items()
    ) or '<div class="muted">No completeness data</div>'

    hypothesis_chips = " ".join(f"{badge(k)} <span class='chipn'>{num(v)}</span>" for k,v in sorted(r['hypotheses'].items())) or '<span class="muted">none</span>'
    verdict_chips = " ".join(f"{badge(k)} <span class='chipn'>{num(v)}</span>" for k,v in sorted(r['verdicts'].items())) or '<span class="muted">none</span>'
    memory_chips = " ".join(f"{badge(k)} <span class='chipn'>{num(v)}</span>" for k,v in sorted(r['memory'].items())) or '<span class="muted">none</span>'

    fail_rate = (100.0 * failed_jobs / max(1, done_jobs + failed_jobs))
    worker_util = 100.0 * active_workers / max(1, total_workers)
    queue_age = r['queue_stats'].get('oldest_age_s')

    return f'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Memecoin Lab V4.1 Control Room</title>
<style>
:root{{--bg:#070910;--panel:#10141d;--panel2:#151b26;--text:#edf2fa;--muted:#8793a8;--line:#252d3a;--green:#45d69d;--red:#ff6d7a;--amber:#f6c85f;--blue:#70a7ff;--purple:#b18cff}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:13px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}.wrap{{max-width:1780px;margin:auto;padding:20px}}h1{{font-size:24px;margin:0}}h2{{font-size:12px;letter-spacing:.10em;text-transform:uppercase;color:#b9c6da;margin:0 0 13px}}.top{{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}}.muted,em{{color:var(--muted);font-style:normal}}.grid{{display:grid;grid-template-columns:repeat(8,1fr);gap:10px;margin-bottom:12px}}.grid6{{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:12px}}.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:11px}}.card{{padding:14px}}.k{{font-size:10px;letter-spacing:.09em;color:var(--muted);text-transform:uppercase}}.v{{font-size:24px;font-weight:760;margin-top:4px}}.sub{{font-size:11px;color:var(--muted);margin-top:4px}}.panels{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px}}.panel{{padding:14px;overflow:auto}}table{{border-collapse:collapse;width:100%;min-width:680px}}th,td{{padding:8px 7px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}}th{{color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.05em;position:sticky;top:0;background:var(--panel)}}.badge{{font-size:9px;font-weight:800;border-radius:999px;padding:3px 7px;background:var(--panel2);display:inline-block}}.good{{color:var(--green)}}.bad{{color:var(--red)}}.warn{{color:var(--amber)}}.dot{{width:8px;height:8px;border-radius:50%;display:inline-block;background:var(--green);margin-right:6px}}code,.mono{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#a9c7ff}}.errorcell{{max-width:280px;overflow:hidden;text-overflow:ellipsis;color:#ff9da6}}.section{{margin-top:4px;margin-bottom:12px}}.metricline{{display:grid;grid-template-columns:170px 120px 1fr;gap:9px;align-items:center;margin:9px 0}}.metricline b{{font-size:11px;text-align:right}}.metricline em{{margin-left:6px}}.bar{{height:6px;background:#1a2130;border-radius:10px;overflow:hidden}}.bar i{{height:100%;display:block;border-radius:10px}}.bar .green{{background:var(--green)}}.bar .red{{background:var(--red)}}.bar .amber{{background:var(--amber)}}.bar .blue{{background:var(--blue)}}.chipn{{font-weight:700;margin-right:10px}}.healthgood{{color:var(--green)}}.healthwarn{{color:var(--amber)}}.healthbad{{color:var(--red)}}.footer{{margin-top:16px;color:var(--muted);font-size:11px}}@media(max-width:1250px){{.grid{{grid-template-columns:repeat(4,1fr)}}.grid6{{grid-template-columns:repeat(3,1fr)}}.panels{{grid-template-columns:1fr}}}}@media(max-width:700px){{.grid,.grid6{{grid-template-columns:repeat(2,1fr)}}}}
</style>
<script>setTimeout(()=>location.reload(),{REFRESH_S*1000})</script></head>
<body><div class="wrap">
<div class="top"><div><h1>MEMECOIN LAB — RESEARCH CONTROL ROOM V4.1</h1><div class="muted">Research-only · refresh {REFRESH_S}s · JSON API ready for MCP</div></div><div><span class="dot"></span><b>LOCAL CONTROL PLANE</b></div></div>

<div class="grid">
<div class="card"><div class="k">Active workers</div><div class="v">{active_workers}/{total_workers}</div><div class="sub">utilization {worker_util:.0f}%</div></div>
<div class="card"><div class="k">Queue</div><div class="v">{num(j.get('QUEUED',0))}</div><div class="sub">oldest {age(queue_age)}</div></div>
<div class="card"><div class="k">Running</div><div class="v">{num(j.get('RUNNING',0))}</div><div class="sub">parallel experiments</div></div>
<div class="card"><div class="k">Done</div><div class="v">{num(done_jobs)}</div><div class="sub">fail rate {fail_rate:.1f}%</div></div>
<div class="card"><div class="k">Results 1m</div><div class="v">{num(r['rates']['1m'])}</div><div class="sub">5m {num(r['rates']['5m'])} · 1h {num(r['rates']['1h'])}</div></div>
<div class="card"><div class="k">Hypotheses</div><div class="v">{num(r['totals'].get('hypotheses_total',0))}</div><div class="sub">scientific ideas</div></div>
<div class="card"><div class="k">Results</div><div class="v">{num(r['totals'].get('results_total',0))}</div><div class="sub">completed evaluations</div></div>
<div class="card"><div class="k">Frozen</div><div class="v">{num(r['totals'].get('candidates_total',0))}</div><div class="sub">prospective candidates</div></div>
</div>

<div class="grid6">
<div class="card"><div class="k">Observed tokens</div><div class="v">{num(total_tokens)}</div><div class="sub">historical sample</div></div>
<div class="card"><div class="k">Observed swaps</div><div class="v">{num(total_swaps)}</div><div class="sub">{num(total_swaps/max(1,total_tokens),2)} / token</div></div>
<div class="card"><div class="k">Wallets</div><div class="v">{num(m.get('wallets'))}</div><div class="sub">multi-token {num(m.get('multi_token_wallets'))}</div></div>
<div class="card"><div class="k">Migrations</div><div class="v">{num(m.get('migrations'))}</div><div class="sub">{pct(m.get('migrations'),total_tokens)} of tokens</div></div>
<div class="card"><div class="k">Newest swap age</div><div class="v">{age(m.get('latest_swap_age_s'))}</div><div class="sub">collector freshness</div></div>
<div class="card"><div class="k">DB sizes</div><div class="v">{bytes_text(m.get('db_size_bytes'))}</div><div class="sub">research {bytes_text(r.get('db_size_bytes'))}</div></div>
</div>

<div class="panels">
<div class="panel"><h2>Historical coverage reality</h2>{coverage_rows}</div>
<div class="panel"><h2>Raw field completeness</h2>{field_rows}<div style="margin-top:15px"><div class="k">Interpretation</div><div class="sub">Broad cross-sectional research can use the large token population. Deep sequence research must apply explicit coverage gates.</div></div></div>
</div>

<div class="panel section"><h2>Research branches — live allocation + best evidence</h2><table><thead><tr><th>Branch</th><th>Hyp.</th><th>Running</th><th>Queued</th><th>Done</th><th>Fail</th><th>Best metric</th><th>Best effect</th><th>Best p</th><th>Last result</th></tr></thead><tbody>{branch_rows}</tbody></table></div>

<div class="panels">
<div class="panel"><h2>Research funnel</h2><div class="metricline"><span>Hypothesis states</span><div style="grid-column:2/4">{hypothesis_chips}</div></div><div class="metricline"><span>Result verdicts</span><div style="grid-column:2/4">{verdict_chips}</div></div><div class="metricline"><span>Scientific memory</span><div style="grid-column:2/4">{memory_chips}</div></div></div>
<div class="panel"><h2>Throughput + queue health</h2><div class="metricline"><span>Results / 1 min</span><b>{num(r['rates']['1m'])}</b>{bar(r['rates']['1m'],max(1,r['rates']['1h']),'green')}</div><div class="metricline"><span>Results / 5 min</span><b>{num(r['rates']['5m'])}</b>{bar(r['rates']['5m'],max(1,r['rates']['1h']),'blue')}</div><div class="metricline"><span>Results / 15 min</span><b>{num(r['rates']['15m'])}</b>{bar(r['rates']['15m'],max(1,r['rates']['1h']),'blue')}</div><div class="metricline"><span>Results / 1 hour</span><b>{num(r['rates']['1h'])}</b>{bar(r['rates']['1h'],max(1,r['rates']['1h']),'green')}</div><div class="sub">Avg completed runtime: {age(r['completed_stats'].get('avg_runtime_s'))} · max: {age(r['completed_stats'].get('max_runtime_s'))} · oldest queued: {age(queue_age)}</div></div>
</div>

<div class="panel section"><h2>Worker pool — every process</h2><table><thead><tr><th>Worker</th><th>PID</th><th>State</th><th>Current job</th><th>Done</th><th>Failed</th><th>Heartbeat</th><th>Uptime</th></tr></thead><tbody>{worker_rows}</tbody></table></div>

<div class="panel section"><h2>Recent jobs — queue / running / failures</h2><table><thead><tr><th>Job</th><th>Branch</th><th>Family</th><th>Type</th><th>Status</th><th>Priority</th><th>Worker</th><th>Try</th><th>Age</th><th>Runtime</th><th>Error</th></tr></thead><tbody>{job_rows}</tbody></table></div>

<div class="panel section"><h2>Latest research results</h2><table><thead><tr><th>Time</th><th>Branch</th><th>Family</th><th>Verdict</th><th>Stage</th><th>Discovery N</th><th>Holdout N</th><th>Positives</th><th>Metric</th><th>Effect</th><th>p</th><th>adj p</th></tr></thead><tbody>{result_rows}</tbody></table></div>

<div class="panel section"><h2>Leaderboard — promoted / weak / robust signals</h2><table><thead><tr><th>Branch</th><th>Family</th><th>Verdict</th><th>Metric</th><th>Effect</th><th>p</th><th>adj p</th><th>Discovery</th><th>Holdout</th><th>Hypothesis</th></tr></thead><tbody>{top_rows}</tbody></table></div>

<div class="panels">
<div class="panel"><h2>Frozen / prospective candidates</h2><table><thead><tr><th>Candidate</th><th>Hypothesis</th><th>Status</th><th>Data cutoff</th><th>Updated</th></tr></thead><tbody>{candidate_rows}</tbody></table></div>
<div class="panel"><h2>Most observed historical tokens</h2><table><thead><tr><th>Token</th><th>Swaps</th><th>Wallets</th><th>Observed duration</th></tr></thead><tbody>{top_token_rows}</tbody></table></div>
</div>

<div class="panel section"><h2>Dataset registry / coverage gates</h2><table><thead><tr><th>Dataset</th><th>Lane</th><th>Description</th><th>Updated</th></tr></thead><tbody>{datasets}</tbody></table></div>

<div class="footer">API: <code>/api/status</code> · <code>/api/research</code> · <code>/api/market</code> · <code>/api/jobs</code> · <code>/api/results</code> · <code>/api/workers</code> · <code>/health</code><br>Market DB is read-only. Research DB is V4.1 state only. Live trading disabled.</div>
</div></body></html>'''


class Handler(BaseHTTPRequestHandler):
    def _json(self, payload, status=200):
        body = json.dumps(payload, separators=(",", ":"), default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == "/":
                body = render().encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            elif path == "/api/status":
                self._json(full_snapshot())
            elif path == "/api/research":
                self._json(research_snapshot())
            elif path == "/api/market":
                self._json(market_snapshot())
            elif path == "/api/jobs":
                self._json(research_snapshot()["recent_jobs"])
            elif path == "/api/results":
                self._json(research_snapshot()["recent_results"])
            elif path == "/api/workers":
                self._json(research_snapshot()["workers"])
            elif path == "/health":
                self._json({"ok": True, "time": time.time(), "version": "V4.1"})
            else:
                self._json({"error": "not found"}, 404)
        except Exception as exc:
            self._json({"error": repr(exc)}, 500)

    def log_message(self, fmt, *args):
        return


def main():
    core.initialize()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print("=" * 94)
    print("MEMECOIN LAB — V4.1 DETAILED CONTROL ROOM")
    print("=" * 94)
    print(f"Dashboard : http://{HOST}:{PORT}")
    print(f"Research  : {core.RESEARCH_DB}")
    print(f"Market    : {core.MARKET_DB} (READ ONLY)")
    print("Live trade: DISABLED")
    print("Endpoints : /api/status /api/research /api/market /api/jobs /api/results /api/workers /health")
    print("CTRL+C stops dashboard only")
    print("=" * 94)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
