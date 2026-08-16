#!/usr/bin/env python3
"""Memecoin Lab V4.4 — live decoded-feature research organism.

Extends V4.3 with an EXPLORATORY_LIVE research lane sourced from v52_features.db.
New research epochs are triggered only when additional *matured* point-in-time
outcomes exist. This keeps the engine data-driven instead of manufacturing jobs.

The live lane is exploratory and is NOT allowed to freeze/promote candidates.
Frozen historical candidates remain untouched pending a dedicated prospective scorer.

Research-only. No trading/signing.
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import signal
import sqlite3
import time
import traceback
from pathlib import Path

import v41_core as core
import v41_engine as base
import v41_organism as old
import v42_organism as v42
import v43_organism as v43

ROOT = Path.home() / "memecoin_lab"
V52_DB = Path(os.environ.get("MEMECOIN_V52_DB", ROOT / "v52_features.db"))
CPU = os.cpu_count() or 4
WORKERS = int(os.environ.get("MEMECOIN_V44_WORKERS", str(min(10, max(4, CPU // 2)))))
MIN_READY = int(os.environ.get("MEMECOIN_V44_MIN_READY", "60"))
EPOCH_STEP = int(os.environ.get("MEMECOIN_V44_EPOCH_STEP", "25"))
MAX_NEW_JOBS = int(os.environ.get("MEMECOIN_V44_MAX_NEW_JOBS", "18"))
IDLE_SLEEP = 0.25
LOOP_SLEEP = 1.0
STOP = False

FEATURES = (
    "swaps", "buy_ratio", "gross_sol", "net_sol", "unique_wallets", "repeat_wallet_ratio",
    "wallet_hhi", "wallet_top1_share", "avg_trade_sol", "max_trade_sol", "trade_hhi",
    "top1_trade_share", "return_pct", "range_pct", "flow_velocity", "flow_acceleration",
    "buy_ratio_delta", "price_velocity",
)
TARGETS = ("future_hit10", "future_hit20", "future_hit50", "future_death50", "future_migration")
STAGES = (10, 20, 30, 60, 120)
HORIZONS = (120, 300, 600, 900)


def stop_handler(*_):
    global STOP
    STOP = True


def open_v52():
    if not V52_DB.exists():
        return None
    db = sqlite3.connect(f"file:{V52_DB}?mode=ro", uri=True, timeout=20)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=20000")
    return db


def init_v44():
    db = core.open_research()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v44_live_epochs (
      context_key TEXT PRIMARY KEY,
      last_epoch INTEGER NOT NULL,
      last_ready_n INTEGER NOT NULL,
      updated_at REAL NOT NULL);
    """)
    db.commit(); db.close()


def live_dataset(stage_s, horizon_s, target, feature, max_rows=None):
    db = open_v52()
    if db is None:
        return []
    # Stable ordering by cutoff time/mint makes each watermark deterministic.
    sql = f"""SELECT s.token_mint,s.cutoff_ts,s.{feature} feature,o.{target} target
              FROM v52_snapshots s
              JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
              WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1
                AND s.{feature} IS NOT NULL AND o.{target} IS NOT NULL
              ORDER BY s.cutoff_ts,s.token_mint"""
    rows = db.execute(sql, (stage_s, horizon_s)).fetchall()
    db.close()
    if max_rows is not None:
        rows = rows[:int(max_rows)]
    return [{"token_mint": str(r["token_mint"]), "feature": r["feature"], "target": int(r["target"])} for r in rows]


def run_payload(payload):
    if payload.get("adapter") == "v52_live_univariate":
        rows = live_dataset(int(payload["stage_s"]), int(payload["horizon_s"]), payload["target"], payload["feature"], payload.get("watermark_n"))
        verdict, metrics = base.evaluate_univariate(rows, "feature")
        metrics["watermark_n"] = int(payload.get("watermark_n") or len(rows))
        metrics["research_mode"] = "EXPLORATORY_LIVE"
        return verdict, metrics
    return v43.run_payload(payload)


def finish_job(job, verdict, metrics):
    if job["payload"].get("adapter") == "v52_live_univariate":
        core.finish_job(
            job, verdict, "EXPLORATORY_LIVE", metrics,
            discovery_n=metrics.get("discovery_n"), holdout_n=metrics.get("holdout_n"), positives=metrics.get("positives"),
            primary_metric=metrics.get("holdout_signed_rho"), effect_size=metrics.get("qdiff_pp"),
            coverage={"n": metrics.get("n"), "watermark_n": metrics.get("watermark_n"), "prospective": False},
        )
    else:
        v43.finish_job(job, verdict, metrics)


def worker_main(index):
    wid = f"ORG44-{index:02d}-{os.getpid()}"
    core.worker_heartbeat(wid, "RUNNING")
    while True:
        job = base.safe_claim(wid)
        if job is None:
            core.worker_heartbeat(wid, "IDLE")
            time.sleep(IDLE_SLEEP)
            continue
        core.worker_heartbeat(wid, "BUSY", job["job_id"])
        try:
            verdict, metrics = run_payload(job["payload"])
            finish_job(job, verdict, metrics)
            core.worker_heartbeat(wid, "RUNNING", done_inc=1)
        except KeyboardInterrupt:
            return
        except Exception:
            core.fail_job(job, traceback.format_exc())
            core.worker_heartbeat(wid, "RUNNING", failed_inc=1)


def ready_count(stage_s, horizon_s, target):
    db = open_v52()
    if db is None:
        return 0
    n = db.execute(f"""SELECT COUNT(*) FROM v52_outcomes
                        WHERE stage_s=? AND horizon_s=? AND ready=1 AND {target} IS NOT NULL""",
                   (stage_s, horizon_s)).fetchone()[0]
    db.close()
    return int(n)


def epoch_state(context):
    db = core.open_research()
    row = db.execute("SELECT last_epoch,last_ready_n FROM v44_live_epochs WHERE context_key=?", (context,)).fetchone()
    db.close()
    return (int(row["last_epoch"]), int(row["last_ready_n"])) if row else (-1, 0)


def set_epoch_state(context, epoch, ready_n):
    db = core.open_research()
    db.execute("""INSERT INTO v44_live_epochs(context_key,last_epoch,last_ready_n,updated_at) VALUES(?,?,?,?)
                  ON CONFLICT(context_key) DO UPDATE SET last_epoch=excluded.last_epoch,last_ready_n=excluded.last_ready_n,updated_at=excluded.updated_at""",
               (context, epoch, ready_n, time.time()))
    db.commit(); db.close()


def seed_live_research():
    if not V52_DB.exists():
        return 0
    made = 0
    # Cycle deterministic contexts; only contexts crossing a new immutable watermark generate jobs.
    for stage in STAGES:
        for horizon in HORIZONS:
            for target in TARGETS:
                n = ready_count(stage, horizon, target)
                if n < MIN_READY:
                    continue
                epoch = (n - MIN_READY) // EPOCH_STEP
                context = f"{stage}:{horizon}:{target}"
                last_epoch, _ = epoch_state(context)
                if epoch <= last_epoch:
                    continue
                watermark = MIN_READY + epoch * EPOCH_STEP
                # Pre-specified feature panel; no adaptive feature invention in live data.
                for feature in FEATURES:
                    if made >= MAX_NEW_JOBS:
                        break
                    spec = {
                        "adapter": "v52_live_univariate", "branch": "LIVE_RESEARCH", "stage_s": stage,
                        "horizon_s": horizon, "target": target, "feature": feature,
                        "epoch": epoch, "watermark_n": watermark,
                    }
                    hid, _ = core.create_hypothesis(
                        "LIVE_RESEARCH", "V52_PRESET", spec,
                        {"lane": "exploratory_live", "pre_specified_panel": True, "not_for_freezing": True},
                        generation=epoch,
                    )
                    _, created = core.enqueue_job(hid, "EXPLORATORY_LIVE", spec, priority=30)
                    made += int(created)
                # Mark only after the context was seeded; if max job cap was reached, next loop will continue via dedup.
                set_epoch_state(context, epoch, n)
                if made >= MAX_NEW_JOBS:
                    return made
    return made


def v52_stats():
    db = open_v52()
    if db is None:
        return 0,0,0,None
    swaps = db.execute("SELECT COUNT(*) FROM v52_swaps").fetchone()[0]
    tokens = db.execute("SELECT COUNT(DISTINCT token_mint) FROM v52_swaps").fetchone()[0]
    ready = db.execute("SELECT COUNT(*) FROM v52_outcomes WHERE ready=1").fetchone()[0]
    latest = db.execute("SELECT MAX(timestamp) FROM v52_swaps").fetchone()[0]
    db.close()
    return int(swaps),int(tokens),int(ready),latest


def display(last_director, seeded_wallet, seeded_ingest, seeded_research):
    db = core.open_research()
    jobs = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    frozen = db.execute("SELECT COUNT(*) FROM v41_candidates WHERE status='FROZEN'").fetchone()[0]
    branches = db.execute("""SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) h,
      SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) q,
      SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) r,
      SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) d,
      SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) f
      FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
      GROUP BY h.branch ORDER BY h DESC""").fetchall()
    db.close()
    swaps,tokens,ready,latest = v52_stats()
    age = "—" if latest is None else f"{max(0,time.time()-float(latest)):.1f}s"
    print("\033[2J\033[H", end="")
    print("="*124)
    print("MEMECOIN LAB — LIVE FEATURE RESEARCH ORGANISM V4.4")
    print("="*124)
    print(f"WORKERS={WORKERS} | QUEUED={jobs.get('QUEUED',0)} | RUNNING={jobs.get('RUNNING',0)} | DONE={jobs.get('DONE',0)} | FAILED={jobs.get('FAILED',0)} | FROZEN={frozen}")
    print(f"V52 SWAPS={swaps:,} | TOKENS={tokens:,} | READY OUTCOMES={ready:,} | DECODE AGE={age} | NEW RESEARCH JOBS={seeded_research}")
    print(f"DIRECTOR={last_director} | WALLET={seeded_wallet} | LIVE INGEST={seeded_ingest}")
    print(); print(f"{'BRANCH':<20}{'HYP':>8}{'Q':>8}{'RUN':>8}{'DONE':>8}{'FAIL':>8}")
    for x in branches:
        print(f"{x['branch']:<20}{x['h'] or 0:>8}{x['q'] or 0:>8}{x['r'] or 0:>8}{x['d'] or 0:>8}{x['f'] or 0:>8}")
    print("\nResearch-only | live feature research is exploratory | historical Frozen candidates unchanged | no live trading")


def main():
    global STOP
    signal.signal(signal.SIGINT, stop_handler); signal.signal(signal.SIGTERM, stop_handler)
    core.initialize(); v43.init_v43(); init_v44()
    v42.seed_wallet_history(); old.seed_discovery_if_needed()
    workers = [mp.Process(target=worker_main, args=(i+1,), daemon=True) for i in range(WORKERS)]
    for p in workers: p.start()
    try:
        while not STOP:
            core.reclaim_expired_jobs()
            old.seed_discovery_if_needed()
            sw = v42.seed_wallet_history()
            si = v43.seed_live_ingest()
            sr = seed_live_research()
            d = v42.auto_director_tick()
            display(d, sw, si, sr)
            time.sleep(LOOP_SLEEP)
    finally:
        for p in workers:
            if p.is_alive(): p.terminate()
        for p in workers: p.join(timeout=3)
        print("V4.4 organism stopped cleanly")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
