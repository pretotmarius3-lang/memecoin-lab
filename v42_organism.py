#!/usr/bin/env python3
"""Memecoin Lab V4.2 autonomous research organism.

Extends V4.1 with a point-in-time WALLET_HISTORY branch while preserving all
existing Migration/Resurrection hypotheses, results, memory and frozen candidates.

Research-only. No trading. No socket. No central writer.

Important scientific rule:
Wallet-history features for a token at cutoff T only use wallet activity and
migration outcomes that were already observable before T. No future leakage.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import signal
import time
import traceback
from collections import defaultdict

import v41_core as core
import v41_engine as base
import v41_organism as old

CPU = os.cpu_count() or 4
WORKERS = int(os.environ.get("MEMECOIN_V42_WORKERS", str(min(10, max(4, CPU // 2)))))
IDLE_SLEEP = 0.30
DIRECTOR_SLEEP = 1.5
STOP = False

WALLET_STAGES = (30, 60, 120)
WALLET_HORIZONS = (300, 600, 900, 1800)
WALLET_FEATURES = (
    "known_wallet_share",
    "mean_prior_tokens",
    "max_prior_tokens",
    "mean_prior_migrations",
    "mean_prior_migration_rate",
    "experienced_wallet_share",
    "mean_prior_swaps",
    "mean_prior_sol",
)


def stop_handler(*_):
    global STOP
    STOP = True


def wallet_history_dataset(stage_s: int, horizon_s: int):
    """Build point-in-time wallet reputation features for future migration.

    For each token cutoff T = first_seen + stage_s:
      * current wallets come only from swaps <= T
      * prior wallet history uses swaps strictly before T on OTHER tokens
      * a prior token counts as migrated only if its migration was known by T
      * target is migration in (T, T+horizon_s]
    """
    db = core.open_market()
    if not base.table_exists(db, "t116_pump_swaps") or not base.table_exists(db, "t101_migrations"):
        db.close()
        return []

    migrations = base.migration_times(db)
    swaps = db.execute(
        """SELECT token_mint,timestamp,wallet,side,sol_delta
           FROM t116_pump_swaps
           WHERE token_mint IS NOT NULL AND timestamp IS NOT NULL
           ORDER BY timestamp"""
    ).fetchall()
    db.close()

    if not swaps:
        return []

    by_token = defaultdict(list)
    births = {}
    for r in swaps:
        mint = str(r["token_mint"])
        ts = float(r["timestamp"])
        by_token[mint].append(r)
        births[mint] = min(births.get(mint, ts), ts)

    # Wallet event history: sorted tuples (timestamp, token, abs_sol).
    wallet_events = defaultdict(list)
    for r in swaps:
        w = r["wallet"]
        if not w:
            continue
        wallet_events[str(w)].append((float(r["timestamp"]), str(r["token_mint"]), abs(float(r["sol_delta"] or 0.0))))

    now = time.time()
    out = []
    for mint, first_ts in births.items():
        cutoff = first_ts + stage_s
        end = cutoff + horizon_s
        if now < end:
            continue
        mig = migrations.get(mint)
        if mig is not None and mig <= cutoff:
            continue

        current_rows = [r for r in by_token[mint] if float(r["timestamp"]) <= cutoff]
        current_wallets = sorted({str(r["wallet"]) for r in current_rows if r["wallet"]})
        if not current_wallets:
            continue

        prior_tokens_counts = []
        prior_migration_counts = []
        prior_migration_rates = []
        prior_swaps_counts = []
        prior_sol_totals = []
        known = 0
        experienced = 0

        for w in current_wallets:
            events = [(ts, tok, sol) for ts, tok, sol in wallet_events.get(w, ()) if ts < cutoff and tok != mint]
            prior_tokens = sorted({tok for _, tok, _ in events})
            n_tokens = len(prior_tokens)
            n_swaps = len(events)
            total_sol = sum(sol for _, _, sol in events)
            n_migrated = sum(1 for tok in prior_tokens if migrations.get(tok) is not None and float(migrations[tok]) < cutoff)

            if n_tokens > 0:
                known += 1
            if n_tokens >= 2:
                experienced += 1

            prior_tokens_counts.append(float(n_tokens))
            prior_migration_counts.append(float(n_migrated))
            prior_migration_rates.append(float(n_migrated) / n_tokens if n_tokens else 0.0)
            prior_swaps_counts.append(float(n_swaps))
            prior_sol_totals.append(float(total_sol))

        n_wallets = len(current_wallets)
        mean = lambda xs: sum(xs) / len(xs) if xs else 0.0
        out.append({
            "token_mint": mint,
            "target": int(mig is not None and cutoff < float(mig) <= end),
            "known_wallet_share": known / n_wallets,
            "mean_prior_tokens": mean(prior_tokens_counts),
            "max_prior_tokens": max(prior_tokens_counts) if prior_tokens_counts else 0.0,
            "mean_prior_migrations": mean(prior_migration_counts),
            "mean_prior_migration_rate": mean(prior_migration_rates),
            "experienced_wallet_share": experienced / n_wallets,
            "mean_prior_swaps": mean(prior_swaps_counts),
            "mean_prior_sol": mean(prior_sol_totals),
        })
    return out


def feature_universe(branch):
    if branch == "WALLET_HISTORY":
        return WALLET_FEATURES
    return old.feature_universe(branch)


def dataset_for_payload(payload):
    if payload["branch"] == "WALLET_HISTORY":
        return wallet_history_dataset(int(payload["stage_s"]), int(payload["horizon_s"]))
    return old.dataset_for_payload(payload)


def run_payload(payload):
    adapter = payload["adapter"]
    if adapter == "wallet_history_univariate":
        rows = wallet_history_dataset(int(payload["stage_s"]), int(payload["horizon_s"]))
        return base.evaluate_univariate(rows, payload["feature"])
    if adapter in ("migration_univariate", "resurrection_univariate"):
        return base.run_job({"payload": payload})
    if adapter == "feature_set_refinement":
        rows = dataset_for_payload(payload)
        return old.evaluate_once(rows, tuple(payload["features"]))
    if adapter == "feature_set_robustness":
        rows = dataset_for_payload(payload)
        return old.evaluate_robust(rows, tuple(payload["features"]))
    raise ValueError(f"unknown adapter {adapter}")


def finish_job(job, verdict, metrics):
    adapter = job["payload"].get("adapter")
    if adapter == "feature_set_robustness":
        stage = "ROBUSTNESS"
    elif adapter == "feature_set_refinement":
        stage = "REFINEMENT"
    else:
        stage = "DISCOVERY"
    core.finish_job(
        job, verdict, stage, metrics,
        discovery_n=metrics.get("discovery_n"),
        holdout_n=metrics.get("holdout_n"),
        positives=metrics.get("positives"),
        primary_metric=metrics.get("holdout_signed_rho"),
        effect_size=metrics.get("qdiff_pp"),
        coverage={"n": metrics.get("n"), "valid_splits": metrics.get("valid_splits")},
    )


def worker_main(index):
    worker_id = f"ORG42-{index:02d}-{os.getpid()}"
    core.worker_heartbeat(worker_id, "RUNNING")
    while True:
        job = base.safe_claim(worker_id)
        if job is None:
            core.worker_heartbeat(worker_id, "IDLE")
            time.sleep(IDLE_SLEEP)
            continue
        core.worker_heartbeat(worker_id, "BUSY", job["job_id"])
        try:
            verdict, metrics = run_payload(job["payload"])
            finish_job(job, verdict, metrics)
            core.worker_heartbeat(worker_id, "RUNNING", done_inc=1)
        except KeyboardInterrupt:
            return
        except Exception:
            core.fail_job(job, traceback.format_exc())
            core.worker_heartbeat(worker_id, "RUNNING", failed_inc=1)


def child_payload(parent_spec, features, adapter):
    payload = {
        "adapter": adapter,
        "branch": parent_spec.get("branch") or ("MIGRATION" if "horizon_s" in parent_spec else "RESURRECTION"),
        "stage_s": int(parent_spec["stage_s"]),
        "features": list(features),
    }
    if "horizon_s" in parent_spec:
        payload["horizon_s"] = int(parent_spec["horizon_s"])
    if "target" in parent_spec:
        payload["target"] = parent_spec["target"]
    return payload


def extract_features(spec):
    if "features" in spec:
        return tuple(spec["features"])
    if "feature" in spec:
        return (spec["feature"],)
    return ()


def spawn_children(row, spec):
    verdict = row["verdict"]
    branch = row["branch"]
    generation = int(row["generation"] or 0)
    parent_features = extract_features(spec)
    if not parent_features:
        return 0

    if row["stage"] == "REFINEMENT" and verdict == "PROMISING":
        payload = child_payload(spec, parent_features, "feature_set_robustness")
        payload["branch"] = branch
        hid, _ = core.create_hypothesis(
            branch, row["family"], payload,
            {"lane": "robustness", "parent_result": row["result_id"]},
            parent_hypothesis_id=row["hypothesis_id"], generation=generation + 1,
        )
        _, created = core.enqueue_job(hid, "ROBUSTNESS", payload, priority=10)
        return int(created)

    if row["stage"] != "DISCOVERY" or verdict not in ("PROMISING", "WEAK"):
        return 0

    limit = old.MAX_CHILDREN_PROMISING if verdict == "PROMISING" else old.MAX_CHILDREN_WEAK
    candidates = [f for f in feature_universe(branch) if f not in parent_features]
    made = 0
    for other in candidates[:limit]:
        features = tuple(dict.fromkeys(parent_features + (other,)))
        payload = child_payload(spec, features, "feature_set_refinement")
        payload["branch"] = branch
        hid, _ = core.create_hypothesis(
            branch, row["family"] + "+AUTO", payload,
            {"lane": "refinement", "parent_result": row["result_id"]},
            parent_hypothesis_id=row["hypothesis_id"], generation=generation + 1,
        )
        _, created = core.enqueue_job(hid, "REFINEMENT", payload, priority=25 if verdict == "PROMISING" else 40)
        made += int(created)
    return made


def auto_director_tick(max_results=120):
    db = core.open_research()
    rows = db.execute(
        """SELECT r.result_id,r.hypothesis_id,r.stage,r.verdict,r.primary_metric,r.effect_size,r.p_value,
                  r.metrics_json,h.branch,h.family,h.spec_json,h.generation
           FROM v41_results r JOIN v41_hypotheses h ON h.hypothesis_id=r.hypothesis_id
           ORDER BY r.created_at ASC"""
    ).fetchall()
    db.close()
    processed = spawned = frozen = 0
    for raw in rows:
        if processed >= max_results:
            break
        row = dict(raw)
        db = core.open_research()
        if old.result_processed(db, row["result_id"]):
            db.close()
            continue
        spec = json.loads(row["spec_json"])
        metrics = json.loads(row["metrics_json"])
        db.execute("BEGIN IMMEDIATE")
        try:
            old.mark_result_memory(db, row, spec)
            db.commit()
        except BaseException:
            db.rollback(); db.close(); raise
        db.close()
        processed += 1
        if row["verdict"] == "ROBUST" and row["stage"] == "ROBUSTNESS":
            frozen += int(old.freeze_candidate(row, spec, metrics))
        else:
            spawned += spawn_children(row, spec)
    return processed, spawned, frozen


def seed_wallet_history():
    made = 0
    counts = core.queue_counts()
    budget = core.generation_budget(WORKERS, counts.get("QUEUED", 0))
    if budget <= 0:
        return 0
    for stage in WALLET_STAGES:
        for horizon in WALLET_HORIZONS:
            for feature in WALLET_FEATURES:
                if made >= budget:
                    return made
                spec = {
                    "adapter": "wallet_history_univariate",
                    "branch": "WALLET_HISTORY",
                    "stage_s": stage,
                    "horizon_s": horizon,
                    "feature": feature,
                }
                hid, _ = core.create_hypothesis(
                    "WALLET_HISTORY", "POINT_IN_TIME", spec,
                    {"lane": "wallet_history", "no_future_leakage": True},
                )
                _, created = core.enqueue_job(hid, "DISCOVERY", spec, priority=35)
                made += int(created)
    return made


def display(last_director, seeded):
    db = core.open_research()
    jobs = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    branches = db.execute(
        """SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) h,
                  SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) q,
                  SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) r,
                  SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) d,
                  SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) f
           FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
           GROUP BY h.branch ORDER BY h DESC"""
    ).fetchall()
    frozen = db.execute("SELECT COUNT(*) FROM v41_candidates WHERE status='FROZEN'").fetchone()[0]
    db.close()
    print("\033[2J\033[H", end="")
    print("=" * 118)
    print("MEMECOIN LAB — AUTONOMOUS RESEARCH ORGANISM V4.2")
    print("=" * 118)
    print(f"WORKERS={WORKERS} | QUEUED={jobs.get('QUEUED',0)} | RUNNING={jobs.get('RUNNING',0)} | DONE={jobs.get('DONE',0)} | FAILED={jobs.get('FAILED',0)} | FROZEN={frozen}")
    print(f"DIRECTOR processed/spawned/frozen={last_director} | WALLET seeded={seeded}")
    print()
    print(f"{'BRANCH':<20}{'HYP':>8}{'Q':>8}{'RUN':>8}{'DONE':>8}{'FAIL':>8}")
    for x in branches:
        print(f"{x['branch']:<20}{x['h'] or 0:>8}{x['q'] or 0:>8}{x['r'] or 0:>8}{x['d'] or 0:>8}{x['f'] or 0:>8}")
    print("\nResearch-only | point-in-time wallet history enabled | no live trading")


def main():
    global STOP
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    core.initialize()
    core.register_dataset(
        "wallet_history_point_in_time_v1", "wallet_history",
        "Wallet reputation known strictly before each token cutoff",
        {"source": "t116_pump_swaps+t101_migrations", "no_future_leakage": True}, {},
    )
    old.seed_discovery_if_needed()
    seeded = seed_wallet_history()
    workers = [mp.Process(target=worker_main, args=(i + 1,), daemon=True) for i in range(WORKERS)]
    for p in workers:
        p.start()
    last_director = (0, 0, 0)
    try:
        while not STOP:
            core.reclaim_expired_jobs()
            old.seed_discovery_if_needed()
            seeded = seed_wallet_history()
            last_director = auto_director_tick()
            display(last_director, seeded)
            time.sleep(DIRECTOR_SLEEP)
    finally:
        for p in workers:
            if p.is_alive():
                p.terminate()
        for p in workers:
            p.join(timeout=3)
        print("V4.2 organism stopped cleanly")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
