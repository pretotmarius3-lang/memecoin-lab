#!/usr/bin/env python3
"""Memecoin Lab V4.1 multi-worker research engine.

Research-only. No live trading. No Unix socket. No central writer process.

This first autonomous engine runs two real research lanes in parallel:
- MIGRATION: early token/wallet microstructure -> future migration
- RESURRECTION: post-entry features -> future hit targets

The director continuously seeds a bounded queue; workers claim jobs atomically,
run research outside write transactions, and write short result transactions.
"""

from __future__ import annotations

import hashlib
import json
import math
import multiprocessing as mp
import os
import signal
import sqlite3
import statistics
import time
import traceback
from collections import defaultdict

import v41_core as core

CPU = os.cpu_count() or 4
WORKERS = int(os.environ.get("MEMECOIN_V41_WORKERS", str(min(10, max(4, CPU // 2)))))
LEASE_S = int(os.environ.get("MEMECOIN_V41_LEASE_S", "900"))
IDLE_SLEEP = 0.35
DIRECTOR_SLEEP = 2.0

STOP = False

MIG_STAGES = (30, 60, 120)
MIG_HORIZONS = (300, 600, 900, 1800)
MIG_FEATURES = {
    "PRICE": ("return_pct", "range_pct"),
    "FLOW": ("swaps", "buy_ratio", "gross_sol", "net_sol"),
    "WALLET": ("unique_wallets", "unique_buyers", "unique_sellers", "wallet_hhi", "wallet_top1_share", "repeat_wallet_ratio"),
    "MICROSTRUCTURE": ("avg_trade_sol", "max_trade_sol", "trade_hhi", "top1_trade_share"),
}

RES_STAGES = (5, 10, 20, 30, 60)
RES_TARGETS = ("future_hit10", "future_hit20", "future_hit30", "future_hit50")
RES_FEATURES = {
    "PRICE": ("return_since_entry", "mfe_so_far", "mae_so_far"),
    "ACTIVITY": ("swaps", "buys", "sells", "buy_ratio"),
    "FLOW": ("buy_sol", "sell_sol", "net_sol"),
}


def _stop(*_):
    global STOP
    STOP = True


def valid(x):
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)


def mean(xs):
    xs = [x for x in xs if valid(x)]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = sorted(x for x in xs if valid(x))
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def ranks(values):
    ordered = sorted(enumerate(values), key=lambda z: z[1])
    out = [0.0] * len(values)
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][1] == ordered[i][1]:
            j += 1
        r = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            out[ordered[k][0]] = r
        i = j + 1
    return out


def pearson(x, y):
    if len(x) < 3:
        return None
    mx, my = mean(x), mean(y)
    if mx is None or my is None:
        return None
    num = sum((a - mx) * (b - my) for a, b in zip(x, y))
    dx = math.sqrt(sum((a - mx) ** 2 for a in x))
    dy = math.sqrt(sum((b - my) ** 2 for b in y))
    return None if dx == 0 or dy == 0 else num / (dx * dy)


def spearman(x, y):
    pairs = [(a, b) for a, b in zip(x, y) if valid(a) and valid(b)]
    if len(pairs) < 3:
        return None
    return pearson(ranks([a for a, _ in pairs]), ranks([b for _, b in pairs]))


def holdout(mint: str) -> bool:
    h = hashlib.sha256(mint.encode()).digest()
    return int.from_bytes(h[:4], "big") % 100 >= 75


def table_exists(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def safe_claim(worker_id: str):
    """Atomic claim that avoids the empty-queue close-inside-context edge case."""
    db = core.open_research()
    now = time.time()
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            "SELECT * FROM v41_jobs WHERE status='QUEUED' AND attempts < ? ORDER BY priority,created_at LIMIT 1",
            (core.MAX_ATTEMPTS,),
        ).fetchone()
        if row is None:
            db.commit()
            db.close()
            return None
        cur = db.execute(
            """
            UPDATE v41_jobs SET status='RUNNING',worker_id=?,lease_until=?,attempts=attempts+1,
                   started_at=COALESCE(started_at,?),updated_at=?
            WHERE job_id=? AND status='QUEUED'
            """,
            (worker_id, now + LEASE_S, now, now, row["job_id"]),
        )
        if cur.rowcount != 1:
            db.rollback(); db.close(); return None
        job = dict(row)
        db.commit(); db.close()
        job.update(status="RUNNING", worker_id=worker_id, lease_until=now + LEASE_S)
        job["payload"] = json.loads(job.pop("payload_json"))
        return job
    except BaseException:
        db.rollback(); db.close(); raise


def migration_times(db):
    c = {r["name"] for r in db.execute("PRAGMA table_info(t101_migrations)").fetchall()}
    time_col = next((x for x in ("block_time", "detected_at", "timestamp", "created_at") if x in c), None)
    if not time_col:
        return {}
    rows = db.execute(
        f"SELECT token_mint,MIN({time_col}) ts FROM t101_migrations WHERE token_mint IS NOT NULL AND {time_col} IS NOT NULL GROUP BY token_mint"
    ).fetchall()
    return {str(r["token_mint"]): float(r["ts"]) for r in rows}


def wallet_stats(rows):
    by_wallet = defaultdict(float)
    buy_wallets, sell_wallets = set(), set()
    for r in rows:
        w = r["wallet"]
        if not w:
            continue
        amount = abs(float(r["sol_delta"] or 0.0))
        by_wallet[w] += amount
        if r["side"] == "BUY": buy_wallets.add(w)
        elif r["side"] == "SELL": sell_wallets.add(w)
    values = sorted(by_wallet.values(), reverse=True)
    total = sum(values)
    hhi = sum((x / total) ** 2 for x in values) if total > 0 else None
    top1 = values[0] / total if values and total > 0 else None
    counts = defaultdict(int)
    for r in rows:
        if r["wallet"]:
            counts[r["wallet"]] += 1
    repeat = sum(v > 1 for v in counts.values()) / len(counts) if counts else None
    return len(by_wallet), len(buy_wallets), len(sell_wallets), hhi, top1, repeat


def migration_dataset(stage_s, horizon_s):
    db = core.open_market()
    if not table_exists(db, "t116_pump_swaps") or not table_exists(db, "t101_migrations"):
        db.close(); return []
    migrations = migration_times(db)
    births = db.execute("SELECT token_mint,MIN(timestamp) first_ts FROM t116_pump_swaps GROUP BY token_mint").fetchall()
    now = time.time()
    out = []
    for b in births:
        mint = str(b["token_mint"])
        if b["first_ts"] is None:
            continue
        first_ts = float(b["first_ts"]); cutoff = first_ts + stage_s; end = cutoff + horizon_s
        if now < end:
            continue
        mig = migrations.get(mint)
        if mig is not None and mig <= cutoff:
            continue
        rs = db.execute(
            """SELECT timestamp,wallet,signature,side,sol_delta,raw_price_sol
               FROM t116_pump_swaps WHERE token_mint=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp""",
            (mint, first_ts, cutoff),
        ).fetchall()
        if not rs:
            continue
        prices = [float(r["raw_price_sol"]) for r in rs if valid(r["raw_price_sol"]) and r["raw_price_sol"] > 0]
        if not prices:
            continue
        buys = [r for r in rs if r["side"] == "BUY"]
        sells = [r for r in rs if r["side"] == "SELL"]
        buy_sol = sum(abs(float(r["sol_delta"] or 0)) for r in buys)
        sell_sol = sum(abs(float(r["sol_delta"] or 0)) for r in sells)
        sizes = [abs(float(r["sol_delta"] or 0)) for r in rs]
        gross = buy_sol + sell_sol
        trade_hhi = sum((x / gross) ** 2 for x in sizes) if gross > 0 else None
        top1_trade = max(sizes) / gross if sizes and gross > 0 else None
        uw, ub, us, whhi, wtop1, repeat = wallet_stats(rs)
        out.append({
            "token_mint": mint,
            "target": int(mig is not None and cutoff < mig <= end),
            "return_pct": 100.0 * (prices[-1] / prices[0] - 1.0) if prices[0] > 0 else None,
            "range_pct": 100.0 * (max(prices) / min(prices) - 1.0) if min(prices) > 0 else None,
            "swaps": len(rs), "buy_ratio": len(buys) / len(rs), "gross_sol": gross, "net_sol": buy_sol - sell_sol,
            "unique_wallets": uw, "unique_buyers": ub, "unique_sellers": us, "wallet_hhi": whhi,
            "wallet_top1_share": wtop1, "repeat_wallet_ratio": repeat,
            "avg_trade_sol": mean(sizes), "max_trade_sol": max(sizes) if sizes else None,
            "trade_hhi": trade_hhi, "top1_trade_share": top1_trade,
        })
    db.close()
    return out


def resurrection_dataset(stage_s, target, feature):
    db = core.open_market()
    if not table_exists(db, "lab_exp0121_stage_features"):
        db.close(); return []
    rows = db.execute(
        f"""SELECT token_mint,{feature} feature,{target} target FROM lab_exp0121_stage_features
            WHERE stage_s=? AND future_ready=1 AND coverage_status='GOOD' AND {feature} IS NOT NULL AND {target} IS NOT NULL""",
        (stage_s,),
    ).fetchall()
    db.close()
    return [{"token_mint": str(r["token_mint"]), "feature": r["feature"], "target": int(r["target"])} for r in rows if valid(r["feature"])]


def evaluate_univariate(rows, feature):
    rows = [r for r in rows if valid(r.get(feature))]
    train = [r for r in rows if not holdout(r["token_mint"])]
    test = [r for r in rows if holdout(r["token_mint"])]
    tpos = sum(r["target"] for r in train); hpos = sum(r["target"] for r in test)
    if len(train) < 40 or len(test) < 15 or tpos < 8 or hpos < 3:
        return "COLLECT_MORE", {"n": len(rows), "discovery_n": len(train), "holdout_n": len(test), "positives": tpos + hpos}
    trho = spearman([r[feature] for r in train], [r["target"] for r in train])
    if trho is None:
        return "REJECT", {"n": len(rows), "discovery_n": len(train), "holdout_n": len(test), "positives": tpos + hpos}
    direction = 1.0 if trho >= 0 else -1.0
    hrho = spearman([direction * r[feature] for r in test], [r["target"] for r in test])
    ordered = sorted(test, key=lambda r: direction * r[feature])
    q = max(1, len(ordered) // 4)
    lo, hi = ordered[:q], ordered[-q:]
    qdiff = 100 * sum(r["target"] for r in hi) / len(hi) - 100 * sum(r["target"] for r in lo) / len(lo)
    verdict = "REJECT"
    if hrho is not None and hrho >= 0.10 and qdiff >= 2.0:
        verdict = "PROMISING"
    elif hrho is not None and hrho >= 0.05:
        verdict = "WEAK"
    return verdict, {
        "n": len(rows), "discovery_n": len(train), "holdout_n": len(test), "positives": tpos + hpos,
        "train_rho": trho, "holdout_signed_rho": hrho, "qdiff_pp": qdiff, "direction": direction,
        "positive_median": median([r[feature] for r in rows if r["target"]]),
        "negative_median": median([r[feature] for r in rows if not r["target"]]),
    }


def run_job(job):
    p = job["payload"]
    adapter = p["adapter"]
    if adapter == "migration_univariate":
        rows = migration_dataset(int(p["stage_s"]), int(p["horizon_s"]))
        verdict, metrics = evaluate_univariate(rows, p["feature"])
    elif adapter == "resurrection_univariate":
        rows = resurrection_dataset(int(p["stage_s"]), p["target"], p["feature"])
        verdict, metrics = evaluate_univariate(rows, "feature")
    else:
        raise ValueError(f"unknown adapter: {adapter}")
    return verdict, metrics


def finish(job, verdict, metrics):
    primary = metrics.get("holdout_signed_rho")
    effect = metrics.get("qdiff_pp")
    core.finish_job(
        job, verdict, "DISCOVERY", metrics,
        discovery_n=metrics.get("discovery_n"), holdout_n=metrics.get("holdout_n"), positives=metrics.get("positives"),
        primary_metric=primary, effect_size=effect,
        coverage={"n": metrics.get("n")},
    )


def worker_main(index):
    worker_id = f"W{index:02d}-{os.getpid()}"
    core.worker_heartbeat(worker_id, "RUNNING")
    while True:
        job = safe_claim(worker_id)
        if job is None:
            core.worker_heartbeat(worker_id, "IDLE")
            time.sleep(IDLE_SLEEP)
            continue
        core.worker_heartbeat(worker_id, "BUSY", job["job_id"])
        try:
            verdict, metrics = run_job(job)
            finish(job, verdict, metrics)
            core.worker_heartbeat(worker_id, "RUNNING", done_inc=1)
        except KeyboardInterrupt:
            return
        except Exception:
            core.fail_job(job, traceback.format_exc())
            core.worker_heartbeat(worker_id, "RUNNING", failed_inc=1)


def seed_catalog():
    specs = []
    for stage in MIG_STAGES:
        for horizon in MIG_HORIZONS:
            for family, features in MIG_FEATURES.items():
                for feature in features:
                    specs.append(("MIGRATION", family, {
                        "adapter": "migration_univariate", "stage_s": stage, "horizon_s": horizon, "feature": feature,
                    }, {"lane": "cross_sectional", "min_positive": 15}))
    for stage in RES_STAGES:
        for target in RES_TARGETS:
            for family, features in RES_FEATURES.items():
                for feature in features:
                    specs.append(("RESURRECTION", family, {
                        "adapter": "resurrection_univariate", "stage_s": stage, "target": target, "feature": feature,
                    }, {"lane": "deep_trajectory", "coverage": "GOOD"}))
    return specs


def director_tick(catalog):
    core.reclaim_expired_jobs()
    counts = core.queue_counts()
    queued = counts.get("QUEUED", 0)
    budget = core.generation_budget(WORKERS, queued)
    if budget <= 0:
        return 0
    made = 0
    for branch, family, spec, req in catalog:
        if made >= budget:
            break
        hid, _ = core.create_hypothesis(branch, family, spec, req)
        _, created = core.enqueue_job(hid, "DISCOVERY", spec, priority=50 if branch == "MIGRATION" else 80)
        if created:
            made += 1
    return made


def display():
    db = core.open_research()
    jobs = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    branches = db.execute(
        """SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) h,
        SUM(j.status='QUEUED') q,SUM(j.status='RUNNING') r,SUM(j.status='DONE') d,SUM(j.status='FAILED') f
        FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id GROUP BY h.branch"""
    ).fetchall()
    db.close()
    print("\033[2J\033[H", end="")
    print("=" * 105)
    print("MEMECOIN LAB — AUTONOMOUS RESEARCH ENGINE V4.1")
    print("=" * 105)
    print(f"WORKERS {WORKERS} | QUEUED {jobs.get('QUEUED',0)} | RUNNING {jobs.get('RUNNING',0)} | DONE {jobs.get('DONE',0)} | FAILED {jobs.get('FAILED',0)}")
    print("Research-only | no live trading | no socket | CTRL+C stops engine")
    print()
    print(f"{'BRANCH':<18}{'HYP':>8}{'QUEUED':>10}{'RUNNING':>10}{'DONE':>10}{'FAILED':>10}")
    for x in branches:
        print(f"{x['branch']:<18}{x['h'] or 0:>8}{x['q'] or 0:>10}{x['r'] or 0:>10}{x['d'] or 0:>10}{x['f'] or 0:>10}")
    print("\nControl room: http://127.0.0.1:8765")


def main():
    global STOP
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    core.initialize()
    core.register_dataset("migration_cross_sectional_v1", "cross_sectional", "Early raw swap/wallet state before future migration", {"source": "t116_pump_swaps"}, {})
    core.register_dataset("resurrection_good_coverage_v1", "deep_trajectory", "GOOD coverage EXP0121 post-entry states", {"source": "lab_exp0121_stage_features"}, {})
    catalog = seed_catalog()
    workers = [mp.Process(target=worker_main, args=(i + 1,), daemon=True) for i in range(WORKERS)]
    for p in workers:
        p.start()
    try:
        while not STOP:
            director_tick(catalog)
            display()
            time.sleep(DIRECTOR_SLEEP)
    finally:
        for p in workers:
            if p.is_alive():
                p.terminate()
        for p in workers:
            p.join(timeout=3)
        print("V4.1 stopped cleanly")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
