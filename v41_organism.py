#!/usr/bin/env python3
"""Memecoin Lab V4.1 recursive research organism.

Research-only. No live trading.

This process supersedes the first finite-catalog `v41_engine.py` run. It reuses the
stable V4.1 queue/DB foundation and adds a recursive scientific director:

DISCOVERY -> REFINEMENT -> ROBUSTNESS -> FROZEN

The director consumes completed results, writes scientific memory, creates bounded
children for WEAK/PROMISING results, promotes promising refined models to repeated
split robustness tests, and freezes only robust survivors.

No Unix socket. No central writer. Every worker uses short WAL transactions.
"""

from __future__ import annotations

import hashlib
import json
import math
import multiprocessing as mp
import os
import signal
import statistics
import time
import traceback
import uuid
from collections import defaultdict

import v41_core as core
import v41_engine as base

CPU = os.cpu_count() or 4
WORKERS = int(os.environ.get("MEMECOIN_V41_WORKERS", str(min(10, max(4, CPU // 2)))))
LEASE_S = int(os.environ.get("MEMECOIN_V41_LEASE_S", "900"))
DIRECTOR_SLEEP = 1.5
IDLE_SLEEP = 0.30
MAX_CHILDREN_PROMISING = 4
MAX_CHILDREN_WEAK = 2
ROBUST_SPLITS = int(os.environ.get("MEMECOIN_V41_ROBUST_SPLITS", "40"))
STOP = False

MIG_ALL_FEATURES = tuple(dict.fromkeys(f for fs in base.MIG_FEATURES.values() for f in fs))
RES_ALL_FEATURES = tuple(dict.fromkeys(f for fs in base.RES_FEATURES.values() for f in fs))


def stop_handler(*_):
    global STOP
    STOP = True


def valid(x):
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)


def median(xs):
    xs = sorted(x for x in xs if valid(x))
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def percentile(xs, q):
    xs = sorted(x for x in xs if valid(x))
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    p = (len(xs) - 1) * q
    lo, hi = math.floor(p), math.ceil(p)
    if lo == hi:
        return xs[lo]
    w = p - lo
    return xs[lo] * (1 - w) + xs[hi] * w


def split_holdout(mint: str, seed: int, pct: int = 25) -> bool:
    h = hashlib.sha256(f"{seed}:{mint}".encode()).digest()
    return int.from_bytes(h[:4], "big") % 100 >= 100 - pct


def feature_score_model(train, features):
    """Fit direction + standardization using discovery rows only."""
    model = {}
    y = [r["target"] for r in train]
    for feature in features:
        vals = [float(r[feature]) for r in train if valid(r.get(feature))]
        if len(vals) != len(train) or len(vals) < 3:
            continue
        rho = base.spearman(vals, y)
        if rho is None:
            continue
        mu = statistics.fmean(vals)
        sd = statistics.pstdev(vals) or 1.0
        model[feature] = {"mu": mu, "sd": sd, "direction": 1.0 if rho >= 0 else -1.0, "train_rho": rho}
    return model


def apply_score(rows, model):
    out = []
    for row in rows:
        z = []
        for feature, info in model.items():
            v = row.get(feature)
            if not valid(v):
                z = []
                break
            z.append(info["direction"] * (float(v) - info["mu"]) / info["sd"])
        if z:
            item = dict(row)
            item["_score"] = statistics.fmean(z)
            out.append(item)
    return out


def evaluate_feature_set(rows, features, seed=0):
    rows = [r for r in rows if all(valid(r.get(f)) for f in features)]
    train = [r for r in rows if not split_holdout(str(r["token_mint"]), seed)]
    test = [r for r in rows if split_holdout(str(r["token_mint"]), seed)]
    tpos = sum(int(r["target"]) for r in train)
    hpos = sum(int(r["target"]) for r in test)
    if len(train) < 40 or len(test) < 15 or tpos < 8 or hpos < 3:
        return None
    model = feature_score_model(train, features)
    if len(model) != len(features):
        return None
    scored = apply_score(test, model)
    if len(scored) < 15:
        return None
    rho = base.spearman([r["_score"] for r in scored], [r["target"] for r in scored])
    if rho is None:
        return None
    ordered = sorted(scored, key=lambda r: r["_score"])
    q = max(1, len(ordered) // 4)
    lo, hi = ordered[:q], ordered[-q:]
    qdiff = 100.0 * sum(r["target"] for r in hi) / len(hi) - 100.0 * sum(r["target"] for r in lo) / len(lo)
    return {
        "rho": rho,
        "qdiff_pp": qdiff,
        "discovery_n": len(train),
        "holdout_n": len(test),
        "positives": tpos + hpos,
        "model": model,
        "n": len(rows),
    }


def evaluate_once(rows, features):
    result = evaluate_feature_set(rows, features, 0)
    if result is None:
        n = len([r for r in rows if all(valid(r.get(f)) for f in features)])
        return "COLLECT_MORE", {"n": n, "features": list(features)}
    rho, qd = result["rho"], result["qdiff_pp"]
    if rho >= 0.12 and qd >= 2.0:
        verdict = "PROMISING"
    elif rho >= 0.05:
        verdict = "WEAK"
    else:
        verdict = "REJECT"
    result["features"] = list(features)
    result["holdout_signed_rho"] = rho
    return verdict, result


def evaluate_robust(rows, features):
    runs = []
    for seed in range(ROBUST_SPLITS):
        r = evaluate_feature_set(rows, features, seed)
        if r is not None:
            runs.append(r)
    rhos = [r["rho"] for r in runs]
    qds = [r["qdiff_pp"] for r in runs]
    if len(runs) < max(15, ROBUST_SPLITS // 2):
        return "COLLECT_MORE", {"features": list(features), "valid_splits": len(runs), "requested_splits": ROBUST_SPLITS}
    med = median(rhos)
    p10 = percentile(rhos, 0.10)
    sign = sum(x > 0 for x in rhos) / len(rhos)
    med_qd = median(qds)
    verdict = "ROBUST" if (
        med is not None and med >= 0.10 and p10 is not None and p10 > 0 and sign >= 0.80 and med_qd is not None and med_qd >= 2.0
    ) else "REJECT_ROBUSTNESS"
    return verdict, {
        "features": list(features),
        "valid_splits": len(runs),
        "requested_splits": ROBUST_SPLITS,
        "median_rho": med,
        "p10_rho": p10,
        "p90_rho": percentile(rhos, 0.90),
        "sign_rate": sign,
        "median_qdiff_pp": med_qd,
        "holdout_signed_rho": med,
        "qdiff_pp": med_qd,
        "discovery_n": int(median([r["discovery_n"] for r in runs]) or 0),
        "holdout_n": int(median([r["holdout_n"] for r in runs]) or 0),
        "positives": int(median([r["positives"] for r in runs]) or 0),
        "n": int(median([r["n"] for r in runs]) or 0),
    }


def resurrection_multifeature_dataset(stage_s, target, features):
    db = core.open_market()
    if not base.table_exists(db, "lab_exp0121_stage_features"):
        db.close()
        return []
    cols = {r["name"] for r in db.execute("PRAGMA table_info(lab_exp0121_stage_features)").fetchall()}
    if target not in cols or any(f not in cols for f in features):
        db.close()
        return []
    selection = ",".join(features)
    where_features = " AND ".join(f"{f} IS NOT NULL" for f in features)
    rows = db.execute(
        f"""SELECT token_mint,{selection},{target} target
            FROM lab_exp0121_stage_features
            WHERE stage_s=? AND future_ready=1 AND coverage_status='GOOD'
              AND {target} IS NOT NULL AND {where_features}""",
        (stage_s,),
    ).fetchall()
    db.close()
    return [dict(r) | {"token_mint": str(r["token_mint"]), "target": int(r["target"])} for r in rows]


def dataset_for_payload(payload):
    branch = payload["branch"]
    if branch == "MIGRATION":
        return base.migration_dataset(int(payload["stage_s"]), int(payload["horizon_s"]))
    if branch == "RESURRECTION":
        return resurrection_multifeature_dataset(int(payload["stage_s"]), payload["target"], payload["features"])
    raise ValueError(f"unknown branch {branch}")


def run_payload(payload):
    adapter = payload["adapter"]
    if adapter in ("migration_univariate", "resurrection_univariate"):
        return base.run_job({"payload": payload})
    rows = dataset_for_payload(payload)
    features = tuple(payload["features"])
    if adapter == "feature_set_refinement":
        return evaluate_once(rows, features)
    if adapter == "feature_set_robustness":
        return evaluate_robust(rows, features)
    raise ValueError(f"unknown adapter {adapter}")


def safe_claim(worker_id):
    return base.safe_claim(worker_id)


def finish_job(job, verdict, metrics):
    stage = "ROBUSTNESS" if job["payload"].get("adapter") == "feature_set_robustness" else (
        "REFINEMENT" if job["payload"].get("adapter") == "feature_set_refinement" else "DISCOVERY"
    )
    primary = metrics.get("holdout_signed_rho")
    effect = metrics.get("qdiff_pp")
    core.finish_job(
        job,
        verdict,
        stage,
        metrics,
        discovery_n=metrics.get("discovery_n"),
        holdout_n=metrics.get("holdout_n"),
        positives=metrics.get("positives"),
        primary_metric=primary,
        effect_size=effect,
        coverage={"n": metrics.get("n"), "valid_splits": metrics.get("valid_splits")},
    )


def worker_main(index):
    worker_id = f"ORG-{index:02d}-{os.getpid()}"
    core.worker_heartbeat(worker_id, "RUNNING")
    while True:
        job = safe_claim(worker_id)
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


def result_processed(db, result_id):
    fp = hashlib.sha256(f"processed:{result_id}".encode()).hexdigest()
    return db.execute("SELECT 1 FROM v41_memory WHERE fingerprint=?", (fp,)).fetchone() is not None


def mark_result_memory(db, row, spec):
    fp = hashlib.sha256(f"processed:{row['result_id']}".encode()).hexdigest()
    memory_id = "M_" + fp[:22]
    lesson = (
        f"{row['branch']}/{row['family']} {row['stage']} -> {row['verdict']} "
        f"metric={row['primary_metric']} effect={row['effect_size']}"
    )
    evidence = {
        "result_id": row["result_id"],
        "hypothesis_id": row["hypothesis_id"],
        "stage": row["stage"],
        "spec": spec,
        "primary_metric": row["primary_metric"],
        "effect_size": row["effect_size"],
        "p_value": row["p_value"],
    }
    db.execute(
        """INSERT OR IGNORE INTO v41_memory(memory_id,fingerprint,branch,family,verdict,lesson,evidence_json,created_at)
           VALUES(?,?,?,?,?,?,?,?)""",
        (memory_id, fp, row["branch"], row["family"], row["verdict"], lesson, core.canonical_json(evidence), time.time()),
    )


def feature_universe(branch):
    return MIG_ALL_FEATURES if branch == "MIGRATION" else RES_ALL_FEATURES if branch == "RESURRECTION" else ()


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
    made = 0

    # A refined PROMISING feature-set gets attacked by repeated split robustness.
    if row["stage"] == "REFINEMENT" and verdict == "PROMISING":
        payload = child_payload(spec, parent_features, "feature_set_robustness")
        hid, _ = core.create_hypothesis(
            branch, row["family"], payload,
            {"lane": "robustness", "parent_result": row["result_id"]},
            parent_hypothesis_id=row["hypothesis_id"], generation=generation + 1,
        )
        _, created = core.enqueue_job(hid, "ROBUSTNESS", payload, priority=10)
        return int(created)

    # Discovery results mutate only when evidence is non-trivial.
    if row["stage"] != "DISCOVERY" or verdict not in ("PROMISING", "WEAK"):
        return 0

    limit = MAX_CHILDREN_PROMISING if verdict == "PROMISING" else MAX_CHILDREN_WEAK
    candidates = [f for f in feature_universe(branch) if f not in parent_features]

    # Prefer candidate features whose own discovery result was strongest in the same branch/context.
    # The deterministic ordering below keeps the search reproducible if no ranking is available.
    for other in candidates[:limit]:
        features = tuple(dict.fromkeys(parent_features + (other,)))
        payload = child_payload(spec, features, "feature_set_refinement")
        hid, _ = core.create_hypothesis(
            branch, row["family"] + "+AUTO", payload,
            {"lane": "refinement", "parent_result": row["result_id"]},
            parent_hypothesis_id=row["hypothesis_id"], generation=generation + 1,
        )
        _, created = core.enqueue_job(hid, "REFINEMENT", payload, priority=25 if verdict == "PROMISING" else 40)
        made += int(created)
    return made


def freeze_candidate(row, spec, metrics):
    if row["verdict"] != "ROBUST" or row["stage"] != "ROBUSTNESS":
        return False
    dbm = core.open_market()
    cutoff = time.time()
    if base.table_exists(dbm, "t116_pump_swaps"):
        x = dbm.execute("SELECT MAX(timestamp) FROM t116_pump_swaps").fetchone()[0]
        if x is not None:
            cutoff = float(x)
    dbm.close()
    candidate_id = "C_" + hashlib.sha256((row["hypothesis_id"] + row["result_id"]).encode()).hexdigest()[:22]
    now = time.time()
    db = core.open_research()
    db.execute("BEGIN IMMEDIATE")
    try:
        db.execute(
            """INSERT OR IGNORE INTO v41_candidates(
                 candidate_id,hypothesis_id,status,frozen_spec_json,frozen_model_json,data_cutoff,
                 promoted_from_result_id,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (candidate_id, row["hypothesis_id"], "FROZEN", core.canonical_json(spec), core.canonical_json(metrics), cutoff, row["result_id"], now, now),
        )
        created = db.total_changes > 0
        db.commit()
    except BaseException:
        db.rollback(); raise
    finally:
        db.close()
    return created


def auto_director_tick(max_results=80):
    """Consume fresh results once, write memory, mutate/promote scientifically."""
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
        if result_processed(db, row["result_id"]):
            db.close(); continue
        spec = json.loads(row["spec_json"])
        metrics = json.loads(row["metrics_json"])
        db.execute("BEGIN IMMEDIATE")
        try:
            mark_result_memory(db, row, spec)
            db.commit()
        except BaseException:
            db.rollback(); db.close(); raise
        db.close()
        processed += 1
        if row["verdict"] == "ROBUST" and row["stage"] == "ROBUSTNESS":
            frozen += int(freeze_candidate(row, spec, metrics))
        else:
            spawned += spawn_children(row, spec)
    return processed, spawned, frozen


def seed_discovery_if_needed():
    """Keep the original generation-0 catalog available, but never duplicate it."""
    return base.director_tick(base.seed_catalog())


def display(last_director):
    db = core.open_research()
    jobs = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    verdicts = {r["verdict"]: r["n"] for r in db.execute("SELECT verdict,COUNT(*) n FROM v41_results GROUP BY verdict")}
    mem = db.execute("SELECT COUNT(*) FROM v41_memory").fetchone()[0]
    frozen = db.execute("SELECT COUNT(*) FROM v41_candidates WHERE status='FROZEN'").fetchone()[0]
    gens = db.execute("SELECT generation,COUNT(*) n FROM v41_hypotheses GROUP BY generation ORDER BY generation").fetchall()
    db.close()
    print("\033[2J\033[H", end="")
    print("=" * 118)
    print("MEMECOIN LAB — AUTONOMOUS RESEARCH ORGANISM V4.1")
    print("=" * 118)
    print(f"WORKERS={WORKERS} | QUEUED={jobs.get('QUEUED',0)} | RUNNING={jobs.get('RUNNING',0)} | DONE={jobs.get('DONE',0)} | FAILED={jobs.get('FAILED',0)}")
    print(f"MEMORY={mem} | FROZEN={frozen} | DIRECTOR processed/spawned/frozen={last_director}")
    print("GENERATIONS:", " ".join(f"G{x['generation']}={x['n']}" for x in gens))
    print("VERDICTS:", " ".join(f"{k}={v}" for k, v in sorted(verdicts.items())))
    print("Research-only | recursive director | no live trading | Control Room http://127.0.0.1:8765")


def main():
    global STOP
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    core.initialize()
    seed_discovery_if_needed()
    workers = [mp.Process(target=worker_main, args=(i + 1,), daemon=True) for i in range(WORKERS)]
    for p in workers:
        p.start()
    last_director = (0, 0, 0)
    try:
        while not STOP:
            core.reclaim_expired_jobs()
            seed_discovery_if_needed()
            last_director = auto_director_tick()
            display(last_director)
            time.sleep(DIRECTOR_SLEEP)
    finally:
        for p in workers:
            if p.is_alive():
                p.terminate()
        for p in workers:
            p.join(timeout=3)
        print("V4.1 organism stopped cleanly")


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()
