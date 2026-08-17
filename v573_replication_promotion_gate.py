#!/usr/bin/env python3
"""Memecoin Lab V5.7.3 — Replication & Promotion Gate.

Purpose:
- consume V5.7.2 scientifically de-duplicated winning challengers;
- distinguish repeated references from genuinely newer temporal evidence;
- measure consistency of each winning hypothesis across materially advanced
  watermarks;
- request a new V4.9 SIDE retest only when enough unseen rows have accumulated;
- mark hypotheses READY_FOR_FUTURE_ONLY only after repeated SIDE replication.

Important: READY_FOR_FUTURE_ONLY is NOT promotion to champion. This module never
changes V5.5 frozen rules, beliefs, prospective records, or CONTROL. It only
identifies which SIDE discoveries have earned the right to enter the normal
future-only validation pipeline.

Research-only. No trading/signing.
"""
from __future__ import annotations

import json
import math
import os
import signal
import statistics
import time

import v41_core as core
import v49_recursive_lab as v49
import v561_diversity_gate as diversity
import v572_candidate_champion_arena as v572

LOOP = float(os.environ.get("MEMECOIN_V573_LOOP_S", "5"))
TARGET_TEMPORAL_REPLICATIONS = int(os.environ.get("MEMECOIN_V573_TARGET_REPS", "3"))
MIN_READY_N = int(os.environ.get("MEMECOIN_V573_MIN_READY_N", "150"))
MIN_MEDIAN_DELTA = float(os.environ.get("MEMECOIN_V573_MIN_MEDIAN_DELTA", "0.08"))
MIN_CONSISTENCY = float(os.environ.get("MEMECOIN_V573_MIN_CONSISTENCY", "0.80"))
STOP = False


def stop(*_):
    global STOP
    STOP = True


def sf(x, d=None):
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def tables(d):
    return {r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def init():
    v572.init()
    d = core.open_research()
    d.executescript("""
    CREATE TABLE IF NOT EXISTS v573_replication_status(
      challenger_scientific_key TEXT PRIMARY KEY,
      scientific_duel_key TEXT NOT NULL,
      control_scientific_key TEXT NOT NULL,
      mutation_kind TEXT NOT NULL,
      challenger_spec_json TEXT NOT NULL,
      mutation_label TEXT NOT NULL,
      status TEXT NOT NULL,
      total_unique_evidence INTEGER NOT NULL,
      temporal_replications INTEGER NOT NULL,
      positive_replications INTEGER NOT NULL,
      consistency REAL NOT NULL,
      median_rho REAL,
      median_delta REAL,
      min_rho REAL,
      max_rho REAL,
      max_n INTEGER NOT NULL,
      earliest_watermark INTEGER,
      latest_evidence_watermark INTEGER,
      latest_available_watermark INTEGER,
      watermark_span INTEGER NOT NULL,
      retest_needed INTEGER NOT NULL,
      retest_experiment_id TEXT,
      evidence_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v573_status ON v573_replication_status(status);

    CREATE TABLE IF NOT EXISTS v573_retest_requests(
      request_key TEXT PRIMARY KEY,
      challenger_scientific_key TEXT NOT NULL,
      scientific_duel_key TEXT NOT NULL,
      requested_watermark INTEGER NOT NULL,
      experiment_id TEXT,
      state TEXT NOT NULL,
      reason TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v573_retest_challenger
      ON v573_retest_requests(challenger_scientific_key);

    CREATE TABLE IF NOT EXISTS v573_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    """)
    d.commit()
    d.close()


def load_winning_scientific_duels():
    d = core.open_research()
    rows = [dict(r) for r in d.execute("""
      SELECT * FROM v572_scientific_duels
      WHERE outcome='CHALLENGER_WINS'
      ORDER BY score DESC
    """).fetchall()]
    d.close()
    return rows


def experiment_bundle(eid):
    d = core.open_research()
    row = d.execute("""
      SELECT e.experiment_id,e.kind,e.spec_json,e.watermark_n,e.status,
             r.n,r.holdout_rho,r.verdict,r.comparison,r.metrics_json
      FROM v49_side_experiments e
      LEFT JOIN v49_side_results r USING(experiment_id)
      WHERE e.experiment_id=?
    """, (eid,)).fetchone()
    d.close()
    return dict(row) if row else None


def distinct_temporal_reps(evidence):
    """Greedily retain only materially newer watermarks as independent reps."""
    usable = sorted(
        [x for x in evidence if x.get("watermark_n") is not None and x.get("holdout_rho") is not None],
        key=lambda x: (int(x["watermark_n"]), x["experiment_id"]),
    )
    if not usable:
        return []

    accepted = [usable[0]]
    last_wm = int(usable[0]["watermark_n"])
    for x in usable[1:]:
        wm = int(x["watermark_n"])
        ok, _, _ = diversity.enough_new_data(last_wm, wm)
        if ok:
            accepted.append(x)
            last_wm = wm
    return accepted


def latest_watermark_for_spec(spec):
    stage = spec.get("stage", spec.get("stage1"))
    if stage is None or "horizon" not in spec or "target" not in spec:
        return None
    try:
        return v49.latest_watermark(int(stage), int(spec["horizon"]), str(spec["target"]))
    except Exception:
        return None


def classify(reps, control_rho):
    if not reps:
        return "WAITING_EVIDENCE", 0, 0.0, None, None, 0

    deltas = []
    rhos = []
    ns = []
    positives = 0

    for x in reps:
        rho = sf(x.get("holdout_rho"))
        if rho is None:
            continue
        delta = rho - control_rho
        rhos.append(rho)
        deltas.append(delta)
        ns.append(int(x.get("n") or 0))
        if delta >= MIN_MEDIAN_DELTA and rho >= 0.10:
            positives += 1

    nrep = len(deltas)
    consistency = positives / nrep if nrep else 0.0
    med_rho = statistics.median(rhos) if rhos else None
    med_delta = statistics.median(deltas) if deltas else None
    max_n = max(ns) if ns else 0

    status = "DISCOVERY"
    if nrep >= 2 and consistency >= 0.75 and med_delta is not None and med_delta >= 0.05:
        status = "REPLICATED"
    if (
        nrep >= TARGET_TEMPORAL_REPLICATIONS
        and consistency >= MIN_CONSISTENCY
        and med_delta is not None
        and med_delta >= MIN_MEDIAN_DELTA
    ):
        status = "STRONG_REPLICATION"
    if status == "STRONG_REPLICATION" and max_n >= MIN_READY_N:
        status = "READY_FOR_FUTURE_ONLY"

    return status, positives, consistency, med_rho, med_delta, max_n


def maybe_request_retest(duel, spec, reps, latest_available):
    if latest_available is None:
        return None, False

    last_wm = max((int(x["watermark_n"]) for x in reps), default=0)
    if last_wm <= 0:
        return None, False

    ok, delta, need = diversity.enough_new_data(last_wm, int(latest_available))
    if not ok:
        return None, False

    req_key = "R573_" + core.fingerprint({
        "h": duel["challenger_scientific_key"],
        "wm": int(latest_available),
    }, "v573retest:")[:22]

    d = core.open_research()
    old = d.execute(
        "SELECT experiment_id,state FROM v573_retest_requests WHERE request_key=?",
        (req_key,),
    ).fetchone()
    d.close()
    if old:
        return old["experiment_id"], True

    feature = spec.get("feature") or spec.get("weak") or ((spec.get("features") or ["COMPOSITE"])[0])
    parent = "V573_" + duel["challenger_scientific_key"]
    eid, _ = v49.insert_exp(
        duel["mutation_kind"],
        parent,
        feature,
        spec,
        int(latest_available),
        f"V5.7.3 temporal replication: {duel['mutation_label']}",
        1,
    )

    now = time.time()
    d = core.open_research()
    d.execute("""
      INSERT OR REPLACE INTO v573_retest_requests(
        request_key,challenger_scientific_key,scientific_duel_key,
        requested_watermark,experiment_id,state,reason,created_at,updated_at
      ) VALUES(?,?,?,?,?,'REQUESTED',?,?,?)
    """, (
        req_key,
        duel["challenger_scientific_key"],
        duel["scientific_duel_key"],
        int(latest_available),
        eid,
        f"watermark advanced +{delta} rows; requirement={need}",
        now,
        now,
    ))
    d.commit()
    d.close()

    diversity.cycle()
    return eid, True


def evaluate_one(duel):
    try:
        evidence_ids = json.loads(duel["evidence_ids_json"])
    except Exception:
        evidence_ids = []

    bundles = []
    for eid in evidence_ids:
        x = experiment_bundle(eid)
        if x and x.get("holdout_rho") is not None:
            bundles.append(x)

    reps = distinct_temporal_reps(bundles)
    control_rho = sf(duel.get("control_rho"), 0.0)
    status, positives, consistency, med_rho, med_delta, max_n = classify(reps, control_rho)

    spec = json.loads(duel["challenger_spec_json"])
    latest_available = latest_watermark_for_spec(spec)
    earliest = min((int(x["watermark_n"]) for x in reps), default=None)
    latest_evidence = max((int(x["watermark_n"]) for x in reps), default=None)
    span = (latest_evidence - earliest) if earliest is not None and latest_evidence is not None else 0

    retest_id = None
    retest_needed = False
    if status != "READY_FOR_FUTURE_ONLY" and len(reps) < TARGET_TEMPORAL_REPLICATIONS:
        retest_id, retest_needed = maybe_request_retest(duel, spec, reps, latest_available)

    evidence = {
        "all_unique_evidence_ids": evidence_ids,
        "temporal_replication_ids": [x["experiment_id"] for x in reps],
        "temporal_watermarks": [int(x["watermark_n"]) for x in reps],
        "temporal_rhos": [sf(x.get("holdout_rho")) for x in reps],
        "control_rho": control_rho,
        "positive_threshold_delta": MIN_MEDIAN_DELTA,
        "target_temporal_replications": TARGET_TEMPORAL_REPLICATIONS,
        "latest_available_watermark": latest_available,
    }

    now = time.time()
    d = core.open_research()
    d.execute("""
      INSERT INTO v573_replication_status(
        challenger_scientific_key,scientific_duel_key,control_scientific_key,
        mutation_kind,challenger_spec_json,mutation_label,status,
        total_unique_evidence,temporal_replications,positive_replications,
        consistency,median_rho,median_delta,min_rho,max_rho,max_n,
        earliest_watermark,latest_evidence_watermark,latest_available_watermark,
        watermark_span,retest_needed,retest_experiment_id,evidence_json,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(challenger_scientific_key) DO UPDATE SET
        scientific_duel_key=excluded.scientific_duel_key,
        control_scientific_key=excluded.control_scientific_key,
        mutation_kind=excluded.mutation_kind,
        challenger_spec_json=excluded.challenger_spec_json,
        mutation_label=excluded.mutation_label,
        status=excluded.status,
        total_unique_evidence=excluded.total_unique_evidence,
        temporal_replications=excluded.temporal_replications,
        positive_replications=excluded.positive_replications,
        consistency=excluded.consistency,
        median_rho=excluded.median_rho,
        median_delta=excluded.median_delta,
        min_rho=excluded.min_rho,
        max_rho=excluded.max_rho,
        max_n=excluded.max_n,
        earliest_watermark=excluded.earliest_watermark,
        latest_evidence_watermark=excluded.latest_evidence_watermark,
        latest_available_watermark=excluded.latest_available_watermark,
        watermark_span=excluded.watermark_span,
        retest_needed=excluded.retest_needed,
        retest_experiment_id=excluded.retest_experiment_id,
        evidence_json=excluded.evidence_json,
        updated_at=excluded.updated_at
    """, (
        duel["challenger_scientific_key"],
        duel["scientific_duel_key"],
        duel["control_scientific_key"],
        duel["mutation_kind"],
        duel["challenger_spec_json"],
        duel["mutation_label"],
        status,
        len(evidence_ids),
        len(reps),
        positives,
        consistency,
        med_rho,
        med_delta,
        min((sf(x.get("holdout_rho"), 0.0) for x in reps), default=None),
        max((sf(x.get("holdout_rho"), 0.0) for x in reps), default=None),
        max_n,
        earliest,
        latest_evidence,
        latest_available,
        span,
        int(retest_needed),
        retest_id,
        core.canonical_json(evidence),
        now,
    ))
    d.commit()
    d.close()


def refresh_retest_states():
    d = core.open_research()
    rows = [dict(r) for r in d.execute("""
      SELECT * FROM v573_retest_requests
      WHERE state IN ('REQUESTED','WAITING')
    """).fetchall()]
    d.close()

    now = time.time()
    for r in rows:
        d = core.open_research()
        x = d.execute(
            "SELECT status FROM v49_side_experiments WHERE experiment_id=?",
            (r["experiment_id"],),
        ).fetchone()
        has_result = d.execute(
            "SELECT 1 FROM v49_side_results WHERE experiment_id=?",
            (r["experiment_id"],),
        ).fetchone()
        if has_result:
            state = "DONE"
        elif x:
            state = str(x["status"])
        else:
            state = "MISSING"
        d.execute(
            "UPDATE v573_retest_requests SET state=?,updated_at=? WHERE request_key=?",
            (state, now, r["request_key"]),
        )
        d.commit()
        d.close()


def display():
    d = core.open_research()
    total = d.execute("SELECT COUNT(*) FROM v573_replication_status").fetchone()[0]
    counts = {r["status"]: int(r["n"]) for r in d.execute("""
      SELECT status,COUNT(*) n FROM v573_replication_status GROUP BY status
    """).fetchall()}
    requests = d.execute("SELECT COUNT(*) FROM v573_retest_requests").fetchone()[0]
    req_done = d.execute("SELECT COUNT(*) FROM v573_retest_requests WHERE state='DONE'").fetchone()[0]
    top = [dict(r) for r in d.execute("""
      SELECT * FROM v573_replication_status
      ORDER BY CASE status
        WHEN 'READY_FOR_FUTURE_ONLY' THEN 0
        WHEN 'STRONG_REPLICATION' THEN 1
        WHEN 'REPLICATED' THEN 2
        WHEN 'DISCOVERY' THEN 3
        ELSE 4 END,
        temporal_replications DESC,
        consistency DESC,
        median_delta DESC
      LIMIT 12
    """).fetchall()]
    d.close()

    print("\033[2J\033[H", end="")
    print("=" * 156)
    print("MEMECOIN LAB — REPLICATION & PROMOTION GATE V5.7.3")
    print("=" * 156)
    print(
        f"WINNING SCIENTIFIC HYPOTHESES={total} | "
        f"DISCOVERY={counts.get('DISCOVERY',0)} | "
        f"REPLICATED={counts.get('REPLICATED',0)} | "
        f"STRONG={counts.get('STRONG_REPLICATION',0)} | "
        f"FUTURE_ONLY_READY={counts.get('READY_FOR_FUTURE_ONLY',0)} | "
        f"WAITING={counts.get('WAITING_EVIDENCE',0)}"
    )
    print(f"TEMPORAL RETEST REQUESTS={requests} | RETESTS DONE={req_done}")

    print("\nREPLICATION LEADERBOARD")
    if not top:
        print("No V5.7.2 winning scientific hypothesis available yet.")
    for r in top:
        print(
            f"{r['status']:<22} reps={r['temporal_replications']:<2} "
            f"positive={r['positive_replications']:<2} consistency={100*r['consistency']:.0f}% "
            f"med_delta={sf(r['median_delta'],0):+.3f} med_rho={sf(r['median_rho'],0):+.3f} "
            f"wm={r['earliest_watermark']}->{r['latest_evidence_watermark']} "
            f"latest={r['latest_available_watermark']}  {r['mutation_label'][:48]}"
        )

    print(
        "\nGuardrail: READY_FOR_FUTURE_ONLY only means replicated SIDE evidence has earned a prospective test. "
        "It does not alter CONTROL, freeze a rule, or promote a champion."
    )


def cycle():
    # Keep V5.7.2 current first.
    v572.cycle()
    refresh_retest_states()

    winners = load_winning_scientific_duels()
    for duel in winners:
        evaluate_one(duel)

    display()

    d = core.open_research()
    state = {
        "winning_hypotheses": len(winners),
        "updated_at": time.time(),
    }
    d.execute("""
      INSERT INTO v573_state(key,value_json,updated_at)
      VALUES('latest',?,?)
      ON CONFLICT(key) DO UPDATE SET
        value_json=excluded.value_json,updated_at=excluded.updated_at
    """, (core.canonical_json(state), time.time()))
    d.commit()
    d.close()


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    init()
    while not STOP:
        try:
            cycle()
        except Exception as e:
            print("V5.7.3 error:", repr(e), flush=True)
        time.sleep(LOOP)


if __name__ == "__main__":
    main()
