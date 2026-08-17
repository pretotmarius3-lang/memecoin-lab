#!/usr/bin/env python3
"""Memecoin Lab V5.7.2 — Scientific Candidate & Champion Arena.

Scientific de-duplication layer over V5.7.1.

V5.7.2 does NOT rewrite V5.7.1 raw arenas or duels. It:
- runs the normal V5.7.1 mechanics;
- canonicalizes controls and challengers by scientific identity;
- collapses duplicate head-to-heads across equivalent candidate lineages;
- counts each V4.9 evidence experiment once;
- exposes a scientific leaderboard whose win count cannot be inflated by the
  same SIDE evidence being referenced from several arenas.

Raw V5.7.1 remains the audit trail. CONTROL is immutable and SIDE evidence
remains exploratory until the normal future-only pipeline validates it.
"""
from __future__ import annotations

import json
import math
import os
import signal
import statistics
import time
from collections import defaultdict

import v41_core as core
import v561_diversity_gate as diversity
import v571_candidate_champion_arena as v571

LOOP = float(os.environ.get("MEMECOIN_V572_LOOP_S", "5"))
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


def init():
    v571.init()
    d = core.open_research()
    d.executescript("""
    CREATE TABLE IF NOT EXISTS v572_scientific_duels(
      scientific_duel_key TEXT PRIMARY KEY,
      control_scientific_key TEXT NOT NULL,
      challenger_scientific_key TEXT NOT NULL,
      control_kind TEXT NOT NULL,
      mutation_kind TEXT NOT NULL,
      family TEXT,
      control_state TEXT,
      control_spec_json TEXT NOT NULL,
      challenger_spec_json TEXT NOT NULL,
      mutation_label TEXT NOT NULL,
      arena_count INTEGER NOT NULL,
      raw_duel_count INTEGER NOT NULL,
      unique_evidence_count INTEGER NOT NULL,
      direct_evidence_count INTEGER NOT NULL,
      reused_evidence_count INTEGER NOT NULL,
      control_rho REAL,
      challenger_rho REAL,
      delta_rho REAL,
      challenger_n INTEGER NOT NULL,
      outcome TEXT NOT NULL,
      score REAL NOT NULL,
      representative_challenger_id TEXT NOT NULL,
      representative_evidence_id TEXT NOT NULL,
      evidence_ids_json TEXT NOT NULL,
      raw_challenger_ids_json TEXT NOT NULL,
      metrics_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v572_control_science
      ON v572_scientific_duels(control_scientific_key);
    CREATE INDEX IF NOT EXISTS idx_v572_challenger_science
      ON v572_scientific_duels(challenger_scientific_key);
    CREATE INDEX IF NOT EXISTS idx_v572_outcome
      ON v572_scientific_duels(outcome);

    CREATE TABLE IF NOT EXISTS v572_scientific_conclusions(
      conclusion_id TEXT PRIMARY KEY,
      control_scientific_key TEXT NOT NULL,
      verdict TEXT NOT NULL,
      statement TEXT NOT NULL,
      evidence_json TEXT NOT NULL,
      next_action TEXT NOT NULL,
      updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v572_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    """)
    d.commit()
    d.close()


def sci_key(kind, raw_spec):
    try:
        spec = raw_spec if isinstance(raw_spec, dict) else json.loads(raw_spec)
        return diversity.scientific_key(kind, spec)
    except Exception:
        raw = raw_spec if isinstance(raw_spec, str) else core.canonical_json(raw_spec)
        return core.fingerprint({"kind": str(kind), "spec": raw}, "v572fallback:")[:28]


def load_raw_duels():
    d = core.open_research()
    rows = [dict(r) for r in d.execute("""
      SELECT d.*,a.control_candidate_id,a.control_state,a.control_kind,
             a.control_spec_json,a.family,c.mutation_kind,
             c.spec_json challenger_spec_json,c.mutation_label
      FROM v571_duels d
      JOIN v571_arenas a USING(arena_id)
      JOIN v571_challengers c USING(challenger_id)
    """).fetchall()]
    d.close()
    return rows


def aggregate_group(rows):
    """Collapse duplicate lineages and duplicate evidence for one scientific duel."""
    by_evidence = {}
    for r in rows:
        eid = r["evidence_experiment_id"]
        old = by_evidence.get(eid)
        if old is None or sf(r.get("score"), -999) > sf(old.get("score"), -999):
            by_evidence[eid] = r

    evidence_rows = list(by_evidence.values())
    representative = max(evidence_rows, key=lambda r: sf(r.get("score"), -999))

    control_rhos = [sf(r.get("control_rho")) for r in evidence_rows]
    control_rhos = [x for x in control_rhos if x is not None]
    challenger_rhos = [sf(r.get("challenger_rho")) for r in evidence_rows]
    challenger_rhos = [x for x in challenger_rhos if x is not None]
    deltas = [sf(r.get("delta_rho")) for r in evidence_rows]
    deltas = [x for x in deltas if x is not None]

    cr = statistics.median(control_rhos) if control_rhos else 0.0
    rr = statistics.median(challenger_rhos) if challenger_rhos else 0.0
    delta = statistics.median(deltas) if deltas else rr - cr
    effective_n = max((int(r.get("challenger_n") or 0) for r in evidence_rows), default=0)
    sample = min(1.0, effective_n / 150.0)
    score = delta * sample

    if delta >= 0.08 and rr >= 0.10:
        outcome = "CHALLENGER_WINS"
    elif delta <= -0.05:
        outcome = "CONTROL_WINS"
    else:
        outcome = "TIE_OR_SPECIALIST"

    direct = sum(1 for r in evidence_rows if r.get("evidence_mode") == "DIRECT")
    reused = sum(1 for r in evidence_rows if r.get("evidence_mode") == "REUSED_DIVERSITY")
    arenas = sorted({r["arena_id"] for r in rows})
    challenger_ids = sorted({r["challenger_id"] for r in rows})
    evidence_ids = sorted(by_evidence)

    metrics = {
        "aggregation": "scientific_pair_unique_evidence_median",
        "arena_count": len(arenas),
        "raw_duel_count": len(rows),
        "unique_evidence_count": len(evidence_rows),
        "control_rho_values": control_rhos,
        "challenger_rho_values": challenger_rhos,
        "delta_rho_values": deltas,
        "effective_max_challenger_n": effective_n,
        "direct_evidence_count": direct,
        "reused_evidence_count": reused,
    }

    return {
        "representative": representative,
        "arena_count": len(arenas),
        "raw_duel_count": len(rows),
        "unique_evidence_count": len(evidence_rows),
        "direct": direct,
        "reused": reused,
        "control_rho": cr,
        "challenger_rho": rr,
        "delta_rho": delta,
        "challenger_n": effective_n,
        "outcome": outcome,
        "score": score,
        "evidence_ids": evidence_ids,
        "challenger_ids": challenger_ids,
        "metrics": metrics,
    }


def rebuild_scientific_duels():
    raw = load_raw_duels()
    groups = defaultdict(list)
    for r in raw:
        ck = sci_key(r["control_kind"], r["control_spec_json"])
        hk = sci_key(r["mutation_kind"], r["challenger_spec_json"])
        groups[(ck, hk)].append(r)

    now = time.time()
    d = core.open_research()
    d.execute("BEGIN IMMEDIATE")
    try:
        d.execute("DELETE FROM v572_scientific_duels")
        for (ck, hk), rows in groups.items():
            a = aggregate_group(rows)
            rep = a["representative"]
            sdk = core.fingerprint({"c": ck, "h": hk}, "v572duel:")[:30]
            d.execute("""
              INSERT INTO v572_scientific_duels(
                scientific_duel_key,control_scientific_key,challenger_scientific_key,
                control_kind,mutation_kind,family,control_state,control_spec_json,
                challenger_spec_json,mutation_label,arena_count,raw_duel_count,
                unique_evidence_count,direct_evidence_count,reused_evidence_count,
                control_rho,challenger_rho,delta_rho,challenger_n,outcome,score,
                representative_challenger_id,representative_evidence_id,
                evidence_ids_json,raw_challenger_ids_json,metrics_json,updated_at
              ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,(
              sdk,ck,hk,rep["control_kind"],rep["mutation_kind"],rep["family"],
              rep["control_state"],rep["control_spec_json"],rep["challenger_spec_json"],
              rep["mutation_label"],a["arena_count"],a["raw_duel_count"],
              a["unique_evidence_count"],a["direct"],a["reused"],a["control_rho"],
              a["challenger_rho"],a["delta_rho"],a["challenger_n"],a["outcome"],
              a["score"],rep["challenger_id"],rep["evidence_experiment_id"],
              core.canonical_json(a["evidence_ids"]),core.canonical_json(a["challenger_ids"]),
              core.canonical_json(a["metrics"]),now
            ))
        d.commit()
    except BaseException:
        d.rollback()
        raise
    finally:
        d.close()
    return len(raw), len(groups)


def rebuild_conclusions():
    d = core.open_research()
    controls = [r[0] for r in d.execute(
        "SELECT DISTINCT control_scientific_key FROM v572_scientific_duels"
    ).fetchall()]
    d.close()

    now = time.time()
    made = 0
    d = core.open_research()
    d.execute("BEGIN IMMEDIATE")
    try:
        d.execute("DELETE FROM v572_scientific_conclusions")
        for ck in controls:
            rows = [dict(r) for r in d.execute("""
              SELECT * FROM v572_scientific_duels
              WHERE control_scientific_key=? ORDER BY score DESC
            """,(ck,)).fetchall()]
            if not rows:
                continue

            wins = [r for r in rows if r["outcome"] == "CHALLENGER_WINS"]
            losses = [r for r in rows if r["outcome"] == "CONTROL_WINS"]
            best = rows[0]
            unique_evidence = len({
                eid for r in rows for eid in json.loads(r["evidence_ids_json"])
            })

            if wins:
                verdict = "EVOLVE"
                next_action = "PROMOTE_UNIQUE_BEST_TO_NORMAL_FUTURE_ONLY_PIPELINE"
                statement = (
                    f"{len(wins)} unique scientific challenger(s) beat this control. "
                    f"Best: {best['mutation_label']} delta_rho={sf(best['delta_rho'],0):+.3f}, "
                    f"rho={sf(best['challenger_rho'],0):+.3f}, "
                    f"unique_evidence={best['unique_evidence_count']}."
                )
            elif losses and len(losses) == len(rows):
                verdict = "CONTROL_DEFENDS"
                next_action = "SEARCH_ORTHOGONAL_SCIENTIFIC_MUTATION"
                statement = (
                    f"Control defended against all {len(rows)} unique scientific challengers "
                    f"across {unique_evidence} unique evidence experiment(s)."
                )
            else:
                verdict = "SPECIALIST_MAP"
                next_action = "TEST_CONDITIONAL_REGIME_OR_TARGET_SPECIALIZATION"
                statement = (
                    f"No universal unique replacement. Best delta_rho="
                    f"{sf(best['delta_rho'],0):+.3f}; map conditional timing/target effects."
                )

            evidence = {
                "unique_scientific_duels": len(rows),
                "unique_challenger_wins": len(wins),
                "unique_control_wins": len(losses),
                "unique_evidence_experiments": unique_evidence,
                "best_scientific_duel_key": best["scientific_duel_key"],
                "best_challenger_scientific_key": best["challenger_scientific_key"],
                "best_delta_rho": best["delta_rho"],
            }
            cid = "K572_" + core.fingerprint({"c": ck, "v": verdict}, "v572conclusion:")[:22]
            d.execute("""
              INSERT INTO v572_scientific_conclusions(
                conclusion_id,control_scientific_key,verdict,statement,
                evidence_json,next_action,updated_at
              ) VALUES(?,?,?,?,?,?,?)
            """,(cid,ck,verdict,statement,core.canonical_json(evidence),next_action,now))
            made += 1
        d.commit()
    except BaseException:
        d.rollback()
        raise
    finally:
        d.close()
    return made


def display(raw_count, unique_count, conclusions_n):
    d = core.open_research()
    raw_arenas = d.execute("SELECT COUNT(*) FROM v571_arenas").fetchone()[0]
    raw_challengers = d.execute("SELECT COUNT(*) FROM v571_challengers").fetchone()[0]
    raw_duels = d.execute("SELECT COUNT(*) FROM v571_duels").fetchone()[0]
    raw_wins = d.execute(
        "SELECT COUNT(*) FROM v571_duels WHERE outcome='CHALLENGER_WINS'"
    ).fetchone()[0]
    unique_controls = d.execute(
        "SELECT COUNT(DISTINCT control_scientific_key) FROM v572_scientific_duels"
    ).fetchone()[0]
    unique_challengers = d.execute(
        "SELECT COUNT(DISTINCT challenger_scientific_key) FROM v572_scientific_duels"
    ).fetchone()[0]
    unique_wins = d.execute(
        "SELECT COUNT(*) FROM v572_scientific_duels WHERE outcome='CHALLENGER_WINS'"
    ).fetchone()[0]
    unique_evidence = d.execute(
        "SELECT COUNT(DISTINCT evidence_experiment_id) FROM v571_duels"
    ).fetchone()[0]
    top = [dict(r) for r in d.execute("""
      SELECT * FROM v572_scientific_duels ORDER BY score DESC LIMIT 10
    """).fetchall()]
    cons = [dict(r) for r in d.execute("""
      SELECT * FROM v572_scientific_conclusions ORDER BY updated_at DESC LIMIT 6
    """).fetchall()]
    d.close()

    collapse = raw_duels - unique_count
    collapse_pct = (100.0 * collapse / raw_duels) if raw_duels else 0.0

    print("\033[2J\033[H", end="")
    print("=" * 154)
    print("MEMECOIN LAB — SCIENTIFIC CANDIDATE & CHAMPION ARENA V5.7.2")
    print("=" * 154)
    print(
      f"RAW: ARENAS={raw_arenas} CHALLENGERS={raw_challengers} DUELS={raw_duels} WINS={raw_wins} | "
      f"SCIENTIFIC: CONTROLS={unique_controls} CHALLENGERS={unique_challengers} "
      f"DUELS={unique_count} WINS={unique_wins}"
    )
    print(
      f"UNIQUE V4.9 EVIDENCE={unique_evidence} | DUPLICATE DUELS COLLAPSED={collapse} ({collapse_pct:.1f}%) | "
      f"SCIENTIFIC CONCLUSIONS={conclusions_n}"
    )

    print("\nUNIQUE SCIENTIFIC HEAD-TO-HEAD LEADERBOARD")
    if not top:
        print("No scientific duel available yet.")
    for r in top:
        print(
          f"{r['outcome']:<20} {r['control_state']:<7} {r['family']:<22} "
          f"delta_rho={sf(r['delta_rho'],0):+.3f} rho={sf(r['challenger_rho'],0):+.3f} "
          f"evidence={r['unique_evidence_count']:<2} raw_refs={r['raw_duel_count']:<2} "
          f"{r['mutation_label'][:48]}"
        )

    print("\nWHAT THE LAB LEARNED — DEDUPLICATED")
    if not cons:
        print("Waiting for unique scientific head-to-heads.")
    for r in cons:
        print(f"{r['verdict']:<16} {r['statement'][:124]}")

    print(
      "\nGuardrail: V5.7.2 never multiplies confidence because the same V4.9 evidence appears in several arenas. "
      "Raw V5.7.1 stays intact for audit; only unique scientific evidence is interpreted here."
    )


def cycle():
    v571.ensure_arenas()
    v571.seed_challengers()
    v571.refresh_duels()
    v571.conclusions()
    raw_count, unique_count = rebuild_scientific_duels()
    conclusions_n = rebuild_conclusions()
    display(raw_count, unique_count, conclusions_n)

    state = {
        "raw_duels_seen": raw_count,
        "unique_scientific_duels": unique_count,
        "scientific_conclusions": conclusions_n,
    }
    d = core.open_research()
    d.execute("""
      INSERT INTO v572_state(key,value_json,updated_at)
      VALUES('latest',?,?)
      ON CONFLICT(key) DO UPDATE SET
        value_json=excluded.value_json,updated_at=excluded.updated_at
    """,(core.canonical_json(state),time.time()))
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
            print("V5.7.2 error:", repr(e), flush=True)
        time.sleep(LOOP)


if __name__ == "__main__":
    main()
