#!/usr/bin/env python3
"""Memecoin Lab V5.7.1 — Candidate & Champion Arena.

Compatibility-focused successor to V5.7.

Key changes vs V5.7:
- keeps V5.7 tables/history untouched by using v571_* tables;
- distinguishes single-stage controls from sequence controls;
- never sends stage1/stage2 sequence specs through V4.9 single-stage experiment kinds;
- reuses scientifically equivalent V4.9 evidence when V5.6.1 marks an arena
  challenger SKIPPED_DIVERSITY;
- preserves CONTROL immutability and SIDE-only status for every challenger.

A challenger can produce a duel from:
1) its own completed v49_side_results row (DIRECT evidence), or
2) a V5.6.1 representative experiment that already has a completed
   v49_side_results row (REUSED_DIVERSITY evidence).

No frozen/prospective rule is changed here.
Research-only. No trading/signing.
"""
from __future__ import annotations

import json
import math
import os
import signal
import time

import v41_core as core
import v49_recursive_lab as v49
import v561_diversity_gate as diversity


LOOP = float(os.environ.get("MEMECOIN_V571_LOOP_S", "5"))
MAX_CONTROLS = int(os.environ.get("MEMECOIN_V571_MAX_CONTROLS", "12"))
MAX_CHALLENGERS = int(os.environ.get("MEMECOIN_V571_MAX_CHALLENGERS", "5"))
MIN_CONTROL_N = int(os.environ.get("MEMECOIN_V571_MIN_CONTROL_N", "20"))

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
    return {
        r[0]
        for r in d.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def init():
    v49.init_db()
    diversity.init()

    d = core.open_research()
    d.executescript(
        """
        CREATE TABLE IF NOT EXISTS v571_arenas(
          arena_id TEXT PRIMARY KEY,
          control_candidate_id TEXT NOT NULL,
          control_state TEXT NOT NULL,
          control_kind TEXT NOT NULL,
          control_spec_json TEXT NOT NULL,
          family TEXT,
          control_n INTEGER NOT NULL,
          control_rho REAL,
          control_lift REAL,
          control_confidence REAL NOT NULL,
          control_shape TEXT NOT NULL,
          state TEXT NOT NULL,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS v571_challengers(
          challenger_id TEXT PRIMARY KEY,
          arena_id TEXT NOT NULL,
          experiment_id TEXT,
          mutation_kind TEXT NOT NULL,
          spec_json TEXT NOT NULL,
          mutation_label TEXT NOT NULL,
          control_shape TEXT NOT NULL,
          state TEXT NOT NULL,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_v571_challenger_arena
          ON v571_challengers(arena_id);
        CREATE INDEX IF NOT EXISTS idx_v571_challenger_exp
          ON v571_challengers(experiment_id);

        CREATE TABLE IF NOT EXISTS v571_duels(
          challenger_id TEXT PRIMARY KEY,
          arena_id TEXT NOT NULL,
          experiment_id TEXT NOT NULL,
          evidence_experiment_id TEXT NOT NULL,
          evidence_mode TEXT NOT NULL,
          control_rho REAL,
          challenger_rho REAL,
          delta_rho REAL,
          challenger_n INTEGER NOT NULL,
          challenger_verdict TEXT,
          challenger_comparison TEXT,
          outcome TEXT NOT NULL,
          score REAL NOT NULL,
          metrics_json TEXT NOT NULL,
          updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_v571_duel_arena
          ON v571_duels(arena_id);

        CREATE TABLE IF NOT EXISTS v571_conclusions(
          conclusion_id TEXT PRIMARY KEY,
          arena_id TEXT NOT NULL,
          control_candidate_id TEXT NOT NULL,
          verdict TEXT NOT NULL,
          statement TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          next_action TEXT NOT NULL,
          created_at REAL NOT NULL,
          updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS v571_state(
          key TEXT PRIMARY KEY,
          value_json TEXT NOT NULL,
          updated_at REAL NOT NULL
        );
        """
    )
    d.commit()
    d.close()


def control_shape(spec):
    if "stage" in spec:
        return "SINGLE_STAGE"
    if "stage1" in spec and "stage2" in spec:
        return "SEQUENCE"
    return "UNSUPPORTED"


def controls():
    d = core.open_research()
    names = tables(d)

    if not {"v55_candidates", "v55_beliefs"} <= names:
        d.close()
        return []

    fam_join = (
        "LEFT JOIN v56_lineage_nodes l USING(candidate_id)"
        if "v56_lineage_nodes" in names
        else ""
    )
    fam_sel = (
        "COALESCE(l.family,'UNKNOWN') family"
        if "v56_lineage_nodes" in names
        else "'UNKNOWN' family"
    )

    rows = [
        dict(r)
        for r in d.execute(
            f"""
            SELECT
              c.candidate_id,
              c.kind,
              c.spec_json,
              b.state,
              b.n,
              b.prospective_rho,
              b.lift,
              b.confidence,
              {fam_sel}
            FROM v55_candidates c
            JOIN v55_beliefs b USING(candidate_id)
            {fam_join}
            WHERE b.state IN ('WAITING','WATCH','PASS')
              AND b.n >= ?
            ORDER BY
              CASE b.state
                WHEN 'PASS' THEN 0
                WHEN 'WATCH' THEN 1
                ELSE 2
              END,
              b.confidence DESC,
              b.n DESC
            LIMIT ?
            """,
            (MIN_CONTROL_N, MAX_CONTROLS),
        ).fetchall()
    ]
    d.close()
    return rows


def ensure_arenas():
    rows = controls()
    now = time.time()
    made = 0

    d = core.open_research()
    d.execute("BEGIN IMMEDIATE")
    try:
        for r in rows:
            spec = json.loads(r["spec_json"])
            shape = control_shape(spec)

            # Unsupported controls remain visible in upstream V5.5, but are not
            # admitted to V5.7.1 because we cannot generate valid V4.9 children.
            if shape == "UNSUPPORTED":
                continue

            aid = (
                "A571_"
                + core.fingerprint(
                    {"c": r["candidate_id"]},
                    "v571arena:",
                )[:22]
            )

            before = d.total_changes
            d.execute(
                """
                INSERT INTO v571_arenas(
                  arena_id,
                  control_candidate_id,
                  control_state,
                  control_kind,
                  control_spec_json,
                  family,
                  control_n,
                  control_rho,
                  control_lift,
                  control_confidence,
                  control_shape,
                  state,
                  created_at,
                  updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,?)
                ON CONFLICT(arena_id) DO UPDATE SET
                  control_state=excluded.control_state,
                  control_n=excluded.control_n,
                  control_rho=excluded.control_rho,
                  control_lift=excluded.control_lift,
                  control_confidence=excluded.control_confidence,
                  family=excluded.family,
                  control_shape=excluded.control_shape,
                  updated_at=excluded.updated_at
                """,
                (
                    aid,
                    r["candidate_id"],
                    r["state"],
                    r["kind"],
                    r["spec_json"],
                    r["family"],
                    int(r["n"]),
                    r["prospective_rho"],
                    r["lift"],
                    sf(r["confidence"], 0),
                    shape,
                    now,
                    now,
                ),
            )
            made += int(d.total_changes > before)

        d.commit()
    except BaseException:
        d.rollback()
        raise
    finally:
        d.close()

    return made


def _append_unique(out, kind, spec, label):
    key = core.canonical_json({"kind": kind, "spec": spec})
    if any(x[3] == key for x in out):
        return
    out.append((kind, spec, label, key))


def single_stage_challengers(kind, s):
    required = ("stage", "horizon", "target", "feature")
    if not all(k in s for k in required):
        return []

    out = []
    st = int(s["stage"])
    hz = int(s["horizon"])
    tg = str(s["target"])
    feature = s["feature"]

    # Closest timing neighbors first.
    neighbors = sorted(
        [x for x in v49.STAGES if x != st and abs(x - st) <= 50],
        key=lambda x: abs(x - st),
    )
    for ns in neighbors:
        _append_unique(
            out,
            "TIME_NEIGHBOR",
            dict(s, stage=ns),
            f"stage {st}->{ns}",
        )

    # Horizon transfers.
    for nh in sorted(v49.HORIZONS, key=lambda x: abs(int(x) - hz)):
        nh = int(nh)
        if nh == hz:
            continue
        _append_unique(
            out,
            "HORIZON_TRANSFER",
            dict(s, horizon=nh),
            f"horizon {hz}->{nh}",
        )

    # Sign stress is valid only for single-stage feature tests.
    if kind != "SIGN_FLIP":
        _append_unique(
            out,
            "SIGN_FLIP",
            dict(s),
            "sign flip stress",
        )

    # Target transfers.
    for nt in v49.TARGETS:
        if str(nt) == tg:
            continue
        _append_unique(
            out,
            "TARGET_TRANSFER",
            dict(s, target=str(nt)),
            f"target {tg}->{nt}",
        )

    # Convert a level test into a trajectory test.
    later = [x for x in v49.STAGES if int(x) > st]
    if later:
        ns = int(later[0])
        _append_unique(
            out,
            "SEQUENCE_DELTA",
            {
                "stage1": st,
                "stage2": ns,
                "horizon": hz,
                "target": tg,
                "feature": feature,
            },
            f"level -> trajectory {st}->{ns}",
        )

    return [(k, sp, label) for k, sp, label, _ in out]


def sequence_challengers(s):
    required = ("stage1", "stage2", "horizon", "target", "feature")
    if not all(k in s for k in required):
        return []

    out = []
    st1 = int(s["stage1"])
    st2 = int(s["stage2"])
    hz = int(s["horizon"])
    tg = str(s["target"])
    feature = s["feature"]

    # Mutate sequence start while preserving stage1 < stage2.
    start_candidates = sorted(
        [int(x) for x in v49.STAGES if int(x) < st2 and int(x) != st1],
        key=lambda x: abs(x - st1),
    )
    for ns in start_candidates[:2]:
        spec = dict(
            s,
            stage1=ns,
            stage2=st2,
            horizon=hz,
            target=tg,
            feature=feature,
        )
        _append_unique(
            out,
            "SEQUENCE_DELTA",
            spec,
            f"sequence start {st1}->{ns} / end {st2}",
        )

    # Mutate sequence end while preserving stage1 < stage2.
    end_candidates = sorted(
        [int(x) for x in v49.STAGES if int(x) > st1 and int(x) != st2],
        key=lambda x: abs(x - st2),
    )
    for ns in end_candidates[:2]:
        spec = dict(
            s,
            stage1=st1,
            stage2=ns,
            horizon=hz,
            target=tg,
            feature=feature,
        )
        _append_unique(
            out,
            "SEQUENCE_DELTA",
            spec,
            f"sequence end {st2}->{ns} / start {st1}",
        )

    # Horizon transfer, still evaluated as a valid sequence experiment.
    for nh in sorted(v49.HORIZONS, key=lambda x: abs(int(x) - hz)):
        nh = int(nh)
        if nh == hz:
            continue
        spec = dict(
            s,
            stage1=st1,
            stage2=st2,
            horizon=nh,
            target=tg,
            feature=feature,
        )
        _append_unique(
            out,
            "SEQUENCE_DELTA",
            spec,
            f"sequence horizon {hz}->{nh}",
        )

    # Target transfer, still evaluated as a valid sequence experiment.
    for nt in v49.TARGETS:
        nt = str(nt)
        if nt == tg:
            continue
        spec = dict(
            s,
            stage1=st1,
            stage2=st2,
            horizon=hz,
            target=nt,
            feature=feature,
        )
        _append_unique(
            out,
            "SEQUENCE_DELTA",
            spec,
            f"sequence target {tg}->{nt}",
        )

    return [(k, sp, label) for k, sp, label, _ in out]


def challenger_specs(kind, s):
    shape = control_shape(s)
    if shape == "SINGLE_STAGE":
        return single_stage_challengers(kind, s)
    if shape == "SEQUENCE":
        return sequence_challengers(s)
    return []


def experiment_status(eid):
    d = core.open_research()
    row = d.execute(
        """
        SELECT status
        FROM v49_side_experiments
        WHERE experiment_id=?
        """,
        (eid,),
    ).fetchone()
    d.close()
    return row["status"] if row else None


def seed_challengers():
    d = core.open_research()
    arenas = [
        dict(r)
        for r in d.execute(
            """
            SELECT *
            FROM v571_arenas
            WHERE state='ACTIVE'
            ORDER BY control_confidence DESC
            LIMIT ?
            """,
            (MAX_CONTROLS,),
        ).fetchall()
    ]
    d.close()

    made = 0
    now = time.time()

    for a in arenas:
        s = json.loads(a["control_spec_json"])

        d = core.open_research()
        n_existing = d.execute(
            """
            SELECT COUNT(*)
            FROM v571_challengers
            WHERE arena_id=?
            """,
            (a["arena_id"],),
        ).fetchone()[0]
        d.close()

        if n_existing >= MAX_CHALLENGERS:
            continue

        parent = a["control_candidate_id"]
        feature = (
            s.get("feature")
            or s.get("weak")
            or ((s.get("features") or ["COMPOSITE"])[0])
        )

        for mkind, spec, label in challenger_specs(a["control_kind"], s):
            if n_existing >= MAX_CHALLENGERS:
                break

            stage_for_wm = spec.get("stage", spec.get("stage1"))
            if stage_for_wm is None:
                continue

            hz = int(spec["horizon"])
            tg = str(spec["target"])
            wm = v49.latest_watermark(int(stage_for_wm), hz, tg)
            if not wm:
                continue

            cid = (
                "C571_"
                + core.fingerprint(
                    {
                        "a": a["arena_id"],
                        "k": mkind,
                        "s": spec,
                    },
                    "v571chall:",
                )[:22]
            )

            d = core.open_research()
            exists = d.execute(
                """
                SELECT 1
                FROM v571_challengers
                WHERE challenger_id=?
                """,
                (cid,),
            ).fetchone()
            d.close()

            if exists:
                continue

            # Important: even when insert_exp returns an already-existing V4.9
            # experiment (created=False), V5.7.1 still records the challenger.
            # The diversity representative/result can then be reused.
            eid, created = v49.insert_exp(
                mkind,
                parent,
                feature,
                spec,
                wm,
                "V5.7.1 arena challenger: " + label,
                2,
            )
            if not eid:
                continue

            status = experiment_status(eid)
            state = "SEEDED" if created else "LINKED_EXISTING"

            d = core.open_research()
            d.execute(
                """
                INSERT OR IGNORE INTO v571_challengers(
                  challenger_id,
                  arena_id,
                  experiment_id,
                  mutation_kind,
                  spec_json,
                  mutation_label,
                  control_shape,
                  state,
                  created_at,
                  updated_at
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    cid,
                    a["arena_id"],
                    eid,
                    mkind,
                    core.canonical_json(spec),
                    label,
                    a["control_shape"],
                    state,
                    now,
                    now,
                ),
            )
            d.commit()
            d.close()

            made += 1
            n_existing += 1

    # Preserve V5.6.1 exactly as designed.
    diversity.cycle()
    return made


def side_result(eid):
    d = core.open_research()
    row = d.execute(
        """
        SELECT *
        FROM v49_side_results
        WHERE experiment_id=?
        """,
        (eid,),
    ).fetchone()
    d.close()
    return dict(row) if row else None


def diversity_decision(eid):
    d = core.open_research()
    names = tables(d)
    if "v561_diversity_decisions" not in names:
        d.close()
        return None

    row = d.execute(
        """
        SELECT *
        FROM v561_diversity_decisions
        WHERE experiment_id=?
        """,
        (eid,),
    ).fetchone()
    d.close()
    return dict(row) if row else None


def resolve_evidence_experiment(eid, max_hops=12):
    """Resolve a challenger to a completed V4.9 evidence experiment.

    Returns:
      (evidence_experiment_id, evidence_mode, path)

    evidence_mode:
      DIRECT
      REUSED_DIVERSITY

    If V5.6.1 points to another skipped/parallel experiment, follow the chain.
    A frozen V5.5 candidate id is deliberately not treated as a V4.9 result.
    """
    seen = set()
    current = eid
    path = []

    for _ in range(max_hops):
        if not current or current in seen:
            return None, None, path

        seen.add(current)
        path.append(current)

        if side_result(current) is not None:
            mode = "DIRECT" if current == eid else "REUSED_DIVERSITY"
            return current, mode, path

        dec = diversity_decision(current)
        if not dec:
            return None, None, path

        rep = dec.get("representative_id")
        decision = str(dec.get("decision") or "")

        # BLOCK_FROZEN points to a V5.5 candidate, not necessarily a V4.9
        # experiment. We do not fabricate SIDE evidence from it.
        if decision == "BLOCK_FROZEN":
            return None, None, path

        if not rep:
            return None, None, path

        current = rep

    return None, None, path


def refresh_duels():
    d = core.open_research()
    names = tables(d)
    if "v49_side_results" not in names:
        d.close()
        return 0

    rows = [
        dict(r)
        for r in d.execute(
            """
            SELECT
              ch.*,
              a.control_rho,
              a.control_lift,
              a.control_n
            FROM v571_challengers ch
            JOIN v571_arenas a USING(arena_id)
            WHERE ch.state IN (
              'SEEDED',
              'LINKED_EXISTING',
              'WAITING_EVIDENCE',
              'DONE'
            )
            """
        ).fetchall()
    ]
    d.close()

    now = time.time()
    made = 0

    for r in rows:
        requested_eid = r["experiment_id"]
        evidence_eid, evidence_mode, path = resolve_evidence_experiment(
            requested_eid
        )

        if not evidence_eid:
            d = core.open_research()
            d.execute(
                """
                UPDATE v571_challengers
                SET state='WAITING_EVIDENCE',
                    updated_at=?
                WHERE challenger_id=?
                  AND state!='DONE'
                """,
                (now, r["challenger_id"]),
            )
            d.commit()
            d.close()
            continue

        sr = side_result(evidence_eid)
        if not sr:
            continue

        cr = sf(r["control_rho"], 0)
        rr = sf(sr.get("holdout_rho"), 0)
        delta = rr - cr

        challenger_n = int(sr.get("n") or 0)
        sample = min(1.0, challenger_n / 150.0)
        score = delta * sample

        if delta >= 0.08 and rr >= 0.10:
            outcome = "CHALLENGER_WINS"
        elif delta <= -0.05:
            outcome = "CONTROL_WINS"
        else:
            outcome = "TIE_OR_SPECIALIST"

        side_metrics = {}
        raw_metrics = sr.get("metrics_json")
        if raw_metrics:
            try:
                side_metrics = json.loads(raw_metrics)
            except Exception:
                side_metrics = {}

        metrics = {
            "control_rho": cr,
            "control_lift": r["control_lift"],
            "control_n": r["control_n"],
            "challenger_rho": rr,
            "delta_rho": delta,
            "challenger_n": challenger_n,
            "requested_experiment_id": requested_eid,
            "evidence_experiment_id": evidence_eid,
            "evidence_mode": evidence_mode,
            "evidence_resolution_path": path,
            "side_metrics": side_metrics,
        }

        d = core.open_research()
        existed = d.execute(
            """
            SELECT 1
            FROM v571_duels
            WHERE challenger_id=?
            """,
            (r["challenger_id"],),
        ).fetchone()

        d.execute(
            """
            INSERT INTO v571_duels(
              challenger_id,
              arena_id,
              experiment_id,
              evidence_experiment_id,
              evidence_mode,
              control_rho,
              challenger_rho,
              delta_rho,
              challenger_n,
              challenger_verdict,
              challenger_comparison,
              outcome,
              score,
              metrics_json,
              updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(challenger_id) DO UPDATE SET
              evidence_experiment_id=excluded.evidence_experiment_id,
              evidence_mode=excluded.evidence_mode,
              challenger_rho=excluded.challenger_rho,
              delta_rho=excluded.delta_rho,
              challenger_n=excluded.challenger_n,
              challenger_verdict=excluded.challenger_verdict,
              challenger_comparison=excluded.challenger_comparison,
              outcome=excluded.outcome,
              score=excluded.score,
              metrics_json=excluded.metrics_json,
              updated_at=excluded.updated_at
            """,
            (
                r["challenger_id"],
                r["arena_id"],
                requested_eid,
                evidence_eid,
                evidence_mode,
                cr,
                rr,
                delta,
                challenger_n,
                sr.get("verdict"),
                sr.get("comparison"),
                outcome,
                score,
                core.canonical_json(metrics),
                now,
            ),
        )

        d.execute(
            """
            UPDATE v571_challengers
            SET state='DONE',
                updated_at=?
            WHERE challenger_id=?
            """,
            (now, r["challenger_id"]),
        )
        d.commit()
        d.close()

        if not existed:
            made += 1

    return made


def conclusions():
    d = core.open_research()
    arenas = [
        dict(r)
        for r in d.execute(
            "SELECT * FROM v571_arenas"
        ).fetchall()
    ]
    d.close()

    made = 0
    now = time.time()

    for a in arenas:
        d = core.open_research()
        duels = [
            dict(r)
            for r in d.execute(
                """
                SELECT
                  d.*,
                  c.mutation_label,
                  c.mutation_kind,
                  c.spec_json
                FROM v571_duels d
                JOIN v571_challengers c USING(challenger_id)
                WHERE d.arena_id=?
                ORDER BY d.score DESC
                """,
                (a["arena_id"],),
            ).fetchall()
        ]
        d.close()

        if not duels:
            continue

        wins = [
            x for x in duels
            if x["outcome"] == "CHALLENGER_WINS"
        ]
        losses = [
            x for x in duels
            if x["outcome"] == "CONTROL_WINS"
        ]
        best = duels[0]

        if wins:
            verdict = "EVOLVE"
            next_action = (
                "PROMOTE_BEST_CHALLENGER_TO_NORMAL_SIDE_PIPELINE"
            )
            statement = (
                f"{len(wins)} challenger(s) beat control in SIDE evidence. "
                f"Best: {best['mutation_label']} "
                f"delta_rho={sf(best['delta_rho'], 0):+.3f}. "
                "Parent remains frozen; child still requires "
                "V5.0/V5.5 prospective validation."
            )
        elif losses and len(losses) == len(duels):
            verdict = "CONTROL_DEFENDS"
            next_action = "KEEP_CONTROL_AND_TEST_ORTHOGONAL_MUTATION"
            statement = (
                f"Control defended against all {len(duels)} completed "
                "challengers. Search a more orthogonal mutation rather "
                "than tuning the same neighborhood."
            )
        else:
            verdict = "SPECIALIST_MAP"
            next_action = "TEST_REGIME_SPECIALIZATION"
            statement = (
                "No universal replacement yet. "
                f"Best challenger delta_rho="
                f"{sf(best['delta_rho'], 0):+.3f}; "
                "investigate whether improvement is conditional on "
                "timing/target/regime."
            )

        evidence = {
            "duels": len(duels),
            "challenger_wins": len(wins),
            "control_wins": len(losses),
            "best_challenger": best["challenger_id"],
            "best_delta_rho": best["delta_rho"],
            "best_evidence_mode": best["evidence_mode"],
            "best_evidence_experiment_id": best["evidence_experiment_id"],
        }

        cid = (
            "K571_"
            + core.fingerprint(
                {
                    "a": a["arena_id"],
                    "v": verdict,
                    "best": best["challenger_id"],
                },
                "v571conclusion:",
            )[:22]
        )

        d = core.open_research()
        existed = d.execute(
            """
            SELECT 1
            FROM v571_conclusions
            WHERE conclusion_id=?
            """,
            (cid,),
        ).fetchone()

        d.execute(
            """
            INSERT INTO v571_conclusions(
              conclusion_id,
              arena_id,
              control_candidate_id,
              verdict,
              statement,
              evidence_json,
              next_action,
              created_at,
              updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(conclusion_id) DO UPDATE SET
              statement=excluded.statement,
              evidence_json=excluded.evidence_json,
              next_action=excluded.next_action,
              updated_at=excluded.updated_at
            """,
            (
                cid,
                a["arena_id"],
                a["control_candidate_id"],
                verdict,
                statement,
                core.canonical_json(evidence),
                next_action,
                now,
                now,
            ),
        )
        d.commit()
        d.close()

        if not existed:
            made += 1

    return made


def status_counts():
    d = core.open_research()

    counts = {
        r["state"]: int(r["n"])
        for r in d.execute(
            """
            SELECT state, COUNT(*) n
            FROM v571_challengers
            GROUP BY state
            """
        ).fetchall()
    }

    direct = d.execute(
        """
        SELECT COUNT(*)
        FROM v571_duels
        WHERE evidence_mode='DIRECT'
        """
    ).fetchone()[0]

    reused = d.execute(
        """
        SELECT COUNT(*)
        FROM v571_duels
        WHERE evidence_mode='REUSED_DIVERSITY'
        """
    ).fetchone()[0]

    d.close()
    return counts, int(direct), int(reused)


def display(new_arenas, new_challengers, new_duels, new_conclusions):
    d = core.open_research()

    ac = d.execute(
        "SELECT COUNT(*) FROM v571_arenas"
    ).fetchone()[0]
    cc = d.execute(
        "SELECT COUNT(*) FROM v571_challengers"
    ).fetchone()[0]
    dc = d.execute(
        "SELECT COUNT(*) FROM v571_duels"
    ).fetchone()[0]
    wc = d.execute(
        """
        SELECT COUNT(*)
        FROM v571_duels
        WHERE outcome='CHALLENGER_WINS'
        """
    ).fetchone()[0]

    top = [
        dict(r)
        for r in d.execute(
            """
            SELECT
              a.control_state,
              a.family,
              a.control_candidate_id,
              d.outcome,
              d.delta_rho,
              d.challenger_rho,
              d.evidence_mode,
              c.mutation_label
            FROM v571_duels d
            JOIN v571_arenas a USING(arena_id)
            JOIN v571_challengers c USING(challenger_id)
            ORDER BY d.score DESC
            LIMIT 10
            """
        ).fetchall()
    ]

    cons = [
        dict(r)
        for r in d.execute(
            """
            SELECT *
            FROM v571_conclusions
            ORDER BY updated_at DESC
            LIMIT 6
            """
        ).fetchall()
    ]

    shapes = [
        dict(r)
        for r in d.execute(
            """
            SELECT control_shape, COUNT(*) n
            FROM v571_arenas
            GROUP BY control_shape
            ORDER BY n DESC
            """
        ).fetchall()
    ]

    d.close()

    sc, direct, reused = status_counts()

    print("\033[2J\033[H", end="")
    print("=" * 148)
    print("MEMECOIN LAB — CANDIDATE & CHAMPION ARENA V5.7.1")
    print("=" * 148)
    print(
        f"ARENAS={ac} NEW={new_arenas} | "
        f"CHALLENGERS={cc} NEW={new_challengers} | "
        f"DUELS={dc} NEW={new_duels} | "
        f"CHALLENGER_WINS={wc} | "
        f"NEW_CONCLUSIONS={new_conclusions}"
    )

    shape_txt = " | ".join(
        f"{r['control_shape']}={r['n']}"
        for r in shapes
    ) or "none"

    print(
        f"CONTROL SHAPES: {shape_txt} | "
        f"EVIDENCE: DIRECT={direct} REUSED_DIVERSITY={reused}"
    )

    print(
        "CHALLENGER STATES: "
        + " | ".join(
            f"{k}={v}"
            for k, v in sorted(sc.items())
        )
    )

    print("\nHEAD-TO-HEAD LEADERBOARD")
    if not top:
        print(
            "No completed duel yet — waiting for direct or reusable "
            "V4.9 SIDE evidence."
        )

    for r in top:
        print(
            f"{r['outcome']:<20} "
            f"{r['control_state']:<7} "
            f"{r['family']:<22} "
            f"delta_rho={sf(r['delta_rho'],0):+.3f} "
            f"challenger_rho={sf(r['challenger_rho'],0):+.3f} "
            f"evidence={r['evidence_mode']:<17} "
            f"{r['mutation_label'][:48]}"
        )

    print("\nWHAT THE LAB LEARNED")
    if not cons:
        print("Waiting for completed head-to-heads.")

    for r in cons:
        print(
            f"{r['verdict']:<16} "
            f"{r['statement'][:116]}"
        )

    print(
        "\nGuardrail: CONTROL is immutable. Reused diversity evidence "
        "only avoids duplicate SIDE computation; no challenger becomes "
        "frozen/champion without the normal future-only pipeline."
    )


def cycle():
    a = ensure_arenas()
    c = seed_challengers()
    d = refresh_duels()
    k = conclusions()

    display(a, c, d, k)

    state = {
        "arenas_new": a,
        "challengers_new": c,
        "duels_new": d,
        "conclusions_new": k,
    }

    db = core.open_research()
    db.execute(
        """
        INSERT INTO v571_state(key,value_json,updated_at)
        VALUES('latest',?,?)
        ON CONFLICT(key) DO UPDATE SET
          value_json=excluded.value_json,
          updated_at=excluded.updated_at
        """,
        (core.canonical_json(state), time.time()),
    )
    db.commit()
    db.close()


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    init()

    while not STOP:
        try:
            cycle()
        except Exception as e:
            print("V5.7.1 error:", repr(e), flush=True)

        time.sleep(LOOP)


if __name__ == "__main__":
    main()
