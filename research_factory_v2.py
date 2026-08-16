#!/usr/bin/env python3

import hashlib
import json
import math
import os
import signal
import subprocess
import sys
import time

from pathlib import Path

import research_client as rc


ROOT = Path.home() / "memecoin_lab"

PYTHON = sys.executable

WORKER_SCRIPT = (
    ROOT
    / "research_worker_v2.py"
)

REFRESH = 2

CPU_COUNT = (
    os.cpu_count()
    or 4
)

MAX_WORKERS = int(
    os.environ.get(
        "MEMECOIN_RESEARCH_WORKERS",
        min(
            8,
            max(
                4,
                CPU_COUNT // 2
            )
        )
    )
)

INITIAL_POPULATION_TARGET = 140

MAX_HYPOTHESES = 300

MAX_CHILDREN_PER_RESULT = 2

QUEUE_LOW_WATERMARK = 30

shutdown_requested = False

workers = {}


# ============================================================
# SPECIES / FAMILIES
# ============================================================

FAMILIES = {

    "PRICE": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",
    ],

    "ACTIVITY": [
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
    ],

    "FLOW": [
        "buy_sol",
        "sell_sol",
        "net_sol",
    ],

    "PRICE_ACTIVITY": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
    ],

    "PRICE_FLOW": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",
        "buy_sol",
        "sell_sol",
        "net_sol",
    ],

    "ACTIVITY_FLOW": [
        "swaps",
        "buys",
        "sells",
        "buy_ratio",
        "buy_sol",
        "sell_sol",
        "net_sol",
    ],

    "ALL": [
        "return_since_entry",
        "mfe_so_far",
        "mae_so_far",
        "new_low",
        "reclaim_entry",

        "swaps",
        "buys",
        "sells",
        "buy_ratio",

        "buy_sol",
        "sell_sol",
        "net_sol",
    ],
}


SPECIES = {
    "PRICE":
        "PRICE_SCOUT",

    "ACTIVITY":
        "ACTIVITY_SCOUT",

    "FLOW":
        "FLOW_SCOUT",

    "PRICE_ACTIVITY":
        "HYBRID_SCOUT",

    "PRICE_FLOW":
        "HYBRID_SCOUT",

    "ACTIVITY_FLOW":
        "HYBRID_SCOUT",

    "ALL":
        "GENERALIST",
}


STAGES = [
    5,
    10,
    20,
    30,
    60,
]


TARGETS = [
    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
]


# ============================================================
# SCHEMA
# ============================================================

def initialize_tables():

    rc.execute("""
    CREATE TABLE IF NOT EXISTS hypothesis_specs (

        hypothesis_id TEXT PRIMARY KEY,

        spec_hash TEXT NOT NULL UNIQUE,

        spec_json TEXT NOT NULL,

        created_at REAL NOT NULL
    )
    """)


    rc.execute("""
    CREATE TABLE IF NOT EXISTS factory_processed_results (

        experiment_id TEXT PRIMARY KEY,

        processed_at REAL NOT NULL,

        children_created INTEGER NOT NULL DEFAULT 0
    )
    """)


    rc.execute("""
    CREATE TABLE IF NOT EXISTS factory_state (

        key TEXT PRIMARY KEY,

        value TEXT NOT NULL,

        updated_at REAL NOT NULL
    )
    """)


initialize_tables()


# ============================================================
# IDS
# ============================================================

def canonical(
    spec
):

    return json.dumps(
        spec,
        sort_keys=True,
        separators=(
            ",",
            ":"
        )
    )


def digest(
    spec
):

    return hashlib.sha256(
        canonical(
            spec
        ).encode()
    ).hexdigest()


def ids_for(
    spec
):

    h = digest(
        spec
    )

    return (
        "H_" + h[:16],
        "E_" + h[:16],
        "J_" + h[:16],
        h,
    )


# ============================================================
# COUNTS
# ============================================================

def counts():

    db = rc.readonly()


    row = db.execute("""
    SELECT

        (
            SELECT COUNT(*)
            FROM hypotheses
        ) AS hypotheses,

        (
            SELECT COUNT(*)
            FROM hypotheses
            WHERE status='QUEUED'
        ) AS hypothesis_queue,

        (
            SELECT COUNT(*)
            FROM experiments
        ) AS experiments,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE status='DISCOVERY'
        ) AS discovery_running,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE status='DISCOVERY_PASSED'
        ) AS discovery_passed,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE status='REJECTED_DISCOVERY'
        ) AS rejected,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE status='COLLECT_MORE'
        ) AS collect_more,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE status='QUEUED'
        ) AS jobs_queued,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE status='RUNNING'
        ) AS jobs_running,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE status='DONE'
        ) AS jobs_done,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE status='FAILED'
        ) AS jobs_failed

    """).fetchone()


    output = dict(row)

    db.close()

    return output


# ============================================================
# NOVELTY GATE
# ============================================================

def spec_exists(
    spec
):

    h = digest(
        spec
    )


    db = rc.readonly()


    row = db.execute("""
    SELECT 1
    FROM hypothesis_specs
    WHERE spec_hash=?
    """, (
        h,
    )).fetchone()


    db.close()


    return (
        row is not None
    )


# ============================================================
# CREATE HYPOTHESIS
# ============================================================

def create_hypothesis(
    spec,
    family_name,
    parent=None,
    rationale=None,
    priority=1.0,
):

    if spec_exists(
        spec
    ):

        return False


    (
        hypothesis_id,
        experiment_id,
        job_id,
        spec_hash,
    ) = ids_for(
        spec
    )


    species = SPECIES.get(
        family_name,
        "AUTONOMOUS_DERIVATION"
    )


    statement = (
        f"{family_name} information observed "
        f"at {spec['stage_s']}s after strict entry "
        f"predicts {spec['target']}."
    )


    now = time.time()


    rc.execute("""
    INSERT INTO hypotheses (

        hypothesis_id,
        created_at,
        parent_hypothesis_id,

        branch,
        species,

        statement,
        rationale,

        novelty_score,
        information_gain_score,

        priority,

        status,

        updated_at
    )

    VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?
    )
    """, (

        hypothesis_id,

        now,

        parent,

        "POST_ENTRY_REVERSAL",

        species,

        statement,

        rationale or (
            "Autonomous initial population."
        ),

        1.0,

        1.0,

        priority,

        "QUEUED",

        now,
    ))


    rc.execute("""
    INSERT INTO hypothesis_specs (

        hypothesis_id,
        spec_hash,
        spec_json,
        created_at
    )

    VALUES (
        ?,?,?,?
    )
    """, (

        hypothesis_id,

        spec_hash,

        canonical(
            spec
        ),

        now,
    ))


    rc.execute("""
    INSERT INTO experiments (

        experiment_id,

        hypothesis_id,

        created_at,

        branch,
        stage,
        status,

        spec_json,

        updated_at
    )

    VALUES (
        ?,?,?,?,?,?,?,?
    )
    """, (

        experiment_id,

        hypothesis_id,

        now,

        "POST_ENTRY_REVERSAL",

        "DISCOVERY",

        "QUEUED",

        canonical(
            spec
        ),

        now,
    ))


    rc.execute("""
    INSERT INTO jobs (

        job_id,

        experiment_id,

        job_type,

        priority,

        status,

        created_at,

        max_attempts,

        payload_json,

        updated_at
    )

    VALUES (
        ?,?,?,?,?,?,?,?,?
    )
    """, (

        job_id,

        experiment_id,

        "DISCOVERY_TEST",

        priority,

        "QUEUED",

        now,

        3,

        canonical(
            spec
        ),

        now,
    ))


    rc.execute("""
    UPDATE hypotheses

    SET
        status='EXPERIMENT_CREATED',
        updated_at=?

    WHERE hypothesis_id=?
    """, (
        now,
        hypothesis_id,
    ))


    return True


# ============================================================
# INITIAL POPULATION
# ============================================================

def generate_initial_population():

    created = 0


    for stage in STAGES:

        for target in TARGETS:

            for (
                family_name,
                features
            ) in FAMILIES.items():


                spec = {
                    "template":
                        "POST_ENTRY_STAGE_SCORE",

                    "stage_s":
                        stage,

                    "target":
                        target,

                    "family":
                        family_name,

                    "features":
                        features,

                    "generation":
                        0,
                }


                priority = 1.0


                if target == "future_hit20":

                    priority += 0.30


                if stage in (
                    10,
                    20
                ):

                    priority += 0.20


                if family_name in (
                    "PRICE_ACTIVITY",
                    "ACTIVITY_FLOW",
                    "ALL"
                ):

                    priority += 0.10


                if create_hypothesis(
                    spec,
                    family_name,
                    priority=priority,
                ):

                    created += 1


    if created:

        rc.event(
            "INITIAL_SWARM_CREATED",
            "RESEARCH_FACTORY",
            {
                "created":
                    created
            }
        )


    return created


# ============================================================
# JOB SCHEDULER
# ============================================================

def queued_jobs(
    limit
):

    db = rc.readonly()


    rows = db.execute("""
    SELECT *

    FROM jobs

    WHERE status='QUEUED'

    ORDER BY
        priority DESC,
        created_at ASC

    LIMIT ?
    """, (
        limit,
    )).fetchall()


    output = [
        dict(r)
        for r in rows
    ]


    db.close()

    return output


def launch_worker(
    job
):

    job_id = job[
        "job_id"
    ]


    if job_id in workers:
        return


    log_dir = (
        ROOT
        / "autonomous_lab_v2_logs"
        / "workers"
    )

    log_dir.mkdir(
        parents=True,
        exist_ok=True
    )


    logfile = open(
        log_dir
        / (
            job_id
            + ".log"
        ),
        "a",
        buffering=1,
    )


    proc = subprocess.Popen(
        [
            PYTHON,
            str(
                WORKER_SCRIPT
            ),
            job_id,
        ],

        cwd=str(ROOT),

        stdout=logfile,
        stderr=subprocess.STDOUT,

        start_new_session=True,
    )


    workers[
        job_id
    ] = {
        "proc":
            proc,

        "log":
            logfile,

        "started":
            time.time(),
    }


def reap_workers():

    finished = []


    for (
        job_id,
        info
    ) in list(
        workers.items()
    ):

        code = info[
            "proc"
        ].poll()


        if code is None:
            continue


        try:

            info[
                "log"
            ].close()

        except Exception:
            pass


        finished.append(
            (
                job_id,
                code,
            )
        )


        workers.pop(
            job_id,
            None
        )


    return finished


def schedule():

    free = (
        MAX_WORKERS
        - len(workers)
    )


    if free <= 0:
        return


    jobs = queued_jobs(
        free
    )


    for job in jobs:

        # Reserve before process starts.
        rc.execute("""
        UPDATE jobs

        SET
            status='DISPATCHED',
            worker_id=?,
            updated_at=?

        WHERE
            job_id=?
            AND status='QUEUED'
        """, (

            "LOCAL_SWARM",

            time.time(),

            job[
                "job_id"
            ],
        ))


        launch_worker(
            job
        )


# ============================================================
# DISPATCHED RECOVERY
# ============================================================

def recover_stale_dispatched():

    # If factory was killed after reservation but before
    # the worker changed state to RUNNING.
    rc.execute("""
    UPDATE jobs

    SET
        status='QUEUED',
        worker_id=NULL,
        updated_at=?

    WHERE
        status='DISPATCHED'
        AND updated_at < ?
    """, (

        time.time(),

        time.time() - 60,
    ))


# ============================================================
# RESULT → NEW HYPOTHESES
# ============================================================

def completed_unprocessed():

    db = rc.readonly()


    rows = db.execute("""
    SELECT
        e.experiment_id,
        e.hypothesis_id,
        e.spec_json,
        e.status,

        j.result_json

    FROM experiments e

    JOIN jobs j
      ON j.experiment_id=e.experiment_id

    LEFT JOIN factory_processed_results p
      ON p.experiment_id=e.experiment_id

    WHERE
        j.status='DONE'
        AND p.experiment_id IS NULL

    LIMIT 50
    """).fetchall()


    output = [
        dict(r)
        for r in rows
    ]


    db.close()

    return output


def strongest_features(
    result,
    n=3
):

    feature_rhos = result.get(
        "feature_rhos",
        {}
    )


    ranked = sorted(
        (
            (
                feature,
                rho
            )

            for (
                feature,
                rho
            ) in feature_rhos.items()

            if rho is not None
        ),

        key=lambda x:
            abs(
                x[1]
            ),

        reverse=True,
    )


    return [
        x[0]

        for x in ranked[
            :n
        ]
    ]


def create_children():

    current_count = counts()[
        "hypotheses"
    ]


    if current_count >= MAX_HYPOTHESES:
        return


    rows = completed_unprocessed()


    for row in rows:

        result = json.loads(
            row[
                "result_json"
            ]
            or "{}"
        )


        old_spec = json.loads(
            row[
                "spec_json"
            ]
        )


        decision = result.get(
            "decision"
        )


        children = 0


        # --------------------------------------------
        # Failure-driven child:
        # keep only strongest features.
        # --------------------------------------------

        if decision in (
            "REJECT_DISCOVERY",
            "PASS_DISCOVERY",
        ):

            best = strongest_features(
                result,
                3
            )


            if (
                len(best) >= 2
                and current_count
                < MAX_HYPOTHESES
            ):

                child = dict(
                    old_spec
                )

                child[
                    "features"
                ] = best

                child[
                    "family"
                ] = (
                    "AUTO_COMPACT"
                )

                child[
                    "generation"
                ] = (
                    int(
                        old_spec.get(
                            "generation",
                            0
                        )
                    )
                    + 1
                )

                child[
                    "parent_experiment"
                ] = row[
                    "experiment_id"
                ]


                if create_hypothesis(
                    child,

                    "AUTO_COMPACT",

                    parent=row[
                        "hypothesis_id"
                    ],

                    rationale=(
                        "Generated from prior result: "
                        "retain strongest empirical features "
                        "while reducing complexity."
                    ),

                    priority=1.15,
                ):

                    children += 1
                    current_count += 1


        # --------------------------------------------
        # Contradiction / timing child:
        # move observation horizon around the parent.
        # --------------------------------------------

        if (
            children
            < MAX_CHILDREN_PER_RESULT
            and current_count
            < MAX_HYPOTHESES
        ):

            stage = int(
                old_spec[
                    "stage_s"
                ]
            )


            candidates = [
                s
                for s in STAGES
                if s != stage
            ]


            if candidates:

                nearest = min(
                    candidates,
                    key=lambda s:
                        abs(
                            s - stage
                        )
                )


                child = dict(
                    old_spec
                )

                child[
                    "stage_s"
                ] = nearest

                child[
                    "generation"
                ] = (
                    int(
                        old_spec.get(
                            "generation",
                            0
                        )
                    )
                    + 1
                )

                child[
                    "parent_experiment"
                ] = row[
                    "experiment_id"
                ]


                if create_hypothesis(
                    child,

                    old_spec.get(
                        "family",
                        "AUTO"
                    ),

                    parent=row[
                        "hypothesis_id"
                    ],

                    rationale=(
                        "Timing perturbation generated "
                        "from completed parent experiment."
                    ),

                    priority=0.90,
                ):

                    children += 1
                    current_count += 1


        rc.execute("""
        INSERT INTO factory_processed_results (

            experiment_id,
            processed_at,
            children_created
        )

        VALUES (
            ?,?,?
        )
        """, (

            row[
                "experiment_id"
            ],

            time.time(),

            children,
        ))


# ============================================================
# SCIENTIFIC MEMORY
# ============================================================

def update_memory():

    db = rc.readonly()


    rows = db.execute("""
    SELECT
        e.experiment_id,
        e.hypothesis_id,
        e.status,
        e.spec_json,

        j.result_json

    FROM experiments e

    JOIN jobs j
      ON j.experiment_id=e.experiment_id

    WHERE
        j.status='DONE'
        AND e.status IN (
            'DISCOVERY_PASSED',
            'REJECTED_DISCOVERY'
        )

    ORDER BY j.finished_at DESC

    LIMIT 20
    """).fetchall()


    db.close()


    for row in rows:

        memory_id = (
            "MEM_"
            + row[
                "experiment_id"
            ]
        )


        result = json.loads(
            row[
                "result_json"
            ]
            or "{}"
        )


        lesson = (
            f"Discovery {row['status']}; "
            f"rho={result.get('target_rho')}; "
            f"Q4-Q1={result.get('q4_minus_q1_pp')}pp."
        )


        rc.execute("""
        INSERT OR IGNORE INTO research_memory (

            memory_id,

            created_at,

            memory_type,

            branch,

            subject,

            lesson,

            evidence_json,

            retest_policy,

            confidence,

            updated_at
        )

        VALUES (
            ?,?,?,?,?,?,?,?,?,?
        )
        """, (

            memory_id,

            time.time(),

            (
                "DISCOVERY_PASS"
                if row[
                    "status"
                ] == "DISCOVERY_PASSED"
                else "DISCOVERY_FAIL"
            ),

            "POST_ENTRY_REVERSAL",

            row[
                "experiment_id"
            ],

            lesson,

            json.dumps(
                result,
                separators=(
                    ",",
                    ":"
                )
            ),

            (
                "Requires independent prospective "
                "validation before execution."
            ),

            0.60,

            time.time(),
        ))


# ============================================================
# THROUGHPUT
# ============================================================

def throughput():

    db = rc.readonly()


    now = time.time()


    hour = db.execute("""
    SELECT COUNT(*)

    FROM jobs

    WHERE
        status='DONE'
        AND finished_at >= ?
    """, (
        now - 3600,
    )).fetchone()[0]


    day = db.execute("""
    SELECT COUNT(*)

    FROM jobs

    WHERE
        status='DONE'
        AND finished_at >= ?
    """, (
        now - 86400,
    )).fetchone()[0]


    generated_day = db.execute("""
    SELECT COUNT(*)

    FROM hypotheses

    WHERE created_at >= ?
    """, (
        now - 86400,
    )).fetchone()[0]


    db.close()


    return {
        "exp_hour":
            hour,

        "exp_day":
            day,

        "hyp_day":
            generated_day,
    }


# ============================================================
# DASHBOARD
# ============================================================

def show():

    os.system(
        "clear"
    )


    c = counts()

    t = throughput()


    print("=" * 180)

    print(
        "MEMECOIN LAB — AUTONOMOUS RESEARCH SWARM — DOSE 2"
    )

    print("=" * 180)


    print(
        f"WORKER CAPACITY       : "
        f"{len(workers)}/{MAX_WORKERS}"
    )


    print(
        f"HYPOTHESES TOTAL      : "
        f"{c['hypotheses']}"
    )


    print(
        f"HYPOTHESIS QUEUE      : "
        f"{c['hypothesis_queue']}"
    )


    print(
        f"EXPERIMENTS           : "
        f"{c['experiments']}"
    )


    print(
        f"JOBS QUEUED           : "
        f"{c['jobs_queued']}"
    )


    print(
        f"JOBS RUNNING          : "
        f"{c['jobs_running']}"
    )


    print(
        f"JOBS DONE             : "
        f"{c['jobs_done']}"
    )


    print(
        f"JOBS FAILED           : "
        f"{c['jobs_failed']}"
    )


    print()

    print(
        f"DISCOVERY PASS        : "
        f"{c['discovery_passed']}"
    )


    print(
        f"DISCOVERY REJECT      : "
        f"{c['rejected']}"
    )


    print(
        f"COLLECT MORE          : "
        f"{c['collect_more']}"
    )


    print()
    print("=" * 180)
    print("RESEARCH THROUGHPUT")
    print("=" * 180)


    print(
        f"EXPERIMENTS / HOUR    : "
        f"{t['exp_hour']}"
    )


    print(
        f"EXPERIMENTS / 24H     : "
        f"{t['exp_day']}"
    )


    print(
        f"HYPOTHESES / 24H      : "
        f"{t['hyp_day']}"
    )


    print()
    print("=" * 180)
    print("ACTIVE WORKERS")
    print("=" * 180)


    for (
        job_id,
        info
    ) in workers.items():

        age = (
            time.time()
            - info[
                "started"
            ]
        )


        print(
            f"🟢 {job_id:<20}"
            f" | PID="
            f"{info['proc'].pid:<7}"
            f" | AGE={age:6.1f}s"
        )


    print()
    print("=" * 180)
    print("LATEST RESULTS")
    print("=" * 180)


    db = rc.readonly()


    rows = db.execute("""
    SELECT
        e.experiment_id,
        e.status,
        e.discovery_n,
        e.discovery_score,
        e.spec_json

    FROM experiments e

    WHERE e.status IN (
        'DISCOVERY_PASSED',
        'REJECTED_DISCOVERY',
        'COLLECT_MORE'
    )

    ORDER BY e.updated_at DESC

    LIMIT 15
    """).fetchall()


    db.close()


    for row in rows:

        spec = json.loads(
            row[
                "spec_json"
            ]
        )


        rho = row[
            "discovery_score"
        ]


        rho_text = (
            "NA"
            if rho is None
            else f"{rho:.3f}"
        )


        print(
            f"{row['experiment_id']:<19}"
            f" | T={spec['stage_s']:>2}s"
            f" | {spec['family']:<16}"
            f" | {spec['target']:<12}"
            f" | N={str(row['discovery_n'] or 0):>3}"
            f" | RHO={rho_text:>6}"
            f" | {row['status']}"
        )


    print()
    print("=" * 180)

    print(
        "MODE: DISCOVERY ONLY — "
        "NO STRATEGY MAY TRADE OR MODIFY ITS OWN HOLDOUT"
    )

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops research swarm"
    )


# ============================================================
# SIGNAL
# ============================================================

def stop(
    signum,
    frame
):

    global shutdown_requested

    shutdown_requested = True


signal.signal(
    signal.SIGINT,
    stop
)

signal.signal(
    signal.SIGTERM,
    stop
)


# ============================================================
# MAIN
# ============================================================

def main():

    recover_stale_dispatched()


    c = counts()


    if (
        c[
            "hypotheses"
        ]
        < INITIAL_POPULATION_TARGET
    ):

        generate_initial_population()


    while not shutdown_requested:

        reap_workers()

        recover_stale_dispatched()

        create_children()

        update_memory()

        schedule()

        show()

        time.sleep(
            REFRESH
        )


    print()

    print(
        "Stopping research swarm..."
    )


    for (
        job_id,
        info
    ) in list(
        workers.items()
    ):

        try:

            os.killpg(
                os.getpgid(
                    info[
                        "proc"
                    ].pid
                ),
                signal.SIGINT
            )

        except Exception:
            pass


    print(
        "✅ Research swarm stopped."
    )


if __name__ == "__main__":

    main()
