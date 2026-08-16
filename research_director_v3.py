#!/usr/bin/env python3

import hashlib
import json
import random
import time

import research_client as rc


ACTIVE_POPULATION_TARGET = 200
QUEUE_TARGET = 100

QUEUE_LOW = 60
QUEUE_HIGH = 150
QUEUE_EMERGENCY = 250

BRANCH_BUDGETS = {
    "RESURRECTION":0.40,
    "MIGRATION":0.45,
    "WILD":0.15,
}

RES_STAGES = [5,10,15,20,30,45,60,90]

RES_TARGETS = [
    "future_hit10",
    "future_hit20",
    "future_hit30",
    "future_hit50",
]

RES_FAMILIES = [
    "price",
    "activity",
    "flow",
    "price_activity",
    "all",
]

MIG_STAGES = [
    15,
    30,
    45,
    60,
    90,
    120,
    180,
    300,
]

MIG_HORIZONS = [
    300,
    600,
    900,
    1800,
]


def canonical(x):
    return json.dumps(
        x,
        sort_keys=True,
        separators=(",",":")
    )


def scientific_spec(spec):

    ignored = {
        "mutation",
        "parent_hypothesis",
        "parent_experiment",
        "statement",
        "rationale",
        "species",
        "generation",
    }

    return {
        k: v
        for k, v in spec.items()
        if k not in ignored
    }


def spec_hash(spec):

    return hashlib.sha256(
        canonical(
            scientific_spec(spec)
        ).encode()
    ).hexdigest()


def ensure_schema():

    rc.execute("""
    CREATE TABLE IF NOT EXISTS director_specs (
        spec_hash TEXT PRIMARY KEY,
        spec_json TEXT NOT NULL,
        branch TEXT NOT NULL,
        created_at REAL NOT NULL
    )
    """)

    rc.execute("""
    CREATE TABLE IF NOT EXISTS branch_state (
        branch TEXT PRIMARY KEY,
        budget REAL NOT NULL,
        generated INTEGER NOT NULL DEFAULT 0,
        passed INTEGER NOT NULL DEFAULT 0,
        rejected INTEGER NOT NULL DEFAULT 0,
        collect_more INTEGER NOT NULL DEFAULT 0,
        saturation REAL NOT NULL DEFAULT 0,
        updated_at REAL NOT NULL
    )
    """)

    rc.execute("""
    CREATE TABLE IF NOT EXISTS director_processed (
        experiment_id TEXT PRIMARY KEY,
        processed_at REAL NOT NULL
    )
    """)


def exists(spec):

    h = spec_hash(spec)

    db = rc.readonly()

    row = db.execute("""
    SELECT 1
    FROM director_specs
    WHERE spec_hash=?
    """,(h,)).fetchone()

    db.close()

    return row is not None


def create(spec, priority=1.0, rationale="autonomous"):

    if exists(spec):
        return False

    h = spec_hash(spec)

    hypothesis_id = "HV3_" + h[:18]
    experiment_id = "EV3_" + h[:18]
    job_id = "JV3_" + h[:18]

    now = time.time()

    branch = spec["branch"]

    rc.execute("""
    INSERT INTO director_specs
    VALUES (?,?,?,?)
    """,(h,canonical(spec),branch,now))

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
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """,(
        hypothesis_id,
        now,
        spec.get("parent_hypothesis"),
        branch,
        spec.get("species","DIRECTOR_V3"),
        spec.get("statement",canonical(spec)),
        rationale,
        1.0,
        1.0,
        priority,
        "EXPERIMENT_CREATED",
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
    VALUES (?,?,?,?,?,?,?,?)
    """,(
        experiment_id,
        hypothesis_id,
        now,
        branch,
        "DISCOVERY",
        "QUEUED",
        canonical(spec),
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
    VALUES (?,?,?,?,?,?,?,?,?)
    """,(
        job_id,
        experiment_id,
        "DISCOVERY_V3",
        priority,
        "QUEUED",
        now,
        3,
        canonical(spec),
        now,
    ))

    return True


def random_resurrection():

    stage = random.choice(RES_STAGES)
    target = random.choice(RES_TARGETS)
    family = random.choice(RES_FAMILIES)

    return {
        "version":3,
        "branch":"RESURRECTION",
        "species":"REVERSAL_SCIENTIST",
        "stage_s":stage,
        "target":target,
        "family":family,
        "statement":(
            f"{family} at {stage}s predicts "
            f"{target} under strict executable entry."
        ),
    }


def random_migration():

    stage = random.choice(MIG_STAGES)
    horizon = random.choice(MIG_HORIZONS)

    return {
        "version":3,
        "branch":"MIGRATION",
        "species":"MIGRATION_SCIENTIST",
        "stage_s":stage,
        "horizon_s":horizon,
        "target":"MIGRATION_AFTER_STAGE",
        "family":"PRE_MIGRATION_FLOW_PATH",
        "statement":(
            f"Strict information available by token age "
            f"{stage}s predicts migration during next "
            f"{horizon}s."
        ),
    }


def active_counts():

    db = rc.readonly()

    rows = db.execute("""
    SELECT
        branch,
        COUNT(*) AS n
    FROM experiments
    WHERE status IN (
        'QUEUED',
        'DISCOVERY',
        'COLLECT_MORE',
        'DISCOVERY_PASSED',
        'ROBUSTNESS_QUEUED',
        'HOLDOUT'
    )
    GROUP BY branch
    """).fetchall()

    total = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE status='QUEUED'
    """).fetchone()[0]

    db.close()

    return {
        r["branch"]:r["n"]
        for r in rows
    }, total


def refill():

    active, queued = active_counts()

    # Hard backpressure.
    # Existing research must be evaluated before generating more.
    if queued >= QUEUE_HIGH:
        return

    # Between target and high watermark we deliberately do nothing.
    if queued >= QUEUE_TARGET:
        return

    attempts = 0
    created = 0

    while (
        queued < QUEUE_TARGET
        and attempts < 500
    ):

        attempts += 1

        branch = random.choices(
            [
                "RESURRECTION",
                "MIGRATION",
            ],
            weights=[
                BRANCH_BUDGETS["RESURRECTION"],
                BRANCH_BUDGETS["MIGRATION"],
            ],
            k=1,
        )[0]

        if branch == "RESURRECTION":

            spec = random_resurrection()

        else:

            spec = random_migration()

        priority = (
            1.15
            if branch == "MIGRATION"
            else 1.0
        )

        if create(
            spec,
            priority=priority,
            rationale="V3 controlled perpetual refill",
        ):

            queued += 1
            created += 1

    if created:

        rc.event(
            "DIRECTOR_REFILL",
            "RESEARCH_DIRECTOR_V3",
            {
                "created": created,
                "queue_after": queued,
            }
        )


def process_results():

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

    LEFT JOIN director_processed p
      ON p.experiment_id=e.experiment_id

    WHERE
        j.status='DONE'
        AND p.experiment_id IS NULL
        AND e.experiment_id LIKE 'EV3_%'

    LIMIT 100
    """).fetchall()

    db.close()

    for row in rows:

        result = json.loads(
            row["result_json"] or "{}"
        )

        spec = json.loads(
            row["spec_json"]
        )

        decision = result.get("decision")

        # PASS → robustness queue marker.
        if decision == "PASS_DISCOVERY":

            rc.execute("""
            UPDATE experiments
            SET
                status='ROBUSTNESS_QUEUED',
                robustness_score=NULL,
                updated_at=?
            WHERE experiment_id=?
            """,(time.time(),row["experiment_id"]))

            # Local mutation around successful stage.
            if spec["branch"] == "MIGRATION":

                for delta in (-15,15):

                    child = dict(spec)

                    child["stage_s"] = max(
                        10,
                        int(spec["stage_s"]) + delta
                    )

                    child["parent_hypothesis"] = row["hypothesis_id"]

                    create(
                        child,
                        priority=1.35,
                        rationale="Local mutation from migration discovery pass",
                    )

            elif spec["branch"] == "RESURRECTION":

                for delta in (-5,5):

                    child = dict(spec)

                    child["stage_s"] = max(
                        5,
                        int(spec["stage_s"]) + delta
                    )

                    child["parent_hypothesis"] = row["hypothesis_id"]

                    create(
                        child,
                        priority=1.30,
                        rationale="Local timing mutation from discovery pass",
                    )


        # FAILURE → one deliberate contradiction.
        elif decision == "REJECT_DISCOVERY":

            if random.random() < 0.20:

                child = dict(spec)
                child["parent_hypothesis"] = row["hypothesis_id"]

                if spec["branch"] == "RESURRECTION":

                    child["family"] = random.choice(
                        RES_FAMILIES
                    )

                elif spec["branch"] == "MIGRATION":

                    child["horizon_s"] = random.choice(
                        MIG_HORIZONS
                    )

                create(
                    child,
                    priority=0.65,
                    rationale="Contradiction probe from rejected discovery",
                )


        rc.execute("""
        INSERT OR IGNORE INTO director_processed
        VALUES (?,?)
        """,(row["experiment_id"],time.time()))


def update_branch_state():

    db = rc.readonly()

    for branch, budget in BRANCH_BUDGETS.items():

        generated = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE branch=?
        """,(branch,)).fetchone()[0]

        passed = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE
            branch=?
            AND status IN (
                'DISCOVERY_PASSED',
                'ROBUSTNESS_QUEUED',
                'FROZEN',
                'HOLDOUT'
            )
        """,(branch,)).fetchone()[0]

        rejected = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE
            branch=?
            AND status='REJECTED_DISCOVERY'
        """,(branch,)).fetchone()[0]

        collect = db.execute("""
        SELECT COUNT(*)
        FROM experiments
        WHERE
            branch=?
            AND status='COLLECT_MORE'
        """,(branch,)).fetchone()[0]

        saturation = (
            rejected/generated
            if generated
            else 0.0
        )

        rc.execute("""
        INSERT INTO branch_state (
            branch,
            budget,
            generated,
            passed,
            rejected,
            collect_more,
            saturation,
            updated_at
        )
        VALUES (?,?,?,?,?,?,?,?)

        ON CONFLICT(branch)
        DO UPDATE SET
            budget=excluded.budget,
            generated=excluded.generated,
            passed=excluded.passed,
            rejected=excluded.rejected,
            collect_more=excluded.collect_more,
            saturation=excluded.saturation,
            updated_at=excluded.updated_at
        """,(
            branch,
            budget,
            generated,
            passed,
            rejected,
            collect,
            saturation,
            time.time(),
        ))

    db.close()


def sync_hypothesis_states():

    # Hypothesis state should represent its experiment lifecycle,
    # not remain permanently EXPERIMENT_CREATED.
    rc.execute("""
    UPDATE hypotheses

    SET
        status = COALESCE(
            (
                SELECT e.status
                FROM experiments e
                WHERE
                    e.hypothesis_id = hypotheses.hypothesis_id
                ORDER BY e.updated_at DESC
                LIMIT 1
            ),
            hypotheses.status
        ),

        updated_at = ?

    WHERE hypothesis_id LIKE 'HV3_%'
    """, (
        time.time(),
    ))


def main():

    ensure_schema()

    while True:

        process_results()

        update_branch_state()

        sync_hypothesis_states()

        refill()

        time.sleep(3)


if __name__ == "__main__":
    main()
