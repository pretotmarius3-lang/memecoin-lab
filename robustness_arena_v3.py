#!/usr/bin/env python3

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import research_client as rc


ROOT = Path.home() / "memecoin_lab"

PYTHON = sys.executable

WORKER = (
    ROOT
    / "robustness_worker_v3.py"
)

MAX_ROBUSTNESS_WORKERS = 3

workers = {}
shutdown = False


def ensure_schema():

    rc.execute("""
    CREATE TABLE IF NOT EXISTS robustness_results_v3 (
        experiment_id TEXT PRIMARY KEY,
        branch TEXT,
        decision TEXT NOT NULL,
        result_json TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """)


def queued():

    db = rc.readonly()

    rows = db.execute("""
    SELECT experiment_id
    FROM experiments
    WHERE
        status='ROBUSTNESS_QUEUED'
        AND experiment_id LIKE 'EV3_%'
    ORDER BY
        updated_at ASC
    LIMIT ?
    """,(
        MAX_ROBUSTNESS_WORKERS-len(workers),
    )).fetchall()

    out = [
        r["experiment_id"]
        for r in rows
    ]

    db.close()

    return out


def launch(exp_id):

    if exp_id in workers:
        return

    rc.execute("""
    UPDATE experiments
    SET
        status='ROBUSTNESS_RUNNING',
        updated_at=?
    WHERE
        experiment_id=?
        AND status='ROBUSTNESS_QUEUED'
    """,(
        time.time(),
        exp_id,
    ))

    logdir = (
        ROOT
        / "autonomous_lab_v2_logs"
        / "robustness_v3"
    )

    logdir.mkdir(
        parents=True,
        exist_ok=True
    )

    fh = open(
        logdir / f"{exp_id}.log",
        "a",
        buffering=1,
    )

    proc = subprocess.Popen(
        [
            PYTHON,
            str(WORKER),
            exp_id,
        ],
        cwd=str(ROOT),
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    workers[exp_id] = {
        "proc":proc,
        "log":fh,
    }


def reap():

    for exp_id,info in list(
        workers.items()
    ):

        code = info[
            "proc"
        ].poll()

        if code is None:
            continue

        try:
            info["log"].close()
        except Exception:
            pass

        if code != 0:

            rc.execute("""
            UPDATE experiments
            SET
                status='ROBUSTNESS_ERROR',
                updated_at=?
            WHERE
                experiment_id=?
                AND status='ROBUSTNESS_RUNNING'
            """,(
                time.time(),
                exp_id,
            ))

        workers.pop(
            exp_id,
            None
        )


def stop(sig,frame):

    global shutdown

    shutdown = True


signal.signal(
    signal.SIGINT,
    stop
)

signal.signal(
    signal.SIGTERM,
    stop
)


def main():

    ensure_schema()

    while not shutdown:

        reap()

        for exp_id in queued():
            launch(exp_id)

        time.sleep(1)

    for info in workers.values():

        try:

            os.killpg(
                os.getpgid(
                    info["proc"].pid
                ),
                signal.SIGINT
            )

        except Exception:
            pass


if __name__ == "__main__":
    main()
