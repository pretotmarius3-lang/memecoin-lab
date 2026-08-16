#!/usr/bin/env python3

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import research_client as rc

ROOT = Path.home() / "memecoin_lab"

PYTHON = sys.executable

WORKER = ROOT / "research_worker_v3.py"
DIRECTOR = ROOT / "research_director_v3.py"

MAX_WORKERS = int(
    os.environ.get(
        "MEMECOIN_RESEARCH_WORKERS",
        min(8,max(4,(os.cpu_count() or 4)//2))
    )
)

REFRESH = 2

shutdown = False
workers = {}

director_proc = None
director_log = None


def start_director():

    global director_proc, director_log

    logs = ROOT / "autonomous_lab_v2_logs"
    logs.mkdir(exist_ok=True)

    director_log = open(
        logs / "research_director_v3.log",
        "a",
        buffering=1,
    )

    director_proc = subprocess.Popen(
        [PYTHON,str(DIRECTOR)],
        cwd=str(ROOT),
        stdout=director_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )



def recommended_workers():

    try:

        db = rc.readonly()

        row = db.execute("""
        SELECT value
        FROM factory_state
        WHERE key='recommended_workers'
        """).fetchone()

        db.close()

        if row:

            return max(
                1,
                int(
                    row["value"]
                )
            )

    except Exception:
        pass

    return MAX_WORKERS


def queue():

    db = rc.readonly()

    limit = max(
        0,
        recommended_workers() - len(workers)
    )

    if limit <= 0:
        db.close()
        return []

    rows = db.execute("""
    SELECT *
    FROM jobs
    WHERE
        status='QUEUED'
        AND job_type='DISCOVERY_V3'
    ORDER BY
        priority DESC,
        created_at ASC
    LIMIT ?
    """,(limit,)).fetchall()

    out = [dict(r) for r in rows]

    db.close()

    return out


def launch(job):

    jid = job["job_id"]

    if jid in workers:
        return

    rc.execute("""
    UPDATE jobs
    SET
        status='DISPATCHED',
        worker_id='V3_LOCAL',
        updated_at=?
    WHERE
        job_id=?
        AND status='QUEUED'
    """,(time.time(),jid))

    logdir = (
        ROOT
        / "autonomous_lab_v2_logs"
        / "workers_v3"
    )

    logdir.mkdir(
        parents=True,
        exist_ok=True
    )

    fh = open(
        logdir / f"{jid}.log",
        "a",
        buffering=1,
    )

    proc = subprocess.Popen(
        [PYTHON,str(WORKER),jid],
        cwd=str(ROOT),
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    workers[jid] = {
        "proc":proc,
        "log":fh,
        "started":time.time(),
    }


def reap():

    for jid, info in list(workers.items()):

        code = info["proc"].poll()

        if code is None:
            continue

        try:
            info["log"].close()
        except Exception:
            pass

        workers.pop(jid,None)


def recover():

    rc.execute("""
    UPDATE jobs
    SET
        status='QUEUED',
        worker_id=NULL,
        updated_at=?
    WHERE
        status='DISPATCHED'
        AND job_type='DISCOVERY_V3'
        AND updated_at < ?
    """,(time.time(),time.time()-60))


def counts():

    db = rc.readonly()

    row = db.execute("""
    SELECT

        (
            SELECT COUNT(*)
            FROM hypotheses
            WHERE hypothesis_id LIKE 'HV3_%'
        ) AS hyp,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE experiment_id LIKE 'EV3_%'
        ) AS exp,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE
                job_type='DISCOVERY_V3'
                AND status='QUEUED'
        ) AS queued,

        (
            SELECT COUNT(*)
            FROM jobs
            WHERE
                job_type='DISCOVERY_V3'
                AND status='DONE'
        ) AS done,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE
                experiment_id LIKE 'EV3_%'
                AND status='ROBUSTNESS_QUEUED'
        ) AS robustness,

        (
            SELECT COUNT(*)
            FROM experiments
            WHERE
                experiment_id LIKE 'EV3_%'
                AND status='REJECTED_DISCOVERY'
        ) AS rejected
    """).fetchone()

    branches = db.execute("""
    SELECT *
    FROM branch_state
    ORDER BY branch
    """).fetchall()

    db.close()

    return dict(row), [dict(r) for r in branches]


def throughput():

    db = rc.readonly()

    now = time.time()

    h = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE
        job_type='DISCOVERY_V3'
        AND status='DONE'
        AND finished_at >= ?
    """,(now-3600,)).fetchone()[0]

    d = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE
        job_type='DISCOVERY_V3'
        AND status='DONE'
        AND finished_at >= ?
    """,(now-86400,)).fetchone()[0]

    db.close()

    return h,d


def show():

    os.system("clear")

    c, branches = counts()
    hour, day = throughput()

    print("="*170)
    print("MEMECOIN LAB — RESEARCH DIRECTOR V3 — PERPETUAL RESEARCH")
    print("="*170)

    print(
        f"WORKERS              : {len(workers)}/{MAX_WORKERS}"
    )

    print(
        f"V3 HYPOTHESES        : {c['hyp']}"
    )

    print(
        f"V3 EXPERIMENTS       : {c['exp']}"
    )

    print(
        f"QUEUE                 : {c['queued']}"
    )

    print(
        f"DONE                  : {c['done']}"
    )

    print(
        f"ROBUSTNESS QUEUED     : {c['robustness']}"
    )

    print(
        f"REJECTED              : {c['rejected']}"
    )

    print()

    print(
        f"EXPERIMENTS / HOUR    : {hour}"
    )

    print(
        f"EXPERIMENTS / 24H     : {day}"
    )

    print()

    print("="*170)
    print("BRANCH ALLOCATION")
    print("="*170)

    for b in branches:

        print(
            f"{b['branch']:<18}"
            f" | BUDGET={100*b['budget']:5.1f}%"
            f" | GEN={b['generated']:5d}"
            f" | PASS={b['passed']:4d}"
            f" | REJECT={b['rejected']:5d}"
            f" | COLLECT={b['collect_more']:4d}"
            f" | SAT={100*b['saturation']:5.1f}%"
        )

    print()

    print("="*170)
    print("ACTIVE WORKERS")
    print("="*170)

    for jid, info in workers.items():

        print(
            f"🟢 {jid:<23}"
            f" PID={info['proc'].pid:<7}"
            f" AGE={time.time()-info['started']:6.1f}s"
        )

    print()

    print("="*170)
    print("MODE")
    print("="*170)

    print(
        "PERPETUAL DISCOVERY / NO GLOBAL HYPOTHESIS LIMIT"
    )

    print(
        "DISCOVERY PASS → ROBUSTNESS QUEUE"
    )

    print(
        "LIVE MONEY DISABLED"
    )

    print(
        "HOLDOUT SELF-MUTATION FORBIDDEN"
    )

    print()

    print(
        f"Refresh every {REFRESH}s"
    )


def stop(sig, frame):

    global shutdown
    shutdown = True


signal.signal(signal.SIGINT,stop)
signal.signal(signal.SIGTERM,stop)


def main():

    start_director()

    recover()

    while not shutdown:

        reap()

        recover()

        for job in queue():
            launch(job)

        show()

        time.sleep(REFRESH)

    for jid,info in list(workers.items()):

        try:
            os.killpg(
                os.getpgid(info["proc"].pid),
                signal.SIGINT
            )
        except Exception:
            pass

    if director_proc:

        try:
            os.killpg(
                os.getpgid(director_proc.pid),
                signal.SIGINT
            )
        except Exception:
            pass

    print("V3 stopped.")


if __name__ == "__main__":
    main()
