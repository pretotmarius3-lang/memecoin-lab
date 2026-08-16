#!/usr/bin/env python3

import collections
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time

from pathlib import Path

import research_client as rc


ROOT = Path.home() / "memecoin_lab"

MARKET_DB = ROOT / "validation_v090.db"

LOG_DIR = ROOT / "autonomous_lab_v2_logs"

LOG_DIR.mkdir(exist_ok=True)

PYTHON = sys.executable

REFRESH = 5

CRASH_LIMIT = 5
CRASH_WINDOW_S = 120

QUARANTINE_S = 600

STARTUP_GRACE_S = 120


PROCESS_SPECS = {

    "T101B_MIGRATION": {
        "script":
            "t101b_migration_recorder.py",

        "kind":
            "SOURCE",

        "data_probe":
            "migration",

        "stale_after_s":
            900,
    },


    "T116E_RAW": {
        "script":
            "t116e_adaptive_pump_collector.py",

        "kind":
            "SOURCE",

        "data_probe":
            "pump",

        "stale_after_s":
            300,
    },


    "T116C_CLEAN": {
        "script":
            "t116c_pump_price_lifecycle.py",

        "kind":
            "DERIVED",
    },


    "T116D_EVENTS": {
        "script":
            "t116d_premigration_pump_dump_recorder.py",

        "kind":
            "DERIVED",
    },


    "T117_OUTCOMES": {
        "script":
            "t117_premigration_outcome_linker.py",

        "kind":
            "DERIVED",
    },


    "RESEARCH_FACTORY": {
        "script":
            "research_factory_v3.py",

        "kind":
            "RESEARCH",
    },


    "RESOURCE_GOVERNOR": {
        "script":
            "resource_governor_v3.py",

        "kind":
            "RESEARCH",
    },


    "ROBUSTNESS_ARENA": {
        "script":
            "robustness_arena_v3.py",

        "kind":
            "RESEARCH",
    },
}


children = {}
logs = {}
start_times = {}

crash_history = {
    name: collections.deque()
    for name in PROCESS_SPECS
}

quarantine_until = {
    name: 0.0
    for name in PROCESS_SPECS
}

stall_last_restart = {
    name: 0.0
    for name in PROCESS_SPECS
}

shutdown_requested = False

writer_proc = None
writer_log = None


def market():

    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=10,
    )

    db.row_factory = sqlite3.Row

    db.execute(
        "PRAGMA busy_timeout=10000"
    )

    return db


def start_writer():

    global writer_proc
    global writer_log

    # ---------------------------------------------------------
    # SINGLE-WRITER GUARANTEE
    #
    # If a healthy writer is already serving the canonical
    # socket, reuse it. Never unlink/replace a live writer.
    # ---------------------------------------------------------

    try:

        response = rc.ping()

        if response.get("ok"):

            print(
                "Research writer already alive — reusing existing writer."
            )

            writer_proc = None
            writer_log = None

            return

    except Exception:
        pass


    # Remove only a stale socket left by a dead writer.
    socket_path = ROOT / ".research_writer.sock"

    if socket_path.exists():

        try:
            socket_path.unlink()
        except Exception:
            pass


    logfile = (
        LOG_DIR
        / "research_writer.log"
    )

    writer_log = open(
        logfile,
        "a",
        buffering=1
    )


    writer_proc = subprocess.Popen(
        [
            PYTHON,
            str(
                ROOT
                / "research_writer.py"
            )
        ],

        cwd=str(ROOT),

        stdout=writer_log,
        stderr=subprocess.STDOUT,

        start_new_session=True,
    )


    deadline = (
        time.time()
        + 15
    )


    while time.time() < deadline:

        if writer_proc.poll() is not None:

            raise RuntimeError(
                "research_writer crashed during startup"
            )


        try:

            response = rc.ping()

            if response.get("ok"):

                print(
                    f"Research writer ready | PID={writer_proc.pid}"
                )

                return

        except Exception:
            pass


        time.sleep(0.2)


    raise RuntimeError(
        "research_writer did not become ready"
    )


def process_update(
    name,
    state,
    pid=None,
    start_inc=0,
    crash_inc=0,
    stall_inc=0,
    exit_code=None,
):

    now = time.time()

    rc.execute("""
    INSERT INTO process_state (
        component,
        script,
        pid,
        state,

        starts,
        crashes,
        stalls,

        last_start,
        last_stop,
        last_crash,
        last_stall,

        last_exit_code,

        quarantined_until,

        updated_at
    )
    VALUES (
        ?,?,?,?,
        ?,?,?,
        ?,?,?,?,
        ?,?,
        ?
    )

    ON CONFLICT(component)
    DO UPDATE SET

        script=
            excluded.script,

        pid=
            excluded.pid,

        state=
            excluded.state,

        starts=
            process_state.starts
            + excluded.starts,

        crashes=
            process_state.crashes
            + excluded.crashes,

        stalls=
            process_state.stalls
            + excluded.stalls,

        last_start=
            CASE
                WHEN excluded.last_start IS NOT NULL
                THEN excluded.last_start
                ELSE process_state.last_start
            END,

        last_stop=
            CASE
                WHEN excluded.last_stop IS NOT NULL
                THEN excluded.last_stop
                ELSE process_state.last_stop
            END,

        last_crash=
            CASE
                WHEN excluded.last_crash IS NOT NULL
                THEN excluded.last_crash
                ELSE process_state.last_crash
            END,

        last_stall=
            CASE
                WHEN excluded.last_stall IS NOT NULL
                THEN excluded.last_stall
                ELSE process_state.last_stall
            END,

        last_exit_code=
            excluded.last_exit_code,

        quarantined_until=
            excluded.quarantined_until,

        updated_at=
            excluded.updated_at
    """, (
        name,

        PROCESS_SPECS[
            name
        ][
            "script"
        ],

        pid,
        state,

        start_inc,
        crash_inc,
        stall_inc,

        (
            now
            if start_inc
            else None
        ),

        (
            now
            if state != "RUNNING"
            else None
        ),

        (
            now
            if crash_inc
            else None
        ),

        (
            now
            if stall_inc
            else None
        ),

        exit_code,

        (
            quarantine_until[name]
            if quarantine_until[name] > now
            else None
        ),

        now,
    ))


def start_child(name):

    now = time.time()

    if (
        quarantine_until[name]
        > now
    ):
        return


    script = (
        ROOT
        / PROCESS_SPECS[
            name
        ][
            "script"
        ]
    )


    if not script.exists():

        process_update(
            name,
            "MISSING"
        )

        rc.event(
            "PROCESS_MISSING",
            "SUPERVISOR",
            {
                "component": name,
                "script": str(script),
            },
            severity="ERROR"
        )

        return


    fh = open(
        LOG_DIR
        / (
            name.lower()
            + ".log"
        ),
        "a",
        buffering=1,
    )


    logs[name] = fh


    proc = subprocess.Popen(
        [
            PYTHON,
            str(script)
        ],
        cwd=str(ROOT),
        stdout=fh,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


    children[name] = proc

    start_times[name] = now


    process_update(
        name,
        "RUNNING",
        pid=proc.pid,
        start_inc=1,
    )


    rc.event(
        "PROCESS_STARTED",
        "SUPERVISOR",
        {
            "component": name,
            "pid": proc.pid,
        }
    )


def stop_child(
    name,
    reason
):

    proc = children.get(name)

    if (
        not proc
        or proc.poll()
        is not None
    ):
        return


    try:

        os.killpg(
            os.getpgid(
                proc.pid
            ),
            signal.SIGINT
        )

    except Exception:
        pass


    try:

        proc.wait(
            timeout=5
        )

    except subprocess.TimeoutExpired:

        try:

            os.killpg(
                os.getpgid(
                    proc.pid
                ),
                signal.SIGTERM
            )

        except Exception:
            pass


    rc.event(
        "PROCESS_STOP",
        "SUPERVISOR",
        {
            "component": name,
            "reason": reason,
        }
    )


def crash_child(
    name,
    code
):

    now = time.time()

    hist = crash_history[name]

    hist.append(now)


    while (
        hist
        and now - hist[0]
        > CRASH_WINDOW_S
    ):

        hist.popleft()


    if len(hist) >= CRASH_LIMIT:

        quarantine_until[name] = (
            now
            + QUARANTINE_S
        )

        state = "QUARANTINED"


    else:

        state = "CRASHED"


    process_update(
        name,
        state,
        crash_inc=1,
        exit_code=code,
    )


    rc.event(
        "PROCESS_CRASH",
        "SUPERVISOR",
        {
            "component": name,
            "exit_code": code,
            "crashes_window": len(hist),
            "state": state,
        },
        severity="ERROR"
    )


def ensure_children():

    now = time.time()


    for name in PROCESS_SPECS:

        if quarantine_until[name] > now:
            continue


        proc = children.get(name)


        if proc is None:

            start_child(name)

            continue


        code = proc.poll()


        if code is None:

            continue


        children.pop(
            name,
            None
        )


        fh = logs.pop(
            name,
            None
        )


        if fh:

            try:
                fh.close()
            except Exception:
                pass


        if code == 0:

            process_update(
                name,
                "STOPPED",
                exit_code=code,
            )

        else:

            crash_child(
                name,
                code
            )


        if quarantine_until[name] <= now:

            time.sleep(2)

            start_child(name)


def health():

    now = time.time()

    result = {
        "migration_age": None,
        "migration_n": 0,

        "pump_age": None,
        "pump_n": 0,

        "pump_events": 0,
        "dump_events": 0,
    }


    try:

        db = market()

    except Exception:

        return result


    try:

        r = db.execute("""
        SELECT
            COUNT(*) AS n,
            MAX(
                COALESCE(
                    block_time,
                    detected_at
                )
            ) AS newest
        FROM t101_migrations
        WHERE status='OK'
        """).fetchone()


        result[
            "migration_n"
        ] = r["n"] or 0


        if r["newest"] is not None:

            result[
                "migration_age"
            ] = (
                now
                - r["newest"]
            )

    except Exception:
        pass


    try:

        r = db.execute("""
        SELECT
            COUNT(*) AS n,
            MAX(timestamp) AS newest
        FROM t116_pump_swaps
        """).fetchone()


        result[
            "pump_n"
        ] = r["n"] or 0


        if r["newest"] is not None:

            result[
                "pump_age"
            ] = (
                now
                - r["newest"]
            )

    except Exception:
        pass


    try:

        result[
            "pump_events"
        ] = db.execute("""
        SELECT COUNT(*)
        FROM t116_pump_events
        """).fetchone()[0]

    except Exception:
        pass


    try:

        result[
            "dump_events"
        ] = db.execute("""
        SELECT COUNT(*)
        FROM t116_premigration_dump_events
        """).fetchone()[0]

    except Exception:
        pass


    db.close()

    return result


def detect_stalls(h):

    now = time.time()


    mapping = {
        "migration":
            h[
                "migration_age"
            ],

        "pump":
            h[
                "pump_age"
            ],
    }


    for name, cfg in (
        PROCESS_SPECS.items()
    ):

        if cfg[
            "kind"
        ] != "SOURCE":
            continue


        proc = children.get(name)


        if (
            not proc
            or proc.poll()
            is not None
        ):
            continue


        if (
            now
            - start_times.get(
                name,
                now
            )
            < STARTUP_GRACE_S
        ):

            continue


        age = mapping.get(
            cfg[
                "data_probe"
            ]
        )


        if age is None:
            continue


        if age <= cfg[
            "stale_after_s"
        ]:

            continue


        if (
            now
            - stall_last_restart[name]
            < 300
        ):

            continue


        stall_last_restart[name] = now


        process_update(
            name,
            "STALLED",
            pid=proc.pid,
            stall_inc=1,
        )


        rc.event(
            "DATA_STALL",
            "SUPERVISOR",
            {
                "component": name,
                "data_age_s": age,
            },
            severity="WARNING"
        )


        stop_child(
            name,
            "DATA_STALL"
        )


def research_counts():

    try:

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
            ) AS queued_hypotheses,

            (
                SELECT COUNT(*)
                FROM experiments
            ) AS experiments,

            (
                SELECT COUNT(*)
                FROM jobs
                WHERE status='QUEUED'
            ) AS queued_jobs,

            (
                SELECT COUNT(*)
                FROM jobs
                WHERE status='RUNNING'
            ) AS running_jobs,

            (
                SELECT COUNT(*)
                FROM research_memory
            ) AS memories
        """).fetchone()


        out = dict(row)

        db.close()

        return out


    except Exception:

        return {
            "hypotheses": 0,
            "queued_hypotheses": 0,
            "experiments": 0,
            "queued_jobs": 0,
            "running_jobs": 0,
            "memories": 0,
        }


def show(h):

    os.system("clear")


    counts = research_counts()


    print("=" * 165)

    print(
        "MEMECOIN LAB — AUTONOMOUS RESEARCH ORGANISM — FOUNDATION V2"
    )

    print("=" * 165)

    print(
        time.strftime(
            "LOCAL TIME : %Y-%m-%d %H:%M:%S"
        )
    )

    print()

    print(
        "MARKET DATA DB : validation_v090.db"
    )

    print(
        "RESEARCH DB    : research_lab.db"
    )

    print(
        "RESEARCH WRITE : SINGLE CENTRAL WRITER"
    )

    print(
        "LIVE MONEY     : DISABLED"
    )

    print(
        "SHADOW/PAPER   : SCHEMA READY"
    )


    print()
    print("=" * 165)
    print("PROCESS SUPERVISOR V2")
    print("=" * 165)


    try:

        db = rc.readonly()

        rows = db.execute("""
        SELECT *
        FROM process_state
        ORDER BY component
        """).fetchall()

        db.close()

    except Exception:

        rows = []


    now = time.time()


    for r in rows:

        state = r["state"]


        if state == "RUNNING":

            icon = "🟢"

        elif state in (
            "CRASHED",
            "QUARANTINED",
            "MISSING"
        ):

            icon = "🔴"

        else:

            icon = "🟡"


        quarantine = ""


        if (
            r[
                "quarantined_until"
            ]
            and r[
                "quarantined_until"
            ] > now
        ):

            quarantine = (
                f" | QUAR="
                f"{r['quarantined_until'] - now:.0f}s"
            )


        print(
            f"{icon} "
            f"{r['component']:<22}"
            f"| {state:<12}"
            f"| PID={str(r['pid'] or '-'):>7} "
            f"| START={r['starts']:>3} "
            f"| CRASH={r['crashes']:>3} "
            f"| STALL={r['stalls']:>3}"
            f"{quarantine}"
        )


    print()
    print("=" * 165)
    print("DATA HEALTH")
    print("=" * 165)


    def age_text(x):

        return (
            "NA"
            if x is None
            else f"{x:.0f}s"
        )


    mig_icon = (
        "🟢"
        if (
            h["migration_age"] is not None
            and h["migration_age"] <= 900
        )
        else "🔴"
    )


    pump_icon = (
        "🟢"
        if (
            h["pump_age"] is not None
            and h["pump_age"] <= 300
        )
        else "🔴"
    )


    print(
        f"{mig_icon} MIGRATIONS "
        f"| N={h['migration_n']} "
        f"| AGE={age_text(h['migration_age'])}"
    )


    print(
        f"{pump_icon} RAW PUMP "
        f"| SWAPS={h['pump_n']:,} "
        f"| AGE={age_text(h['pump_age'])}"
    )


    print(
        f"🟢 EVENTS "
        f"| PUMP={h['pump_events']:,} "
        f"| DUMP={h['dump_events']:,}"
    )


    print()
    print("=" * 165)
    print("RESEARCH FACTORY")
    print("=" * 165)


    print(
        f"HYPOTHESES TOTAL : "
        f"{counts['hypotheses']}"
    )

    print(
        f"HYPOTHESIS QUEUE : "
        f"{counts['queued_hypotheses']}"
    )

    print(
        f"EXPERIMENTS      : "
        f"{counts['experiments']}"
    )

    print(
        f"JOBS QUEUED      : "
        f"{counts['queued_jobs']}"
    )

    print(
        f"JOBS RUNNING     : "
        f"{counts['running_jobs']}"
    )

    print(
        f"SCIENTIFIC MEMORY: "
        f"{counts['memories']}"
    )


    print()
    print("=" * 165)
    print("NEXT")
    print("=" * 165)

    print(
        "DOSE 2 → HYPOTHESIS SWARM + PARALLEL RESEARCH WORKERS"
    )

    print(
        "Target initial population: 50–100 hypotheses"
    )

    print(
        "Target worker pool: adaptive / CPU-aware"
    )

    print(
        "Branches: rebound, continuation, migration, success, "
        "death, wallets, sequences, regimes"
    )


    print()
    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C stops complete V2 foundation"
    )


def request_shutdown(
    signum,
    frame
):

    global shutdown_requested

    shutdown_requested = True


signal.signal(
    signal.SIGINT,
    request_shutdown
)

signal.signal(
    signal.SIGTERM,
    request_shutdown
)


def main():

    global shutdown_requested


    start_writer()


    rc.event(
        "LAB_V2_START",
        "SUPERVISOR",
        {
            "version":
                "FOUNDATION_V2"
        }
    )


    for name in PROCESS_SPECS:

        start_child(name)

        time.sleep(0.5)


    while not shutdown_requested:

        ensure_children()

        h = health()

        detect_stalls(h)

        show(h)


        for _ in range(REFRESH):

            if shutdown_requested:
                break

            time.sleep(1)


    print()

    print(
        "Stopping Foundation V2..."
    )


    for name in list(
        children.keys()
    ):

        stop_child(
            name,
            "LAB_SHUTDOWN"
        )


    if writer_proc:

        try:

            os.killpg(
                os.getpgid(
                    writer_proc.pid
                ),
                signal.SIGINT
            )

        except Exception:
            pass


        try:

            writer_proc.wait(
                timeout=5
            )

        except Exception:
            pass


    for fh in logs.values():

        try:
            fh.close()
        except Exception:
            pass


    if writer_log:

        try:
            writer_log.close()
        except Exception:
            pass


    print(
        "✅ Foundation V2 stopped safely."
    )


if __name__ == "__main__":

    main()
