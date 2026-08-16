#!/usr/bin/env python3

import os
import sys
import time
import signal
import sqlite3
import subprocess
from pathlib import Path

# ============================================================
# PATHS
# ============================================================

ROOT = Path.home() / "memecoin_lab"
DB_PATH = ROOT / "validation_v090.db"
LOG_DIR = ROOT / "autonomous_lab_logs"

LOG_DIR.mkdir(exist_ok=True)

PYTHON = sys.executable

REFRESH = 5
RESTART_DELAY = 5


# ============================================================
# MANAGED LIVE PIPELINE
# ============================================================

PROCESSES = {
    "T101B_MIGRATION": {
        "script": "t101b_migration_recorder.py",
        "critical": True,
    },

    "T116E_RAW": {
        "script": "t116e_adaptive_pump_collector.py",
        "critical": True,
    },

    "T116C_CLEAN": {
        "script": "t116c_pump_price_lifecycle.py",
        "critical": True,
    },

    "T116D_EVENTS": {
        "script": "t116d_premigration_pump_dump_recorder.py",
        "critical": True,
    },

    "T117_OUTCOMES": {
        "script": "t117_premigration_outcome_linker.py",
        "critical": True,
    },

    "EXP0121_FEATURES": {
        "script": "exp0121_post_entry_feature_engine.py",
        "critical": False,
    },

    "RESEARCH_EVALUATOR": {
        "script": "autonomous_research_evaluator.py",
        "critical": False,
    },
}


# ============================================================
# RUNTIME
# ============================================================

children = {}
log_handles = {}
shutdown_requested = False


# ============================================================
# DATABASE
# ============================================================

db = sqlite3.connect(
    str(DB_PATH),
    timeout=30,
    isolation_level=None
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA busy_timeout=30000")
db.execute("PRAGMA wal_autocheckpoint=1000")


def db_write(sql, args=(), attempts=30):

    last_error = None

    for attempt in range(attempts):

        try:

            return db.execute(
                sql,
                args
            )

        except sqlite3.OperationalError as e:

            last_error = e

            msg = str(e).lower()

            if (
                "database is locked" not in msg
                and "database is busy" not in msg
            ):
                raise

            time.sleep(
                min(
                    0.05 * (attempt + 1),
                    1.0
                )
            )

    raise last_error


def init_lab_db():

    db.execute("""
    CREATE TABLE IF NOT EXISTS lab_processes (

        process_name TEXT PRIMARY KEY,

        script_name TEXT NOT NULL,

        pid INTEGER,

        status TEXT NOT NULL,

        starts INTEGER NOT NULL DEFAULT 0,
        crashes INTEGER NOT NULL DEFAULT 0,

        started_at REAL,
        stopped_at REAL,

        last_exit_code INTEGER,

        last_update_at REAL NOT NULL
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS lab_experiments (

        experiment_id TEXT PRIMARY KEY,

        parent_experiment TEXT,

        created_at REAL NOT NULL,

        status TEXT NOT NULL,

        research_branch TEXT NOT NULL,

        hypothesis TEXT NOT NULL,

        unit TEXT,

        target TEXT,

        entry_definition TEXT,

        feature_family TEXT,

        feature_spec TEXT,

        stage_spec TEXT,

        horizon_spec TEXT,

        discovery_start REAL,
        discovery_end REAL,

        frozen_at REAL,

        holdout_start REAL,

        discovery_n INTEGER,
        holdout_n INTEGER,

        conclusion TEXT,

        rejection_reason TEXT,

        last_update_at REAL NOT NULL
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS lab_decisions (

        id INTEGER PRIMARY KEY AUTOINCREMENT,

        timestamp REAL NOT NULL,

        experiment_id TEXT,

        decision TEXT NOT NULL,

        reason TEXT,

        automatic INTEGER NOT NULL DEFAULT 1
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS lab_meta (

        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    db.commit()


init_lab_db()


# ============================================================
# GENERIC DB HELPERS
# ============================================================

def table_exists(name):

    return db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE type='table'
      AND name=?
    """, (
        name,
    )).fetchone() is not None


def scalar(sql, args=()):

    try:

        r = db.execute(
            sql,
            args
        ).fetchone()

        if not r:
            return None

        return r[0]

    except sqlite3.Error:
        return None


def set_meta(key, value):

    db.execute("""
    INSERT INTO lab_meta (
        key,
        value
    )

    VALUES (?,?)

    ON CONFLICT(key)
    DO UPDATE SET
        value=excluded.value
    """, (
        key,
        str(value)
    ))

    db.commit()


def get_meta(key):

    r = db.execute("""
    SELECT value
    FROM lab_meta
    WHERE key=?
    """, (
        key,
    )).fetchone()

    return (
        r["value"]
        if r
        else None
    )


# ============================================================
# PRESERVE OUR RESEARCH HISTORY
# ============================================================

def register_historical_research():

    # T119/T120 remains in the scientific record.
    # We DO NOT delete or overwrite it.

    exists = db.execute("""
    SELECT 1
    FROM lab_experiments
    WHERE experiment_id='LEGACY_T119_T120'
    """).fetchone()

    if exists:
        return

    now = time.time()

    db.execute("""
    INSERT INTO lab_experiments (

        experiment_id,
        parent_experiment,

        created_at,
        status,

        research_branch,

        hypothesis,

        unit,

        target,

        entry_definition,

        feature_family,

        feature_spec,

        stage_spec,

        horizon_spec,

        conclusion,

        rejection_reason,

        last_update_at
    )

    VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
    )
    """, (

        "LEGACY_T119_T120",

        None,

        now,

        "REJECTED_STRICT",

        "PRE_MIGRATION_RESURRECTION",

        (
            "Pre-dump structure/activity/buy family "
            "predicts +20% resurrection"
        ),

        "UNIQUE_TOKEN",

        "+20% within 300s",

        "STRICT_ACTUAL_ENTRY",

        "STRUCTURE+ACTIVITY+BUY",

        (
            "run_from_first_pct,"
            "drawdown_pct,"
            "swaps_30s,"
            "swaps_60s,"
            "buys_30s,"
            "buys_60s,"
            "buys_total"
        ),

        "FIRST_DUMP",

        "300s",

        (
            "Historical relationship existed, but frozen "
            "prospective score failed strict executable-entry "
            "validation."
        ),

        (
            "T120B/T120C strict forward outcome audit "
            "did not validate frozen score."
        ),

        now,
    ))

    db.execute("""
    INSERT INTO lab_decisions (
        timestamp,
        experiment_id,
        decision,
        reason,
        automatic
    )

    VALUES (?,?,?,?,?)
    """, (

        now,

        "LEGACY_T119_T120",

        "REJECT",

        (
            "Frozen score not validated on strict "
            "good-coverage prospective outcomes."
        ),

        0,
    ))

    db.commit()


register_historical_research()


# ============================================================
# PROCESS MANAGEMENT
# ============================================================

def log_path(name):

    return (
        LOG_DIR
        / f"{name.lower()}.log"
    )


def update_process_db(
    name,
    script,
    pid,
    status,
    exit_code=None,
    increment_start=False,
    increment_crash=False,
):

    now = time.time()

    current = db.execute("""
    SELECT starts, crashes
    FROM lab_processes
    WHERE process_name=?
    """, (
        name,
    )).fetchone()

    starts = (
        current["starts"]
        if current
        else 0
    )

    crashes = (
        current["crashes"]
        if current
        else 0
    )

    if increment_start:
        starts += 1

    if increment_crash:
        crashes += 1

    db_write("""
    INSERT INTO lab_processes (

        process_name,
        script_name,

        pid,
        status,

        starts,
        crashes,

        started_at,
        stopped_at,

        last_exit_code,
        last_update_at
    )

    VALUES (
        ?,?,?,?,?,?,?,?,?,?
    )

    ON CONFLICT(process_name)
    DO UPDATE SET

        script_name=excluded.script_name,

        pid=excluded.pid,
        status=excluded.status,

        starts=excluded.starts,
        crashes=excluded.crashes,

        started_at=
            CASE
                WHEN excluded.status='RUNNING'
                THEN excluded.started_at
                ELSE lab_processes.started_at
            END,

        stopped_at=
            CASE
                WHEN excluded.status!='RUNNING'
                THEN excluded.stopped_at
                ELSE lab_processes.stopped_at
            END,

        last_exit_code=excluded.last_exit_code,

        last_update_at=excluded.last_update_at
    """, (

        name,
        script,

        pid,
        status,

        starts,
        crashes,

        (
            now
            if status == "RUNNING"
            else None
        ),

        (
            now
            if status != "RUNNING"
            else None
        ),

        exit_code,

        now,
    ))

    db.commit()


def start_process(name):

    cfg = PROCESSES[name]

    script = (
        ROOT
        / cfg["script"]
    )

    if not script.exists():

        update_process_db(
            name,
            cfg["script"],
            None,
            "MISSING"
        )

        return False

    logfile = open(
        log_path(name),
        "a",
        buffering=1
    )

    log_handles[name] = logfile

    logfile.write(
        "\n\n"
        + "=" * 100
        + "\n"
    )

    logfile.write(
        f"AUTONOMOUS LAB START "
        f"{time.strftime('%Y-%m-%d %H:%M:%S')}\n"
    )

    logfile.flush()

    proc = subprocess.Popen(

        [
            PYTHON,
            str(script)
        ],

        cwd=str(ROOT),

        stdout=logfile,
        stderr=subprocess.STDOUT,

        start_new_session=True,

        env=os.environ.copy()
    )

    children[name] = proc

    update_process_db(
        name,
        cfg["script"],
        proc.pid,
        "RUNNING",
        increment_start=True
    )

    return True


def stop_process(name):

    proc = children.get(name)

    if not proc:
        return

    if proc.poll() is not None:
        return

    try:

        os.killpg(
            os.getpgid(proc.pid),
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
                os.getpgid(proc.pid),
                signal.SIGTERM
            )

        except Exception:
            pass

    update_process_db(
        name,
        PROCESSES[name]["script"],
        None,
        "STOPPED",
        exit_code=proc.poll()
    )


def ensure_processes():

    for name in PROCESSES:

        proc = children.get(name)

        if proc is None:

            start_process(name)
            continue

        code = proc.poll()

        if code is None:

            update_process_db(
                name,
                PROCESSES[name]["script"],
                proc.pid,
                "RUNNING"
            )

            continue

        update_process_db(
            name,
            PROCESSES[name]["script"],
            None,
            "CRASHED",
            exit_code=code,
            increment_crash=True
        )

        children.pop(
            name,
            None
        )

        old_log = log_handles.pop(
            name,
            None
        )

        if old_log:

            try:
                old_log.close()
            except Exception:
                pass

        time.sleep(
            RESTART_DELAY
        )

        start_process(
            name
        )


# ============================================================
# DATA HEALTH
# ============================================================

def migration_health(now):

    if not table_exists(
        "t101_migrations"
    ):

        return {
            "status": "RED",
            "total": 0,
            "last_hour": 0,
            "newest_age": None,
        }

    row = db.execute("""
    SELECT

        COUNT(*) AS total,

        SUM(
            CASE
                WHEN status='OK'
                THEN 1 ELSE 0
            END
        ) AS ok,

        SUM(
            CASE
                WHEN status='OK'
                 AND COALESCE(
                        block_time,
                        detected_at
                     ) >= strftime('%s','now')-3600
                THEN 1 ELSE 0
            END
        ) AS last_hour,

        MAX(
            CASE
                WHEN status='OK'
                THEN COALESCE(
                    block_time,
                    detected_at
                )
            END
        ) AS newest

    FROM t101_migrations
    """).fetchone()

    newest_age = (
        now - row["newest"]
        if row["newest"] is not None
        else None
    )

    status = (
        "GREEN"
        if (row["last_hour"] or 0) > 0
        else "YELLOW"
    )

    return {
        "status": status,
        "total": row["total"] or 0,
        "ok": row["ok"] or 0,
        "last_hour": row["last_hour"] or 0,
        "newest_age": newest_age,
    }


def raw_health(now):

    if not (
        table_exists(
            "t116_pump_signatures"
        )
        and table_exists(
            "t116_pump_swaps"
        )
    ):

        return {
            "status": "RED"
        }

    sig = db.execute("""
    SELECT

        SUM(
            status='WAITING'
        ) AS waiting,

        SUM(
            status='RETRY'
        ) AS retry,

        SUM(
            status='FAILED'
        ) AS failed,

        MIN(
            CASE
                WHEN status IN (
                    'WAITING',
                    'RETRY'
                )
                THEN received_at
            END
        ) AS oldest

    FROM t116_pump_signatures
    """).fetchone()

    sw = db.execute("""
    SELECT

        COUNT(*) AS swaps,

        COUNT(
            DISTINCT token_mint
        ) AS tokens,

        SUM(
            timestamp
            >= strftime('%s','now')-120
        ) AS recent,

        MAX(timestamp) AS newest

    FROM t116_pump_swaps
    """).fetchone()

    oldest_age = (
        now - sig["oldest"]
        if sig["oldest"]
        else 0
    )

    newest_age = (
        now - sw["newest"]
        if sw["newest"]
        else None
    )

    if (
        oldest_age <= 60
        and (sw["recent"] or 0) > 0
        and (sig["failed"] or 0) == 0
    ):

        status = "GREEN"

    elif oldest_age <= 180:

        status = "YELLOW"

    else:

        status = "RED"

    return {
        "status": status,

        "waiting": sig["waiting"] or 0,
        "retry": sig["retry"] or 0,
        "failed": sig["failed"] or 0,

        "oldest_age": oldest_age,

        "swaps": sw["swaps"] or 0,
        "tokens": sw["tokens"] or 0,
        "recent": sw["recent"] or 0,

        "newest_age": newest_age,
    }


def lifecycle_health():

    if not table_exists(
        "t116_token_state"
    ):

        return {
            "status": "RED"
        }

    r = db.execute("""
    SELECT

        COUNT(*) AS tokens,

        SUM(
            migrated=1
        ) AS migrated,

        SUM(
            CASE
                WHEN migrated=0
                 AND seconds_since_last_clean_swap <= 300
                THEN 1 ELSE 0
            END
        ) AS active,

        SUM(
            CASE
                WHEN migrated=0
                 AND run_from_first_pct >= 50
                THEN 1 ELSE 0
            END
        ) AS pump50,

        SUM(
            CASE
                WHEN migrated=0
                 AND drawdown_from_peak_pct <= -20
                THEN 1 ELSE 0
            END
        ) AS dump20

    FROM t116_token_state
    """).fetchone()

    return {
        "status": "GREEN",

        "tokens": r["tokens"] or 0,
        "active": r["active"] or 0,

        "pump50": r["pump50"] or 0,
        "dump20": r["dump20"] or 0,

        "migrated": r["migrated"] or 0,
    }


def event_health():

    if not (
        table_exists(
            "t116_pump_events"
        )
        and table_exists(
            "t116_premigration_dump_events"
        )
    ):

        return {
            "status": "RED"
        }

    p = db.execute("""
    SELECT
        COUNT(*) AS events,
        COUNT(DISTINCT token_mint) AS tokens
    FROM t116_pump_events
    """).fetchone()

    d = db.execute("""
    SELECT
        COUNT(*) AS events,
        COUNT(DISTINCT token_mint) AS tokens
    FROM t116_premigration_dump_events
    """).fetchone()

    return {
        "status": "GREEN",

        "pump_events": p["events"] or 0,
        "pump_tokens": p["tokens"] or 0,

        "dump_events": d["events"] or 0,
        "dump_tokens": d["tokens"] or 0,
    }


# ============================================================
# AUTONOMOUS RESEARCH STATE
# ============================================================

def next_experiment_id():

    last = get_meta(
        "next_experiment_number"
    )

    n = (
        int(last)
        if last is not None
        else 121
    )

    set_meta(
        "next_experiment_number",
        n + 1
    )

    return (
        f"EXP_{n:04d}"
    )


def ensure_next_research_experiment():

    active = db.execute("""
    SELECT *
    FROM lab_experiments

    WHERE status IN (
        'PLANNED',
        'COLLECTING',
        'DISCOVERY',
        'FROZEN',
        'HOLDOUT'
    )

    ORDER BY created_at ASC

    LIMIT 1
    """).fetchone()

    if active:
        return active

    experiment_id = (
        next_experiment_id()
    )

    now = time.time()

    # First autonomous branch:
    # POST-ENTRY reversal information.
    #
    # This deliberately changes the information timing
    # after T119/T120 failed strict-entry validation.

    db.execute("""
    INSERT INTO lab_experiments (

        experiment_id,

        created_at,
        status,

        research_branch,

        hypothesis,

        unit,

        target,

        entry_definition,

        feature_family,

        feature_spec,

        stage_spec,

        horizon_spec,

        conclusion,

        last_update_at
    )

    VALUES (
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?
    )
    """, (

        experiment_id,

        now,

        "PLANNED",

        "POST_ENTRY_REVERSAL",

        (
            "Information observed AFTER a strict executable "
            "post-dump entry can distinguish genuine rebounds "
            "from continued collapse."
        ),

        "UNIQUE_FIRST_DUMP_TOKEN",

        (
            "+20% strictly after observation stage "
            "within forward 300s"
        ),

        (
            "FIRST_VALID_CLEAN_PRICE_AT_OR_AFTER_FIRST_DUMP"
        ),

        "POST_ENTRY_PRICE_ACTIVITY_FLOW",

        (
            "return_since_entry,"
            "new_low,"
            "reclaim_entry,"
            "mfe_so_far,"
            "mae_so_far,"
            "swaps,"
            "buys,"
            "sells,"
            "buy_ratio,"
            "buy_sol,"
            "sell_sol,"
            "net_sol"
        ),

        "5s,10s,20s,30s,60s",

        "300s",

        None,

        now,
    ))

    db.execute("""
    INSERT INTO lab_decisions (

        timestamp,
        experiment_id,

        decision,
        reason,

        automatic
    )

    VALUES (
        ?,?,?,?,1
    )
    """, (

        now,

        experiment_id,

        "CREATE_EXPERIMENT",

        (
            "Previous pre-trigger score failed strict "
            "prospective validation. Move information timing "
            "forward to executable post-entry stages."
        ),
    ))

    db.commit()

    return db.execute("""
    SELECT *
    FROM lab_experiments
    WHERE experiment_id=?
    """, (
        experiment_id,
    )).fetchone()


# ============================================================
# AUTONOMOUS DECISION ENGINE
# ============================================================

def update_decision_engine():

    exp = ensure_next_research_experiment()

    if not exp:
        return

    # EXP_0121 currently exists as PLANNED.
    #
    # The next module we build will be the generic experiment
    # runner. For now the orchestrator knows exactly what
    # experiment comes next and keeps it immutable.

    if exp["status"] == "PLANNED":

        set_meta(
            "research_next_action",
            (
                "BUILD_POST_ENTRY_FEATURE_ENGINE"
            )
        )


# ============================================================
# DASHBOARD
# ============================================================

def icon(status):

    return {
        "GREEN": "🟢",
        "YELLOW": "🟡",
        "RED": "🔴",
    }.get(
        status,
        "⚪"
    )


def process_dashboard():

    rows = db.execute("""
    SELECT *
    FROM lab_processes
    ORDER BY process_name
    """).fetchall()

    for r in rows:

        if r["status"] == "RUNNING":
            ic = "🟢"

        elif r["status"] in (
            "CRASHED",
            "MISSING"
        ):
            ic = "🔴"

        else:
            ic = "🟡"

        print(
            f"{ic} "
            f"{r['process_name']:<22} "
            f"| {r['status']:<9} "
            f"| PID={str(r['pid'] or '-'):>7} "
            f"| STARTS={r['starts']:>3} "
            f"| CRASHES={r['crashes']:>3}"
        )


def show_dashboard():

    os.system("clear")

    now = time.time()

    mh = migration_health(
        now
    )

    rh = raw_health(
        now
    )

    lh = lifecycle_health()

    eh = event_health()

    exp = ensure_next_research_experiment()

    print("=" * 150)

    print(
        "MEMECOIN LAB — AUTONOMOUS RESEARCH LAB"
    )

    print("=" * 150)

    print(
        time.strftime(
            "LOCAL TIME : %Y-%m-%d %H:%M:%S",
            time.localtime(now)
        )
    )

    print()

    print(
        "MODE       : AUTONOMOUS RESEARCH"
    )

    print(
        "LIVE TRADE : DISABLED"
    )

    print(
        "HOLDOUT MUTATION : FORBIDDEN"
    )

    print(
        "OLD RESEARCH : PRESERVED"
    )


    print()
    print("=" * 150)
    print("PROCESS SUPERVISOR")
    print("=" * 150)

    process_dashboard()


    print()
    print("=" * 150)
    print("DATA PIPELINE")
    print("=" * 150)

    print(
        f"{icon(mh['status'])} MIGRATIONS "
        f"| OK={mh.get('ok',0)} "
        f"| LAST1H={mh.get('last_hour',0)} "
        f"| NEWEST AGE="
        f"{mh.get('newest_age'):.0f}s"
        if mh.get("newest_age") is not None
        else
        f"{icon(mh['status'])} MIGRATIONS | NO DATA"
    )

    print(
        f"{icon(rh['status'])} RAW PUMP "
        f"| SWAPS={rh.get('swaps',0):,} "
        f"| TOKENS={rh.get('tokens',0):,} "
        f"| LAST120={rh.get('recent',0)} "
        f"| WAIT={rh.get('waiting',0)} "
        f"| OLDEST={rh.get('oldest_age',0):.0f}s"
    )

    print(
        f"{icon(lh['status'])} LIFECYCLE "
        f"| TOKENS={lh.get('tokens',0):,} "
        f"| ACTIVE={lh.get('active',0)} "
        f"| PUMP50={lh.get('pump50',0)} "
        f"| DUMP20={lh.get('dump20',0)} "
        f"| MIGRATED={lh.get('migrated',0)}"
    )

    print(
        f"{icon(eh['status'])} EVENTS "
        f"| PUMP={eh.get('pump_events',0):,}"
        f"/{eh.get('pump_tokens',0)} tokens "
        f"| DUMP={eh.get('dump_events',0):,}"
        f"/{eh.get('dump_tokens',0)} tokens"
    )


    print()
    print("=" * 150)
    print("AUTONOMOUS RESEARCH ENGINE")
    print("=" * 150)

    if exp:

        print(
            f"ACTIVE EXPERIMENT : "
            f"{exp['experiment_id']}"
        )

        print(
            f"STATUS            : "
            f"{exp['status']}"
        )

        print(
            f"BRANCH            : "
            f"{exp['research_branch']}"
        )

        print(
            f"HYPOTHESIS        : "
            f"{exp['hypothesis']}"
        )

        print(
            f"ENTRY             : "
            f"{exp['entry_definition']}"
        )

        print(
            f"STAGES            : "
            f"{exp['stage_spec']}"
        )

        print(
            f"TARGET            : "
            f"{exp['target']}"
        )

    print()

    action = get_meta(
        "research_next_action"
    )

    print(
        f"NEXT ACTION       : "
        f"{action or 'NONE'}"
    )


    print()
    print("=" * 150)
    print("RESEARCH MEMORY")
    print("=" * 150)

    rejected = db.execute("""
    SELECT
        experiment_id,
        conclusion

    FROM lab_experiments

    WHERE status LIKE 'REJECT%'

    ORDER BY created_at DESC

    LIMIT 5
    """).fetchall()

    for r in rejected:

        print(
            f"🔴 {r['experiment_id']} "
            f"| {r['conclusion']}"
        )


    print()
    print("=" * 150)

    print(
        f"Refresh every {REFRESH}s "
        "| CTRL+C gracefully stops the entire lab"
    )

    print("=" * 150)


# ============================================================
# SIGNAL HANDLING
# ============================================================

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


# ============================================================
# MAIN
# ============================================================

def main():

    global shutdown_requested

    set_meta(
        "lab_started_at",
        time.time()
    )

    print(
        "Starting autonomous research lab..."
    )

    for name in PROCESSES:

        start_process(
            name
        )

        time.sleep(
            1
        )

    while not shutdown_requested:

        ensure_processes()

        update_decision_engine()

        show_dashboard()

        for _ in range(
            REFRESH
        ):

            if shutdown_requested:
                break

            time.sleep(
                1
            )

    print()
    print(
        "Stopping autonomous research lab..."
    )

    for name in list(
        children.keys()
    ):

        stop_process(
            name
        )

    for handle in log_handles.values():

        try:
            handle.close()
        except Exception:
            pass

    set_meta(
        "lab_stopped_at",
        time.time()
    )

    print(
        "✅ All managed processes stopped safely."
    )


if __name__ == "__main__":

    try:

        main()

    finally:

        db.close()
