#!/usr/bin/env python3

import json
import os
import signal
import socket
import sqlite3
import time
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"

DB = ROOT / "research_lab.db"
SOCKET = ROOT / ".research_writer.sock"

stop_requested = False


db = sqlite3.connect(
    str(DB),
    timeout=30,
    isolation_level=None,
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA busy_timeout=30000")
db.execute("PRAGMA wal_autocheckpoint=1000")


db.executescript("""
CREATE TABLE IF NOT EXISTS lab_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS process_state (
    component TEXT PRIMARY KEY,
    script TEXT,
    pid INTEGER,
    state TEXT NOT NULL,

    starts INTEGER NOT NULL DEFAULT 0,
    crashes INTEGER NOT NULL DEFAULT 0,
    stalls INTEGER NOT NULL DEFAULT 0,

    last_start REAL,
    last_stop REAL,
    last_crash REAL,
    last_stall REAL,

    last_exit_code INTEGER,

    quarantined_until REAL,

    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS lab_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    severity TEXT NOT NULL,
    payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_lab_events_timestamp
ON lab_events(timestamp);

CREATE TABLE IF NOT EXISTS hypotheses (
    hypothesis_id TEXT PRIMARY KEY,

    created_at REAL NOT NULL,

    parent_hypothesis_id TEXT,

    branch TEXT NOT NULL,
    species TEXT NOT NULL,

    statement TEXT NOT NULL,
    rationale TEXT,

    novelty_score REAL,
    information_gain_score REAL,

    priority REAL NOT NULL DEFAULT 0,

    status TEXT NOT NULL,

    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hypotheses_status_priority
ON hypotheses(
    status,
    priority DESC
);

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id TEXT PRIMARY KEY,

    hypothesis_id TEXT,

    created_at REAL NOT NULL,

    branch TEXT NOT NULL,

    stage TEXT NOT NULL,

    status TEXT NOT NULL,

    spec_json TEXT NOT NULL,

    discovery_n INTEGER,
    positive_n INTEGER,
    negative_n INTEGER,

    discovery_score REAL,
    robustness_score REAL,
    prospective_score REAL,
    execution_score REAL,
    survival_score REAL,

    conclusion TEXT,

    frozen_at REAL,

    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_experiments_status
ON experiments(status);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,

    experiment_id TEXT,

    job_type TEXT NOT NULL,

    priority REAL NOT NULL,

    status TEXT NOT NULL,

    worker_id TEXT,

    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,

    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,

    payload_json TEXT,
    result_json TEXT,

    error TEXT,

    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_queue
ON jobs(
    status,
    priority DESC,
    created_at
);

CREATE TABLE IF NOT EXISTS research_memory (
    memory_id TEXT PRIMARY KEY,

    created_at REAL NOT NULL,

    memory_type TEXT NOT NULL,

    branch TEXT,

    subject TEXT NOT NULL,
    lesson TEXT NOT NULL,

    evidence_json TEXT,

    retest_policy TEXT,

    confidence REAL,

    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS research_species_stats (
    species TEXT PRIMARY KEY,

    hypotheses_generated INTEGER NOT NULL DEFAULT 0,
    experiments_launched INTEGER NOT NULL DEFAULT 0,
    discovery_passed INTEGER NOT NULL DEFAULT 0,
    holdout_passed INTEGER NOT NULL DEFAULT 0,
    shadow_passed INTEGER NOT NULL DEFAULT 0,
    rejected INTEGER NOT NULL DEFAULT 0,

    compute_seconds REAL NOT NULL DEFAULT 0,

    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_samples (
    timestamp REAL PRIMARY KEY,

    load1 REAL,
    load5 REAL,
    load15 REAL,

    child_count INTEGER,

    jobs_queued INTEGER,
    jobs_running INTEGER,

    hypotheses_queued INTEGER
);

CREATE TABLE IF NOT EXISTS throughput_samples (
    timestamp REAL PRIMARY KEY,

    hypotheses_total INTEGER,
    hypotheses_queued INTEGER,

    experiments_total INTEGER,
    experiments_active INTEGER,

    jobs_queued INTEGER,
    jobs_running INTEGER,

    rejected INTEGER,
    frozen INTEGER,
    shadow INTEGER,
    demo INTEGER
);

CREATE TABLE IF NOT EXISTS shadow_signals (
    signal_id TEXT PRIMARY KEY,

    experiment_id TEXT NOT NULL,

    token_mint TEXT NOT NULL,

    signal_ts REAL NOT NULL,

    reference_price REAL,
    score REAL,

    simulated_fill_price REAL,
    simulated_latency_ms REAL,
    simulated_slippage_pct REAL,

    status TEXT NOT NULL,

    outcome_json TEXT,

    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_trades (
    trade_id TEXT PRIMARY KEY,

    experiment_id TEXT NOT NULL,

    token_mint TEXT NOT NULL,

    opened_at REAL,
    closed_at REAL,

    entry_price REAL,
    exit_price REAL,

    pnl_pct REAL,

    fees_pct REAL,
    slippage_pct REAL,

    state TEXT NOT NULL,

    metadata_json TEXT,

    updated_at REAL NOT NULL
);
""")


now = time.time()

db.execute("""
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
VALUES (?,?,?,?,?,?,?,?,?,?)
""", (
    "LEGACY_T119_T120",
    now,
    "FAILED_HYPOTHESIS",
    "RESURRECTION",
    "PRE_DUMP_STRUCTURE_ACTIVITY_BUY",
    (
        "Historical signal did not survive strict executable-entry "
        "prospective validation."
    ),
    json.dumps({
        "experiments": [
            "T119",
            "T120",
            "T120B",
            "T120C"
        ]
    }),
    (
        "Do not retest unchanged. Retest only if timing, source data "
        "or causal structure changes."
    ),
    0.95,
    now,
))


def retry(fn):

    last = None

    for i in range(50):

        try:
            return fn()

        except sqlite3.OperationalError as e:

            last = e

            msg = str(e).lower()

            if (
                "locked" not in msg
                and "busy" not in msg
            ):
                raise

            time.sleep(
                min(
                    0.02 * (1.3 ** i),
                    1.0
                )
            )

    raise last


def handle(req):

    op = req.get("op")

    if op == "ping":

        return {
            "ok": True,
            "timestamp": time.time(),
        }


    if op == "execute":

        cur = retry(
            lambda: db.execute(
                req["sql"],
                req.get("params", [])
            )
        )

        return {
            "ok": True,
            "rowcount": cur.rowcount,
            "lastrowid": cur.lastrowid,
        }


    if op == "event":

        retry(
            lambda: db.execute("""
            INSERT INTO lab_events (
                timestamp,
                event_type,
                source,
                severity,
                payload_json
            )
            VALUES (?,?,?,?,?)
            """, (
                time.time(),
                req["event_type"],
                req["source"],
                req.get("severity", "INFO"),
                json.dumps(
                    req.get("payload", {}),
                    separators=(",", ":")
                )
            ))
        )

        return {
            "ok": True
        }


    raise RuntimeError(
        f"Unknown operation: {op}"
    )


def stop(signum, frame):

    global stop_requested

    stop_requested = True


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


if SOCKET.exists():

    try:
        SOCKET.unlink()
    except Exception:
        pass


server = socket.socket(
    socket.AF_UNIX,
    socket.SOCK_STREAM
)

server.bind(str(SOCKET))

os.chmod(
    SOCKET,
    0o600
)

server.listen(128)

server.settimeout(1.0)


print("=" * 90)
print("MEMECOIN LAB — CENTRAL RESEARCH WRITER")
print("=" * 90)
print(f"DB     : {DB}")
print(f"SOCKET : {SOCKET}")
print("MODE   : WAL / SINGLE WRITER")
print("STATUS : READY")


try:

    while not stop_requested:

        try:

            conn, _ = server.accept()

        except socket.timeout:

            continue


        with conn:

            try:

                buf = b""

                while b"\n" not in buf:

                    chunk = conn.recv(65536)

                    if not chunk:
                        break

                    buf += chunk


                if not buf:
                    continue


                request = json.loads(
                    buf.split(
                        b"\n",
                        1
                    )[0].decode()
                )

                response = handle(
                    request
                )


            except Exception as e:

                response = {
                    "ok": False,
                    "error": repr(e),
                }


            conn.sendall(
                (
                    json.dumps(
                        response,
                        separators=(",", ":")
                    )
                    + "\n"
                ).encode()
            )


finally:

    try:
        server.close()
    except Exception:
        pass

    try:

        if SOCKET.exists():
            SOCKET.unlink()

    except Exception:
        pass

    db.close()
