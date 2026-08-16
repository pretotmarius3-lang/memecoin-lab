#!/usr/bin/env python3
"""Memecoin Lab V4.1 research-state foundation.

Research only. No trading/execution code lives here.

Design goals:
- no Unix socket / central writer
- short SQLite write transactions
- WAL + busy timeout
- atomic job leases
- crash recovery
- queue backpressure
- immutable-ish result records
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

ROOT = Path.home() / "memecoin_lab"
RESEARCH_DB = Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB", ROOT / "research_v4_1.db"))
MARKET_DB = Path(os.environ.get("MEMECOIN_MARKET_DB", ROOT / "validation_v090.db"))

BUSY_TIMEOUT_MS = int(os.environ.get("MEMECOIN_SQLITE_BUSY_MS", "30000"))
DEFAULT_LEASE_S = int(os.environ.get("MEMECOIN_JOB_LEASE_S", "900"))
MAX_ATTEMPTS = int(os.environ.get("MEMECOIN_JOB_MAX_ATTEMPTS", "3"))


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def fingerprint(value: Any, prefix: str = "") -> str:
    payload = (prefix + canonical_json(value)).encode()
    return hashlib.sha256(payload).hexdigest()


def open_research() -> sqlite3.Connection:
    db = sqlite3.connect(RESEARCH_DB, timeout=BUSY_TIMEOUT_MS / 1000.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def open_market() -> sqlite3.Connection:
    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=BUSY_TIMEOUT_MS / 1000.0,
    )
    db.row_factory = sqlite3.Row
    db.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
    return db


@contextmanager
def immediate(db: sqlite3.Connection):
    """Short atomic write transaction. Never do expensive research inside it."""
    db.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        db.rollback()
        raise
    else:
        db.commit()


def initialize() -> None:
    db = open_research()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS v41_meta (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS v41_hypotheses (
            hypothesis_id TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL UNIQUE,
            branch TEXT NOT NULL,
            family TEXT NOT NULL,
            spec_json TEXT NOT NULL,
            data_requirement_json TEXT NOT NULL,
            parent_hypothesis_id TEXT,
            generation INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(parent_hypothesis_id) REFERENCES v41_hypotheses(hypothesis_id)
        );

        CREATE TABLE IF NOT EXISTS v41_jobs (
            job_id TEXT PRIMARY KEY,
            hypothesis_id TEXT NOT NULL,
            job_type TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 100,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            worker_id TEXT,
            lease_until REAL,
            attempts INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            started_at REAL,
            finished_at REAL,
            FOREIGN KEY(hypothesis_id) REFERENCES v41_hypotheses(hypothesis_id)
        );

        CREATE INDEX IF NOT EXISTS idx_v41_jobs_claim
        ON v41_jobs(status, priority, created_at);

        CREATE INDEX IF NOT EXISTS idx_v41_jobs_lease
        ON v41_jobs(status, lease_until);

        CREATE TABLE IF NOT EXISTS v41_results (
            result_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            hypothesis_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            verdict TEXT NOT NULL,
            discovery_n INTEGER,
            holdout_n INTEGER,
            positives INTEGER,
            primary_metric REAL,
            effect_size REAL,
            p_value REAL,
            adjusted_p_value REAL,
            coverage_json TEXT,
            metrics_json TEXT NOT NULL,
            created_at REAL NOT NULL,
            FOREIGN KEY(job_id) REFERENCES v41_jobs(job_id),
            FOREIGN KEY(hypothesis_id) REFERENCES v41_hypotheses(hypothesis_id)
        );

        CREATE TABLE IF NOT EXISTS v41_memory (
            memory_id TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL UNIQUE,
            branch TEXT NOT NULL,
            family TEXT NOT NULL,
            verdict TEXT NOT NULL,
            lesson TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            created_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS v41_candidates (
            candidate_id TEXT PRIMARY KEY,
            hypothesis_id TEXT NOT NULL,
            status TEXT NOT NULL,
            frozen_spec_json TEXT NOT NULL,
            frozen_model_json TEXT,
            data_cutoff REAL NOT NULL,
            promoted_from_result_id TEXT NOT NULL,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            FOREIGN KEY(hypothesis_id) REFERENCES v41_hypotheses(hypothesis_id),
            FOREIGN KEY(promoted_from_result_id) REFERENCES v41_results(result_id)
        );

        CREATE TABLE IF NOT EXISTS v41_dataset_registry (
            dataset_id TEXT PRIMARY KEY,
            lane TEXT NOT NULL,
            description TEXT NOT NULL,
            requirements_json TEXT NOT NULL,
            snapshot_json TEXT NOT NULL,
            updated_at REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS v41_workers (
            worker_id TEXT PRIMARY KEY,
            pid INTEGER,
            state TEXT NOT NULL,
            current_job_id TEXT,
            jobs_done INTEGER NOT NULL DEFAULT 0,
            jobs_failed INTEGER NOT NULL DEFAULT 0,
            last_heartbeat REAL NOT NULL,
            started_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        """
    )
    now = time.time()
    db.execute(
        """
        INSERT INTO v41_meta(key,value_json,updated_at)
        VALUES('architecture',?,?)
        ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
        """,
        (canonical_json({"version": "V4.1", "ipc": "NONE", "live_trading": False}), now),
    )
    db.commit()
    db.close()


def create_hypothesis(
    branch: str,
    family: str,
    spec: dict[str, Any],
    data_requirement: dict[str, Any] | None = None,
    parent_hypothesis_id: str | None = None,
    generation: int = 0,
) -> tuple[str, bool]:
    data_requirement = data_requirement or {}
    identity = {"branch": branch, "family": family, "spec": spec, "data_requirement": data_requirement}
    fp = fingerprint(identity, "hypothesis:")
    hypothesis_id = "H_" + fp[:20]
    now = time.time()
    db = open_research()
    with immediate(db):
        before = db.total_changes
        db.execute(
            """
            INSERT OR IGNORE INTO v41_hypotheses(
                hypothesis_id,fingerprint,branch,family,spec_json,data_requirement_json,
                parent_hypothesis_id,generation,status,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                hypothesis_id, fp, branch, family, canonical_json(spec), canonical_json(data_requirement),
                parent_hypothesis_id, generation, "READY", now, now,
            ),
        )
        created = db.total_changes > before
    db.close()
    return hypothesis_id, created


def enqueue_job(
    hypothesis_id: str,
    job_type: str,
    payload: dict[str, Any],
    priority: int = 100,
) -> tuple[str, bool]:
    identity = {"hypothesis_id": hypothesis_id, "job_type": job_type, "payload": payload}
    job_id = "J_" + fingerprint(identity, "job:")[:24]
    now = time.time()
    db = open_research()
    with immediate(db):
        before = db.total_changes
        db.execute(
            """
            INSERT OR IGNORE INTO v41_jobs(
                job_id,hypothesis_id,job_type,priority,payload_json,status,created_at,updated_at
            ) VALUES(?,?,?,?,?,'QUEUED',?,?)
            """,
            (job_id, hypothesis_id, job_type, priority, canonical_json(payload), now, now),
        )
        created = db.total_changes > before
    db.close()
    return job_id, created


def queue_counts() -> dict[str, int]:
    db = open_research()
    rows = db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status").fetchall()
    db.close()
    return {row["status"]: row["n"] for row in rows}


def reclaim_expired_jobs(now: float | None = None) -> int:
    now = now or time.time()
    db = open_research()
    with immediate(db):
        cur = db.execute(
            """
            UPDATE v41_jobs
            SET status='QUEUED', worker_id=NULL, lease_until=NULL,
                error=COALESCE(error,'') || CASE WHEN error IS NULL OR error='' THEN '' ELSE '\n' END || 'lease expired',
                updated_at=?
            WHERE status='RUNNING' AND lease_until IS NOT NULL AND lease_until < ? AND attempts < ?
            """,
            (now, now, MAX_ATTEMPTS),
        )
        reclaimed = cur.rowcount
        db.execute(
            """
            UPDATE v41_jobs
            SET status='FAILED', finished_at=?, updated_at=?,
                error=COALESCE(error,'') || CASE WHEN error IS NULL OR error='' THEN '' ELSE '\n' END || 'max attempts exceeded'
            WHERE status='RUNNING' AND lease_until IS NOT NULL AND lease_until < ? AND attempts >= ?
            """,
            (now, now, now, MAX_ATTEMPTS),
        )
    db.close()
    return reclaimed


def claim_job(worker_id: str, lease_s: int = DEFAULT_LEASE_S) -> dict[str, Any] | None:
    now = time.time()
    lease_until = now + lease_s
    db = open_research()
    with immediate(db):
        row = db.execute(
            """
            SELECT * FROM v41_jobs
            WHERE status='QUEUED' AND attempts < ?
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            """,
            (MAX_ATTEMPTS,),
        ).fetchone()
        if row is None:
            db.close()
            return None
        cur = db.execute(
            """
            UPDATE v41_jobs
            SET status='RUNNING', worker_id=?, lease_until=?, attempts=attempts+1,
                started_at=COALESCE(started_at,?), updated_at=?
            WHERE job_id=? AND status='QUEUED'
            """,
            (worker_id, lease_until, now, now, row["job_id"]),
        )
        if cur.rowcount != 1:
            db.close()
            return None
        claimed = dict(row)
        claimed.update({"status": "RUNNING", "worker_id": worker_id, "lease_until": lease_until})
    db.close()
    claimed["payload"] = json.loads(claimed.pop("payload_json"))
    return claimed


def renew_lease(job_id: str, worker_id: str, lease_s: int = DEFAULT_LEASE_S) -> bool:
    now = time.time()
    db = open_research()
    with immediate(db):
        cur = db.execute(
            """
            UPDATE v41_jobs SET lease_until=?,updated_at=?
            WHERE job_id=? AND worker_id=? AND status='RUNNING'
            """,
            (now + lease_s, now, job_id, worker_id),
        )
    db.close()
    return cur.rowcount == 1


def finish_job(
    job: dict[str, Any],
    verdict: str,
    stage: str,
    metrics: dict[str, Any],
    *,
    coverage: dict[str, Any] | None = None,
    discovery_n: int | None = None,
    holdout_n: int | None = None,
    positives: int | None = None,
    primary_metric: float | None = None,
    effect_size: float | None = None,
    p_value: float | None = None,
    adjusted_p_value: float | None = None,
) -> str:
    now = time.time()
    result_id = "R_" + uuid.uuid4().hex
    db = open_research()
    with immediate(db):
        owned = db.execute(
            "SELECT 1 FROM v41_jobs WHERE job_id=? AND worker_id=? AND status='RUNNING'",
            (job["job_id"], job["worker_id"]),
        ).fetchone()
        if owned is None:
            raise RuntimeError(f"job lease lost: {job['job_id']}")
        db.execute(
            """
            INSERT INTO v41_results(
                result_id,job_id,hypothesis_id,stage,verdict,discovery_n,holdout_n,positives,
                primary_metric,effect_size,p_value,adjusted_p_value,coverage_json,metrics_json,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                result_id, job["job_id"], job["hypothesis_id"], stage, verdict,
                discovery_n, holdout_n, positives, primary_metric, effect_size, p_value,
                adjusted_p_value, canonical_json(coverage or {}), canonical_json(metrics), now,
            ),
        )
        db.execute(
            """
            UPDATE v41_jobs SET status='DONE',finished_at=?,lease_until=NULL,updated_at=?
            WHERE job_id=? AND worker_id=?
            """,
            (now, now, job["job_id"], job["worker_id"]),
        )
        db.execute(
            "UPDATE v41_hypotheses SET status=?,updated_at=? WHERE hypothesis_id=?",
            (verdict, now, job["hypothesis_id"]),
        )
    db.close()
    return result_id


def fail_job(job: dict[str, Any], error: str) -> None:
    now = time.time()
    db = open_research()
    with immediate(db):
        row = db.execute("SELECT attempts FROM v41_jobs WHERE job_id=?", (job["job_id"],)).fetchone()
        attempts = row["attempts"] if row else MAX_ATTEMPTS
        next_status = "FAILED" if attempts >= MAX_ATTEMPTS else "QUEUED"
        db.execute(
            """
            UPDATE v41_jobs SET status=?,worker_id=NULL,lease_until=NULL,error=?,
                finished_at=CASE WHEN ?='FAILED' THEN ? ELSE finished_at END,updated_at=?
            WHERE job_id=?
            """,
            (next_status, error[-8000:], next_status, now, now, job["job_id"]),
        )
    db.close()


def worker_heartbeat(worker_id: str, state: str, current_job_id: str | None = None, done_inc: int = 0, failed_inc: int = 0) -> None:
    now = time.time()
    db = open_research()
    with immediate(db):
        db.execute(
            """
            INSERT INTO v41_workers(worker_id,pid,state,current_job_id,jobs_done,jobs_failed,last_heartbeat,started_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(worker_id) DO UPDATE SET
                pid=excluded.pid,state=excluded.state,current_job_id=excluded.current_job_id,
                jobs_done=v41_workers.jobs_done+?,jobs_failed=v41_workers.jobs_failed+?,
                last_heartbeat=excluded.last_heartbeat,updated_at=excluded.updated_at
            """,
            (worker_id, os.getpid(), state, current_job_id, done_inc, failed_inc, now, now, now, done_inc, failed_inc),
        )
    db.close()


def register_dataset(dataset_id: str, lane: str, description: str, requirements: dict[str, Any], snapshot: dict[str, Any]) -> None:
    now = time.time()
    db = open_research()
    with immediate(db):
        db.execute(
            """
            INSERT INTO v41_dataset_registry(dataset_id,lane,description,requirements_json,snapshot_json,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(dataset_id) DO UPDATE SET lane=excluded.lane,description=excluded.description,
                requirements_json=excluded.requirements_json,snapshot_json=excluded.snapshot_json,updated_at=excluded.updated_at
            """,
            (dataset_id, lane, description, canonical_json(requirements), canonical_json(snapshot), now),
        )
    db.close()


def should_generate(workers: int, queued: int | None = None) -> bool:
    queued = queue_counts().get("QUEUED", 0) if queued is None else queued
    return queued < max(4, workers * 4)


def generation_budget(workers: int, queued: int | None = None) -> int:
    queued = queue_counts().get("QUEUED", 0) if queued is None else queued
    target = max(8, workers * 8)
    hard_stop = max(16, workers * 16)
    if queued >= hard_stop:
        return 0
    return max(0, target - queued)


def rows_to_dicts(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


if __name__ == "__main__":
    initialize()
    print(f"V4.1 research DB ready: {RESEARCH_DB}")
    print(f"Market DB (read-only): {MARKET_DB}")
    print("Queue:", queue_counts())
