#!/usr/bin/env python3
"""Recover V4.1 jobs that failed only because a legacy v41_engine worker claimed
an adapter it did not understand.

This script is intentionally narrow:
- only FAILED jobs
- only adapters feature_set_refinement / feature_set_robustness
- only errors containing `unknown adapter`
- resets lease/worker/error/attempt counters
- does not modify completed results or scientific memory

Run only after stopping every legacy `v41_engine.py` process. The active research
process should be `v41_organism.py`.
"""

from __future__ import annotations

import json
import subprocess
import time

import v41_core as core

RECOVERABLE = {"feature_set_refinement", "feature_set_robustness"}


def legacy_processes() -> list[str]:
    try:
        out = subprocess.check_output(["ps", "aux"], text=True)
    except Exception:
        return []
    return [
        line for line in out.splitlines()
        if "v41_engine.py" in line and "grep" not in line
    ]


def main() -> int:
    legacy = legacy_processes()
    print("=" * 118)
    print("MEMECOIN LAB — V4.1 ADAPTER RECOVERY")
    print("=" * 118)

    if legacy:
        print("\nABORT: legacy v41_engine.py is still running. Stop it with CTRL+C first.\n")
        for line in legacy:
            print(line)
        return 2

    db = core.open_research()
    rows = db.execute(
        """
        SELECT job_id,payload_json,error,job_type,status,attempts
        FROM v41_jobs
        WHERE status='FAILED'
          AND error LIKE '%unknown adapter:%'
        ORDER BY created_at
        """
    ).fetchall()

    recover = []
    skipped = []
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            skipped.append((row["job_id"], "invalid payload json"))
            continue
        adapter = payload.get("adapter")
        if adapter in RECOVERABLE:
            recover.append((row["job_id"], adapter))
        else:
            skipped.append((row["job_id"], f"adapter={adapter!r}"))

    print(f"Recoverable jobs : {len(recover):,}")
    print(f"Skipped jobs     : {len(skipped):,}")

    if not recover:
        db.close()
        print("\nNothing to recover.")
        return 0

    counts = {}
    for _, adapter in recover:
        counts[adapter] = counts.get(adapter, 0) + 1
    print("\nBY ADAPTER")
    for adapter, n in sorted(counts.items()):
        print(f"{adapter:<36} {n:>6,}")

    now = time.time()
    db.execute("BEGIN IMMEDIATE")
    try:
        for job_id, _ in recover:
            db.execute(
                """
                UPDATE v41_jobs
                SET status='QUEUED',
                    worker_id=NULL,
                    lease_until=NULL,
                    attempts=0,
                    error=NULL,
                    started_at=NULL,
                    finished_at=NULL,
                    updated_at=?
                WHERE job_id=?
                  AND status='FAILED'
                """,
                (now, job_id),
            )
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()

    print(f"\nRECOVERED: {len(recover):,} jobs -> QUEUED")
    print("The running v41_organism.py workers can now claim them safely.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
