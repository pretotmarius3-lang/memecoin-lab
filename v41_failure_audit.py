#!/usr/bin/env python3
"""V4.1 failure diagnostics.

Read-only with respect to failed jobs. It groups failures by root cause, branch,
job type, adapter and generation so we can fix causes before requeueing anything.

Safe to run while the organism is active.
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict

import v41_core as core


def root_cause(error: str | None) -> str:
    if not error:
        return "NO_ERROR_TEXT"
    lines = [x.strip() for x in str(error).splitlines() if x.strip()]
    if not lines:
        return "EMPTY_ERROR"
    # Prefer the final exception line from Python tracebacks.
    last = lines[-1]
    # Normalize volatile values while preserving the useful exception identity.
    last = re.sub(r"0x[0-9a-fA-F]+", "0x…", last)
    last = re.sub(r"\b\d{4,}\b", "N", last)
    return last[:240]


def main() -> None:
    core.initialize()
    db = core.open_research()
    rows = db.execute(
        """
        SELECT j.job_id,j.job_type,j.status,j.error,j.attempts,j.payload_json,
               h.branch,h.family,h.generation
        FROM v41_jobs j
        JOIN v41_hypotheses h ON h.hypothesis_id=j.hypothesis_id
        WHERE j.status='FAILED'
        ORDER BY j.updated_at DESC
        """
    ).fetchall()
    db.close()

    causes = Counter()
    by_branch = Counter()
    by_job_type = Counter()
    by_adapter = Counter()
    by_generation = Counter()
    samples: dict[str, list[str]] = defaultdict(list)

    for r in rows:
        cause = root_cause(r["error"])
        causes[cause] += 1
        by_branch[r["branch"]] += 1
        by_job_type[r["job_type"]] += 1
        by_generation[int(r["generation"] or 0)] += 1
        try:
            payload = json.loads(r["payload_json"] or "{}")
        except Exception:
            payload = {}
        adapter = payload.get("adapter", "UNKNOWN")
        by_adapter[adapter] += 1
        if len(samples[cause]) < 3:
            samples[cause].append(r["job_id"])

    print("=" * 132)
    print("MEMECOIN LAB — V4.1 FAILURE AUDIT")
    print("=" * 132)
    print(f"FAILED JOBS: {len(rows):,}")

    def section(title, counter):
        print("\n" + title)
        print("-" * 100)
        for key, n in counter.most_common():
            print(f"{str(key):<70} {n:>7,}  {100*n/len(rows) if rows else 0:>6.1f}%")

    section("BY ROOT CAUSE", causes)
    section("BY BRANCH", by_branch)
    section("BY JOB TYPE", by_job_type)
    section("BY ADAPTER", by_adapter)
    section("BY GENERATION", by_generation)

    print("\nROOT-CAUSE SAMPLES")
    print("-" * 100)
    for cause, n in causes.most_common(15):
        print(f"[{n:>4}] {cause}")
        print("       jobs:", ", ".join(samples[cause]))

    print("\n" + "=" * 132)
    print("AUDIT COMPLETE — nothing was requeued or modified")
    print("=" * 132)


if __name__ == "__main__":
    main()
