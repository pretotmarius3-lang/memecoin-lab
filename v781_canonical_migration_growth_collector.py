#!/usr/bin/env python3
"""MEMECOIN LAB — CANONICAL MIGRATION GROWTH COLLECTOR V7.8.1

Continuously refreshes the frozen V7.7.9 canonical migration table and V7.8.0
PRE/POST reconstruction as new locally captured migration transactions/swaps arrive.

Research infrastructure only. It does NOT alter the canonical evidence rule and does
NOT search/tune any alpha threshold.
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
MIG = ROOT / "v779_canonical_migrations.db"
PATHS = ROOT / "v780_canonical_migration_paths.db"
POLL = 30.0


def scalar(db: Path, sql: str):
    if not db.exists():
        return 0
    try:
        d = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=10)
        x = d.execute(sql).fetchone()[0]
        d.close()
        return int(x or 0)
    except Exception:
        return 0


def snapshot():
    canonical = scalar(MIG, "SELECT COUNT(*) FROM canonical_migrations")
    paths = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths")
    n5 = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths WHERE ret5 IS NOT NULL")
    n10 = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths WHERE ret10 IS NOT NULL")
    n30 = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths WHERE ret30 IS NOT NULL")
    n60 = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths WHERE ret60 IS NOT NULL")
    n120 = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths WHERE ret120 IS NOT NULL")
    n300 = scalar(PATHS, "SELECT COUNT(*) FROM migration_paths WHERE ret300 IS NOT NULL")
    return canonical, paths, n5, n10, n30, n60, n120, n300


def run(script: str):
    p = subprocess.run(
        [sys.executable, "-u", str(ROOT / script)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=180,
    )
    if p.returncode != 0:
        tail = "\n".join((p.stdout or "").splitlines()[-12:])
        print(f"V781 WARN {script} rc={p.returncode}\n{tail}", flush=True)
        return False
    return True


def main():
    print("MEMECOIN LAB V7.8.1 CANONICAL MIGRATION GROWTH COLLECTOR", flush=True)
    print("frozen_truth=V7.7.9 | reconstruction=V7.8.0 | no alpha tuning | poll=30s", flush=True)
    previous = snapshot()
    print(
        "V781 start canonical=%d paths=%d post5/10/30/60/120/300=%d/%d/%d/%d/%d/%d"
        % previous,
        flush=True,
    )

    while True:
        t0 = time.time()
        ok1 = run("v779_canonical_migration_table.py")
        ok2 = run("v780_canonical_migration_path_reconstruction.py") if ok1 else False
        now = snapshot()
        dc = now[0] - previous[0]
        dp = now[1] - previous[1]
        print(
            "V781 heartbeat canonical=%d (%+d) paths=%d (%+d) post5/10/30/60/120/300=%d/%d/%d/%d/%d/%d refresh=%.1fs status=%s"
            % (*now[:2], *now[2:], time.time() - t0, "OK" if ok1 and ok2 else "WARN"),
            flush=True,
        )
        # The tuple formatting above needs deltas in explicit positions; emit a compact delta line too.
        print(f"V781 delta canonical={dc:+d} paths={dp:+d}", flush=True)
        previous = now
        time.sleep(max(1.0, POLL - (time.time() - t0)))


if __name__ == "__main__":
    main()
