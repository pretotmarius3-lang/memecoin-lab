#!/usr/bin/env python3
"""Non-destructive smoke test for the V4.1 research queue.

Uses a temporary research DB and never touches validation_v090.db.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="memecoin-v41-") as tmp:
        db_path = Path(tmp) / "research_test.db"
        env = os.environ.copy()
        env["MEMECOIN_RESEARCH_V41_DB"] = str(db_path)
        code = r'''
import v41_core as c
c.initialize()
hid, created = c.create_hypothesis("TEST", "QUEUE", {"x": 1}, {"min_n": 1})
assert created
hid2, created2 = c.create_hypothesis("TEST", "QUEUE", {"x": 1}, {"min_n": 1})
assert hid2 == hid and not created2
jid, queued = c.enqueue_job(hid, "SMOKE", {"hello": "world"}, priority=1)
assert queued
job = c.claim_job("smoke-worker", lease_s=60)
assert job and job["job_id"] == jid
rid = c.finish_job(job, "PASS", "SMOKE", {"ok": True}, discovery_n=10, holdout_n=5)
assert rid.startswith("R_")
counts = c.queue_counts()
assert counts.get("DONE") == 1
print("V4.1 SMOKE TEST OK", counts)
'''
        proc = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).resolve().parent, env=env)
        return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
