#!/usr/bin/env python3

import sqlite3
import json
import time
import hashlib
from pathlib import Path

DB = "validation_v090.db"
OUT = Path("t82_target5_frozen.json")

db = sqlite3.connect(DB, timeout=30)

boundary_id = db.execute("""
SELECT COALESCE(MAX(id),0)
FROM events
""").fetchone()[0]

freeze = {
    "experiment": "T82_TARGET5_PROSPECTIVE",

    "created_at": time.time(),

    "boundary_id": int(boundary_id),

    "target": {
        "horizon_seconds": 60,
        "runner_threshold": 5.0,
        "dump_threshold": -5.0,
        "neutral_definition": "-5 < dex_return_60s < +5",
    },

    "purpose":
        "Independent prospective target-density experiment",

    "reference_models": {
        "t59":
            "CAP-v2",

        "t78":
            "CAP-v2 + buyer_velocity_10",
    },

    "constraints": {
        "historical_rows_allowed": False,
        "model_refitting": False,
        "threshold_optimization": False,
        "modify_t59": False,
        "modify_t78": False,
    }
}

canonical = json.dumps(
    freeze,
    sort_keys=True,
    separators=(",", ":")
).encode()

freeze["freeze_sha256"] = hashlib.sha256(
    canonical
).hexdigest()

OUT.write_text(
    json.dumps(
        freeze,
        indent=2,
        sort_keys=True
    )
)

print("=" * 100)
print("MEMECOIN LAB — T82 ±5% TARGET FREEZE")
print("=" * 100)
print()
print("BOUNDARY ID :", boundary_id)
print("FREEZE HASH :", freeze["freeze_sha256"])
print()
print("RUN         : >= +5%")
print("DUMP        : <= -5%")
print("HORIZON     : 60s")
print()
print("T59         : UNTOUCHED")
print("T78         : UNTOUCHED")
print("REFIT       : FORBIDDEN")
print("RETRO DATA  : FORBIDDEN")
print()
print("🟢 T82 TARGET FREEZE CREATED.")
