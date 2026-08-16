#!/usr/bin/env python3

import sqlite3
import json
import time
import hashlib
from pathlib import Path

DB = "validation_v090.db"
OUT = Path("t86_capv2_300s_frozen.json")

MODEL = {
    "name": "T85_M1_CAPV2_300S",

    "intercept": -1.0798400615158867,

    "features": [
        "fa",
        "new_wallets30",
        "recent_buy_share",
        "recent_net_share",
        "breadth_score",
        "late_chase_score",
        "early_div",
    ],

    "means": {
        "fa": 0.22344848446440677,
        "new_wallets30": 3.7796610169491527,
        "recent_buy_share": 0.4094735608556974,
        "recent_net_share": 0.13195146193057472,
        "breadth_score": 1.0243857811465655,
        "late_chase_score": 0.670955292492045,
        "early_div": 2.0395921004254185,
    },

    "stds": {
        "fa": 0.20503291652243646,
        "new_wallets30": 2.343890700076275,
        "recent_buy_share": 0.23899978701310534,
        "recent_net_share": 0.43044683970129727,
        "breadth_score": 0.686162821285442,
        "late_chase_score": 0.38368953685234575,
        "early_div": 25.82383364169655,
    },

    "betas": {
        "fa": 0.3841317859351703,
        "new_wallets30": -0.2060035579193797,
        "recent_buy_share": -0.04381572861339318,
        "recent_net_share": 0.1249024972888682,
        "breadth_score": -0.245718126926708,
        "late_chase_score": 0.037951106553451026,
        "early_div": 0.0972579849560012,
    }
}

db = sqlite3.connect(DB, timeout=30)

boundary = db.execute("""
SELECT COALESCE(MAX(id), 0)
FROM events
""").fetchone()[0]

db.close()

freeze = {
    "experiment": "T86_CAPV2_300S_PROSPECTIVE",
    "created_at": time.time(),

    "boundary_id": int(boundary),

    "target": {
        "column": "dex_return_300s",
        "horizon_seconds": 300,
        "run_threshold": 10.0,
        "dump_threshold": -10.0,
    },

    "model": MODEL,

    "early_div_definition":
        "early_price_return - early_net_sol",

    "constraints": {
        "historical_rows_allowed": False,
        "model_refitting": False,
        "threshold_search": False,
        "coefficient_changes": False,
        "modify_t59": False,
        "modify_t78": False,
        "modify_t82": False,
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

print("=" * 110)
print("MEMECOIN LAB — T86 CAP-v2 @300s FREEZE")
print("=" * 110)
print()
print("BOUNDARY ID :", boundary)
print("FREEZE HASH :", freeze["freeze_sha256"])
print()
print("MODEL       : T85 M1 CAP-v2")
print("TARGET      : dex_return_300s")
print("RUN         : >= +10%")
print("DUMP        : <= -10%")
print()
print("FEATURES    :", len(MODEL["features"]))
print("REFIT       : FORBIDDEN")
print("THRESHOLDS  : FROZEN")
print("RETRO DATA  : FORBIDDEN")
print()
print("T59/T78/T82 : UNTOUCHED")
print()
print("🟢 T86 FREEZE CREATED.")
