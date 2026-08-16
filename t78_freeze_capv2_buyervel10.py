import sqlite3
import json
import hashlib
import time
from pathlib import Path

DB = "validation_v090.db"
OUT = Path("t78_capv2_buyervel10_frozen.json")

FEATURES = [
    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
    "early_div",
    "buyer_velocity_10",
]

MEANS = {
    "fa": 0.2706161312983871,
    "new_wallets30": 4.387096774193548,
    "recent_buy_share": 0.396151192090759,
    "recent_net_share": 0.09202832556355285,
    "breadth_score": 0.8937922443625443,
    "late_chase_score": 0.6635622212444809,
    "early_div": -1.2903575681095678,
    "buyer_velocity_10": 0.1467741935483871,
}

STDS = {
    "fa": 0.207957136024741,
    "new_wallets30": 2.6479135956427267,
    "recent_buy_share": 0.21471996463291348,
    "recent_net_share": 0.4110133608483303,
    "breadth_score": 0.6330871664086611,
    "late_chase_score": 0.3611234458670888,
    "early_div": 20.017149812385068,
    "buyer_velocity_10": 0.11460696079671301,
}

COEFFICIENTS = {
    "fa": 0.0031483701506290714,
    "new_wallets30": 0.06081582224475871,
    "recent_buy_share": -0.5002956936357325,
    "recent_net_share": 0.6183692253895903,
    "breadth_score": -0.1515540253215279,
    "late_chase_score": 0.007366799312321981,
    "early_div": -0.4515979780534501,
    "buyer_velocity_10": -0.5087326021234615,
}

INTERCEPT = -0.39902815727009566

db = sqlite3.connect(DB, timeout=30)

boundary_id = db.execute("""
SELECT COALESCE(MAX(id), 0)
FROM events
""").fetchone()[0]

freeze = {
    "experiment": "T78_CAPV2_BUYERVEL10_PROSPECTIVE",
    "created_at": time.time(),

    "boundary_id": int(boundary_id),

    "historical_source": "T77",

    "model": {
        "name": "M3_CAPV2_BUYERVEL10",
        "features": FEATURES,
        "intercept": INTERCEPT,
        "coefficients": COEFFICIENTS,
    },

    "standardization": {
        "source": "T77_TRAIN_ONLY",
        "means": MEANS,
        "stds": STDS,
    },

    "definitions": {
        "early_div":
            "early_price_return - early_net_sol",

        "buyer_velocity_10":
            "unique buyer arrivals in final 10 seconds / 10",

        "buyer_window_seconds": 30.0,

        "buyer_velocity_direction":
            "LOWER => RUN-like",
    },

    "constraints": {
        "model_refitting": False,
        "threshold_optimization": False,
        "interaction_search": False,
        "future_swaps_allowed": False,
        "t59_modified": False,
    },
}

canonical = json.dumps(
    freeze,
    sort_keys=True,
    separators=(",", ":"),
).encode()

freeze["freeze_sha256"] = hashlib.sha256(
    canonical
).hexdigest()

OUT.write_text(
    json.dumps(
        freeze,
        indent=2,
        sort_keys=True,
    )
)

print("=" * 100)
print("MEMECOIN LAB — T78 FREEZE")
print("=" * 100)
print()
print("BOUNDARY ID :", boundary_id)
print("FREEZE HASH :", freeze["freeze_sha256"])
print()
print("MODEL       : M3_CAPV2_BUYERVEL10")
print("FEATURES    :", len(FEATURES))
print()
print("BUYER VEL   : final 10s unique buyer arrivals / 10")
print("WINDOW      : strict pre-event 30s")
print("DIRECTION   : LOWER => RUN-like")
print()
print("MODEL REFIT : FORBIDDEN")
print("THRESHOLDS  : FROZEN / NO SEARCH")
print("T59         : UNTOUCHED")
print()
print("🟢 T78 FREEZE CREATED.")
print("Next = prospective recorder.")
