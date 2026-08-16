import sqlite3
import math
from collections import Counter

DB = "validation_v090.db"
TABLE = "t59_capv2_prospective"


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def section(title):
    print()
    print("=" * 160)
    print(title)
    print("=" * 160)


# ============================================================
# READ-ONLY DB
# ============================================================

db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=10
)

db.row_factory = sqlite3.Row


# ============================================================
# TABLE PRESENCE
# ============================================================

exists = db.execute("""
SELECT 1
FROM sqlite_master
WHERE type='table'
AND name=?
""", (TABLE,)).fetchone()

if not exists:
    raise RuntimeError(
        f"Missing table: {TABLE}"
    )


rows = db.execute(
    f"""
    SELECT *
    FROM {TABLE}
    ORDER BY event_id
    """
).fetchall()


cols = [
    r["name"]
    for r in db.execute(
        f"PRAGMA table_info({TABLE})"
    ).fetchall()
]


# ============================================================
# HEADER
# ============================================================

section("MEMECOIN LAB — T72 T59 PROSPECTIVE DATA INTEGRITY AUDIT")

print(f"TABLE              : {TABLE}")
print(f"ROWS               : {len(rows)}")
print(
    f"UNIQUE TOKENS      : "
    f"{len(set(r['token_mint'] for r in rows if r['token_mint']))}"
)
print("MODE               : READ-ONLY")
print("MODEL EVALUATION   : DISABLED")
print("THRESHOLD TUNING   : DISABLED")
print("DB WRITES          : DISABLED")


# ============================================================
# A) REQUIRED COLUMNS
# ============================================================

section("A) REQUIRED COLUMN CHECK")

required = [
    "event_id",
    "token_mint",
    "event_timestamp",
    "captured_at",

    "boundary_id",
    "freeze_sha256",

    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",

    "early_price_return",
    "early_net_sol",
    "early_div",

    "net_eff_diag",

    "control_score",
    "capv2_score",

    "dex_return_60s",
    "status",
    "binary_label",
    "labeled_60",
]

missing = [
    c for c in required
    if c not in cols
]

if missing:
    print("❌ MISSING:")
    for c in missing:
        print(f"  {c}")
else:
    print("✅ All expected T59 columns present.")


# ============================================================
# B) FREEZE INTEGRITY
# ============================================================

section("B) FREEZE INTEGRITY")

hashes = [
    r["freeze_sha256"]
    for r in rows
    if r["freeze_sha256"]
]

boundaries = [
    r["boundary_id"]
    for r in rows
    if r["boundary_id"] is not None
]

hash_counts = Counter(hashes)
boundary_counts = Counter(boundaries)

print(
    f"UNIQUE FREEZE HASHES : {len(hash_counts)}"
)

for h, n in hash_counts.items():
    print(
        f"  {h} | N={n}"
    )

print(
    f"UNIQUE BOUNDARIES    : {len(boundary_counts)}"
)

for b, n in boundary_counts.items():
    print(
        f"  {b} | N={n}"
    )

freeze_ok = (
    len(hash_counts) == 1
    and len(boundary_counts) == 1
)

print(
    "FREEZE STATUS        : "
    + (
        "✅ CONSISTENT"
        if freeze_ok
        else "❌ INCONSISTENT"
    )
)


# ============================================================
# C) STRICT PROSPECTIVE BOUNDARY
# ============================================================

section("C) STRICT PROSPECTIVE BOUNDARY")

if boundaries:

    boundary = boundaries[0]

    bad_boundary = [
        r
        for r in rows
        if r["event_id"] <= boundary
    ]

    print(
        f"BOUNDARY ID          : {boundary}"
    )

    print(
        f"ROWS <= BOUNDARY     : {len(bad_boundary)}"
    )

    if bad_boundary:
        print(
            "❌ Historical contamination detected."
        )
    else:
        print(
            "✅ Every captured event is strictly after boundary."
        )

else:
    print("Boundary unavailable.")


# ============================================================
# D) UNIQUE EVENT IDS / DUPLICATES
# ============================================================

section("D) EVENT-ID UNIQUENESS")

event_ids = [
    r["event_id"]
    for r in rows
]

counts = Counter(event_ids)

dups = {
    eid: n
    for eid, n in counts.items()
    if n > 1
}

print(
    f"UNIQUE EVENT IDS     : {len(counts)}"
)

print(
    f"DUPLICATE EVENT IDS  : {len(dups)}"
)

if dups:
    for eid, n in sorted(dups.items()):
        print(
            f"  event_id={eid} | N={n}"
        )
else:
    print(
        "✅ No duplicate prospective event IDs."
    )


# ============================================================
# E) TOKEN COVERAGE
# ============================================================

section("E) TOKEN COVERAGE")

token_counts = Counter(
    r["token_mint"]
    for r in rows
    if r["token_mint"]
)

print(
    f"UNIQUE TOKENS        : {len(token_counts)}"
)

print(
    f"MED/AVG EVENTS TOKEN : "
    f"{sum(token_counts.values())/len(token_counts):.2f}"
    if token_counts
    else "NA"
)

print()
print("TOP TOKEN EVENT COUNTS")

for tok, n in token_counts.most_common(10):
    print(
        f"{tok[:30]:30} | N={n}"
    )


# ============================================================
# F) FEATURE COMPLETENESS
# ============================================================

section("F) FEATURE COMPLETENESS")

feature_cols = [
    "fa",
    "new_wallets30",
    "recent_buy_share",
    "recent_net_share",
    "breadth_score",
    "late_chase_score",
    "early_price_return",
    "early_net_sol",
    "early_div",
    "control_score",
    "capv2_score",
]

feature_missing_total = 0

for c in feature_cols:

    miss = sum(
        not valid(r[c])
        for r in rows
    )

    feature_missing_total += miss

    print(
        f"{c:24} "
        f"MISSING={miss:3d}/{len(rows):3d}"
    )


# ============================================================
# G) EARLY_DIV RECONSTRUCTION
# ============================================================

section("G) EARLY_DIV FORMULA CHECK")

formula_bad = []

for r in rows:

    ep = r["early_price_return"]
    en = r["early_net_sol"]
    ed = r["early_div"]

    if not (
        valid(ep)
        and valid(en)
        and valid(ed)
    ):
        continue

    expected = (
        ep - en
    )

    if abs(
        expected - ed
    ) > 1e-9:
        formula_bad.append(
            (
                r["event_id"],
                ed,
                expected
            )
        )

print(
    f"MISMATCHES           : {len(formula_bad)}"
)

if formula_bad:

    for eid, actual, expected in formula_bad[:10]:
        print(
            f"  ID={eid} "
            f"| stored={actual} "
            f"| expected={expected}"
        )

else:
    print(
        "✅ early_div matches frozen definition on all complete rows."
    )


# ============================================================
# H) SCORE VALIDITY
# ============================================================

section("H) SCORE VALIDITY")

for c in [
    "control_score",
    "capv2_score"
]:

    invalid = []

    for r in rows:

        x = r[c]

        if not valid(x):
            invalid.append(
                r["event_id"]
            )
            continue

        if x < 0 or x > 1:
            invalid.append(
                r["event_id"]
            )

    print(
        f"{c:20} INVALID={len(invalid)}"
    )

    if invalid:
        print(
            f"  IDs: {invalid[:20]}"
        )


# ============================================================
# I) LABEL CONSISTENCY
# ============================================================

section("I) LABEL / STATUS CONSISTENCY")

issues = []

status_counts = Counter(
    r["status"]
    for r in rows
)

print("STATUS DISTRIBUTION")

for status, n in sorted(
    status_counts.items(),
    key=lambda x: str(x[0])
):
    print(
        f"  {status} -> {n}"
    )


for r in rows:

    r60 = r["dex_return_60s"]
    status = r["status"]
    binary = r["binary_label"]
    labeled = r["labeled_60"]

    if labeled == 0:

        if binary is not None:
            issues.append(
                (
                    r["event_id"],
                    "WAIT row has binary label"
                )
            )

        continue


    if not valid(r60):

        issues.append(
            (
                r["event_id"],
                "labeled row missing r60"
            )
        )

        continue


    if r60 >= 10:

        expected_status = "RUN"
        expected_binary = 1

    elif r60 <= -10:

        expected_status = "DUMP"
        expected_binary = 0

    else:

        expected_status = "NEUTRAL"
        expected_binary = None


    if status != expected_status:

        issues.append(
            (
                r["event_id"],
                f"status={status}, expected={expected_status}"
            )
        )


    if binary != expected_binary:

        issues.append(
            (
                r["event_id"],
                f"binary={binary}, expected={expected_binary}"
            )
        )


print()
print(
    f"LABEL CONSISTENCY ISSUES : {len(issues)}"
)

if issues:

    for x in issues[:20]:
        print(
            f"  ID={x[0]} | {x[1]}"
        )

else:
    print(
        "✅ Status/binary labels are internally consistent."
    )


# ============================================================
# J) CAPTURE TIMING
# ============================================================

section("J) CAPTURE TIMING")

delays = []

for r in rows:

    et = r["event_timestamp"]
    ct = r["captured_at"]

    if valid(et) and valid(ct):
        delays.append(
            ct-et
        )


if delays:

    delays_sorted = sorted(
        delays
    )

    n = len(delays_sorted)

    def pct(p):
        idx = int(
            p*(n-1)
        )
        return delays_sorted[
            idx
        ]

    print(
        f"N                  : {n}"
    )

    print(
        f"MIN CAPTURE DELAY  : {min(delays):.3f}s"
    )

    print(
        f"MED CAPTURE DELAY  : {delays_sorted[n//2]:.3f}s"
    )

    print(
        f"P90 CAPTURE DELAY  : {pct(0.90):.3f}s"
    )

    print(
        f"MAX CAPTURE DELAY  : {max(delays):.3f}s"
    )

    negative = sum(
        x < 0
        for x in delays
    )

    print(
        f"NEGATIVE DELAYS    : {negative}"
    )

else:
    print(
        "No timing rows available."
    )


# ============================================================
# K) EVENT TABLE EXISTENCE
# ============================================================

section("K) SOURCE EVENT EXISTENCE")

missing_source = []

for r in rows:

    exists = db.execute("""
    SELECT 1
    FROM events
    WHERE id=?
    """, (
        r["event_id"],
    )).fetchone()

    if not exists:
        missing_source.append(
            r["event_id"]
        )


print(
    f"MISSING SOURCE EVENTS : {len(missing_source)}"
)

if missing_source:
    print(
        missing_source[:20]
    )
else:
    print(
        "✅ Every T59 row maps to a source event."
    )


# ============================================================
# L) FINAL INTEGRITY SCORECARD
# ============================================================

section("L) INTEGRITY SCORECARD")

checks = {
    "freeze_single_hash":
        len(hash_counts) == 1,

    "freeze_single_boundary":
        len(boundary_counts) == 1,

    "strict_after_boundary":
        (
            bool(boundaries)
            and all(
                r["event_id"] > boundaries[0]
                for r in rows
            )
        ),

    "event_ids_unique":
        len(dups) == 0,

    "features_complete":
        feature_missing_total == 0,

    "early_div_formula":
        len(formula_bad) == 0,

    "labels_consistent":
        len(issues) == 0,

    "source_events_exist":
        len(missing_source) == 0,
}


passed = sum(
    checks.values()
)

for name, ok in checks.items():

    print(
        f"{name:28} "
        f"| {'✅ PASS' if ok else '❌ FAIL'}"
    )


print()
print(
    f"PASSED = {passed}/{len(checks)}"
)


if passed == len(checks):

    print()
    print(
        "🟢 T59 PROSPECTIVE DATA PIPELINE PASSES "
        "ALL INTEGRITY CHECKS."
    )

    print(
        "Continue collecting untouched until 30 tokens."
    )


elif passed >= len(checks)-1:

    print()
    print(
        "🟡 T59 DATA IS MOSTLY CLEAN, "
        "BUT ONE INTEGRITY ISSUE NEEDS REVIEW."
    )

    print(
        "Do not alter frozen model."
    )


else:

    print()
    print(
        "🔴 T59 HAS MULTIPLE DATA-INTEGRITY ISSUES."
    )

    print(
        "Do not use prospective performance until resolved."
    )


print()
print("IMPORTANT:")
print("• T72 does NOT evaluate CAP-v2 performance.")
print("• T72 does NOT fit any model.")
print("• T72 does NOT optimize any threshold.")
print("• T72 opens SQLite in read-only mode.")
print("• T59 freeze remains untouched.")
print("• Run T72 anytime while waiting for 30 tokens.")

db.close()
