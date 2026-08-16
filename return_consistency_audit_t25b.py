import sqlite3
import statistics
import math

DB = "validation_v090.db"

MAX_ID = 545
VOLUME_CUT = 8837.925

HORIZONS = [30, 60, 300]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def corr(xs, ys):
    pairs = [
        (x, y)
        for x, y in zip(xs, ys)
        if valid(x) and valid(y)
    ]

    if len(pairs) < 3:
        return None

    xa = [x for x, _ in pairs]
    ya = [y for _, y in pairs]

    mx = avg(xa)
    my = avg(ya)

    num = sum(
        (x-mx)*(y-my)
        for x,y in pairs
    )

    denx = sum(
        (x-mx)**2
        for x in xa
    )

    deny = sum(
        (y-my)**2
        for y in ya
    )

    if denx <= 0 or deny <= 0:
        return None

    return num / math.sqrt(denx*deny)


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# FIRST V2 SIGNAL PER TOKEN
# ============================================================

rows = db.execute("""
WITH first_dex AS (

    SELECT d.*

    FROM dex_prices d

    JOIN (
        SELECT
            event_id,
            MIN(timestamp) AS first_time

        FROM dex_prices
        GROUP BY event_id
    ) x

      ON d.event_id=x.event_id
     AND d.timestamp=x.first_time
)

SELECT
    e.id,
    e.timestamp AS event_timestamp,
    e.token_mint,

    e.dex_return_30s,
    e.dex_delay_30s,

    e.dex_return_60s,
    e.dex_delay_60s,

    e.dex_return_300s,
    e.dex_delay_300s,

    d.timestamp AS first_dex_timestamp,
    d.price_usd AS first_dex_price,

    d.volume_m5

FROM events e

JOIN first_dex d
ON d.event_id=e.id

WHERE
    e.id <= ?
    AND e.fa95=1
    AND e.new_wallets30 >= 2
    AND d.volume_m5 >= ?

ORDER BY e.id
""", (
    MAX_ID,
    VOLUME_CUT
)).fetchall()


signals = []
seen = set()

for r in rows:

    if r["token_mint"] in seen:
        continue

    seen.add(
        r["token_mint"]
    )

    signals.append(r)


# ============================================================
# PATH RETURN
# ============================================================

def path_snapshot(signal, horizon):

    start = signal["first_dex_timestamp"]
    target = start + horizon

    before = db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND price_usd IS NOT NULL
            AND price_usd > 0
            AND timestamp <= ?

        ORDER BY timestamp DESC
        LIMIT 1
    """, (
        signal["id"],
        target
    )).fetchone()

    after = db.execute("""
        SELECT
            timestamp,
            price_usd

        FROM dex_prices

        WHERE
            event_id=?
            AND price_usd IS NOT NULL
            AND price_usd > 0
            AND timestamp >= ?

        ORDER BY timestamp ASC
        LIMIT 1
    """, (
        signal["id"],
        target
    )).fetchone()

    candidates = []

    if before:
        candidates.append(
            before
        )

    if after:
        candidates.append(
            after
        )

    if not candidates:
        return None

    # nearest actual snapshot to target horizon
    snap = min(
        candidates,
        key=lambda x:
            abs(x["timestamp"] - target)
    )

    entry = signal[
        "first_dex_price"
    ]

    price = snap[
        "price_usd"
    ]

    if (
        not valid(entry)
        or not valid(price)
        or entry <= 0
    ):
        return None

    ret = (
        price/entry - 1
    ) * 100

    return {
        "return": ret,

        "actual_delay":
            snap["timestamp"]
            - start,

        "target_error":
            snap["timestamp"]
            - target,

        "snapshot_time":
            snap["timestamp"],

        "snapshot_price":
            price,
    }


# ============================================================
# AUDIT
# ============================================================

records = []

for s in signals:

    row = {
        "id": s["id"],
        "token": s["token_mint"],
    }

    for h in HORIZONS:

        official_return = s[
            f"dex_return_{h}s"
        ]

        official_delay = s[
            f"dex_delay_{h}s"
        ]

        path = path_snapshot(
            s,
            h
        )

        row[
            f"official_{h}"
        ] = official_return

        row[
            f"official_delay_{h}"
        ] = official_delay

        if path:

            row[
                f"path_{h}"
            ] = path["return"]

            row[
                f"path_delay_{h}"
            ] = path["actual_delay"]

            row[
                f"path_target_error_{h}"
            ] = path["target_error"]

            if (
                valid(official_return)
                and valid(
                    path["return"]
                )
            ):

                row[
                    f"diff_{h}"
                ] = (
                    path["return"]
                    - official_return
                )

            else:

                row[
                    f"diff_{h}"
                ] = None

        else:

            row[
                f"path_{h}"
            ] = None

            row[
                f"path_delay_{h}"
            ] = None

            row[
                f"path_target_error_{h}"
            ] = None

            row[
                f"diff_{h}"
            ] = None

    records.append(row)


print("="*145)
print(
    "MEMECOIN LAB — "
    "T25B RETURN CONSISTENCY AUDIT"
)
print("="*145)

print(
    f"FIRST V2 SIGNAL / TOKEN: "
    f"{len(records)}"
)

print(
    f"BOUNDARY: ID <= {MAX_ID}"
)

print()


# ============================================================
# SUMMARY
# ============================================================

print("="*145)
print(
    "A) OFFICIAL RETURN vs RECONSTRUCTED PATH RETURN"
)
print("="*145)

print(
    f"{'H':>5}"
    f"{'N':>6}"
    f"{'OFF AVG':>12}"
    f"{'PATH AVG':>12}"
    f"{'AVG Δ':>12}"
    f"{'MED |Δ|':>12}"
    f"{'MAX |Δ|':>12}"
    f"{'CORR':>10}"
)

print("-"*90)

for h in HORIZONS:

    off = [
        r[f"official_{h}"]
        for r in records
        if (
            valid(
                r[f"official_{h}"]
            )
            and valid(
                r[f"path_{h}"]
            )
        )
    ]

    path = [
        r[f"path_{h}"]
        for r in records
        if (
            valid(
                r[f"official_{h}"]
            )
            and valid(
                r[f"path_{h}"]
            )
        )
    ]

    diffs = [
        p-o
        for o,p in zip(
            off,
            path
        )
    ]

    absdiff = [
        abs(x)
        for x in diffs
    ]

    c = corr(
        off,
        path
    )

    print(
        f"{h:>4}s"
        f"{len(off):6d}"
        f"{avg(off):+11.2f}%"
        f"{avg(path):+11.2f}%"
        f"{avg(diffs):+11.2f}"
        f"{med(absdiff):11.2f}"
        f"{max(absdiff):11.2f}"
        f"{(c if c is not None else 0):10.3f}"
    )


# ============================================================
# DELAY CHECK
# ============================================================

print()
print("="*145)
print(
    "B) TIMESTAMP / DELAY CONSISTENCY"
)
print("="*145)

print(
    f"{'H':>5}"
    f"{'N':>6}"
    f"{'OFFICIAL DELAY MED':>22}"
    f"{'PATH DELAY MED':>18}"
    f"{'PATH TARGET ERR MED':>22}"
    f"{'MAX TARGET ERR':>18}"
)

print("-"*100)

for h in HORIZONS:

    official_delays = [
        r[
            f"official_delay_{h}"
        ]
        for r in records
        if valid(
            r[
                f"official_delay_{h}"
            ]
        )
    ]

    path_delays = [
        r[
            f"path_delay_{h}"
        ]
        for r in records
        if valid(
            r[
                f"path_delay_{h}"
            ]
        )
    ]

    errors = [
        r[
            f"path_target_error_{h}"
        ]
        for r in records
        if valid(
            r[
                f"path_target_error_{h}"
            ]
        )
    ]

    print(
        f"{h:>4}s"
        f"{len(path_delays):6d}"
        f"{(med(official_delays) if official_delays else 0):21.2f}s"
        f"{(med(path_delays) if path_delays else 0):17.2f}s"
        f"{(med(errors) if errors else 0):21.2f}s"
        f"{(max(abs(x) for x in errors) if errors else 0):17.2f}s"
    )


# ============================================================
# TOKEN DETAIL
# ============================================================

print()
print("="*145)
print(
    "C) TOKEN DETAIL — 60s"
)
print("="*145)

print(
    f"{'ID':>5} "
    f"{'TOKEN':20} "
    f"{'OFF60':>9} "
    f"{'PATH60':>9} "
    f"{'DIFF':>9} "
    f"{'OFF DEL':>9} "
    f"{'PATH DEL':>9} "
    f"{'ERR':>8}"
)

print("-"*100)

detail = sorted(
    records,
    key=lambda r:
        abs(
            r["diff_60"]
        )
        if valid(
            r["diff_60"]
        )
        else -1,
    reverse=True
)

for r in detail:

    print(
        f"{r['id']:5d} "
        f"{r['token'][:20]:20} "
        f"{(r['official_60'] if valid(r['official_60']) else 0):+8.2f}% "
        f"{(r['path_60'] if valid(r['path_60']) else 0):+8.2f}% "
        f"{(r['diff_60'] if valid(r['diff_60']) else 0):+8.2f} "
        f"{(r['official_delay_60'] if valid(r['official_delay_60']) else 0):8.2f}s "
        f"{(r['path_delay_60'] if valid(r['path_delay_60']) else 0):8.2f}s "
        f"{(r['path_target_error_60'] if valid(r['path_target_error_60']) else 0):+7.2f}s"
    )


# ============================================================
# LARGE DISCREPANCIES
# ============================================================

print()
print("="*145)
print(
    "D) LARGE RETURN DISCREPANCIES"
)
print("="*145)

for h in HORIZONS:

    bad = [
        r for r in records
        if (
            valid(
                r[f"diff_{h}"]
            )
            and abs(
                r[f"diff_{h}"]
            ) >= 5.0
        )
    ]

    print(
        f"{h:>4}s | "
        f"|PATH-OFFICIAL| >= 5 pts : "
        f"{len(bad)}/{len(records)}"
    )


print()
print("="*145)
print("HOW TO INTERPRET")
print("="*145)

print("""
GOOD:
• high correlation
• median absolute difference small
• path target errors only a few seconds
• few >=5 point discrepancies

BAD:
• low correlation
• systematic path-vs-official bias
• large timing errors
• several large discrepancies

If 60s is inconsistent but 120/300 path coverage is clean,
we should use one single path-based price engine for all future
execution simulations instead of mixing event returns and snapshots.
""")

db.close()
