import sqlite3
import statistics
import math
import os
import time

DB = "validation_v090.db"
OOS_START_ID = 162

H = [5, 10, 20, 30, 60, 300]

NEW30_MIN = 2
VOLUME_M5_MIN = 8837.925


def percentile(vals, p):
    vals = sorted(
        x for x in vals
        if x is not None and math.isfinite(x)
    )

    if not vals:
        return None

    k = (len(vals)-1) * p
    lo = math.floor(k)
    hi = math.ceil(k)

    if lo == hi:
        return vals[lo]

    return (
        vals[lo] * (hi-k)
        + vals[hi] * (k-lo)
    )


def stats(vals):
    vals = [
        x for x in vals
        if x is not None and math.isfinite(x)
    ]

    if not vals:
        return None

    return {
        "n": len(vals),
        "avg": statistics.mean(vals),
        "med": statistics.median(vals),
        "win": 100 * sum(x > 0 for x in vals) / len(vals),
        "p10": percentile(vals, .10),
        "p90": percentile(vals, .90),
        "worst": min(vals),
        "best": max(vals),
    }


def safe(row, key):
    try:
        return row[key]
    except Exception:
        return None


def load():

    db = sqlite3.connect(
        DB,
        timeout=30
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")

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

            ON d.event_id = x.event_id
            AND d.timestamp = x.first_time
        )

        SELECT
            e.*,

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,
            d.buys_m5,
            d.sells_m5

        FROM events e

        LEFT JOIN first_dex d
        ON d.event_id = e.id

        WHERE e.dex_return_60s IS NOT NULL

        ORDER BY e.id
    """).fetchall()

    db.close()

    return rows


def groups(rows):

    return {
        "ALL EVENTS":
            list(rows),

        "FA95":
            [
                r for r in rows
                if safe(r, "fa95") == 1
            ],

        "FA95 + NEW30>=2":
            [
                r for r in rows
                if (
                    safe(r, "fa95") == 1
                    and safe(r, "new_wallets30") is not None
                    and safe(r, "new_wallets30") >= NEW30_MIN
                )
            ],

        "FA95 + VOLUME":
            [
                r for r in rows
                if (
                    safe(r, "fa95") == 1
                    and safe(r, "volume_m5") is not None
                    and safe(r, "volume_m5") >= VOLUME_M5_MIN
                )
            ],

        "V2 FROZEN":
            [
                r for r in rows
                if (
                    safe(r, "fa95") == 1
                    and safe(r, "new_wallets30") is not None
                    and safe(r, "new_wallets30") >= NEW30_MIN
                    and safe(r, "volume_m5") is not None
                    and safe(r, "volume_m5") >= VOLUME_M5_MIN
                )
            ],
    }


def horizon_table(name, rows):

    print()
    print(name)
    print("-" * 110)

    print(
        f"EVENTS={len(rows)} | "
        f"TOKENS={len(set(safe(r,'token_mint') for r in rows if safe(r,'token_mint')))}"
    )

    best_h = None
    best_med = -999

    for h in H:

        vals = [
            safe(r, f"dex_return_{h}s")
            for r in rows
            if safe(r, f"dex_return_{h}s") is not None
        ]

        s = stats(vals)

        if not s:
            print(
                f"{h:>3}s | N=0"
            )
            continue

        if s["med"] > best_med:
            best_med = s["med"]
            best_h = h

        print(
            f"{h:>3}s | "
            f"N={s['n']:>4} | "
            f"AVG={s['avg']:+8.2f}% | "
            f"MED={s['med']:+8.2f}% | "
            f"WIN={s['win']:5.1f}% | "
            f"P10={s['p10']:+8.2f}% | "
            f"P90={s['p90']:+8.2f}%"
        )

    print(
        f"BEST MEDIAN HORIZON : "
        f"{best_h}s ({best_med:+.2f}%)"
        if best_h is not None
        else
        "BEST MEDIAN HORIZON : NA"
    )


def deterioration(name, rows):

    print()
    print(name)
    print("-" * 110)

    usable20 = [
        r for r in rows
        if (
            safe(r, "dex_return_20s") is not None
            and safe(r, "dex_return_60s") is not None
        )
    ]

    usable30 = [
        r for r in rows
        if (
            safe(r, "dex_return_30s") is not None
            and safe(r, "dex_return_60s") is not None
        )
    ]

    positive20 = [
        r for r in usable20
        if safe(r, "dex_return_20s") > 0
    ]

    flip20 = [
        r for r in positive20
        if safe(r, "dex_return_60s") <= 0
    ]

    positive30 = [
        r for r in usable30
        if safe(r, "dex_return_30s") > 0
    ]

    flip30 = [
        r for r in positive30
        if safe(r, "dex_return_60s") <= 0
    ]

    d20 = [
        safe(r, "dex_return_60s")
        - safe(r, "dex_return_20s")
        for r in usable20
    ]

    d30 = [
        safe(r, "dex_return_60s")
        - safe(r, "dex_return_30s")
        for r in usable30
    ]

    print(
        f"20s POSITIVE -> 60s NON-POSITIVE : "
        f"{len(flip20)}/{len(positive20)}"
        + (
            f" ({100*len(flip20)/len(positive20):.1f}%)"
            if positive20 else ""
        )
    )

    print(
        f"30s POSITIVE -> 60s NON-POSITIVE : "
        f"{len(flip30)}/{len(positive30)}"
        + (
            f" ({100*len(flip30)/len(positive30):.1f}%)"
            if positive30 else ""
        )
    )

    if d20:
        print(
            f"MEDIAN CHANGE 20->60 : "
            f"{statistics.median(d20):+.2f}%"
        )

    if d30:
        print(
            f"MEDIAN CHANGE 30->60 : "
            f"{statistics.median(d30):+.2f}%"
        )


def path_patterns(name, rows):

    print()
    print(name)
    print("-" * 110)

    patterns = {
        "EARLY_POP":
            lambda r:
                safe(r, "dex_return_10s") is not None
                and safe(r, "dex_return_10s") > 0
                and safe(r, "dex_return_30s") is not None
                and safe(r, "dex_return_30s") <= safe(r, "dex_return_10s"),

        "LATE_BUILD":
            lambda r:
                safe(r, "dex_return_10s") is not None
                and safe(r, "dex_return_30s") is not None
                and safe(r, "dex_return_60s") is not None
                and safe(r, "dex_return_30s") > safe(r, "dex_return_10s")
                and safe(r, "dex_return_60s") >= safe(r, "dex_return_30s"),

        "PEAK_30":
            lambda r:
                safe(r, "dex_return_30s") is not None
                and safe(r, "dex_return_60s") is not None
                and safe(r, "dex_return_30s") > 0
                and safe(r, "dex_return_60s") < safe(r, "dex_return_30s"),

        "DUMP":
            lambda r:
                safe(r, "dex_return_30s") is not None
                and safe(r, "dex_return_60s") is not None
                and safe(r, "dex_return_30s") <= 0
                and safe(r, "dex_return_60s") <= 0,
    }

    for pname, fn in patterns.items():

        subset = []

        for r in rows:
            try:
                if fn(r):
                    subset.append(r)
            except Exception:
                pass

        print(
            f"{pname:12} : "
            f"{len(subset):>4}/{len(rows):<4}"
            + (
                f" ({100*len(subset)/len(rows):5.1f}%)"
                if rows else ""
            )
        )


while True:

    try:

        rows = load()

        discovery = rows

        oos = [
            r for r in rows
            if safe(r, "id") > OOS_START_ID
        ]

        os.system("clear")

        print("=" * 110)
        print("MEMECOIN LAB — TRAJECTORY ANALYZER V1.8")
        print("=" * 110)

        print(
            f"ALL USABLE : {len(discovery)}"
            f" | OOS ID>{OOS_START_ID}: {len(oos)}"
        )

        print()

        print("=" * 110)
        print("DISCOVERY — HISTORICAL")
        print("=" * 110)

        for name, subset in groups(discovery).items():
            horizon_table(
                name,
                subset
            )

        print()
        print("=" * 110)
        print("DISCOVERY — DETERIORATION")
        print("=" * 110)

        for name, subset in groups(discovery).items():
            deterioration(
                name,
                subset
            )

        print()
        print("=" * 110)
        print("OOS — ID > 162")
        print("=" * 110)

        for name, subset in groups(oos).items():
            horizon_table(
                name,
                subset
            )

        print()
        print("=" * 110)
        print("OOS — DETERIORATION")
        print("=" * 110)

        for name, subset in groups(oos).items():
            deterioration(
                name,
                subset
            )

        print()
        print("=" * 110)
        print("OOS — PATH PATTERNS")
        print("=" * 110)

        for name, subset in groups(oos).items():
            path_patterns(
                name,
                subset
            )

        print()
        print("=" * 110)
        print("IMPORTANT")
        print("=" * 110)

        print(
            "Ne modifie aucun seuil live avec cette analyse."
        )

        print(
            "On cherche seulement à identifier le meilleur horizon de sortie."
        )

        print(
            "Refresh toutes les 20 secondes."
        )

        time.sleep(20)

    except KeyboardInterrupt:

        print(
            "\nTrajectory analyzer stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)
