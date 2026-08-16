#!/usr/bin/env python3

import sqlite3
import math
import statistics
import time

DB = "validation_v090.db"

TABLE = "t100_dex_market_structure_prospective"
META = "t100_dex_market_structure_meta"

REFRESH = 10


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def med(xs):
    xs = [
        x for x in xs
        if valid(x)
    ]

    return (
        statistics.median(xs)
        if xs
        else None
    )


def quantile(xs, q):

    xs = sorted(
        x for x in xs
        if valid(x)
    )

    if not xs:
        return None

    p = (
        (len(xs)-1)
        * q
    )

    lo = int(
        math.floor(p)
    )

    hi = int(
        math.ceil(p)
    )


    if lo == hi:
        return xs[lo]


    w = p-lo

    return (
        xs[lo]*(1-w)
        + xs[hi]*w
    )


def fmt(x, n=3):
    return (
        "NA"
        if x is None
        else f"{x:.{n}f}"
    )


def pct(n, d):
    return (
        "NA"
        if not d
        else f"{100*n/d:.1f}%"
    )


db = sqlite3.connect(
    f"file:{DB}?mode=ro",
    uri=True,
    timeout=30
)

db.row_factory = sqlite3.Row


meta = db.execute(f"""
SELECT *
FROM {META}
WHERE id=1
""").fetchone()


if meta is None:

    raise RuntimeError(
        "T100 meta not found. Start T100A first."
    )


boundary = int(
    meta["boundary_id"]
)

freeze_hash = meta[
    "freeze_hash"
]


try:

    while True:

        rows = db.execute(f"""
        SELECT *
        FROM {TABLE}
        ORDER BY
            event_id,
            target_offset
        """).fetchall()


        print(
            "\033[2J\033[H",
            end=""
        )


        print("=" * 155)

        print(
            "MEMECOIN LAB — T100B DEX MARKET-STRUCTURE INTEGRITY MONITOR"
        )

        print("=" * 155)

        print(
            f"BOUNDARY ID      : {boundary}"
        )

        print(
            f"FREEZE HASH      : {freeze_hash}"
        )

        print()

        events = len({
            r["event_id"]
            for r in rows
        })

        tokens = len({
            r["token_mint"]
            for r in rows
        })


        print(
            f"ROWS             : {len(rows)}"
        )

        print(
            f"EVENTS           : {events}"
        )

        print(
            f"TOKENS           : {tokens}"
        )


        print()
        print("=" * 155)
        print("A) COVERAGE BY OFFSET")
        print("=" * 155)


        for offset in [
            0,
            30,
            60,
            300,
        ]:

            rr = [
                r for r in rows
                if r["target_offset"] == offset
            ]

            matched = sum(
                r["matched"] == 1
                for r in rr
            )

            waiting = sum(
                r["status"] == "WAIT"
                for r in rr
            )

            missing = sum(
                r["status"] == "NO_SNAPSHOT"
                for r in rr
            )


            print(
                f"+{offset:3d}s "
                f"| N={len(rr):4d} "
                f"| MATCHED={matched:4d} "
                f"({pct(matched,len(rr)):>6}) "
                f"| WAIT={waiting:3d} "
                f"| NO_SNAPSHOT={missing:3d}"
            )


        print()
        print("=" * 155)
        print("B) SNAPSHOT AGE")
        print("=" * 155)


        for offset in [
            0,
            30,
            60,
            300,
        ]:

            ages = [
                r["snapshot_age"]
                for r in rows
                if (
                    r["target_offset"] == offset
                    and r["matched"] == 1
                    and valid(
                        r["snapshot_age"]
                    )
                )
            ]

            print(
                f"+{offset:3d}s "
                f"| N={len(ages):4d} "
                f"| MED={fmt(med(ages)):>8} "
                f"| P90={fmt(quantile(ages,.90)):>8} "
                f"| P95={fmt(quantile(ages,.95)):>8} "
                f"| MAX={fmt(max(ages) if ages else None):>8}"
            )


        print()
        print("=" * 155)
        print("C) <=10s QUALITY COVERAGE")
        print("=" * 155)


        for offset in [
            0,
            30,
            60,
            300,
        ]:

            rr = [
                r for r in rows
                if r["target_offset"] == offset
            ]

            good = sum(
                r["matched"] == 1
                and valid(
                    r["snapshot_age"]
                )
                and r["snapshot_age"] <= 10
                for r in rr
            )


            print(
                f"+{offset:3d}s "
                f"| GOOD={good:4d}/{len(rr):4d} "
                f"| RATE={pct(good,len(rr))}"
            )


        print()
        print("=" * 155)
        print("D) FEATURE COMPLETENESS")
        print("=" * 155)


        features = [
            "price_usd",
            "liquidity_usd",
            "market_cap",
            "fdv",
            "volume_m5",
            "buys_m5",
            "sells_m5",
        ]


        matched_rows = [
            r
            for r in rows
            if r["matched"] == 1
        ]


        for f in features:

            n = sum(
                valid(r[f])
                for r in matched_rows
            )

            print(
                f"{f:20} "
                f"| {n:4d}/{len(matched_rows):4d} "
                f"| {pct(n,len(matched_rows))}"
            )


        print()
        print("=" * 155)
        print("E) PAIR / DEX STABILITY")
        print("=" * 155)


        by_event = {}

        for r in rows:

            by_event.setdefault(
                r["event_id"],
                []
            ).append(r)


        complete_events = 0
        pair_switch = 0
        dex_switch = 0


        for event_id, rr in by_event.items():

            matched = [
                r for r in rr
                if r["matched"] == 1
            ]


            offsets = {
                r["target_offset"]
                for r in matched
            }


            if offsets != {
                0,
                30,
                60,
                300,
            }:

                continue


            complete_events += 1


            pairs = {
                r["pair_address"]
                for r in matched
                if r["pair_address"]
            }


            dexs = {
                r["dex_id"]
                for r in matched
                if r["dex_id"]
            }


            if len(pairs) > 1:
                pair_switch += 1


            if len(dexs) > 1:
                dex_switch += 1


        print(
            f"COMPLETE EVENTS   : {complete_events}"
        )

        print(
            f"PAIR SWITCH       : {pair_switch} "
            f"({pct(pair_switch,complete_events)})"
        )

        print(
            f"DEX SWITCH        : {dex_switch} "
            f"({pct(dex_switch,complete_events)})"
        )


        print()
        print("=" * 155)
        print("F) INTEGRITY")
        print("=" * 155)


        boundary_bad = sum(
            r["event_id"] <= boundary
            for r in rows
        )


        hash_bad = sum(
            r["freeze_hash"] != freeze_hash
            for r in rows
        )


        future_bad = sum(
            r["matched"] == 1
            and valid(
                r["dex_timestamp"]
            )
            and r["dex_timestamp"] > r["target_timestamp"]
            for r in rows
        )


        print(
            f"PRE-BOUNDARY ROWS : {boundary_bad}"
        )

        print(
            f"HASH MISMATCH     : {hash_bad}"
        )

        print(
            f"FUTURE SNAPSHOTS  : {future_bad}"
        )


        print()
        print("=" * 155)
        print("G) READINESS")
        print("=" * 155)


        complete_good_events = 0


        for event_id, rr in by_event.items():

            if len(rr) < 4:
                continue


            ok = True


            required = {
                0,
                30,
                60,
                300,
            }


            found = set()


            for r in rr:

                if (
                    r["matched"] == 1
                    and valid(
                        r["snapshot_age"]
                    )
                    and r["snapshot_age"] <= 10
                ):

                    found.add(
                        r["target_offset"]
                    )


            if found == required:

                complete_good_events += 1


        print(
            f"GOOD COMPLETE EVENTS : {complete_good_events}"
        )

        print(
            f"TO 30                : "
            f"{complete_good_events}/30"
        )

        print(
            f"TO 50                : "
            f"{complete_good_events}/50"
        )

        print(
            f"TO 100               : "
            f"{complete_good_events}/100"
        )


        if complete_good_events >= 100:

            print()
            print(
                "🟢 T100 READY FOR FIRST SERIOUS MARKET-STRUCTURE DISCOVERY."
            )

        elif complete_good_events >= 50:

            print()
            print(
                "🟡 T100 DESCRIPTIVE CHECKPOINT REACHED."
            )

        elif complete_good_events >= 30:

            print()
            print(
                "🔵 T100 INTEGRITY CHECKPOINT REACHED."
            )

        else:

            print()
            print(
                "🔵 T100 COLLECTING."
            )


        print()
        print(
            f"Refresh every {REFRESH}s."
        )

        print(
            "CTRL+C stops monitor only."
        )


        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "T100B stopped safely."
    )


finally:

    db.close()
