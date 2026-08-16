import sqlite3
import os
import time
import statistics
import math

DB = "validation_v090.db"


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


def pct(x):
    return "NA" if x is None else f"{x:+.2f}%"


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


def table_exists(db, name):
    r = db.execute("""
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
    """, (name,)).fetchone()
    return r is not None


def event_stats(rows, field):
    vals = [
        r[field]
        for r in rows
        if valid(r[field])
    ]

    if not vals:
        return None

    return {
        "n": len(vals),
        "avg": avg(vals),
        "med": med(vals),
        "win": 100 * sum(x > 0 for x in vals) / len(vals),
        "worst": min(vals),
        "best": max(vals),
    }


def print_horizon(rows, field, label):
    s = event_stats(rows, field)

    if not s:
        print(f"{label:>5} | N=0")
        return

    print(
        f"{label:>5} | "
        f"N={s['n']:>4} | "
        f"AVG={s['avg']:+7.2f}% | "
        f"MED={s['med']:+7.2f}% | "
        f"WIN={s['win']:5.1f}% | "
        f"WORST={s['worst']:+7.2f}% | "
        f"BEST={s['best']:+7.2f}%"
    )


while True:

    try:
        db = connect()

        os.system("clear")

        print("=" * 120)
        print("MEMECOIN LAB — MASTER MONITOR V7.0")
        print("=" * 120)

        # =====================================================
        # GLOBAL DB
        # =====================================================

        events_total = db.execute("""
            SELECT COUNT(*) FROM events
        """).fetchone()[0]

        tokens_total = db.execute("""
            SELECT COUNT(DISTINCT token_mint)
            FROM events
        """).fetchone()[0]

        max_id = db.execute("""
            SELECT COALESCE(MAX(id),0)
            FROM events
        """).fetchone()[0]

        print()
        print("GLOBAL")
        print("-" * 120)

        print(
            f"EVENTS={events_total} | "
            f"TOKENS={tokens_total} | "
            f"MAX_ID={max_id}"
        )

        # =====================================================
        # V2 FROZEN
        # =====================================================

        print()
        print("=" * 120)
        print("V2 FROZEN — FA95 + NEW30>=2 + VOLUME_M5>=8837.925")
        print("=" * 120)

        rows_v2 = db.execute("""
            WITH first_dex AS (
                SELECT d.*
                FROM dex_prices d
                JOIN (
                    SELECT event_id, MIN(timestamp) AS first_time
                    FROM dex_prices
                    GROUP BY event_id
                ) x
                ON d.event_id=x.event_id
                AND d.timestamp=x.first_time
            )

            SELECT
                e.*,
                d.volume_m5

            FROM events e

            LEFT JOIN first_dex d
            ON d.event_id=e.id

            WHERE
                e.fa95=1
                AND e.new_wallets30 >= 2
                AND d.volume_m5 >= 8837.925

            ORDER BY e.id
        """).fetchall()

        print(
            f"EVENTS={len(rows_v2)} | "
            f"TOKENS={len(set(r['token_mint'] for r in rows_v2))}"
        )

        print_horizon(rows_v2, "dex_return_10s", "10s")
        print_horizon(rows_v2, "dex_return_20s", "20s")
        print_horizon(rows_v2, "dex_return_30s", "30s")
        print_horizon(rows_v2, "dex_return_60s", "60s")
        print_horizon(rows_v2, "dex_return_300s", "300s")

        print()
        if len(rows_v2) >= 30:
            print("✅ V2 CHECKPOINT 30 REACHED")
        elif len(rows_v2) >= 20:
            print("🟡 V2 CHECKPOINT 20 REACHED")
        else:
            print(f"⏳ V2 progress: {len(rows_v2)}/30")

        # =====================================================
        # V4 FROZEN PROSPECTIVE
        # =====================================================

        print()
        print("=" * 120)
        print("V4 FROZEN — PROSPECTIVE OOS")
        print("=" * 120)

        if table_exists(db, "v4_frozen_predictions"):

            v4 = db.execute("""
                SELECT *
                FROM v4_frozen_predictions
                ORDER BY event_id
            """).fetchall()

            labeled = [
                r for r in v4
                if r["labeled"] == 1
            ]

            binary = [
                r for r in labeled
                if r["label"] is not None
            ]

            unique_binary = len(set(
                r["token_mint"]
                for r in binary
            ))

            runners = sum(
                r["label"] == 1
                for r in binary
            )

            dumps = sum(
                r["label"] == 0
                for r in binary
            )

            print(
                f"PREDICTIONS={len(v4)} | "
                f"BINARY={len(binary)} | "
                f"TOKENS={unique_binary} | "
                f"RUN={runners} | DUMP={dumps}"
            )

            if binary:
                for t in [.50,.60,.65,.70,.75,.80]:

                    selected = [
                        r for r in binary
                        if r["probability_runner"] >= t
                    ]

                    if not selected:
                        continue

                    rr = sum(
                        r["label"] == 1
                        for r in selected
                    )

                    dd = sum(
                        r["label"] == 0
                        for r in selected
                    )

                    print(
                        f"P>={t:.2f} | "
                        f"N={len(selected):>3} | "
                        f"RUN={rr:>3} | "
                        f"DUMP={dd:>3} | "
                        f"PREC={100*rr/len(selected):5.1f}% | "
                        f"TOK={len(set(r['token_mint'] for r in selected))}"
                    )

            if unique_binary >= 20:
                print("✅ V4 has >=20 unique binary tokens")
            elif unique_binary >= 10:
                print("🟡 V4 has >=10 unique binary tokens")
            else:
                print(f"⏳ V4 unique binary tokens: {unique_binary}/10")

        else:
            print("Table v4_frozen_predictions not found.")

        # =====================================================
        # V6.2 / T20
        # =====================================================

        print()
        print("=" * 120)
        print("V6.2 / T20 — FROZEN REGIME FORWARD")
        print("=" * 120)

        if table_exists(db, "frozen_regime_v620"):

            all_fr = db.execute("""
                SELECT *
                FROM frozen_regime_v620
                ORDER BY event_id
            """).fetchall()

            for reg in [0,1,2]:

                rr = [
                    r for r in all_fr
                    if r["regime"] == reg
                ]

                binary = [
                    r for r in rr
                    if (
                        r["labeled"] == 1
                        and r["label"] is not None
                    )
                ]

                tok = len(set(
                    r["token_mint"]
                    for r in binary
                ))

                run = sum(
                    r["label"] == 1
                    for r in binary
                )

                dump = sum(
                    r["label"] == 0
                    for r in binary
                )

                edge = (
                    100*(run-dump)/len(binary)
                    if binary
                    else None
                )

                print(
                    f"R{reg} | "
                    f"ASSIGNED={len(rr):>3} | "
                    f"BINARY={len(binary):>3} | "
                    f"TOK={tok:>3} | "
                    f"RUN={run:>3} | "
                    f"DUMP={dump:>3} | "
                    f"EDGE={pct(edge)}"
                )

            r1 = [
                r for r in all_fr
                if r["regime"] == 1
            ]

            r1_binary = [
                r for r in r1
                if (
                    r["labeled"] == 1
                    and r["label"] is not None
                )
            ]

            r1_tok = len(set(
                r["token_mint"]
                for r in r1_binary
            ))

            print()
            print(
                f"R1 CHECKPOINT | "
                f"BINARY={len(r1_binary)} | "
                f"TOKENS={r1_tok}"
            )

            if len(r1_binary) >= 50:
                print("✅ R1 checkpoint 50 reached")
            elif len(r1_binary) >= 25:
                print("🟡 R1 checkpoint 25 reached")
            else:
                print(f"⏳ R1 binary: {len(r1_binary)}/25")

            if r1_tok >= 15:
                print("✅ R1 has >=15 unique binary tokens")
            elif r1_tok >= 10:
                print("🟡 R1 has >=10 unique binary tokens")
            else:
                print(f"⏳ R1 unique tokens: {r1_tok}/10")

            # recent_net_sol split
            if len(r1_binary) >= 8:

                vals = sorted([
                    r["recent_net_sol"]
                    for r in r1_binary
                    if valid(r["recent_net_sol"])
                ])

                if vals:

                    cut = med(vals)

                    high = [
                        r for r in r1_binary
                        if (
                            valid(r["recent_net_sol"])
                            and r["recent_net_sol"] >= cut
                        )
                    ]

                    low = [
                        r for r in r1_binary
                        if (
                            valid(r["recent_net_sol"])
                            and r["recent_net_sol"] < cut
                        )
                    ]

                    def edge(part):
                        if not part:
                            return None

                        run = sum(
                            r["label"] == 1
                            for r in part
                        )

                        dump = sum(
                            r["label"] == 0
                            for r in part
                        )

                        return (
                            100*(run-dump)/len(part)
                        )

                    print()
                    print(
                        f"R1 recent_net_sol | CUT={cut:+.4f} | "
                        f"HIGH_EDGE={pct(edge(high))} | "
                        f"LOW_EDGE={pct(edge(low))}"
                    )

        else:
            print("Table frozen_regime_v620 not found.")

        # =====================================================
        # LATEST EVENTS
        # =====================================================

        print()
        print("=" * 120)
        print("LATEST EVENTS")
        print("=" * 120)

        latest = db.execute("""
            SELECT
                id,
                token_mint,
                fa,
                new_wallets30,
                dex_return_60s

            FROM events

            ORDER BY id DESC
            LIMIT 10
        """).fetchall()

        for r in latest:

            ret = (
                f"{r['dex_return_60s']:+7.2f}%"
                if valid(r["dex_return_60s"])
                else "     NA"
            )

            print(
                f"ID={r['id']:>4} | "
                f"FA={r['fa']:+.3f} | "
                f"NEW30={r['new_wallets30']} | "
                f"R60={ret} | "
                f"{r['token_mint'][:18]}"
            )

        print()
        print("=" * 120)
        print("MASTER CHECKPOINT")
        print("=" * 120)

        print(
            "Send me this screen when any of these happens:"
        )

        print(
            "• V2 Frozen >= 30 events"
        )

        print(
            "• V4 >= 10 unique binary tokens"
        )

        print(
            "• R1 >= 25 binary cases"
        )

        print(
            "• R1 >= 10 unique binary tokens"
        )

        print()
        print(
            "Refresh every 15 seconds."
        )

        db.close()

        time.sleep(15)

    except KeyboardInterrupt:
        print("\nMaster monitor stopped.")
        break

    except Exception as e:
        print("ERROR:", repr(e))
        time.sleep(5)
