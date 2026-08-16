import sqlite3
import statistics
import time
import os

DB = "validation_v090.db"

while True:

    try:
        db = sqlite3.connect(DB)
        db.row_factory = sqlite3.Row

        rows = db.execute("""
            SELECT *
            FROM events
            WHERE fa95 = 1
            ORDER BY timestamp
        """).fetchall()

        total = len(rows)

        tokens = len(set(
            r["token_mint"]
            for r in rows
        ))

        os.system("clear")

        print("=" * 78)
        print("MEMECOIN LAB — FA95 OOS VALIDATION")
        print("=" * 78)

        print(
            f"FA95 EVENTS : {total}/100"
        )

        print(
            f"TOKENS      : {tokens}"
        )

        print(
            f"PROGRESS    : "
            f"{min(total,100):3d}%"
        )

        print("-" * 78)

        for h in [5,10,20,30,60,300]:

            col = f"dex_return_{h}s"

            vals = [
                r[col]
                for r in rows
                if r[col] is not None
            ]

            if not vals:
                print(
                    f"{h:>3}s | N=0"
                )
                continue

            avg = statistics.mean(vals)
            med = statistics.median(vals)

            win = (
                100
                * sum(x > 0 for x in vals)
                / len(vals)
            )

            print(
                f"{h:>3}s | "
                f"N={len(vals):>4} | "
                f"AVG={avg:+8.2f}% | "
                f"MED={med:+8.2f}% | "
                f"WIN={win:5.1f}%"
            )

        print("-" * 78)

        r60 = [
            r["dex_return_60s"]
            for r in rows
            if r["dex_return_60s"]
            is not None
        ]

        if r60:

            losses = [
                x for x in r60
                if x < 0
            ]

            big_losses = [
                x for x in r60
                if x <= -20
            ]

            print(
                f"60s LOSSES  : "
                f"{len(losses)}/{len(r60)}"
            )

            print(
                f"60s <= -20% : "
                f"{len(big_losses)}"
            )

            print(
                f"WORST 60s   : "
                f"{min(r60):+.2f}%"
            )

            print(
                f"BEST 60s    : "
                f"{max(r60):+.2f}%"
            )

        print("-" * 78)

        if total < 30:
            status = "COLLECT — échantillon encore petit"

        elif total < 100:
            status = "COLLECT — validation en cours"

        else:
            status = "CHECKPOINT 100 ATTEINT — STOP OPTIMISATION"

        print(status)

        print()
        print(
            "Ne modifie PAS FA95 pendant cette collecte."
        )

        db.close()

        time.sleep(10)

    except KeyboardInterrupt:
        print("\nMonitor stopped.")
        break

    except Exception as e:
        print("ERROR:", e)
        time.sleep(5)
