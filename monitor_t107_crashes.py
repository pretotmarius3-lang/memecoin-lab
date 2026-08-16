import sqlite3
import time
import os

DB = os.path.expanduser("~/memecoin_lab/validation_v090.db")

while True:
    os.system("clear")

    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row

    start = db.execute("""
        SELECT MIN(received_at)
        FROM t107_targeted_pumpswap_signatures
    """).fetchone()[0]

    print("=" * 115)
    print("MEMECOIN LAB — POST-T107 CRASH MONITOR")
    print("=" * 115)

    if start is None:
        print("\n❌ T107 n'a encore aucune donnée.")
        db.close()
        time.sleep(10)
        continue

    # --------------------------------------------------------
    # T107 HEALTH — last 120 seconds
    # --------------------------------------------------------

    health = db.execute("""
        SELECT
            COUNT(*) AS received,
            SUM(status='DONE') AS done,
            SUM(status='WAITING') AS waiting,
            SUM(status='NOT_SWAP') AS not_swap,
            COUNT(DISTINCT token_mint) AS tokens
        FROM t107_targeted_pumpswap_signatures
        WHERE received_at >= strftime('%s','now') - 120
    """).fetchone()

    print()
    print(
        f"T107 LAST 120s | "
        f"RX={health['received'] or 0} | "
        f"DONE={health['done'] or 0} | "
        f"WAIT={health['waiting'] or 0} | "
        f"NOT_SWAP={health['not_swap'] or 0} | "
        f"TOKENS={health['tokens'] or 0}"
    )

    # --------------------------------------------------------
    # POST-T107 CRASHES
    # --------------------------------------------------------

    rows = db.execute("""
        SELECT
            f.token_mint,
            f.crash_timestamp,
            ROUND(f.crash_timestamp - ?, 1) AS after_t107,

            f.holders_at_crash,

            f.pre60_done,
            f.pre60_swaps,

            f.post30_done,
            f.post30_swaps,

            f.post30_60_done,
            f.post30_60_swaps,

            f.post60_300_done,
            f.post60_300_swaps,

            f.outcome_recovery50_300,
            f.outcome_reclaim_peak_300

        FROM t106_resurrection_features f

        WHERE f.crash_timestamp > ?

        ORDER BY f.crash_timestamp DESC
    """, (start, start)).fetchall()

    print()
    print("=" * 115)

    if not rows:

        print("🟡 WAITING FOR FIRST POST-T107 CRASH")
        print()
        print("Aucun RUN → CRASH couvert prospectivement pour l'instant.")

    else:

        print(
            f"🔥 POST-T107 CRASHES : {len(rows)}"
        )
        print("=" * 115)

        for r in rows:

            mint = r["token_mint"]

            actual = db.execute("""
                SELECT
                    COUNT(*) AS n,
                    SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) AS buys,
                    SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) AS sells,
                    SUM(
                        CASE WHEN side='BUY'
                        THEN ABS(sol_delta)
                        ELSE 0 END
                    ) AS buy_sol,
                    SUM(
                        CASE WHEN side='SELL'
                        THEN ABS(sol_delta)
                        ELSE 0 END
                    ) AS sell_sol
                FROM swaps
                WHERE token_mint=?
                  AND program='PUMPSWAP'
                  AND timestamp >= ?
                  AND timestamp < ?
            """, (
                mint,
                r["crash_timestamp"] - 60,
                r["crash_timestamp"] + 300
            )).fetchone()

            rec = r["outcome_recovery50_300"]
            peak = r["outcome_reclaim_peak_300"]

            if rec == 1:
                state = "🟢 RECOVERED"
            elif rec == 0:
                state = "🔴 NO RECOVERY"
            else:
                state = "🟠 PENDING"

            print()
            print(
                f"{mint[:20]}... | "
                f"H={r['holders_at_crash']} | "
                f"+{r['after_t107']:.0f}s after T107 | "
                f"{state}"
            )

            print(
                f"   PRE60      "
                f"DONE={r['pre60_done']} "
                f"SWAPS={r['pre60_swaps']}"
            )

            print(
                f"   POST30     "
                f"DONE={r['post30_done']} "
                f"SWAPS={r['post30_swaps']}"
            )

            print(
                f"   POST30-60  "
                f"DONE={r['post30_60_done']} "
                f"SWAPS={r['post30_60_swaps']}"
            )

            print(
                f"   POST60-300 "
                f"DONE={r['post60_300_done']} "
                f"SWAPS={r['post60_300_swaps']}"
            )

            print(
                f"   FLOW -60/+300 "
                f"N={actual['n'] or 0} | "
                f"B={actual['buys'] or 0} "
                f"S={actual['sells'] or 0} | "
                f"BUY_SOL={actual['buy_sol'] or 0:.3f} "
                f"SELL_SOL={actual['sell_sol'] or 0:.3f}"
            )

            print(
                f"   REC300={rec} | "
                f"RECLAIM300={peak}"
            )

    db.close()

    print()
    print("=" * 115)
    print("Refresh every 10s | CTRL+C stops monitor only")

    time.sleep(10)
