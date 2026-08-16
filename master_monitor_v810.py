import sqlite3
import time
import math
import os
import subprocess
import statistics

DB = "validation_v090.db"
REFRESH = 10


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def pct(a, b):
    if not b:
        return "NA"
    return f"{100*a/b:.1f}%"


def fmt(x, n=2):
    if x is None:
        return "NA"
    if isinstance(x, float):
        return f"{x:.{n}f}"
    return str(x)


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


def table_exists(name):
    r = db.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
          AND name=?
    """, (name,)).fetchone()

    return r is not None


def scalar(sql, args=()):
    try:
        r = db.execute(
            sql,
            args
        ).fetchone()

        return r[0] if r else None

    except Exception:
        return None


def columns(table):
    try:
        return [
            r["name"]
            for r in db.execute(
                f"PRAGMA table_info({table})"
            ).fetchall()
        ]
    except Exception:
        return []


# ============================================================
# PROCESS CHECK
# ============================================================

def python_processes():

    try:

        out = subprocess.check_output(
            [
                "ps",
                "-axo",
                "pid,etime,command"
            ],
            text=True
        )

    except Exception:
        return []

    lines = []

    for line in out.splitlines():

        low = line.lower()

        if (
            "python" in low
            and "grep" not in low
        ):
            lines.append(
                line.strip()
            )

    return lines


def process_has(script_name, lines):

    script_name = script_name.lower()

    return any(
        script_name in line.lower()
        for line in lines
    )


# ============================================================
# DISPLAY
# ============================================================

def show():

    print(
        "\033[2J\033[H",
        end=""
    )

    max_id = scalar(
        """
        SELECT COALESCE(MAX(id),0)
        FROM events
        """
    )

    event_n = scalar(
        """
        SELECT COUNT(*)
        FROM events
        """
    )

    token_n = scalar(
        """
        SELECT COUNT(DISTINCT token_mint)
        FROM events
        """
    )

    processes = python_processes()


    print("=" * 145)
    print(
        "MEMECOIN LAB — MASTER MONITOR V8.1"
    )
    print("=" * 145)

    print()
    print("GLOBAL")
    print("-" * 145)

    print(
        f"EVENTS={event_n} | "
        f"TOKENS={token_n} | "
        f"MAX_ID={max_id}"
    )


    # ========================================================
    # T23
    # ========================================================

    print()
    print("=" * 145)
    print(
        "T23 — V2 FROZEN PROSPECTIVE"
    )
    print("=" * 145)

    if table_exists(
        "v2_frozen_firstsignal_t23"
    ):

        total = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
        """)

        labeled = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE labeled_60=1
        """)

        binary = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE binary_label IS NOT NULL
        """)

        run = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE binary_label=1
        """)

        dump = scalar("""
            SELECT COUNT(*)
            FROM v2_frozen_firstsignal_t23
            WHERE binary_label=0
        """)

        proc = (
            "RUNNING"
            if process_has(
                "v2_frozen_prospective_t23.py",
                processes
            )
            else "NOT SEEN"
        )

        print(
            f"TOKENS={total} | "
            f"LABELED60={labeled} | "
            f"BINARY={binary} | "
            f"RUN={run} | "
            f"DUMP={dump}"
        )

        print(
            f"PROCESS={proc}"
        )

        if total is not None:

            if total < 15:
                print(
                    f"⏳ {total}/15 prospective tokens"
                )

            elif total < 30:
                print(
                    f"🟡 checkpoint 15 reached | "
                    f"{total}/30"
                )

            else:
                print(
                    f"🟢 checkpoint 30 reached | "
                    f"N={total}"
                )

    else:
        print(
            "T23 table not found"
        )


    # ========================================================
    # T31
    # ========================================================

    print()
    print("=" * 145)
    print(
        "T31 — FROZEN BASE PROSPECTIVE EXECUTION"
    )
    print("=" * 145)

    t31 = "v2_frozen_execution_t31"

    if table_exists(t31):

        cols = columns(t31)

        total = scalar(
            f"""
            SELECT COUNT(*)
            FROM {t31}
            """
        )

        done = (
            scalar(
                f"""
                SELECT COUNT(*)
                FROM {t31}
                WHERE status='DONE'
                """
            )
            if "status" in cols
            else None
        )

        live = (
            scalar(
                f"""
                SELECT COUNT(*)
                FROM {t31}
                WHERE status!='DONE'
                """
            )
            if "status" in cols
            else None
        )

        net_col = next(
            (
                c
                for c in [
                    "net",
                    "net_return",
                    "return_net",
                    "net_pct"
                ]
                if c in cols
            ),
            None
        )

        proc = (
            "RUNNING"
            if process_has(
                "t31_frozen_base_prospective_execution.py",
                processes
            )
            else "NOT SEEN"
        )

        print(
            f"TOTAL={total} | "
            f"DONE={done} | "
            f"LIVE/WAIT={live}"
        )

        print(
            f"PROCESS={proc}"
        )

        if net_col:

            vals = [
                r[0]
                for r in db.execute(
                    f"""
                    SELECT {net_col}
                    FROM {t31}
                    WHERE {net_col}
                    IS NOT NULL
                    """
                ).fetchall()
                if valid(
                    r[0]
                )
            ]

            if vals:

                print(
                    f"AVG={statistics.mean(vals):+.2f}% | "
                    f"MED={statistics.median(vals):+.2f}% | "
                    f"WIN={pct(sum(x>0 for x in vals),len(vals))}"
                )

    else:
        print(
            "T31 table not found"
        )


    # ========================================================
    # T32
    # ========================================================

    print()
    print("=" * 145)
    print(
        "T32 — PROSPECTIVE SHADOW RECORDER"
    )
    print("=" * 145)

    t32 = "prospective_shadow_t32"

    if table_exists(t32):

        cols = columns(t32)

        total = scalar(
            f"""
            SELECT COUNT(*)
            FROM {t32}
            """
        )

        proc = (
            "RUNNING"
            if process_has(
                "t32_prospective_shadow_recorder.py",
                processes
            )
            else "NOT SEEN"
        )

        print(
            f"ROWS={total}"
        )

        print(
            f"PROCESS={proc}"
        )

        # Try to show useful status fields dynamically.
        for col in [
            "status",
            "labeled_60",
            "token_mint"
        ]:

            if col in cols:

                if col == "token_mint":

                    uniq = scalar(
                        f"""
                        SELECT COUNT(DISTINCT token_mint)
                        FROM {t32}
                        """
                    )

                    print(
                        f"UNIQUE TOKENS={uniq}"
                    )

                elif col == "labeled_60":

                    labeled = scalar(
                        f"""
                        SELECT COUNT(*)
                        FROM {t32}
                        WHERE labeled_60=1
                        """
                    )

                    print(
                        f"LABELED60={labeled}"
                    )

                elif col == "status":

                    statuses = db.execute(
                        f"""
                        SELECT status, COUNT(*) AS n
                        FROM {t32}
                        GROUP BY status
                        ORDER BY n DESC
                        """
                    ).fetchall()

                    if statuses:

                        print(
                            "STATUS="
                            + ", ".join(
                                f"{r['status']}:{r['n']}"
                                for r in statuses
                            )
                        )

    else:
        print(
            "T32 table not found"
        )


    # ========================================================
    # T47
    # ========================================================

    print()
    print("=" * 145)
    print(
        "T47 — FAST-FLIP PROSPECTIVE SHADOW"
    )
    print("=" * 145)

    t47 = "t47_fastflip_prospective"

    if table_exists(t47):

        total = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
        """)

        labeled = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE labeled_60=1
        """)

        binary = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE binary_label IS NOT NULL
        """)

        run = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE binary_label=1
        """)

        dump = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE binary_label=0
        """)

        wait = scalar("""
            SELECT COUNT(*)
            FROM t47_fastflip_prospective
            WHERE labeled_60=0
        """)

        proc = (
            "RUNNING"
            if process_has(
                "t47_fastflip_prospective_shadow.py",
                processes
            )
            else "NOT SEEN"
        )

        print(
            f"TOKENS={total} | "
            f"LABELED={labeled} | "
            f"BINARY={binary} | "
            f"RUN={run} | "
            f"DUMP={dump} | "
            f"WAIT={wait}"
        )

        print(
            f"PROCESS={proc}"
        )

        rows = db.execute("""
            SELECT
                buyer_fast_mean,
                binary_label
            FROM t47_fastflip_prospective
            WHERE
                binary_label IS NOT NULL
                AND buyer_fast_mean IS NOT NULL
        """).fetchall()

        if rows:

            runs = [
                r["buyer_fast_mean"]
                for r in rows
                if r["binary_label"] == 1
            ]

            dumps = [
                r["buyer_fast_mean"]
                for r in rows
                if r["binary_label"] == 0
            ]

            if runs and dumps:

                print(
                    f"RUN FAST MED="
                    f"{statistics.median(runs):.3f} | "
                    f"DUMP FAST MED="
                    f"{statistics.median(dumps):.3f}"
                )

        if total is not None:

            if total < 15:
                print(
                    f"⏳ {total}/15 prospective tokens"
                )

            elif total < 30:
                print(
                    f"🟡 checkpoint 15 reached | "
                    f"{total}/30"
                )

            else:
                print(
                    f"🟢 checkpoint 30 reached | "
                    f"N={total}"
                )

    else:
        print(
            "T47 table not found"
        )


    # ========================================================
    # V4
    # ========================================================

    print()
    print("=" * 145)
    print(
        "V4 — FROZEN PREDICTIONS"
    )
    print("=" * 145)

    v4 = "v4_frozen_predictions"

    if table_exists(v4):

        cols = columns(v4)

        total = scalar(
            f"""
            SELECT COUNT(*)
            FROM {v4}
            """
        )

        token_col = (
            "token_mint"
            if "token_mint" in cols
            else None
        )

        unique = (
            scalar(
                f"""
                SELECT COUNT(DISTINCT token_mint)
                FROM {v4}
                """
            )
            if token_col
            else None
        )

        print(
            f"PREDICTIONS={total} | "
            f"TOKENS={unique}"
        )

        # Try common columns dynamically.
        prob_col = next(
            (
                c
                for c in [
                    "probability",
                    "prob",
                    "p_run",
                    "pred_prob"
                ]
                if c in cols
            ),
            None
        )

        label_col = next(
            (
                c
                for c in [
                    "binary_label",
                    "label",
                    "outcome"
                ]
                if c in cols
            ),
            None
        )

        if (
            prob_col
            and label_col
        ):

            for threshold in [
                0.50,
                0.60,
                0.70,
                0.80
            ]:

                n = scalar(
                    f"""
                    SELECT COUNT(*)
                    FROM {v4}
                    WHERE {prob_col} >= ?
                    """,
                    (
                        threshold,
                    )
                )

                if n is not None:

                    print(
                        f"P>={threshold:.2f} | "
                        f"N={n}"
                    )

    else:
        print(
            "V4 table not found"
        )


    # ========================================================
    # V6.2
    # ========================================================

    print()
    print("=" * 145)
    print(
        "V6.2 — FROZEN REGIME FORWARD"
    )
    print("=" * 145)

    regime = "frozen_regime_v620"

    if table_exists(regime):

        cols = columns(regime)

        total = scalar(
            f"""
            SELECT COUNT(*)
            FROM {regime}
            """
        )

        print(
            f"ROWS={total}"
        )

        # Dynamic regime column
        regime_col = next(
            (
                c
                for c in [
                    "regime",
                    "regime_id",
                    "cluster",
                    "assigned_regime"
                ]
                if c in cols
            ),
            None
        )

        if regime_col:

            rows = db.execute(
                f"""
                SELECT
                    {regime_col} AS regime,
                    COUNT(*) AS n
                FROM {regime}
                GROUP BY {regime_col}
                ORDER BY regime
                """
            ).fetchall()

            for r in rows:

                print(
                    f"{r['regime']} | "
                    f"N={r['n']}"
                )

        else:

            print(
                "Regime column not auto-detected"
            )

    else:
        print(
            "V6.2 table not found"
        )


    # ========================================================
    # PROCESS STATUS
    # ========================================================

    print()
    print("=" * 145)
    print(
        "PROCESS / TERMINAL STATUS"
    )
    print("=" * 145)

    watched = [
        (
            "event_tracker_v090.py",
            "CORE EVENT TRACKER"
        ),
        (
            "price_tracker_v100.py",
            "CORE PRICE TRACKER"
        ),
        (
            "event_sequence_v340.py",
            "SEQUENCE ENGINE"
        ),
        (
            "frozen_regime_forward_v620.py",
            "REGIME V6.2"
        ),
        (
            "v2_frozen_prospective_t23.py",
            "T23"
        ),
        (
            "t31_frozen_base_prospective_execution.py",
            "T31"
        ),
        (
            "t32_prospective_shadow_recorder.py",
            "T32"
        ),
        (
            "t47_fastflip_prospective_shadow.py",
            "T47"
        ),
    ]


    for script, label in watched:

        state = (
            "✅ RUNNING"
            if process_has(
                script,
                processes
            )
            else "❌ NOT SEEN"
        )

        print(
            f"{label:24} | "
            f"{state:12} | "
            f"{script}"
        )


    # ========================================================
    # LATEST EVENTS
    # ========================================================

    print()
    print("=" * 145)
    print(
        "LATEST EVENTS"
    )
    print("=" * 145)

    recent = db.execute("""
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


    for r in recent:

        r60 = (
            f"{r['dex_return_60s']:+.2f}%"
            if valid(
                r[
                    "dex_return_60s"
                ]
            )
            else "NA"
        )

        print(
            f"ID={r['id']:4d} | "
            f"FA="
            f"{r['fa'] if r['fa'] is not None else 'NA'} | "
            f"NEW30="
            f"{r['new_wallets30'] if r['new_wallets30'] is not None else 'NA'} | "
            f"R60={r60:>9} | "
            f"{r['token_mint'][:24]}"
        )


    print()
    print("=" * 145)
    print(
        "STATUS"
    )
    print("=" * 145)

    print(
        "READ-ONLY MONITOR"
    )

    print(
        "Do not close recorder processes until checked here."
    )

    print(
        f"Refresh every {REFRESH}s."
    )

    print(
        "CTRL+C to stop monitor."
    )


try:

    while True:

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()
    print(
        "MASTER V8.1 stopped safely."
    )


finally:

    db.close()
