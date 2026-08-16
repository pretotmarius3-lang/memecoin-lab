#!/usr/bin/env python3

import sqlite3
import time
import math

DB = "validation_v090.db"

MIGRATIONS = "t101_migrations"
HOLDERS = "t101_migrated_holder_snapshots"

STATE_TABLE = "t103_token_lifecycle_state"
EVENT_TABLE = "t103_token_lifecycle_events"

REFRESH = 15

RUN_RETURN = 100.0
CRASH_DRAWDOWN = -50.0
RECOVERY_RETURN = 50.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def pct(a, b):
    if (
        not valid(a)
        or not valid(b)
        or a <= 0
    ):
        return None

    return 100.0 * (
        b / a - 1.0
    )


def fmt(x, n=2):
    return "NA" if x is None else f"{x:.{n}f}"


db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLES
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (

    token_mint TEXT PRIMARY KEY,

    migration_signature TEXT NOT NULL,
    migrated_at REAL,

    holder_count INTEGER,

    first_price REAL,
    first_price_at REAL,

    current_price REAL,
    current_price_at REAL,

    run_confirmed INTEGER NOT NULL DEFAULT 0,
    run_confirmed_at REAL,
    run_confirmed_price REAL,

    run_peak_price REAL,
    run_peak_at REAL,

    crash_confirmed INTEGER NOT NULL DEFAULT 0,
    crash_confirmed_at REAL,
    crash_confirmed_price REAL,

    post_crash_trough REAL,
    post_crash_trough_at REAL,

    recovery_confirmed INTEGER NOT NULL DEFAULT 0,
    recovery_confirmed_at REAL,
    recovery_confirmed_price REAL,

    second_run_confirmed INTEGER NOT NULL DEFAULT 0,
    second_run_confirmed_at REAL,
    second_run_confirmed_price REAL,

    state TEXT NOT NULL DEFAULT 'MIGRATED',

    last_update_at REAL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {EVENT_TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    token_mint TEXT NOT NULL,

    lifecycle_event TEXT NOT NULL,

    event_timestamp REAL NOT NULL,

    price_usd REAL,

    holder_count INTEGER,

    reference_price REAL,

    move_pct REAL,

    note TEXT
)
""")


db.execute(f"""
CREATE INDEX IF NOT EXISTS
idx_t103_events_mint
ON {EVENT_TABLE}(
    token_mint,
    event_timestamp
)
""")

db.commit()


# ============================================================
# SOURCES
# ============================================================

def migrated_tokens():

    return db.execute(f"""
    SELECT
        signature,
        token_mint,
        COALESCE(
            block_time,
            detected_at
        ) AS migrated_at

    FROM {MIGRATIONS}

    WHERE
        status='OK'
        AND confirmed=1
        AND migrate_v2=1
        AND create_pool=1
        AND token_mint IS NOT NULL

    ORDER BY migrated_at
    """).fetchall()


def latest_holder_count(mint):

    row = db.execute(f"""
    SELECT holder_count

    FROM {HOLDERS}

    WHERE
        token_mint=?
        AND status='OK'
        AND holder_count IS NOT NULL

    ORDER BY checked_at DESC
    LIMIT 1
    """, (
        mint,
    )).fetchone()

    return (
        row["holder_count"]
        if row
        else None
    )


def price_history(
    mint,
    migrated_at
):

    return db.execute("""
    SELECT
        timestamp,
        price_usd,
        market_cap,
        liquidity_usd,
        volume_m5,
        buys_m5,
        sells_m5

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp >= ?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp ASC
    """, (
        mint,
        migrated_at
    )).fetchall()


# ============================================================
# EVENT LOGGING
# ============================================================

def log_event(
    mint,
    lifecycle_event,
    ts,
    price,
    holder_count,
    reference_price=None,
    move_pct=None,
    note=None
):

    db.execute(f"""
    INSERT INTO {EVENT_TABLE} (
        token_mint,
        lifecycle_event,
        event_timestamp,
        price_usd,
        holder_count,
        reference_price,
        move_pct,
        note
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?
    )
    """, (
        mint,
        lifecycle_event,
        ts,
        price,
        holder_count,
        reference_price,
        move_pct,
        note
    ))

    db.commit()


# ============================================================
# STATE
# ============================================================

def get_state(mint):

    return db.execute(f"""
    SELECT *
    FROM {STATE_TABLE}
    WHERE token_mint=?
    """, (
        mint,
    )).fetchone()


def create_state(
    migration,
    first,
    holder_count
):

    mint = migration[
        "token_mint"
    ]

    db.execute(f"""
    INSERT OR IGNORE INTO {STATE_TABLE} (

        token_mint,
        migration_signature,
        migrated_at,

        holder_count,

        first_price,
        first_price_at,

        current_price,
        current_price_at,

        state,
        last_update_at
    )

    VALUES (
        ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        'MIGRATED',
        ?
    )
    """, (

        mint,
        migration["signature"],
        migration["migrated_at"],

        holder_count,

        first["price_usd"],
        first["timestamp"],

        first["price_usd"],
        first["timestamp"],

        time.time()
    ))

    db.commit()


# ============================================================
# PROCESS TOKEN
# ============================================================

def process_token(
    migration
):

    mint = migration[
        "token_mint"
    ]

    history = price_history(
        mint,
        migration["migrated_at"]
    )

    if not history:
        return

    holders = latest_holder_count(
        mint
    )

    state = get_state(
        mint
    )

    if state is None:

        create_state(
            migration,
            history[0],
            holders
        )

        state = get_state(
            mint
        )

        log_event(
            mint,
            "MIGRATED_TRACKING_STARTED",
            history[0]["timestamp"],
            history[0]["price_usd"],
            holders,
            note="First usable post-migration DEX price"
        )


    first_price = state[
        "first_price"
    ]


    # ========================================================
    # PROCESS ONLY OBSERVATIONS AFTER LAST STATE UPDATE PRICE
    # ========================================================

    current_state = dict(
        state
    )


    for row in history:

        ts = row[
            "timestamp"
        ]

        price = row[
            "price_usd"
        ]


        # ----------------------------------------------------
        # CURRENT
        # ----------------------------------------------------

        current_state[
            "current_price"
        ] = price

        current_state[
            "current_price_at"
        ] = ts


        # ----------------------------------------------------
        # BEFORE RUN
        # ----------------------------------------------------

        if not current_state[
            "run_confirmed"
        ]:

            move = pct(
                first_price,
                price
            )

            if (
                move is not None
                and move >= RUN_RETURN
            ):

                current_state[
                    "run_confirmed"
                ] = 1

                current_state[
                    "run_confirmed_at"
                ] = ts

                current_state[
                    "run_confirmed_price"
                ] = price

                current_state[
                    "run_peak_price"
                ] = price

                current_state[
                    "run_peak_at"
                ] = ts

                current_state[
                    "state"
                ] = "RUNNING"

                log_event(
                    mint,
                    "RUN_CONFIRMED",
                    ts,
                    price,
                    holders,
                    reference_price=first_price,
                    move_pct=move,
                    note="Price reached frozen +100% research label"
                )

            continue


        # ----------------------------------------------------
        # RUNNING, BEFORE CRASH
        # ----------------------------------------------------

        if (
            current_state[
                "run_confirmed"
            ]
            and not current_state[
                "crash_confirmed"
            ]
        ):

            peak = current_state[
                "run_peak_price"
            ]

            if (
                not valid(peak)
                or price > peak
            ):

                current_state[
                    "run_peak_price"
                ] = price

                current_state[
                    "run_peak_at"
                ] = ts

                peak = price


            dd = pct(
                peak,
                price
            )


            if (
                dd is not None
                and dd <= CRASH_DRAWDOWN
            ):

                current_state[
                    "crash_confirmed"
                ] = 1

                current_state[
                    "crash_confirmed_at"
                ] = ts

                current_state[
                    "crash_confirmed_price"
                ] = price

                current_state[
                    "post_crash_trough"
                ] = price

                current_state[
                    "post_crash_trough_at"
                ] = ts

                current_state[
                    "state"
                ] = "CRASH_WATCH"

                log_event(
                    mint,
                    "CRASH_CONFIRMED",
                    ts,
                    price,
                    holders,
                    reference_price=peak,
                    move_pct=dd,
                    note="Frozen -50% drawdown from run peak"
                )

            continue


        # ----------------------------------------------------
        # AFTER CRASH
        # ----------------------------------------------------

        if current_state[
            "crash_confirmed"
        ]:

            trough = current_state[
                "post_crash_trough"
            ]


            if (
                not valid(trough)
                or price < trough
            ):

                current_state[
                    "post_crash_trough"
                ] = price

                current_state[
                    "post_crash_trough_at"
                ] = ts

                trough = price


            recovery = pct(
                trough,
                price
            )


            if (
                not current_state[
                    "recovery_confirmed"
                ]
                and recovery is not None
                and recovery >= RECOVERY_RETURN
            ):

                current_state[
                    "recovery_confirmed"
                ] = 1

                current_state[
                    "recovery_confirmed_at"
                ] = ts

                current_state[
                    "recovery_confirmed_price"
                ] = price

                current_state[
                    "state"
                ] = "RECOVERY"

                log_event(
                    mint,
                    "RECOVERY_CONFIRMED",
                    ts,
                    price,
                    holders,
                    reference_price=trough,
                    move_pct=recovery,
                    note="Frozen +50% recovery from post-crash trough"
                )


            pre_crash_peak = current_state[
                "run_peak_price"
            ]


            if (
                not current_state[
                    "second_run_confirmed"
                ]
                and valid(
                    pre_crash_peak
                )
                and price > pre_crash_peak
            ):

                second_move = pct(
                    pre_crash_peak,
                    price
                )

                current_state[
                    "second_run_confirmed"
                ] = 1

                current_state[
                    "second_run_confirmed_at"
                ] = ts

                current_state[
                    "second_run_confirmed_price"
                ] = price

                current_state[
                    "state"
                ] = "SECOND_RUN"

                log_event(
                    mint,
                    "SECOND_RUN_CONFIRMED",
                    ts,
                    price,
                    holders,
                    reference_price=pre_crash_peak,
                    move_pct=second_move,
                    note="Price exceeded pre-crash run peak"
                )


    # ========================================================
    # SAVE STATE
    # ========================================================

    db.execute(f"""
    UPDATE {STATE_TABLE}

    SET
        holder_count=?,

        current_price=?,
        current_price_at=?,

        run_confirmed=?,
        run_confirmed_at=?,
        run_confirmed_price=?,

        run_peak_price=?,
        run_peak_at=?,

        crash_confirmed=?,
        crash_confirmed_at=?,
        crash_confirmed_price=?,

        post_crash_trough=?,
        post_crash_trough_at=?,

        recovery_confirmed=?,
        recovery_confirmed_at=?,
        recovery_confirmed_price=?,

        second_run_confirmed=?,
        second_run_confirmed_at=?,
        second_run_confirmed_price=?,

        state=?,
        last_update_at=?

    WHERE token_mint=?
    """, (

        holders,

        current_state[
            "current_price"
        ],

        current_state[
            "current_price_at"
        ],

        current_state[
            "run_confirmed"
        ],

        current_state[
            "run_confirmed_at"
        ],

        current_state[
            "run_confirmed_price"
        ],

        current_state[
            "run_peak_price"
        ],

        current_state[
            "run_peak_at"
        ],

        current_state[
            "crash_confirmed"
        ],

        current_state[
            "crash_confirmed_at"
        ],

        current_state[
            "crash_confirmed_price"
        ],

        current_state[
            "post_crash_trough"
        ],

        current_state[
            "post_crash_trough_at"
        ],

        current_state[
            "recovery_confirmed"
        ],

        current_state[
            "recovery_confirmed_at"
        ],

        current_state[
            "recovery_confirmed_price"
        ],

        current_state[
            "second_run_confirmed"
        ],

        current_state[
            "second_run_confirmed_at"
        ],

        current_state[
            "second_run_confirmed_price"
        ],

        current_state[
            "state"
        ],

        time.time(),

        mint
    ))

    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    rows = db.execute(f"""
    SELECT *
    FROM {STATE_TABLE}

    ORDER BY

        CASE state

            WHEN 'SECOND_RUN'
                THEN 1

            WHEN 'RECOVERY'
                THEN 2

            WHEN 'CRASH_WATCH'
                THEN 3

            WHEN 'RUNNING'
                THEN 4

            WHEN 'MIGRATED'
                THEN 5

            ELSE 6

        END,

        holder_count DESC
    """).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )

    print("=" * 175)

    print(
        "MEMECOIN LAB — T103 MIGRATED TOKEN LIFECYCLE RECORDER"
    )

    print("=" * 175)


    print(
        f"TOKENS              : {len(rows)}"
    )

    print(
        f">=50 HOLDERS        : "
        f"{sum((r['holder_count'] or 0)>=50 for r in rows)}"
    )

    print(
        f"RUN CONFIRMED       : "
        f"{sum(r['run_confirmed'] for r in rows)}"
    )

    print(
        f"CRASH CONFIRMED     : "
        f"{sum(r['crash_confirmed'] for r in rows)}"
    )

    print(
        f"RECOVERY CONFIRMED  : "
        f"{sum(r['recovery_confirmed'] for r in rows)}"
    )

    print(
        f"SECOND RUN          : "
        f"{sum(r['second_run_confirmed'] for r in rows)}"
    )


    print()

    print(
        "FROZEN LABELS       : "
        "RUN +100% | CRASH -50% | RECOVERY +50% | SECOND RUN > PRE-CRASH PEAK"
    )

    print(
        ">=50 FILTER         : NOT ACTIVE"
    )


    print()

    print("=" * 175)
    print("LIFECYCLE")
    print("=" * 175)


    for r in rows[:30]:

        current = r[
            "current_price"
        ]

        first = r[
            "first_price"
        ]

        peak = r[
            "run_peak_price"
        ]

        trough = r[
            "post_crash_trough"
        ]


        from0 = pct(
            first,
            current
        )


        dd = (
            pct(
                peak,
                current
            )
            if valid(peak)
            else None
        )


        rec = (
            pct(
                trough,
                current
            )
            if valid(trough)
            else None
        )


        print(
            f"{r['token_mint'][:18]:18} "
            f"| H={str(r['holder_count']):>5} "
            f"| STATE={r['state']:12} "
            f"| FROM0={fmt(from0):>8}% "
            f"| DD={fmt(dd):>8}% "
            f"| REC={fmt(rec):>8}%"
        )


    print()

    recent = db.execute(f"""
    SELECT *
    FROM {EVENT_TABLE}

    ORDER BY
        event_timestamp DESC,
        id DESC

    LIMIT 15
    """).fetchall()


    print("=" * 175)
    print("LATEST LIFECYCLE EVENTS")
    print("=" * 175)


    for r in recent:

        print(
            f"{r['lifecycle_event']:24} "
            f"| {r['token_mint'][:18]:18} "
            f"| H={str(r['holder_count']):>5} "
            f"| MOVE={fmt(r['move_pct']):>8}% "
            f"| PRICE={fmt(r['price_usd'],10)}"
        )


    print()

    print(
        f"Refresh every {REFRESH}s."
    )

    print(
        "CTRL+C stops T103 only."
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for migration in migrated_tokens():

            process_token(
                migration
            )

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()

    print(
        "T103 stopped safely."
    )


finally:

    db.close()
