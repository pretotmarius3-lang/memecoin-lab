#!/usr/bin/env python3

import sqlite3
import time
import math

DB = "validation_v090.db"

T103_STATE = "t103_token_lifecycle_state"
T103_EVENTS = "t103_token_lifecycle_events"
HOLDERS = "t101_migrated_holder_snapshots"

TABLE = "t104_resurrection_cohort"

REFRESH = 15

HORIZONS = [
    30,
    60,
    300,
    900,
    1800,
]

RECOVERY_THRESHOLD = 50.0


# ============================================================
# HELPERS
# ============================================================

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


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLE
# ============================================================

horizon_columns = []

for h in HORIZONS:

    horizon_columns.append(f"""
        price_{h}s REAL,
        price_delay_{h}s REAL,

        return_from_crash_{h}s REAL,
        return_from_peak_{h}s REAL,

        recovery50_{h}s INTEGER,
        reclaim_peak_{h}s INTEGER,

        liquidity_{h}s REAL,
        market_cap_{h}s REAL,
        volume_m5_{h}s REAL,
        buys_m5_{h}s INTEGER,
        sells_m5_{h}s INTEGER,

        done_{h}s INTEGER NOT NULL DEFAULT 0
    """)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    token_mint TEXT NOT NULL UNIQUE,

    crash_event_id INTEGER NOT NULL,
    crash_timestamp REAL NOT NULL,

    migrated_at REAL,

    holders_at_crash INTEGER,
    holder_snapshot_at REAL,
    holder_snapshot_age REAL,

    holders_prev INTEGER,
    holders_prev_at REAL,
    holder_change INTEGER,

    first_price REAL,

    run_confirmed_at REAL,
    run_confirmed_price REAL,

    pre_crash_peak REAL,
    pre_crash_peak_at REAL,

    crash_price REAL,

    first_run_return_pct REAL,
    crash_drawdown_pct REAL,

    migration_to_run_s REAL,
    run_to_crash_s REAL,
    peak_to_crash_s REAL,

    dex_snapshot_at REAL,
    dex_snapshot_age REAL,

    liquidity_at_crash REAL,
    market_cap_at_crash REAL,
    fdv_at_crash REAL,

    volume_m5_at_crash REAL,
    buys_m5_at_crash INTEGER,
    sells_m5_at_crash INTEGER,

    buy_share_m5_at_crash REAL,
    buy_sell_imbalance_m5 REAL,

    liquidity_to_mc REAL,
    volume_to_liquidity REAL,

    pair_address TEXT,
    dex_id TEXT,

    {",".join(horizon_columns)},

    ever_recovery50 INTEGER NOT NULL DEFAULT 0,
    ever_reclaim_peak INTEGER NOT NULL DEFAULT 0,

    created_at REAL NOT NULL,
    last_update_at REAL NOT NULL
)
""")

db.commit()


# ============================================================
# SOURCE HELPERS
# ============================================================

def latest_dex_before(
    mint,
    ts
):

    return db.execute("""
    SELECT
        timestamp,
        price_usd,
        liquidity_usd,
        market_cap,
        fdv,
        volume_m5,
        buys_m5,
        sells_m5,
        pair_address,
        dex_id

    FROM dex_prices

    WHERE
        token_mint=?
        AND timestamp <= ?
        AND price_usd IS NOT NULL
        AND price_usd > 0

    ORDER BY timestamp DESC

    LIMIT 1
    """, (
        mint,
        ts
    )).fetchone()


def first_dex_after(
    mint,
    ts
):

    return db.execute("""
    SELECT
        timestamp,
        price_usd,
        liquidity_usd,
        market_cap,
        fdv,
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

    LIMIT 1
    """, (
        mint,
        ts
    )).fetchone()


def holder_before(
    mint,
    ts
):

    return db.execute(f"""
    SELECT
        checked_at,
        holder_count

    FROM {HOLDERS}

    WHERE
        token_mint=?
        AND status='OK'
        AND holder_count IS NOT NULL
        AND checked_at <= ?

    ORDER BY checked_at DESC

    LIMIT 1
    """, (
        mint,
        ts
    )).fetchone()


def previous_holder(
    mint,
    before_ts
):

    return db.execute(f"""
    SELECT
        checked_at,
        holder_count

    FROM {HOLDERS}

    WHERE
        token_mint=?
        AND status='OK'
        AND holder_count IS NOT NULL
        AND checked_at < ?

    ORDER BY checked_at DESC

    LIMIT 1
    """, (
        mint,
        before_ts
    )).fetchone()


# ============================================================
# CRASH DISCOVERY
# ============================================================

def crash_events():

    return db.execute(f"""
    SELECT
        e.id AS crash_event_id,
        e.token_mint,
        e.event_timestamp AS crash_timestamp,
        e.price_usd AS crash_event_price,
        e.reference_price AS event_peak_reference,
        e.move_pct AS crash_move_pct,

        s.migrated_at,

        s.first_price,

        s.run_confirmed_at,
        s.run_confirmed_price,

        s.run_peak_price,
        s.run_peak_at

    FROM {T103_EVENTS} e

    JOIN {T103_STATE} s
        ON s.token_mint=e.token_mint

    WHERE
        e.lifecycle_event='CRASH_CONFIRMED'

    ORDER BY e.event_timestamp
    """).fetchall()


# ============================================================
# FREEZE ONE CRASH
# ============================================================

def freeze_crash(c):

    exists = db.execute(f"""
    SELECT id
    FROM {TABLE}
    WHERE token_mint=?
    """, (
        c["token_mint"],
    )).fetchone()

    if exists:
        return False


    mint = c[
        "token_mint"
    ]

    crash_ts = c[
        "crash_timestamp"
    ]


    # --------------------------------------------------------
    # STRICT PRE/AT CRASH DEX SNAPSHOT
    # --------------------------------------------------------

    dex = latest_dex_before(
        mint,
        crash_ts
    )

    crash_price = c[
        "crash_event_price"
    ]


    dex_ts = None
    dex_age = None

    liquidity = None
    mc = None
    fdv = None
    vol5 = None
    buys5 = None
    sells5 = None

    pair = None
    dex_id = None


    if dex:

        dex_ts = dex[
            "timestamp"
        ]

        dex_age = (
            crash_ts
            - dex_ts
        )

        liquidity = dex[
            "liquidity_usd"
        ]

        mc = dex[
            "market_cap"
        ]

        fdv = dex[
            "fdv"
        ]

        vol5 = dex[
            "volume_m5"
        ]

        buys5 = dex[
            "buys_m5"
        ]

        sells5 = dex[
            "sells_m5"
        ]

        pair = dex[
            "pair_address"
        ]

        dex_id = dex[
            "dex_id"
        ]


    # --------------------------------------------------------
    # HOLDERS AT CRASH
    # --------------------------------------------------------

    h = holder_before(
        mint,
        crash_ts
    )

    holders_now = None
    holder_ts = None
    holder_age = None

    holders_prev = None
    holder_prev_ts = None
    holder_change = None


    if h:

        holders_now = h[
            "holder_count"
        ]

        holder_ts = h[
            "checked_at"
        ]

        holder_age = (
            crash_ts
            - holder_ts
        )


        prev = previous_holder(
            mint,
            holder_ts
        )

        if prev:

            holders_prev = prev[
                "holder_count"
            ]

            holder_prev_ts = prev[
                "checked_at"
            ]

            holder_change = (
                holders_now
                - holders_prev
            )


    # --------------------------------------------------------
    # LIFECYCLE DURATIONS
    # --------------------------------------------------------

    first_price = c[
        "first_price"
    ]

    peak = c[
        "run_peak_price"
    ]

    run_price = c[
        "run_confirmed_price"
    ]


    first_run_return = pct(
        first_price,
        peak
    )

    crash_dd = pct(
        peak,
        crash_price
    )


    migration_to_run = None

    if (
        valid(c["migrated_at"])
        and valid(c["run_confirmed_at"])
    ):
        migration_to_run = (
            c["run_confirmed_at"]
            - c["migrated_at"]
        )


    run_to_crash = None

    if (
        valid(c["run_confirmed_at"])
        and valid(crash_ts)
    ):
        run_to_crash = (
            crash_ts
            - c["run_confirmed_at"]
        )


    peak_to_crash = None

    if (
        valid(c["run_peak_at"])
        and valid(crash_ts)
    ):
        peak_to_crash = (
            crash_ts
            - c["run_peak_at"]
        )


    # --------------------------------------------------------
    # MICRO MARKET STRUCTURE AT CRASH
    # --------------------------------------------------------

    buy_share = None
    imbalance = None

    if (
        buys5 is not None
        and sells5 is not None
        and (
            buys5 + sells5
        ) > 0
    ):

        buy_share = (
            buys5
            / (
                buys5
                + sells5
            )
        )

        imbalance = (
            buys5
            - sells5
        ) / (
            buys5
            + sells5
        )


    liq_to_mc = None

    if (
        valid(liquidity)
        and valid(mc)
        and mc > 0
    ):

        liq_to_mc = (
            liquidity
            / mc
        )


    vol_to_liq = None

    if (
        valid(vol5)
        and valid(liquidity)
        and liquidity > 0
    ):

        vol_to_liq = (
            vol5
            / liquidity
        )


    # --------------------------------------------------------
    # STORE FREEZE
    # --------------------------------------------------------

    now = time.time()


    db.execute(f"""
    INSERT INTO {TABLE} (

        token_mint,
        crash_event_id,
        crash_timestamp,

        migrated_at,

        holders_at_crash,
        holder_snapshot_at,
        holder_snapshot_age,

        holders_prev,
        holders_prev_at,
        holder_change,

        first_price,

        run_confirmed_at,
        run_confirmed_price,

        pre_crash_peak,
        pre_crash_peak_at,

        crash_price,

        first_run_return_pct,
        crash_drawdown_pct,

        migration_to_run_s,
        run_to_crash_s,
        peak_to_crash_s,

        dex_snapshot_at,
        dex_snapshot_age,

        liquidity_at_crash,
        market_cap_at_crash,
        fdv_at_crash,

        volume_m5_at_crash,
        buys_m5_at_crash,
        sells_m5_at_crash,

        buy_share_m5_at_crash,
        buy_sell_imbalance_m5,

        liquidity_to_mc,
        volume_to_liquidity,

        pair_address,
        dex_id,

        created_at,
        last_update_at
    )

    VALUES (
        ?, ?, ?,
        ?,
        ?, ?, ?,
        ?, ?, ?,
        ?,
        ?, ?,
        ?, ?,
        ?,
        ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?
    )
    """, (

        mint,
        c["crash_event_id"],
        crash_ts,

        c["migrated_at"],

        holders_now,
        holder_ts,
        holder_age,

        holders_prev,
        holder_prev_ts,
        holder_change,

        first_price,

        c["run_confirmed_at"],
        run_price,

        peak,
        c["run_peak_at"],

        crash_price,

        first_run_return,
        crash_dd,

        migration_to_run,
        run_to_crash,
        peak_to_crash,

        dex_ts,
        dex_age,

        liquidity,
        mc,
        fdv,

        vol5,
        buys5,
        sells5,

        buy_share,
        imbalance,

        liq_to_mc,
        vol_to_liq,

        pair,
        dex_id,

        now,
        now
    ))

    db.commit()

    print(
        f"🧊 CRASH FREEZE "
        f"| {mint[:18]}... "
        f"| H={holders_now} "
        f"| DD={fmt(crash_dd)}%"
    )

    return True


# ============================================================
# FUTURE OUTCOMES
# ============================================================

def fill_outcomes():

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}
    ORDER BY crash_timestamp
    """).fetchall()


    now = time.time()


    for r in rows:

        updates = {}


        for h in HORIZONS:

            if r[
                f"done_{h}s"
            ] == 1:
                continue


            target = (
                r["crash_timestamp"]
                + h
            )


            if now < target:
                continue


            snap = first_dex_after(
                r["token_mint"],
                target
            )


            if not snap:
                continue


            price = snap[
                "price_usd"
            ]


            delay = (
                snap["timestamp"]
                - target
            )


            from_crash = pct(
                r["crash_price"],
                price
            )


            from_peak = pct(
                r["pre_crash_peak"],
                price
            )


            recovery50 = int(
                from_crash is not None
                and from_crash >= RECOVERY_THRESHOLD
            )


            reclaim_peak = int(
                valid(
                    r["pre_crash_peak"]
                )
                and price
                >= r["pre_crash_peak"]
            )


            updates[
                f"price_{h}s"
            ] = price

            updates[
                f"price_delay_{h}s"
            ] = delay

            updates[
                f"return_from_crash_{h}s"
            ] = from_crash

            updates[
                f"return_from_peak_{h}s"
            ] = from_peak

            updates[
                f"recovery50_{h}s"
            ] = recovery50

            updates[
                f"reclaim_peak_{h}s"
            ] = reclaim_peak

            updates[
                f"liquidity_{h}s"
            ] = snap[
                "liquidity_usd"
            ]

            updates[
                f"market_cap_{h}s"
            ] = snap[
                "market_cap"
            ]

            updates[
                f"volume_m5_{h}s"
            ] = snap[
                "volume_m5"
            ]

            updates[
                f"buys_m5_{h}s"
            ] = snap[
                "buys_m5"
            ]

            updates[
                f"sells_m5_{h}s"
            ] = snap[
                "sells_m5"
            ]

            updates[
                f"done_{h}s"
            ] = 1


        if not updates:
            continue


        # --------------------------------------------
        # PERSIST
        # --------------------------------------------

        assignments = [
            f"{k}=?"
            for k in updates
        ]

        values = [
            updates[k]
            for k in updates
        ]


        db.execute(
            f"""
            UPDATE {TABLE}

            SET
                {", ".join(assignments)},
                last_update_at=?

            WHERE id=?
            """,
            (
                *values,
                time.time(),
                r["id"]
            )
        )

        db.commit()


    # ========================================================
    # REFRESH EVER FLAGS
    # ========================================================

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}
    """).fetchall()


    for r in rows:

        ever_recovery = 0
        ever_peak = 0


        for h in HORIZONS:

            if r[
                f"recovery50_{h}s"
            ] == 1:
                ever_recovery = 1

            if r[
                f"reclaim_peak_{h}s"
            ] == 1:
                ever_peak = 1


        db.execute(f"""
        UPDATE {TABLE}

        SET
            ever_recovery50=?,
            ever_reclaim_peak=?

        WHERE id=?
        """, (
            ever_recovery,
            ever_peak,
            r["id"]
        ))


    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}

    ORDER BY
        ever_reclaim_peak DESC,
        ever_recovery50 DESC,
        crash_timestamp DESC
    """).fetchall()


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 190)

    print(
        "MEMECOIN LAB — T104 RESURRECTION COHORT RECORDER"
    )

    print("=" * 190)


    print(
        f"CRASH COHORT       : {len(rows)}"
    )

    print(
        f">=50 HOLDERS       : "
        f"{sum((r['holders_at_crash'] or 0)>=50 for r in rows)}"
    )

    print(
        f"RECOVERY >=50%     : "
        f"{sum(r['ever_recovery50'] for r in rows)}"
    )

    print(
        f"RECLAIM PEAK       : "
        f"{sum(r['ever_reclaim_peak'] for r in rows)}"
    )


    print()

    print(
        "MODE               : OBSERVATION ONLY"
    )

    print(
        "MODEL FITTING      : NONE"
    )

    print(
        "FEATURE THRESHOLD  : NONE"
    )

    print(
        ">=50 FILTER        : NOT ACTIVE"
    )

    print(
        "CRASH FREEZE       : STRICT DATA <= CRASH TIME"
    )


    print()

    print("=" * 190)
    print("COHORT")
    print("=" * 190)


    for r in rows[:30]:

        print(
            f"{r['token_mint'][:18]:18} "
            f"| H={str(r['holders_at_crash']):>5} "
            f"| RUN={fmt(r['first_run_return_pct']):>7}% "
            f"| CRASH={fmt(r['crash_drawdown_pct']):>7}% "
            f"| HΔ={str(r['holder_change']):>5} "
            f"| MC={fmt(r['market_cap_at_crash'],0):>9} "
            f"| LIQ={fmt(r['liquidity_at_crash'],0):>8} "
            f"| BUYSH={fmt(r['buy_share_m5_at_crash'],2):>5} "
            f"| REC50={r['ever_recovery50']} "
            f"| PEAK={r['ever_reclaim_peak']}"
        )


    print()

    print("=" * 190)
    print("FORWARD OUTCOMES")
    print("=" * 190)


    for r in rows[:15]:

        print()

        print(
            f"{r['token_mint'][:24]}"
        )


        for h in HORIZONS:

            print(
                f"  +{h:4d}s "
                f"| DONE={r[f'done_{h}s']} "
                f"| RET_CRASH={fmt(r[f'return_from_crash_{h}s']):>8}% "
                f"| RET_PEAK={fmt(r[f'return_from_peak_{h}s']):>8}% "
                f"| REC50={str(r[f'recovery50_{h}s']):>4} "
                f"| RECLAIM={str(r[f'reclaim_peak_{h}s']):>4} "
                f"| DELAY={fmt(r[f'price_delay_{h}s']):>7}s"
            )


    print()

    print("=" * 190)
    print("READINESS")
    print("=" * 190)


    print(
        f"CRASH TOKENS       : {len(rows)}/30 integrity"
    )

    print(
        f"CRASH TOKENS       : {len(rows)}/50 descriptive"
    )

    print(
        f"CRASH TOKENS       : {len(rows)}/100 discovery"
    )


    if len(rows) >= 100:

        print(
            "🟢 T104 READY FOR SERIOUS RESURRECTION DISCOVERY."
        )

    elif len(rows) >= 50:

        print(
            "🟡 T104 DESCRIPTIVE CHECKPOINT REACHED."
        )

    elif len(rows) >= 30:

        print(
            "🔵 T104 INTEGRITY CHECKPOINT REACHED."
        )

    else:

        print(
            "🔵 T104 COLLECTING CRASHED FORMER RUNNERS."
        )


    print()

    print(
        f"Refresh every {REFRESH}s."
    )

    print(
        "CTRL+C stops T104 only."
    )


# ============================================================
# LOOP
# ============================================================

try:

    while True:

        for crash in crash_events():

            freeze_crash(
                crash
            )


        fill_outcomes()

        show()

        time.sleep(
            REFRESH
        )


except KeyboardInterrupt:

    print()

    print(
        "T104 stopped safely."
    )


finally:

    db.close()
