import sqlite3
import time
import math
import statistics
from collections import defaultdict

DB = "validation_v090.db"

# ============================================================
# T47 — FROZEN PROSPECTIVE SPEC
# ============================================================

MIN_PRIOR_TRADES = 1
PRE_EVENT_SEC = 30.0

RUNNER = 10.0
DUMP = -10.0

REFRESH_SEC = 10

TABLE = "t47_fastflip_prospective"

# IMPORTANT:
# T47 begins at the DB maximum event ID seen the FIRST time
# the recorder is launched.
BOUNDARY_KEY = "T47_BOUNDARY_ID"


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def safe_div(a, b):
    if b is None or b == 0:
        return None
    return a / b


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def fmt(x, n=3):
    if x is None:
        return "NA"
    return f"{x:.{n}f}"


def label_r60(x):
    if not valid(x):
        return None

    if x >= RUNNER:
        return "RUN"

    if x <= DUMP:
        return "DUMP"

    return "NEUTRAL"


# ============================================================
# DB INITIALIZATION
# ============================================================

db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


db.execute("""
CREATE TABLE IF NOT EXISTS t47_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    token_mint TEXT PRIMARY KEY,
    event_id INTEGER NOT NULL,
    event_timestamp REAL NOT NULL,
    captured_at REAL NOT NULL,

    boundary_id INTEGER NOT NULL,

    buyer_fast_mean REAL,
    exp_buyer_count INTEGER,
    buyer_count INTEGER,
    coverage REAL,

    cohort_token_count REAL,

    dex_return_60s REAL,
    status TEXT,
    binary_label INTEGER,
    labeled_60 INTEGER DEFAULT 0
)
""")


db.commit()


# ============================================================
# FROZEN PROSPECTIVE BOUNDARY
# ============================================================

row = db.execute("""
SELECT value
FROM t47_meta
WHERE key=?
""", (BOUNDARY_KEY,)).fetchone()


if row is None:

    max_id = db.execute("""
    SELECT COALESCE(MAX(id), 0)
    FROM events
    """).fetchone()[0]

    boundary_id = int(max_id)

    db.execute("""
    INSERT INTO t47_meta(key, value)
    VALUES (?, ?)
    """, (
        BOUNDARY_KEY,
        str(boundary_id)
    ))

    db.commit()

    print(
        f"✅ T47 prospective boundary frozen at ID={boundary_id}"
    )

else:

    boundary_id = int(
        row["value"]
    )


# ============================================================
# HISTORICAL WALLET SNAPSHOT AT EVENT TIME
# ============================================================

def build_wallet_history_until(event_ts):

    rows = db.execute("""
    SELECT
        timestamp,
        wallet,
        side,
        token_mint,
        clean_price
    FROM swaps
    WHERE
        timestamp < ?
        AND wallet IS NOT NULL
        AND token_mint IS NOT NULL
        AND side IN ('BUY','SELL')
        AND clean_price IS NOT NULL
        AND clean_price > 0
        AND (
            price_valid IS NULL
            OR price_valid=1
        )
    ORDER BY timestamp
    """, (
        event_ts,
    )).fetchall()


    completed = defaultdict(int)
    fast_flips = defaultdict(int)
    wallet_tokens = defaultdict(set)

    open_pos = {}


    for s in rows:

        wallet = s["wallet"]
        token = s["token_mint"]
        side = s["side"]
        ts = s["timestamp"]

        wallet_tokens[
            wallet
        ].add(token)

        key = (
            wallet,
            token
        )


        if side == "BUY":

            if key not in open_pos:

                open_pos[key] = ts


        elif side == "SELL":

            if key not in open_pos:
                continue

            entry_ts = open_pos[
                key
            ]

            hold = (
                ts
                - entry_ts
            )

            if hold >= 0:

                completed[
                    wallet
                ] += 1

                if hold <= 60:

                    fast_flips[
                        wallet
                    ] += 1

            open_pos.pop(
                key,
                None
            )


    return (
        completed,
        fast_flips,
        wallet_tokens
    )


# ============================================================
# FEATURE CREATION FOR ONE EVENT
# ============================================================

def create_event_feature(event):

    event_ts = event[
        "timestamp"
    ]

    token = event[
        "token_mint"
    ]


    (
        completed,
        fast_flips,
        wallet_tokens
    ) = build_wallet_history_until(
        event_ts
    )


    pre = db.execute("""
    SELECT
        wallet,
        side
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND wallet IS NOT NULL
    """, (
        token,
        event_ts - PRE_EVENT_SEC,
        event_ts
    )).fetchall()


    buyers = sorted(
        set(
            r["wallet"]
            for r in pre
            if r["side"] == "BUY"
        )
    )


    wallets = sorted(
        set(
            r["wallet"]
            for r in pre
        )
    )


    experienced_buyers = []

    for wallet in buyers:

        n = completed[
            wallet
        ]

        if n < MIN_PRIOR_TRADES:
            continue

        ff = safe_div(
            fast_flips[
                wallet
            ],
            n
        )

        if ff is None:
            continue

        experienced_buyers.append(
            (
                wallet,
                ff,
                n,
                len(
                    wallet_tokens[
                        wallet
                    ]
                )
            )
        )


    if not experienced_buyers:
        return None


    buyer_fast_mean = avg([
        x[1]
        for x in experienced_buyers
    ])


    exp_buyer_count = len(
        experienced_buyers
    )


    buyer_count = len(
        buyers
    )


    coverage = safe_div(
        exp_buyer_count,
        buyer_count
    )


    experienced_wallet_tokens = []

    for wallet in wallets:

        if completed[
            wallet
        ] >= MIN_PRIOR_TRADES:

            experienced_wallet_tokens.append(
                len(
                    wallet_tokens[
                        wallet
                    ]
                )
            )


    cohort_token_count = med(
        experienced_wallet_tokens
    )


    return {
        "buyer_fast_mean":
            buyer_fast_mean,

        "exp_buyer_count":
            exp_buyer_count,

        "buyer_count":
            buyer_count,

        "coverage":
            coverage,

        "cohort_token_count":
            cohort_token_count,
    }


# ============================================================
# CAPTURE NEW FIRST-SIGNAL TOKENS
# ============================================================

def capture_new_tokens():

    # Tokens already captured by T47
    existing = set(
        r[0]
        for r in db.execute(
            f"""
            SELECT token_mint
            FROM {TABLE}
            """
        ).fetchall()
    )


    rows = db.execute("""
    SELECT
        id,
        timestamp,
        token_mint,
        dex_return_60s
    FROM events
    WHERE
        id > ?
        AND timestamp IS NOT NULL
        AND token_mint IS NOT NULL
    ORDER BY id
    """, (
        boundary_id,
    )).fetchall()


    # First post-boundary event per token only.
    first_events = {}

    for e in rows:

        tok = e[
            "token_mint"
        ]

        if tok in existing:
            continue

        if tok not in first_events:

            first_events[
                tok
            ] = e


    captured = 0


    for token, event in first_events.items():

        features = create_event_feature(
            event
        )

        # If no experienced buyer exists,
        # the event is not eligible for the frozen metric.
        if features is None:
            continue


        r60 = event[
            "dex_return_60s"
        ]

        status = (
            label_r60(r60)
            if valid(r60)
            else "WAIT"
        )


        binary = None

        if status == "RUN":
            binary = 1

        elif status == "DUMP":
            binary = 0


        labeled = (
            1
            if valid(r60)
            else 0
        )


        db.execute(f"""
        INSERT OR IGNORE INTO {TABLE} (
            token_mint,
            event_id,
            event_timestamp,
            captured_at,
            boundary_id,

            buyer_fast_mean,
            exp_buyer_count,
            buyer_count,
            coverage,

            cohort_token_count,

            dex_return_60s,
            status,
            binary_label,
            labeled_60
        )
        VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
        """, (
            token,
            event["id"],
            event["timestamp"],
            time.time(),
            boundary_id,

            features[
                "buyer_fast_mean"
            ],

            features[
                "exp_buyer_count"
            ],

            features[
                "buyer_count"
            ],

            features[
                "coverage"
            ],

            features[
                "cohort_token_count"
            ],

            r60,
            status,
            binary,
            labeled
        ))


        if db.total_changes:
            captured += 1


    db.commit()

    return captured


# ============================================================
# UPDATE +60s LABELS
# ============================================================

def update_labels():

    waiting = db.execute(f"""
    SELECT
        token_mint,
        event_id
    FROM {TABLE}
    WHERE labeled_60=0
    """).fetchall()


    for row in waiting:

        e = db.execute("""
        SELECT dex_return_60s
        FROM events
        WHERE id=?
        """, (
            row[
                "event_id"
            ],
        )).fetchone()


        if e is None:
            continue


        r60 = e[
            "dex_return_60s"
        ]


        if not valid(r60):
            continue


        status = label_r60(
            r60
        )


        binary = None

        if status == "RUN":
            binary = 1

        elif status == "DUMP":
            binary = 0


        db.execute(f"""
        UPDATE {TABLE}
        SET
            dex_return_60s=?,
            status=?,
            binary_label=?,
            labeled_60=1
        WHERE token_mint=?
        """, (
            r60,
            status,
            binary,
            row[
                "token_mint"
            ]
        ))


    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    rows = db.execute(f"""
    SELECT *
    FROM {TABLE}
    ORDER BY event_id
    """).fetchall()


    total = len(
        rows
    )


    labeled = [
        r for r in rows
        if r[
            "labeled_60"
        ] == 1
    ]


    binary = [
        r for r in labeled
        if r[
            "binary_label"
        ] is not None
    ]


    runners = [
        r for r in binary
        if r[
            "binary_label"
        ] == 1
    ]


    dumps = [
        r for r in binary
        if r[
            "binary_label"
        ] == 0
    ]


    neutral = [
        r for r in labeled
        if r[
            "status"
        ] == "NEUTRAL"
    ]


    wait = [
        r for r in rows
        if r[
            "labeled_60"
        ] == 0
    ]


    print("\033[2J\033[H", end="")

    print("=" * 160)
    print(
        "MEMECOIN LAB — T47 FROZEN FAST-FLIP PROSPECTIVE SHADOW RECORDER"
    )
    print("=" * 160)

    print(
        f"BOUNDARY ID       : {boundary_id}"
    )

    print(
        "FROZEN FEATURE    : buyer_fast_mean"
    )

    print(
        "EXPERIENCE        : >=1 prior completed trade"
    )

    print(
        "FROZEN DIRECTION  : LOWER = MORE RUN-LIKE"
    )

    print(
        "OUTCOME           : DEX RETURN +60s"
    )

    print(
        "RUN >= +10% | DUMP <= -10%"
    )

    print()


    print(
        f"TOKENS CAPTURED   : {total}"
    )

    print(
        f"LABELED +60s      : {len(labeled)}"
    )

    print(
        f"BINARY            : {len(binary)}"
    )

    print(
        f"RUN               : {len(runners)}"
    )

    print(
        f"DUMP              : {len(dumps)}"
    )

    print(
        f"NEUTRAL           : {len(neutral)}"
    )

    print(
        f"LIVE / WAIT       : {len(wait)}"
    )


    # --------------------------------------------------------
    # Binary direction
    # --------------------------------------------------------

    print()
    print("=" * 160)
    print("PROSPECTIVE FAST-FLIP SEPARATION")
    print("=" * 160)


    if runners and dumps:

        run_med = med([
            r[
                "buyer_fast_mean"
            ]
            for r in runners
        ])

        dump_med = med([
            r[
                "buyer_fast_mean"
            ]
            for r in dumps
        ])


        print(
            f"RUN MED FAST-FLIP  : "
            f"{fmt(run_med,4)}"
        )

        print(
            f"DUMP MED FAST-FLIP : "
            f"{fmt(dump_med,4)}"
        )

        print(
            f"RUN-DUMP DIFF      : "
            f"{fmt(run_med-dump_med,4)}"
        )


        try:

            from sklearn.metrics import roc_auc_score

            y = [
                r[
                    "binary_label"
                ]
                for r in binary
            ]

            # frozen direction:
            # lower value = higher RUN score
            score = [
                -r[
                    "buyer_fast_mean"
                ]
                for r in binary
            ]

            auc = roc_auc_score(
                y,
                score
            )

            print(
                f"DIRECTIONAL AUC    : "
                f"{auc:.3f}"
            )

        except Exception:

            print(
                "DIRECTIONAL AUC    : NA"
            )

    else:

        print(
            "Waiting for both RUN and DUMP binary outcomes..."
        )


    # --------------------------------------------------------
    # Coverage diagnostics
    # --------------------------------------------------------

    print()
    print("=" * 160)
    print("COVERAGE / QUALITY")
    print("=" * 160)


    if rows:

        print(
            f"MED EXP BUYERS : "
            f"{fmt(med([r['exp_buyer_count'] for r in rows]),1)}"
        )

        print(
            f"MED BUYERS     : "
            f"{fmt(med([r['buyer_count'] for r in rows]),1)}"
        )

        print(
            f"MED COVERAGE   : "
            f"{fmt(100*med([r['coverage'] for r in rows if valid(r['coverage'])]),1)}%"
        )

        print(
            f"MED COHORT TOK : "
            f"{fmt(med([r['cohort_token_count'] for r in rows if valid(r['cohort_token_count'])]),1)}"
        )


    # --------------------------------------------------------
    # Checkpoints
    # --------------------------------------------------------

    print()
    print("=" * 160)
    print("FROZEN CHECKPOINTS")
    print("=" * 160)


    if total < 15:

        print(
            f"⏳ {total}/15 prospective unique tokens"
        )

    elif total < 30:

        print(
            f"🟡 CHECKPOINT 15 REACHED | {total}/30"
        )

        print(
            "Early directional validation only."
        )

    else:

        print(
            f"🟢 CHECKPOINT 30 REACHED | N={total}"
        )

        print(
            "Enough prospective tokens for first serious audit."
        )


    # --------------------------------------------------------
    # Latest
    # --------------------------------------------------------

    print()
    print("=" * 160)
    print("LATEST PROSPECTIVE TOKENS")
    print("=" * 160)

    print(
        f"{'ID':>6} "
        f"{'FAST':>8} "
        f"{'EXP':>4} "
        f"{'BUY':>4} "
        f"{'COV':>7} "
        f"{'R60':>9} "
        f"{'STATUS':>8} "
        f"TOKEN"
    )

    print("-" * 120)


    for r in rows[-15:][::-1]:

        r60 = (
            f"{r['dex_return_60s']:+.2f}%"
            if valid(
                r[
                    "dex_return_60s"
                ]
            )
            else "NA"
        )

        cov = (
            f"{100*r['coverage']:.1f}%"
            if valid(
                r[
                    "coverage"
                ]
            )
            else "NA"
        )

        print(
            f"{r['event_id']:6d} "
            f"{r['buyer_fast_mean']:8.3f} "
            f"{r['exp_buyer_count']:4d} "
            f"{r['buyer_count']:4d} "
            f"{cov:>7} "
            f"{r60:>9} "
            f"{r['status']:>8} "
            f"{r['token_mint'][:24]}"
        )


    print()
    print("IMPORTANT:")
    print("• T47 is prospective only.")
    print("• One first eligible post-boundary event per token.")
    print("• buyer_fast_mean definition is frozen.")
    print("• >=1 prior completed wallet trade is frozen.")
    print("• LOWER fast-flip = RUN-like is frozen.")
    print("• No threshold optimization.")
    print("• No execution decisions.")
    print("• Do not reset this table.")
    print("• T23/T31/T32 remain untouched.")
    print(
        f"• Refresh every {REFRESH_SEC} seconds."
    )


# ============================================================
# MAIN
# ============================================================

print(
    "Starting T47 prospective recorder..."
)

print(
    "CTRL+C to stop safely."
)

time.sleep(1)


try:

    while True:

        capture_new_tokens()

        update_labels()

        show()

        time.sleep(
            REFRESH_SEC
        )


except KeyboardInterrupt:

    print()
    print(
        "T47 stopped safely."
    )

finally:

    db.close()
