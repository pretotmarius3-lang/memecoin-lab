import sqlite3
import statistics
import math
import os
import time

import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

DB = "validation_v090.db"

BOUNDARY_ID = 417

RUNNER = 10.0
DUMP = -10.0

K = 3

FEATURES = [
    "fa",
    "nf30",
    "new_wallets10",
    "new_wallets30",

    "volume_m5",
    "liquidity_usd",
    "market_cap",

    "vol_liq",

    "mid_buy_count",
    "mid_sell_count",
    "mid_flow_balance",

    "recent_unique_buyers",

    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",
    "swap_velocity_mean",

    "buy_concentration_trend",

    "recent_price_return",
    "mid_price_return",

    "recent_sell_sol",
    "recent_net_sol",
    "recent_buy_share",

    "late_chase_score",
    "breadth_score",
]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def mean(vals):
    vals = [
        x for x in vals
        if valid(x)
    ]
    return statistics.mean(vals) if vals else None


def median(vals):
    vals = [
        x for x in vals
        if valid(x)
    ]
    return statistics.median(vals) if vals else None


def safe_div(a,b):
    if not valid(a) or not valid(b) or b == 0:
        return None
    return a/b


def connect():
    db = sqlite3.connect(
        DB,
        timeout=30
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def load(db):

    return db.execute("""
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
            e.timestamp,
            e.token_mint,
            e.dex_return_60s,

            e.fa,
            e.nf30,
            e.new_wallets10,
            e.new_wallets30,

            d.volume_m5,
            d.liquidity_usd,
            d.market_cap,

            s.mid_buy_count,
            s.mid_sell_count,
            s.recent_unique_buyers,

            s.early_swaps_per_sec,
            s.mid_swaps_per_sec,
            s.recent_swaps_per_sec,

            s.buy_concentration_trend,

            s.recent_price_return,
            s.mid_price_return,

            s.recent_sell_sol,
            s.recent_net_sol,
            s.recent_buy_share,

            s.late_chase_score,
            s.breadth_score

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        LEFT JOIN first_dex d
        ON d.event_id=e.id

        ORDER BY e.id
    """).fetchall()


def f(r, name):

    if name in r.keys():
        return r[name]

    if name == "vol_liq":
        return safe_div(
            r["volume_m5"],
            r["liquidity_usd"]
        )

    if name == "mid_flow_balance":
        b = r["mid_buy_count"]
        s = r["mid_sell_count"]

        if not valid(b) or not valid(s):
            return None

        return b-s

    if name == "swap_velocity_mean":

        vals = [
            r["early_swaps_per_sec"],
            r["mid_swaps_per_sec"],
            r["recent_swaps_per_sec"],
        ]

        vals = [
            x for x in vals
            if valid(x)
        ]

        return mean(vals) if vals else None

    return None


def build_matrix(rows):

    X = []

    for r in rows:

        vals = []

        for name in FEATURES:

            v = f(r,name)

            vals.append(
                float(v)
                if valid(v)
                else np.nan
            )

        X.append(vals)

    return np.asarray(
        X,
        dtype=float
    )


def ensure_table(db):

    db.execute("""
        CREATE TABLE IF NOT EXISTS frozen_regime_v620 (

            event_id INTEGER PRIMARY KEY,

            token_mint TEXT,

            regime INTEGER,

            assigned_at REAL,

            boundary_id INTEGER,

            recent_net_sol REAL,
            recent_buy_share REAL,
            fa REAL,

            dex_return_60s REAL,

            label INTEGER,

            labeled INTEGER DEFAULT 0
        )
    """)

    db.commit()


def outcome_label(ret):

    if not valid(ret):
        return None

    if ret >= RUNNER:
        return 1

    if ret <= DUMP:
        return 0

    return None


def stats(rows):

    usable = [
        r for r in rows
        if (
            r["labeled"] == 1
            and r["label"] is not None
        )
    ]

    if not usable:
        return None

    runners = sum(
        r["label"] == 1
        for r in usable
    )

    dumps = sum(
        r["label"] == 0
        for r in usable
    )

    returns = [
        r["dex_return_60s"]
        for r in usable
        if valid(
            r["dex_return_60s"]
        )
    ]

    return {
        "n": len(usable),

        "tokens":
            len(set(
                r["token_mint"]
                for r in usable
            )),

        "runner":
            100*runners/len(usable),

        "dump":
            100*dumps/len(usable),

        "edge":
            100*(runners-dumps)/len(usable),

        "med":
            median(returns),

        "avg":
            mean(returns),
    }


def token_balanced(rows):

    usable = [
        r for r in rows
        if (
            r["labeled"] == 1
            and r["label"] is not None
        )
    ]

    groups = {}

    for r in usable:

        groups.setdefault(
            r["token_mint"],
            []
        ).append(
            r["dex_return_60s"]
        )

    if not groups:
        return None

    token_returns = [
        median(v)
        for v in groups.values()
    ]

    runner = sum(
        x >= RUNNER
        for x in token_returns
    )

    dump = sum(
        x <= DUMP
        for x in token_returns
    )

    return {
        "tokens":
            len(token_returns),

        "runner":
            100*runner/len(token_returns),

        "dump":
            100*dump/len(token_returns),

        "edge":
            100*(runner-dump)/len(token_returns),

        "med":
            median(token_returns),
    }


def first_event_per_token(rows):

    seen = set()
    out = []

    for r in sorted(
        rows,
        key=lambda x:x["event_id"]
    ):

        token = r[
            "token_mint"
        ]

        if token in seen:
            continue

        seen.add(token)
        out.append(r)

    return out


def directional_test(
    rows,
    field,
    title
):

    usable = [
        r for r in rows
        if (
            r["labeled"] == 1
            and r["label"] is not None
            and valid(r[field])
        )
    ]

    print()
    print(title)
    print("-"*100)

    if len(usable) < 8:

        print(
            f"Not enough cases: "
            f"{len(usable)}"
        )
        return

    cut = median(
        [r[field] for r in usable]
    )

    high = [
        r for r in usable
        if r[field] >= cut
    ]

    low = [
        r for r in usable
        if r[field] < cut
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
            100*(run-dump)
            / len(part)
        )

    he = edge(high)
    le = edge(low)

    print(
        f"MEDIAN CUT = {cut:+.4f}"
    )

    print(
        f"HIGH | N={len(high):3d} "
        f"| EDGE={he:+6.1f}%"
    )

    print(
        f"LOW  | N={len(low):3d} "
        f"| EDGE={le:+6.1f}%"
    )

    print(
        f"HIGH-LOW = "
        f"{he-le:+6.1f} pts"
    )


db = connect()

ensure_table(db)

# ============================================================
# FROZEN TRAINING
# ============================================================

all_rows = load(db)

train_rows = [
    r for r in all_rows
    if r["id"] <= BOUNDARY_ID
]

X_train = build_matrix(
    train_rows
)

pre = Pipeline([
    (
        "imputer",
        SimpleImputer(
            strategy="median"
        )
    ),

    (
        "scale",
        StandardScaler()
    ),
])

Xs_train = pre.fit_transform(
    X_train
)

kmeans = KMeans(
    n_clusters=K,
    n_init=20,
    random_state=42,
)

kmeans.fit(
    Xs_train
)

# ============================================================
# LIVE LOOP
# ============================================================

while True:

    try:

        all_rows = load(db)

        future = [
            r for r in all_rows
            if r["id"] > BOUNDARY_ID
        ]

        existing = {
            r["event_id"]
            for r in db.execute("""
                SELECT event_id
                FROM frozen_regime_v620
            """).fetchall()
        }

        pending = [
            r for r in future
            if r["id"] not in existing
        ]

        for r in pending:

            X = build_matrix(
                [r]
            )

            Xs = pre.transform(
                X
            )

            regime = int(
                kmeans.predict(
                    Xs
                )[0]
            )

            ret = (
                r["dex_return_60s"]
                if valid(
                    r["dex_return_60s"]
                )
                else None
            )

            label = outcome_label(
                ret
            )

            labeled = (
                1
                if ret is not None
                else 0
            )

            db.execute("""
                INSERT OR IGNORE INTO
                frozen_regime_v620 (

                    event_id,
                    token_mint,

                    regime,

                    assigned_at,

                    boundary_id,

                    recent_net_sol,
                    recent_buy_share,
                    fa,

                    dex_return_60s,

                    label,

                    labeled
                )

                VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?
                )
            """, (
                r["id"],
                r["token_mint"],

                regime,

                time.time(),

                BOUNDARY_ID,

                f(
                    r,
                    "recent_net_sol"
                ),

                f(
                    r,
                    "recent_buy_share"
                ),

                f(
                    r,
                    "fa"
                ),

                ret,

                label,

                labeled,
            ))

        db.commit()

        # Refresh outcomes for entries that were assigned
        # before 60s result existed.

        unresolved = db.execute("""
            SELECT event_id
            FROM frozen_regime_v620
            WHERE labeled=0
        """).fetchall()

        for item in unresolved:

            eid = item[
                "event_id"
            ]

            rr = db.execute("""
                SELECT dex_return_60s
                FROM events
                WHERE id=?
            """, (
                eid,
            )).fetchone()

            if (
                rr
                and valid(
                    rr["dex_return_60s"]
                )
            ):

                ret = rr[
                    "dex_return_60s"
                ]

                label = outcome_label(
                    ret
                )

                db.execute("""
                    UPDATE frozen_regime_v620

                    SET
                        dex_return_60s=?,
                        label=?,
                        labeled=1

                    WHERE event_id=?
                """, (
                    ret,
                    label,
                    eid,
                ))

        db.commit()

        predictions = db.execute("""
            SELECT *
            FROM frozen_regime_v620
            ORDER BY event_id
        """).fetchall()

        os.system("clear")

        print("="*120)
        print(
            "MEMECOIN LAB — "
            "V6.2 FROZEN REGIME FORWARD VALIDATOR"
        )
        print("="*120)

        print(
            f"REGIME MODEL FROZEN AT "
            f"ID <= {BOUNDARY_ID}"
        )

        print(
            f"TRAIN EVENTS : "
            f"{len(train_rows)}"
        )

        print(
            f"FORWARD EVENTS ASSIGNED : "
            f"{len(predictions)}"
        )

        print(
            f"UNIQUE FORWARD TOKENS   : "
            f"{len(set(r['token_mint'] for r in predictions))}"
        )

        print()

        print("="*120)
        print(
            "REGIME FORWARD OUTCOMES"
        )
        print("="*120)

        for regime in range(K):

            rr = [
                r for r in predictions
                if r["regime"] == regime
            ]

            s = stats(rr)
            tb = token_balanced(rr)

            print()
            print(
                f"R{regime}"
            )

            print("-"*80)

            if not s:

                print(
                    f"ASSIGNED={len(rr)} "
                    f"| no binary outcomes yet"
                )

                continue

            print(
                f"ASSIGNED={len(rr)} "
                f"| BINARY={s['n']} "
                f"| TOKENS={s['tokens']}"
            )

            print(
                f"MED60={s['med']:+.2f}% "
                f"| AVG60={s['avg']:+.2f}% "
                f"| RUNNER={s['runner']:.1f}% "
                f"| DUMP={s['dump']:.1f}% "
                f"| EDGE={s['edge']:+.1f}%"
            )

            if tb:

                print(
                    f"TOKEN-BALANCED "
                    f"| TOK={tb['tokens']} "
                    f"| MED={tb['med']:+.2f}% "
                    f"| EDGE={tb['edge']:+.1f}%"
                )

        # ====================================================
        # R1 HYPOTHESES
        # ====================================================

        r1 = [
            r for r in predictions
            if r["regime"] == 1
        ]

        print()
        print("="*120)
        print(
            "R1 FROZEN HYPOTHESIS TESTS"
        )
        print("="*120)

        directional_test(
            r1,
            "recent_net_sol",
            "A) recent_net_sol"
        )

        directional_test(
            r1,
            "recent_buy_share",
            "B) recent_buy_share"
        )

        directional_test(
            r1,
            "fa",
            "C) FA"
        )

        print()
        print("="*120)
        print(
            "R1 — FIRST EVENT PER TOKEN"
        )
        print("="*120)

        r1_first = first_event_per_token(
            r1
        )

        s_first = stats(
            r1_first
        )

        if s_first:

            print(
                f"N={s_first['n']} "
                f"| TOKENS={s_first['tokens']} "
                f"| RUNNER={s_first['runner']:.1f}% "
                f"| DUMP={s_first['dump']:.1f}% "
                f"| EDGE={s_first['edge']:+.1f}%"
            )

        else:

            print(
                "Not enough labeled cases yet."
            )

        print()
        print("="*120)
        print(
            "LATEST ASSIGNMENTS"
        )
        print("="*120)

        print(
            f"{'ID':>5} "
            f"{'REG':>4} "
            f"{'NETSOL':>9} "
            f"{'BUYSH':>8} "
            f"{'FA':>8} "
            f"{'STATUS':>9} "
            f"{'R60':>9} "
            f"{'TOKEN':18}"
        )

        print("-"*95)

        for r in reversed(
            predictions[-20:]
        ):

            if not r["labeled"]:
                status = "WAIT"

            elif r["label"] == 1:
                status = "RUN"

            elif r["label"] == 0:
                status = "DUMP"

            else:
                status = "NEUTRAL"

            ret = (
                f"{r['dex_return_60s']:+8.2f}%"
                if valid(
                    r["dex_return_60s"]
                )
                else "      NA"
            )

            print(
                f"{r['event_id']:>5} "
                f"R{r['regime']:<3} "
                f"{(r['recent_net_sol'] if valid(r['recent_net_sol']) else 0):+8.3f} "
                f"{(r['recent_buy_share'] if valid(r['recent_buy_share']) else 0):7.3f} "
                f"{(r['fa'] if valid(r['fa']) else 0):7.3f} "
                f"{status:>9} "
                f"{ret} "
                f"{r['token_mint'][:18]}"
            )

        print()
        print("="*120)
        print("CHECKPOINTS")
        print("="*120)

        print(
            "R1 binary 10  = only first look"
        )

        print(
            "R1 binary 25  = early evidence"
        )

        print(
            "R1 binary 50  = meaningful"
        )

        print(
            "R1 binary 100 = strong test"
        )

        print()
        print(
            "IMPORTANT:"
        )

        print(
            "• KMeans and preprocessing are frozen at ID<=417."
        )

        print(
            "• Future events only use predict()."
        )

        print(
            "• No future outcomes enter the regime assignment."
        )

        print(
            "• Do NOT change thresholds from this monitor."
        )

        print()
        print(
            "Refresh every 10 seconds."
        )

        time.sleep(10)

    except KeyboardInterrupt:

        print(
            "\nV6.2 forward validator stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)

db.close()
