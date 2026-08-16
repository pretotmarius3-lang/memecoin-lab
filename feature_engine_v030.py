import sqlite3
import time
import statistics
from collections import defaultdict

DB_FILE = "memecoin_lab_sampler.db"

WINDOWS = [5, 10, 30, 60]

SNAPSHOT_INTERVAL = 5

MIN_SWAPS_TOKEN = 2

db = sqlite3.connect(
    DB_FILE,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# SNAPSHOT TABLE
# ============================================================

db.execute("""
CREATE TABLE IF NOT EXISTS feature_snapshots (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    timestamp REAL,

    token_mint TEXT,

    last_price REAL,

    total_swaps INTEGER,

    buyers_5 INTEGER,
    sellers_5 INTEGER,
    buy_vol_5 REAL,
    sell_vol_5 REAL,
    net_flow_5 REAL,
    imbalance_5 REAL,

    buyers_10 INTEGER,
    sellers_10 INTEGER,
    buy_vol_10 REAL,
    sell_vol_10 REAL,
    net_flow_10 REAL,
    imbalance_10 REAL,

    buyers_30 INTEGER,
    sellers_30 INTEGER,
    buy_vol_30 REAL,
    sell_vol_30 REAL,
    net_flow_30 REAL,
    imbalance_30 REAL,

    buyers_60 INTEGER,
    sellers_60 INTEGER,
    buy_vol_60 REAL,
    sell_vol_60 REAL,
    net_flow_60 REAL,
    imbalance_60 REAL,

    buyer_velocity_5 REAL,
    buyer_velocity_10 REAL,
    buyer_velocity_30 REAL,

    buyer_accel_fast REAL,
    buyer_accel_slow REAL,

    flow_velocity_5 REAL,
    flow_velocity_10 REAL,
    flow_velocity_30 REAL,

    flow_accel_fast REAL,
    flow_accel_slow REAL,

    median_buy_size_30 REAL,

    buys_over_005_30 INTEGER,
    buys_over_025_30 INTEGER,
    buys_over_1_30 INTEGER,

    price_change_10 REAL,
    price_change_30 REAL,
    price_change_60 REAL,

    UNIQUE(timestamp, token_mint)
)
""")

db.execute("""
CREATE INDEX IF NOT EXISTS idx_features_token_time
ON feature_snapshots(token_mint, timestamp)
""")

db.commit()


# ============================================================
# LOAD SWAPS
# ============================================================

def load_recent_swaps():

    cutoff = time.time() - 180

    rows = db.execute("""
        SELECT
            timestamp,
            wallet,
            side,
            token_mint,
            sol_delta,
            price_sol
        FROM swaps
        WHERE timestamp >= ?
        ORDER BY timestamp ASC
    """, (cutoff,)).fetchall()

    events = defaultdict(list)

    for row in rows:

        events[row["token_mint"]].append({
            "time": row["timestamp"],
            "wallet": row["wallet"],
            "side": row["side"],
            "sol": abs(row["sol_delta"]),
            "price": row["price_sol"]
        })

    return events


# ============================================================
# WINDOW STATS
# ============================================================

def window_stats(
    events,
    now,
    seconds
):

    cutoff = now - seconds

    data = [
        e for e in events
        if cutoff <= e["time"] <= now
    ]

    buys = [
        e for e in data
        if e["side"] == "BUY"
    ]

    sells = [
        e for e in data
        if e["side"] == "SELL"
    ]

    buyers = set(
        e["wallet"]
        for e in buys
    )

    sellers = set(
        e["wallet"]
        for e in sells
    )

    buy_vol = sum(
        e["sol"]
        for e in buys
    )

    sell_vol = sum(
        e["sol"]
        for e in sells
    )

    net_flow = (
        buy_vol
        - sell_vol
    )

    total_vol = (
        buy_vol
        + sell_vol
    )

    imbalance = (
        net_flow / total_vol
        if total_vol > 0
        else 0.0
    )

    buy_sizes = [
        e["sol"]
        for e in buys
    ]

    median_buy = (
        statistics.median(
            buy_sizes
        )
        if buy_sizes
        else 0.0
    )

    return {
        "buyers": len(buyers),
        "sellers": len(sellers),

        "buy_vol": buy_vol,
        "sell_vol": sell_vol,

        "net_flow": net_flow,
        "imbalance": imbalance,

        "median_buy": median_buy,

        "over005": sum(
            1
            for x in buy_sizes
            if x >= 0.05
        ),

        "over025": sum(
            1
            for x in buy_sizes
            if x >= 0.25
        ),

        "over1": sum(
            1
            for x in buy_sizes
            if x >= 1.0
        ),
    }


# ============================================================
# PRICE CHANGE
# ============================================================

def price_at_or_before(
    events,
    target
):

    candidate = None

    for event in events:

        if (
            event["time"]
            <= target
        ):

            candidate = (
                event["price"]
            )

        else:

            break

    return candidate


def price_change(
    events,
    now,
    seconds,
    current_price
):

    old_price = (
        price_at_or_before(
            events,
            now - seconds
        )
    )

    if (
        old_price is None
        or old_price <= 0
        or current_price <= 0
    ):

        return None

    return (
        current_price
        / old_price
        - 1
    ) * 100


# ============================================================
# CREATE SNAPSHOT
# ============================================================

def create_snapshot(
    token,
    events,
    now
):

    if len(events) < MIN_SWAPS_TOKEN:

        return False

    latest = events[-1]

    current_price = (
        latest["price"]
    )

    if (
        current_price is None
        or current_price <= 0
    ):

        return False


    s5 = window_stats(
        events,
        now,
        5
    )

    s10 = window_stats(
        events,
        now,
        10
    )

    s30 = window_stats(
        events,
        now,
        30
    )

    s60 = window_stats(
        events,
        now,
        60
    )


    # --------------------------------------------------------
    # BUYER VELOCITY
    # --------------------------------------------------------

    bv5 = (
        s5["buyers"] / 5
    )

    bv10 = (
        s10["buyers"] / 10
    )

    bv30 = (
        s30["buyers"] / 30
    )


    buyer_accel_fast = (
        bv5 - bv10
    )

    buyer_accel_slow = (
        bv10 - bv30
    )


    # --------------------------------------------------------
    # FLOW VELOCITY
    # --------------------------------------------------------

    fv5 = (
        s5["net_flow"] / 5
    )

    fv10 = (
        s10["net_flow"] / 10
    )

    fv30 = (
        s30["net_flow"] / 30
    )


    flow_accel_fast = (
        fv5 - fv10
    )

    flow_accel_slow = (
        fv10 - fv30
    )


    pc10 = price_change(
        events,
        now,
        10,
        current_price
    )

    pc30 = price_change(
        events,
        now,
        30,
        current_price
    )

    pc60 = price_change(
        events,
        now,
        60,
        current_price
    )


    try:

        db.execute("""
        INSERT OR IGNORE INTO feature_snapshots
        (
            timestamp,
            token_mint,
            last_price,
            total_swaps,

            buyers_5,
            sellers_5,
            buy_vol_5,
            sell_vol_5,
            net_flow_5,
            imbalance_5,

            buyers_10,
            sellers_10,
            buy_vol_10,
            sell_vol_10,
            net_flow_10,
            imbalance_10,

            buyers_30,
            sellers_30,
            buy_vol_30,
            sell_vol_30,
            net_flow_30,
            imbalance_30,

            buyers_60,
            sellers_60,
            buy_vol_60,
            sell_vol_60,
            net_flow_60,
            imbalance_60,

            buyer_velocity_5,
            buyer_velocity_10,
            buyer_velocity_30,

            buyer_accel_fast,
            buyer_accel_slow,

            flow_velocity_5,
            flow_velocity_10,
            flow_velocity_30,

            flow_accel_fast,
            flow_accel_slow,

            median_buy_size_30,

            buys_over_005_30,
            buys_over_025_30,
            buys_over_1_30,

            price_change_10,
            price_change_30,
            price_change_60
        )

        VALUES (
            ?,?,?,?,
            ?,?,?,?,?,?,
            ?,?,?,?,?,?,
            ?,?,?,?,?,?,
            ?,?,?,?,?,?,
            ?,?,?,
            ?,?,
            ?,?,?,
            ?,?,
            ?,
            ?,?,?,
            ?,?,?
        )
        """,
        (
            now,
            token,
            current_price,
            len(events),

            s5["buyers"],
            s5["sellers"],
            s5["buy_vol"],
            s5["sell_vol"],
            s5["net_flow"],
            s5["imbalance"],

            s10["buyers"],
            s10["sellers"],
            s10["buy_vol"],
            s10["sell_vol"],
            s10["net_flow"],
            s10["imbalance"],

            s30["buyers"],
            s30["sellers"],
            s30["buy_vol"],
            s30["sell_vol"],
            s30["net_flow"],
            s30["imbalance"],

            s60["buyers"],
            s60["sellers"],
            s60["buy_vol"],
            s60["sell_vol"],
            s60["net_flow"],
            s60["imbalance"],

            bv5,
            bv10,
            bv30,

            buyer_accel_fast,
            buyer_accel_slow,

            fv5,
            fv10,
            fv30,

            flow_accel_fast,
            flow_accel_slow,

            s30["median_buy"],

            s30["over005"],
            s30["over025"],
            s30["over1"],

            pc10,
            pc30,
            pc60
        ))

        db.commit()

        return True

    except sqlite3.OperationalError:

        return False


# ============================================================
# MONITOR
# ============================================================

def database_stats():

    swaps = db.execute("""
        SELECT COUNT(*)
        FROM swaps
    """).fetchone()[0]

    tokens = db.execute("""
        SELECT COUNT(DISTINCT token_mint)
        FROM swaps
    """).fetchone()[0]

    snapshots = db.execute("""
        SELECT COUNT(*)
        FROM feature_snapshots
    """).fetchone()[0]

    feature_tokens = db.execute("""
        SELECT COUNT(DISTINCT token_mint)
        FROM feature_snapshots
    """).fetchone()[0]

    return (
        swaps,
        tokens,
        snapshots,
        feature_tokens
    )


# ============================================================
# LIVE LOOP
# ============================================================

def main():

    print()
    print("=" * 85)
    print(
        "MEMECOIN LAB — FEATURE ENGINE V0.3"
    )
    print("=" * 85)

    print(
        f"Snapshot every : "
        f"{SNAPSHOT_INTERVAL}s"
    )

    print(
        "Windows        : "
        "5 / 10 / 30 / 60s"
    )

    print(
        f"Database       : {DB_FILE}"
    )

    print("=" * 85)

    while True:

        start = time.time()

        events_by_token = (
            load_recent_swaps()
        )

        now = time.time()

        created = 0

        for (
            token,
            events
        ) in events_by_token.items():

            if create_snapshot(
                token,
                events,
                now
            ):

                created += 1

        (
            swaps,
            tokens,
            snapshots,
            feature_tokens
        ) = database_stats()

        print()
        print(
            "─" * 85
        )

        print(
            f"SWAPS {swaps:,}"
            f" | TOKENS {tokens:,}"
            f" | ACTIVE {len(events_by_token):,}"
        )

        print(
            f"SNAPSHOTS {snapshots:,}"
            f" | FEATURE TOKENS {feature_tokens:,}"
            f" | CREATED +{created:,}"
        )

        if events_by_token:

            ranked = sorted(
                events_by_token.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )

            token, events = ranked[0]

            now2 = time.time()

            s10 = window_stats(
                events,
                now2,
                10
            )

            s30 = window_stats(
                events,
                now2,
                30
            )

            bv10 = (
                s10["buyers"]
                / 10
            )

            bv30 = (
                s30["buyers"]
                / 30
            )

            ba = (
                bv10 - bv30
            )

            fv10 = (
                s10["net_flow"]
                / 10
            )

            fv30 = (
                s30["net_flow"]
                / 30
            )

            fa = (
                fv10 - fv30
            )

            print()
            print(
                f"TOP TOKEN : "
                f"{token[:12]}..."
            )

            print(
                f"BUYERS 10s {s10['buyers']}"
                f" | BUYERS 30s {s30['buyers']}"
                f" | BA {ba:+.4f}"
            )

            print(
                f"NETFLOW 10s "
                f"{s10['net_flow']:+.4f} SOL"
                f" | 30s "
                f"{s30['net_flow']:+.4f} SOL"
            )

            print(
                f"FLOW ACCEL "
                f"{fa:+.5f} SOL/s"
            )

        elapsed = (
            time.time()
            - start
        )

        sleep_for = max(
            0.5,
            SNAPSHOT_INTERVAL
            - elapsed
        )

        time.sleep(
            sleep_for
        )


if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print(
            "\nFeature Engine stopped."
        )

    finally:

        db.commit()
        db.close()
