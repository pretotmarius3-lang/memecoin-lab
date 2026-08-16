#!/usr/bin/env python3

import json
import math
import sqlite3
import statistics
import time
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path.home() / "memecoin_lab"

MARKET_DB = ROOT / "validation_v090.db"
RESEARCH_DB = ROOT / "research_v4.db"

SNAPSHOTS = [
    5,
    10,
    20,
    30,
    60,
    120,
]

SOURCE_TABLE = "t116_pump_swaps"


# ============================================================
# DB
# ============================================================

def market():

    db = sqlite3.connect(
        f"file:{MARKET_DB}?mode=ro",
        uri=True,
        timeout=30,
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")

    return db


def research():

    db = sqlite3.connect(
        RESEARCH_DB,
        timeout=30,
    )

    db.row_factory = sqlite3.Row

    db.execute(
        "PRAGMA journal_mode=WAL"
    )

    db.execute(
        "PRAGMA synchronous=NORMAL"
    )

    db.execute(
        "PRAGMA busy_timeout=30000"
    )

    return db


def table_exists(
    db,
    name
):

    return db.execute("""
    SELECT 1
    FROM sqlite_master
    WHERE
        type='table'
        AND name=?
    """, (
        name,
    )).fetchone() is not None


def columns(
    db,
    table
):

    return {
        row["name"]:
            row

        for row in db.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()
    }


# ============================================================
# UTILS
# ============================================================

def valid(x):

    return (
        x is not None
        and isinstance(
            x,
            (int, float)
        )
        and math.isfinite(x)
    )


def safe_mean(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    return (
        sum(xs) / len(xs)
        if xs
        else None
    )


def safe_median(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    return (
        statistics.median(xs)
        if xs
        else None
    )


def safe_std(xs):

    xs = [
        x for x in xs
        if valid(x)
    ]

    if len(xs) < 2:
        return 0.0

    return statistics.stdev(xs)


def hhi(values):

    values = [
        abs(x)
        for x in values
        if valid(x)
        and abs(x) > 0
    ]

    total = sum(values)

    if total <= 0:
        return None

    shares = [
        x / total
        for x in values
    ]

    return sum(
        s * s
        for s in shares
    )


def entropy(values):

    values = [
        abs(x)
        for x in values
        if valid(x)
        and abs(x) > 0
    ]

    total = sum(values)

    if total <= 0:
        return None

    e = 0.0

    for x in values:

        p = x / total

        if p > 0:

            e -= (
                p
                * math.log(p)
            )

    return e


def top_share(
    values,
    n
):

    values = sorted(
        (
            abs(x)
            for x in values
            if valid(x)
            and abs(x) > 0
        ),
        reverse=True,
    )

    total = sum(values)

    if total <= 0:
        return None

    return (
        sum(values[:n])
        / total
    )


# ============================================================
# COLUMN DISCOVERY
# ============================================================

ALIASES = {

    "mint": [
        "token_mint",
        "mint",
    ],

    "timestamp": [
        "timestamp",
        "block_time",
        "created_at",
    ],

    "signature": [
        "signature",
        "tx_signature",
        "txid",
    ],

    "side": [
        "side",
        "direction",
    ],

    "sol": [
        "sol_delta",
        "sol_amount",
        "sol",
    ],

    "price": [
        "raw_price_sol",
        "raw_price",
        "price_sol",
        "price",
    ],

    "wallet": [
        "wallet",
        "wallet_address",
        "user",
        "user_address",
        "owner",
        "trader",
        "trader_wallet",
        "signer",
        "fee_payer",
    ],
}


def pick_column(
    cols,
    logical_name
):

    for candidate in ALIASES[
        logical_name
    ]:

        if candidate in cols:
            return candidate

    return None


# ============================================================
# SCHEMA
# ============================================================

def initialize_output():

    db = research()

    db.executescript("""
    CREATE TABLE IF NOT EXISTS v4_onchain_snapshots (

        token_mint TEXT NOT NULL,

        snapshot_s INTEGER NOT NULL,

        birth_ts REAL,

        snapshot_ts REAL,

        swaps INTEGER,

        buys INTEGER,
        sells INTEGER,

        buy_ratio REAL,

        buy_sol REAL,
        sell_sol REAL,
        net_sol REAL,
        gross_sol REAL,

        avg_trade_sol REAL,
        median_trade_sol REAL,
        std_trade_sol REAL,

        max_trade_sol REAL,

        top1_trade_share REAL,
        top3_trade_share REAL,
        top5_trade_share REAL,

        trade_hhi REAL,
        trade_entropy REAL,

        unique_signatures INTEGER,

        unique_wallets INTEGER,

        unique_buyers INTEGER,
        unique_sellers INTEGER,

        buyer_seller_ratio REAL,

        wallet_top1_share REAL,
        wallet_top3_share REAL,
        wallet_top5_share REAL,

        wallet_hhi REAL,
        wallet_entropy REAL,

        repeat_wallet_ratio REAL,

        first_price REAL,
        last_price REAL,
        max_price REAL,
        min_price REAL,

        return_pct REAL,
        range_pct REAL,
        max_run_pct REAL,
        max_drawdown_pct REAL,

        buy_sol_per_swap REAL,
        net_sol_per_swap REAL,

        price_move_per_gross_sol REAL,
        price_move_per_net_sol REAL,

        first_half_net_sol REAL,
        second_half_net_sol REAL,

        net_flow_acceleration REAL,

        first_half_buy_ratio REAL,
        second_half_buy_ratio REAL,

        buy_ratio_change REAL,

        source_has_wallet INTEGER NOT NULL,

        created_at REAL NOT NULL,

        PRIMARY KEY (
            token_mint,
            snapshot_s
        )
    );


    CREATE TABLE IF NOT EXISTS v4_onchain_schema_audit (

        logical_field TEXT PRIMARY KEY,
        actual_column TEXT,
        available INTEGER NOT NULL,
        updated_at REAL NOT NULL
    );
    """)

    db.commit()
    db.close()


# ============================================================
# LOAD TOKEN SWAPS
# ============================================================

def load_all():

    db = market()

    if not table_exists(
        db,
        SOURCE_TABLE
    ):

        raise SystemExit(
            f"Missing table: {SOURCE_TABLE}"
        )

    cols = columns(
        db,
        SOURCE_TABLE
    )

    mapping = {
        logical:
            pick_column(
                cols,
                logical
            )

        for logical in ALIASES
    }


    print()
    print(
        "COLUMN MAPPING"
    )

    print(
        "-" * 90
    )


    for logical, actual in mapping.items():

        print(
            f"{logical:<12}"
            f" -> "
            f"{actual or 'MISSING'}"
        )


    required = [
        "mint",
        "timestamp",
        "side",
        "sol",
        "price",
    ]


    missing = [
        name
        for name in required
        if mapping[name] is None
    ]


    if missing:

        raise SystemExit(
            "Required fields missing: "
            + ", ".join(missing)
        )


    rdb = research()

    now = time.time()


    for logical in ALIASES:

        rdb.execute("""
        INSERT INTO v4_onchain_schema_audit (
            logical_field,
            actual_column,
            available,
            updated_at
        )

        VALUES (
            ?,?,?,?
        )

        ON CONFLICT(logical_field)
        DO UPDATE SET

            actual_column=
                excluded.actual_column,

            available=
                excluded.available,

            updated_at=
                excluded.updated_at
        """, (

            logical,

            mapping[
                logical
            ],

            int(
                mapping[
                    logical
                ]
                is not None
            ),

            now,
        ))


    rdb.commit()
    rdb.close()


    select = [

        f"{mapping['mint']} AS token_mint",

        f"{mapping['timestamp']} AS ts",

        f"{mapping['side']} AS side",

        f"{mapping['sol']} AS sol",

        f"{mapping['price']} AS price",
    ]


    if mapping["signature"]:

        select.append(
            f"{mapping['signature']} AS signature"
        )

    else:

        select.append(
            "NULL AS signature"
        )


    if mapping["wallet"]:

        select.append(
            f"{mapping['wallet']} AS wallet"
        )

    else:

        select.append(
            "NULL AS wallet"
        )


    sql = f"""
    SELECT
        {",".join(select)}

    FROM {SOURCE_TABLE}

    WHERE
        {mapping['mint']} IS NOT NULL
        AND {mapping['timestamp']} IS NOT NULL

    ORDER BY
        {mapping['mint']},
        {mapping['timestamp']}
    """


    rows = [
        dict(r)
        for r in db.execute(
            sql
        ).fetchall()
    ]


    db.close()


    tokens = defaultdict(
        list
    )


    for row in rows:

        tokens[
            row[
                "token_mint"
            ]
        ].append(
            row
        )


    return (
        tokens,
        mapping
    )


# ============================================================
# SNAPSHOT FEATURES
# ============================================================

def compute_snapshot(
    mint,
    swaps,
    snapshot_s,
    has_wallet
):

    if not swaps:
        return None


    birth_ts = swaps[0][
        "ts"
    ]


    cutoff = (
        birth_ts
        + snapshot_s
    )


    selected = [
        r
        for r in swaps
        if r[
            "ts"
        ] <= cutoff
    ]


    if not selected:
        return None


    clean = []


    for row in selected:

        sol = row[
            "sol"
        ]

        price = row[
            "price"
        ]


        if not valid(sol):
            continue


        side = str(
            row[
                "side"
            ]
            or ""
        ).upper()


        clean.append({
            **row,

            "sol_abs":
                abs(sol),

            "side_norm":
                side,
        })


    if not clean:
        return None


    buys = [
        r
        for r in clean
        if r[
            "side_norm"
        ] == "BUY"
    ]


    sells = [
        r
        for r in clean
        if r[
            "side_norm"
        ] == "SELL"
    ]


    buy_sol = sum(
        r[
            "sol_abs"
        ]
        for r in buys
    )


    sell_sol = sum(
        r[
            "sol_abs"
        ]
        for r in sells
    )


    gross_sol = (
        buy_sol
        + sell_sol
    )


    net_sol = (
        buy_sol
        - sell_sol
    )


    trade_sizes = [
        r[
            "sol_abs"
        ]
        for r in clean
    ]


    prices = [
        r[
            "price"
        ]
        for r in clean
        if valid(
            r[
                "price"
            ]
        )
        and r[
            "price"
        ] > 0
    ]


    signatures = {
        r[
            "signature"
        ]
        for r in clean
        if r[
            "signature"
        ]
    }


    wallet_volume = defaultdict(
        float
    )

    wallet_buys = set()
    wallet_sells = set()
    wallets = []


    if has_wallet:

        for row in clean:

            wallet = row[
                "wallet"
            ]

            if not wallet:
                continue


            wallets.append(
                wallet
            )


            wallet_volume[
                wallet
            ] += row[
                "sol_abs"
            ]


            if row[
                "side_norm"
            ] == "BUY":

                wallet_buys.add(
                    wallet
                )


            if row[
                "side_norm"
            ] == "SELL":

                wallet_sells.add(
                    wallet
                )


    wallet_values = list(
        wallet_volume.values()
    )


    unique_wallets = (
        len(
            wallet_volume
        )
        if has_wallet
        else None
    )


    repeat_wallet_ratio = None


    if (
        has_wallet
        and wallets
    ):

        counts = Counter(
            wallets
        )


        repeats = sum(
            count > 1
            for count in counts.values()
        )


        repeat_wallet_ratio = (
            repeats
            / len(counts)
            if counts
            else None
        )


    first_price = (
        prices[0]
        if prices
        else None
    )


    last_price = (
        prices[-1]
        if prices
        else None
    )


    max_price = (
        max(prices)
        if prices
        else None
    )


    min_price = (
        min(prices)
        if prices
        else None
    )


    return_pct = None
    range_pct = None
    max_run_pct = None
    max_drawdown_pct = None


    if (
        first_price
        and first_price > 0
        and last_price
    ):

        return_pct = (
            100.0
            * (
                last_price
                / first_price
                - 1.0
            )
        )


        range_pct = (
            100.0
            * (
                max_price
                / min_price
                - 1.0
            )
        )


        max_run_pct = (
            100.0
            * (
                max_price
                / first_price
                - 1.0
            )
        )


        peak = prices[0]

        worst = 0.0


        for price in prices:

            peak = max(
                peak,
                price
            )


            dd = (
                100.0
                * (
                    price
                    / peak
                    - 1.0
                )
            )


            worst = min(
                worst,
                dd
            )


        max_drawdown_pct = worst


    # --------------------------------------------------------
    # TIME STRUCTURE
    # --------------------------------------------------------

    midpoint = (
        birth_ts
        + snapshot_s / 2
    )


    first_half = [
        r
        for r in clean
        if r[
            "ts"
        ] <= midpoint
    ]


    second_half = [
        r
        for r in clean
        if r[
            "ts"
        ] > midpoint
    ]


    def half_flow(rows):

        b = sum(
            r[
                "sol_abs"
            ]
            for r in rows
            if r[
                "side_norm"
            ] == "BUY"
        )


        s = sum(
            r[
                "sol_abs"
            ]
            for r in rows
            if r[
                "side_norm"
            ] == "SELL"
        )


        n = len(
            rows
        )


        buy_count = sum(
            r[
                "side_norm"
            ] == "BUY"
            for r in rows
        )


        ratio = (
            buy_count / n
            if n
            else None
        )


        return (
            b - s,
            ratio,
        )


    first_net, first_ratio = (
        half_flow(
            first_half
        )
    )


    second_net, second_ratio = (
        half_flow(
            second_half
        )
    )


    net_flow_acceleration = (
        second_net
        - first_net
    )


    buy_ratio_change = None


    if (
        first_ratio is not None
        and second_ratio is not None
    ):

        buy_ratio_change = (
            second_ratio
            - first_ratio
        )


    n_swaps = len(
        clean
    )


    return {

        "token_mint":
            mint,

        "snapshot_s":
            snapshot_s,

        "birth_ts":
            birth_ts,

        "snapshot_ts":
            cutoff,

        "swaps":
            n_swaps,

        "buys":
            len(
                buys
            ),

        "sells":
            len(
                sells
            ),

        "buy_ratio":
            (
                len(buys)
                / n_swaps
                if n_swaps
                else None
            ),

        "buy_sol":
            buy_sol,

        "sell_sol":
            sell_sol,

        "net_sol":
            net_sol,

        "gross_sol":
            gross_sol,

        "avg_trade_sol":
            safe_mean(
                trade_sizes
            ),

        "median_trade_sol":
            safe_median(
                trade_sizes
            ),

        "std_trade_sol":
            safe_std(
                trade_sizes
            ),

        "max_trade_sol":
            max(
                trade_sizes
            )
            if trade_sizes
            else None,

        "top1_trade_share":
            top_share(
                trade_sizes,
                1
            ),

        "top3_trade_share":
            top_share(
                trade_sizes,
                3
            ),

        "top5_trade_share":
            top_share(
                trade_sizes,
                5
            ),

        "trade_hhi":
            hhi(
                trade_sizes
            ),

        "trade_entropy":
            entropy(
                trade_sizes
            ),

        "unique_signatures":
            len(
                signatures
            ),

        "unique_wallets":
            unique_wallets,

        "unique_buyers":
            (
                len(
                    wallet_buys
                )
                if has_wallet
                else None
            ),

        "unique_sellers":
            (
                len(
                    wallet_sells
                )
                if has_wallet
                else None
            ),

        "buyer_seller_ratio":
            (
                len(wallet_buys)
                / max(
                    1,
                    len(wallet_sells)
                )
                if has_wallet
                else None
            ),

        "wallet_top1_share":
            (
                top_share(
                    wallet_values,
                    1
                )
                if has_wallet
                else None
            ),

        "wallet_top3_share":
            (
                top_share(
                    wallet_values,
                    3
                )
                if has_wallet
                else None
            ),

        "wallet_top5_share":
            (
                top_share(
                    wallet_values,
                    5
                )
                if has_wallet
                else None
            ),

        "wallet_hhi":
            (
                hhi(
                    wallet_values
                )
                if has_wallet
                else None
            ),

        "wallet_entropy":
            (
                entropy(
                    wallet_values
                )
                if has_wallet
                else None
            ),

        "repeat_wallet_ratio":
            repeat_wallet_ratio,

        "first_price":
            first_price,

        "last_price":
            last_price,

        "max_price":
            max_price,

        "min_price":
            min_price,

        "return_pct":
            return_pct,

        "range_pct":
            range_pct,

        "max_run_pct":
            max_run_pct,

        "max_drawdown_pct":
            max_drawdown_pct,

        "buy_sol_per_swap":
            (
                buy_sol / n_swaps
                if n_swaps
                else None
            ),

        "net_sol_per_swap":
            (
                net_sol / n_swaps
                if n_swaps
                else None
            ),

        "price_move_per_gross_sol":
            (
                return_pct
                / gross_sol
                if (
                    return_pct is not None
                    and gross_sol > 0
                )
                else None
            ),

        "price_move_per_net_sol":
            (
                return_pct
                / abs(net_sol)
                if (
                    return_pct is not None
                    and abs(net_sol) > 1e-12
                )
                else None
            ),

        "first_half_net_sol":
            first_net,

        "second_half_net_sol":
            second_net,

        "net_flow_acceleration":
            net_flow_acceleration,

        "first_half_buy_ratio":
            first_ratio,

        "second_half_buy_ratio":
            second_ratio,

        "buy_ratio_change":
            buy_ratio_change,

        "source_has_wallet":
            int(
                has_wallet
            ),

        "created_at":
            time.time(),
    }


# ============================================================
# WRITE
# ============================================================

FIELDS = [

    "token_mint",
    "snapshot_s",

    "birth_ts",
    "snapshot_ts",

    "swaps",
    "buys",
    "sells",

    "buy_ratio",

    "buy_sol",
    "sell_sol",
    "net_sol",
    "gross_sol",

    "avg_trade_sol",
    "median_trade_sol",
    "std_trade_sol",

    "max_trade_sol",

    "top1_trade_share",
    "top3_trade_share",
    "top5_trade_share",

    "trade_hhi",
    "trade_entropy",

    "unique_signatures",

    "unique_wallets",
    "unique_buyers",
    "unique_sellers",

    "buyer_seller_ratio",

    "wallet_top1_share",
    "wallet_top3_share",
    "wallet_top5_share",

    "wallet_hhi",
    "wallet_entropy",

    "repeat_wallet_ratio",

    "first_price",
    "last_price",
    "max_price",
    "min_price",

    "return_pct",
    "range_pct",
    "max_run_pct",
    "max_drawdown_pct",

    "buy_sol_per_swap",
    "net_sol_per_swap",

    "price_move_per_gross_sol",
    "price_move_per_net_sol",

    "first_half_net_sol",
    "second_half_net_sol",

    "net_flow_acceleration",

    "first_half_buy_ratio",
    "second_half_buy_ratio",

    "buy_ratio_change",

    "source_has_wallet",

    "created_at",
]


def store(
    rows
):

    db = research()


    placeholders = ",".join(
        "?"
        for _ in FIELDS
    )


    update_fields = [
        field
        for field in FIELDS
        if field not in (
            "token_mint",
            "snapshot_s",
        )
    ]


    update_sql = ",".join(
        f"{field}=excluded.{field}"
        for field in update_fields
    )


    sql = f"""
    INSERT INTO v4_onchain_snapshots (
        {",".join(FIELDS)}
    )

    VALUES (
        {placeholders}
    )

    ON CONFLICT(
        token_mint,
        snapshot_s
    )

    DO UPDATE SET
        {update_sql}
    """


    db.executemany(
        sql,

        [
            tuple(
                row.get(
                    field
                )
                for field in FIELDS
            )

            for row in rows
        ],
    )


    db.commit()
    db.close()


# ============================================================
# REPORT
# ============================================================

def report(
    mapping
):

    db = research()


    total = db.execute("""
    SELECT COUNT(*)
    FROM v4_onchain_snapshots
    """).fetchone()[0]


    tokens = db.execute("""
    SELECT COUNT(
        DISTINCT token_mint
    )
    FROM v4_onchain_snapshots
    """).fetchone()[0]


    rows = db.execute("""
    SELECT
        snapshot_s,
        COUNT(*) AS n,

        ROUND(
            AVG(swaps),
            2
        ) AS avg_swaps,

        ROUND(
            AVG(gross_sol),
            4
        ) AS avg_gross_sol,

        ROUND(
            AVG(return_pct),
            2
        ) AS avg_return,

        ROUND(
            AVG(
                net_flow_acceleration
            ),
            4
        ) AS avg_accel,

        SUM(
            CASE
                WHEN unique_wallets IS NOT NULL
                THEN 1
                ELSE 0
            END
        ) AS wallet_rows

    FROM v4_onchain_snapshots

    GROUP BY snapshot_s

    ORDER BY snapshot_s
    """).fetchall()


    db.close()


    print()
    print("=" * 130)

    print(
        "V4 ON-CHAIN FEATURE ENGINE — COMPLETE"
    )

    print("=" * 130)


    print(
        f"UNIQUE TOKENS : {tokens}"
    )

    print(
        f"SNAPSHOT ROWS : {total}"
    )


    print()

    print(
        f"WALLET COLUMN : "
        f"{mapping['wallet'] or 'NOT AVAILABLE'}"
    )


    print()

    print(
        f"{'STAGE':>7}"
        f"{'ROWS':>9}"
        f"{'AVG SWAPS':>13}"
        f"{'AVG SOL':>13}"
        f"{'AVG RET%':>12}"
        f"{'FLOW ACC':>13}"
        f"{'WALLET ROWS':>14}"
    )


    for row in rows:

        print(
            f"{row['snapshot_s']:>7}"
            f"{row['n']:>9}"
            f"{row['avg_swaps'] or 0:>13.2f}"
            f"{row['avg_gross_sol'] or 0:>13.4f}"
            f"{row['avg_return'] or 0:>12.2f}"
            f"{row['avg_accel'] or 0:>13.4f}"
            f"{row['wallet_rows']:>14}"
        )


    print()
    print("=" * 130)

    if mapping[
        "wallet"
    ]:

        print(
            "🟢 WALLET-LEVEL MICROSTRUCTURE AVAILABLE NOW"
        )

    else:

        print(
            "🟡 SWAP MICROSTRUCTURE AVAILABLE"
        )

        print(
            "🔴 WALLET IDENTITY NOT PRESENT IN t116_pump_swaps"
        )

        print(
            "Next collector upgrade must record trader / signer / fee payer."
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 130)

    print(
        "MEMECOIN LAB — V4 ON-CHAIN MICROSTRUCTURE FEATURE ENGINE"
    )

    print("=" * 130)


    initialize_output()


    tokens, mapping = (
        load_all()
    )


    print()

    print(
        f"TOKENS FOUND : {len(tokens)}"
    )


    output = []


    for i, (
        mint,
        swaps
    ) in enumerate(
        tokens.items(),
        start=1,
    ):

        for snapshot_s in SNAPSHOTS:

            row = compute_snapshot(
                mint,
                swaps,
                snapshot_s,
                mapping[
                    "wallet"
                ] is not None,
            )


            if row:

                output.append(
                    row
                )


        if i % 250 == 0:

            print(
                f"Processed {i:,}"
                f"/{len(tokens):,} tokens"
            )


    store(
        output
    )


    report(
        mapping
    )


if __name__ == "__main__":

    main()
