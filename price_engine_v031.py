import sqlite3
import statistics
from collections import defaultdict

DB_FILE = "memecoin_lab_sampler.db"

# Actifs qu'on ne veut pas analyser comme memecoins
EXCLUDED_MINTS = {
    # WSOL
    "So11111111111111111111111111111111111111112",

    # USDC
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",

    # USDT
    "Es9vMFrzaCERmJfrF4H2FYD7iBFGYpVbR9U4kXQfM8g",
}

# Contrôles de cohérence
MAX_JUMP_RATIO = 5.0
MIN_JUMP_RATIO = 0.20

# Au moins quelques observations pour établir une référence
MIN_HISTORY = 3


db = sqlite3.connect(
    DB_FILE,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# CLEAN TABLE
# ============================================================

db.execute("""
DROP TABLE IF EXISTS clean_swaps
""")

db.execute("""
CREATE TABLE clean_swaps (

    signature TEXT PRIMARY KEY,

    timestamp REAL,

    slot INTEGER,

    program TEXT,

    wallet TEXT,

    side TEXT,

    token_mint TEXT,

    token_delta REAL,

    sol_delta REAL,

    raw_price REAL,

    clean_price REAL,

    price_valid INTEGER,

    reject_reason TEXT
)
""")

db.execute("""
CREATE INDEX idx_clean_token_time
ON clean_swaps(token_mint, timestamp)
""")

db.commit()


# ============================================================
# LOAD RAW
# ============================================================

rows = db.execute("""
SELECT
    signature,
    timestamp,
    slot,
    program,
    wallet,
    side,
    token_mint,
    token_delta,
    sol_delta,
    price_sol

FROM swaps

ORDER BY timestamp ASC
""").fetchall()


print()
print("=" * 90)
print("MEMECOIN LAB — PRICE ENGINE V0.3.1")
print("=" * 90)
print(f"RAW SWAPS : {len(rows):,}")
print()


# ============================================================
# HISTORY PER TOKEN
# ============================================================

history = defaultdict(list)

stats = defaultdict(int)


# ============================================================
# PROCESS
# ============================================================

for row in rows:

    mint = row["token_mint"]

    raw_price = row["price_sol"]

    valid = True
    reason = None
    clean_price = None


    # --------------------------------------------------------
    # EXCLUDED TOKEN
    # --------------------------------------------------------

    if mint in EXCLUDED_MINTS:

        valid = False
        reason = "EXCLUDED_ASSET"

        stats["excluded_asset"] += 1


    # --------------------------------------------------------
    # BASIC PRICE VALIDITY
    # --------------------------------------------------------

    elif (
        raw_price is None
        or raw_price <= 0
    ):

        valid = False
        reason = "INVALID_PRICE"

        stats["invalid_price"] += 1


    # --------------------------------------------------------
    # TOKEN HISTORY CHECK
    # --------------------------------------------------------

    else:

        previous = history[mint]

        if len(previous) >= MIN_HISTORY:

            reference_window = (
                previous[-15:]
            )

            reference = (
                statistics.median(
                    reference_window
                )
            )

            if reference <= 0:

                valid = False
                reason = "INVALID_REFERENCE"

                stats[
                    "invalid_reference"
                ] += 1

            else:

                ratio = (
                    raw_price
                    / reference
                )

                if ratio > MAX_JUMP_RATIO:

                    valid = False
                    reason = "PRICE_SPIKE"

                    stats[
                        "price_spike"
                    ] += 1

                elif ratio < MIN_JUMP_RATIO:

                    valid = False
                    reason = "PRICE_CRASH"

                    stats[
                        "price_crash"
                    ] += 1


    # --------------------------------------------------------
    # ACCEPT
    # --------------------------------------------------------

    if valid:

        clean_price = raw_price

        history[mint].append(
            raw_price
        )

        stats["accepted"] += 1

    else:

        stats["rejected"] += 1


    db.execute("""
    INSERT INTO clean_swaps
    (
        signature,
        timestamp,
        slot,
        program,
        wallet,
        side,
        token_mint,
        token_delta,
        sol_delta,
        raw_price,
        clean_price,
        price_valid,
        reject_reason
    )

    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        row["signature"],
        row["timestamp"],
        row["slot"],
        row["program"],
        row["wallet"],
        row["side"],
        mint,
        row["token_delta"],
        row["sol_delta"],
        raw_price,
        clean_price,
        int(valid),
        reason
    ))


db.commit()


# ============================================================
# REPORT
# ============================================================

total = len(rows)

accepted = stats["accepted"]

rejected = stats["rejected"]


print("=" * 90)
print("CLEANING RESULTS")
print("=" * 90)

print(
    f"TOTAL       : {total:,}"
)

print(
    f"ACCEPTED    : {accepted:,}"
)

print(
    f"REJECTED    : {rejected:,}"
)

if total:

    print(
        f"KEEP RATE   : "
        f"{accepted / total * 100:.2f}%"
    )


print()
print("REJECTION REASONS")
print("-" * 90)

for key in [
    "excluded_asset",
    "invalid_price",
    "invalid_reference",
    "price_spike",
    "price_crash"
]:

    print(
        f"{key:20} : "
        f"{stats[key]:,}"
    )


# ============================================================
# TOKENS
# ============================================================

raw_tokens = db.execute("""
SELECT COUNT(DISTINCT token_mint)
FROM swaps
""").fetchone()[0]

clean_tokens = db.execute("""
SELECT COUNT(DISTINCT token_mint)
FROM clean_swaps
WHERE price_valid = 1
""").fetchone()[0]


print()
print(
    f"RAW TOKENS   : {raw_tokens:,}"
)

print(
    f"CLEAN TOKENS : {clean_tokens:,}"
)


# ============================================================
# TOP REJECTED TOKENS
# ============================================================

print()
print("=" * 90)
print("TOP REJECTED TOKENS")
print("=" * 90)

rejected_rows = db.execute("""
SELECT
    token_mint,
    COUNT(*) AS n

FROM clean_swaps

WHERE price_valid = 0

GROUP BY token_mint

ORDER BY n DESC

LIMIT 15
""").fetchall()


for r in rejected_rows:

    print(
        f"{r['token_mint']} | "
        f"REJECTED={r['n']}"
    )


# ============================================================
# CLEAN PRICE RANGE
# ============================================================

print()
print("=" * 90)
print("CLEAN DATA CHECK")
print("=" * 90)

check = db.execute("""
SELECT
    COUNT(*) AS n,
    MIN(clean_price) AS min_price,
    MAX(clean_price) AS max_price

FROM clean_swaps

WHERE price_valid = 1
""").fetchone()


print(
    f"VALID SWAPS : {check['n']:,}"
)

print(
    f"MIN PRICE   : {check['min_price']}"
)

print(
    f"MAX PRICE   : {check['max_price']}"
)


# ============================================================
# PER TOKEN PRICE CONSISTENCY
# ============================================================

print()
print("=" * 90)
print("TOP TOKENS — CLEAN ACTIVITY")
print("=" * 90)

top = db.execute("""
SELECT
    token_mint,
    COUNT(*) AS n,
    COUNT(DISTINCT wallet) AS wallets,
    SUM(
        CASE
        WHEN side='BUY'
        THEN 1
        ELSE 0
        END
    ) AS buys,
    SUM(
        CASE
        WHEN side='SELL'
        THEN 1
        ELSE 0
        END
    ) AS sells

FROM clean_swaps

WHERE price_valid = 1

GROUP BY token_mint

ORDER BY n DESC

LIMIT 15
""").fetchall()


for r in top:

    print(
        f"{r['token_mint']} | "
        f"N={r['n']:>4} | "
        f"W={r['wallets']:>4} | "
        f"B={r['buys']:>4} | "
        f"S={r['sells']:>4}"
    )


print()
print("=" * 90)
print("PRICE ENGINE DONE")
print("=" * 90)

db.close()
