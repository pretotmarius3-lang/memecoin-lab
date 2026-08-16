import sqlite3
import time

DB_FILE = "memecoin_lab_sampler.db"

INTERVAL = 5

db = sqlite3.connect(
    DB_FILE,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# OUTCOME COLUMNS
# ============================================================

columns = {
    "return_10s": "REAL",
    "return_30s": "REAL",
    "return_60s": "REAL",
    "return_300s": "REAL",

    "outcome_10s_done": "INTEGER DEFAULT 0",
    "outcome_30s_done": "INTEGER DEFAULT 0",
    "outcome_60s_done": "INTEGER DEFAULT 0",
    "outcome_300s_done": "INTEGER DEFAULT 0",
}

existing = {
    row["name"]
    for row in db.execute(
        "PRAGMA table_info(feature_snapshots)"
    )
}

for name, dtype in columns.items():

    if name not in existing:

        db.execute(
            f"""
            ALTER TABLE feature_snapshots
            ADD COLUMN {name} {dtype}
            """
        )

db.commit()


# ============================================================
# FIND FUTURE PRICE
# ============================================================

def future_price(
    token,
    target_time
):

    # First swap AT or AFTER target
    row = db.execute(
        """
        SELECT
            timestamp,
            price_sol

        FROM swaps

        WHERE
            token_mint = ?
            AND timestamp >= ?

        ORDER BY timestamp ASC

        LIMIT 1
        """,
        (
            token,
            target_time
        )
    ).fetchone()

    if not row:
        return None

    # Don't accept a price that's too far from target.
    # Prevents "+10s" becoming "+3 minutes".
    if (
        row["timestamp"]
        - target_time
        > 15
    ):
        return None

    return row["price_sol"]


# ============================================================
# CALCULATE RETURN
# ============================================================

def calculate_return(
    start_price,
    future
):

    if (
        start_price is None
        or future is None
        or start_price <= 0
        or future <= 0
    ):

        return None

    return (
        future / start_price
        - 1
    ) * 100


# ============================================================
# PROCESS HORIZON
# ============================================================

def process_horizon(
    seconds,
    return_column,
    done_column
):

    now = time.time()

    # Snapshot must be old enough
    rows = db.execute(
        f"""
        SELECT
            id,
            timestamp,
            token_mint,
            last_price

        FROM feature_snapshots

        WHERE
            {done_column} = 0
            AND timestamp <= ?

        ORDER BY timestamp ASC

        LIMIT 5000
        """,
        (
            now - seconds,
        )
    ).fetchall()

    updated = 0
    missing = 0

    for row in rows:

        target = (
            row["timestamp"]
            + seconds
        )

        fp = future_price(
            row["token_mint"],
            target
        )

        ret = calculate_return(
            row["last_price"],
            fp
        )

        # Mark done even if unavailable.
        # Avoid rechecking forever.
        db.execute(
            f"""
            UPDATE feature_snapshots

            SET
                {return_column} = ?,
                {done_column} = 1

            WHERE id = ?
            """,
            (
                ret,
                row["id"]
            )
        )

        if ret is None:

            missing += 1

        else:

            updated += 1

    db.commit()

    return (
        updated,
        missing
    )


# ============================================================
# STATS
# ============================================================

def stats():

    total = db.execute(
        """
        SELECT COUNT(*)
        FROM feature_snapshots
        """
    ).fetchone()[0]

    r10 = db.execute(
        """
        SELECT COUNT(*)
        FROM feature_snapshots
        WHERE return_10s IS NOT NULL
        """
    ).fetchone()[0]

    r30 = db.execute(
        """
        SELECT COUNT(*)
        FROM feature_snapshots
        WHERE return_30s IS NOT NULL
        """
    ).fetchone()[0]

    r60 = db.execute(
        """
        SELECT COUNT(*)
        FROM feature_snapshots
        WHERE return_60s IS NOT NULL
        """
    ).fetchone()[0]

    r300 = db.execute(
        """
        SELECT COUNT(*)
        FROM feature_snapshots
        WHERE return_300s IS NOT NULL
        """
    ).fetchone()[0]

    return (
        total,
        r10,
        r30,
        r60,
        r300
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 85)
    print(
        "MEMECOIN LAB — OUTCOME ENGINE V0.4"
    )
    print("=" * 85)

    print(
        "Horizons : +10s / +30s / +60s / +300s"
    )

    print(
        f"Database : {DB_FILE}"
    )

    print("=" * 85)

    while True:

        u10, m10 = process_horizon(
            10,
            "return_10s",
            "outcome_10s_done"
        )

        u30, m30 = process_horizon(
            30,
            "return_30s",
            "outcome_30s_done"
        )

        u60, m60 = process_horizon(
            60,
            "return_60s",
            "outcome_60s_done"
        )

        u300, m300 = process_horizon(
            300,
            "return_300s",
            "outcome_300s_done"
        )

        (
            total,
            r10,
            r30,
            r60,
            r300
        ) = stats()

        print()
        print("─" * 85)

        print(
            f"SNAPSHOTS {total:,}"
        )

        print(
            f"RETURNS "
            f"10s={r10:,}"
            f" | 30s={r30:,}"
            f" | 60s={r60:,}"
            f" | 300s={r300:,}"
        )

        print(
            f"NEW "
            f"10s=+{u10:,}"
            f" | 30s=+{u30:,}"
            f" | 60s=+{u60:,}"
            f" | 300s=+{u300:,}"
        )

        print(
            f"MISSING "
            f"10s={m10:,}"
            f" | 30s={m30:,}"
            f" | 60s={m60:,}"
            f" | 300s={m300:,}"
        )

        time.sleep(
            INTERVAL
        )


if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print(
            "\nOutcome Engine stopped."
        )

    finally:

        db.commit()
        db.close()
