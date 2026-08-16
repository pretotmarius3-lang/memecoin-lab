import sqlite3
from collections import defaultdict
from datetime import datetime

DB = "memecoin_lab_v02.db"

WINDOWS = [10, 30, 60]

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row


def load_swaps():
    return conn.execute("""
        SELECT
            timestamp,
            wallet,
            side,
            token_mint,
            token_delta,
            sol_delta
        FROM swaps
        WHERE side IN ('BUY', 'SELL')
        ORDER BY timestamp ASC
    """).fetchall()


def parse_time(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    ).timestamp()


def build_events(rows):

    events = defaultdict(list)

    for row in rows:

        token_amount = abs(row["token_delta"])
        sol_amount = abs(row["sol_delta"])

        if token_amount <= 0 or sol_amount <= 0:
            continue

        price = sol_amount / token_amount

        events[row["token_mint"]].append({
            "time": parse_time(row["timestamp"]),
            "wallet": row["wallet"],
            "side": row["side"],
            "sol": sol_amount,
            "tokens": token_amount,
            "price": price
        })

    return events


def compute_window(token_events, now, seconds):

    start = now - seconds

    data = [
        e for e in token_events
        if start <= e["time"] <= now
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
        e["wallet"] for e in buys
    )

    sellers = set(
        e["wallet"] for e in sells
    )

    buy_volume = sum(
        e["sol"] for e in buys
    )

    sell_volume = sum(
        e["sol"] for e in sells
    )

    total_volume = (
        buy_volume + sell_volume
    )

    net_flow = (
        buy_volume - sell_volume
    )

    if total_volume > 0:
        imbalance = (
            net_flow / total_volume
        )
    else:
        imbalance = 0

    # Anti-dust metrics

    real_buys_005 = [
        e for e in buys
        if e["sol"] >= 0.05
    ]

    real_buys_025 = [
        e for e in buys
        if e["sol"] >= 0.25
    ]

    real_buys_1 = [
        e for e in buys
        if e["sol"] >= 1
    ]

    return {
        "buyers": len(buyers),
        "sellers": len(sellers),

        "buy_tx": len(buys),
        "sell_tx": len(sells),

        "buy_volume": buy_volume,
        "sell_volume": sell_volume,

        "net_flow": net_flow,
        "imbalance": imbalance,

        "buys_005": len(real_buys_005),
        "buys_025": len(real_buys_025),
        "buys_1": len(real_buys_1),
    }


def price_change(token_events, now, seconds):

    current_candidates = [
        e for e in token_events
        if e["time"] <= now
    ]

    if not current_candidates:
        return 0

    current_price = (
        current_candidates[-1]["price"]
    )

    target = now - seconds

    previous_candidates = [
        e for e in token_events
        if e["time"] <= target
    ]

    if not previous_candidates:
        return 0

    old_price = (
        previous_candidates[-1]["price"]
    )

    if old_price <= 0:
        return 0

    return (
        current_price / old_price - 1
    ) * 100


def main():

    rows = load_swaps()

    print("=" * 80)
    print("MEMECOIN LAB — FLOW ENGINE V0.4")
    print("=" * 80)

    print()
    print(
        f"Swaps disponibles : {len(rows):,}"
    )

    events = build_events(rows)

    print(
        f"Tokens détectés    : {len(events):,}"
    )

    print()

    # Show tokens with most events first

    ranking = sorted(
        events.items(),
        key=lambda x: len(x[1]),
        reverse=True
    )

    for token, token_events in ranking[:20]:

        if len(token_events) < 2:
            continue

        now = token_events[-1]["time"]

        last_price = (
            token_events[-1]["price"]
        )

        print()
        print("=" * 80)

        print(
            f"TOKEN: {token}"
        )

        print(
            f"SWAPS: {len(token_events)}"
        )

        print(
            f"PRICE: {last_price:.14f} SOL/token"
        )

        print()

        for window in WINDOWS:

            stats = compute_window(
                token_events,
                now,
                window
            )

            change = price_change(
                token_events,
                now,
                window
            )

            print(
                f"--- {window}s ---"
            )

            print(
                f"BUYERS      : {stats['buyers']:>5}"
                f" | SELLERS: {stats['sellers']:>5}"
            )

            print(
                f"BUY VOL     : "
                f"{stats['buy_volume']:>10.4f} SOL"
            )

            print(
                f"SELL VOL    : "
                f"{stats['sell_volume']:>10.4f} SOL"
            )

            print(
                f"NET FLOW    : "
                f"{stats['net_flow']:>+10.4f} SOL"
            )

            print(
                f"IMBALANCE   : "
                f"{stats['imbalance']:>+10.3f}"
            )

            print(
                f"PRICE MOVE  : "
                f"{change:>+10.2f}%"
            )

            print(
                "BUY SIZE     : "
                f">0.05={stats['buys_005']} | "
                f">0.25={stats['buys_025']} | "
                f">1={stats['buys_1']}"
            )

            print()


if __name__ == "__main__":
    main()
