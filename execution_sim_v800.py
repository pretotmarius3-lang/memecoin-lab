import sqlite3
import statistics
import math

DB = "validation_v090.db"

# Frozen V2 filter
VOLUME_CUT = 8837.925

# Execution assumptions
# round-trip fee as % of position value
FEE_RT_PCT = 1.0

# additional slippage per side, %
SLIPPAGE_PER_SIDE_PCT = 0.75

# optional latency penalty approximation, %
# used as an extra execution haircut
LATENCY_PENALTY_PCT = 0.50

# only historical / pre-T23 boundary
MAX_ID = 545


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.mean(vals) if vals else None


def med(vals):
    vals = [x for x in vals if valid(x)]
    return statistics.median(vals) if vals else None


def pct(x):
    if x is None:
        return "NA"
    return f"{x:+.2f}%"


def apply_execution_cost(raw_return):

    if not valid(raw_return):
        return None

    total_cost = (
        FEE_RT_PCT
        + 2 * SLIPPAGE_PER_SIDE_PCT
        + LATENCY_PENALTY_PCT
    )

    return raw_return - total_cost


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


rows = db.execute("""
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
    e.token_mint,
    e.timestamp,

    e.dex_return_30s,
    e.dex_return_60s,
    e.dex_return_300s,

    d.volume_m5

FROM events e

JOIN first_dex d
ON d.event_id=e.id

WHERE
    e.id <= ?
    AND e.fa95=1
    AND e.new_wallets30 >= 2
    AND d.volume_m5 >= ?

ORDER BY e.id ASC
""", (
    MAX_ID,
    VOLUME_CUT
)).fetchall()


# ------------------------------------------------------------
# lifecycle position
# ------------------------------------------------------------

token_seen = {}
events = []

for r in rows:

    token = r["token_mint"]

    token_seen[token] = (
        token_seen.get(token, 0) + 1
    )

    events.append({
        "id": r["id"],
        "token": token,
        "position": token_seen[token],

        "r30_raw": r["dex_return_30s"],
        "r60_raw": r["dex_return_60s"],
        "r300_raw": r["dex_return_300s"],
    })


def describe(title, subset, raw_field):

    raw = [
        r[raw_field]
        for r in subset
        if valid(r[raw_field])
    ]

    net = [
        apply_execution_cost(r[raw_field])
        for r in subset
        if valid(r[raw_field])
    ]

    print()
    print(title)
    print("-" * 115)

    if not raw:
        print("N=0")
        return

    win_raw = 100 * sum(x > 0 for x in raw) / len(raw)
    win_net = 100 * sum(x > 0 for x in net) / len(net)

    print(
        f"N={len(raw)} | "
        f"TOKENS={len(set(r['token'] for r in subset))}"
    )

    print(
        f"RAW | "
        f"AVG={avg(raw):+7.2f}% | "
        f"MED={med(raw):+7.2f}% | "
        f"WIN={win_raw:5.1f}% | "
        f"WORST={min(raw):+7.2f}% | "
        f"BEST={max(raw):+7.2f}%"
    )

    print(
        f"NET | "
        f"AVG={avg(net):+7.2f}% | "
        f"MED={med(net):+7.2f}% | "
        f"WIN={win_net:5.1f}% | "
        f"WORST={min(net):+7.2f}% | "
        f"BEST={max(net):+7.2f}%"
    )


print("=" * 125)
print("MEMECOIN LAB — T24 EXECUTION & EXIT SIMULATOR")
print("=" * 125)

print()
print(
    f"HISTORICAL ONLY | ID <= {MAX_ID}"
)

print(
    f"V2 FILTER = FA95 + NEW30>=2 + "
    f"VOLUME_M5>={VOLUME_CUT}"
)

print()
print("EXECUTION ASSUMPTIONS")
print("-" * 80)

print(
    f"ROUND-TRIP FEE        : {FEE_RT_PCT:.2f}%"
)

print(
    f"SLIPPAGE PER SIDE     : {SLIPPAGE_PER_SIDE_PCT:.2f}%"
)

print(
    f"LATENCY PENALTY       : {LATENCY_PENALTY_PCT:.2f}%"
)

total_cost = (
    FEE_RT_PCT
    + 2 * SLIPPAGE_PER_SIDE_PCT
    + LATENCY_PENALTY_PCT
)

print(
    f"TOTAL EXECUTION COST  : {total_cost:.2f}%"
)

print()
print(
    f"V2 EVENTS={len(events)} | "
    f"TOKENS={len(set(r['token'] for r in events))}"
)


# ------------------------------------------------------------
# all events
# ------------------------------------------------------------

print()
print("=" * 125)
print("A) ALL V2 EVENTS")
print("=" * 125)

describe(
    "30s EXIT",
    events,
    "r30_raw"
)

describe(
    "60s EXIT",
    events,
    "r60_raw"
)

describe(
    "300s EXIT",
    events,
    "r300_raw"
)


# ------------------------------------------------------------
# lifecycle
# ------------------------------------------------------------

first = [
    r for r in events
    if r["position"] == 1
]

second = [
    r for r in events
    if r["position"] == 2
]

third_plus = [
    r for r in events
    if r["position"] >= 3
]

print()
print("=" * 125)
print("B) FIRST SIGNAL / TOKEN")
print("=" * 125)

describe(
    "FIRST — 30s",
    first,
    "r30_raw"
)

describe(
    "FIRST — 60s",
    first,
    "r60_raw"
)

describe(
    "FIRST — 300s",
    first,
    "r300_raw"
)


print()
print("=" * 125)
print("C) SECOND SIGNAL / TOKEN")
print("=" * 125)

describe(
    "SECOND — 30s",
    second,
    "r30_raw"
)

describe(
    "SECOND — 60s",
    second,
    "r60_raw"
)


print()
print("=" * 125)
print("D) THIRD+ SIGNAL / TOKEN")
print("=" * 125)

describe(
    "THIRD+ — 30s",
    third_plus,
    "r30_raw"
)

describe(
    "THIRD+ — 60s",
    third_plus,
    "r60_raw"
)


# ------------------------------------------------------------
# cost sensitivity
# ------------------------------------------------------------

print()
print("=" * 125)
print("E) COST SENSITIVITY — FIRST SIGNAL / TOKEN @ 60s")
print("=" * 125)

raw60 = [
    r["r60_raw"]
    for r in first
    if valid(r["r60_raw"])
]

print(
    f"{'TOTAL COST':>12} "
    f"{'AVG NET':>10} "
    f"{'MED NET':>10} "
    f"{'WIN NET':>10}"
)

print("-" * 50)

for cost in [
    0.5,
    1.0,
    1.5,
    2.0,
    2.5,
    3.0,
    4.0,
    5.0,
]:

    net = [
        x - cost
        for x in raw60
    ]

    print(
        f"{cost:11.2f}% "
        f"{avg(net):+9.2f}% "
        f"{med(net):+9.2f}% "
        f"{100*sum(x>0 for x in net)/len(net):9.1f}%"
    )


# ------------------------------------------------------------
# token-balanced first signal
# ------------------------------------------------------------

print()
print("=" * 125)
print("F) FIRST-SIGNAL TOKEN DETAIL @ 60s")
print("=" * 125)

print(
    f"{'TOKEN':20} "
    f"{'RAW60':>10} "
    f"{'NET60':>10}"
)

print("-" * 48)

for r in sorted(
    first,
    key=lambda x:
        x["r60_raw"]
        if valid(x["r60_raw"])
        else -999,
    reverse=True
):

    if not valid(r["r60_raw"]):
        continue

    net = apply_execution_cost(
        r["r60_raw"]
    )

    print(
        f"{r['token'][:20]:20} "
        f"{r['r60_raw']:+9.2f}% "
        f"{net:+9.2f}%"
    )


print()
print("=" * 125)
print("READ THIS CAREFULLY")
print("=" * 125)

print("""
This is NOT a real tick-level execution backtest.

It answers only:
"How much room does the historical V2 edge have
before reasonable execution costs destroy it?"

Important:

• Do NOT tune V2 from T24.
• T23 remains the true prospective signal test.
• 60s first-signal/token is the primary scenario.
• If the edge dies at 1-2% total cost, it is probably too fragile.
• If average remains positive around 2-3% cost,
  deeper execution simulation becomes worthwhile.
• 300s is diagnostic only, not the preferred horizon.
""")

db.close()
