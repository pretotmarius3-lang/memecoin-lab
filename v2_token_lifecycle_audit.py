import sqlite3
import statistics
import math
from collections import defaultdict

DB = "validation_v090.db"

VOLUME_CUT = 8837.925
RUNNER = 10.0
DUMP = -10.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def avg(v):
    v = [x for x in v if valid(x)]
    return statistics.mean(v) if v else None


def med(v):
    v = [x for x in v if valid(x)]
    return statistics.median(v) if v else None


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")

rows = db.execute("""
WITH first_dex AS (
    SELECT d.*
    FROM dex_prices d
    JOIN (
        SELECT event_id, MIN(timestamp) AS first_time
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
    e.fa,
    e.new_wallets30,
    e.dex_return_30s,
    e.dex_return_60s,
    e.dex_return_300s,
    d.volume_m5

FROM events e

JOIN first_dex d
ON d.event_id=e.id

WHERE
    e.fa95=1
    AND e.new_wallets30 >= 2
    AND d.volume_m5 >= ?

ORDER BY e.id
""", (VOLUME_CUT,)).fetchall()


# ------------------------------------------------------------
# Add lifecycle position
# ------------------------------------------------------------

token_count = defaultdict(int)
enriched = []

for r in rows:
    token = r["token_mint"]
    token_count[token] += 1

    enriched.append({
        "id": r["id"],
        "timestamp": r["timestamp"],
        "token": token,
        "position": token_count[token],
        "r30": r["dex_return_30s"],
        "r60": r["dex_return_60s"],
        "r300": r["dex_return_300s"],
        "fa": r["fa"],
        "new30": r["new_wallets30"],
        "volume": r["volume_m5"],
    })


def describe(name, subset):

    vals30 = [r["r30"] for r in subset if valid(r["r30"])]
    vals60 = [r["r60"] for r in subset if valid(r["r60"])]
    vals300 = [r["r300"] for r in subset if valid(r["r300"])]

    tokens = len(set(r["token"] for r in subset))

    print()
    print(name)
    print("-" * 105)

    print(
        f"EVENTS={len(subset)} | "
        f"TOKENS={tokens}"
    )

    for label, vals in [
        ("30s", vals30),
        ("60s", vals60),
        ("300s", vals300),
    ]:

        if not vals:
            print(f"{label:>5} | N=0")
            continue

        wins = sum(x > 0 for x in vals)

        runners = sum(x >= RUNNER for x in vals)
        dumps = sum(x <= DUMP for x in vals)

        print(
            f"{label:>5} | "
            f"N={len(vals):>3} | "
            f"AVG={avg(vals):+7.2f}% | "
            f"MED={med(vals):+7.2f}% | "
            f"WIN={100*wins/len(vals):5.1f}% | "
            f"RUN10={100*runners/len(vals):5.1f}% | "
            f"DUMP10={100*dumps/len(vals):5.1f}% | "
            f"EDGE={100*(runners-dumps)/len(vals):+6.1f}% | "
            f"WORST={min(vals):+7.2f}% | "
            f"BEST={max(vals):+7.2f}%"
        )


print("=" * 125)
print("MEMECOIN LAB — V2 FROZEN TOKEN / LIFECYCLE AUDIT")
print("=" * 125)

print(
    f"FILTER: FA95 + NEW30>=2 + VOLUME_M5>={VOLUME_CUT}"
)

print(
    f"TOTAL EVENTS={len(enriched)} | "
    f"UNIQUE TOKENS={len(set(r['token'] for r in enriched))}"
)

describe(
    "A) ALL V2 EVENTS",
    enriched
)

describe(
    "B) FIRST V2 SIGNAL PER TOKEN",
    [r for r in enriched if r["position"] == 1]
)

describe(
    "C) SECOND V2 SIGNAL PER TOKEN",
    [r for r in enriched if r["position"] == 2]
)

describe(
    "D) THIRD+ V2 SIGNAL PER TOKEN",
    [r for r in enriched if r["position"] >= 3]
)


# ------------------------------------------------------------
# TOKEN BALANCED
# ------------------------------------------------------------

groups = defaultdict(list)

for r in enriched:
    if valid(r["r60"]):
        groups[r["token"]].append(r["r60"])

token_medians = []

for token, vals in groups.items():
    token_medians.append({
        "token": token,
        "n": len(vals),
        "median60": med(vals),
        "avg60": avg(vals),
        "best60": max(vals),
        "worst60": min(vals),
    })

print()
print("=" * 125)
print("E) TOKEN-BALANCED — 60s")
print("=" * 125)

tm = [x["median60"] for x in token_medians]

if tm:

    runners = sum(x >= RUNNER for x in tm)
    dumps = sum(x <= DUMP for x in tm)

    print(
        f"TOKENS={len(tm)} | "
        f"AVG TOKEN MED={avg(tm):+.2f}% | "
        f"MED TOKEN MED={med(tm):+.2f}% | "
        f"POSITIVE={100*sum(x>0 for x in tm)/len(tm):.1f}% | "
        f"RUN10={100*runners/len(tm):.1f}% | "
        f"DUMP10={100*dumps/len(tm):.1f}%"
    )


# ------------------------------------------------------------
# TOKEN DETAIL
# ------------------------------------------------------------

print()
print("=" * 125)
print("F) TOKEN DETAIL — SORTED BY MEDIAN 60s")
print("=" * 125)

print(
    f"{'TOKEN':20} "
    f"{'N':>3} "
    f"{'MED60':>9} "
    f"{'AVG60':>9} "
    f"{'WORST':>9} "
    f"{'BEST':>9}"
)

print("-" * 70)

for x in sorted(
    token_medians,
    key=lambda z: z["median60"],
    reverse=True
):

    print(
        f"{x['token'][:20]:20} "
        f"{x['n']:>3} "
        f"{x['median60']:+8.2f}% "
        f"{x['avg60']:+8.2f}% "
        f"{x['worst60']:+8.2f}% "
        f"{x['best60']:+8.2f}%"
    )


# ------------------------------------------------------------
# REPEATED VS SINGLETON TOKENS
# ------------------------------------------------------------

freq = defaultdict(int)

for r in enriched:
    freq[r["token"]] += 1

singletons = [
    r for r in enriched
    if freq[r["token"]] == 1
]

repeated = [
    r for r in enriched
    if freq[r["token"]] >= 2
]

describe(
    "G) TOKENS THAT ONLY TRIGGERED V2 ONCE",
    singletons
)

describe(
    "H) EVENTS FROM REPEATED V2 TOKENS",
    repeated
)


print()
print("=" * 125)
print("INTERPRETATION CHECK")
print("=" * 125)

print("""
What we want to see:

1. FIRST SIGNAL/token remains positive at 60s.
2. TOKEN-BALANCED median remains positive.
3. Edge is not produced only by THIRD+ repeated signals.
4. Several independent tokens contribute to the positive result.
5. 30-60s remains stronger than 300s.

If ALL-events looks good but FIRST/token collapses,
V2 is partly repetition-driven.

If FIRST/token + TOKEN-BALANCED both survive,
V2 becomes our strongest current candidate.
""")

db.close()
