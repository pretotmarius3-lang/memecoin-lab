import sqlite3
import math
import random
import numpy as np

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0
SEVERE = -20.0

RANDOM_SEED = 42

PRICE_FEATURE = "recent_price_return"
FLOW_FEATURE = "recent_swaps_per_sec"

N_BINS = 4


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def connect():
    db = sqlite3.connect(DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    return db


def label(r):

    x = r["dex_return_60s"]

    if not valid(x):
        return None

    if x >= RUNNER:
        return "RUNNER"

    if x <= SEVERE:
        return "SEVERE"

    if x <= DUMP:
        return "DUMP"

    return "NEUTRAL"


db = connect()


rows = db.execute("""
SELECT
    e.id,
    e.token_mint,
    e.dex_return_60s,

    s.recent_price_return,
    s.recent_swaps_per_sec

FROM events e

JOIN event_sequence_features_v340 s
ON s.event_id=e.id

WHERE
    e.dex_return_60s IS NOT NULL
    AND s.recent_price_return IS NOT NULL
    AND s.recent_swaps_per_sec IS NOT NULL

ORDER BY e.id
""").fetchall()


usable = [
    r for r in rows
    if label(r) in [
        "RUNNER",
        "DUMP",
        "SEVERE"
    ]
]


print("=" * 150)
print("MEMECOIN LAB — T36 DETERIORATION SURFACE / MONOTONIC RISK AUDIT")
print("=" * 150)

print(
    f"LABELED EVENTS : {len(usable)}"
)

print(
    f"UNIQUE TOKENS  : "
    f"{len(set(r['token_mint'] for r in usable))}"
)


# ============================================================
# TOKEN HOLDOUT
# ============================================================

tokens = sorted(
    set(
        r["token_mint"]
        for r in usable
    )
)

random.seed(RANDOM_SEED)
random.shuffle(tokens)

n = len(tokens)

n_train = int(n * 0.60)
n_val = int(n * 0.20)

train_tokens = set(
    tokens[:n_train]
)

val_tokens = set(
    tokens[n_train:n_train+n_val]
)

test_tokens = set(
    tokens[n_train+n_val:]
)


def subset_for(token_set):
    return [
        r for r in usable
        if r["token_mint"] in token_set
    ]


train = subset_for(
    train_tokens
)

val = subset_for(
    val_tokens
)

test = subset_for(
    test_tokens
)


def count_classes(rows):

    return {
        "runner":
            sum(
                label(r) == "RUNNER"
                for r in rows
            ),

        "dump":
            sum(
                label(r) in [
                    "DUMP",
                    "SEVERE"
                ]
                for r in rows
            ),

        "severe":
            sum(
                label(r) == "SEVERE"
                for r in rows
            ),
    }


for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    c = count_classes(rr)

    print(
        f"{name:8} | "
        f"N={len(rr):3d} "
        f"| TOK={len(set(r['token_mint'] for r in rr)):3d} "
        f"| RUN={c['runner']:3d} "
        f"| DUMP={c['dump']:3d} "
        f"| SEVERE={c['severe']:3d}"
    )


# ============================================================
# TRAIN-ONLY BIN CUTS
# ============================================================

price_vals = np.asarray(
    [
        r[PRICE_FEATURE]
        for r in train
        if valid(r[PRICE_FEATURE])
    ],
    dtype=float
)

flow_vals = np.asarray(
    [
        r[FLOW_FEATURE]
        for r in train
        if valid(r[FLOW_FEATURE])
    ],
    dtype=float
)


def quantile_edges(vals, n_bins):

    qs = [
        100 * i / n_bins
        for i in range(
            1,
            n_bins
        )
    ]

    edges = [
        float(
            np.percentile(
                vals,
                q
            )
        )
        for q in qs
    ]

    return sorted(
        set(edges)
    )


price_edges = quantile_edges(
    price_vals,
    N_BINS
)

flow_edges = quantile_edges(
    flow_vals,
    N_BINS
)


print()
print("=" * 150)
print("A) TRAIN-ONLY QUANTILE CUTS")
print("=" * 150)

print(
    f"PRICE EDGES : {price_edges}"
)

print(
    f"FLOW EDGES  : {flow_edges}"
)


# ============================================================
# BINNING
# ============================================================

def assign_bin(value, edges):

    if not valid(value):
        return None

    idx = 0

    for edge in edges:
        if value > edge:
            idx += 1
        else:
            break

    return idx


def cell_stats(rows):

    n = len(rows)

    if n == 0:
        return None

    runs = sum(
        label(r) == "RUNNER"
        for r in rows
    )

    dumps = sum(
        label(r) in [
            "DUMP",
            "SEVERE"
        ]
        for r in rows
    )

    severe = sum(
        label(r) == "SEVERE"
        for r in rows
    )

    return {
        "n":
            n,

        "run_rate":
            100 * runs / n,

        "dump_rate":
            100 * dumps / n,

        "severe_rate":
            100 * severe / n,

        "edge":
            100 * (
                runs - dumps
            ) / n,
    }


def build_surface(rows):

    cells = {}

    for pb in range(N_BINS):

        for fb in range(N_BINS):

            subset = []

            for r in rows:

                rp = assign_bin(
                    r[PRICE_FEATURE],
                    price_edges
                )

                rf = assign_bin(
                    r[FLOW_FEATURE],
                    flow_edges
                )

                if (
                    rp == pb
                    and rf == fb
                ):
                    subset.append(r)

            cells[
                (pb, fb)
            ] = cell_stats(
                subset
            )

    return cells


train_surface = build_surface(
    train
)

val_surface = build_surface(
    val
)

test_surface = build_surface(
    test
)


# ============================================================
# PRINT SURFACES
#
# Price bins: low → high
# Flow bins: low → high
# Low price + low flow = candidate deterioration zone
# ============================================================

def print_surface(
    title,
    surface,
    metric
):

    print()
    print("=" * 150)
    print(title)
    print("=" * 150)

    print(
        f"{'PRICE\\FLOW':>12}",
        end=""
    )

    for fb in range(N_BINS):

        print(
            f" F{fb:>2}",
            end="       "
        )

    print()

    print("-" * 80)

    for pb in range(N_BINS):

        print(
            f"P{pb:>2}         ",
            end=""
        )

        for fb in range(N_BINS):

            s = surface[
                (pb, fb)
            ]

            if not s:

                text = "   NA   "

            else:

                text = (
                    f"{s[metric]:6.1f}%"
                )

            print(
                f"{text:>9}",
                end=" "
            )

        print()


for split_name, surface in [
    ("TRAIN", train_surface),
    ("VALIDATION", val_surface),
    ("TEST", test_surface),
]:

    print_surface(
        f"B) {split_name} — DUMP RATE",
        surface,
        "dump_rate"
    )

    print_surface(
        f"C) {split_name} — SEVERE DUMP RATE",
        surface,
        "severe_rate"
    )

    print_surface(
        f"D) {split_name} — RUNNER RATE",
        surface,
        "run_rate"
    )


# ============================================================
# DETERIORATION SCORE
#
# Lower price bin = worse
# Lower flow bin = worse
#
# score 0 = healthiest corner
# score 6 = worst corner
# ============================================================

def deterioration_score(r):

    pb = assign_bin(
        r[PRICE_FEATURE],
        price_edges
    )

    fb = assign_bin(
        r[FLOW_FEATURE],
        flow_edges
    )

    if (
        pb is None
        or fb is None
    ):
        return None

    price_risk = (
        N_BINS - 1 - pb
    )

    flow_risk = (
        N_BINS - 1 - fb
    )

    return (
        price_risk
        + flow_risk
    )


def score_stats(rows):

    out = []

    for score in range(
        0,
        2*(N_BINS-1) + 1
    ):

        subset = [
            r for r in rows
            if deterioration_score(r)
            == score
        ]

        st = cell_stats(
            subset
        )

        out.append(
            (
                score,
                st
            )
        )

    return out


print()
print("=" * 150)
print("E) MONOTONIC DETERIORATION SCORE")
print("=" * 150)

for split_name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    print()
    print(split_name)
    print("-" * 90)

    print(
        f"{'SCORE':>6} "
        f"{'N':>5} "
        f"{'RUN':>9} "
        f"{'DUMP':>9} "
        f"{'SEVERE':>9} "
        f"{'EDGE':>9}"
    )

    for score, st in score_stats(rr):

        if not st:
            continue

        print(
            f"{score:6d} "
            f"{st['n']:5d} "
            f"{st['run_rate']:8.1f}% "
            f"{st['dump_rate']:8.1f}% "
            f"{st['severe_rate']:8.1f}% "
            f"{st['edge']:+8.1f}%"
        )


# ============================================================
# MONOTONICITY AUDIT
# ============================================================

def monotonic_audit(rows):

    score_rows = []

    for score, st in score_stats(rows):

        if (
            st
            and st["n"] >= 3
        ):

            score_rows.append(
                (
                    score,
                    st["dump_rate"],
                    st["severe_rate"],
                    st["run_rate"]
                )
            )

    dump_up = 0
    dump_pairs = 0

    severe_up = 0
    severe_pairs = 0

    runner_down = 0
    runner_pairs = 0

    for i in range(
        len(score_rows)-1
    ):

        a = score_rows[i]
        b = score_rows[i+1]

        dump_pairs += 1

        if b[1] >= a[1]:
            dump_up += 1

        severe_pairs += 1

        if b[2] >= a[2]:
            severe_up += 1

        runner_pairs += 1

        if b[3] <= a[3]:
            runner_down += 1

    return {
        "dump_monotonic":
            (
                100*dump_up/dump_pairs
                if dump_pairs
                else 0
            ),

        "severe_monotonic":
            (
                100*severe_up/severe_pairs
                if severe_pairs
                else 0
            ),

        "runner_monotonic":
            (
                100*runner_down/runner_pairs
                if runner_pairs
                else 0
            ),
    }


print()
print("=" * 150)
print("F) MONOTONICITY AUDIT")
print("=" * 150)

for name, rr in [
    ("TRAIN", train),
    ("VALID", val),
    ("TEST", test),
]:

    m = monotonic_audit(
        rr
    )

    print(
        f"{name:8} | "
        f"DUMP RISK UP={m['dump_monotonic']:5.1f}% | "
        f"SEVERE UP={m['severe_monotonic']:5.1f}% | "
        f"RUNNER DOWN={m['runner_monotonic']:5.1f}%"
    )


# ============================================================
# FIRST EVENT PER TOKEN AUDIT
# ============================================================

def first_per_token(rows):

    seen = set()
    out = []

    for r in sorted(
        rows,
        key=lambda x: x["id"]
    ):

        token = r[
            "token_mint"
        ]

        if token in seen:
            continue

        seen.add(token)
        out.append(r)

    return out


print()
print("=" * 150)
print("G) FIRST-EVENT/TOKEN DETERIORATION SCORE")
print("=" * 150)

for name, rr in [
    (
        "VALID FIRST",
        first_per_token(val)
    ),
    (
        "TEST FIRST",
        first_per_token(test)
    ),
]:

    print()
    print(name)
    print("-" * 90)

    for score, st in score_stats(rr):

        if not st:
            continue

        print(
            f"SCORE={score} "
            f"| N={st['n']:2d} "
            f"| RUN={st['run_rate']:5.1f}% "
            f"| DUMP={st['dump_rate']:5.1f}% "
            f"| SEVERE={st['severe_rate']:5.1f}% "
            f"| EDGE={st['edge']:+6.1f}%"
        )


# ============================================================
# DECISION
# ============================================================

print()
print("=" * 150)
print("H) DECISION SUPPORT")
print("=" * 150)

train_m = monotonic_audit(
    train
)

val_m = monotonic_audit(
    val
)

test_m = monotonic_audit(
    test
)

good = (
    train_m[
        "dump_monotonic"
    ] >= 60
    and val_m[
        "dump_monotonic"
    ] >= 60
    and test_m[
        "dump_monotonic"
    ] >= 60
)

if good:

    print(
        "MONOTONIC DETERIORATION STRUCTURE DETECTED."
    )

    print(
        "Do NOT convert it into a veto yet."
    )

    print(
        "Next step = freeze a continuous deterioration score "
        "and test prospectively."
    )

else:

    print(
        "NO STABLE MONOTONIC DETERIORATION SURFACE."
    )

    print(
        "Price weakness + flow weakness do not generalize "
        "cleanly enough as a universal risk axis."
    )


print()
print("IMPORTANT:")
print("• T23/T31/T32 remain untouched.")
print("• Quantile thresholds come from TRAIN only.")
print("• TEST is final audit only.")
print("• No V2 rule is modified.")
print("• Do not retune T36 using TEST.")

db.close()
