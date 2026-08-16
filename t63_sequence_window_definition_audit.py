from pathlib import Path
import re

FILE = Path("event_sequence_v340.py")

if not FILE.exists():
    raise RuntimeError(
        "event_sequence_v340.py not found in current directory."
    )

text = FILE.read_text(errors="replace")
lines = text.splitlines()


KEYWORDS = [
    "early",
    "mid",
    "recent",

    "price_return",
    "early_price_return",
    "mid_price_return",
    "recent_price_return",

    "event_timestamp",
    "timestamp",

    "window",
    "start",
    "end",

    "updated_at",

    "clean_price",
    "raw_price",

    "SELECT",
    "FROM swaps",
    "WHERE",
]


print("=" * 180)
print("MEMECOIN LAB — T63 SEQUENCE WINDOW DEFINITION AUDIT")
print("=" * 180)

print(f"FILE  : {FILE}")
print(f"LINES : {len(lines)}")

print()
print("NO OUTCOME LABELS / NO MODEL / NO DB WRITE")


# ============================================================
# A) FUNCTION DEFINITIONS
# ============================================================

print()
print("=" * 180)
print("A) FUNCTIONS")
print("=" * 180)

for i, line in enumerate(lines, start=1):

    s = line.strip()

    if s.startswith("def "):

        print(
            f"{i:5d} | {s}"
        )


# ============================================================
# B) DIRECT REFERENCES TO PRICE-RETURN FEATURES
# ============================================================

print()
print("=" * 180)
print("B) PRICE-RETURN FEATURE REFERENCES")
print("=" * 180)

targets = [
    "early_price_return",
    "mid_price_return",
    "recent_price_return",
]

hits = []

for i, line in enumerate(lines, start=1):

    if any(
        t in line
        for t in targets
    ):
        hits.append(i)


if not hits:

    print(
        "No direct references found."
    )

else:

    seen = set()

    for hit in hits:

        start = max(
            1,
            hit-12
        )

        end = min(
            len(lines),
            hit+18
        )

        key = (
            start,
            end
        )

        if key in seen:
            continue

        seen.add(key)

        print()
        print(
            f"--- lines {start}-{end} ---"
        )

        for j in range(
            start,
            end+1
        ):

            marker = (
                ">>>"
                if j == hit
                else "   "
            )

            print(
                f"{marker} {j:5d} | "
                f"{lines[j-1]}"
            )


# ============================================================
# C) EARLY / MID / RECENT WINDOW CONSTRUCTION
# ============================================================

print()
print("=" * 180)
print("C) EARLY / MID / RECENT WINDOW CONSTRUCTION")
print("=" * 180)

window_hits = []

for i, line in enumerate(lines, start=1):

    low = line.lower()

    if (
        (
            "early" in low
            or "mid" in low
            or "recent" in low
        )
        and (
            "timestamp" in low
            or "window" in low
            or "duration" in low
            or "start" in low
            or "end" in low
            or "mask" in low
            or "filter" in low
        )
    ):

        window_hits.append(i)


if not window_hits:

    print(
        "No obvious window-construction lines found."
    )

else:

    for hit in window_hits:

        print(
            f"{hit:5d} | {lines[hit-1]}"
        )


# ============================================================
# D) SWAP QUERY / TEMPORAL FILTERING
# ============================================================

print()
print("=" * 180)
print("D) SWAP QUERY / TEMPORAL FILTERING")
print("=" * 180)

sql_hits = []

for i, line in enumerate(lines, start=1):

    low = line.lower()

    if (
        "from swaps" in low
        or "select " in low
        or (
            "timestamp" in low
            and (
                "<=" in line
                or "<" in line
                or ">=" in line
                or ">" in line
            )
        )
    ):

        sql_hits.append(i)


shown = set()

for hit in sql_hits:

    start = max(
        1,
        hit-8
    )

    end = min(
        len(lines),
        hit+12
    )

    key = (
        start,
        end
    )

    if key in shown:
        continue

    shown.add(key)

    print()
    print(
        f"--- lines {start}-{end} ---"
    )

    for j in range(
        start,
        end+1
    ):

        print(
            f"{j:5d} | "
            f"{lines[j-1]}"
        )


# ============================================================
# E) UPDATED_AT CONSTRUCTION
# ============================================================

print()
print("=" * 180)
print("E) UPDATED_AT REFERENCES")
print("=" * 180)

found_updated = False

for i, line in enumerate(lines, start=1):

    if "updated_at" in line:

        found_updated = True

        start = max(
            1,
            i-8
        )

        end = min(
            len(lines),
            i+10
        )

        print()
        print(
            f"--- lines {start}-{end} ---"
        )

        for j in range(
            start,
            end+1
        ):

            marker = (
                ">>>"
                if j == i
                else "   "
            )

            print(
                f"{marker} {j:5d} | "
                f"{lines[j-1]}"
            )


if not found_updated:

    print(
        "No updated_at reference found."
    )


# ============================================================
# F) SUSPICIOUS FUTURE-LOOKING PATTERNS
# ============================================================

print()
print("=" * 180)
print("F) FUTURE-LOOKING PATTERN SCAN")
print("=" * 180)

patterns = [
    r"timestamp\s*>\s*event",
    r"timestamp\s*>=\s*event",
    r"event_timestamp\s*\+",
    r"timestamp\s*<=\s*event_timestamp\s*\+",
    r"event_timestamp\s*\+\s*\d+",
    r"time\.time\(",
]


found = []


for i, line in enumerate(lines, start=1):

    for p in patterns:

        if re.search(
            p,
            line,
            flags=re.I
        ):

            found.append(
                (
                    i,
                    p,
                    line
                )
            )


if not found:

    print(
        "No obvious future-looking pattern found."
    )

else:

    for i, p, line in found:

        print(
            f"{i:5d} | PATTERN={p:35} | {line}"
        )


# ============================================================
# G) PRICE SOURCE REFERENCES
# ============================================================

print()
print("=" * 180)
print("G) PRICE SOURCE REFERENCES")
print("=" * 180)

for i, line in enumerate(lines, start=1):

    low = line.lower()

    if (
        "clean_price" in low
        or "raw_price" in low
        or "dex_price" in low
        or "price_usd" in low
    ):

        print(
            f"{i:5d} | {line}"
        )


# ============================================================
# H) COMPACT RELEVANT EXTRACT
# ============================================================

print()
print("=" * 180)
print("H) COMPACT RELEVANT EXTRACT")
print("=" * 180)

interesting = []

for i, line in enumerate(lines, start=1):

    low = line.lower()

    if any(
        k in low
        for k in [
            "early_",
            "mid_",
            "recent_",
            "event_timestamp",
            "clean_price",
            "from swaps",
            "updated_at",
        ]
    ):

        interesting.append(
            (
                i,
                line
            )
        )


for i, line in interesting:

    print(
        f"{i:5d} | {line}"
    )


print()
print("=" * 180)
print("I) WHAT TO CHECK MANUALLY")
print("=" * 180)

print(
    "1. Exact temporal ranges assigned to EARLY / MID / RECENT."
)

print(
    "2. Whether every swap used satisfies swap.timestamp <= event_timestamp."
)

print(
    "3. Exact formula used for each *_price_return."
)

print(
    "4. Whether updated_at is only write-time metadata or affects feature inputs."
)

print(
    "5. Whether the sequence engine is recomputed later using swaps that arrived after the event."
)

print()
print(
    "T63 writes nothing and uses no outcome labels."
)
