import sqlite3
import time
import math
from collections import Counter

DB = "validation_v090.db"

REFRESH_SEC = 10

RUNNER = 10.0
DUMP = -10.0

BASE_EPS = 0.05
TABLE = "t51_capital_efficiency_forward"
META = "t51_meta"
BOUNDARY_KEY = "T51_BOUNDARY_ID"

PRE_EVENT_PROGRAM_WINDOW = 30.0


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def sdiv(a, b, eps=BASE_EPS):
    if not valid(a) or not valid(b):
        return None

    if abs(b) < eps:
        return None

    return a / b


def label_r60(x):
    if not valid(x):
        return None

    if x >= RUNNER:
        return "RUN"

    if x <= DUMP:
        return "DUMP"

    return "NEUTRAL"


def fmt(x, n=3):
    if x is None:
        return "NA"

    return f"{x:.{n}f}"


db = sqlite3.connect(DB, timeout=30)
db.row_factory = sqlite3.Row
db.execute("PRAGMA busy_timeout=5000")


# ============================================================
# TABLES
# ============================================================

db.execute(f"""
CREATE TABLE IF NOT EXISTS {META} (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""")


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    event_id INTEGER PRIMARY KEY,
    token_mint TEXT NOT NULL,
    event_timestamp REAL NOT NULL,
    captured_at REAL NOT NULL,

    boundary_id INTEGER NOT NULL,

    recent_price_per_net_sol REAL,
    recent_net_efficiency REAL,
    early_flow_price_div REAL,

    recent_price_return REAL,
    recent_net_sol REAL,
    recent_buy_sol REAL,
    recent_sell_sol REAL,
    early_price_return REAL,
    early_net_sol REAL,

    regime INTEGER,
    program TEXT,

    dex_return_60s REAL,
    status TEXT,
    binary_label INTEGER,
    labeled_60 INTEGER DEFAULT 0
)
""")


db.commit()


# ============================================================
# FROZEN PROSPECTIVE BOUNDARY
# ============================================================

row = db.execute(
    f"""
    SELECT value
    FROM {META}
    WHERE key=?
    """,
    (BOUNDARY_KEY,)
).fetchone()


if row is None:

    boundary_id = int(
        db.execute("""
        SELECT COALESCE(MAX(id),0)
        FROM events
        """).fetchone()[0]
    )

    db.execute(
        f"""
        INSERT INTO {META}(key,value)
        VALUES (?,?)
        """,
        (
            BOUNDARY_KEY,
            str(boundary_id)
        )
    )

    db.commit()

    print(
        f"✅ T51 prospective boundary frozen at ID={boundary_id}"
    )

else:

    boundary_id = int(
        row["value"]
    )


# ============================================================
# REGIME CONTEXT
# ============================================================

def get_regime(event_id):

    # Try the frozen regime table first.
    cols = [
        r["name"]
        for r in db.execute(
            "PRAGMA table_info(frozen_regime_v620)"
        ).fetchall()
    ]

    if not cols:
        return None

    event_col = next(
        (
            c for c in [
                "event_id",
                "id"
            ]
            if c in cols
        ),
        None
    )

    regime_col = next(
        (
            c for c in [
                "regime",
                "regime_id",
                "cluster",
                "assigned_regime"
            ]
            if c in cols
        ),
        None
    )

    if not event_col or not regime_col:
        return None

    try:
        r = db.execute(
            f"""
            SELECT {regime_col}
            FROM frozen_regime_v620
            WHERE {event_col}=?
            LIMIT 1
            """,
            (event_id,)
        ).fetchone()

        return (
            r[0]
            if r
            else None
        )

    except Exception:
        return None


# ============================================================
# PROGRAM CONTEXT
# ============================================================

def dominant_program(token, event_ts):

    rows = db.execute("""
    SELECT
        program,
        COUNT(*) AS n
    FROM swaps
    WHERE
        token_mint=?
        AND timestamp >= ?
        AND timestamp < ?
        AND program IS NOT NULL
    GROUP BY program
    ORDER BY n DESC
    """, (
        token,
        event_ts - PRE_EVENT_PROGRAM_WINDOW,
        event_ts
    )).fetchall()

    if not rows:
        return "UNKNOWN"

    return rows[0]["program"]


# ============================================================
# FEATURE BUILD
# ============================================================

def build_features(event_id):

    r = db.execute("""
    SELECT
        e.id,
        e.timestamp,
        e.token_mint,
        e.dex_return_60s,

        s.recent_price_return,
        s.recent_net_sol,
        s.recent_buy_sol,
        s.recent_sell_sol,

        s.early_price_return,
        s.early_net_sol

    FROM events e
    JOIN event_sequence_features_v340 s
        ON s.event_id = e.id

    WHERE e.id=?
    """, (
        event_id,
    )).fetchone()

    if r is None:
        return None


    recent_price = r["recent_price_return"]
    recent_net = r["recent_net_sol"]

    rpns = sdiv(
        recent_price,
        recent_net,
        BASE_EPS
    )


    rb = r["recent_buy_sol"]
    rs = r["recent_sell_sol"]

    gross = (
        abs(rb) + abs(rs)
        if valid(rb) and valid(rs)
        else None
    )

    rne = sdiv(
        recent_net,
        gross,
        BASE_EPS
    )


    efpd = (
        r["early_price_return"]
        - r["early_net_sol"]
        if (
            valid(r["early_price_return"])
            and valid(r["early_net_sol"])
        )
        else None
    )


    return {
        "event_id":
            r["id"],

        "event_timestamp":
            r["timestamp"],

        "token_mint":
            r["token_mint"],

        "recent_price_per_net_sol":
            rpns,

        "recent_net_efficiency":
            rne,

        "early_flow_price_div":
            efpd,

        "recent_price_return":
            recent_price,

        "recent_net_sol":
            recent_net,

        "recent_buy_sol":
            rb,

        "recent_sell_sol":
            rs,

        "early_price_return":
            r["early_price_return"],

        "early_net_sol":
            r["early_net_sol"],

        "regime":
            get_regime(
                r["id"]
            ),

        "program":
            dominant_program(
                r["token_mint"],
                r["timestamp"]
            ),

        "dex_return_60s":
            r["dex_return_60s"],
    }


# ============================================================
# CAPTURE NEW EVENTS
# ============================================================

def capture_new_events():

    rows = db.execute("""
    SELECT id
    FROM events
    WHERE id > ?
    ORDER BY id
    """, (
        boundary_id,
    )).fetchall()


    inserted = 0


    for row in rows:

        event_id = row["id"]

        exists = db.execute(
            f"""
            SELECT 1
            FROM {TABLE}
            WHERE event_id=?
            """,
            (
                event_id,
            )
        ).fetchone()

        if exists:
            continue


        f = build_features(
            event_id
        )

        if f is None:
            continue


        # frozen primary must exist
        if not valid(
            f[
                "recent_price_per_net_sol"
            ]
        ):
            continue


        r60 = f[
            "dex_return_60s"
        ]


        status = (
            label_r60(r60)
            if valid(r60)
            else "WAIT"
        )


        binary = None

        if status == "RUN":
            binary = 1

        elif status == "DUMP":
            binary = 0


        labeled = (
            1
            if valid(r60)
            else 0
        )


        before = db.total_changes


        db.execute(
            f"""
            INSERT OR IGNORE INTO {TABLE} (
                event_id,
                token_mint,
                event_timestamp,
                captured_at,

                boundary_id,

                recent_price_per_net_sol,
                recent_net_efficiency,
                early_flow_price_div,

                recent_price_return,
                recent_net_sol,
                recent_buy_sol,
                recent_sell_sol,
                early_price_return,
                early_net_sol,

                regime,
                program,

                dex_return_60s,
                status,
                binary_label,
                labeled_60
            )
            VALUES (
                ?,?,?,?,
                ?,
                ?,?,?,
                ?,?,?,?,?,?,
                ?,?,
                ?,?,?,?
            )
            """,
            (
                f["event_id"],
                f["token_mint"],
                f["event_timestamp"],
                time.time(),

                boundary_id,

                f["recent_price_per_net_sol"],
                f["recent_net_efficiency"],
                f["early_flow_price_div"],

                f["recent_price_return"],
                f["recent_net_sol"],
                f["recent_buy_sol"],
                f["recent_sell_sol"],
                f["early_price_return"],
                f["early_net_sol"],

                f["regime"],
                f["program"],

                r60,
                status,
                binary,
                labeled
            )
        )


        if db.total_changes > before:
            inserted += 1


    db.commit()

    return inserted


# ============================================================
# UPDATE LABELS
# ============================================================

def update_labels():

    waiting = db.execute(
        f"""
        SELECT event_id
        FROM {TABLE}
        WHERE labeled_60=0
        """
    ).fetchall()


    for row in waiting:

        r = db.execute("""
        SELECT dex_return_60s
        FROM events
        WHERE id=?
        """, (
            row["event_id"],
        )).fetchone()


        if not r:
            continue


        r60 = r["dex_return_60s"]


        if not valid(r60):
            continue


        status = label_r60(
            r60
        )


        binary = None

        if status == "RUN":
            binary = 1

        elif status == "DUMP":
            binary = 0


        db.execute(
            f"""
            UPDATE {TABLE}
            SET
                dex_return_60s=?,
                status=?,
                binary_label=?,
                labeled_60=1
            WHERE event_id=?
            """,
            (
                r60,
                status,
                binary,
                row["event_id"]
            )
        )


    db.commit()


# ============================================================
# DISPLAY
# ============================================================

def show():

    rows = db.execute(
        f"""
        SELECT *
        FROM {TABLE}
        ORDER BY event_id
        """
    ).fetchall()


    labeled = [
        r for r in rows
        if r["labeled_60"] == 1
    ]


    binary = [
        r for r in labeled
        if r["binary_label"]
        is not None
    ]


    runs = [
        r for r in binary
        if r["binary_label"] == 1
    ]


    dumps = [
        r for r in binary
        if r["binary_label"] == 0
    ]


    neutral = [
        r for r in labeled
        if r["status"] == "NEUTRAL"
    ]


    waiting = [
        r for r in rows
        if r["labeled_60"] == 0
    ]


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 170)
    print(
        "MEMECOIN LAB — T51 FROZEN CAPITAL-EFFICIENCY FORWARD RECORDER"
    )
    print("=" * 170)


    print(
        f"BOUNDARY ID              : {boundary_id}"
    )

    print(
        "PRIMARY                  : recent_price_per_net_sol"
    )

    print(
        "PRIMARY FROZEN DIRECTION : LOWER => RUN-like"
    )

    print(
        "SECONDARY                : recent_net_efficiency / early_flow_price_div"
    )

    print(
        "OUTCOME                  : dex_return_60s"
    )

    print()


    print(
        f"EVENTS CAPTURED : {len(rows)}"
    )

    print(
        f"LABELED +60s    : {len(labeled)}"
    )

    print(
        f"BINARY          : {len(binary)}"
    )

    print(
        f"RUN             : {len(runs)}"
    )

    print(
        f"DUMP            : {len(dumps)}"
    )

    print(
        f"NEUTRAL         : {len(neutral)}"
    )

    print(
        f"WAIT            : {len(waiting)}"
    )


    # ========================================================
    # PRIMARY SEPARATION
    # ========================================================

    print()
    print("=" * 170)
    print(
        "PRIMARY PROSPECTIVE SEPARATION"
    )
    print("=" * 170)


    if runs and dumps:

        run_vals = [
            r[
                "recent_price_per_net_sol"
            ]
            for r in runs
        ]

        dump_vals = [
            r[
                "recent_price_per_net_sol"
            ]
            for r in dumps
        ]


        run_med = sorted(run_vals)[
            len(run_vals)//2
        ]

        dump_med = sorted(dump_vals)[
            len(dump_vals)//2
        ]


        print(
            f"RUN MED  = {run_med:.3f}"
        )

        print(
            f"DUMP MED = {dump_med:.3f}"
        )

        print(
            f"DIFF     = "
            f"{run_med-dump_med:+.3f}"
        )


        try:

            from sklearn.metrics import roc_auc_score

            y = [
                r[
                    "binary_label"
                ]
                for r in binary
            ]

            score = [
                -r[
                    "recent_price_per_net_sol"
                ]
                for r in binary
            ]

            auc = roc_auc_score(
                y,
                score
            )


            print(
                f"DIR-AUC  = {auc:.3f}"
            )

        except Exception:

            print(
                "DIR-AUC  = NA"
            )

    else:

        print(
            "Waiting for at least one RUN and one DUMP..."
        )


    # ========================================================
    # REGIME CONTEXT
    # ========================================================

    print()
    print("=" * 170)
    print(
        "REGIME CONTEXT — DESCRIPTIVE ONLY"
    )
    print("=" * 170)


    regimes = sorted(
        set(
            r["regime"]
            for r in rows
            if r["regime"]
            is not None
        )
    )


    if not regimes:

        print(
            "No regime assignments captured yet."
        )

    else:

        for regime in regimes:

            rr = [
                r for r in rows
                if r["regime"]
                == regime
            ]


            br = [
                r for r in rr
                if r["binary_label"]
                is not None
            ]


            run_n = sum(
                r["binary_label"] == 1
                for r in br
            )


            dump_n = sum(
                r["binary_label"] == 0
                for r in br
            )


            print(
                f"R{regime} "
                f"| N={len(rr):3d} "
                f"| BIN={len(br):3d} "
                f"| RUN={run_n:3d} "
                f"| DUMP={dump_n:3d}"
            )


    # ========================================================
    # CHECKPOINTS
    # ========================================================

    print()
    print("=" * 170)
    print(
        "FROZEN CHECKPOINTS"
    )
    print("=" * 170)


    total = len(
        rows
    )


    if total < 15:

        print(
            f"⏳ {total}/15 prospective events"
        )

    elif total < 30:

        print(
            f"🟡 CHECKPOINT 15 REACHED | "
            f"{total}/30"
        )

        print(
            "Observation only — do not retune."
        )

    elif total < 50:

        print(
            f"🟢 CHECKPOINT 30 REACHED | "
            f"{total}/50"
        )

        print(
            "First serious prospective audit is allowed."
        )

    else:

        print(
            f"🔵 CHECKPOINT 50 REACHED | N={total}"
        )

        print(
            "Enough for stronger historical-vs-forward comparison."
        )


    # ========================================================
    # LATEST
    # ========================================================

    print()
    print("=" * 170)
    print(
        "LATEST FORWARD EVENTS"
    )
    print("=" * 170)

    print(
        f"{'ID':>6} "
        f"{'P/NET':>10} "
        f"{'NETEFF':>9} "
        f"{'EARLYDIV':>10} "
        f"{'REG':>5} "
        f"{'R60':>9} "
        f"{'STATUS':>8} "
        f"TOKEN"
    )

    print(
        "-" * 135
    )


    for r in rows[-15:][::-1]:

        r60 = (
            f"{r['dex_return_60s']:+.2f}%"
            if valid(
                r[
                    "dex_return_60s"
                ]
            )
            else "NA"
        )


        regime = (
            f"R{r['regime']}"
            if r["regime"]
            is not None
            else "NA"
        )


        print(
            f"{r['event_id']:6d} "
            f"{fmt(r['recent_price_per_net_sol']):>10} "
            f"{fmt(r['recent_net_efficiency']):>9} "
            f"{fmt(r['early_flow_price_div']):>10} "
            f"{regime:>5} "
            f"{r60:>9} "
            f"{r['status']:>8} "
            f"{r['token_mint'][:24]}"
        )


    print()
    print("IMPORTANT:")
    print("• T51 is forward-only after frozen boundary.")
    print("• No trading threshold.")
    print("• No model fitting.")
    print("• No regime filtering.")
    print("• Regime is context only.")
    print("• recent_price_per_net_sol definition frozen at EPS=0.05 SOL.")
    print("• LOWER recent_price_per_net_sol = RUN-like direction is frozen.")
    print("• T23/T31/T32/T47 remain untouched.")
    print("• Do not reset T51.")
    print(
        f"• Refresh every {REFRESH_SEC}s."
    )


# ============================================================
# MAIN LOOP
# ============================================================

print(
    "Starting T51 forward recorder..."
)

print(
    "CTRL+C to stop safely."
)

time.sleep(1)


try:

    while True:

        capture_new_events()

        update_labels()

        show()

        time.sleep(
            REFRESH_SEC
        )


except KeyboardInterrupt:

    print()
    print(
        "T51 stopped safely."
    )


finally:

    db.close()
