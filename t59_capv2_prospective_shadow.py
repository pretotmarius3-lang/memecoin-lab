import sqlite3
import json
import math
import time
import hashlib
from pathlib import Path

DB = "validation_v090.db"

FREEZE_FILE = Path(
    "t59_capv2_frozen.json"
)

TABLE = "t59_capv2_prospective"

REFRESH_SEC = 10


# ============================================================
# HELPERS
# ============================================================

def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def sigmoid(z):
    z = max(
        min(z, 35.0),
        -35.0
    )

    return (
        1.0
        / (
            1.0
            + math.exp(-z)
        )
    )


def auc(y, score):

    pos = [
        score[i]
        for i in range(len(y))
        if y[i] == 1
    ]

    neg = [
        score[i]
        for i in range(len(y))
        if y[i] == 0
    ]

    if not pos or not neg:
        return None

    wins = 0.0
    total = 0

    for a in pos:
        for b in neg:

            total += 1

            if a > b:
                wins += 1.0

            elif a == b:
                wins += 0.5

    return wins / total


def logloss(y, p):

    if not y:
        return None

    eps = 1e-12

    out = []

    for yy, pp in zip(y, p):

        pp = min(
            max(pp, eps),
            1.0-eps
        )

        out.append(
            -(
                yy * math.log(pp)
                + (1-yy)
                * math.log(1-pp)
            )
        )

    return (
        sum(out)
        / len(out)
    )


def brier(y, p):

    if not y:
        return None

    return (
        sum(
            (pp-yy)**2
            for yy, pp
            in zip(y, p)
        )
        / len(y)
    )


def fmt(x, n=4):

    if x is None:
        return "NA"

    return f"{x:.{n}f}"


# ============================================================
# LOAD / VERIFY FREEZE
# ============================================================

if not FREEZE_FILE.exists():

    raise RuntimeError(
        "Missing t59_capv2_frozen.json. "
        "Run t59_freeze_capv2.py first."
    )


freeze = json.loads(
    FREEZE_FILE.read_text()
)


stored_hash = freeze.get(
    "freeze_sha256"
)


hash_copy = dict(
    freeze
)

hash_copy.pop(
    "freeze_sha256",
    None
)


canonical = json.dumps(
    hash_copy,
    sort_keys=True,
    separators=(",", ":"),
).encode()


computed_hash = hashlib.sha256(
    canonical
).hexdigest()


if stored_hash != computed_hash:

    raise RuntimeError(
        "T59 FREEZE HASH MISMATCH. "
        "Frozen specification appears modified."
    )


boundary_id = int(
    freeze[
        "boundary_id"
    ]
)


CONTROL = freeze[
    "control_model"
]

PRIMARY = freeze[
    "primary_model"
]


models = freeze[
    "models"
]

means = freeze[
    "standardization"
][
    "means"
]

stds = freeze[
    "standardization"
][
    "stds"
]


runner_threshold = freeze[
    "outcome"
][
    "runner_threshold"
]

dump_threshold = freeze[
    "outcome"
][
    "dump_threshold"
]


# ============================================================
# MODEL SCORING
# ============================================================

def score_model(
    model_name,
    values
):

    model = models[
        model_name
    ]

    z = model[
        "intercept"
    ]

    for feature in model[
        "features"
    ]:

        x = values.get(
            feature
        )

        if not valid(x):
            return None

        mu = means[
            feature
        ]

        sd = stds[
            feature
        ]

        if (
            not valid(sd)
            or abs(sd) < 1e-12
        ):
            sd = 1.0

        zx = (
            x - mu
        ) / sd

        z += (
            model[
                "coefficients"
            ][
                feature
            ]
            * zx
        )

    return sigmoid(z)


# ============================================================
# DB
# ============================================================

db = sqlite3.connect(
    DB,
    timeout=30
)

db.row_factory = sqlite3.Row

db.execute(
    "PRAGMA busy_timeout=5000"
)


db.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    event_id INTEGER PRIMARY KEY,

    token_mint TEXT NOT NULL,
    event_timestamp REAL NOT NULL,
    captured_at REAL NOT NULL,

    boundary_id INTEGER NOT NULL,
    freeze_sha256 TEXT NOT NULL,

    fa REAL,
    new_wallets30 REAL,
    recent_buy_share REAL,
    recent_net_share REAL,
    breadth_score REAL,
    late_chase_score REAL,

    early_price_return REAL,
    early_net_sol REAL,
    early_div REAL,

    net_eff_diag REAL,

    control_score REAL,
    capv2_score REAL,

    dex_return_60s REAL,

    status TEXT,
    binary_label INTEGER,
    labeled_60 INTEGER DEFAULT 0
)
""")


db.commit()


# ============================================================
# OUTCOME
# ============================================================

def classify(r60):

    if not valid(r60):
        return (
            "WAIT",
            None,
            0
        )

    if r60 >= runner_threshold:
        return (
            "RUN",
            1,
            1
        )

    if r60 <= dump_threshold:
        return (
            "DUMP",
            0,
            1
        )

    return (
        "NEUTRAL",
        None,
        1
    )


# ============================================================
# BUILD PROSPECTIVE EVENT
# ============================================================

def build_event(event_id):

    r = db.execute("""
    SELECT
        e.id,
        e.timestamp,
        e.token_mint,

        e.fa,
        e.new_wallets30,
        e.dex_return_60s,

        s.recent_buy_share,
        s.recent_net_share,
        s.breadth_score,
        s.late_chase_score,

        s.early_price_return,
        s.early_net_sol,

        s.recent_net_sol,
        s.recent_buy_sol,
        s.recent_sell_sol

    FROM events e

    JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

    WHERE
        e.id=?
    """, (
        event_id,
    )).fetchone()


    # Sequence engine may not have produced
    # this event yet. Retry next cycle.
    if r is None:
        return None


    early_div = None

    if (
        valid(
            r[
                "early_price_return"
            ]
        )
        and valid(
            r[
                "early_net_sol"
            ]
        )
    ):

        early_div = (
            r[
                "early_price_return"
            ]
            - r[
                "early_net_sol"
            ]
        )


    rb = r[
        "recent_buy_sol"
    ]

    rs = r[
        "recent_sell_sol"
    ]

    rn = r[
        "recent_net_sol"
    ]


    gross = (
        abs(rb)
        + abs(rs)
        if (
            valid(rb)
            and valid(rs)
        )
        else None
    )


    net_eff_diag = None

    if (
        valid(rn)
        and valid(gross)
        and abs(gross) >= 0.05
    ):

        net_eff_diag = (
            rn
            / gross
        )


    values = {
        "fa":
            r[
                "fa"
            ],

        "new_wallets30":
            r[
                "new_wallets30"
            ],

        "recent_buy_share":
            r[
                "recent_buy_share"
            ],

        "recent_net_share":
            r[
                "recent_net_share"
            ],

        "breadth_score":
            r[
                "breadth_score"
            ],

        "late_chase_score":
            r[
                "late_chase_score"
            ],

        "early_div":
            early_div,
    }


    control_score = score_model(
        CONTROL,
        values
    )

    capv2_score = score_model(
        PRIMARY,
        values
    )


    # Exact same complete-case cohort
    # for CONTROL and CAP-v2.
    if (
        not valid(
            control_score
        )
        or not valid(
            capv2_score
        )
    ):
        return None


    status, binary, labeled = classify(
        r[
            "dex_return_60s"
        ]
    )


    return {
        "event_id":
            r[
                "id"
            ],

        "token_mint":
            r[
                "token_mint"
            ],

        "event_timestamp":
            r[
                "timestamp"
            ],

        "fa":
            r[
                "fa"
            ],

        "new_wallets30":
            r[
                "new_wallets30"
            ],

        "recent_buy_share":
            r[
                "recent_buy_share"
            ],

        "recent_net_share":
            r[
                "recent_net_share"
            ],

        "breadth_score":
            r[
                "breadth_score"
            ],

        "late_chase_score":
            r[
                "late_chase_score"
            ],

        "early_price_return":
            r[
                "early_price_return"
            ],

        "early_net_sol":
            r[
                "early_net_sol"
            ],

        "early_div":
            early_div,

        "net_eff_diag":
            net_eff_diag,

        "control_score":
            control_score,

        "capv2_score":
            capv2_score,

        "dex_return_60s":
            r[
                "dex_return_60s"
            ],

        "status":
            status,

        "binary_label":
            binary,

        "labeled_60":
            labeled,
    }


# ============================================================
# CAPTURE STRICTLY AFTER BOUNDARY
# ============================================================

def capture_new():

    ids = db.execute("""
    SELECT id
    FROM events
    WHERE id > ?
    ORDER BY id
    """, (
        boundary_id,
    )).fetchall()


    inserted = 0


    for x in ids:

        event_id = x[
            "id"
        ]


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


        rec = build_event(
            event_id
        )


        if rec is None:
            continue


        before = db.total_changes


        db.execute(
            f"""
            INSERT OR IGNORE INTO {TABLE} (
                event_id,
                token_mint,
                event_timestamp,
                captured_at,

                boundary_id,
                freeze_sha256,

                fa,
                new_wallets30,
                recent_buy_share,
                recent_net_share,
                breadth_score,
                late_chase_score,

                early_price_return,
                early_net_sol,
                early_div,

                net_eff_diag,

                control_score,
                capv2_score,

                dex_return_60s,
                status,
                binary_label,
                labeled_60
            )
            VALUES (
                ?,?,?,?,
                ?,?,
                ?,?,?,?,?,?,
                ?,?,?,
                ?,
                ?,?,
                ?,?,?,?
            )
            """,
            (
                rec[
                    "event_id"
                ],

                rec[
                    "token_mint"
                ],

                rec[
                    "event_timestamp"
                ],

                time.time(),

                boundary_id,

                stored_hash,

                rec[
                    "fa"
                ],

                rec[
                    "new_wallets30"
                ],

                rec[
                    "recent_buy_share"
                ],

                rec[
                    "recent_net_share"
                ],

                rec[
                    "breadth_score"
                ],

                rec[
                    "late_chase_score"
                ],

                rec[
                    "early_price_return"
                ],

                rec[
                    "early_net_sol"
                ],

                rec[
                    "early_div"
                ],

                rec[
                    "net_eff_diag"
                ],

                rec[
                    "control_score"
                ],

                rec[
                    "capv2_score"
                ],

                rec[
                    "dex_return_60s"
                ],

                rec[
                    "status"
                ],

                rec[
                    "binary_label"
                ],

                rec[
                    "labeled_60"
                ],
            )
        )


        if db.total_changes > before:
            inserted += 1


    db.commit()

    return inserted


# ============================================================
# UPDATE +60 LABELS
# ============================================================

def update_labels():

    waiting = db.execute(
        f"""
        SELECT event_id
        FROM {TABLE}
        WHERE labeled_60=0
        """
    ).fetchall()


    for x in waiting:

        r = db.execute("""
        SELECT dex_return_60s
        FROM events
        WHERE id=?
        """, (
            x[
                "event_id"
            ],
        )).fetchone()


        if (
            r is None
            or not valid(
                r[
                    "dex_return_60s"
                ]
            )
        ):
            continue


        r60 = r[
            "dex_return_60s"
        ]


        status, binary, labeled = classify(
            r60
        )


        db.execute(
            f"""
            UPDATE {TABLE}
            SET
                dex_return_60s=?,
                status=?,
                binary_label=?,
                labeled_60=?
            WHERE event_id=?
            """,
            (
                r60,
                status,
                binary,
                labeled,
                x[
                    "event_id"
                ],
            )
        )


    db.commit()


# ============================================================
# FIRST TOKEN
# ============================================================

def first_per_token(rows):

    seen = set()
    out = []

    for r in rows:

        tok = r[
            "token_mint"
        ]

        if tok in seen:
            continue

        seen.add(
            tok
        )

        out.append(
            r
        )

    return out


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


    binary = [
        r for r in rows
        if r[
            "binary_label"
        ] is not None
    ]


    first_binary = [
        r for r in first_per_token(
            rows
        )
        if r[
            "binary_label"
        ] is not None
    ]


    print(
        "\033[2J\033[H",
        end=""
    )


    print("=" * 175)

    print(
        "MEMECOIN LAB — T59 FROZEN CAP-V2 PROSPECTIVE SHADOW"
    )

    print("=" * 175)


    print(
        f"BOUNDARY ID   : {boundary_id}"
    )

    print(
        f"FREEZE HASH   : {stored_hash}"
    )

    print(
        "CONTROL       : CONTEXT"
    )

    print(
        "PRIMARY       : CONTEXT + EARLY_DIV"
    )

    print(
        "EARLY_DIV     : early_price_return - early_net_sol"
    )

    print(
        "NET_EFF       : diagnostic only"
    )


    print()
    print(
        f"EVENTS        : {len(rows)}"
    )

    print(
        f"UNIQUE TOKENS : "
        f"{len(set(r['token_mint'] for r in rows))}"
    )

    print(
        f"BINARY        : {len(binary)}"
    )

    print(
        f"RUN           : "
        f"{sum(r['binary_label']==1 for r in binary)}"
    )

    print(
        f"DUMP          : "
        f"{sum(r['binary_label']==0 for r in binary)}"
    )

    print(
        f"WAIT          : "
        f"{sum(r['labeled_60']==0 for r in rows)}"
    )


    print()
    print("=" * 175)

    print(
        "A) FORWARD CONTROL vs CAP-V2"
    )

    print("=" * 175)


    if len(binary) >= 2:

        y = [
            r[
                "binary_label"
            ]
            for r in binary
        ]


        if len(set(y)) >= 2:

            p0 = [
                r[
                    "control_score"
                ]
                for r in binary
            ]

            p1 = [
                r[
                    "capv2_score"
                ]
                for r in binary
            ]


            a0 = auc(
                y,
                p0
            )

            a1 = auc(
                y,
                p1
            )


            ll0 = logloss(
                y,
                p0
            )

            ll1 = logloss(
                y,
                p1
            )


            br0 = brier(
                y,
                p0
            )

            br1 = brier(
                y,
                p1
            )


            print(
                f"CONTROL | "
                f"AUC={fmt(a0)} "
                f"LL={fmt(ll0)} "
                f"BRIER={fmt(br0)}"
            )

            print(
                f"CAP-v2  | "
                f"AUC={fmt(a1)} "
                f"LL={fmt(ll1)} "
                f"BRIER={fmt(br1)}"
            )

            print()

            print(
                f"ΔAUC     = "
                f"{fmt(a1-a0)}"
            )

            print(
                f"ΔLOGLOSS = "
                f"{fmt(ll0-ll1)}"
            )

            print(
                f"ΔBRIER   = "
                f"{fmt(br0-br1)}"
            )

        else:

            print(
                "Need both RUN and DUMP."
            )

    else:

        print(
            "Waiting for binary forward outcomes."
        )


    print()
    print("=" * 175)

    print(
        "B) FIRST-EVENT/TOKEN"
    )

    print("=" * 175)


    if len(first_binary) >= 2:

        y = [
            r[
                "binary_label"
            ]
            for r in first_binary
        ]


        if len(set(y)) >= 2:

            p0 = [
                r[
                    "control_score"
                ]
                for r in first_binary
            ]

            p1 = [
                r[
                    "capv2_score"
                ]
                for r in first_binary
            ]


            print(
                f"TOKENS={len(first_binary)} "
                f"| CONTROL AUC={fmt(auc(y,p0))} "
                f"| CAP-v2 AUC={fmt(auc(y,p1))}"
            )

        else:

            print(
                "Need first-token RUN and DUMP."
            )

    else:

        print(
            "Waiting for first-token binary outcomes."
        )


    print()
    print("=" * 175)

    print(
        "C) CHECKPOINT"
    )

    print("=" * 175)


    unique_tokens = len(
        set(
            r[
                "token_mint"
            ]
            for r in rows
        )
    )


    if unique_tokens < 15:

        print(
            f"⏳ {unique_tokens}/15 prospective tokens"
        )

    elif unique_tokens < 30:

        print(
            f"🟡 {unique_tokens}/30 — observation only"
        )

    elif unique_tokens < 50:

        print(
            f"🟢 {unique_tokens}/50 — serious audit allowed"
        )

    else:

        print(
            f"🔵 {unique_tokens} tokens — stronger forward evidence"
        )


    print()
    print("=" * 175)

    print(
        "LATEST"
    )

    print("=" * 175)


    for r in rows[
        -12:
    ][
        ::-1
    ]:

        r60 = (
            f"{r['dex_return_60s']:+.2f}%"
            if valid(
                r[
                    "dex_return_60s"
                ]
            )
            else "NA"
        )


        print(
            f"ID={r['event_id']:4d} "
            f"| CTRL={r['control_score']:.4f} "
            f"| CAPV2={r['capv2_score']:.4f} "
            f"| EDIV={r['early_div']:+.3f} "
            f"| NETEFF={fmt(r['net_eff_diag'],3):>7} "
            f"| R60={r60:>9} "
            f"| {r['status']:7} "
            f"| {r['token_mint'][:22]}"
        )


    print()
    print("IMPORTANT:")
    print("• Strictly event_id > frozen boundary.")
    print("• Frozen coefficients never refit.")
    print("• Frozen TRAIN means/stds never refit.")
    print("• CONTROL and CAP-v2 use identical events.")
    print("• early_div definition is frozen.")
    print("• net_eff is diagnostic only.")
    print("• No trading threshold.")
    print("• Do not regenerate t59_capv2_frozen.json.")
    print(
        f"• Refresh every {REFRESH_SEC}s."
    )


# ============================================================
# MAIN
# ============================================================

print(
    "Starting T59 frozen CAP-v2 prospective shadow..."
)

time.sleep(1)


try:

    while True:

        capture_new()

        update_labels()

        show()

        time.sleep(
            REFRESH_SEC
        )


except KeyboardInterrupt:

    print()
    print(
        "T59 stopped safely."
    )


finally:

    db.close()
