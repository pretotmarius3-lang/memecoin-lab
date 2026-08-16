import sqlite3
import math
import os
import time
from collections import defaultdict

import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    roc_auc_score,
    balanced_accuracy_score,
)

DB = "validation_v090.db"

BOUNDARY_ID = 417

RUNNER = 10.0
DUMP = -10.0

FEATURES = [
    "mid_buy_count",
    "mid_sell_count",
    "recent_unique_buyers",

    "early_swaps_per_sec",
    "mid_swaps_per_sec",
    "recent_swaps_per_sec",

    "buy_concentration_trend",

    "recent_price_return",
    "mid_price_return",

    "recent_sell_sol",
    "recent_net_sol",

    "recent_buy_share",
]


def valid(x):
    return (
        x is not None
        and isinstance(x, (int, float))
        and math.isfinite(x)
    )


def connect():
    db = sqlite3.connect(
        DB,
        timeout=30
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA journal_mode=WAL")
    return db


def build_x(row):
    vals = []

    for f in FEATURES:
        v = row[f]
        vals.append(
            float(v)
            if valid(v)
            else np.nan
        )

    return np.array(
        [vals],
        dtype=float
    )


def safe_auc(y, p):
    if len(y) < 2:
        return None

    if len(set(y)) < 2:
        return None

    return roc_auc_score(
        y,
        p
    )


def train_frozen_model(db):

    rows = db.execute("""
        SELECT
            e.id,
            e.dex_return_60s,
            s.*

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        WHERE
            e.id <= ?
            AND e.dex_return_60s IS NOT NULL
            AND (
                e.dex_return_60s >= ?
                OR e.dex_return_60s <= ?
            )

        ORDER BY e.id ASC
    """, (
        BOUNDARY_ID,
        RUNNER,
        DUMP,
    )).fetchall()

    X = []
    y = []

    for r in rows:

        vals = []

        for f in FEATURES:
            v = r[f]

            vals.append(
                float(v)
                if valid(v)
                else np.nan
            )

        X.append(vals)

        y.append(
            1
            if r["dex_return_60s"] >= RUNNER
            else 0
        )

    X = np.asarray(
        X,
        dtype=float
    )

    y = np.asarray(
        y,
        dtype=int
    )

    model = Pipeline([
        (
            "imputer",
            SimpleImputer(
                strategy="median"
            )
        ),

        (
            "scale",
            StandardScaler()
        ),

        (
            "clf",
            LogisticRegression(
                C=0.5,
                max_iter=500,
                class_weight="balanced",
                random_state=42,
            )
        ),
    ])

    model.fit(
        X,
        y
    )

    return (
        model,
        len(rows),
        int(y.sum()),
        int(len(y)-y.sum()),
    )


def ensure_table(db):

    db.execute("""
        CREATE TABLE IF NOT EXISTS v4_frozen_predictions (

            event_id INTEGER PRIMARY KEY,

            token_mint TEXT,

            prediction_timestamp REAL,

            model_boundary_id INTEGER,

            probability_runner REAL,

            predicted_class INTEGER,

            outcome_timestamp REAL,

            dex_return_60s REAL,

            label INTEGER,

            labeled INTEGER DEFAULT 0
        )
    """)

    db.commit()


def predict_new_events(
    db,
    model
):

    rows = db.execute("""
        SELECT
            e.id,
            e.token_mint,

            s.*

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id=e.id

        LEFT JOIN v4_frozen_predictions p
        ON p.event_id=e.id

        WHERE
            e.id > ?
            AND p.event_id IS NULL

        ORDER BY e.id ASC
    """, (
        BOUNDARY_ID,
    )).fetchall()

    added = 0

    for r in rows:

        X = build_x(r)

        prob = float(
            model.predict_proba(
                X
            )[0,1]
        )

        pred = (
            1
            if prob >= 0.50
            else 0
        )

        db.execute("""
            INSERT OR IGNORE INTO
            v4_frozen_predictions (

                event_id,
                token_mint,

                prediction_timestamp,

                model_boundary_id,

                probability_runner,

                predicted_class,

                labeled
            )

            VALUES (
                ?,?,?,?,?,?,0
            )
        """, (
            r["id"],
            r["token_mint"],
            time.time(),
            BOUNDARY_ID,
            prob,
            pred,
        ))

        added += 1

    db.commit()

    return added


def label_outcomes(db):

    rows = db.execute("""
        SELECT
            p.event_id,
            e.dex_return_60s

        FROM v4_frozen_predictions p

        JOIN events e
        ON e.id=p.event_id

        WHERE
            p.labeled=0
            AND e.dex_return_60s
            IS NOT NULL
    """).fetchall()

    labeled_now = 0

    for r in rows:

        ret = r[
            "dex_return_60s"
        ]

        if ret >= RUNNER:
            label = 1

        elif ret <= DUMP:
            label = 0

        else:
            # Neutral event:
            # prediction stays stored,
            # but it is not part of
            # RUNNER-vs-DUMP evaluation.
            label = None

        db.execute("""
            UPDATE v4_frozen_predictions

            SET
                outcome_timestamp=?,
                dex_return_60s=?,
                label=?,
                labeled=1

            WHERE event_id=?
        """, (
            time.time(),
            ret,
            label,
            r["event_id"],
        ))

        labeled_now += 1

    db.commit()

    return labeled_now


def first_per_token(rows):

    seen = set()
    out = []

    for r in rows:

        token = r[
            "token_mint"
        ]

        if token in seen:
            continue

        seen.add(token)
        out.append(r)

    return out


def eval_rows(
    rows,
    title
):

    usable = [
        r for r in rows
        if r["label"] is not None
    ]

    print()
    print(title)
    print("-" * 110)

    if not usable:
        print("NO LABELED RUNNER/DUMP CASES")
        return

    y = [
        int(r["label"])
        for r in usable
    ]

    p = [
        float(
            r["probability_runner"]
        )
        for r in usable
    ]

    pred = [
        1 if x >= .50 else 0
        for x in p
    ]

    auc = safe_auc(
        y,
        p
    )

    bal = balanced_accuracy_score(
        y,
        pred
    )

    tokens = len(set(
        r["token_mint"]
        for r in usable
    ))

    runners = sum(y)
    dumps = len(y)-runners

    print(
        f"N={len(usable)} "
        f"| TOKENS={tokens} "
        f"| RUNNERS={runners} "
        f"| DUMPS={dumps}"
    )

    print(
        "AUC="
        + (
            f"{auc:.3f}"
            if auc is not None
            else "NA"
        )
        + f" | BAL_ACC={bal:.3f}"
    )

    print()

    print(
        f"{'THRESH':>7} "
        f"{'SIGNALS':>8} "
        f"{'RUN':>6} "
        f"{'DUMP':>6} "
        f"{'PREC':>8} "
        f"{'TOKENS':>7}"
    )

    for t in [
        .50,
        .55,
        .60,
        .65,
        .70,
        .75,
        .80,
    ]:

        selected = [
            r for r in usable
            if r[
                "probability_runner"
            ] >= t
        ]

        if not selected:
            continue

        nr = sum(
            r["label"] == 1
            for r in selected
        )

        nd = sum(
            r["label"] == 0
            for r in selected
        )

        ntok = len(set(
            r["token_mint"]
            for r in selected
        ))

        precision = (
            nr / len(selected)
        )

        print(
            f"{t:7.2f} "
            f"{len(selected):8d} "
            f"{nr:6d} "
            f"{nd:6d} "
            f"{precision:8.1%} "
            f"{ntok:7d}"
        )


def token_macro_summary(rows):

    usable = [
        r for r in rows
        if r["label"] is not None
    ]

    groups = defaultdict(list)

    for r in usable:
        groups[
            r["token_mint"]
        ].append(r)

    if not groups:
        return

    token_scores = []

    for token, rr in groups.items():

        avg_prob = sum(
            r["probability_runner"]
            for r in rr
        ) / len(rr)

        # Majority token label.
        labels = [
            r["label"]
            for r in rr
        ]

        token_label = (
            1
            if sum(labels)
            >= len(labels)/2
            else 0
        )

        token_scores.append(
            (
                token_label,
                avg_prob
            )
        )

    y = [
        x[0]
        for x in token_scores
    ]

    p = [
        x[1]
        for x in token_scores
    ]

    auc = safe_auc(
        y,
        p
    )

    print()
    print(
        "TOKEN-MACRO:"
    )

    print(
        f"TOKENS={len(token_scores)}"
        f" | AUC="
        + (
            f"{auc:.3f}"
            if auc is not None
            else "NA"
        )
    )


db = connect()

ensure_table(db)

model, train_n, train_run, train_dump = (
    train_frozen_model(
        db
    )
)

while True:

    try:

        new_preds = predict_new_events(
            db,
            model
        )

        new_labels = label_outcomes(
            db
        )

        rows = db.execute("""
            SELECT *
            FROM v4_frozen_predictions
            ORDER BY event_id ASC
        """).fetchall()

        labeled = [
            r for r in rows
            if r["labeled"] == 1
        ]

        binary = [
            r for r in labeled
            if r["label"] is not None
        ]

        neutral = [
            r for r in labeled
            if r["label"] is None
        ]

        os.system("clear")

        print("=" * 110)
        print(
            "MEMECOIN LAB — "
            "V4 FROZEN PROSPECTIVE OOS"
        )
        print("=" * 110)

        print(
            f"MODEL FROZEN AT ID <= "
            f"{BOUNDARY_ID}"
        )

        print(
            f"TRAIN LABELED : "
            f"{train_n}"
            f" | RUN={train_run}"
            f" | DUMP={train_dump}"
        )

        print()

        print(
            f"PROSPECTIVE PREDICTIONS : "
            f"{len(rows)}"
        )

        print(
            f"OUTCOME AVAILABLE       : "
            f"{len(labeled)}"
        )

        print(
            f"RUNNER/DUMP LABELED     : "
            f"{len(binary)}"
        )

        print(
            f"NEUTRAL (-10% to +10%)  : "
            f"{len(neutral)}"
        )

        print(
            f"UNIQUE TOKENS BINARY    : "
            f"{len(set(r['token_mint'] for r in binary))}"
        )

        eval_rows(
            binary,
            "A) ALL PROSPECTIVE RUNNER/DUMP EVENTS"
        )

        eval_rows(
            first_per_token(
                binary
            ),
            "B) FIRST LABELED EVENT PER TOKEN"
        )

        token_macro_summary(
            binary
        )

        print()
        print("=" * 110)
        print(
            "LATEST PROSPECTIVE CASES"
        )
        print("=" * 110)

        latest = rows[-20:]

        print(
            f"{'ID':>5} "
            f"{'PROB':>7} "
            f"{'STATUS':>10} "
            f"{'R60':>9} "
            f"{'TOKEN':18}"
        )

        print("-" * 70)

        for r in reversed(
            latest
        ):

            if not r["labeled"]:
                status = "WAIT"

            elif r["label"] == 1:
                status = "RUN"

            elif r["label"] == 0:
                status = "DUMP"

            else:
                status = "NEUTRAL"

            ret = r[
                "dex_return_60s"
            ]

            ret_txt = (
                f"{ret:+8.2f}%"
                if ret is not None
                else "      NA"
            )

            print(
                f"{r['event_id']:>5} "
                f"{r['probability_runner']:>6.1%} "
                f"{status:>10} "
                f"{ret_txt} "
                f"{r['token_mint'][:18]}"
            )

        print()
        print("=" * 110)
        print("CHECKPOINTS")
        print("=" * 110)

        print(
            "10 binary cases  = first look only"
        )

        print(
            "25 binary cases  = early signal"
        )

        print(
            "50 binary cases  = meaningful checkpoint"
        )

        print(
            "100 binary cases = much stronger test"
        )

        print()
        print(
            "Do NOT change V4 Frozen from these results."
        )

        print(
            "New predictions are stored before outcomes."
        )

        print(
            "Refresh every 10 seconds."
        )

        time.sleep(10)

    except KeyboardInterrupt:

        print(
            "\nV4 Frozen monitor stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(5)

db.close()
