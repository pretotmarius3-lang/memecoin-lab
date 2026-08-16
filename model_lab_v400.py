import sqlite3
import math
import os
import time
import statistics

import numpy as np

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier, export_text

from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
)

DB = "validation_v090.db"

RUNNER = 10.0
DUMP = -10.0

TRAIN_FRAC = 0.60
VAL_FRAC = 0.20

# Features chosen BEFORE this V4 test.
# Keep this list frozen during evaluation.
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
    return db


def load():

    db = connect()

    rows = db.execute("""
        SELECT
            e.id,
            e.token_mint,
            e.timestamp,

            e.dex_return_60s,

            s.*

        FROM events e

        JOIN event_sequence_features_v340 s
        ON s.event_id = e.id

        WHERE
            e.dex_return_60s IS NOT NULL
            AND (
                e.dex_return_60s >= ?
                OR e.dex_return_60s <= ?
            )

        ORDER BY e.id ASC
    """, (
        RUNNER,
        DUMP
    )).fetchall()

    db.close()

    return rows


def build_xy(rows):

    X = []
    y = []
    ids = []
    tokens = []

    for r in rows:

        row = []

        for f in FEATURES:
            v = r[f]

            if valid(v):
                row.append(float(v))
            else:
                row.append(np.nan)

        X.append(row)

        y.append(
            1
            if r["dex_return_60s"] >= RUNNER
            else 0
        )

        ids.append(r["id"])
        tokens.append(r["token_mint"])

    return (
        np.array(X, dtype=float),
        np.array(y, dtype=int),
        ids,
        tokens,
    )


def metrics(y_true, pred, prob):

    out = {
        "N":
            len(y_true),

        "RUNNERS":
            int(sum(y_true == 1)),

        "DUMPS":
            int(sum(y_true == 0)),

        "ACC":
            accuracy_score(
                y_true,
                pred
            ),

        "BAL_ACC":
            balanced_accuracy_score(
                y_true,
                pred
            ),

        "PRECISION":
            precision_score(
                y_true,
                pred,
                zero_division=0
            ),

        "RECALL":
            recall_score(
                y_true,
                pred,
                zero_division=0
            ),

        "F1":
            f1_score(
                y_true,
                pred,
                zero_division=0
            ),
    }

    try:
        out["AUC"] = roc_auc_score(
            y_true,
            prob
        )
    except Exception:
        out["AUC"] = None

    out["CM"] = confusion_matrix(
        y_true,
        pred,
        labels=[0,1]
    )

    return out


def print_metrics(name, m):

    print()
    print(name)
    print("-" * 100)

    print(
        f"N={m['N']} "
        f"| RUNNER={m['RUNNERS']} "
        f"| DUMP={m['DUMPS']}"
    )

    print(
        f"ACC={m['ACC']:.3f} "
        f"| BAL_ACC={m['BAL_ACC']:.3f} "
        f"| PREC={m['PRECISION']:.3f} "
        f"| RECALL={m['RECALL']:.3f} "
        f"| F1={m['F1']:.3f} "
        f"| AUC="
        + (
            f"{m['AUC']:.3f}"
            if m["AUC"] is not None
            else "NA"
        )
    )

    cm = m["CM"]

    print()
    print("CONFUSION MATRIX")
    print("             PRED DUMP   PRED RUNNER")
    print(
        f"TRUE DUMP    {cm[0,0]:>9}   {cm[0,1]:>11}"
    )
    print(
        f"TRUE RUNNER  {cm[1,0]:>9}   {cm[1,1]:>11}"
    )


def threshold_table(
    title,
    y,
    prob
):

    print()
    print(title)
    print("-" * 100)

    print(
        f"{'THRESH':>7} "
        f"{'SIGNALS':>8} "
        f"{'RUNNERS':>8} "
        f"{'DUMPS':>7} "
        f"{'PRECISION':>10}"
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

        mask = (
            prob >= t
        )

        n = int(
            mask.sum()
        )

        if n == 0:
            continue

        runners = int(
            y[mask].sum()
        )

        dumps = (
            n-runners
        )

        precision = (
            runners/n
        )

        print(
            f"{t:>7.2f} "
            f"{n:>8} "
            f"{runners:>8} "
            f"{dumps:>7} "
            f"{precision:>9.1%}"
        )


def logistic_coefficients(
    model
):

    clf = model.named_steps[
        "clf"
    ]

    coefs = clf.coef_[0]

    ranked = sorted(
        zip(
            FEATURES,
            coefs
        ),
        key=lambda x:
            abs(x[1]),
        reverse=True
    )

    print()
    print("=" * 100)
    print(
        "LOGISTIC — "
        "STANDARDIZED FEATURE WEIGHTS"
    )
    print("=" * 100)

    print(
        "Positive weight => more RUNNER-like."
    )

    print(
        "Negative weight => more DUMP-like."
    )

    print()

    for name,coef in ranked:

        print(
            f"{name:32} "
            f"{coef:+.4f}"
        )


def prediction_detail(
    title,
    rows,
    y,
    prob,
):

    print()
    print("=" * 110)
    print(title)
    print("=" * 110)

    ranked = sorted(
        zip(
            rows,
            y,
            prob
        ),
        key=lambda x:
            x[2],
        reverse=True
    )

    print(
        f"{'ID':>5} "
        f"{'PROB':>7} "
        f"{'TRUE':>7} "
        f"{'R60':>9} "
        f"{'TOKEN':18}"
    )

    print("-" * 70)

    for r,true,p in ranked:

        print(
            f"{r['id']:>5} "
            f"{p:>6.1%} "
            f"{('RUN' if true else 'DUMP'):>7} "
            f"{r['dex_return_60s']:+8.2f}% "
            f"{r['token_mint'][:18]}"
        )


while True:

    try:

        rows = load()

        n = len(rows)

        if n < 60:

            print(
                "Need more runner/dump cases."
            )

            print(
                f"Current N={n}"
            )

            time.sleep(20)
            continue

        train_end = int(
            n * TRAIN_FRAC
        )

        val_end = int(
            n * (
                TRAIN_FRAC
                + VAL_FRAC
            )
        )

        train_rows = (
            rows[:train_end]
        )

        val_rows = (
            rows[
                train_end:val_end
            ]
        )

        test_rows = (
            rows[val_end:]
        )

        X_train,y_train,_,_ = (
            build_xy(
                train_rows
            )
        )

        X_val,y_val,_,_ = (
            build_xy(
                val_rows
            )
        )

        X_test,y_test,_,_ = (
            build_xy(
                test_rows
            )
        )

        # ====================================================
        # LOGISTIC REGRESSION
        # ====================================================

        logistic = Pipeline([
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

        logistic.fit(
            X_train,
            y_train
        )

        log_val_prob = (
            logistic.predict_proba(
                X_val
            )[:,1]
        )

        log_val_pred = (
            log_val_prob >= .50
        ).astype(int)

        log_test_prob = (
            logistic.predict_proba(
                X_test
            )[:,1]
        )

        log_test_pred = (
            log_test_prob >= .50
        ).astype(int)

        # ====================================================
        # SMALL DECISION TREE
        # ====================================================

        tree = Pipeline([
            (
                "imputer",
                SimpleImputer(
                    strategy="median"
                )
            ),

            (
                "clf",
                DecisionTreeClassifier(
                    max_depth=3,
                    min_samples_leaf=8,
                    class_weight="balanced",
                    random_state=42,
                )
            ),
        ])

        tree.fit(
            X_train,
            y_train
        )

        tree_val_prob = (
            tree.predict_proba(
                X_val
            )[:,1]
        )

        tree_val_pred = (
            tree_val_prob >= .50
        ).astype(int)

        tree_test_prob = (
            tree.predict_proba(
                X_test
            )[:,1]
        )

        tree_test_pred = (
            tree_test_prob >= .50
        ).astype(int)

        # ====================================================
        # METRICS
        # ====================================================

        log_val_metrics = metrics(
            y_val,
            log_val_pred,
            log_val_prob
        )

        log_test_metrics = metrics(
            y_test,
            log_test_pred,
            log_test_prob
        )

        tree_val_metrics = metrics(
            y_val,
            tree_val_pred,
            tree_val_prob
        )

        tree_test_metrics = metrics(
            y_test,
            tree_test_pred,
            tree_test_prob
        )

        os.system("clear")

        print("=" * 110)
        print(
            "MEMECOIN LAB — "
            "V4 MODEL LAB"
        )
        print("=" * 110)

        print(
            f"TARGET: "
            f"RUNNER >= +{RUNNER:.0f}% "
            f"vs DUMP <= {DUMP:.0f}%"
        )

        print()

        print(
            f"TOTAL LABELED : {n}"
        )

        print(
            f"TRAIN         : "
            f"{len(train_rows)} "
            f"(IDs "
            f"{train_rows[0]['id']}"
            f"→"
            f"{train_rows[-1]['id']})"
        )

        print(
            f"VALIDATION    : "
            f"{len(val_rows)} "
            f"(IDs "
            f"{val_rows[0]['id']}"
            f"→"
            f"{val_rows[-1]['id']})"
        )

        print(
            f"TEST          : "
            f"{len(test_rows)} "
            f"(IDs "
            f"{test_rows[0]['id']}"
            f"→"
            f"{test_rows[-1]['id']})"
        )

        print()

        print(
            "FEATURES FROZEN:"
        )

        for f in FEATURES:
            print(
                "•",
                f
            )

        print()
        print("=" * 110)
        print(
            "LOGISTIC REGRESSION"
        )
        print("=" * 110)

        print_metrics(
            "VALIDATION",
            log_val_metrics
        )

        print_metrics(
            "FINAL TEST",
            log_test_metrics
        )

        threshold_table(
            "LOGISTIC — TEST PROBABILITY THRESHOLDS",
            y_test,
            log_test_prob
        )

        logistic_coefficients(
            logistic
        )

        print()
        print("=" * 110)
        print(
            "SMALL DECISION TREE"
        )
        print("=" * 110)

        print_metrics(
            "VALIDATION",
            tree_val_metrics
        )

        print_metrics(
            "FINAL TEST",
            tree_test_metrics
        )

        threshold_table(
            "TREE — TEST PROBABILITY THRESHOLDS",
            y_test,
            tree_test_prob
        )

        tree_model = (
            tree.named_steps[
                "clf"
            ]
        )

        print()
        print(
            "TREE RULES"
        )
        print("-" * 110)

        print(
            export_text(
                tree_model,
                feature_names=FEATURES
            )
        )

        prediction_detail(
            "LOGISTIC — FINAL TEST DETAIL",
            test_rows,
            y_test,
            log_test_prob
        )

        print()
        print("=" * 110)
        print("IMPORTANT")
        print("=" * 110)

        print(
            "The FINAL TEST must not be used "
            "to tune this exact V4 model."
        )

        print(
            "If performance is weak, "
            "we formulate V4.1 and validate it "
            "on NEW future data."
        )

        print(
            "AUC around 0.50 = no useful discrimination."
        )

        print(
            "AUC materially above 0.60 on untouched test "
            "would already be interesting at this stage."
        )

        print(
            "Refresh every 60 seconds as new labeled events arrive."
        )

        time.sleep(60)

    except KeyboardInterrupt:

        print(
            "\nV4 Model Lab stopped."
        )

        break

    except Exception as e:

        print(
            "ERROR:",
            repr(e)
        )

        time.sleep(10)
