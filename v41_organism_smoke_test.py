#!/usr/bin/env python3
"""Static/import smoke test for the recursive V4.1 organism.

This does not run market research. It verifies that the module imports and the
recursive director exposes the expected entry points.
"""

import importlib


def main():
    m = importlib.import_module("v41_organism")
    required = [
        "auto_director_tick",
        "spawn_children",
        "evaluate_feature_set",
        "evaluate_robust",
        "freeze_candidate",
        "worker_main",
        "main",
    ]
    missing = [name for name in required if not hasattr(m, name)]
    if missing:
        raise SystemExit(f"missing organism symbols: {missing}")
    assert m.WORKERS >= 1
    assert m.ROBUST_SPLITS >= 1
    print("V4.1 ORGANISM IMPORT TEST OK")


if __name__ == "__main__":
    main()
