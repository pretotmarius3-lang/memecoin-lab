#!/usr/bin/env python3
"""MEMECOIN LAB — 30K MARKET-CAP MIGRATION CROSSING AUDIT V7.7.1

READ-ONLY discovery infrastructure.
Operational migration definition supplied by research protocol:
    first causal trade where estimated market cap crosses >= 30,000 USD.

MC proxy:
    price_sol * token_supply * SOL_USD

Important:
- Uses canonical v52_swaps only.
- No strategy / no capital decision.
- Requires an explicit SOL/USD reference via --sol-usd or MEMECOIN_V771_SOL_USD.
- Supply defaults to 1,000,000,000 tokens but is printed prominently and can be overridden.
- This version is an AUDIT: it measures whether the proxy gives a credible cohort before any alpha study.
"""
from __future__ import annotations

import argparse
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
FEATURE = ROOT / "v52_features.db"
DEFAULT_SUPPLY = float(os.environ.get("MEMECOIN_V771_TOKEN_SUPPLY", "1000000000"))
DEFAULT_MC = float(os.environ.get("MEMECOIN_V771_MC_USD", "30000"))


def sf(x):
    try:
        z = float(x)
        return z if math.isfinite(z) else None
    except Exception:
        return None


def pct(xs, q):
    if not xs:
        return None
    ys = sorted(xs)
    p = (len(ys)-1)*q
    lo = int(p); hi = min(len(ys)-1, lo+1); f = p-lo
    return ys[lo] + (ys[hi]-ys[lo])*f


def fmt(x, nd=3):
    return "None" if x is None else f"{x:.{nd}f}"


def ro():
    if not FEATURE.exists():
        raise SystemExit(f"Missing {FEATURE}")
    d = sqlite3.connect(f"file:{FEATURE}?mode=ro", uri=True, timeout=30)
    d.row_factory = sqlite3.Row
    d.execute("PRAGMA query_only=ON")
    d.execute("PRAGMA busy_timeout=30000")
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sol-usd", type=float, default=sf(os.environ.get("MEMECOIN_V771_SOL_USD")), help="Explicit SOL/USD reference used only for the 30k MC proxy")
    ap.add_argument("--supply", type=float, default=DEFAULT_SUPPLY)
    ap.add_argument("--mc", type=float, default=DEFAULT_MC)
    args = ap.parse_args()

    print("="*136)
    print("MEMECOIN LAB — 30K MC MIGRATION CROSSING AUDIT V7.7.1")
    print("="*136)
    print("READ-ONLY | first causal price crossing only | no strategy evidence")
    print(f"mc_threshold_usd={args.mc:.2f} token_supply_assumption={args.supply:.0f} SOL_USD={args.sol_usd}")

    if args.sol_usd is None or args.sol_usd <= 0:
        print("\nSTATUS=NEED_EXPLICIT_SOL_USD")
        print("Run e.g. python3 v771_30k_mc_migration_crossing_audit.py --sol-usd <SOL_USD>")
        print("Guardrail: refusing to invent a historical/current SOL/USD conversion.")
        return

    price_threshold_sol = args.mc / (args.supply * args.sol_usd)
    print(f"derived_price_threshold_sol={price_threshold_sol:.12g} SOL/token")

    d = ro()
    total_swaps = d.execute("SELECT COUNT(*) FROM v52_swaps").fetchone()[0]
    total_tokens = d.execute("SELECT COUNT(DISTINCT token_mint) FROM v52_swaps").fetchone()[0]
    priced_swaps = d.execute("SELECT COUNT(*) FROM v52_swaps WHERE price_sol IS NOT NULL AND price_sol>0").fetchone()[0]
    priced_tokens = d.execute("SELECT COUNT(DISTINCT token_mint) FROM v52_swaps WHERE price_sol IS NOT NULL AND price_sol>0").fetchone()[0]
    print("\nCANONICAL PRICE COVERAGE")
    print(f"swaps={total_swaps} priced_swaps={priced_swaps} coverage={100*priced_swaps/max(1,total_swaps):.2f}%")
    print(f"tokens={total_tokens} priced_tokens={priced_tokens} coverage={100*priced_tokens/max(1,total_tokens):.2f}%")

    # Stream ordered canonical prices. This avoids loading the whole DB into memory.
    cur = d.execute("""
        SELECT token_mint,timestamp,observed_at,price_sol,signature
        FROM v52_swaps
        WHERE price_sol IS NOT NULL AND price_sol>0
        ORDER BY token_mint,timestamp,signature
    """)

    first = {}
    crossings = {}
    pre_seen = set()
    max_mc = defaultdict(float)
    for r in cur:
        mint = str(r["token_mint"])
        ts = float(r["timestamp"])
        obs = float(r["observed_at"])
        p = float(r["price_sol"])
        mc = p * args.supply * args.sol_usd
        if mint not in first:
            first[mint] = (ts, obs, mc)
        max_mc[mint] = max(max_mc[mint], mc)
        if mc < args.mc:
            pre_seen.add(mint)
        elif mint not in crossings:
            # Strict crossing when we have seen a below-threshold price. If the token's
            # first canonical observation is already above 30k, retain separately as left-censored.
            crossings[mint] = {
                "ts": ts, "obs": obs, "mc": mc, "sig": str(r["signature"]),
                "strict": mint in pre_seen,
            }
    d.close()

    strict = {m:z for m,z in crossings.items() if z["strict"]}
    censored = {m:z for m,z in crossings.items() if not z["strict"]}
    never = [m for m in first if m not in crossings]
    ages = [z["ts"] - first[m][0] for m,z in strict.items()]
    obs_lags = [z["obs"] - z["ts"] for z in strict.values()]
    overshoot = [(z["mc"] / args.mc - 1.0)*100 for z in strict.values()]

    print("\n30K CROSSING CENSUS")
    print(f"priced_cohort={len(first)} strict_crossings={len(strict)} left_censored_above30k={len(censored)} never_crossed={len(never)}")
    print(f"strict_migration_rate={100*len(strict)/max(1,len(first)):.2f}%")
    if ages:
        print("create_proxy(first canonical swap) -> 30k crossing age_s "
              f"p50={fmt(pct(ages,.5))} p90={fmt(pct(ages,.9))} p95={fmt(pct(ages,.95))} max={fmt(max(ages))}")
        print("crossing chain ts -> canonical observed lag_s "
              f"p50={fmt(pct(obs_lags,.5))} p90={fmt(pct(obs_lags,.9))} p95={fmt(pct(obs_lags,.95))}")
        print("first crossing MC overshoot_pct "
              f"p50={fmt(pct(overshoot,.5))} p90={fmt(pct(overshoot,.9))} p95={fmt(pct(overshoot,.95))}")

    # Snapshot availability around strict migration cohort.
    if strict:
        d = ro()
        mints = list(strict)
        covered = {10:0,20:0,30:0}
        all3 = 0
        chunk = 700
        have = defaultdict(set)
        for i in range(0,len(mints),chunk):
            sub = mints[i:i+chunk]
            qs = ",".join("?" for _ in sub)
            for r in d.execute(f"SELECT token_mint,stage_s FROM v7611_causal_snapshots WHERE token_mint IN ({qs}) AND stage_s IN (10,20,30)", sub):
                have[str(r[0])].add(int(r[1]))
        d.close()
        for m in mints:
            ss = have.get(m,set())
            for st in covered:
                covered[st] += int(st in ss)
            all3 += int(all(st in ss for st in (10,20,30)))
        print("\nCAUSAL SNAPSHOT COVERAGE — STRICT CROSSINGS")
        print(f"T10={covered[10]} T20={covered[20]} T30={covered[30]} all3={all3}/{len(strict)}")

    print("\nQUALITY GATE")
    if len(strict) >= 50 and priced_tokens/max(1,total_tokens) >= .8:
        print("STATUS=30K_PROXY_COHORT_USABLE")
        print("Next: V7.7.2 PRE/MIGRATION/POST path reconstruction using only strict first crossings.")
    elif len(strict) >= 20:
        print("STATUS=30K_PROXY_COHORT_SMALL_BUT_USABLE_FOR_CENSUS")
        print("Accumulate more before high-dimensional alpha discovery.")
    else:
        print("STATUS=INSUFFICIENT_STRICT_30K_CROSSINGS")
        print("Do not run alpha discovery yet; verify supply/SOLUSD assumptions and accumulate more data.")

    print("\nGuardrails: first canonical swap is only a CREATE proxy in this audit; left-censored tokens are excluded from strict migration evidence.")

if __name__ == "__main__":
    main()
