#!/usr/bin/env python3
"""MEMECOIN LAB — PRE / MIGRATION / POST PATH RECONSTRUCTION V7.7.2

Research infrastructure only. Reconstructs event-time price/flow paths around the
first strict 30k market-cap crossing defined in V7.7.1.

Operational migration proxy:
    MC = price_sol * supply * SOL_USD
    migration = first price >= threshold after at least one observed price < threshold

Important guardrails:
- Source v52_features.db is opened READ-ONLY.
- Left-censored tokens (first canonical price already >=30k) are excluded.
- first canonical swap is only a create proxy.
- A fixed SOL/USD reference is a proxy, not historical FX truth.
- No alpha rule / no threshold tuning / no capital decision.
"""
from __future__ import annotations

import argparse, math, os, sqlite3, statistics
from collections import defaultdict
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/"v52_features.db"
OUT=ROOT/"v772_migration_paths.db"
PRE=(-60,-30,-20,-10,-5)
POST=(5,10,20,30,60,120,300)
DEFAULT_SUPPLY=float(os.environ.get("MEMECOIN_V772_TOKEN_SUPPLY","1000000000"))
DEFAULT_MC=float(os.environ.get("MEMECOIN_V772_MC_USD","30000"))


def sf(x):
    try:
        z=float(x); return z if math.isfinite(z) else None
    except Exception:return None


def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def ro():
    d=sqlite3.connect(f"file:{FEATURE}?mode=ro",uri=True,timeout=30)
    d.row_factory=sqlite3.Row; d.execute("PRAGMA query_only=ON"); d.execute("PRAGMA busy_timeout=30000")
    return d


def odb():
    d=sqlite3.connect(OUT,timeout=30); d.row_factory=sqlite3.Row
    d.execute("PRAGMA journal_mode=WAL"); d.execute("PRAGMA synchronous=NORMAL"); d.execute("PRAGMA busy_timeout=30000")
    return d


def init_out():
    d=odb(); d.executescript('''
    CREATE TABLE IF NOT EXISTS migration_crossings(
      token_mint TEXT PRIMARY KEY, first_ts REAL, migration_ts REAL, migration_observed_at REAL,
      migration_signature TEXT, migration_price_sol REAL, migration_mc_usd REAL, age_s REAL,
      sol_usd REAL, supply REAL, mc_threshold REAL
    );
    CREATE TABLE IF NOT EXISTS migration_path(
      token_mint TEXT NOT NULL, offset_s INTEGER NOT NULL, side TEXT NOT NULL,
      target_ts REAL NOT NULL, sample_ts REAL, gap_s REAL, price_sol REAL,
      return_from_migration_pct REAL, quote_sol REAL, trade_side TEXT,
      observed_at REAL, PRIMARY KEY(token_mint,offset_s)
    );
    CREATE INDEX IF NOT EXISTS idx_v772_path_offset ON migration_path(offset_s);
    '''); d.commit(); d.close()


def choose_sample(rows,target,offset):
    # PRE uses latest trade at/before target; POST uses earliest trade at/after target.
    if offset<0:
        cand=[r for r in rows if float(r['timestamp'])<=target]
        if not cand:return None
        return cand[-1]
    cand=[r for r in rows if float(r['timestamp'])>=target]
    return cand[0] if cand else None


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--sol-usd',type=float,default=sf(os.environ.get('MEMECOIN_V772_SOL_USD')))
    ap.add_argument('--supply',type=float,default=DEFAULT_SUPPLY)
    ap.add_argument('--mc',type=float,default=DEFAULT_MC)
    ap.add_argument('--max-gap',type=float,default=15.0,help='tight coverage tolerance in seconds')
    args=ap.parse_args()
    if args.sol_usd is None or args.sol_usd<=0: raise SystemExit('Provide --sol-usd, e.g. --sol-usd 82')
    th=args.mc/(args.supply*args.sol_usd)
    print('='*140)
    print('MEMECOIN LAB — PRE / MIGRATION / POST PATH RECONSTRUCTION V7.7.2')
    print('='*140)
    print(f'RESEARCH ONLY | MC={args.mc:.0f} supply={args.supply:.0f} SOL_USD={args.sol_usd:.2f} price_threshold={th:.12g}')

    d=ro()
    rows=d.execute('''SELECT token_mint,timestamp,observed_at,price_sol,quote_sol,side,signature
                      FROM v52_swaps WHERE price_sol IS NOT NULL AND price_sol>0
                      ORDER BY token_mint,timestamp,signature''').fetchall()
    d.close()
    by=defaultdict(list)
    for r in rows: by[str(r['token_mint'])].append(r)

    strict={}; censored=0
    for mint,rr in by.items():
        first=rr[0]; seen_below=False
        for r in rr:
            p=float(r['price_sol'])
            if p<th:
                seen_below=True; continue
            if not seen_below:
                censored+=1
            else:
                strict[mint]=(first,r)
            break

    init_out(); out=odb(); out.execute('DELETE FROM migration_crossings'); out.execute('DELETE FROM migration_path')
    cover={o:0 for o in PRE+POST}; tight={o:0 for o in PRE+POST}; returns=defaultdict(list); gaps=defaultdict(list)
    for mint,(first,mig) in strict.items():
        mts=float(mig['timestamp']); mp=float(mig['price_sol']); mc=mp*args.supply*args.sol_usd
        out.execute('INSERT OR REPLACE INTO migration_crossings VALUES(?,?,?,?,?,?,?,?,?,?,?)',(
            mint,float(first['timestamp']),mts,float(mig['observed_at']),str(mig['signature']),mp,mc,
            mts-float(first['timestamp']),args.sol_usd,args.supply,args.mc))
        rr=by[mint]
        for off in PRE+POST:
            target=mts+off; s=choose_sample(rr,target,off)
            if s is None: continue
            sts=float(s['timestamp']); gap=abs(sts-target); p=float(s['price_sol']); ret=(p/mp-1.0)*100
            cover[off]+=1; gaps[off].append(gap)
            if gap<=args.max_gap:tight[off]+=1
            if off>0:returns[off].append(ret)
            out.execute('INSERT OR REPLACE INTO migration_path VALUES(?,?,?,?,?,?,?,?,?,?,?)',(
                mint,off,'PRE' if off<0 else 'POST',target,sts,gap,p,ret,s['quote_sol'],s['side'],s['observed_at']))
    out.commit(); out.close()

    n=len(strict)
    print(f'\nCOHORT strict_crossings={n} left_censored={censored} output={OUT.name}')
    print('\nEVENT-TIME COVERAGE')
    for off in PRE+POST:
        medgap=pct(gaps[off],.5); p90gap=pct(gaps[off],.9)
        print(f'{off:+4d}s any={cover[off]:4d}/{n:<4d} tight<={args.max_gap:g}s={tight[off]:4d}/{n:<4d} gap p50/p90={medgap if medgap is not None else None}/{p90gap if p90gap is not None else None}')

    print('\nPOST-MIGRATION PRICE PATH — relative to crossing price')
    for off in POST:
        xs=returns[off]
        if not xs: continue
        win=sum(x>=25 for x in xs)/len(xs); crash=sum(x<=-50 for x in xs)/len(xs)
        print(f'+{off:3d}s n={len(xs):4d} mean={statistics.mean(xs):+9.2f}% med={statistics.median(xs):+8.2f}% p10/p90={pct(xs,.1):+8.2f}/{pct(xs,.9):+8.2f}% win>=25={100*win:5.1f}% crash<=-50={100*crash:5.1f}%')

    complete=sum(1 for mint in strict if all(any(str(r['token_mint'])==mint and int(r['offset_s'])==o for r in []) for o in ()))
    # Gate is based on direct coverage counts, not the dummy variable above.
    core=min(tight.get(-10,0),tight.get(10,0),tight.get(30,0),tight.get(60,0))
    print('\nQUALITY GATE')
    if n>=100 and core>=50:
        print('STATUS=MIGRATION_PATH_COHORT_USABLE')
        print('Next: V7.7.3 discovery on PRE-state vs post-migration winners/crashes, with strict event-time gap filters.')
    elif n>=50:
        print('STATUS=MIGRATION_PATH_COHORT_USABLE_WITH_SPARSE_EVENT_TIME')
        print('Use gap-filtered subsets; do not impute missing event-time observations.')
    else:
        print('STATUS=INSUFFICIENT_MIGRATION_PATHS')
    print('\nGuardrail: fixed SOL/USD=%.2f is a migration proxy; replace with timestamped SOL/USD before treating 30k as exact historical USD MC.'%args.sol_usd)

if __name__=='__main__':main()
