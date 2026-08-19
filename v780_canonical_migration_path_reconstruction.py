#!/usr/bin/env python3
"""MEMECOIN LAB — CANONICAL MIGRATION PRE/POST RECONSTRUCTION V7.8.0

Uses ONLY v779_canonical_migrations.db as migration truth.
Reconstructs causal pre-migration state and post-migration price path from v52_swaps.
No strategy rule, no threshold search, no future-only confirmation claim.
Source DBs are READ-ONLY; output is a separate research DB.
"""
from __future__ import annotations

import math, sqlite3, statistics, time
from collections import Counter
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
MIG=ROOT/'v779_canonical_migrations.db'
FEATURE=ROOT/'v52_features.db'
OUT=ROOT/'v780_canonical_migration_paths.db'
PRE_WINDOWS=(10,30,60,120)
POST_H=(5,10,30,60,120,300)
MAX_POST_GAP=15.0


def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d


def odb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=NORMAL');d.execute('PRAGMA busy_timeout=30000');return d


def init_out():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS migration_paths(
      signature TEXT PRIMARY KEY,mint TEXT NOT NULL,migration_ts REAL NOT NULL,
      first_swap_ts REAL,last_pre_ts REAL,pre_age_s REAL,
      pre10_swaps INTEGER,pre30_swaps INTEGER,pre60_swaps INTEGER,pre120_swaps INTEGER,
      pre10_buys INTEGER,pre10_sells INTEGER,pre30_buys INTEGER,pre30_sells INTEGER,
      pre60_buys INTEGER,pre60_sells INTEGER,pre120_buys INTEGER,pre120_sells INTEGER,
      pre10_gross_sol REAL,pre30_gross_sol REAL,pre60_gross_sol REAL,pre120_gross_sol REAL,
      pre10_unique_wallets INTEGER,pre30_unique_wallets INTEGER,pre60_unique_wallets INTEGER,pre120_unique_wallets INTEGER,
      pre10_return_pct REAL,pre30_return_pct REAL,pre60_return_pct REAL,pre120_return_pct REAL,
      migration_price REAL,
      ret5 REAL,ret10 REAL,ret30 REAL,ret60 REAL,ret120 REAL,ret300 REAL,
      gap5 REAL,gap10 REAL,gap30 REAL,gap60 REAL,gap120 REAL,gap300 REAL,
      built_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v780_mint ON migration_paths(mint);
    CREATE INDEX IF NOT EXISTS idx_v780_time ON migration_paths(migration_ts);
    ''');d.commit();d.close()


def cols(d):
    return {str(r['name']) for r in d.execute('PRAGMA table_info(v52_swaps)')}


def pick(c,*names):
    return next((x for x in names if x in c),None)


def num(x):
    try:
        y=float(x)
        return y if math.isfinite(y) else None
    except Exception:return None


def side_of(r,side_col):
    if not side_col:return None
    s=str(r[side_col] or '').upper()
    if 'BUY' in s:return 'BUY'
    if 'SELL' in s:return 'SELL'
    return None


def price_of(r,pcol):return num(r[pcol]) if pcol else None


def sol_of(r,scol):
    x=num(r[scol]) if scol else None
    return abs(x) if x is not None else 0.0


def ret(a,b):
    if a is None or b is None or a==0:return None
    return (b/a-1.0)*100.0


def nearest_after(rows,target,pcol):
    best=None
    for r in rows:
        ts=float(r['timestamp'])
        if ts<target:continue
        p=price_of(r,pcol)
        if p is None or p<=0:continue
        g=ts-target
        if best is None or g<best[0]:best=(g,p,ts)
    return best


def window_metrics(rows,t0,sec,side_col,sol_col,wallet_col,pcol):
    z=[r for r in rows if t0-sec < float(r['timestamp']) <= t0]
    buys=sum(1 for r in z if side_of(r,side_col)=='BUY');sells=sum(1 for r in z if side_of(r,side_col)=='SELL')
    gross=sum(sol_of(r,sol_col) for r in z)
    uw=len({str(r[wallet_col]) for r in z if wallet_col and r[wallet_col] is not None})
    pp=[(float(r['timestamp']),price_of(r,pcol)) for r in z]
    pp=[x for x in pp if x[1] is not None and x[1]>0]
    rp=ret(pp[0][1],pp[-1][1]) if len(pp)>=2 else None
    return len(z),buys,sells,gross,uw,rp


def pct(xs,q):
    z=sorted(float(x) for x in xs if x is not None)
    if not z:return None
    p=(len(z)-1)*q;lo=int(p);hi=min(lo+1,len(z)-1);f=p-lo
    return z[lo]+(z[hi]-z[lo])*f


def main():
    init_out()
    print('='*140)
    print('MEMECOIN LAB — CANONICAL MIGRATION PRE/POST RECONSTRUCTION V7.8.0')
    print('='*140)
    print('Research only | migration truth = V7.7.9 canonical table ONLY | no alpha threshold search')
    if not MIG.exists() or not FEATURE.exists():raise SystemExit('required DB missing')
    md=ro(MIG);migs=md.execute('SELECT signature,mint,block_time FROM canonical_migrations WHERE block_time IS NOT NULL ORDER BY block_time').fetchall();md.close()
    fd=ro(FEATURE);c=cols(fd)
    pcol=pick(c,'price_sol','price','token_price_sol')
    side_col=pick(c,'side','trade_side','direction')
    sol_col=pick(c,'sol_amount','amount_sol','trade_sol','gross_sol')
    wallet_col=pick(c,'wallet','trader','user','owner')
    print(f'canonical_migrations={len(migs)} price_col={pcol} side_col={side_col} sol_col={sol_col} wallet_col={wallet_col}')
    if not pcol:raise SystemExit('No usable price column in v52_swaps')

    out=[];cover=Counter();postvals={h:[] for h in POST_H};postgaps={h:[] for h in POST_H}
    for m in migs:
        mint=str(m['mint']);mt=float(m['block_time'])
        rows=fd.execute('SELECT * FROM v52_swaps WHERE token_mint=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp',(mint,mt-120,mt+330)).fetchall()
        pre=[r for r in rows if float(r['timestamp'])<=mt]
        if not pre:continue
        first=fd.execute('SELECT MIN(timestamp) FROM v52_swaps WHERE token_mint=?',(mint,)).fetchone()[0]
        last_pre=pre[-1];mp=price_of(last_pre,pcol)
        metrics={w:window_metrics(pre,mt,w,side_col,sol_col,wallet_col,pcol) for w in PRE_WINDOWS}
        rets={};gaps={}
        for h in POST_H:
            z=nearest_after(rows,mt+h,pcol)
            if z and z[0]<=MAX_POST_GAP and mp and mp>0:
                gaps[h]=z[0];rets[h]=ret(mp,z[1]);cover[h]+=1;postvals[h].append(rets[h]);postgaps[h].append(z[0])
            else:gaps[h]=None;rets[h]=None
        out.append((m['signature'],mint,mt,float(first) if first is not None else None,float(last_pre['timestamp']),mt-float(first) if first is not None else None,
                    metrics[10][0],metrics[30][0],metrics[60][0],metrics[120][0],
                    metrics[10][1],metrics[10][2],metrics[30][1],metrics[30][2],metrics[60][1],metrics[60][2],metrics[120][1],metrics[120][2],
                    metrics[10][3],metrics[30][3],metrics[60][3],metrics[120][3],
                    metrics[10][4],metrics[30][4],metrics[60][4],metrics[120][4],
                    metrics[10][5],metrics[30][5],metrics[60][5],metrics[120][5],mp,
                    rets[5],rets[10],rets[30],rets[60],rets[120],rets[300],
                    gaps[5],gaps[10],gaps[30],gaps[60],gaps[120],gaps[300],time.time()))
    fd.close()
    d=odb();d.execute('DELETE FROM migration_paths');d.executemany('INSERT INTO migration_paths VALUES('+','.join('?' for _ in range(44))+')',out);d.commit();d.close()

    print('\nPRE-MIGRATION COVERAGE')
    print(f'rows_with_pre={len(out)}/{len(migs)}')
    for w in PRE_WINDOWS:
        xs=[r[{10:6,30:7,60:8,120:9}[w]] for r in out]
        print(f'pre{w:>3}s swaps>0={sum(1 for x in xs if x>0)}/{len(out)} median_swaps={statistics.median(xs) if xs else None}')

    print('\nPOST-MIGRATION PATH — tight observation <=15s after target')
    for h in POST_H:
        xs=postvals[h];gs=postgaps[h]
        if not xs:
            print(f'+{h:>3}s n=0');continue
        win=sum(1 for x in xs if x>=25)/len(xs)*100;cr=sum(1 for x in xs if x<=-50)/len(xs)*100
        print(f'+{h:>3}s n={len(xs):3d} med={pct(xs,.5):+8.2f}% p10/p90={pct(xs,.1):+8.2f}/{pct(xs,.9):+8.2f}% win>=25={win:5.1f}% crash<=-50={cr:5.1f}% gap_p50={pct(gs,.5):.2f}s')

    print('\nQUALITY GATE')
    n30=cover[30];n120=cover[120]
    if len(out)>=70 and n30>=40 and n120>=25:
        print('STATUS=CANONICAL_MIGRATION_PATH_COHORT_STRONG')
        print('Next: V7.8.1 discovery of PRE-state separators for post-migration WIN / CRASH, with temporal robustness and no threshold freezing yet.')
    elif len(out)>=50 and n30>=25:
        print('STATUS=CANONICAL_MIGRATION_PATH_COHORT_USABLE')
        print('Next: low-dimensional PRE-state discovery only; keep collecting for robustness.')
    else:
        print('STATUS=CANONICAL_MIGRATION_PATH_COHORT_THIN')
        print('Keep collecting canonical migrations before high-dimensional discovery.')
    print(f'output={OUT.name}')
    print('\nGuardrail: post outcomes are descriptive labels only. This cohort is discovery evidence, not future-only confirmation.')

if __name__=='__main__':main()
