#!/usr/bin/env python3
"""MEMECOIN LAB — CANONICAL PUMP MIGRATION LOG AUDIT V7.7.3

READ-ONLY infrastructure audit.

Goal: stop using price/market-cap crossings as migration truth and look for an
explicit Pump `migrate` instruction in captured transaction logs.

Authoritative protocol facts used by this audit:
- Pump program: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
- Anchor instruction name: migrate
- Anchor discriminator for `global:migrate` = 9beae792ec9ea21e
- A completed bonding curve has complete=true and real_token_reserves=0.

This version does NOT infer curve completion from price. It classifies only
explicit log evidence as EXACT_LOG and reports whether the local raw capture has
enough such events to build the PRE/MIGRATION/POST cohort.

No strategy logic. No writes to source DBs. No capital decision.
"""
from __future__ import annotations

import json
import sqlite3
import statistics
from collections import Counter, defaultdict
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW=ROOT/"v5_raw_events.db"
FEATURE=ROOT/"v52_features.db"
PUMP_PROGRAM="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
MIGRATE_DISC_HEX="9beae792ec9ea21e"
EXACT_NEEDLES=(
    "Instruction: Migrate",
    "Program log: Instruction: Migrate",
)


def ro(path):
    d=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute("PRAGMA query_only=ON")
    d.execute("PRAGMA busy_timeout=30000")
    return d


def tables(d):
    return [r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")]


def cols(d,t):
    return [r[1] for r in d.execute(f"PRAGMA table_info('{t}')")]


def has_exact_log(txt):
    if txt is None:return False
    s=str(txt)
    return any(n in s for n in EXACT_NEEDLES)


def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def main():
    print('='*136)
    print('MEMECOIN LAB — CANONICAL PUMP MIGRATION LOG AUDIT V7.7.3')
    print('='*136)
    print('READ-ONLY | exact Pump migrate instruction logs only | no price-crossing inference')
    print(f'Pump program={PUMP_PROGRAM} migrate_anchor_discriminator={MIGRATE_DISC_HEX}')

    if not RAW.exists():
        raise SystemExit(f'Missing {RAW}')

    d=ro(RAW)
    candidates=[]
    for t in tables(d):
        cc=set(cols(d,t))
        if 'logs_json' in cc:
            candidates.append((t,cc))
    print('\nLOG-BEARING RAW TABLES')
    for t,cc in candidates:
        print(f'{t:<42} cols={",".join(sorted(cc))}')

    hits=[]
    for t,cc in candidates:
        sel=['logs_json']
        for c in ('signature','mint','kind','source','slot','first_seen','updated_at','event_hint','status'):
            if c in cc:sel.append(c)
        q=f"SELECT {','.join(sel)} FROM {t} WHERE logs_json LIKE '%Instruction: Migrate%'"
        try: rr=d.execute(q).fetchall()
        except sqlite3.Error as e:
            print(f'{t}: query_error={e!r}'); continue
        for r in rr:
            z={k:r[k] for k in r.keys()}
            if not has_exact_log(z.get('logs_json')):continue
            z['_table']=t
            hits.append(z)
    d.close()

    # Deduplicate by signature where possible.
    dedup={}
    anon=[]
    for z in hits:
        sig=str(z.get('signature') or '')
        if sig: dedup.setdefault(sig,z)
        else: anon.append(z)
    exact=list(dedup.values())+anon

    print('\nEXACT MIGRATE LOG CENSUS')
    print(f'raw_rows={len(hits)} unique_exact_events={len(exact)} signatures={len(dedup)} anonymous={len(anon)}')
    by_table=Counter(z['_table'] for z in exact)
    by_source=Counter(str(z.get('source')) for z in exact if z.get('source') is not None)
    by_kind=Counter(str(z.get('kind')) for z in exact if z.get('kind') is not None)
    print('by_table=' + repr(dict(by_table)))
    print('by_source=' + repr(dict(by_source)))
    print('by_kind=' + repr(dict(by_kind)))

    mints=[str(z.get('mint')) for z in exact if z.get('mint') not in (None,'','None')]
    print(f'mint_attribution rows={len(mints)} unique_mints={len(set(mints))}')

    # Cross-link exact migration mint evidence with canonical swaps.
    linked=[]
    if FEATURE.exists() and mints:
        f=ro(FEATURE)
        uniq=sorted(set(mints)); chunk=700
        first_swap={}
        last_swap={}
        nswaps={}
        for i in range(0,len(uniq),chunk):
            sub=uniq[i:i+chunk]; qs=','.join('?' for _ in sub)
            for r in f.execute(f'''SELECT token_mint,MIN(timestamp) first_ts,MAX(timestamp) last_ts,COUNT(*) n
                                    FROM v52_swaps WHERE token_mint IN ({qs}) GROUP BY token_mint''',sub):
                m=str(r['token_mint']); first_swap[m]=float(r['first_ts']); last_swap[m]=float(r['last_ts']); nswaps[m]=int(r['n'])
        f.close()
        linked=[m for m in uniq if m in first_swap]
        print('\nCANONICAL SWAP LINKAGE')
        print(f'exact_migration_mints={len(uniq)} linked_to_v52_swaps={len(linked)} coverage={100*len(linked)/max(1,len(uniq)):.1f}%')
        print(f'linked swap-count p50/p90={pct([nswaps[m] for m in linked],.5)}/{pct([nswaps[m] for m in linked],.9)}')

    # Print a few auditable examples without dumping full logs.
    print('\nEXAMPLES')
    for z in exact[:10]:
        print(f"table={z['_table']} sig={z.get('signature')} mint={z.get('mint')} source={z.get('source')} kind={z.get('kind')} slot={z.get('slot')}")

    print('\nQUALITY GATE')
    um=len(set(mints))
    if um>=50 and (not mints or len(linked)>=max(20,int(.5*um))):
        print('STATUS=EXACT_MIGRATION_LOG_COHORT_USABLE')
        print('Next: reconstruct PRE/MIGRATION/POST around these exact migrate transactions; separately decode curve-complete state if raw account data supports it.')
    elif um>=10:
        print('STATUS=EXACT_MIGRATION_LOG_COHORT_SMALL_BUT_REAL')
        print('Accumulate more exact migrate logs before high-dimensional alpha discovery.')
    elif len(exact)>0 and um==0:
        print('STATUS=EXACT_MIGRATE_LOGS_FOUND_BUT_MINT_ATTRIBUTION_MISSING')
        print('Next: parse the migrate transaction accounts / raw RPC payload to recover the mint deterministically.')
    else:
        print('STATUS=EXACT_MIGRATE_LOGS_NOT_CAPTURED')
        print('Next: add an explicit Pump migrate instruction decoder to acquisition rather than inferring migration from price.')

    print('\nGuardrail: `Instruction: Migrate` is treated as explicit transaction-log evidence. Curve completion itself is not inferred here.')

if __name__=='__main__':main()
