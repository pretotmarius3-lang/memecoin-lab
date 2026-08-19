#!/usr/bin/env python3
"""MEMECOIN LAB — CANONICAL PUMP MIGRATION TABLE V7.7.9

Build a canonical migration table from frozen evidence rules:
- exact Pump-emitted `Instruction: Migrate` runtime log,
- unique structurally referenced mint with canonical pre-migration history,
- derived bonding-curve PDA present in Pump structural context.

This script writes ONLY v779_canonical_migrations.db. Source DBs are read-only.
No alpha discovery is performed here.
"""
from __future__ import annotations

import sqlite3,time
from pathlib import Path

import v774_deterministic_migration_mint_decoder as base
import v7752_migrate_log_emitter_audit as emit
import v777_cpi_migration_account_resolver as cpi
import v778_bonding_curve_pda_validator as pda

ROOT=Path.home()/"memecoin_lab"
OUT=ROOT/'v779_canonical_migrations.db'
PUMP=base.PUMP


def odb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=NORMAL');d.execute('PRAGMA busy_timeout=30000')
    return d


def init_out():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS canonical_migrations(
      signature TEXT PRIMARY KEY,
      mint TEXT NOT NULL,
      slot INTEGER,
      block_time REAL,
      evidence TEXT NOT NULL,
      pump_emitter INTEGER NOT NULL,
      unique_prehistory INTEGER NOT NULL,
      bonding_curve TEXT NOT NULL,
      bonding_curve_bump INTEGER,
      pda_in_structural_context INTEGER NOT NULL,
      canonicalized_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v779_mint ON canonical_migrations(mint);
    CREATE INDEX IF NOT EXISTS idx_v779_time ON canonical_migrations(block_time);
    ''');d.commit();d.close()


def rebuild_candidates(sigs,txs):
    ctx={};bt={};meta={}
    for s in sigs:
        tx=txs.get(s)
        if not tx:continue
        ev=[x for x in emit.emitter_events(tx) if x['emitter']==PUMP]
        if not ev:continue
        acc,groups=cpi.structural_accounts(tx)
        ctx[s]=acc;bt[s]=float(tx.get('blockTime') or 0)
        meta[s]=(tx,groups,ev)
    hist=cpi.prehistory_for(ctx,bt)
    cand={}
    for s,z in hist.items():
        seen={}
        for item in z:seen.setdefault(item[0],item)
        if len(seen)==1:cand[s]=next(iter(seen.values()))
    return cand,ctx,meta


def main():
    init_out()
    print('='*136)
    print('MEMECOIN LAB — CANONICAL PUMP MIGRATION TABLE V7.7.9')
    print('='*136)
    print('Frozen evidence: Pump-emitted migrate log + unique prehistory mint + bonding-curve PDA in structural context')
    sigmeta=base.exact_signatures();sigs=sorted(sigmeta);txs=base.local_transactions(sigs)
    cand,ctx,meta=rebuild_candidates(sigs,txs)

    rows=[];rejected=0
    for s,item in cand.items():
        mint=item[0];curve,bump=pda.derive_curve(mint);tx,groups,ev=meta[s]
        struct=set(ctx.get(s,[]))
        if not curve or curve not in struct:
            rejected+=1;continue
        rows.append((
            s,mint,tx.get('slot') or sigmeta[s].get('slot'),tx.get('blockTime'),
            'PUMP_MIGRATE_LOG+UNIQUE_PREHISTORY_MINT+BONDING_CURVE_PDA_STRUCTURAL',
            1,1,curve,bump,1,time.time()
        ))

    d=odb();d.execute('DELETE FROM canonical_migrations');d.executemany('INSERT INTO canonical_migrations VALUES(?,?,?,?,?,?,?,?,?,?,?)',rows);d.commit()
    uniq=d.execute('SELECT COUNT(DISTINCT mint) FROM canonical_migrations').fetchone()[0]
    dup=d.execute('SELECT COUNT(*) FROM (SELECT mint,COUNT(*) c FROM canonical_migrations GROUP BY mint HAVING c>1)').fetchone()[0]
    d.close()

    print(f'input_migrate_signatures={len(sigs)} unique_structural_candidates={len(cand)}')
    print(f'canonical_rows={len(rows)} unique_mints={uniq} rejected_by_pda={rejected} duplicate_mints={dup}')
    print(f'output={OUT.name}')
    print('\nQUALITY GATE')
    if len(rows)>=80 and rejected==0:
        print('STATUS=CANONICAL_MIGRATION_TABLE_STRONG')
        print('Next: reconstruct PRE/MIGRATION/POST using this table only; unresolved signatures remain excluded.')
    elif len(rows)>=50:
        print('STATUS=CANONICAL_MIGRATION_TABLE_USABLE')
        print('Next: reconstruct PRE/MIGRATION/POST on canonical rows only.')
    else:
        print('STATUS=CANONICAL_MIGRATION_TABLE_SMALL')
        print('Keep collecting exact migrations before high-dimensional discovery.')
    print('\nGuardrail: unresolved/ambiguous migrations are intentionally excluded rather than heuristically filled.')

if __name__=='__main__':main()
