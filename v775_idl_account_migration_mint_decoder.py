#!/usr/bin/env python3
"""MEMECOIN LAB — IDL ACCOUNT MIGRATION MINT DECODER V7.7.5

Recovers Pump migration mints from the actual `migrate` instruction account list.
Official Pump IDL defines:
  discriminator = [155,234,231,146,236,158,162,30]
  account[0] = global
  account[1] = withdraw_authority
  account[2] = mint

This pass is intended to resolve V7.7.4 NO_MINT rows where token balances do not
expose the migrated mint. It does not use price or market-cap inference.
Source DBs are read-only. Output is a separate SQLite audit DB.
"""
from __future__ import annotations

import json, sqlite3, time
from collections import Counter
from pathlib import Path

import v774_deterministic_migration_mint_decoder as base

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/"v52_features.db"
OUT=ROOT/"v775_idl_exact_migrations.db"
PUMP=base.PUMP
DISC=bytes([155,234,231,146,236,158,162,30])
ALPH="123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58decode(s):
    if not isinstance(s,str) or not s:return b''
    n=0
    try:
        for ch in s:n=n*58+ALPH.index(ch)
    except ValueError:return b''
    raw=n.to_bytes((n.bit_length()+7)//8,'big') if n else b''
    pad=0
    for ch in s:
        if ch=='1':pad+=1
        else:break
    return b'\x00'*pad+raw


def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row; d.execute('PRAGMA query_only=ON'); d.execute('PRAGMA busy_timeout=30000')
    return d


def odb():
    d=sqlite3.connect(OUT,timeout=30); d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL'); d.execute('PRAGMA synchronous=NORMAL'); d.execute('PRAGMA busy_timeout=30000')
    return d


def init_out():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS idl_exact_migrations(
      signature TEXT PRIMARY KEY,
      slot INTEGER,
      block_time REAL,
      mint TEXT,
      decode_status TEXT NOT NULL,
      evidence TEXT NOT NULL,
      instruction_level TEXT,
      instruction_index INTEGER,
      prior_v774_status TEXT,
      prior_v774_mint TEXT,
      canonical_pre_swaps INTEGER,
      decoded_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v775_mint ON idl_exact_migrations(mint);
    CREATE INDEX IF NOT EXISTS idx_v775_status ON idl_exact_migrations(decode_status);
    ''');d.commit();d.close()


def key_str(k):
    if isinstance(k,str):return k
    if isinstance(k,dict):return str(k.get('pubkey') or '')
    return ''


def all_keys(tx):
    try: msg=tx['transaction']['message']
    except Exception:return []
    keys=[key_str(x) for x in (msg.get('accountKeys') or [])]
    loaded=(tx.get('meta') or {}).get('loadedAddresses') or {}
    keys += [str(x) for x in (loaded.get('writable') or [])]
    keys += [str(x) for x in (loaded.get('readonly') or [])]
    return keys


def account_ref_to_key(ref,keys):
    if isinstance(ref,int):return keys[ref] if 0<=ref<len(keys) else None
    if isinstance(ref,str):
        if ref.isdigit():
            i=int(ref);return keys[i] if 0<=i<len(keys) else None
        return ref
    if isinstance(ref,dict):return str(ref.get('pubkey') or '') or None
    return None


def program_for(ix,keys):
    if ix.get('programId'):return str(ix['programId'])
    p=ix.get('programIdIndex')
    if isinstance(p,int) and 0<=p<len(keys):return keys[p]
    return None


def is_migrate_ix(ix,keys):
    if program_for(ix,keys)!=PUMP:return False
    data=ix.get('data')
    if not isinstance(data,str):return False
    raw=b58decode(data)
    return len(raw)>=8 and raw[:8]==DISC


def iter_instructions(tx):
    keys=all_keys(tx)
    msg=((tx.get('transaction') or {}).get('message') or {})
    for i,ix in enumerate(msg.get('instructions') or []):
        if isinstance(ix,dict):yield 'TOP',i,ix,keys
    for group in (tx.get('meta') or {}).get('innerInstructions') or []:
        parent=group.get('index')
        for j,ix in enumerate(group.get('instructions') or []):
            if isinstance(ix,dict):yield f'INNER@{parent}',j,ix,keys


def decode_mint(tx):
    found=[]
    for level,i,ix,keys in iter_instructions(tx):
        if not is_migrate_ix(ix,keys):continue
        acc=ix.get('accounts') or []
        if len(acc)<3:
            found.append((None,level,i,'migrate discriminator matched but fewer than 3 accounts'))
            continue
        mint=account_ref_to_key(acc[2],keys)
        found.append((mint,level,i,'Pump IDL migrate account[2]=mint'))
    good=[x for x in found if x[0]]
    uniq=sorted(set(x[0] for x in good))
    if len(uniq)==1:
        x=next(x for x in good if x[0]==uniq[0]);return x[0],'EXACT_IDL',x[1],x[2],x[3],found
    if len(uniq)>1:return None,'AMBIGUOUS_IDL',None,None,'multiple distinct migrate account[2] mints',found
    if found:return None,'MIGRATE_IX_NO_MINT',None,None,'migrate instruction found but mint unresolved',found
    return None,'MIGRATE_IX_NOT_FOUND',None,None,'exact migrate instruction not found in decoded instruction arrays',found


def v774_rows():
    p=ROOT/'v774_exact_migrations.db'
    if not p.exists():return {}
    d=ro(p);rr=d.execute('SELECT signature,mint,decode_status FROM exact_migrations').fetchall();d.close()
    return {str(r['signature']):(r['mint'],str(r['decode_status'])) for r in rr}


def pre_swap_counts(mints,block_by_mint):
    out={}
    if not FEATURE.exists() or not mints:return out
    d=ro(FEATURE)
    for m in sorted(set(mints)):
        bt=block_by_mint.get(m)
        if bt:
            n=d.execute('SELECT COUNT(*) FROM v52_swaps WHERE token_mint=? AND timestamp<=?',(m,bt)).fetchone()[0]
        else:n=d.execute('SELECT COUNT(*) FROM v52_swaps WHERE token_mint=?',(m,)).fetchone()[0]
        out[m]=int(n)
    d.close();return out


def main():
    init_out();base.load_dotenv()
    print('='*136)
    print('MEMECOIN LAB — IDL ACCOUNT MIGRATION MINT DECODER V7.7.5')
    print('='*136)
    print('Official Pump migrate discriminator + account[2]=mint | NO market-cap inference')
    print('discriminator='+DISC.hex()+' mint_account_position=2')

    sigmeta=base.exact_signatures();sigs=sorted(sigmeta)
    txs=base.local_transactions(sigs)
    print(f'exact_migrate_signatures={len(sigs)} local_full_transactions={len(txs)}/{len(sigs)}')
    prior=v774_rows()

    decoded={};statuses=Counter();mints=[];block_by_mint={}
    for s in sigs:
        tx=txs.get(s)
        if not tx:
            decoded[s]=(None,'NO_TX',None,None,'full transaction unavailable',[])
            statuses['NO_TX']+=1;continue
        z=decode_mint(tx);decoded[s]=z;statuses[z[1]]+=1
        if z[0]:
            mints.append(z[0]);
            if tx.get('blockTime'):block_by_mint[z[0]]=float(tx['blockTime'])
    pre=pre_swap_counts(mints,block_by_mint)

    rows=[];agree=disagree=recovered=0
    for s in sigs:
        mint,status,level,idx,evidence,_=decoded[s]
        pm,ps=prior.get(s,(None,None))
        if status=='EXACT_IDL' and ps=='EXACT':
            if pm==mint:agree+=1
            else:disagree+=1
        if status=='EXACT_IDL' and ps!='EXACT':recovered+=1
        tx=txs.get(s) or {}
        rows.append((s,tx.get('slot') or sigmeta[s].get('slot'),tx.get('blockTime'),mint,status,evidence,level,idx,ps,pm,pre.get(mint,0) if mint else 0,time.time()))

    d=odb();d.execute('DELETE FROM idl_exact_migrations');d.executemany('INSERT INTO idl_exact_migrations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',rows);d.commit();d.close()

    exact=statuses['EXACT_IDL'];uniq=len(set(mints))
    print('\nDECODE CENSUS')
    print('status='+repr(dict(statuses)))
    print(f'exact_idl_rows={exact}/{len(sigs)} unique_exact_mints={uniq}')
    print(f'v774_exact_agreement={agree} disagreement={disagree} newly_recovered_from_nonexact={recovered}')
    print(f'output={OUT.name}')

    print('\nEXACT IDL EXAMPLES')
    shown=0
    for r in rows:
        if r[4]=='EXACT_IDL':
            print(f'sig={r[0]} mint={r[3]} slot={r[1]} level={r[6]} ix={r[7]} prior={r[8]} pre_swaps={r[10]}')
            shown+=1
            if shown>=8:break

    print('\nQUALITY GATE')
    rate=exact/max(1,len(sigs))
    if exact>=100 and rate>=.80 and disagree==0:
        print('STATUS=CANONICAL_IDL_MIGRATION_COHORT_STRONG')
        print('Next: build PRE/MIGRATION/POST cohort around exact migrate blockTime using IDL-attributed mints.')
    elif exact>=50 and disagree<=2:
        print('STATUS=CANONICAL_IDL_MIGRATION_COHORT_USABLE')
        print('Next: V7.7.6 PRE/MIGRATION/POST reconstruction on exact IDL rows; audit any disagreements separately.')
    else:
        print('STATUS=IDL_MINT_ATTRIBUTION_STILL_PARTIAL')
        print('Inspect instruction representation / versioned account loading before alpha discovery.')

    print('\nGuardrail: EXACT_IDL requires matching the Pump migrate discriminator and reading the documented mint account position.')

if __name__=='__main__':main()
