#!/usr/bin/env python3
"""MEMECOIN LAB — CPI MIGRATION ACCOUNT RESOLVER V7.7.7

READ-ONLY infrastructure/discovery diagnostic.
Goal: recover deterministic candidate mints for Pump migrations executed through CPI.

Evidence used:
- exact `Instruction: Migrate` runtime log emitted by Pump,
- parent outer instruction + Pump inner-instruction account references,
- canonical pre-migration v52_swaps history for referenced account keys,
- V7.7.4 exact labels only as a validation control.

No candidate is promoted to canonical. No alpha discovery. No source DB writes.
"""
from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

import v774_deterministic_migration_mint_decoder as base
import v7752_migrate_log_emitter_audit as emit

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
PUMP=base.PUMP


def key_str(k):
    if isinstance(k,str): return k
    if isinstance(k,dict): return str(k.get('pubkey') or '')
    return ''


def all_keys(tx):
    msg=((tx or {}).get('transaction') or {}).get('message') or {}
    ks=[key_str(x) for x in (msg.get('accountKeys') or [])]
    loaded=((tx or {}).get('meta') or {}).get('loadedAddresses') or {}
    ks += [str(x) for x in (loaded.get('writable') or [])]
    ks += [str(x) for x in (loaded.get('readonly') or [])]
    return ks


def resolve_ref(a,ks):
    if isinstance(a,str):
        if a.isdigit():
            i=int(a); return ks[i] if 0<=i<len(ks) else None
        return a
    if isinstance(a,int): return ks[a] if 0<=a<len(ks) else None
    if isinstance(a,dict): return str(a.get('pubkey') or '') or None
    return None


def ix_program(ix,ks):
    if not isinstance(ix,dict): return None
    if ix.get('programId'): return str(ix['programId'])
    j=ix.get('programIdIndex')
    try:return ks[int(j)]
    except Exception:return None


def ix_accounts(ix,ks):
    out=[]
    for a in (ix.get('accounts') or []):
        x=resolve_ref(a,ks)
        if x: out.append(x)
    return out


def known_v774():
    try:
        d=base.ro(base.OUT)
        rr=d.execute("SELECT signature,mint FROM exact_migrations WHERE decode_status='EXACT' AND mint IS NOT NULL").fetchall()
        d.close();return {str(r['signature']):str(r['mint']) for r in rr}
    except Exception:return {}


def prehistory_for(keys_by_sig, block_by_sig):
    allk=sorted({k for vals in keys_by_sig.values() for k in vals})
    out={}
    if not allk:return out
    d=base.ro(FEATURE)
    # Pull only account keys that are actual token_mint values in canonical swaps.
    present={}
    for i in range(0,len(allk),700):
        sub=allk[i:i+700];qs=','.join('?' for _ in sub)
        for r in d.execute(f'SELECT token_mint,MIN(timestamp) mn,MAX(timestamp) mx,COUNT(*) n FROM v52_swaps WHERE token_mint IN ({qs}) GROUP BY token_mint',sub):
            present[str(r['token_mint'])]=(float(r['mn']),float(r['mx']),int(r['n']))
    d.close()
    for s,vals in keys_by_sig.items():
        bt=block_by_sig.get(s) or 0
        z=[]
        for k in vals:
            h=present.get(k)
            if h and (not bt or h[0] <= bt): z.append((k,h[2],h[0],h[1]))
        out[s]=z
    return out


def structural_accounts(tx):
    """Return accounts structurally adjacent to Pump CPI migration activity.

    For each innerInstructions group containing a Pump instruction, include:
    - the parent top-level instruction accounts,
    - accounts of all Pump instructions in that same inner group.
    """
    ks=all_keys(tx);msg=((tx or {}).get('transaction') or {}).get('message') or {}
    tops=msg.get('instructions') or []
    vals=[];groups=[]
    for grp in ((tx or {}).get('meta') or {}).get('innerInstructions') or []:
        inner=[ix for ix in (grp.get('instructions') or []) if isinstance(ix,dict)]
        pump=[ix for ix in inner if ix_program(ix,ks)==PUMP]
        if not pump:continue
        outer=grp.get('index')
        acc=[]
        try:
            p=tops[int(outer)]
            if isinstance(p,dict):acc += ix_accounts(p,ks)
        except Exception:pass
        for ix in pump:acc += ix_accounts(ix,ks)
        acc=list(dict.fromkeys(acc))
        vals += acc;groups.append((outer,acc,len(pump)))
    return list(dict.fromkeys(vals)),groups


def main():
    print('='*136)
    print('MEMECOIN LAB — CPI MIGRATION ACCOUNT RESOLVER V7.7.7')
    print('='*136)
    print('READ-ONLY | Pump-emitted CPI migrate context + canonical prehistory | NO mint promotion / NO alpha discovery')

    sigmeta=base.exact_signatures();sigs=sorted(sigmeta);txs=base.local_transactions(sigs);known=known_v774()
    print(f'migrate_signatures={len(sigs)} local_full_transactions={len(txs)}/{len(sigs)} v774_exact_labels={len(known)}')

    ctx={};bt={};is_cpi={};group_counts={}
    for s in sigs:
        tx=txs.get(s)
        if not tx:continue
        ev=[x for x in emit.emitter_events(tx) if x['emitter']==PUMP]
        if not ev:continue
        cpi=any(x['depth']>=2 for x in ev);is_cpi[s]=cpi
        acc,groups=structural_accounts(tx);ctx[s]=acc;group_counts[s]=groups
        bt[s]=float(tx.get('blockTime') or 0)

    hist=prehistory_for(ctx,bt)
    statuses=Counter();cand={};ambiguous={}
    for s in ctx:
        z=hist.get(s,[])
        # Deterministic candidate only when exactly one structurally referenced account
        # is also a canonical token mint with pre-migration history.
        uniq=[]
        seen=set()
        for item in z:
            if item[0] not in seen:uniq.append(item);seen.add(item[0])
        if len(uniq)==1:
            cand[s]=uniq[0];statuses['UNIQUE_PREHISTORY_MINT']+=1
        elif len(uniq)>1:
            ambiguous[s]=uniq;statuses['MULTIPLE_PREHISTORY_MINTS']+=1
        else:statuses['NO_PREHISTORY_MINT']+=1

    print('\nRESOLUTION CENSUS')
    cpi_n=sum(1 for s in ctx if is_cpi.get(s))
    top_n=len(ctx)-cpi_n
    print(f'pump_context_signatures={len(ctx)} CPI={cpi_n} TOP_ONLY={top_n}')
    print('status='+repr(dict(statuses)))
    print(f'unique_candidates={len(cand)} ambiguous={len(ambiguous)}')

    agree=disagree=known_covered=new_cpi=0
    for s,(m,*_) in cand.items():
        if s in known:
            known_covered+=1
            if known[s]==m:agree+=1
            else:disagree+=1
        elif is_cpi.get(s):new_cpi+=1
    print('\nVALIDATION AGAINST V7.7.4')
    print(f'known_covered={known_covered}/{len(known)} agreement={agree} disagreement={disagree}')
    print(f'new_unique_CPI_candidates={new_cpi}')

    print('\nCPI UNIQUE CANDIDATE EXAMPLES')
    shown=0
    for s,item in cand.items():
        if not is_cpi.get(s):continue
        m,n,mn,mx=item
        print(f'sig={s} candidate_mint={m} pre_swaps={n} prior={known.get(s)} inner_groups={[(g[0],g[2],len(g[1])) for g in group_counts.get(s,[])]}')
        shown+=1
        if shown>=12:break

    print('\nAMBIGUOUS CPI EXAMPLES')
    shown=0
    for s,z in ambiguous.items():
        if not is_cpi.get(s):continue
        print(f'sig={s} candidates={[x[0] for x in z]} counts={[x[1] for x in z]}')
        shown+=1
        if shown>=8:break

    print('\nQUALITY GATE')
    if disagree==0 and new_cpi>=70:
        print('STATUS=CPI_ACCOUNT_RESOLUTION_STRONG')
        print('Next: independent invariant validation, then write a separate canonical CPI mint decoder.')
    elif disagree==0 and new_cpi>=30:
        print('STATUS=CPI_ACCOUNT_RESOLUTION_PROMISING')
        print('Next: validate candidate mint via bonding-curve PDA / account-role invariants before canonical promotion.')
    elif disagree==0 and len(cand)>0:
        print('STATUS=CPI_ACCOUNT_RESOLUTION_PARTIAL')
        print('Next: enrich with bonding-curve PDA/account-role invariants; do not promote candidates yet.')
    else:
        print('STATUS=CPI_ACCOUNT_RESOLUTION_UNSAFE')
        print('Observed conflicts or insufficient uniqueness; do not use candidates as migration truth.')

    print('\nGuardrail: UNIQUE_PREHISTORY_MINT is a structural candidate, not canonical evidence. No alpha study should use newly recovered rows yet.')

if __name__=='__main__':main()
