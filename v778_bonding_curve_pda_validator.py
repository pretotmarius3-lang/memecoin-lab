#!/usr/bin/env python3
"""MEMECOIN LAB — BONDING-CURVE PDA INVARIANT VALIDATOR V7.7.8

READ-ONLY independent validation of V7.7.7 CPI mint candidates.

For each UNIQUE_PREHISTORY_MINT candidate, derive the Pump bonding-curve PDA from
official seeds [b"bonding-curve", mint] and verify that the derived PDA is actually
present in the migration transaction / Pump CPI structural context.

This does NOT promote new candidates to canonical and does NOT run alpha discovery.
No source DB writes.
"""
from __future__ import annotations

import hashlib
from collections import Counter

import base58
import v774_deterministic_migration_mint_decoder as base
import v7752_migrate_log_emitter_audit as emit
import v777_cpi_migration_account_resolver as cpi

PUMP=base.PUMP
PDA_MARKER=b"ProgramDerivedAddress"
SEED=b"bonding-curve"

# ed25519 field/constants for pure-Python on-curve test (no solders/nacl dependency)
P=2**255-19
D=(-121665 * pow(121666, P-2, P)) % P
I=pow(2, (P-1)//4, P)


def is_on_ed25519(pubkey32: bytes) -> bool:
    """Approximate Solana Pubkey::is_on_curve via Edwards compressed-point decode."""
    if len(pubkey32)!=32:return False
    yb=bytearray(pubkey32)
    sign=(yb[31]>>7)&1
    yb[31]&=0x7f
    y=int.from_bytes(yb,'little')
    if y>=P:return False
    y2=(y*y)%P
    den=(D*y2+1)%P
    if den==0:return False
    x2=((y2-1)*pow(den,P-2,P))%P
    x=pow(x2,(P+3)//8,P)
    if (x*x-x2)%P!=0:x=(x*I)%P
    if (x*x-x2)%P!=0:return False
    if x==0 and sign:return False
    return True


def create_program_address(seeds, program_id: bytes):
    if any(len(s)>32 for s in seeds):return None
    h=hashlib.sha256(b''.join(seeds)+program_id+PDA_MARKER).digest()
    return None if is_on_ed25519(h) else h


def find_program_address(seeds, program_id: bytes):
    for bump in range(255,-1,-1):
        a=create_program_address(list(seeds)+[bytes([bump])],program_id)
        if a is not None:return a,bump
    raise RuntimeError('unable to find PDA')


def derive_curve(mint: str):
    try:
        m=base58.b58decode(mint);p=base58.b58decode(PUMP)
        if len(m)!=32 or len(p)!=32:return None,None
        raw,bump=find_program_address([SEED,m],p)
        return base58.b58encode(raw).decode(),bump
    except Exception:return None,None


def known_v774():
    return cpi.known_v774()


def rebuild_candidates(sigs,txs):
    ctx={};bt={};is_cpi={};groups={}
    for s in sigs:
        tx=txs.get(s)
        if not tx:continue
        ev=[x for x in emit.emitter_events(tx) if x['emitter']==PUMP]
        if not ev:continue
        is_cpi[s]=any(x['depth']>=2 for x in ev)
        acc,g=cpi.structural_accounts(tx);ctx[s]=acc;groups[s]=g
        bt[s]=float(tx.get('blockTime') or 0)
    hist=cpi.prehistory_for(ctx,bt)
    cand={}
    for s,z in hist.items():
        seen={};
        for item in z:seen.setdefault(item[0],item)
        if len(seen)==1:cand[s]=next(iter(seen.values()))
    return cand,ctx,is_cpi,groups


def main():
    print('='*136)
    print('MEMECOIN LAB — BONDING-CURVE PDA INVARIANT VALIDATOR V7.7.8')
    print('='*136)
    print('READ-ONLY | derive PDA([bonding-curve,mint]) and verify transaction/CPI presence | NO mint promotion / NO alpha')

    sigmeta=base.exact_signatures();sigs=sorted(sigmeta);txs=base.local_transactions(sigs);known=known_v774()
    cand,ctx,is_cpi,groups=rebuild_candidates(sigs,txs)
    print(f'migrate_signatures={len(sigs)} local_full_transactions={len(txs)}/{len(sigs)} unique_v777_candidates={len(cand)} v774_exact_labels={len(known)}')

    status=Counter();valid={};details={}
    for s,item in cand.items():
        mint=item[0];curve,bump=derive_curve(mint);tx=txs.get(s)
        allk=set(cpi.all_keys(tx)) if tx else set();struct=set(ctx.get(s,[]))
        in_all=bool(curve and curve in allk);in_struct=bool(curve and curve in struct)
        if not curve:st='PDA_DERIVE_FAILED'
        elif in_struct:st='PDA_IN_STRUCTURAL_CONTEXT'
        elif in_all:st='PDA_IN_TX_ONLY'
        else:st='PDA_ABSENT'
        status[st]+=1;details[s]=(mint,curve,bump,in_all,in_struct,item[1])
        if in_all:valid[s]=mint

    print('\nPDA VALIDATION CENSUS')
    print('status='+repr(dict(status)))
    print(f'pda_present_anywhere={len(valid)}/{len(cand)} structural={status["PDA_IN_STRUCTURAL_CONTEXT"]} tx_only={status["PDA_IN_TX_ONLY"]} absent={status["PDA_ABSENT"]}')

    covered=agree=disagree=0
    for s,m in valid.items():
        if s in known:
            covered+=1
            if known[s]==m:agree+=1
            else:disagree+=1
    new_cpi=sum(1 for s in valid if s not in known and is_cpi.get(s))
    print('\nVALIDATION AGAINST V7.7.4')
    print(f'known_validated={covered}/{len(known)} agreement={agree} disagreement={disagree}')
    print(f'new_CPI_candidates_passing_PDA={new_cpi}')

    print('\nNEW CPI PDA-PASS EXAMPLES')
    shown=0
    for s in sorted(valid):
        if s in known or not is_cpi.get(s):continue
        mint,curve,bump,in_all,in_struct,n=details[s]
        print(f'sig={s} mint={mint} bonding_curve={curve} bump={bump} pre_swaps={n} structural={in_struct} groups={[(g[0],g[2],len(g[1])) for g in groups.get(s,[])]}')
        shown+=1
        if shown>=12:break

    print('\nPDA-ABSENT EXAMPLES')
    shown=0
    for s,(mint,curve,bump,in_all,in_struct,n) in details.items():
        if in_all:continue
        print(f'sig={s} mint={mint} derived_curve={curve} pre_swaps={n} prior={known.get(s)} cpi={is_cpi.get(s)}')
        shown+=1
        if shown>=8:break

    print('\nQUALITY GATE')
    if disagree==0 and new_cpi>=50 and status['PDA_ABSENT']==0:
        print('STATUS=CPI_PDA_INVARIANT_VALIDATION_STRONG')
        print('Next: freeze this evidence rule and build a separate canonical migration table; then reconstruct PRE/MIGRATION/POST.')
    elif disagree==0 and new_cpi>=30:
        print('STATUS=CPI_PDA_INVARIANT_VALIDATION_USABLE')
        print('Next: canonicalize only candidates that pass BOTH unique-prehistory and bonding-curve-PDA presence; retain failures unresolved.')
    elif disagree==0 and len(valid)>0:
        print('STATUS=CPI_PDA_INVARIANT_VALIDATION_PARTIAL')
        print('Next: inspect PDA-absent cases / account loading, but do not weaken the invariant.')
    else:
        print('STATUS=CPI_PDA_INVARIANT_VALIDATION_UNSAFE')
        print('Conflicts or too little independent confirmation; no canonical promotion.')

    print('\nGuardrail: candidate selection came from V7.7.7; PDA presence is an independent protocol/account invariant. No trading conclusion is drawn.')

if __name__=='__main__':main()
