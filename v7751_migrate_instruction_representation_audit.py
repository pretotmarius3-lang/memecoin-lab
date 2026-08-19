#!/usr/bin/env python3
"""MEMECOIN LAB — MIGRATE INSTRUCTION REPRESENTATION AUDIT V7.7.5.1

READ-ONLY diagnostic for the 155 exact `Instruction: Migrate` log transactions
that V7.7.5 did not match by discriminator.

Goal: inspect how Pump instructions are represented in the locally stored RPC
transactions before changing the canonical decoder. No alpha logic and no source
DB writes.
"""
from __future__ import annotations

import json
from collections import Counter

import v774_deterministic_migration_mint_decoder as base
import v775_idl_account_migration_mint_decoder as old

PUMP=base.PUMP
DISC=old.DISC


def data_bytes(data):
    if isinstance(data,str):
        b=old.b58decode(data)
        return b,'str/base58'
    if isinstance(data,list):
        # Some RPC shapes expose [encoded, encoding].
        if len(data)>=1 and isinstance(data[0],str):
            enc=str(data[1]).lower() if len(data)>1 else 'base58'
            if enc=='base58': return old.b58decode(data[0]),'list/base58'
            return b'',f'list/{enc}'
        return b'','list'
    if isinstance(data,dict):
        return b'','dict'
    return b'',type(data).__name__


def all_keys(tx):
    return old.all_keys(tx)


def prog(ix,keys):
    p=old.program_for(ix,keys)
    if p:return p
    # jsonParsed can use `program` + `programId`; only programId is an address.
    if isinstance(ix.get('programId'),dict):
        return str(ix['programId'].get('pubkey') or '')
    return None


def iter_ix(tx):
    keys=all_keys(tx)
    msg=((tx.get('transaction') or {}).get('message') or {})
    for i,ix in enumerate(msg.get('instructions') or []):
        if isinstance(ix,dict): yield 'TOP',i,ix,keys
    for g in (tx.get('meta') or {}).get('innerInstructions') or []:
        parent=g.get('index')
        for j,ix in enumerate(g.get('instructions') or []):
            if isinstance(ix,dict): yield f'INNER@{parent}',j,ix,keys


def main():
    print('='*136)
    print('MEMECOIN LAB — MIGRATE INSTRUCTION REPRESENTATION AUDIT V7.7.5.1')
    print('='*136)
    print('READ-ONLY | exact migrate-log signatures only | diagnose instruction encoding; NO alpha discovery')
    print(f'Pump={PUMP} expected_disc={DISC.hex()}')

    sigmeta=base.exact_signatures(); sigs=sorted(sigmeta)
    txs=base.local_transactions(sigs)
    print(f'exact_migrate_signatures={len(sigs)} local_full_transactions={len(txs)}/{len(sigs)}')

    status=Counter(); pump_ix_total=0; exact_disc=0
    disc_counter=Counter(); shape_counter=Counter(); data_shape=Counter(); levels=Counter()
    no_pump=[]; pump_no_disc=[]

    for s in sigs:
        tx=txs.get(s)
        if not tx:
            status['NO_TX']+=1; continue
        seen_pump=[]; seen_match=[]
        for level,i,ix,keys in iter_ix(tx):
            shape_counter[tuple(sorted(ix.keys()))]+=1
            p=prog(ix,keys)
            if p!=PUMP: continue
            pump_ix_total+=1; levels[level.split('@')[0]]+=1
            raw,shape=data_bytes(ix.get('data')); data_shape[shape]+=1
            prefix=raw[:8].hex() if len(raw)>=8 else f'LEN{len(raw)}'
            disc_counter[prefix]+=1
            rec=(level,i,prefix,len(raw),len(ix.get('accounts') or []),sorted(ix.keys()))
            seen_pump.append(rec)
            if len(raw)>=8 and raw[:8]==DISC:
                exact_disc+=1;seen_match.append(rec)
        if seen_match: status['MATCH']+=1
        elif seen_pump:
            status['PUMP_IX_BUT_NO_DISC']+=1
            if len(pump_no_disc)<12:pump_no_disc.append((s,seen_pump))
        else:
            status['NO_PUMP_IX_VISIBLE']+=1
            if len(no_pump)<12:
                # retain a compact inventory of all visible programs for diagnosis
                c=Counter()
                for level,i,ix,keys in iter_ix(tx): c[str(prog(ix,keys))]+=1
                no_pump.append((s,dict(c)))

    print('\nREPRESENTATION CENSUS')
    print('status='+repr(dict(status)))
    print(f'visible_pump_instructions={pump_ix_total} exact_discriminator_matches={exact_disc}')
    print('pump_instruction_levels='+repr(dict(levels)))
    print('pump_data_shapes='+repr(dict(data_shape)))

    print('\nPUMP DISCRIMINATOR PREFIXES — top 20')
    for k,n in disc_counter.most_common(20): print(f'{k:<20} n={n}')

    print('\nEXAMPLES — Pump instruction visible but migrate discriminator missing')
    for s,recs in pump_no_disc:
        print(f'sig={s}')
        for r in recs[:12]:print(f'  level={r[0]} ix={r[1]} prefix={r[2]} data_len={r[3]} accounts={r[4]} keys={r[5]}')

    print('\nEXAMPLES — migrate log exists but no Pump instruction visible')
    for s,progs in no_pump:
        print(f'sig={s} programs={progs}')

    print('\nDIAGNOSTIC GATE')
    if status['PUMP_IX_BUT_NO_DISC'] >= 50:
        print('STATUS=PUMP_INSTRUCTIONS_VISIBLE_DISCRIMINATOR_REPRESENTATION_MISMATCH')
        print('Next: decode the observed instruction data representation / historical discriminator before mint attribution.')
    elif status['NO_PUMP_IX_VISIBLE'] >= 50:
        print('STATUS=MIGRATE_OCCURS_BEHIND_UNRESOLVED_CPI_OR_ACCOUNT_LOADING')
        print('Next: use log invocation stack + inner-instruction parent mapping and resolve versioned loaded addresses / alternate RPC representation.')
    elif status['MATCH'] >= 50:
        print('STATUS=REPRESENTATION_AUDIT_NOW_MATCHES_MAJORITY')
        print('Next: port this representation handling into the canonical IDL decoder.')
    else:
        print('STATUS=MIXED_REPRESENTATION_REQUIRES_TARGETED_FIX')
        print('Use the examples above to choose the smallest deterministic decoder correction.')

    print('\nGuardrail: this audit diagnoses serialization only; it does not promote any new mint to EXACT.')

if __name__=='__main__':main()
