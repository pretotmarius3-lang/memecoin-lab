#!/usr/bin/env python3
"""MEMECOIN LAB — MIGRATE LOG EMITTER AUDIT V7.7.5.2

READ-ONLY diagnostic.
For every signature containing `Instruction: Migrate`, parse Solana runtime logs
and identify the program that was actually executing when that log line was emitted.
This tests a critical assumption from V7.7.3: that the text `Instruction: Migrate`
was emitted by the Pump bonding-curve program itself.

No mint promotion. No alpha discovery. No source DB writes.
"""
from __future__ import annotations

import re
from collections import Counter

import v774_deterministic_migration_mint_decoder as base

PUMP=base.PUMP
NEEDLE='Instruction: Migrate'
INVOKE=re.compile(r'^Program\s+([^\s]+)\s+invoke\s+\[(\d+)\]')
EXIT=re.compile(r'^Program\s+([^\s]+)\s+(?:success|failed:)')


def emitter_events(tx):
    logs=((tx or {}).get('meta') or {}).get('logMessages') or []
    stack=[]
    out=[]
    for i,line in enumerate(logs):
        s=str(line)
        m=INVOKE.match(s)
        if m:
            pid=m.group(1);depth=int(m.group(2))
            # Runtime depth is authoritative; repair stack if capture is odd.
            while len(stack)>=depth: stack.pop()
            stack.append(pid)
            continue
        if NEEDLE in s:
            out.append({
                'log_index':i,
                'line':s,
                'emitter':stack[-1] if stack else None,
                'depth':len(stack),
                'stack':tuple(stack),
            })
        m=EXIT.match(s)
        if m:
            pid=m.group(1)
            if stack and stack[-1]==pid:
                stack.pop()
            elif pid in stack:
                # Conservative repair for malformed/partial nested logs.
                j=len(stack)-1-stack[::-1].index(pid)
                del stack[j:]
    return out


def main():
    print('='*136)
    print('MEMECOIN LAB — MIGRATE LOG EMITTER AUDIT V7.7.5.2')
    print('='*136)
    print('READ-ONLY | runtime log stack attribution | NO mint promotion / NO alpha discovery')
    print(f'expected Pump program={PUMP}')

    sigmeta=base.exact_signatures();sigs=sorted(sigmeta)
    txs=base.local_transactions(sigs)
    print(f'exact_migrate_text_signatures={len(sigs)} local_full_transactions={len(txs)}/{len(sigs)}')

    emitters=Counter(); depths=Counter(); per_sig={}; no_context=[]; multi=[]
    pump_sigs=[]; nonpump_sigs=[]
    for s in sigs:
        ev=emitter_events(txs.get(s)) if s in txs else []
        per_sig[s]=ev
        if not ev:
            no_context.append(s);continue
        if len(ev)>1:multi.append(s)
        es={x['emitter'] for x in ev if x['emitter']}
        for x in ev:
            emitters[str(x['emitter'])]+=1;depths[x['depth']]+=1
        if PUMP in es:pump_sigs.append(s)
        else:nonpump_sigs.append(s)

    print('\nEMITTER CENSUS')
    print(f'log_events={sum(emitters.values())} signatures_with_context={len(sigs)-len(no_context)} no_context={len(no_context)} multi_migrate_logs={len(multi)}')
    print(f'Pump_emitter_signatures={len(pump_sigs)}/{len(sigs)} non_Pump_emitter_signatures={len(nonpump_sigs)}')
    print('depths='+repr(dict(sorted(depths.items()))))

    print('\nEMITTER PROGRAMS')
    for pid,n in emitters.most_common(20):
        tag='  <== PUMP' if pid==PUMP else ''
        print(f'{pid:<46} n={n}{tag}')

    print('\nPUMP-EMITTED EXAMPLES')
    for s in pump_sigs[:8]:
        for x in per_sig[s]:
            if x['emitter']==PUMP:
                print(f"sig={s} log_index={x['log_index']} depth={x['depth']} stack={' > '.join(x['stack'])}")

    print('\nNON-PUMP-EMITTED EXAMPLES')
    shown=0
    for s in nonpump_sigs:
        for x in per_sig[s]:
            print(f"sig={s} emitter={x['emitter']} log_index={x['log_index']} depth={x['depth']} stack={' > '.join(x['stack'])}")
            shown+=1
            if shown>=10:break
        if shown>=10:break

    print('\nDIAGNOSTIC GATE')
    if len(pump_sigs)>=100:
        print('STATUS=MIGRATE_TEXT_MOSTLY_PUMP_EMITTED')
        print('Next: map the historical Pump discriminator/version for Pump-emitted rows only; do not use non-Pump migrate text.')
    elif len(pump_sigs)>=10:
        print('STATUS=MIGRATE_TEXT_MIXED_PROGRAMS')
        print('Next: restrict canonical migration cohort to Pump-emitted migrate logs, then decode their instruction/account representation.')
    elif len(pump_sigs)>0:
        print('STATUS=MIGRATE_TEXT_MOSTLY_NOT_PUMP')
        print('Critical correction: V7.7.3 text search mixed other programs. Keep only Pump-emitted migrate logs as canonical Pump migration evidence.')
    else:
        print('STATUS=MIGRATE_TEXT_NOT_PUMP_EVIDENCE')
        print('Critical correction: `Instruction: Migrate` text in this capture is not emitted by the Pump program; V7.7.3 cohort must be invalidated.')

    print('\nGuardrail: emitter attribution comes from the runtime invoke/success stack surrounding each exact log line; no strategy conclusions are drawn.')

if __name__=='__main__':main()
