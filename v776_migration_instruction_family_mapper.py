#!/usr/bin/env python3
"""MEMECOIN LAB — PUMP MIGRATION INSTRUCTION FAMILY MAPPER V7.7.6

READ-ONLY discovery/infrastructure diagnostic.
For signatures whose exact `Instruction: Migrate` runtime log is emitted by Pump,
inspect the Pump instruction active at that log, its discriminator, accounts and
agreement with V7.7.4 known mint labels.

No alpha discovery. No source DB writes. No mint is promoted to canonical here.
"""
from __future__ import annotations

import base58
from collections import Counter, defaultdict

import v774_deterministic_migration_mint_decoder as base
import v7752_migrate_log_emitter_audit as emit

PUMP=base.PUMP


def keys(tx):
    return base.account_keys(tx)


def ix_program(ix, aks):
    p=ix.get('programId') if isinstance(ix,dict) else None
    if p:return str(p)
    j=ix.get('programIdIndex') if isinstance(ix,dict) else None
    try:return aks[int(j)]
    except Exception:return None


def ix_accounts(ix, aks):
    out=[]
    for a in (ix.get('accounts') or []):
        if isinstance(a,str):out.append(a)
        else:
            try:out.append(aks[int(a)])
            except Exception:out.append(f'IDX:{a}')
    return out


def disc(ix):
    d=ix.get('data') if isinstance(ix,dict) else None
    if not isinstance(d,str):return None
    try:return base58.b58decode(d)[:8].hex()
    except Exception:return None


def pump_ixs(tx):
    aks=keys(tx);out=[]
    msg=((tx or {}).get('transaction') or {}).get('message') or {}
    for i,ix in enumerate(msg.get('instructions') or []):
        if isinstance(ix,dict) and ix_program(ix,aks)==PUMP:
            out.append({'level':'TOP','outer':i,'ix':i,'disc':disc(ix),'accounts':ix_accounts(ix,aks)})
    meta=(tx or {}).get('meta') or {}
    for grp in meta.get('innerInstructions') or []:
        outer=grp.get('index')
        for j,ix in enumerate(grp.get('instructions') or []):
            if isinstance(ix,dict) and ix_program(ix,aks)==PUMP:
                out.append({'level':'INNER','outer':outer,'ix':j,'disc':disc(ix),'accounts':ix_accounts(ix,aks)})
    return out


def known_v774():
    try:
        d=base.ro(base.OUT)
        rr=d.execute("SELECT signature,mint FROM exact_migrations WHERE decode_status='EXACT' AND mint IS NOT NULL").fetchall()
        d.close();return {str(r['signature']):str(r['mint']) for r in rr}
    except Exception:return {}


def main():
    print('='*136)
    print('MEMECOIN LAB — PUMP MIGRATION INSTRUCTION FAMILY MAPPER V7.7.6')
    print('='*136)
    print('READ-ONLY | Pump-emitted migrate logs -> instruction families/account positions | NO mint promotion / NO alpha discovery')
    sigmeta=base.exact_signatures();sigs=sorted(sigmeta);txs=base.local_transactions(sigs);known=known_v774()
    print(f'migrate_text_signatures={len(sigs)} local_full_transactions={len(txs)}/{len(sigs)} v774_exact_labels={len(known)}')

    fam=Counter(); family_pos=defaultdict(Counter); family_known=Counter(); rows=[]; unresolved=0
    for s in sigs:
        tx=txs.get(s)
        if not tx:continue
        ev=[x for x in emit.emitter_events(tx) if x['emitter']==PUMP]
        if not ev:continue
        pis=pump_ixs(tx)
        km=known.get(s)
        # Runtime log attribution proves Pump emitter, but JSON instruction ordering does not
        # directly expose a log-index->instruction-index map. Therefore examine all Pump IX
        # families in the tx and learn account-position consistency only from known labels.
        for p in pis:
            f=(p['level'],p['disc'],len(p['accounts']))
            fam[f]+=1
            if km:
                family_known[f]+=1
                for pos,a in enumerate(p['accounts']):
                    if a==km: family_pos[f][pos]+=1
        rows.append((s,km,pis))
        if not pis:unresolved+=1

    print('\nINSTRUCTION FAMILY CENSUS')
    print(f'pump_emitter_signatures={len(rows)} pump_emitter_without_visible_pump_ix={unresolved}')
    for f,n in fam.most_common(30):
        lvl,di,na=f; kp=family_pos.get(f,Counter()); labels=family_known.get(f,0)
        pos='none'
        if kp: pos=','.join(f'{k}:{v}/{labels}' for k,v in kp.most_common())
        print(f'{lvl:<5} disc={str(di):16} accounts={na:2d} n={n:3d} known_labels={labels:2d} mint_positions={pos}')

    print('\nCANDIDATE DETERMINISTIC FAMILIES')
    candidates=[]
    for f,n in fam.items():
        labels=family_known.get(f,0); cp=family_pos.get(f,Counter())
        if labels>=3 and cp:
            pos,hits=cp.most_common(1)[0]
            if hits==labels:
                candidates.append((labels,n,f,pos))
    candidates.sort(reverse=True)
    for labels,n,f,pos in candidates:
        print(f'family={f} n={n} known={labels} position={pos} agreement={labels}/{labels}')
    if not candidates:print('none')

    print('\nSIGNATURE-LEVEL RECOVERY DIAGNOSTIC')
    recovered={};amb=0
    for s,km,pis in rows:
        vals=[]
        for p in pis:
            f=(p['level'],p['disc'],len(p['accounts']))
            for labels,n,cf,pos in candidates:
                if f==cf and pos<len(p['accounts']): vals.append(p['accounts'][pos])
        vals=sorted(set(vals))
        if len(vals)==1:recovered[s]=vals[0]
        elif len(vals)>1:amb+=1
    agreement=sum(1 for s,m in recovered.items() if s in known and known[s]==m)
    disagreement=sum(1 for s,m in recovered.items() if s in known and known[s]!=m)
    new=sum(1 for s in recovered if s not in known)
    print(f'unique_candidate_mints={len(recovered)}/{len(rows)} newly_recovered_noncanonical={new} ambiguous={amb}')
    print(f'v774_known_agreement={agreement} disagreement={disagreement}')
    for s,m in list(recovered.items())[:10]:print(f'sig={s} candidate_mint={m} prior={known.get(s)}')

    print('\nQUALITY GATE')
    if candidates and disagreement==0 and len(recovered)>=100:
        print('STATUS=DETERMINISTIC_FAMILY_MAPPING_STRONG')
        print('Next: independent validation of candidate account-position mapping, then write a separate canonical decoder.')
    elif candidates and disagreement==0 and len(recovered)>=50:
        print('STATUS=DETERMINISTIC_FAMILY_MAPPING_PROMISING')
        print('Next: validate unresolved/ambiguous families before canonical mint promotion.')
    else:
        print('STATUS=FAMILY_MAPPING_NOT_YET_SUFFICIENT')
        print('Next: inspect CPI parent/child instruction structure and account-role invariants; do not promote candidate mints.')
    print('\nGuardrail: V7.7.4 labels are used only to diagnose account-position consistency. Newly recovered values remain NONCANONICAL candidates.')

if __name__=='__main__':main()
