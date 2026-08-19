#!/usr/bin/env python3
"""MEMECOIN LAB — HOT COVERAGE / REFRESH PROFILER V7.6.3

READ-ONLY diagnostic. Correlates the fresh V7611 causal cohort with V750
acquisition trace and raw payload blockTime to decompose late inputs into:
  chain blockTime -> queue enqueue
  enqueue -> first claim
  HTTP start -> end
  enqueue -> raw store
  chain -> raw store
It also reports HOT token admission/subscription ages where available.

No writes, no strategy logic, no queue mutation.
"""
from __future__ import annotations
import json, sqlite3, statistics, time, zlib
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW=ROOT/'v5_raw_events.db'
TRACE=ROOT/'v750_acquisition_trace.db'
FEAT=ROOT/'v52_features.db'
WINDOW=300.0


def con(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON')
    d.execute('PRAGMA busy_timeout=30000')
    return d

def pct(xs,q):
    xs=[float(x) for x in xs if x is not None]
    if not xs:return None
    xs.sort();p=(len(xs)-1)*q;lo=int(p);hi=min(len(xs)-1,lo+1);f=p-lo
    return xs[lo]+(xs[hi]-xs[lo])*f

def fmt(x):
    return 'None' if x is None else f'{x:.3f}s'

def stat(name,xs):
    xs=[float(x) for x in xs if x is not None]
    print(f'{name:<39} n={len(xs):4d} p50={fmt(pct(xs,.5)):>9} p90={fmt(pct(xs,.9)):>9} p95={fmt(pct(xs,.95)):>9} max={fmt(max(xs) if xs else None):>9}')

def block_time(payload_zlib, fallback):
    try:
        obj=json.loads(zlib.decompress(payload_zlib).decode())
        tx=obj.get('rpc_transaction') or {}
        return float(tx.get('blockTime') or fallback)
    except Exception:
        return float(fallback)

def main():
    now=time.time();f=con(FEAT);r=con(RAW);t=con(TRACE)
    state=f.execute('SELECT * FROM v7611_scheduler_state WHERE id=1').fetchone()
    if not state:
        raise SystemExit('No V7611 state found')
    activation=float(state['activation_observed_at'] or 0)
    cohort=f.execute('''SELECT token_mint,stage_s,cutoff_ts,built_at,first_ts,first_observed_at
        FROM v7611_causal_snapshots
        WHERE built_at>=? AND stage_s IN (20,30)
        ORDER BY built_at''',(now-WINDOW,)).fetchall()
    print('='*150)
    print('MEMECOIN LAB — HOT COVERAGE / REFRESH PROFILER V7.6.3')
    print('='*150)
    print(f'window={WINDOW:.0f}s cohort_rows={len(cohort)} activation>{activation:.3f} READ-ONLY')
    if not cohort:
        print('No recent V7611 T+20/T+30 cohort yet.');return

    tokens=sorted({str(x['token_mint']) for x in cohort})
    qmarks=','.join('?' for _ in tokens)
    swaps=f.execute(f'''SELECT signature,token_mint,timestamp,observed_at FROM v52_swaps
        WHERE token_mint IN ({qmarks}) AND observed_at>? ORDER BY observed_at''',tokens+[activation]).fetchall()
    bysig={str(x['signature']):x for x in swaps}
    sigs=list(bysig)
    if not sigs:
        print('No swaps for cohort');return
    qm=','.join('?' for _ in sigs)
    raws=r.execute(f'''SELECT signature,observed_at,payload_zlib FROM v5_raw_transactions
        WHERE signature IN ({qm})''',sigs).fetchall()
    rawmap={str(x['signature']):x for x in raws}
    traces=t.execute(f'''SELECT signature,kind,mint,enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at
        FROM trace WHERE signature IN ({qm})''',sigs).fetchall()
    trmap={str(x['signature']):x for x in traces}

    chain_enqueue=[]; enqueue_claim=[]; http=[]; enqueue_store=[]; chain_store=[]; chain_canonical=[]
    missing_trace=0; hot_count=0; create_count=0
    rows=[]
    for sig,s in bysig.items():
        rr=rawmap.get(sig);tr=trmap.get(sig)
        if not rr:continue
        bt=block_time(rr['payload_zlib'],s['timestamp'])
        obs=float(s['observed_at'])
        chain_canonical.append(max(0,obs-bt))
        if not tr:
            missing_trace+=1;continue
        kind=str(tr['kind'] or '')
        hot_count+=kind=='HOT';create_count+=kind=='CREATE'
        en=tr['enqueue_at'];cl=tr['first_claim_at'];hs=tr['http_start_at'];he=tr['http_end_at'];rs=tr['raw_store_at']
        if en is not None:chain_enqueue.append(max(0,float(en)-bt))
        if en is not None and cl is not None:enqueue_claim.append(max(0,float(cl)-float(en)))
        if hs is not None and he is not None:http.append(max(0,float(he)-float(hs)))
        if en is not None and rs is not None:enqueue_store.append(max(0,float(rs)-float(en)))
        if rs is not None:chain_store.append(max(0,float(rs)-bt))
        rows.append((sig,kind,str(s['token_mint']),bt,float(en) if en is not None else None,float(rs) if rs is not None else None,obs))

    print('\nPATH DECOMPOSITION')
    stat('chain blockTime -> queue enqueue',chain_enqueue)
    stat('enqueue -> first claim',enqueue_claim)
    stat('HTTP start -> end',http)
    stat('enqueue -> raw store',enqueue_store)
    stat('chain blockTime -> raw store',chain_store)
    stat('chain blockTime -> canonical observed',chain_canonical)
    print(f'\ncoverage: cohort_swaps={len(swaps)} raw={len(raws)} trace={len(traces)} missing_trace={missing_trace} HOT={hot_count} CREATE={create_count}')

    # Token-level HOT registry context for current epoch rows, if present.
    try:
        hotrows=r.execute(f'''SELECT mint,admitted_at,expires_at,last_subscribed_at,updated_at,status,epoch_id
            FROM v515_hot_tokens WHERE mint IN ({qmarks})''',tokens).fetchall()
    except Exception:
        hotrows=[]
    admit_to_first=[]; sub_to_first=[]
    first_by_token={}
    for sig,kind,mint,bt,en,rs,obs in rows:
        first_by_token[mint]=min(first_by_token.get(mint,bt),bt)
    for h in hotrows:
        mint=str(h['mint']);first=first_by_token.get(mint)
        if first is None:continue
        if h['admitted_at'] is not None:admit_to_first.append(max(0,first-float(h['admitted_at'])))
        if h['last_subscribed_at'] is not None:sub_to_first.append(max(0,first-float(h['last_subscribed_at'])))
    if hotrows:
        print('\nHOT REGISTRY CONTEXT')
        stat('admission -> first cohort chain ts',admit_to_first)
        stat('last subscribe -> first cohort chain ts',sub_to_first)

    ce90=pct(chain_enqueue,.9); es90=pct(enqueue_store,.9); cc90=pct(chain_canonical,.9)
    print('\nVERDICT')
    if ce90 is not None and ce90>10 and (es90 is None or es90<3):
        verdict='HOT_DISCOVERY_OR_SUBSCRIPTION_COVERAGE_DOMINANT'
    elif es90 is not None and es90>5:
        verdict='QUEUE_OR_HTTP_ENRICHMENT_DOMINANT'
    elif cc90 is not None and cc90>10:
        verdict='TRACE_COVERAGE_INCOMPLETE_OR_NONHOT_PATH_LATE'
    else:
        verdict='CURRENT_HOT_PATH_FAST'
    print(' ',verdict)
    print(f' chain->enqueue p90={fmt(ce90)} | enqueue->store p90={fmt(es90)} | chain->canonical p90={fmt(cc90)}')
    print('\nGuardrail: READ-ONLY infrastructure diagnosis; no strategy evidence or queue mutation.')

if __name__=='__main__':main()
