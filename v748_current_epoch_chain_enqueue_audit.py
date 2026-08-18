#!/usr/bin/env python3
"""MEMECOIN LAB — CURRENT-EPOCH CHAIN -> ENQUEUE AUDIT V7.4.8

Read-only diagnostic for the currently instrumented V7.4.7 acquisition epoch.
It joins v747_acquisition_trace.db to v5_raw_events.db and decompresses stored
rpc_transaction payloads in Python to recover blockTime.

Purpose:
- measure blockTime -> enqueue_at (closest durable timestamp to WS handling),
- measure enqueue_at -> raw_store_at,
- measure blockTime -> raw_store_at,
- split CREATE vs HOT,
- decide whether current latency is provider/WS/commitment dominated or queue dominated.

No queue mutations, no strategy changes, no live trading.
"""
from __future__ import annotations
import json, math, sqlite3, statistics, time, zlib
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW=ROOT/'v5_raw_events.db'
TRACE=ROOT/'v747_acquisition_trace.db'


def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(math.floor(p)); hi=int(math.ceil(p))
    return ys[lo] if lo==hi else ys[lo]+(ys[hi]-ys[lo])*(p-lo)

def sf(x,d=0.0):
    try:return float(x)
    except:return d

def fmt(x):
    return f"{sf(x):6.2f}s"

def stats(label,xs):
    if not xs:
        print(f"{label:<34} n=0")
        return
    le1=100*sum(x<=1 for x in xs)/len(xs)
    le5=100*sum(x<=5 for x in xs)/len(xs)
    le15=100*sum(x<=15 for x in xs)/len(xs)
    le30=100*sum(x<=30 for x in xs)/len(xs)
    le60=100*sum(x<=60 for x in xs)/len(xs)
    print(f"{label:<34} n={len(xs):5d} p50={fmt(pct(xs,.50))} p90={fmt(pct(xs,.90))} p95={fmt(pct(xs,.95))} p99={fmt(pct(xs,.99))} max={fmt(max(xs))}")
    print(f"  <=1/5/15/30/60s = {le1:5.1f}/{le5:5.1f}/{le15:5.1f}/{le30:5.1f}/{le60:5.1f}%")

def main():
    if not RAW.exists():raise SystemExit(f"Missing {RAW}")
    if not TRACE.exists():raise SystemExit(f"Missing {TRACE}")

    r=sqlite3.connect(f'file:{RAW}?mode=ro',uri=True,timeout=30); r.row_factory=sqlite3.Row
    t=sqlite3.connect(f'file:{TRACE}?mode=ro',uri=True,timeout=30); t.row_factory=sqlite3.Row
    epoch=t.execute("SELECT epoch_id,COUNT(*) n FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY MAX(updated_at) DESC LIMIT 1").fetchone()
    if not epoch:raise SystemExit('No V747 epoch in trace DB')
    epoch_id=epoch['epoch_id']
    trs=t.execute("SELECT signature,kind,enqueue_at,raw_store_at FROM trace WHERE epoch_id=? AND enqueue_at IS NOT NULL ORDER BY enqueue_at",(epoch_id,)).fetchall()

    chain_enqueue={'CREATE':[],'HOT':[]}
    enqueue_store={'CREATE':[],'HOT':[]}
    chain_store={'CREATE':[],'HOT':[]}
    missing_raw=bad_payload=missing_block=anomaly=0

    for x in trs:
        rr=r.execute("SELECT payload_zlib FROM v5_raw_transactions WHERE signature=?",(x['signature'],)).fetchone()
        if not rr:
            missing_raw+=1; continue
        try:
            payload=json.loads(zlib.decompress(rr['payload_zlib']).decode('utf-8'))
        except Exception:
            bad_payload+=1; continue
        bt=(payload.get('rpc_transaction') or {}).get('blockTime')
        if bt is None:
            missing_block+=1; continue
        try:bt=float(bt)
        except Exception:
            missing_block+=1; continue
        kind=(x['kind'] or 'OTHER').upper()
        if kind not in chain_enqueue: continue
        eq=float(x['enqueue_at']) if x['enqueue_at'] is not None else None
        st=float(x['raw_store_at']) if x['raw_store_at'] is not None else None
        if eq is not None:
            d=eq-bt
            if -1 <= d < 3600: chain_enqueue[kind].append(max(0.0,d))
            else: anomaly+=1
        if eq is not None and st is not None:
            d=st-eq
            if -1 <= d < 3600: enqueue_store[kind].append(max(0.0,d))
            else: anomaly+=1
        if st is not None:
            d=st-bt
            if -1 <= d < 3600: chain_store[kind].append(max(0.0,d))
            else: anomaly+=1

    print('='*178)
    print('MEMECOIN LAB — CURRENT-EPOCH CHAIN -> ENQUEUE AUDIT V7.4.8')
    print('='*178)
    print(f"epoch={epoch_id} traces={len(trs)} missing_raw={missing_raw} bad_payload={bad_payload} missing_blockTime={missing_block} anomalies={anomaly} | READ-ONLY\n")

    for kind in ('CREATE','HOT'):
        print(f'[{kind}]')
        stats('CHAIN blockTime -> enqueue',chain_enqueue[kind])
        stats('enqueue -> raw store',enqueue_store[kind])
        stats('CHAIN blockTime -> raw store',chain_store[kind])
        print()

    # verdict on HOT, since it was the historical bottleneck
    ce=chain_enqueue['HOT']; es=enqueue_store['HOT']
    ce50=pct(ce,.50) if ce else None; ce90=pct(ce,.90) if ce else None
    es50=pct(es,.50) if es else None; es90=pct(es,.90) if es else None
    if not ce:
        verdict='INSUFFICIENT_HOT_DATA'
    elif (es90 or 0) > 15:
        verdict='QUEUE_OR_HTTP_STILL_DOMINANT'
    elif (ce50 or 0) > 15:
        verdict='PROVIDER_WS_OR_COMMITMENT_LATENCY_DOMINANT'
    elif (ce90 or 0) > 15:
        verdict='TAIL_PROVIDER_WS_OR_COMMITMENT_LATENCY'
    else:
        verdict='CURRENT_ACQUISITION_PATH_CAUSALLY_FAST'

    print('ROOT-CAUSE INTERPRETATION')
    print(f"  VERDICT={verdict}")
    if ce:
        print(f"  HOT chain->enqueue p50/p90={sf(ce50):.2f}/{sf(ce90):.2f}s | enqueue->store p50/p90={sf(es50):.2f}/{sf(es90):.2f}s")
    print('  If chain->enqueue remains high while enqueue->store is sub-second, investigate commitment/provider/WS receive latency.')
    print('  If both are now low, historical delay was backlog-state dependent; keep monitoring and re-freeze future-only evidence only after infrastructure stabilization.')
    print('Guardrail: diagnosis only; no strategy retuning, no capital decision.')

    r.close(); t.close()

if __name__=='__main__':main()
