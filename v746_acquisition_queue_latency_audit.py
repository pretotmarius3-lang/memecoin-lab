#!/usr/bin/env python3
"""MEMECOIN LAB — ACQUISITION QUEUE LATENCY AUDIT V7.4.6

READ-ONLY diagnostic over v5_raw_events.db.

Important correction to V7.4.5 interpretation:
`v5_raw_transactions.observed_at` is written by V5.1.7 store_tx() only AFTER
getTransaction enrichment succeeds. It is therefore not the websocket receive
time. The durable `v515_hot_queue.first_seen` timestamp is the earliest queue
receipt marker available in the current schema.

This audit splits pre-V522 latency into:
  chain blockTime -> queue first_seen
  queue first_seen -> raw transaction stored (observed_at)
  raw stored -> v52_processed.processed_at (when V52 is available)

No strategies, queues, rows, or verdicts are modified.
"""
from __future__ import annotations
import json, math, sqlite3, statistics, time, zlib
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
V5=ROOT/'v5_raw_events.db'
V52=ROOT/'v52_features.db'
OUT=ROOT/'v746_acquisition_queue_latency.db'
LIMIT=30000

def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def odb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=30000');return d

def pct(xs,q):
    if not xs:return None
    ys=sorted(xs);p=(len(ys)-1)*q;lo=int(math.floor(p));hi=int(math.ceil(p))
    return ys[lo] if lo==hi else ys[lo]+(ys[hi]-ys[lo])*(p-lo)
def sf(x,d=0.0):
    try:return float(x) if x is not None and math.isfinite(float(x)) else d
    except:return d
def line(name,xs):
    if not xs:
        print(f'{name:<34} n=0');return
    print(f"{name:<34} n={len(xs):6d} p50={sf(pct(xs,.50)):7.1f}s p90={sf(pct(xs,.90)):7.1f}s p95={sf(pct(xs,.95)):7.1f}s p99={sf(pct(xs,.99)):7.1f}s max={max(xs):8.1f}s")
    print(f"  <=1/5/15/30/60s = {100*sum(v<=1 for v in xs)/len(xs):5.1f}/{100*sum(v<=5 for v in xs)/len(xs):5.1f}/{100*sum(v<=15 for v in xs)/len(xs):5.1f}/{100*sum(v<=30 for v in xs)/len(xs):5.1f}/{100*sum(v<=60 for v in xs)/len(xs):5.1f}%")
def blocktime_from_payload(blob):
    try:
        p=json.loads(zlib.decompress(blob));tx=p.get('rpc_transaction') or {};v=tx.get('blockTime') or p.get('blockTime')
        return float(v) if v is not None else None
    except:return None

def init():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS audit_run(run_id TEXT PRIMARY KEY,created_at REAL,rows_examined INTEGER,joined_queue INTEGER,joined_processed INTEGER,verdict TEXT,note TEXT);
    CREATE TABLE IF NOT EXISTS stage_summary(run_id TEXT,stage TEXT,n INTEGER,p50 REAL,p90 REAL,p95 REAL,p99 REAL,max_s REAL,PRIMARY KEY(run_id,stage));
    CREATE TABLE IF NOT EXISTS kind_summary(run_id TEXT,kind TEXT,n INTEGER,queue_wait_p50 REAL,queue_wait_p90 REAL,chain_to_queue_p50 REAL,chain_to_queue_p90 REAL,PRIMARY KEY(run_id,kind));
    ''');d.commit();d.close()
def main():
    init()
    if not V5.exists():raise SystemExit(f'Missing {V5}')
    a=ro(V5);b=ro(V52) if V52.exists() else None;o=odb();run=f'Q746_{int(time.time())}'
    rows=a.execute('''SELECT r.signature,r.observed_at,r.payload_zlib,q.first_seen,q.kind,q.attempts,q.updated_at,q.status
                      FROM v5_raw_transactions r
                      LEFT JOIN v515_hot_queue q ON q.signature=r.signature
                      ORDER BY r.observed_at DESC LIMIT ?''',(LIMIT,)).fetchall()
    chain_queue=[];queue_store=[];chain_store=[];store_processed=[];chain_processed=[];bykind={};jq=jp=0;an=0
    for r in rows:
        bt=blocktime_from_payload(r['payload_zlib'])
        store=float(r['observed_at']) if r['observed_at'] is not None else None
        first=float(r['first_seen']) if r['first_seen'] is not None else None
        if first is not None:jq+=1
        if bt is not None and first is not None:
            x=first-bt
            if -2<=x<3600:chain_queue.append(max(0,x))
            else:an+=1
        if first is not None and store is not None:
            x=store-first
            if -2<=x<3600:
                queue_store.append(max(0,x));bykind.setdefault(str(r['kind'] or 'UNKNOWN'),{'qs':[],'cq':[]})['qs'].append(max(0,x))
            else:an+=1
        if bt is not None and store is not None:
            x=store-bt
            if -2<=x<3600:chain_store.append(max(0,x))
        if b is not None:
            p=b.execute('SELECT processed_at FROM v52_processed WHERE signature=?',(r['signature'],)).fetchone()
            if p and p[0] is not None:
                jp+=1;pt=float(p[0])
                if store is not None and -2<=pt-store<3600:store_processed.append(max(0,pt-store))
                if bt is not None and -2<=pt-bt<3600:chain_processed.append(max(0,pt-bt))
        if bt is not None and first is not None:
            x=first-bt
            if -2<=x<3600:bykind.setdefault(str(r['kind'] or 'UNKNOWN'),{'qs':[],'cq':[]})['cq'].append(max(0,x))
    print('='*178);print('MEMECOIN LAB — ACQUISITION QUEUE LATENCY AUDIT V7.4.6');print('='*178)
    print(f'rows_examined={len(rows)} joined_queue={jq} joined_processed={jp} anomalies={an} | READ-ONLY')
    print('Correction: raw observed_at is STORE time after HTTP enrichment, not websocket receive time.\n')
    line('CHAIN -> QUEUE first_seen',chain_queue)
    line('QUEUE first_seen -> RAW stored',queue_store)
    line('CHAIN -> RAW stored total',chain_store)
    line('RAW stored -> V52 processed',store_processed)
    line('CHAIN -> V52 processed total',chain_processed)
    cq=pct(chain_queue,.50) or 0;qs=pct(queue_store,.50) or 0;sp=pct(store_processed,.50) or 0
    if cq>max(qs,sp)*1.5 and cq>15:ver='WS_DISCOVERY_OR_PROVIDER_DELIVERY_DOMINANT'
    elif qs>max(cq,sp)*1.5 and qs>15:ver='ACQUISITION_QUEUE_HTTP_ENRICHMENT_DOMINANT'
    elif sp>max(cq,qs)*1.5 and sp>15:ver='V522_DECODER_QUEUE_DOMINANT'
    else:ver='MIXED_PRE_V52_LATENCY'
    print('\nBY QUEUE KIND')
    for k,d in sorted(bykind.items()):
        print(f"  {k:<12} n={len(d['qs']):5d} chain->queue p50/p90={sf(pct(d['cq'],.5)):6.1f}/{sf(pct(d['cq'],.9)):6.1f}s | queue->store p50/p90={sf(pct(d['qs'],.5)):6.1f}/{sf(pct(d['qs'],.9)):6.1f}s")
    print('\nROOT-CAUSE INTERPRETATION')
    print(f'  VERDICT={ver}')
    if ver=='WS_DISCOVERY_OR_PROVIDER_DELIVERY_DOMINANT':
        print('  Earliest durable queue receipt is already late versus blockTime. Instrument websocket receive callback and provider subscription path next.')
    elif ver=='ACQUISITION_QUEUE_HTTP_ENRICHMENT_DOMINANT':
        print('  Queue sees signatures reasonably early, but HTTP getTransaction workers/store_tx are delayed. Inspect pending debt, worker throughput, RPS and retries.')
    elif ver=='V522_DECODER_QUEUE_DOMINANT':
        print('  Acquisition is timely; V522 processing dominates.')
    else:
        print('  No single stage dominates strongly; instrument live receive/claim/store timestamps before changing capacity.')
    print('  Do not alter strategy fill windows from this diagnostic.')
    def ins(stage,xs):
        if not xs:return
        o.execute('INSERT OR REPLACE INTO stage_summary VALUES(?,?,?,?,?,?,?,?)',(run,stage,len(xs),pct(xs,.5),pct(xs,.9),pct(xs,.95),pct(xs,.99),max(xs)))
    for st,xs in [('CHAIN_QUEUE',chain_queue),('QUEUE_STORE',queue_store),('CHAIN_STORE',chain_store),('STORE_PROCESSED',store_processed),('CHAIN_PROCESSED',chain_processed)]:ins(st,xs)
    for k,d in bykind.items():o.execute('INSERT OR REPLACE INTO kind_summary VALUES(?,?,?,?,?,?,?)',(run,k,len(d['qs']),pct(d['qs'],.5),pct(d['qs'],.9),pct(d['cq'],.5),pct(d['cq'],.9)))
    o.execute('INSERT INTO audit_run VALUES(?,?,?,?,?,?,?)',(run,time.time(),len(rows),jq,jp,ver,'raw observed_at is post-enrichment store time; queue.first_seen used as earliest durable receipt'))
    o.commit();o.close();a.close();
    if b:b.close()
    print(f'OUTPUT={OUT}')
    print('Guardrail: timing diagnosis only; no strategy retuning, no queue mutation, no capital decision.')
if __name__=='__main__':main()
