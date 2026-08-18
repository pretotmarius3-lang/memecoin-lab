#!/usr/bin/env python3
"""MEMECOIN LAB — RAW PIPELINE LATENCY ROOT-CAUSE AUDIT V7.4.5

READ-ONLY diagnostic over V5 raw events + V52 processed/swaps.
No strategy, threshold, arena, snapshot, or verdict is modified.

Goal
----
Decompose end-to-end causal delay into observable stages:
  chain blockTime -> raw observed_at -> v52_processed.processed_at -> v52_swaps.observed_at

Notes
-----
- v52_swaps.observed_at is copied from the raw row observation time, so the gap
  blockTime->swap.observed_at measures source/raw ingest latency, not decoder CPU time.
- processed_at - raw observed_at approximates decoder queue/processing delay.
- This script decodes raw payloads only to recover blockTime; it never writes to V5/V52.
- Negative or absurd lags are reported as clock/data anomalies and excluded from percentiles.

Paper infrastructure audit only.
"""
from __future__ import annotations
import math, sqlite3, time
from pathlib import Path
import v52_decode_features as old

ROOT=Path.home()/"memecoin_lab"
V5=ROOT/'v5_raw_events.db'
V52=ROOT/'v52_features.db'
OUT=ROOT/'v745_raw_pipeline_latency.db'
MAX_ROWS=20000


def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=60);d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=60000');return d

def odb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=30000');return d

def sf(x,d=None):
    try:
        v=float(x);return v if math.isfinite(v) else d
    except:return d

def pct(xs,q):
    if not xs:return None
    ys=sorted(xs);p=(len(ys)-1)*q;lo=int(math.floor(p));hi=int(math.ceil(p))
    return ys[lo] if lo==hi else ys[lo]+(ys[hi]-ys[lo])*(p-lo)
def fmt(x):return 'NA' if x is None else f'{x:.1f}s'

def init():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS audit_run(
      run_id TEXT PRIMARY KEY,created_at REAL,rows_examined INTEGER,valid_blocktime INTEGER,note TEXT);
    CREATE TABLE IF NOT EXISTS stage_summary(
      run_id TEXT,stage TEXT,n INTEGER,p50 REAL,p90 REAL,p95 REAL,p99 REAL,max REAL,
      le1 REAL,le5 REAL,le15 REAL,le30 REAL,le60 REAL,PRIMARY KEY(run_id,stage));
    CREATE TABLE IF NOT EXISTS event_sample(
      run_id TEXT,signature TEXT,event_hint TEXT,block_time REAL,raw_observed_at REAL,processed_at REAL,
      swap_observed_at REAL,raw_ingest_lag REAL,decoder_lag REAL,total_to_processed REAL,status TEXT,reason TEXT);
    ''');d.commit();d.close()

def summarize(o,run_id,name,xs):
    n=len(xs)
    vals=(run_id,name,n,pct(xs,.50),pct(xs,.90),pct(xs,.95),pct(xs,.99),max(xs) if xs else None,
          100*sum(x<=1 for x in xs)/n if n else 0,100*sum(x<=5 for x in xs)/n if n else 0,
          100*sum(x<=15 for x in xs)/n if n else 0,100*sum(x<=30 for x in xs)/n if n else 0,
          100*sum(x<=60 for x in xs)/n if n else 0)
    o.execute('INSERT INTO stage_summary VALUES('+','.join('?'*13)+')',vals)
    print(f'{name:<34} n={n:6d} p50={fmt(pct(xs,.50)):>8} p90={fmt(pct(xs,.90)):>8} p95={fmt(pct(xs,.95)):>8} p99={fmt(pct(xs,.99)):>8} max={fmt(max(xs) if xs else None):>9}')
    if n:print(f'  <=1/5/15/30/60s = {vals[8]:5.1f}/{vals[9]:5.1f}/{vals[10]:5.1f}/{vals[11]:5.1f}/{vals[12]:5.1f}%')

def main():
    if not V5.exists():raise SystemExit(f'Missing {V5}')
    if not V52.exists():raise SystemExit(f'Missing {V52}')
    init();r=ro(V5);f=ro(V52);o=odb();run_id=f'R745_{int(time.time())}'
    # Focus on rows that the decoder has already seen; newest first keeps audit relevant.
    rows=r.execute('''SELECT signature,observed_at,event_hint,payload_zlib FROM v5_raw_transactions ORDER BY observed_at DESC LIMIT ?''',(MAX_ROWS,)).fetchall()
    raw_lag=[];decoder_lag=[];total_proc=[];swap_raw_lag=[];valid=0;decode_errors=0;clock_bad=0;samples=[]
    for row in rows:
        sig=row['signature'];obs=sf(row['observed_at']);bt=None
        try:
            p=old.decode_payload(row);tx=p.get('rpc_transaction') or {};bt=sf(tx.get('blockTime') or p.get('blockTime'))
        except Exception:
            decode_errors+=1
        pr=f.execute('SELECT processed_at,status,reason FROM v52_processed WHERE signature=?',(sig,)).fetchone()
        sw=f.execute('SELECT observed_at,timestamp FROM v52_swaps WHERE signature=?',(sig,)).fetchone()
        proc=sf(pr['processed_at']) if pr else None;swobs=sf(sw['observed_at']) if sw else None
        ril=dl=tp=None
        if bt is not None and obs is not None:
            ril=obs-bt
            if 0<=ril<3600:raw_lag.append(ril);valid+=1
            else:clock_bad+=1
        if proc is not None and obs is not None:
            dl=proc-obs
            if 0<=dl<3600:decoder_lag.append(dl)
        if bt is not None and proc is not None:
            tp=proc-bt
            if 0<=tp<7200:total_proc.append(tp)
        if bt is not None and swobs is not None:
            q=swobs-bt
            if 0<=q<3600:swap_raw_lag.append(q)
        if len(samples)<100 and (ril is not None or dl is not None):
            samples.append((run_id,sig,row['event_hint'],bt,obs,proc,swobs,ril,dl,tp,pr['status'] if pr else None,pr['reason'] if pr else None))
    o.execute('INSERT INTO audit_run VALUES(?,?,?,?,?)',(run_id,time.time(),len(rows),valid,'blockTime decoded from raw payload; V5/V52 read-only'))
    print('='*178);print('MEMECOIN LAB — RAW PIPELINE LATENCY ROOT-CAUSE AUDIT V7.4.5');print('='*178)
    print(f'rows_examined={len(rows)} valid_blocktime={valid} decode_errors={decode_errors} clock/data_anomalies={clock_bad} | READ-ONLY\n')
    summarize(o,run_id,'CHAIN -> RAW observed_at',raw_lag)
    summarize(o,run_id,'RAW observed_at -> PROCESSED',decoder_lag)
    summarize(o,run_id,'CHAIN -> PROCESSED total',total_proc)
    summarize(o,run_id,'CHAIN -> SWAP observed_at',swap_raw_lag)
    for x in samples:o.execute('INSERT INTO event_sample VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',x)
    # Root-cause classification based on medians and p90s.
    raw50=pct(raw_lag,.50) or 0;dec50=pct(decoder_lag,.50) or 0;raw90=pct(raw_lag,.90) or 0;dec90=pct(decoder_lag,.90) or 0
    print('\nROOT-CAUSE INTERPRETATION')
    if raw50>30 and raw50>max(2*dec50,10):
        verdict='UPSTREAM_RAW_INGEST_DOMINANT'
        print('  Dominant delay is BEFORE V522: chain event -> raw observed_at.')
        print('  Investigate Alchemy/RPC delivery, collector backlog, websocket/reconnect behavior, and raw writer throughput.')
    elif dec50>30 and dec50>max(2*raw50,10):
        verdict='V522_DECODER_QUEUE_DOMINANT'
        print('  Dominant delay is AFTER raw capture: raw observed_at -> processed_at.')
        print('  Investigate V522 backlog, CPU saturation, batch ordering, SQLite write contention, and duplicate decoder processes.')
    elif raw50>15 or dec50>15:
        verdict='MIXED_PIPELINE_LATENCY'
        print('  Both upstream ingest and decoder/queue latency materially exceed the 15s execution budget.')
    else:
        verdict='PIPELINE_MEDIAN_WITHIN_BUDGET_CHECK_TAILS'
        print('  Median pipeline timing is acceptable; inspect p90/p95 tails and feature construction next.')
    print(f'  VERDICT={verdict} | raw p50/p90={raw50:.1f}/{raw90:.1f}s | decoder p50/p90={dec50:.1f}/{dec90:.1f}s')
    print('\nNEXT')
    print('  If UPSTREAM_RAW_INGEST_DOMINANT: instrument the raw collector itself with receive-time/backlog counters before changing any strategy.')
    print('  If V522_DECODER_QUEUE_DOMINANT: profile fetch_new backlog and per-batch service time; do not widen fill windows.')
    print('  Any infrastructure fix requires a brand-new future-only arena; old evidence stays quarantined.')
    o.commit();o.close();r.close();f.close();print(f'OUTPUT={OUT}')
    print('Guardrail: infrastructure diagnosis only; no strategy retuning or capital decision.')

if __name__=='__main__':main()
