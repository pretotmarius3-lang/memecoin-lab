#!/usr/bin/env python3
"""MEMECOIN LAB — LIVE ACQUISITION INSTRUMENTATION V7.4.7

Operationally equivalent capacity profile to V5.1.7.6, with sidecar timing only.
It instruments the live acquisition path without changing research rules:
WS/enqueue -> first claim -> HTTP start -> HTTP end -> raw store.

IMPORTANT:
- Run this INSTEAD OF v5176_alchemy_high_capacity_engine.py, never alongside it.
- Strategy rules / V52 schemas are untouched.
- Timing is written to v747_acquisition_trace.db.
- enqueue_at is the closest durable timestamp to websocket handling available
  without changing the provider protocol itself.

Research/infrastructure diagnostics only. Never signs or submits transactions.
"""
from __future__ import annotations

import asyncio, os, sqlite3, statistics, time
from pathlib import Path

# Exact V5.1.7.6 operating envelope.
os.environ.setdefault('MEMECOIN_V517_BASE_SAMPLE_MOD','1')
os.environ.setdefault('MEMECOIN_V517_HOT_TTL_S','180')
os.environ.setdefault('MEMECOIN_V517_MAX_HOT','128')
os.environ.setdefault('MEMECOIN_V517_WORKERS','32')
os.environ.setdefault('MEMECOIN_V517_BASE_RPS','20')
os.environ.setdefault('MEMECOIN_V517_MAX_RPS','40')

import v5171_alchemy_prospective_engine as entry
base = entry.base

ROOT = Path.home()/"memecoin_lab"
TRACE = ROOT/'v747_acquisition_trace.db'
REPORT_S = float(os.environ.get('MEMECOIN_V747_REPORT_S','15'))


def tdb():
    d=sqlite3.connect(TRACE,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL')
    d.execute('PRAGMA synchronous=NORMAL')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def init_trace():
    d=tdb(); d.executescript('''
    CREATE TABLE IF NOT EXISTS trace(
      signature TEXT PRIMARY KEY,
      epoch_id TEXT,
      kind TEXT,
      mint TEXT,
      enqueue_at REAL,
      first_claim_at REAL,
      http_start_at REAL,
      http_end_at REAL,
      http_state TEXT,
      raw_store_at REAL,
      updated_at REAL
    );
    CREATE INDEX IF NOT EXISTS idx_trace_kind_enqueue ON trace(kind,enqueue_at);
    CREATE TABLE IF NOT EXISTS queue_sample(
      sampled_at REAL PRIMARY KEY,
      epoch_id TEXT,
      pending_total INTEGER,
      pending_create INTEGER,
      pending_hot INTEGER,
      fetching INTEGER,
      oldest_pending_age_s REAL,
      current_rps REAL
    );
    '''); d.commit(); d.close()


def up(sig, **kw):
    if not sig:return
    d=tdb(); now=time.time()
    d.execute('INSERT OR IGNORE INTO trace(signature,epoch_id,updated_at) VALUES(?,?,?)',(sig,base.EPOCH_ID,now))
    if kw:
        cols=[]; vals=[]
        for k,v in kw.items(): cols.append(f'{k}=?'); vals.append(v)
        vals += [now,sig]
        d.execute('UPDATE trace SET '+','.join(cols)+',updated_at=? WHERE signature=?',vals)
    d.commit(); d.close()


_orig_enqueue = base.enqueue
_orig_claim = base.claim_one
_orig_rpc = base.rpc_get_tx
_orig_store = base.store_tx


def enqueue_instrumented(sig,mint,kind,source,slot,logs,mod=None):
    now=time.time()
    added=_orig_enqueue(sig,mint,kind,source,slot,logs,mod)
    if added:
        up(sig,kind=kind,mint=mint,enqueue_at=now)
    return added


def claim_instrumented():
    x=_orig_claim()
    if x:
        d=tdb(); now=time.time(); sig=x.get('signature')
        d.execute('INSERT OR IGNORE INTO trace(signature,epoch_id,kind,mint,updated_at) VALUES(?,?,?,?,?)',(sig,base.EPOCH_ID,x.get('kind'),x.get('mint'),now))
        d.execute('UPDATE trace SET first_claim_at=COALESCE(first_claim_at,?),updated_at=? WHERE signature=?',(now,now,sig))
        d.commit(); d.close()
    return x


async def rpc_instrumented(session,sig):
    start=time.time(); up(sig,http_start_at=start)
    state,tx=await _orig_rpc(session,sig)
    up(sig,http_end_at=time.time(),http_state=state)
    return state,tx


def store_instrumented(x,tx):
    out=_orig_store(x,tx)
    up(x.get('signature'),raw_store_at=time.time())
    return out


base.enqueue=enqueue_instrumented
base.claim_one=claim_instrumented
base.rpc_get_tx=rpc_instrumented
base.store_tx=store_instrumented


def qstats():
    c=base.db(); now=time.time()
    total=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0])
    pc=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING' AND kind='CREATE'").fetchone()[0])
    ph=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING' AND kind='HOT'").fetchone()[0])
    fetching=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='FETCHING'").fetchone()[0])
    oldest=c.execute("SELECT MIN(first_seen) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0]
    age=max(0.0,now-float(oldest)) if oldest is not None else 0.0
    c.close(); return now,total,pc,ph,fetching,age


def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def recent_latency(kind,limit=2000):
    d=tdb(); rs=d.execute('''SELECT enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at
      FROM trace WHERE kind=? AND raw_store_at IS NOT NULL ORDER BY raw_store_at DESC LIMIT ?''',(kind,limit)).fetchall(); d.close()
    qclaim=[]; http=[]; total=[]
    for r in rs:
        if r['enqueue_at'] is not None and r['first_claim_at'] is not None:qclaim.append(max(0,float(r['first_claim_at'])-float(r['enqueue_at'])))
        if r['http_start_at'] is not None and r['http_end_at'] is not None:http.append(max(0,float(r['http_end_at'])-float(r['http_start_at'])))
        if r['enqueue_at'] is not None and r['raw_store_at'] is not None:total.append(max(0,float(r['raw_store_at'])-float(r['enqueue_at'])))
    return len(rs),qclaim,http,total


async def reporter():
    while not base.STOP.is_set():
        await asyncio.sleep(REPORT_S)
        try:
            now,total,pc,ph,fetching,age=await asyncio.to_thread(qstats)
            d=tdb(); d.execute('INSERT OR REPLACE INTO queue_sample VALUES(?,?,?,?,?,?,?,?)',(now,base.EPOCH_ID,total,pc,ph,fetching,age,float(base.CURRENT_RPS))); d.commit(); d.close()
            print('\n===== V7.4.7 LIVE ACQUISITION LATENCY =====',flush=True)
            print(f'epoch={base.EPOCH_ID} rps={base.CURRENT_RPS:.1f} pending={total} CREATE={pc} HOT={ph} fetching={fetching} oldest_pending={age:.1f}s',flush=True)
            for kind in ('CREATE','HOT'):
                n,qc,ht,tt=await asyncio.to_thread(recent_latency,kind)
                def f(x,q): return pct(x,q) if x else 0.0
                print(f'{kind:<6} completed={n:4d} enqueue->claim p50/p90={f(qc,.5):6.2f}/{f(qc,.9):6.2f}s | HTTP p50/p90={f(ht,.5):5.2f}/{f(ht,.9):5.2f}s | enqueue->store p50/p90={f(tt,.5):6.2f}/{f(tt,.9):6.2f}s',flush=True)
        except Exception as e:
            print(f'V747 reporter error: {e!r}',flush=True)


async def main():
    init_trace()
    print('MEMECOIN LAB V7.4.7 instrumented acquisition | SAME V5.1.7.6 capacity | sidecar timing only',flush=True)
    print(f'TRACE={TRACE} | epoch={base.EPOCH_ID} | commitment={base.COMMITMENT}',flush=True)
    await asyncio.gather(base.main(),reporter())


if __name__=='__main__':
    asyncio.run(main())
