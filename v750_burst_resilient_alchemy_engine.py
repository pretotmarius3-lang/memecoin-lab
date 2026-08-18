#!/usr/bin/env python3
"""MEMECOIN LAB — BURST-RESILIENT ALCHEMY ACQUISITION V7.5.0

Infrastructure-only successor to V7.4.7 / V5.1.7.6.

Goal
----
Keep the prospective acquisition path causal during HOT bursts without changing
strategy rules or importing old evidence.

Changes vs V5.1.7.6
-------------------
- 64 workers (was 32)
- 30 base RPS / 60 max RPS (was 20 / 40)
- controller reacts every 2s instead of every 15s
- debt/oldest-pending based step-up to high RPS
- HOT gets queue priority while HOT backlog >= 25; CREATE keeps priority otherwise
- CREATE admission throttles earlier when queue debt rises
- live sidecar trace identical in spirit to V7.4.7

Scientific note
---------------
This changes acquisition infrastructure/capacity only. Any post-change strategy
evidence requires a fresh future-only arena and cutoff. Never run this alongside
V7.4.7 or V5.1.7.6 because they share the same acquisition DB/queue.

Research only. Never signs or submits transactions.
"""
from __future__ import annotations

import asyncio, os, sqlite3, time
from pathlib import Path

# Set capacity BEFORE importing the base engine.
os.environ.setdefault('MEMECOIN_V517_BASE_SAMPLE_MOD','1')
os.environ.setdefault('MEMECOIN_V517_HOT_TTL_S','180')
os.environ.setdefault('MEMECOIN_V517_MAX_HOT','128')
os.environ.setdefault('MEMECOIN_V517_WORKERS','64')
os.environ.setdefault('MEMECOIN_V517_BASE_RPS','30')
os.environ.setdefault('MEMECOIN_V517_MAX_RPS','60')
os.environ.setdefault('MEMECOIN_V517_REPORT_S','10')

import v5171_alchemy_prospective_engine as entry
base = entry.base

ROOT = Path.home()/"memecoin_lab"
TRACE = ROOT/'v750_acquisition_trace.db'
REPORT_S = float(os.environ.get('MEMECOIN_V750_REPORT_S','10'))
BURST_HOT_PRIORITY = int(os.environ.get('MEMECOIN_V750_HOT_PRIORITY_AT','25'))


def tdb():
    d=sqlite3.connect(TRACE,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL')
    d.execute('PRAGMA synchronous=NORMAL')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def init_trace():
    d=tdb();d.executescript('''
    CREATE TABLE IF NOT EXISTS trace(
      signature TEXT PRIMARY KEY, epoch_id TEXT, kind TEXT, mint TEXT,
      enqueue_at REAL, first_claim_at REAL, http_start_at REAL, http_end_at REAL,
      http_state TEXT, raw_store_at REAL, updated_at REAL);
    CREATE INDEX IF NOT EXISTS idx_v750_trace_kind ON trace(kind,raw_store_at);
    CREATE TABLE IF NOT EXISTS queue_sample(
      sampled_at REAL PRIMARY KEY, epoch_id TEXT, pending_total INTEGER,
      pending_create INTEGER, pending_hot INTEGER, fetching INTEGER,
      oldest_pending_age_s REAL, current_rps REAL, admission_mod INTEGER,
      recent_429 INTEGER);
    ''');d.commit();d.close()


def up(sig, **kw):
    if not sig:return
    d=tdb();now=time.time()
    d.execute('INSERT OR IGNORE INTO trace(signature,epoch_id,updated_at) VALUES(?,?,?)',(sig,base.EPOCH_ID,now))
    if kw:
        cols=[];vals=[]
        for k,v in kw.items():cols.append(f'{k}=?');vals.append(v)
        vals += [now,sig]
        d.execute('UPDATE trace SET '+','.join(cols)+',updated_at=? WHERE signature=?',vals)
    d.commit();d.close()


# ---------------- instrumentation ----------------
_orig_enqueue=base.enqueue
_orig_rpc=base.rpc_get_tx
_orig_store=base.store_tx


def enqueue_instrumented(sig,mint,kind,source,slot,logs,mod=None):
    now=time.time();added=_orig_enqueue(sig,mint,kind,source,slot,logs,mod)
    if added:up(sig,kind=kind,mint=mint,enqueue_at=now)
    return added


async def rpc_instrumented(session,sig):
    up(sig,http_start_at=time.time())
    state,tx=await _orig_rpc(session,sig)
    up(sig,http_end_at=time.time(),http_state=state)
    return state,tx


def store_instrumented(x,tx):
    out=_orig_store(x,tx);up(x.get('signature'),raw_store_at=time.time());return out


# ---------------- burst-aware queue ----------------
def queue_snapshot():
    c=base.db();now=time.time()
    q=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0])
    pc=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING' AND kind='CREATE'").fetchone()[0])
    ph=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING' AND kind='HOT'").fetchone()[0])
    fetching=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='FETCHING'").fetchone()[0])
    oldest=c.execute("SELECT MIN(first_seen) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0]
    age=max(0.0,now-float(oldest)) if oldest is not None else 0.0
    c.close();return q,pc,ph,fetching,age


def admission_mod_burst():
    q,pc,ph,fetching,age=queue_snapshot()
    debt=q/max(.2,float(base.CURRENT_RPS))
    m=max(1,int(base.BASE_SAMPLE_MOD))
    if debt>=45 or age>=45:return None
    if debt>=20 or age>=20:return m*8
    if debt>=10 or age>=10:return m*4
    if debt>=5 or age>=5:return m*2
    return m


def claim_one_burst():
    c=base.db();now=time.time();c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        ph=int(c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING' AND kind='HOT'").fetchone()[0])
        if ph>=BURST_HOT_PRIORITY:
            order="CASE kind WHEN 'HOT' THEN 0 ELSE 1 END"
        else:
            order="CASE kind WHEN 'CREATE' THEN 0 ELSE 1 END"
        r=c.execute(f"""SELECT * FROM v515_hot_queue WHERE status='PENDING' AND attempts<?
          ORDER BY {order}, first_seen ASC LIMIT 1""",(base.MAX_RETRIES,)).fetchone()
        if not r:c.commit();return None
        cur=c.execute("UPDATE v515_hot_queue SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+base.LEASE_S,now,r['signature']))
        if cur.rowcount!=1:c.rollback();return None
        x=dict(r);x['attempts']=int(r['attempts'])+1;c.commit()
        up(x.get('signature'),kind=x.get('kind'),mint=x.get('mint'),first_claim_at=now)
        return x
    finally:c.close()


async def controller_burst():
    """Fast debt controller. 429 always wins; otherwise jump directly to required tier."""
    while not base.STOP.is_set():
        await asyncio.sleep(2)
        q,pc,ph,fetching,age=await asyncio.to_thread(queue_snapshot)
        n429=int(base.RECENT_429);base.RECENT_429=0
        cur=float(base.CURRENT_RPS)
        if n429:
            target=max(float(base.BASE_RPS),cur*.75)
        elif q>=1000 or age>=25:
            target=float(base.MAX_RPS)
        elif q>=500 or age>=15:
            target=min(float(base.MAX_RPS),55.0)
        elif q>=200 or age>=8:
            target=min(float(base.MAX_RPS),50.0)
        elif q>=75 or age>=4:
            target=min(float(base.MAX_RPS),45.0)
        elif q>=20 or age>=2:
            target=min(float(base.MAX_RPS),38.0)
        elif q==0 and fetching<8:
            target=float(base.BASE_RPS)
        else:
            target=max(float(base.BASE_RPS),min(cur,35.0))
        base.CURRENT_RPS=target
        base.bump('noop',0)


def pct(xs,q):
    if not xs:return 0.0
    ys=sorted(xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def recent_latency(kind,limit=3000):
    d=tdb();rs=d.execute('''SELECT enqueue_at,first_claim_at,http_start_at,http_end_at,raw_store_at
      FROM trace WHERE epoch_id=? AND kind=? AND raw_store_at IS NOT NULL ORDER BY raw_store_at DESC LIMIT ?''',(base.EPOCH_ID,kind,limit)).fetchall();d.close()
    qc=[];ht=[];tt=[]
    for r in rs:
        if r['enqueue_at'] is not None and r['first_claim_at'] is not None:qc.append(max(0,float(r['first_claim_at'])-float(r['enqueue_at'])))
        if r['http_start_at'] is not None and r['http_end_at'] is not None:ht.append(max(0,float(r['http_end_at'])-float(r['http_start_at'])))
        if r['enqueue_at'] is not None and r['raw_store_at'] is not None:tt.append(max(0,float(r['raw_store_at'])-float(r['enqueue_at'])))
    return len(rs),qc,ht,tt


async def reporter_burst():
    while not base.STOP.is_set():
        await asyncio.sleep(REPORT_S)
        try:
            q,pc,ph,fetching,age=await asyncio.to_thread(queue_snapshot)
            mod=await asyncio.to_thread(admission_mod_burst)
            d=tdb();d.execute('INSERT OR REPLACE INTO queue_sample VALUES(?,?,?,?,?,?,?,?,?,?)',(
                time.time(),base.EPOCH_ID,q,pc,ph,fetching,age,float(base.CURRENT_RPS),mod,int(base.RECENT_429)))
            d.commit();d.close()
            print('\n===== V7.5.0 BURST-RESILIENT ACQUISITION =====',flush=True)
            print(f'epoch={base.EPOCH_ID} rps={base.CURRENT_RPS:.1f} pending={q} CREATE={pc} HOT={ph} fetching={fetching} oldest={age:.1f}s admission={"PAUSE" if mod is None else "1/"+str(mod)}',flush=True)
            for kind in ('CREATE','HOT'):
                n,qc,ht,tt=await asyncio.to_thread(recent_latency,kind)
                print(f'{kind:<6} completed={n:4d} claim p50/p90={pct(qc,.5):6.2f}/{pct(qc,.9):6.2f}s | HTTP={pct(ht,.5):5.2f}/{pct(ht,.9):5.2f}s | total={pct(tt,.5):6.2f}/{pct(tt,.9):6.2f}s',flush=True)
        except Exception as e:print(f'V750 reporter error: {e!r}',flush=True)


# Install patches.
base.enqueue=enqueue_instrumented
base.rpc_get_tx=rpc_instrumented
base.store_tx=store_instrumented
base.claim_one=claim_one_burst
base.admission_mod=admission_mod_burst
base.controller=controller_burst


async def main():
    init_trace()
    print('MEMECOIN LAB V7.5.0 burst-resilient acquisition | infrastructure experiment',flush=True)
    print(f'TRACE={TRACE} | epoch={base.EPOCH_ID} | commitment={base.COMMITMENT} | workers={base.WORKERS} rps={base.BASE_RPS}->{base.MAX_RPS}',flush=True)
    await asyncio.gather(base.main(),reporter_burst())


if __name__=='__main__':
    asyncio.run(main())
