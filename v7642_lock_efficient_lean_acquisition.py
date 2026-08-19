#!/usr/bin/env python3
"""MEMECOIN LAB V7.6.4.2 — LOCK-EFFICIENT LEAN ACQUISITION

Infrastructure-only successor to V7.6.4.1.

Fixes two remaining hot-path problems in the V7.5/V7.6 claimer:
1) 64 worker threads were all competing for BEGIN IMMEDIATE on the same SQLite
   queue, while each claim also executed a HOT COUNT and expired-lease UPDATE.
2) V7.6.4 called v750.claim_one_burst(), which still wrote a per-claim row into
   v750_acquisition_trace.db even though V7.6.4 was intended to be trace-free.

This version:
- keeps the raw DB persistently in WAL mode without renegotiating journal_mode;
- serializes only the tiny claim transaction with an in-process threading.Lock;
- reaps expired leases at most once per second instead of on every claim;
- tests HOT>=priority with LIMIT/OFFSET instead of COUNT(*);
- removes all per-claim trace writes;
- preserves 64 HTTP workers, burst HOT priority, admission policy and controller.

Research infrastructure only. Never signs or submits transactions.
"""
from __future__ import annotations
import asyncio, sqlite3, threading, time

import v764_lean_burst_acquisition as v764
import v750_burst_resilient_alchemy_engine as v750

base=v764.base
CLAIM_LOCK=threading.Lock()
LAST_REAP=0.0


def safe_db():
    c=sqlite3.connect(base.DB_PATH,timeout=30)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA synchronous=NORMAL')
    c.execute('PRAGMA busy_timeout=30000')
    return c

base.db=safe_db


def claim_one_lock_efficient():
    global LAST_REAP
    with CLAIM_LOCK:
        c=base.db(); now=time.time()
        try:
            c.execute('BEGIN IMMEDIATE')
            if now-LAST_REAP >= 1.0:
                c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
                LAST_REAP=now

            # We only need to know whether there are at least N pending HOT rows.
            # OFFSET N-1 avoids scanning/counting the entire HOT backlog every claim.
            n=max(1,int(v750.BURST_HOT_PRIORITY))
            hot_enough=c.execute(
                "SELECT 1 FROM v515_hot_queue WHERE status='PENDING' AND kind='HOT' LIMIT 1 OFFSET ?",
                (n-1,),
            ).fetchone() is not None
            order="CASE kind WHEN 'HOT' THEN 0 ELSE 1 END" if hot_enough else "CASE kind WHEN 'CREATE' THEN 0 ELSE 1 END"
            r=c.execute(f"""SELECT * FROM v515_hot_queue
                WHERE status='PENDING' AND attempts<?
                ORDER BY {order}, first_seen ASC LIMIT 1""",(base.MAX_RETRIES,)).fetchone()
            if not r:
                c.commit(); return None
            cur=c.execute("""UPDATE v515_hot_queue
                SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=?
                WHERE signature=? AND status='PENDING'""",
                (now+base.LEASE_S,now,r['signature']))
            if cur.rowcount!=1:
                c.rollback(); return None
            x=dict(r);x['attempts']=int(r['attempts'])+1
            c.commit()
            return x
        except BaseException:
            try:c.rollback()
            except Exception:pass
            raise
        finally:
            c.close()

base.claim_one=claim_one_lock_efficient

async def main():
    print('MEMECOIN LAB V7.6.4.2 lock-efficient lean burst acquisition',flush=True)
    print('claim path: serialized tiny txn | no per-claim trace | lease reap <=1Hz | no HOT COUNT(*)',flush=True)
    await v764.main()

if __name__=='__main__':
    asyncio.run(main())
