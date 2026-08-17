#!/usr/bin/env python3
"""Memecoin Lab V5.3.1 — isolated historical catch-up lane.

Uses the existing V5.3 implementation but claims only signatures older than the
LIVE protection window. Run beside v511_live_priority_collector.py.
"""
from __future__ import annotations

import asyncio
import os
import signal
import time

import v53_catchup_worker as base

LIVE_PROTECT_S=float(os.environ.get('MEMECOIN_V531_LIVE_PROTECT_S','180'))


def catchup_claim(limit):
    c=base.db(); now=time.time(); cutoff=now-LIVE_PROTECT_S; c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        rows=c.execute("""SELECT * FROM v51_signature_spool
                          WHERE status='PENDING' AND attempts<? AND first_seen<?
                          ORDER BY priority ASC, first_seen ASC LIMIT ?""",
                       (base.MAX_RETRIES,cutoff,int(limit))).fetchall()
        out=[]
        for r in rows:
            cur=c.execute("""UPDATE v51_signature_spool
                             SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=?
                             WHERE signature=? AND status='PENDING'""",
                          (now+base.LEASE,now,r['signature']))
            if cur.rowcount==1:
                x=dict(r); x['attempts']=int(r['attempts'])+1; out.append(x)
        c.commit(); return out
    except BaseException:
        c.rollback(); raise
    finally:
        c.close()


base.claim=catchup_claim

if __name__=='__main__':
    print(f'V5.3.1 CATCH-UP lane | protects newest {LIVE_PROTECT_S:.0f}s from historical drain',flush=True)
    signal.signal(signal.SIGINT,base.stop); signal.signal(signal.SIGTERM,base.stop)
    try:
        asyncio.run(base.main())
    finally:
        if base.DB_PATH.exists():
            c=base.db(); c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),)); c.commit(); c.close()
