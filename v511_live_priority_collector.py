#!/usr/bin/env python3
"""Memecoin Lab V5.1.1 — LIVE priority lane.

Wrapper around v51_spool_collector.py.
Only enriches very recent signatures; older PENDING rows are left untouched for
V5.3.1 catch-up. The WebSocket subscription itself is unchanged.

Run beside v531_catchup_worker.py.
"""
from __future__ import annotations

import os
import time

import v51_spool_collector as base

LIVE_WINDOW_S=float(os.environ.get('MEMECOIN_V511_LIVE_WINDOW_S','180'))


def live_claim_spool(worker):
    now=time.time(); cutoff=now-LIVE_WINDOW_S
    db=base.open_db(); db.execute('BEGIN IMMEDIATE')
    try:
        # Recover any abandoned leases globally; lane ownership is re-decided
        # from first_seen on the next claim.
        db.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        row=db.execute("""SELECT * FROM v51_signature_spool
                          WHERE status='PENDING' AND attempts<? AND first_seen>=?
                          ORDER BY first_seen DESC, priority ASC LIMIT 1""",
                       (base.MAX_RETRIES,cutoff)).fetchone()
        if row is None:
            db.commit(); db.close(); return None
        cur=db.execute("""UPDATE v51_signature_spool
                          SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=?
                          WHERE signature=? AND status='PENDING'""",
                       (now+base.CLAIM_LEASE_S,now,row['signature']))
        if cur.rowcount!=1:
            db.rollback(); db.close(); return None
        out=dict(row); out['attempts']=int(row['attempts'])+1
        db.commit(); db.close(); return out
    except BaseException:
        db.rollback(); db.close(); raise


base.claim_spool=live_claim_spool

if __name__=='__main__':
    print(f'V5.1.1 LIVE lane | recent_window={LIVE_WINDOW_S:.0f}s | newest-first enrichment',flush=True)
    base.main()
