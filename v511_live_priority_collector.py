#!/usr/bin/env python3
"""Memecoin Lab V5.1.2 — LIVE anti-starvation lane.

Wrapper around v51_spool_collector.py.

Why this exists
---------------
The previous LIVE wrapper claimed the newest pending signature first. Under
sustained load that can starve signatures that are only 10–30 seconds old,
which destroys early-token temporal coverage (for example price_velocity at
stage=20s). Those signatures eventually fall out of the LIVE window and join
the huge historical backlog.

V5.1.2 keeps the same protected LIVE window but drains it FIFO within event
priority. This preserves temporal continuity while V5.3.1 remains responsible
for historical catch-up.

Research-only. No change to V5.2 feature definitions or V6.x frozen rules.
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
        db.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))

        # Anti-starvation policy:
        # 1) keep CREATE/MIGRATE/BUY/SELL event priority semantics
        # 2) among equal-priority LIVE work, process OLDEST first
        # This is intentionally different from the old newest-first policy.
        row=db.execute("""SELECT * FROM v51_signature_spool
                          WHERE status='PENDING' AND attempts<? AND first_seen>=?
                          ORDER BY priority ASC, first_seen ASC LIMIT 1""",
                       (base.MAX_RETRIES,cutoff)).fetchone()
        if row is None:
            db.commit(); db.close(); return None

        cur=db.execute("""UPDATE v51_signature_spool
                          SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=?
                          WHERE signature=? AND status='PENDING'""",
                       (now+base.CLAIM_LEASE_S,now,row['signature']))
        if cur.rowcount!=1:
            db.rollback(); db.close(); return None

        out=dict(row)
        out['attempts']=int(row['attempts'])+1
        out['_live_claim_age_s']=max(0.0,now-float(row['first_seen']))

        # Lightweight telemetry for dashboard/diagnostics.
        try:
            base.set_state(db,'v511_scheduler',{
                'mode':'LIVE_FIFO_ANTI_STARVATION',
                'window_s':LIVE_WINDOW_S,
                'last_claim_age_s':out['_live_claim_age_s'],
                'last_claim_event':out.get('event_hint'),
                'updated_at':now,
            })
        except Exception:
            pass

        db.commit(); db.close(); return out
    except BaseException:
        db.rollback(); db.close(); raise


base.claim_spool=live_claim_spool

if __name__=='__main__':
    print(f'V5.1.2 LIVE lane | recent_window={LIVE_WINDOW_S:.0f}s | FIFO anti-starvation scheduling',flush=True)
    base.main()
