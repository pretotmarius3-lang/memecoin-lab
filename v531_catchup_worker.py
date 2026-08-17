#!/usr/bin/env python3
"""Memecoin Lab V5.3.1 — isolated historical catch-up lane.

Uses the existing V5.3 implementation but claims only signatures older than the
LIVE protection window. Run beside v511_live_priority_collector.py.

V5.3.1 also hardens SQLite writes against contention with the live lane:
- async write serialization inside this worker
- retry/backoff on SQLITE_BUSY / database locked
- short transactions only
- a failed diagnostic/reset write can no longer kill the whole worker
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import time
import zlib

import v53_catchup_worker as base

LIVE_PROTECT_S=float(os.environ.get('MEMECOIN_V531_LIVE_PROTECT_S','180'))
DB_RETRIES=int(os.environ.get('MEMECOIN_V531_DB_RETRIES','12'))
DB_BACKOFF=float(os.environ.get('MEMECOIN_V531_DB_BACKOFF','0.05'))
WRITE_LOCK=None


def is_locked(exc):
    return isinstance(exc,sqlite3.OperationalError) and ('locked' in str(exc).lower() or 'busy' in str(exc).lower())


def with_db_retry(fn):
    last=None
    for attempt in range(DB_RETRIES):
        try:
            return fn()
        except Exception as exc:
            last=exc
            if not is_locked(exc):
                raise
            time.sleep(min(1.0,DB_BACKOFF*(attempt+1)))
    raise last


def catchup_claim(limit):
    def op():
        c=base.db(); now=time.time(); cutoff=now-LIVE_PROTECT_S
        try:
            c.execute('BEGIN IMMEDIATE')
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
            try:c.rollback()
            except Exception:pass
            raise
        finally:
            c.close()
    return with_db_retry(op)


def safe_reset(row,err,terminal=False):
    def op():
        c=base.db()
        try:
            status='FAILED' if terminal or int(row.get('attempts',0))>=base.MAX_RETRIES else 'PENDING'
            c.execute("UPDATE v51_signature_spool SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",
                      (status,str(err)[-1800:],time.time(),row['signature']))
            c.commit()
        finally:c.close()
    try:
        return with_db_retry(op)
    except Exception as exc:
        # Never let an error-reporting write kill the whole catch-up process.
        print(f'V5.3.1 reset warning signature={row.get("signature","?")[-10:]} err={exc!r}',flush=True)
        return None


def safe_store(row,tx):
    logs=json.loads(row['logs_json'] or '[]'); event=row['event_hint']; token,creator=base.hints(tx,row['source_program'],event)
    raw=json.dumps({'signature':row['signature'],'slot':row['slot'],'logs':logs,'rpc_transaction':tx},separators=(',',':'),ensure_ascii=False).encode(); comp=zlib.compress(raw,3)
    def op():
        c=base.db()
        try:
            c.execute('BEGIN IMMEDIATE')
            before=c.total_changes
            c.execute("""INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,
              observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (row['signature'],row['source_program'],row['source_program_id'],row['subscription_id'],row['slot'],None,time.time(),event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
            inserted=c.total_changes-before
            c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),row['signature']))
            c.commit(); return int(inserted)
        except BaseException:
            try:c.rollback()
            except Exception:pass
            raise
        finally:c.close()
    return with_db_retry(op)


async def hardened_fetch_one(session,sem,row):
    global WRITE_LOCK
    if WRITE_LOCK is None:
        WRITE_LOCK=asyncio.Lock()
    async with sem:
        body={'jsonrpc':'2.0','id':row['signature'][-12:],'method':'getTransaction','params':[row['signature'],{'encoding':'jsonParsed','commitment':base.COMMITMENT,'maxSupportedTransactionVersion':0}]}
        try:
            async with session.post(base.RPC,json=body,timeout=base.aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status==429:
                    await asyncio.to_thread(safe_reset,row,'HTTP 429 rate limited'); return '429',0
                if resp.status in (401,403):
                    await asyncio.to_thread(safe_reset,row,f'HTTP {resp.status}',True); return 'AUTH',0
                if resp.status!=200:
                    await asyncio.to_thread(safe_reset,row,f'HTTP {resp.status}'); return 'ERR',0
                data=await resp.json(content_type=None)
            if data.get('error'):
                await asyncio.to_thread(safe_reset,row,data['error']); return 'ERR',0
            tx=data.get('result')
            if tx is None:
                await asyncio.to_thread(safe_reset,row,'getTransaction returned null'); return 'NULL',0
            # Serialize DB writes from concurrent HTTP tasks. Network fetch remains concurrent.
            async with WRITE_LOCK:
                inserted=await asyncio.to_thread(safe_store,row,tx)
            return 'OK',inserted
        except Exception as exc:
            await asyncio.to_thread(safe_reset,row,repr(exc))
            return 'ERR',0


base.claim=catchup_claim
base.reset=safe_reset
base.store=safe_store
base.fetch_one=hardened_fetch_one

if __name__=='__main__':
    print(f'V5.3.1 CATCH-UP lane | protects newest {LIVE_PROTECT_S:.0f}s | sqlite retries={DB_RETRIES}',flush=True)
    signal.signal(signal.SIGINT,base.stop); signal.signal(signal.SIGTERM,base.stop)
    try:
        asyncio.run(base.main())
    finally:
        if base.DB_PATH.exists():
            def cleanup():
                c=base.db()
                try:
                    c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),)); c.commit()
                finally:c.close()
            try:with_db_retry(cleanup)
            except Exception as exc:print(f'V5.3.1 cleanup warning: {exc!r}',flush=True)
