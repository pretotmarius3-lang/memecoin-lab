#!/usr/bin/env python3
"""Memecoin Lab V4.3 — data-driven research organism.

Extends V4.2 with real continuous work sourced from V5/V5.1 enriched transactions.
This version does NOT invent new scientific hypotheses merely to keep CPUs busy.
Instead it adds a LIVE_INGEST branch that converts newly enriched V5 transactions
into a research-side live token-state store. Those ingestion jobs are created only
when new V5 rows exist.

Scientific research branches from V4.2 continue unchanged. Frozen candidates are
not mutated here. Exact prospective scoring remains gated on the future exact IDL
feature decoder; this bridge prepares the live point-in-time state needed for it.

Research-only. No transaction signing. No live trading.
"""
from __future__ import annotations

import json, multiprocessing as mp, os, signal, sqlite3, time, traceback
from pathlib import Path

import v41_core as core
import v41_engine as base
import v41_organism as old
import v42_organism as v42

ROOT=Path.home()/"memecoin_lab"
V5_DB=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
CPU=os.cpu_count() or 4
WORKERS=int(os.environ.get("MEMECOIN_V43_WORKERS",str(min(10,max(4,CPU//2)))))
BATCH=int(os.environ.get("MEMECOIN_V43_INGEST_BATCH","250"))
MAX_INGEST_JOBS=int(os.environ.get("MEMECOIN_V43_MAX_INGEST_JOBS","8"))
IDLE_SLEEP=.25; LOOP_SLEEP=1.0; STOP=False


def stop_handler(*_):
    global STOP; STOP=True


def init_v43():
    db=core.open_research(); db.executescript("""
    CREATE TABLE IF NOT EXISTS v43_live_ingested (
      signature TEXT PRIMARY KEY,
      observed_at REAL NOT NULL,
      ingested_at REAL NOT NULL,
      source_program TEXT,
      event_hint TEXT,
      token_hint TEXT,
      creator_hint TEXT,
      slot INTEGER);
    CREATE INDEX IF NOT EXISTS idx_v43_live_token ON v43_live_ingested(token_hint,observed_at);

    CREATE TABLE IF NOT EXISTS v43_live_token_state (
      token_mint TEXT PRIMARY KEY,
      first_seen REAL NOT NULL,
      last_seen REAL NOT NULL,
      tx_count INTEGER NOT NULL,
      buys INTEGER NOT NULL,
      sells INTEGER NOT NULL,
      creates INTEGER NOT NULL,
      migrations INTEGER NOT NULL,
      other_events INTEGER NOT NULL,
      pump_tx INTEGER NOT NULL,
      pumpswap_tx INTEGER NOT NULL,
      unique_creators INTEGER NOT NULL DEFAULT 0,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v43_live_creators (
      token_mint TEXT NOT NULL,
      creator TEXT NOT NULL,
      first_seen REAL NOT NULL,
      PRIMARY KEY(token_mint,creator));

    CREATE TABLE IF NOT EXISTS v43_live_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at REAL NOT NULL);
    """); db.commit(); db.close()


def open_v5():
    if not V5_DB.exists(): return None
    db=sqlite3.connect(f"file:{V5_DB}?mode=ro",uri=True,timeout=20); db.row_factory=sqlite3.Row; db.execute("PRAGMA busy_timeout=20000"); return db


def unprocessed_rows(limit=BATCH):
    v5=open_v5()
    if v5 is None: return []
    rdb=core.open_research(); seen={r[0] for r in rdb.execute("SELECT signature FROM v43_live_ingested").fetchall()}; rdb.close()
    rows=v5.execute("""SELECT signature,source_program,slot,observed_at,event_hint,token_hint,creator_hint
                       FROM v5_raw_transactions ORDER BY observed_at ASC""").fetchall(); v5.close()
    out=[]
    for r in rows:
        if r['signature'] not in seen:
            out.append(dict(r))
            if len(out)>=limit: break
    return out


def ingest_rows(rows):
    if not rows: return {'rows':0,'tokens':0}
    db=core.open_research(); db.execute("BEGIN IMMEDIATE"); tokens=set(); now=time.time()
    try:
        for r in rows:
            db.execute("""INSERT OR IGNORE INTO v43_live_ingested(signature,observed_at,ingested_at,source_program,event_hint,token_hint,creator_hint,slot)
                          VALUES(?,?,?,?,?,?,?,?)""",
                       (r['signature'],r['observed_at'],now,r['source_program'],r['event_hint'],r['token_hint'],r['creator_hint'],r['slot']))
            mint=r.get('token_hint')
            if not mint: continue
            tokens.add(mint); event=(r.get('event_hint') or 'OTHER').upper(); source=(r.get('source_program') or '').upper()
            buy=int(event=='BUY'); sell=int(event=='SELL'); create=int(event=='CREATE'); mig=int(event=='MIGRATE'); other=int(event not in ('BUY','SELL','CREATE','MIGRATE'))
            pump=int(source=='PUMP'); pumpswap=int(source=='PUMPSWAP')
            db.execute("""INSERT INTO v43_live_token_state(token_mint,first_seen,last_seen,tx_count,buys,sells,creates,migrations,other_events,pump_tx,pumpswap_tx,updated_at)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(token_mint) DO UPDATE SET
                first_seen=MIN(v43_live_token_state.first_seen,excluded.first_seen),
                last_seen=MAX(v43_live_token_state.last_seen,excluded.last_seen),
                tx_count=v43_live_token_state.tx_count+1,
                buys=v43_live_token_state.buys+excluded.buys,
                sells=v43_live_token_state.sells+excluded.sells,
                creates=v43_live_token_state.creates+excluded.creates,
                migrations=v43_live_token_state.migrations+excluded.migrations,
                other_events=v43_live_token_state.other_events+excluded.other_events,
                pump_tx=v43_live_token_state.pump_tx+excluded.pump_tx,
                pumpswap_tx=v43_live_token_state.pumpswap_tx+excluded.pumpswap_tx,
                updated_at=excluded.updated_at""",
              (mint,r['observed_at'],r['observed_at'],1,buy,sell,create,mig,other,pump,pumpswap,now))
            creator=r.get('creator_hint')
            if creator:
                db.execute("INSERT OR IGNORE INTO v43_live_creators(token_mint,creator,first_seen) VALUES(?,?,?)",(mint,creator,r['observed_at']))
                db.execute("UPDATE v43_live_token_state SET unique_creators=(SELECT COUNT(*) FROM v43_live_creators WHERE token_mint=?) WHERE token_mint=?",(mint,mint))
        db.execute("""INSERT INTO v43_live_state(key,value,updated_at) VALUES('last_ingest',?,?)
                      ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
                   (json.dumps({'rows':len(rows),'tokens':len(tokens),'latest':max(float(r['observed_at']) for r in rows)}),now))
        db.commit()
    except BaseException:
        db.rollback(); raise
    finally: db.close()
    return {'rows':len(rows),'tokens':len(tokens)}


def run_payload(payload):
    if payload.get('adapter')=='live_ingest_batch':
        return 'INGESTED', ingest_rows(payload['rows'])
    return v42.run_payload(payload)


def finish_job(job,verdict,metrics):
    if job['payload'].get('adapter')=='live_ingest_batch':
        core.finish_job(job,verdict,'LIVE_INGEST',metrics,coverage={'rows':metrics.get('rows'),'tokens':metrics.get('tokens')})
    else:
        v42.finish_job(job,verdict,metrics)


def worker_main(index):
    wid=f"ORG43-{index:02d}-{os.getpid()}"; core.worker_heartbeat(wid,'RUNNING')
    while True:
        job=base.safe_claim(wid)
        if job is None:
            core.worker_heartbeat(wid,'IDLE'); time.sleep(IDLE_SLEEP); continue
        core.worker_heartbeat(wid,'BUSY',job['job_id'])
        try:
            verdict,metrics=run_payload(job['payload']); finish_job(job,verdict,metrics); core.worker_heartbeat(wid,'RUNNING',done_inc=1)
        except KeyboardInterrupt: return
        except Exception:
            core.fail_job(job,traceback.format_exc()); core.worker_heartbeat(wid,'RUNNING',failed_inc=1)


def seed_live_ingest():
    counts=core.queue_counts(); if_queued=counts.get('QUEUED',0)
    if if_queued>=WORKERS*8: return 0
    made=0
    for _ in range(MAX_INGEST_JOBS):
        rows=unprocessed_rows(BATCH)
        if not rows: break
        first,last=rows[0]['signature'],rows[-1]['signature']
        spec={'adapter':'live_ingest_batch','branch':'LIVE_INGEST','first_signature':first,'last_signature':last,'count':len(rows),'rows':rows}
        hid,_=core.create_hypothesis('LIVE_INGEST','V5_BRIDGE',{'adapter':'live_ingest_batch','branch':'LIVE_INGEST','first_signature':first,'last_signature':last,'count':len(rows)}, {'lane':'live_data','source':'v5_raw_transactions'})
        _,created=core.enqueue_job(hid,'LIVE_INGEST',spec,priority=5)
        if not created: break
        made+=1
        # Avoid scheduling the same unprocessed rows repeatedly before workers finish.
        break
    return made


def live_stats():
    db=core.open_research()
    ing=db.execute("SELECT COUNT(*) FROM v43_live_ingested").fetchone()[0]
    tok=db.execute("SELECT COUNT(*) FROM v43_live_token_state").fetchone()[0]
    latest=db.execute("SELECT MAX(last_seen) FROM v43_live_token_state").fetchone()[0]
    db.close(); return ing,tok,latest


def display(last_director,seeded_wallet,seeded_live):
    db=core.open_research(); jobs={r['status']:r['n'] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    frozen=db.execute("SELECT COUNT(*) FROM v41_candidates WHERE status='FROZEN'").fetchone()[0]
    branches=db.execute("""SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) h,
      SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) q,
      SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) r,
      SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) d,
      SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) f
      FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id GROUP BY h.branch ORDER BY h DESC""").fetchall(); db.close()
    ing,tok,latest=live_stats(); age='—' if latest is None else f"{max(0,time.time()-latest):.1f}s"
    print('\033[2J\033[H',end=''); print('='*118); print('MEMECOIN LAB — DATA-DRIVEN RESEARCH ORGANISM V4.3'); print('='*118)
    print(f"WORKERS={WORKERS} | QUEUED={jobs.get('QUEUED',0)} | RUNNING={jobs.get('RUNNING',0)} | DONE={jobs.get('DONE',0)} | FAILED={jobs.get('FAILED',0)} | FROZEN={frozen}")
    print(f"LIVE INGESTED={ing:,} | LIVE TOKENS={tok:,} | LIVE AGE={age} | NEW LIVE JOBS={seeded_live}")
    print(f"DIRECTOR={last_director} | WALLET SEEDED={seeded_wallet}")
    print(); print(f"{'BRANCH':<20}{'HYP':>8}{'Q':>8}{'RUN':>8}{'DONE':>8}{'FAIL':>8}")
    for x in branches: print(f"{x['branch']:<20}{x['h'] or 0:>8}{x['q'] or 0:>8}{x['r'] or 0:>8}{x['d'] or 0:>8}{x['f'] or 0:>8}")
    print('\nResearch-only | V5-driven live ingest enabled | frozen candidates unchanged | no live trading')


def main():
    global STOP
    signal.signal(signal.SIGINT,stop_handler); signal.signal(signal.SIGTERM,stop_handler)
    core.initialize(); init_v43()
    v42.seed_wallet_history(); old.seed_discovery_if_needed()
    workers=[mp.Process(target=worker_main,args=(i+1,),daemon=True) for i in range(WORKERS)]
    for p in workers: p.start()
    try:
        while not STOP:
            core.reclaim_expired_jobs(); old.seed_discovery_if_needed(); sw=v42.seed_wallet_history(); sl=seed_live_ingest(); d=v42.auto_director_tick(); display(d,sw,sl); time.sleep(LOOP_SLEEP)
    finally:
        for p in workers:
            if p.is_alive(): p.terminate()
        for p in workers: p.join(timeout=3)
        print('V4.3 organism stopped cleanly')

if __name__=='__main__':
    mp.set_start_method('spawn',force=True); main()
