#!/usr/bin/env python3
"""V5.4.3 science-aware RPC fetcher.

Consumes V5.4.2 admission tiers instead of trying to enrich the full firehose.
A/B/C/D are scheduled by RPC attempt share (not success share), so bad/null rows
cannot monopolize a lane. Uses V5.4.1's proven fetch/store implementation.
"""
from __future__ import annotations
import asyncio, math, os, signal, sqlite3, time
import aiohttp
import v541_governed_fetcher as base

DB=base.DB_PATH
RPS=float(os.environ.get('MEMECOIN_V543_RPS','6.0'))
MAX_RPS=float(os.environ.get('MEMECOIN_V543_MAX_RPS','8.0'))
LEASE=float(os.environ.get('MEMECOIN_V543_LEASE_S','120'))
HEART=float(os.environ.get('MEMECOIN_V543_HEARTBEAT_S','5'))
STOP=False
BUDGET={'A':.35,'B':.40,'C':.20,'D':.05}

def stop(*_):
    global STOP; STOP=True

def db():
    c=sqlite3.connect(DB,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000'); return c

def init():
    base.init(); c=db(); names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}; c.close()
    if 'v542_admission' not in names: raise SystemExit('Start v542_admission_controller.py first.')
    print(f'V5.4.3 SCIENCE FETCHER | provider={base.KEY_SOURCE} target={RPS:.1f}rps tiers=A/B/C/D',flush=True)

def choose(attempts):
    total=sum(attempts.values())
    if total<1:return 'B'
    deficit={k:BUDGET[k]*(total+1)-attempts[k] for k in BUDGET}
    return max(deficit,key=deficit.get)

def claim(tier):
    c=db(); now=time.time(); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        r=c.execute("""SELECT s.* FROM v51_signature_spool s
                       JOIN v542_admission a ON a.signature=s.signature
                       WHERE s.status='PENDING' AND s.attempts<? AND a.tier=?
                       ORDER BY a.score DESC,s.first_seen DESC LIMIT 1""",(base.MAX_RETRIES,tier)).fetchone()
        if not r: c.commit(); return None
        cur=c.execute("UPDATE v51_signature_spool SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+LEASE,now,r['signature']))
        c.commit()
        if cur.rowcount!=1:return None
        x=dict(r); x['attempts']=int(r['attempts'])+1; return x
    except BaseException:
        c.rollback(); raise
    finally:c.close()

def mark_duplicate_done(row):
    c=db(); exists=c.execute('SELECT 1 FROM v5_raw_transactions WHERE signature=?',(row['signature'],)).fetchone()
    if exists:
        c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),row['signature'])); c.commit(); c.close(); return True
    c.close(); return False

def counts():
    c=db(); pending=c.execute("SELECT COUNT(*) FROM v51_signature_spool WHERE status='PENDING'").fetchone()[0]; raw=c.execute('SELECT COUNT(*) FROM v5_raw_transactions').fetchone()[0]
    tiers={r['tier']:r['n'] for r in c.execute("SELECT a.tier,COUNT(*) n FROM v542_admission a JOIN v51_signature_spool s USING(signature) WHERE s.status='PENDING' GROUP BY a.tier")}; c.close(); return pending,raw,tiers

def emit(started,attempts,states,inserted,rps):
    pending,raw,tiers=counts(); runtime=max(1,time.time()-started); ok=states.get('OK',0); okps=ok/runtime; total=sum(states.values()); err=total-ok
    mix=' '.join(f'{k}:{attempts[k]}' for k in 'ABCD'); avail=' '.join(f'{k}:{tiers.get(k,0)}' for k in 'ABCD')
    print(f"V5.4.3 | pace={rps:.2f} ok={okps:.2f}/s attempts={total} OK={ok} ERR={err} NULL={states.get('NULL',0)} 429={states.get('429',0)} dup={states.get('DUP',0)} inserted={inserted} pending={pending:,} raw={raw:,} served[{mix}] available[{avail}]",flush=True)

async def main():
    init(); started=time.time(); attempts={k:0 for k in 'ABCD'}; states={}; inserted=0; rps=RPS; last=0
    connector=aiohttp.TCPConnector(limit=2,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        while not STOP:
            tier=choose(attempts); row=claim(tier)
            if row is None:
                for alt in 'ABCD':
                    if alt!=tier:
                        row=claim(alt)
                        if row is not None: tier=alt; break
            if row is None:
                if time.time()-last>=HEART: emit(started,attempts,states,inserted,rps); last=time.time()
                await asyncio.sleep(.5); continue
            attempts[tier]+=1
            if mark_duplicate_done(row): state='DUP'; ins=0; retry=None
            else:
                t0=time.monotonic(); state,ins,retry=await base.fetch(session,row); elapsed=time.monotonic()-t0
                if state=='429':
                    rps=max(.5,rps*.65); await asyncio.sleep(max(retry or 0,1.0))
                elif state=='OK' and states.get('429',0)==0 and sum(states.values())%25==0: rps=min(MAX_RPS,rps+.25)
            states[state]=states.get(state,0)+1; inserted+=ins
            if time.time()-last>=HEART: emit(started,attempts,states,inserted,rps); last=time.time()
            await asyncio.sleep(max(0,1/max(.1,rps)-(elapsed if 'elapsed' in locals() else 0)))

if __name__=='__main__':
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
    try: asyncio.run(main())
    finally:
        if DB.exists():
            c=db(); c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),)); c.commit(); c.close()
