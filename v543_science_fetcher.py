#!/usr/bin/env python3
"""V5.4.3 science-aware RPC fetcher.

Consumes V5.4.2 admission tiers and V5.4.4 learned tier budgets. If V5.4.5 is
running, each pending signature is additionally ranked by expected scientific
yield before RPC. Scheduling remains by attempt share so weak lanes cannot
monopolize capacity.
"""
from __future__ import annotations
import asyncio, os, signal, sqlite3, time
import aiohttp
import v541_governed_fetcher as base

DB=base.DB_PATH
RPS=float(os.environ.get('MEMECOIN_V543_RPS','6.0'))
MAX_RPS=float(os.environ.get('MEMECOIN_V543_MAX_RPS','8.0'))
LEASE=float(os.environ.get('MEMECOIN_V543_LEASE_S','120'))
HEART=float(os.environ.get('MEMECOIN_V543_HEARTBEAT_S','5'))
STOP=False
STATIC_BUDGET={'A':.35,'B':.40,'C':.20,'D':.05}

def stop(*_):
    global STOP; STOP=True

def db():
    c=sqlite3.connect(DB,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000'); return c

def init():
    base.init(); c=db(); names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}; c.close()
    if 'v542_admission' not in names: raise SystemExit('Start v542_admission_controller.py first.')
    print(f'V5.4.3 SCIENCE FETCHER | provider={base.KEY_SOURCE} target={RPS:.1f}rps tiers=A/B/C/D adaptive_budget=contextual_score',flush=True)

def learned_budget():
    c=db()
    try:
        names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'v544_rpc_yield' not in names:return dict(STATIC_BUDGET),'STATIC'
        rows=c.execute("SELECT tier,budget,updated_at FROM v544_rpc_yield").fetchall()
        if len(rows)!=4 or time.time()-max(float(r['updated_at']) for r in rows)>60:return dict(STATIC_BUDGET),'STATIC'
        b={r['tier']:float(r['budget']) for r in rows}; total=sum(b.values())
        return ({k:b.get(k,0)/total for k in 'ABCD'},'LEARNED') if total>0 else (dict(STATIC_BUDGET),'STATIC')
    finally:c.close()

def choose(attempts,budget):
    total=sum(attempts.values())
    if total<1:return max(budget,key=budget.get)
    deficit={k:budget[k]*(total+1)-attempts[k] for k in budget}
    return max(deficit,key=deficit.get)

def claim(tier):
    c=db(); now=time.time(); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        contextual='v545_signature_score' in names
        if contextual:
            r=c.execute("""SELECT s.* FROM v51_signature_spool s
              JOIN v542_admission a ON a.signature=s.signature
              LEFT JOIN v545_signature_score y ON y.signature=s.signature
              WHERE s.status='PENDING' AND s.attempts<? AND a.tier=?
              ORDER BY COALESCE(y.expected_yield,0.35) DESC,COALESCE(y.confidence,0) DESC,a.score DESC,s.first_seen DESC LIMIT 1""",
              (base.MAX_RETRIES,tier)).fetchone()
        else:
            r=c.execute("""SELECT s.* FROM v51_signature_spool s JOIN v542_admission a ON a.signature=s.signature
              WHERE s.status='PENDING' AND s.attempts<? AND a.tier=? ORDER BY a.score DESC,s.first_seen DESC LIMIT 1""",
              (base.MAX_RETRIES,tier)).fetchone()
        if not r:c.commit();return None
        cur=c.execute("UPDATE v51_signature_spool SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+LEASE,now,r['signature']))
        c.commit()
        if cur.rowcount!=1:return None
        x=dict(r);x['attempts']=int(r['attempts'])+1;return x
    except BaseException:
        c.rollback();raise
    finally:c.close()

def mark_duplicate_done(row):
    c=db();exists=c.execute('SELECT 1 FROM v5_raw_transactions WHERE signature=?',(row['signature'],)).fetchone()
    if exists:
        c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),row['signature']));c.commit();c.close();return True
    c.close();return False

def counts():
    c=db();pending=c.execute("SELECT COUNT(*) FROM v51_signature_spool WHERE status='PENDING'").fetchone()[0];raw=c.execute('SELECT COUNT(*) FROM v5_raw_transactions').fetchone()[0]
    tiers={r['tier']:r['n'] for r in c.execute("SELECT a.tier,COUNT(*) n FROM v542_admission a JOIN v51_signature_spool s USING(signature) WHERE s.status='PENDING' GROUP BY a.tier")}
    ctx=c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='v545_signature_score'").fetchone()[0]>0
    c.close();return pending,raw,tiers,ctx

def emit(started,attempts,states,inserted,rps,budget,mode):
    pending,raw,tiers,ctx=counts();runtime=max(1,time.time()-started);ok=states.get('OK',0);okps=ok/runtime;total=sum(states.values());err=total-ok
    mix=' '.join(f'{k}:{attempts[k]}' for k in 'ABCD');avail=' '.join(f'{k}:{tiers.get(k,0)}' for k in 'ABCD');plan='/'.join(str(round(100*budget[k])) for k in 'ABCD')
    print(f"V5.4.3 | {mode}+{'CTX' if ctx else 'NOCTX'} plan[{plan}] pace={rps:.2f} ok={okps:.2f}/s attempts={total} OK={ok} ERR={err} NULL={states.get('NULL',0)} 429={states.get('429',0)} dup={states.get('DUP',0)} inserted={inserted} pending={pending:,} raw={raw:,} served[{mix}] available[{avail}]",flush=True)

async def main():
    init();started=time.time();attempts={k:0 for k in 'ABCD'};states={};inserted=0;rps=RPS;last=0;elapsed=0
    connector=aiohttp.TCPConnector(limit=2,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        while not STOP:
            budget,mode=learned_budget();tier=choose(attempts,budget);row=claim(tier)
            if row is None:
                for alt in sorted('ABCD',key=lambda x:budget[x],reverse=True):
                    if alt!=tier:
                        row=claim(alt)
                        if row is not None:tier=alt;break
            if row is None:
                if time.time()-last>=HEART:emit(started,attempts,states,inserted,rps,budget,mode);last=time.time()
                await asyncio.sleep(.5);continue
            attempts[tier]+=1
            if mark_duplicate_done(row):state='DUP';ins=0;retry=None;elapsed=0
            else:
                t0=time.monotonic();state,ins,retry=await base.fetch(session,row);elapsed=time.monotonic()-t0
                if state=='429':rps=max(.5,rps*.65);await asyncio.sleep(max(retry or 0,1.0))
                elif state=='OK' and states.get('429',0)==0 and sum(states.values())%25==0:rps=min(MAX_RPS,rps+.25)
            states[state]=states.get(state,0)+1;inserted+=ins
            if time.time()-last>=HEART:emit(started,attempts,states,inserted,rps,budget,mode);last=time.time()
            await asyncio.sleep(max(0,1/max(.1,rps)-elapsed))

if __name__=='__main__':
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
    try:asyncio.run(main())
    finally:
        if DB.exists():
            c=db();c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),));c.commit();c.close()
