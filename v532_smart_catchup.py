#!/usr/bin/env python3
"""V5.3.2 Smart Catch-Up — paced, priority-aware, live-first.

Why: large concurrent bursts against Free Helius RPC create mostly HTTP 429s.
This worker deliberately sends individual getTransaction calls at a controlled
rate, honors Retry-After, uses exponential backoff, and selects high-value spool
rows first. It uses HELIUS_API_KEY_2 by default so the live collector can keep
HELIUS_API_KEY.

Research data infrastructure only. Never signs or submits transactions.
"""
from __future__ import annotations
import asyncio, json, math, os, random, signal, sqlite3, time, zlib
from pathlib import Path
from urllib.parse import quote
try:
    import aiohttp
except ImportError:
    raise SystemExit('Missing aiohttp. Run: pip install aiohttp')

ROOT=Path.home()/"memecoin_lab"
DB_PATH=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
KEY2=os.environ.get('HELIUS_API_KEY_2','').strip(); KEY1=os.environ.get('HELIUS_API_KEY','').strip(); KEY=KEY2 or KEY1
KEY_SOURCE='HELIUS_API_KEY_2' if KEY2 else ('HELIUS_API_KEY' if KEY1 else 'MISSING')
HTTP_BASE=os.environ.get('HELIUS_HTTP_BASE','https://mainnet.helius-rpc.com/')
RPC=os.environ.get('HELIUS_RPC_URL_2') or (f'{HTTP_BASE}?api-key={quote(KEY)}' if KEY else '')
COMMITMENT=os.environ.get('MEMECOIN_V5_COMMITMENT','confirmed')
TARGET_RPS=float(os.environ.get('MEMECOIN_V532_RPS','6.0'))
MIN_RPS=float(os.environ.get('MEMECOIN_V532_MIN_RPS','0.5'))
MAX_RPS=float(os.environ.get('MEMECOIN_V532_MAX_RPS','8.0'))
CLAIM_N=int(os.environ.get('MEMECOIN_V532_CLAIM','60'))
LEASE=float(os.environ.get('MEMECOIN_V532_LEASE_S','120'))
MAX_RETRIES=int(os.environ.get('MEMECOIN_V51_MAX_RETRIES','20'))
LIVE_FIRST_S=float(os.environ.get('MEMECOIN_V532_LIVE_FIRST_S','900'))
STOP=False

def stop(*_):
    global STOP; STOP=True

def db():
    c=sqlite3.connect(DB_PATH,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000'); return c

def init():
    if not RPC: raise SystemExit('Set HELIUS_API_KEY_2 in this terminal.')
    if not DB_PATH.exists(): raise SystemExit(f'V5 DB not found: {DB_PATH}')
    c=db(); names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}; c.close()
    miss={'v51_signature_spool','v5_raw_transactions'}-names
    if miss: raise SystemExit(f'Missing V5.1 tables: {sorted(miss)}')
    print(f'V5.3.2 SMART | provider={KEY_SOURCE} target_rps={TARGET_RPS:.1f} max_rps={MAX_RPS:.1f} mode=LIVE_FIRST+PRIORITY',flush=True)

def claim(limit):
    c=db(); now=time.time(); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        # Priority first, then recent rows inside each priority band. This prevents
        # an unbounded historical queue from starving fresh research data.
        rows=c.execute("""SELECT * FROM v51_signature_spool
          WHERE status='PENDING' AND attempts<?
          ORDER BY
            CASE WHEN event_hint IN ('CREATE','MIGRATE') THEN 0
                 WHEN priority<=1 THEN 1
                 WHEN first_seen>=? THEN 2 ELSE 3 END ASC,
            priority ASC,
            CASE WHEN first_seen>=? THEN -first_seen ELSE first_seen END ASC
          LIMIT ?""",(MAX_RETRIES,now-LIVE_FIRST_S,now-LIVE_FIRST_S,int(limit))).fetchall()
        out=[]
        for r in rows:
            cur=c.execute("UPDATE v51_signature_spool SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+LEASE,now,r['signature']))
            if cur.rowcount==1:
                x=dict(r); x['attempts']=int(r['attempts'])+1; out.append(x)
        c.commit(); return out
    except BaseException:
        c.rollback(); raise
    finally:c.close()

def reset(row,err,terminal=False):
    c=db(); status='FAILED' if terminal or int(row.get('attempts',0))>=MAX_RETRIES else 'PENDING'
    c.execute("UPDATE v51_signature_spool SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",(status,str(err)[-1500:],time.time(),row['signature'])); c.commit(); c.close()

def keys(tx):
    try:a=tx['transaction']['message']['accountKeys']
    except Exception:return []
    return [x if isinstance(x,str) else str(x.get('pubkey')) for x in a or [] if isinstance(x,str) or (isinstance(x,dict) and x.get('pubkey'))]

def mints(tx):
    out=[]; meta=tx.get('meta') or {}
    for name in ('preTokenBalances','postTokenBalances'):
        for b in meta.get(name) or []:
            m=b.get('mint') if isinstance(b,dict) else None
            if m and m!='So11111111111111111111111111111111111111112' and m not in out:out.append(str(m))
    return out

def store(row,tx):
    ks=keys(tx); ms=mints(tx); event=row['event_hint']; token=ms[0] if len(ms)==1 else (ks[1] if row['source_program']=='PUMP' and event=='CREATE' and len(ks)>1 else None); creator=ks[0] if ks else None
    logs=json.loads(row['logs_json'] or '[]'); raw=json.dumps({'signature':row['signature'],'slot':row['slot'],'logs':logs,'rpc_transaction':tx},separators=(',',':'),ensure_ascii=False).encode(); comp=zlib.compress(raw,3)
    c=db(); before=c.total_changes
    c.execute("""INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",(row['signature'],row['source_program'],row['source_program_id'],row['subscription_id'],row['slot'],None,time.time(),event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
    ins=c.total_changes-before; c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),row['signature'])); c.commit(); c.close(); return int(ins)

async def fetch(session,row):
    body={'jsonrpc':'2.0','id':row['signature'][-12:],'method':'getTransaction','params':[row['signature'],{'encoding':'jsonParsed','commitment':COMMITMENT,'maxSupportedTransactionVersion':0}]}
    try:
        async with session.post(RPC,json=body,timeout=aiohttp.ClientTimeout(total=20)) as resp:
            retry=resp.headers.get('Retry-After')
            if resp.status==429:
                reset(row,'HTTP 429'); return '429',0,float(retry) if retry and retry.replace('.','',1).isdigit() else None
            if resp.status in (401,403):reset(row,f'HTTP {resp.status}',True); return 'AUTH',0,None
            if resp.status!=200:reset(row,f'HTTP {resp.status}'); return 'ERR',0,None
            data=await resp.json(content_type=None)
        if data.get('error'):reset(row,data['error']); return 'ERR',0,None
        tx=data.get('result')
        if tx is None:reset(row,'null transaction'); return 'NULL',0,None
        return 'OK',store(row,tx),None
    except Exception as e:
        reset(row,repr(e)); return 'ERR',0,None

def stats():
    c=db(); counts={r['status']:r['n'] for r in c.execute("SELECT status,COUNT(*) n FROM v51_signature_spool GROUP BY status")}; raw=c.execute('SELECT COUNT(*) FROM v5_raw_transactions').fetchone()[0]; newest=c.execute("SELECT MAX(observed_at) FROM v5_raw_transactions").fetchone()[0]; c.close(); return counts,raw,newest

async def main():
    init(); rps=max(MIN_RPS,min(MAX_RPS,TARGET_RPS)); streak=0; total_ok=total_429=total_ins=0; started=time.time(); backoff=0.0
    connector=aiohttp.TCPConnector(limit=2,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        while not STOP:
            rows=claim(CLAIM_N)
            if not rows:
                await asyncio.sleep(.5); continue
            for row in rows:
                if STOP: reset(row,'shutdown'); break
                if backoff>0: await asyncio.sleep(backoff)
                t0=time.monotonic(); state,ins,retry=await fetch(session,row); elapsed=time.monotonic()-t0
                total_ins+=ins
                if state=='OK':
                    total_ok+=1; streak+=1; backoff=0
                    if streak>=25: rps=min(MAX_RPS,rps+.25); streak=0
                elif state=='429':
                    total_429+=1; streak=0; rps=max(MIN_RPS,rps*.65); backoff=max(retry or 0, min(30,1.0+random.random()))
                else: streak=0
                await asyncio.sleep(max(0,1.0/max(.1,rps)-elapsed))
            counts,raw,newest=stats(); runtime=max(1,time.time()-started); success_rps=total_ok/runtime; ratio=total_429/max(1,total_ok+total_429); pending=counts.get('PENDING',0)
            eta_h=(pending/max(.01,success_rps))/3600 if success_rps>0 else math.inf
            eta='∞' if not math.isfinite(eta_h) else f'{eta_h:.1f}h'
            print(f"V5.3.2 | pace={rps:4.2f}rps success={success_rps:4.2f}/s 429={ratio*100:4.1f}% pending={pending:,} done={counts.get('DONE',0):,} raw={raw:,} inserted={total_ins:,} ETA={eta}",flush=True)

if __name__=='__main__':
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
    try:asyncio.run(main())
    finally:
        if DB_PATH.exists():
            c=db(); c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),)); c.commit(); c.close()
