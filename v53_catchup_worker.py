#!/usr/bin/env python3
"""V5.3 catch-up worker for the existing V5.1 durable Helius spool.

Runs beside v51_spool_collector.py. It does NOT open another websocket.
It drains v51_signature_spool from v5_raw_events.db with bounded concurrency,
writes into v5_raw_transactions using the exact V5.1 schema, and backs off on
429s. By default it uses HELIUS_API_KEY_2 so the catch-up lane can be isolated
from the live collector using HELIUS_API_KEY.
Research-only data infrastructure; never signs/submits transactions.
"""
from __future__ import annotations
import asyncio, json, os, random, signal, sqlite3, time, zlib
from pathlib import Path
from urllib.parse import quote

try:
    import aiohttp
except ImportError:
    raise SystemExit('Missing aiohttp. Run: pip install aiohttp')

ROOT=Path.home()/"memecoin_lab"
DB_PATH=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
KEY2=os.environ.get('HELIUS_API_KEY_2','').strip()
KEY1=os.environ.get('HELIUS_API_KEY','').strip()
KEY=KEY2 or KEY1
KEY_SOURCE='HELIUS_API_KEY_2' if KEY2 else ('HELIUS_API_KEY' if KEY1 else 'MISSING')
HTTP_BASE=os.environ.get('HELIUS_HTTP_BASE','https://mainnet.helius-rpc.com/')
RPC=os.environ.get('HELIUS_RPC_URL_2') or os.environ.get('HELIUS_RPC_URL') or (f'{HTTP_BASE}?api-key={quote(KEY)}' if KEY else '')
COMMITMENT=os.environ.get('MEMECOIN_V5_COMMITMENT','confirmed')
BASE=int(os.environ.get('MEMECOIN_V53_CONCURRENCY','8'))
MAXC=int(os.environ.get('MEMECOIN_V53_MAX_CONCURRENCY','16'))
BATCH=int(os.environ.get('MEMECOIN_V53_BATCH','120'))
LEASE=float(os.environ.get('MEMECOIN_V53_LEASE_S','90'))
MAX_RETRIES=int(os.environ.get('MEMECOIN_V51_MAX_RETRIES','12'))
STOP=False


def stop(*_):
    global STOP; STOP=True


def db():
    c=sqlite3.connect(DB_PATH,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000'); return c


def init():
    if not RPC: raise SystemExit('No Helius key loaded. Set HELIUS_API_KEY_2 for catch-up (preferred) or HELIUS_API_KEY as fallback.')
    if not DB_PATH.exists(): raise SystemExit(f'V5 DB not found: {DB_PATH}')
    c=db()
    names={r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    missing={'v51_signature_spool','v5_raw_transactions'}-names
    c.close()
    if missing: raise SystemExit(f'V5.1 tables missing in {DB_PATH}: {sorted(missing)}')
    print(f'V5.3 provider: {KEY_SOURCE} | key_length={len(KEY)} | base_concurrency={BASE} | batch={BATCH}',flush=True)


def claim(limit):
    c=db(); now=time.time(); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        rows=c.execute("""SELECT * FROM v51_signature_spool
                          WHERE status='PENDING' AND attempts<?
                          ORDER BY priority ASC, first_seen ASC LIMIT ?""",(MAX_RETRIES,int(limit))).fetchall()
        out=[]
        for r in rows:
            cur=c.execute("""UPDATE v51_signature_spool
                             SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=?
                             WHERE signature=? AND status='PENDING'""",(now+LEASE,now,r['signature']))
            if cur.rowcount==1:
                x=dict(r); x['attempts']=int(r['attempts'])+1; out.append(x)
        c.commit(); return out
    except BaseException:
        c.rollback(); raise
    finally:c.close()


def reset(row,err,terminal=False):
    c=db(); status='FAILED' if terminal or int(row.get('attempts',0))>=MAX_RETRIES else 'PENDING'
    c.execute("UPDATE v51_signature_spool SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",(status,str(err)[-1800:],time.time(),row['signature'])); c.commit(); c.close()


def account_keys(tx):
    try: keys=tx['transaction']['message']['accountKeys']
    except Exception:return []
    return [k if isinstance(k,str) else str(k.get('pubkey')) for k in keys or [] if isinstance(k,str) or (isinstance(k,dict) and k.get('pubkey'))]


def token_mints(tx):
    meta=tx.get('meta') or {}; out=[]
    for name in ('preTokenBalances','postTokenBalances'):
        for b in meta.get(name) or []:
            if isinstance(b,dict) and b.get('mint') and b['mint'] not in out: out.append(str(b['mint']))
    return out


def hints(tx,source,event):
    keys=account_keys(tx); creator=keys[0] if keys else None
    mints=[m for m in token_mints(tx) if m!='So11111111111111111111111111111111111111112']
    token=mints[0] if len(mints)==1 else (keys[1] if source=='PUMP' and event=='CREATE' and len(keys)>1 else None)
    return token,creator


def store(row,tx):
    logs=json.loads(row['logs_json'] or '[]'); event=row['event_hint']; token,creator=hints(tx,row['source_program'],event)
    raw=json.dumps({'signature':row['signature'],'slot':row['slot'],'logs':logs,'rpc_transaction':tx},separators=(',',':'),ensure_ascii=False).encode(); comp=zlib.compress(raw,3)
    c=db(); before=c.total_changes
    c.execute("""INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,
      observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (row['signature'],row['source_program'],row['source_program_id'],row['subscription_id'],row['slot'],None,time.time(),event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
    inserted=c.total_changes-before
    c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),row['signature']))
    c.commit(); c.close(); return int(inserted)


async def fetch_one(session,sem,row):
    async with sem:
        body={'jsonrpc':'2.0','id':row['signature'][-12:],'method':'getTransaction','params':[row['signature'],{'encoding':'jsonParsed','commitment':COMMITMENT,'maxSupportedTransactionVersion':0}]}
        try:
            async with session.post(RPC,json=body,timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status==429: reset(row,'HTTP 429 rate limited'); return '429',0
                if resp.status in (401,403): reset(row,f'HTTP {resp.status}',True); return 'AUTH',0
                if resp.status!=200: reset(row,f'HTTP {resp.status}'); return 'ERR',0
                data=await resp.json(content_type=None)
            if data.get('error'): reset(row,data['error']); return 'ERR',0
            tx=data.get('result')
            if tx is None: reset(row,'getTransaction returned null'); return 'NULL',0
            return 'OK',store(row,tx)
        except Exception as e:
            reset(row,repr(e)); return 'ERR',0


async def main():
    init(); concurrency=BASE; total_ok=total_inserted=0
    connector=aiohttp.TCPConnector(limit=MAXC,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        while not STOP:
            rows=claim(BATCH)
            if not rows:
                await asyncio.sleep(.5); continue
            sem=asyncio.Semaphore(concurrency)
            res=await asyncio.gather(*(fetch_one(session,sem,r) for r in rows))
            states=[x[0] for x in res]; inserted=sum(x[1] for x in res); total_ok+=states.count('OK'); total_inserted+=inserted
            rate429=states.count('429')/max(1,len(states))
            if rate429>.08: concurrency=max(2,int(concurrency*.65)); await asyncio.sleep(1.5+random.random())
            elif rate429==0 and states.count('OK')/max(1,len(states))>.78: concurrency=min(MAXC,concurrency+1)
            c=db(); counts={r['status']:r['n'] for r in c.execute("SELECT status,COUNT(*) n FROM v51_signature_spool GROUP BY status")}; raw=c.execute('SELECT COUNT(*) FROM v5_raw_transactions').fetchone()[0]; c.close()
            print(f"V5.3 catchup[{KEY_SOURCE}] | c={concurrency:02d} batch={len(states):3d} ok={states.count('OK'):3d} 429={states.count('429'):3d} pending={counts.get('PENDING',0):,} fetching={counts.get('FETCHING',0):,} done={counts.get('DONE',0):,} raw={raw:,} inserted+={inserted} total_inserted={total_inserted:,}",flush=True)


if __name__=='__main__':
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
    try: asyncio.run(main())
    finally:
        if DB_PATH.exists():
            c=db(); c.execute("UPDATE v51_signature_spool SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),)); c.commit(); c.close()
