#!/usr/bin/env python3
"""V5.3 catch-up worker for the durable Helius spool.

Runs beside v51_spool_collector.py. It does NOT open another websocket.
It drains pending signatures from v5_spool.db with bounded concurrency, writes
raw RPC transactions into v5_raw_events.db using the same schema as V5.1, and
backs off on 429s. Research-only data infrastructure.
"""
from __future__ import annotations
import asyncio, json, os, random, signal, sqlite3, time, zlib
from pathlib import Path

try:
    import aiohttp
except ImportError:
    raise SystemExit('Missing aiohttp. Run: pip install aiohttp')

ROOT=Path.home()/"memecoin_lab"
SPOOL=ROOT/'v5_spool.db'; RAW=ROOT/'v5_raw_events.db'
KEY=os.environ.get('HELIUS_API_KEY','').strip()
RPC=os.environ.get('HELIUS_RPC_URL') or (f'https://mainnet.helius-rpc.com/?api-key={KEY}' if KEY else '')
BASE=int(os.environ.get('MEMECOIN_V53_CONCURRENCY','16'))
MAXC=int(os.environ.get('MEMECOIN_V53_MAX_CONCURRENCY','40'))
BATCH=int(os.environ.get('MEMECOIN_V53_BATCH','300'))
STOP=False

def stop(*_):
    global STOP; STOP=True

def db(path):
    c=sqlite3.connect(path,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000'); return c

def init():
    if not RPC: raise SystemExit('HELIUS_API_KEY is not set')
    if not SPOOL.exists(): raise SystemExit(f'Spool not found: {SPOOL}')
    r=db(RAW); r.executescript('''
    CREATE TABLE IF NOT EXISTS raw_transactions(signature TEXT PRIMARY KEY,slot INTEGER,block_time INTEGER,source TEXT,event_hint TEXT,token_hint TEXT,wallet_hint TEXT,received_at REAL NOT NULL,payload_zlib BLOB NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_raw_received ON raw_transactions(received_at);
    CREATE TABLE IF NOT EXISTS collector_state(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); r.commit(); r.close()

def claim(limit):
    s=db(SPOOL); now=time.time()
    rows=s.execute("SELECT signature,source,slot,logs_json FROM spool WHERE state='PENDING' ORDER BY received_at LIMIT ?",(limit,)).fetchall()
    if rows:
        s.executemany("UPDATE spool SET state='FETCHING',updated_at=? WHERE signature=? AND state='PENDING'",[(now,r['signature']) for r in rows]); s.commit()
    s.close(); return [dict(r) for r in rows]

def reset(sig, err, retry=True):
    s=db(SPOOL); s.execute("UPDATE spool SET state=?,last_error=?,updated_at=? WHERE signature=?",('PENDING' if retry else 'FAILED',str(err)[:500],time.time(),sig)); s.commit(); s.close()

def store(item, result):
    sig=item['signature']; source=item['source']; logs=json.loads(item['logs_json'] or '[]')
    meta=result.get('meta') or {}; tx=result.get('transaction') or {}; msg=tx.get('message') or {}
    keys=msg.get('accountKeys') or []
    def key(x): return x.get('pubkey','') if isinstance(x,dict) else str(x)
    wallet=key(keys[0]) if keys else None
    pre=meta.get('preTokenBalances') or []; post=meta.get('postTokenBalances') or []
    token=None
    for b in post+pre:
        m=b.get('mint')
        if m and m not in ('So11111111111111111111111111111111111111112','So11111111111111111111111111111111111111111'):
            token=m; break
    text=' '.join(logs).lower(); hint='CREATE' if 'initialize' in text or 'create' in text else ('BUY' if 'buy' in text else ('SELL' if 'sell' in text else 'OTHER'))
    payload=zlib.compress(json.dumps(result,separators=(',',':')).encode(),6)
    r=db(RAW); r.execute("INSERT OR IGNORE INTO raw_transactions(signature,slot,block_time,source,event_hint,token_hint,wallet_hint,received_at,payload_zlib) VALUES(?,?,?,?,?,?,?,?,?)",(sig,result.get('slot') or item.get('slot'),result.get('blockTime'),source,hint,token,wallet,time.time(),payload)); r.commit(); r.close()
    s=db(SPOOL); s.execute("UPDATE spool SET state='DONE',last_error=NULL,updated_at=? WHERE signature=?",(time.time(),sig)); s.commit(); s.close()

async def fetch_one(session, sem, item):
    async with sem:
        body={'jsonrpc':'2.0','id':item['signature'][-10:],'method':'getTransaction','params':[item['signature'],{'encoding':'jsonParsed','maxSupportedTransactionVersion':0,'commitment':'confirmed'}]}
        try:
            async with session.post(RPC,json=body,timeout=aiohttp.ClientTimeout(total=18)) as resp:
                if resp.status==429:
                    reset(item['signature'],'429 rate limited'); return '429'
                if resp.status!=200:
                    reset(item['signature'],f'HTTP {resp.status}'); return 'ERR'
                data=await resp.json(content_type=None)
            if data.get('error'):
                reset(item['signature'],data['error']); return 'ERR'
            if data.get('result') is None:
                reset(item['signature'],'null transaction'); return 'NULL'
            store(item,data['result']); return 'OK'
        except Exception as e:
            reset(item['signature'],repr(e)); return 'ERR'

async def main():
    init(); concurrency=BASE; total=ok=rl=err=0
    connector=aiohttp.TCPConnector(limit=MAXC,ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        while not STOP:
            items=claim(BATCH)
            if not items:
                await asyncio.sleep(1); continue
            sem=asyncio.Semaphore(concurrency)
            res=await asyncio.gather(*(fetch_one(session,sem,x) for x in items))
            total+=len(res); ok+=res.count('OK'); rl+=res.count('429'); err+=len(res)-res.count('OK')-res.count('429')
            rate429=res.count('429')/max(1,len(res))
            if rate429>.08: concurrency=max(2,int(concurrency*.65)); await asyncio.sleep(1.5+random.random())
            elif rate429==0 and res.count('OK')/max(1,len(res))>.75: concurrency=min(MAXC,concurrency+1)
            s=db(SPOOL); pending=s.execute("SELECT COUNT(*) FROM spool WHERE state='PENDING'").fetchone()[0]; fetching=s.execute("SELECT COUNT(*) FROM spool WHERE state='FETCHING'").fetchone()[0]; s.close()
            r=db(RAW); raw=r.execute('SELECT COUNT(*) FROM raw_transactions').fetchone()[0]; r.close()
            print(f'V5.3 catchup | c={concurrency:02d} batch={len(res):3d} ok={res.count("OK"):3d} 429={res.count("429"):3d} pending={pending:,} fetching={fetching:,} raw={raw:,} total_ok={ok:,}',flush=True)

if __name__=='__main__':
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
    try: asyncio.run(main())
    finally:
        if SPOOL.exists():
            s=db(SPOOL); s.execute("UPDATE spool SET state='PENDING',updated_at=? WHERE state='FETCHING'",(time.time(),)); s.commit(); s.close()
