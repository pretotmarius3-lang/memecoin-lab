#!/usr/bin/env python3
"""MEMECOIN LAB V5.1.7 — ALCHEMY PROSPECTIVE ENGINE

Single-provider prospective acquisition engine for Solana Mainnet.
Alchemy handles BOTH:
  1) WebSocket discovery of Pump CREATE transactions,
  2) temporary per-token HOT subscriptions,
  3) HTTP getTransaction enrichment into v5_raw_transactions.

This removes Helius from the critical prospective path while reusing the durable
V5.1.5 queue / HOT registry and preserving all scientific freezes.

Priority objective: complete prospective token paths, not drain history.
Research only. Never signs or submits transactions.
"""
from __future__ import annotations

import asyncio, hashlib, json, os, random, sqlite3, time, uuid, zlib
from pathlib import Path
from urllib.parse import quote

import aiohttp
import websockets
import v51_spool_collector as base

ROOT=Path.home()/"memecoin_lab"
DB_PATH=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
KEY=(os.environ.get("ALCHEMY_API_KEY") or "").strip()
HTTP_URL=os.environ.get("ALCHEMY_SOLANA_RPC_URL") or (f"https://solana-mainnet.g.alchemy.com/v2/{quote(KEY)}" if KEY else "")
WS_URL=os.environ.get("ALCHEMY_SOLANA_WS_URL") or (f"wss://solana-mainnet.g.alchemy.com/v2/{quote(KEY)}" if KEY else "")
COMMITMENT=os.environ.get("MEMECOIN_V5_COMMITMENT","confirmed")

HOT_TTL_S=float(os.environ.get("MEMECOIN_V517_HOT_TTL_S","180"))
BASE_SAMPLE_MOD=max(1,int(os.environ.get("MEMECOIN_V517_BASE_SAMPLE_MOD","16")))
SAMPLE_BUCKET=int(os.environ.get("MEMECOIN_V517_SAMPLE_BUCKET","0"))
MAX_HOT=max(1,int(os.environ.get("MEMECOIN_V517_MAX_HOT","64")))
WORKERS=max(1,int(os.environ.get("MEMECOIN_V517_WORKERS","16")))
BASE_RPS=max(.2,float(os.environ.get("MEMECOIN_V517_BASE_RPS","12")))
MAX_RPS=max(BASE_RPS,float(os.environ.get("MEMECOIN_V517_MAX_RPS","30")))
HTTP_TIMEOUT=float(os.environ.get("MEMECOIN_V517_HTTP_TIMEOUT","15"))
LEASE_S=float(os.environ.get("MEMECOIN_V517_LEASE_S","60"))
MAX_RETRIES=max(2,int(os.environ.get("MEMECOIN_V517_MAX_RETRIES","8")))
REPORT_S=float(os.environ.get("MEMECOIN_V517_REPORT_S","10"))

STOP=asyncio.Event(); RATE_LOCK=asyncio.Lock(); NEXT_HTTP_AT=0.0
CURRENT_RPS=BASE_RPS; RECENT_429=0; REQ_ID=517000
EPOCH_ID='A517_'+time.strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:8]
STARTED_AT=time.time()


def db():
    c=sqlite3.connect(DB_PATH,timeout=30); c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=NORMAL"); c.execute("PRAGMA busy_timeout=30000")
    return c


def ensure_column(c,table,col,decl):
    cols={r[1] for r in c.execute(f"PRAGMA table_info({table})")}
    if col not in cols:c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


def init_db():
    base.initialize(); c=db(); c.executescript('''
    CREATE TABLE IF NOT EXISTS v515_acquisition_epochs(
      epoch_id TEXT PRIMARY KEY, started_at REAL NOT NULL, stopped_at REAL,
      hot_ttl_s REAL NOT NULL, base_sample_mod INTEGER NOT NULL, sample_bucket INTEGER NOT NULL,
      base_rps REAL NOT NULL, max_rps REAL NOT NULL, notes TEXT);
    CREATE TABLE IF NOT EXISTS v515_hot_tokens(
      mint TEXT PRIMARY KEY, create_signature TEXT NOT NULL, source TEXT NOT NULL,
      admitted_at REAL NOT NULL, expires_at REAL NOT NULL, status TEXT NOT NULL,
      epoch_id TEXT NOT NULL, admission_mod INTEGER NOT NULL,
      last_subscribed_at REAL, updated_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v515_hot_expiry ON v515_hot_tokens(status,expires_at);
    CREATE TABLE IF NOT EXISTS v515_hot_queue(
      signature TEXT PRIMARY KEY, mint TEXT, kind TEXT NOT NULL, source TEXT NOT NULL,
      slot INTEGER, logs_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
      attempts INTEGER NOT NULL DEFAULT 0, lease_until REAL, first_seen REAL NOT NULL,
      updated_at REAL NOT NULL, last_error TEXT, epoch_id TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v515_queue_claim ON v515_hot_queue(status,kind,first_seen);
    CREATE INDEX IF NOT EXISTS idx_v515_queue_lease ON v515_hot_queue(status,lease_until);
    CREATE TABLE IF NOT EXISTS v517_provider_stats(
      epoch_id TEXT PRIMARY KEY, provider TEXT NOT NULL, started_at REAL NOT NULL, updated_at REAL NOT NULL,
      ws_messages INTEGER DEFAULT 0, creates_seen INTEGER DEFAULT 0, creates_admitted INTEGER DEFAULT 0,
      hot_logs INTEGER DEFAULT 0, hot_enqueued INTEGER DEFAULT 0, requests INTEGER DEFAULT 0,
      ok INTEGER DEFAULT 0, inserted INTEGER DEFAULT 0, nulls INTEGER DEFAULT 0,
      http_429 INTEGER DEFAULT 0, errors INTEGER DEFAULT 0, reconnects INTEGER DEFAULT 0,
      current_rps REAL DEFAULT 0, last_error TEXT);
    ''')
    ensure_column(c,'v515_hot_queue','admission_mod','INTEGER')
    c.execute('INSERT OR REPLACE INTO v515_acquisition_epochs VALUES(?,?,?,?,?,?,?,?,?)',(
      EPOCH_ID,STARTED_AT,None,HOT_TTL_S,BASE_SAMPLE_MOD,SAMPLE_BUCKET,BASE_RPS,MAX_RPS,
      'V5.1.7 Alchemy-only discovery + HOT + HTTP; prospective priority'))
    c.execute('INSERT OR REPLACE INTO v517_provider_stats(epoch_id,provider,started_at,updated_at,current_rps) VALUES(?,?,?,?,?)',
              (EPOCH_ID,'ALCHEMY',STARTED_AT,STARTED_AT,BASE_RPS))
    now=time.time()
    c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND COALESCE(lease_until,0)<?",(now,now))
    c.execute("UPDATE v515_hot_tokens SET status='EXPIRED',updated_at=? WHERE status='ACTIVE' AND expires_at<=?",(now,now))
    c.commit(); c.close()


def sample_hash(signature,mod):
    h=int.from_bytes(hashlib.sha256(signature.encode()).digest()[:8],'big')
    return h%int(mod)==SAMPLE_BUCKET%int(mod)


def queue_pending():
    c=db(); n=c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0]; c.close(); return int(n)


def active_hot():
    c=db(); rs=[dict(r) for r in c.execute("SELECT * FROM v515_hot_tokens WHERE status='ACTIVE' AND expires_at>? ORDER BY admitted_at",(time.time(),))]; c.close(); return rs


def admission_mod():
    debt=queue_pending()/max(.2,CURRENT_RPS)
    if debt>=180:return None
    if debt>=90:return BASE_SAMPLE_MOD*4
    if debt>=30:return BASE_SAMPLE_MOD*2
    return BASE_SAMPLE_MOD


def enqueue(sig,mint,kind,source,slot,logs,mod=None):
    now=time.time(); c=db(); before=c.total_changes
    c.execute('''INSERT OR IGNORE INTO v515_hot_queue(signature,mint,kind,source,slot,logs_json,status,attempts,first_seen,updated_at,epoch_id,admission_mod)
                 VALUES(?,?,?,?,?,?,'PENDING',0,?,?,?,?,?)''',
              (sig,mint,kind,source,slot,json.dumps(logs,separators=(',',':')),now,now,EPOCH_ID,mod))
    added=c.total_changes-before; c.commit(); c.close(); return bool(added)


def persist_hot(mint,create_sig,source,mod):
    now=time.time(); c=db(); c.execute('''INSERT INTO v515_hot_tokens(mint,create_signature,source,admitted_at,expires_at,status,epoch_id,admission_mod,last_subscribed_at,updated_at)
      VALUES(?,?,?,?,?,'ACTIVE',?,?,NULL,?)
      ON CONFLICT(mint) DO UPDATE SET expires_at=MAX(v515_hot_tokens.expires_at,excluded.expires_at),status='ACTIVE',epoch_id=excluded.epoch_id,admission_mod=excluded.admission_mod,updated_at=excluded.updated_at''',
      (mint,create_sig,source,now,now+HOT_TTL_S,EPOCH_ID,int(mod or BASE_SAMPLE_MOD),now)); c.commit(); c.close()


def expire_hot():
    c=db(); now=time.time(); c.execute("UPDATE v515_hot_tokens SET status='EXPIRED',updated_at=? WHERE status='ACTIVE' AND expires_at<=?",(now,now)); c.commit(); c.close()


def claim_one():
    c=db(); now=time.time(); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        r=c.execute("""SELECT * FROM v515_hot_queue WHERE status='PENDING' AND attempts<?
          ORDER BY CASE kind WHEN 'CREATE' THEN 0 ELSE 1 END, first_seen ASC LIMIT 1""",(MAX_RETRIES,)).fetchone()
        if not r:c.commit(); return None
        cur=c.execute("UPDATE v515_hot_queue SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+LEASE_S,now,r['signature']))
        if cur.rowcount!=1:c.rollback(); return None
        x=dict(r);x['attempts']=int(r['attempts'])+1;c.commit();return x
    finally:c.close()


def q_done(sig):
    c=db();c.execute("UPDATE v515_hot_queue SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),sig));c.commit();c.close()


def q_retry(x,err,terminal=False):
    st='FAILED' if terminal or int(x['attempts'])>=MAX_RETRIES else 'PENDING';c=db();c.execute("UPDATE v515_hot_queue SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",(st,str(err)[-1200:],time.time(),x['signature']));c.commit();c.close()


def bump(field,n=1,error=None):
    allowed={'ws_messages','creates_seen','creates_admitted','hot_logs','hot_enqueued','requests','ok','inserted','nulls','http_429','errors','reconnects'}
    c=db(); sets=['updated_at=?','current_rps=?'];vals=[time.time(),CURRENT_RPS]
    if field in allowed:sets.append(f'{field}={field}+?');vals.append(n)
    if error is not None:sets.append('last_error=?');vals.append(str(error)[-1200:])
    vals.append(EPOCH_ID);c.execute('UPDATE v517_provider_stats SET '+','.join(sets)+' WHERE epoch_id=?',vals);c.commit();c.close()


def store_tx(x,tx):
    logs=(tx.get('meta') or {}).get('logMessages') or json.loads(x.get('logs_json') or '[]')
    event='CREATE' if x['kind']=='CREATE' else base.infer_event_hint(logs)
    keys=set(base.account_keys(tx)); source='PUMPSWAP' if base.PUMP_AMM_PROGRAM in keys else 'PUMP'
    token,creator=base.hints(tx,source,event)
    payload={'signature':x['signature'],'slot':x.get('slot'),'logs':logs,'rpc_transaction':tx,
             'v517_provider':'ALCHEMY','v517_epoch':EPOCH_ID}
    raw=json.dumps(payload,separators=(',',':'),ensure_ascii=False).encode();comp=zlib.compress(raw,3);now=time.time();c=db();before=c.total_changes
    pid=base.PUMP_AMM_PROGRAM if source=='PUMPSWAP' else base.PUMP_PROGRAM
    c.execute('''INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
              (x['signature'],source,pid,None,x.get('slot'),None,now,event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
    ins=c.total_changes-before
    c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=? AND status IN ('PENDING','FETCHING')",(now,x['signature']))
    c.commit();c.close();return int(ins),token,source,event


async def pace():
    global NEXT_HTTP_AT
    async with RATE_LOCK:
        wait=max(0,NEXT_HTTP_AT-time.monotonic())
        if wait:await asyncio.sleep(wait)
        NEXT_HTTP_AT=max(time.monotonic(),NEXT_HTTP_AT)+1/max(.2,CURRENT_RPS)


async def rpc_get_tx(session,sig):
    global RECENT_429
    await pace();bump('requests')
    body={'jsonrpc':'2.0','id':sig[-12:],'method':'getTransaction','params':[sig,{'encoding':'jsonParsed','commitment':COMMITMENT,'maxSupportedTransactionVersion':0}]}
    try:
        async with session.post(HTTP_URL,json=body,timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as r:
            if r.status==429:RECENT_429+=1;bump('http_429',error='HTTP429');return '429',None
            if r.status in (401,403):bump('errors',error=f'HTTP{r.status}');return 'AUTH',None
            if r.status!=200:bump('errors',error=f'HTTP{r.status}');return 'ERR',None
            data=await r.json(content_type=None)
        if data.get('error'):bump('errors',error=data['error']);return 'ERR',None
        if data.get('result') is None:bump('nulls');return 'NULL',None
        bump('ok');return 'OK',data['result']
    except Exception as e:bump('errors',error=repr(e));return 'ERR',None


async def worker(session,wake):
    while not STOP.is_set():
        x=await asyncio.to_thread(claim_one)
        if not x:await asyncio.sleep(.04);continue
        state=tx=None
        for n in range(5):
            state,tx=await rpc_get_tx(session,x['signature'])
            if state=='OK':break
            if state=='AUTH':break
            await asyncio.sleep(.12*(n+1)+random.random()*.08)
        if state!='OK':await asyncio.to_thread(q_retry,x,state,state=='AUTH');continue
        ins,token,source,event=await asyncio.to_thread(store_tx,x,tx);await asyncio.to_thread(q_done,x['signature']);bump('inserted',ins)
        if x['kind']=='CREATE' and token:
            await asyncio.to_thread(persist_hot,token,x['signature'],source,x.get('admission_mod') or BASE_SAMPLE_MOD);wake.set()


async def controller():
    global CURRENT_RPS,RECENT_429
    while not STOP.is_set():
        await asyncio.sleep(15);q=await asyncio.to_thread(queue_pending);n429=RECENT_429;RECENT_429=0
        if n429:CURRENT_RPS=max(BASE_RPS,CURRENT_RPS*.75)
        elif q>150:CURRENT_RPS=min(MAX_RPS,CURRENT_RPS+2)
        elif q>40:CURRENT_RPS=min(MAX_RPS,CURRENT_RPS+1)
        elif q<8 and CURRENT_RPS>BASE_RPS:CURRENT_RPS=max(BASE_RPS,CURRENT_RPS-1)
        bump('noop',0)


async def websocket_loop(wake):
    global REQ_ID
    backoff=1.0
    while not STOP.is_set():
      try:
        async with websockets.connect(WS_URL,ping_interval=20,ping_timeout=30,close_timeout=10,max_size=None,max_queue=32768) as ws:
            bump('reconnects');request_map={};sub_map={};sid_by_mint={}
            # One global stream only: Pump CREATE discovery.
            REQ_ID+=1;request_map[REQ_ID]={'type':'GLOBAL','source':'PUMP'}
            await ws.send(json.dumps({'jsonrpc':'2.0','id':REQ_ID,'method':'logsSubscribe','params':[{'mentions':[base.PUMP_PROGRAM]},{'commitment':COMMITMENT}]},separators=(',',':')))

            async def subscribe_active():
                active=await asyncio.to_thread(active_hot)
                for h in active:
                    mint=h['mint']
                    if mint in sid_by_mint or len(sid_by_mint)>=MAX_HOT:continue
                    globals()['REQ_ID']+=1;rid=globals()['REQ_ID'];request_map[rid]={'type':'HOT','mint':mint}
                    await ws.send(json.dumps({'jsonrpc':'2.0','id':rid,'method':'logsSubscribe','params':[{'mentions':[mint]},{'commitment':COMMITMENT}]},separators=(',',':')))

            await subscribe_active();wake.clear();print(f"V5.1.7 connected | provider=ALCHEMY | epoch={EPOCH_ID} | restored_hot={len(await asyncio.to_thread(active_hot))} | rps={CURRENT_RPS:.1f}",flush=True);backoff=1.0;last_maint=time.monotonic()
            while not STOP.is_set():
                try:raw=await asyncio.wait_for(ws.recv(),timeout=.5)
                except asyncio.TimeoutError:
                    if wake.is_set():await subscribe_active();wake.clear()
                    if time.monotonic()-last_maint>2:
                        await asyncio.to_thread(expire_hot);last_maint=time.monotonic()
                        active={x['mint'] for x in await asyncio.to_thread(active_hot)}
                        for mint,sid in list(sid_by_mint.items()):
                            if mint not in active:
                                REQ_ID+=1;request_map[REQ_ID]={'type':'UNSUB','mint':mint,'sid':sid}
                                await ws.send(json.dumps({'jsonrpc':'2.0','id':REQ_ID,'method':'logsUnsubscribe','params':[sid]},separators=(',',':')))
                    continue
                bump('ws_messages');msg=json.loads(raw)
                if 'id' in msg:
                    info=request_map.pop(msg.get('id'),None)
                    if not info:continue
                    if msg.get('error'):bump('errors',error=msg['error']);continue
                    if info['type']=='GLOBAL':sub_map[int(msg['result'])]={'type':'GLOBAL'}
                    elif info['type']=='HOT':
                        sid=int(msg['result']);mint=info['mint'];sid_by_mint[mint]=sid;sub_map[sid]={'type':'HOT','mint':mint}
                        c=db();c.execute('UPDATE v515_hot_tokens SET last_subscribed_at=?,updated_at=? WHERE mint=?',(time.time(),time.time(),mint));c.commit();c.close()
                    elif info['type']=='UNSUB':
                        sid_by_mint.pop(info['mint'],None);sub_map.pop(info['sid'],None)
                    continue
                if msg.get('method')!='logsNotification':continue
                p=msg.get('params') or {};res=p.get('result') or {};val=res.get('value') or {};sid=p.get('subscription')
                sig=val.get('signature');logs=val.get('logs') or [];slot=(res.get('context') or {}).get('slot')
                if not sig or val.get('err') is not None or sid is None:continue
                route=sub_map.get(int(sid));
                if not route:continue
                event=base.infer_event_hint(logs)
                if route['type']=='GLOBAL':
                    if event!='CREATE':continue
                    bump('creates_seen');mod=await asyncio.to_thread(admission_mod)
                    if mod is not None and sample_hash(sig,mod) and await asyncio.to_thread(enqueue,sig,None,'CREATE','PUMP',slot,logs,mod):bump('creates_admitted')
                else:
                    bump('hot_logs')
                    if event in ('BUY','SELL') and await asyncio.to_thread(enqueue,sig,route['mint'],'HOT','PUMP',slot,logs,None):bump('hot_enqueued')
      except asyncio.CancelledError:raise
      except Exception as e:
        bump('errors',error=repr(e));print(f"V5.1.7 WS error: {e!r} | reconnect in {backoff:.0f}s",flush=True)
        try:await asyncio.wait_for(STOP.wait(),timeout=backoff)
        except asyncio.TimeoutError:pass
        backoff=min(30,backoff*2)


async def reporter():
    while not STOP.is_set():
        await asyncio.sleep(REPORT_S);c=db();q=c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0];a=c.execute("SELECT COUNT(*) FROM v515_hot_tokens WHERE status='ACTIVE' AND expires_at>?",(time.time(),)).fetchone()[0];s=dict(c.execute("SELECT * FROM v517_provider_stats WHERE epoch_id=?",(EPOCH_ID,)).fetchone());c.close()
        debt=q/max(.2,CURRENT_RPS);mod=await asyncio.to_thread(admission_mod)
        print(f"ALCHEMY517 creates={s['creates_seen']:,} admitted={s['creates_admitted']:,} hot={a} hot_logs={s['hot_logs']:,} enq={s['hot_enqueued']:,} req={s['requests']:,} ok={s['ok']:,} inserted={s['inserted']:,} q={q:,} debt={debt:.1f}s rps={CURRENT_RPS:.1f} sample={'PAUSE' if mod is None else '1/'+str(mod)} 429={s['http_429']:,} err={s['errors']:,}",flush=True)
        try:
            c=db();base.set_state(c,'v517_alchemy_engine',{'epoch_id':EPOCH_ID,'provider':'ALCHEMY','hot_active':a,'queue':q,'queue_debt_s':debt,'current_rps':CURRENT_RPS,'effective_sample_mod':mod,**s,'updated_at':time.time()});c.commit();c.close()
        except Exception:pass


async def main():
    if not KEY and not (os.environ.get('ALCHEMY_SOLANA_RPC_URL') and os.environ.get('ALCHEMY_SOLANA_WS_URL')):raise SystemExit('Set ALCHEMY_API_KEY in .env')
    init_db();wake=asyncio.Event();print(f"V5.1.7 ALCHEMY PROSPECTIVE ENGINE | epoch={EPOCH_ID} | sample=1/{BASE_SAMPLE_MOD} | ttl={HOT_TTL_S:.0f}s | rps={BASE_RPS:.1f}->{MAX_RPS:.1f} | workers={WORKERS}",flush=True)
    connector=aiohttp.TCPConnector(limit=max(32,WORKERS*2),ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        tasks=[asyncio.create_task(worker(session,wake)) for _ in range(WORKERS)]
        tasks += [asyncio.create_task(websocket_loop(wake)),asyncio.create_task(controller()),asyncio.create_task(reporter())]
        try:await asyncio.gather(*tasks)
        finally:
            STOP.set()
            for t in tasks:t.cancel()
            c=db();c.execute('UPDATE v515_acquisition_epochs SET stopped_at=? WHERE epoch_id=?',(time.time(),EPOCH_ID));c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),));c.commit();c.close()

if __name__=='__main__':asyncio.run(main())
