#!/usr/bin/env python3
"""Memecoin Lab V5.1.5 — durable adaptive HOT-token lane.

Replaces V5.1.4's in-memory critical path while preserving its scientific rule:
a CREATE is eligible only by deterministic hash sampling, never by future price
or outcome.

Improvements
------------
* Persistent HOT-token registry in SQLite.
* Persistent signature queue in SQLite; process restarts no longer drop work.
* HOT subscriptions are automatically restored after WebSocket reconnects.
* Backpressure changes only NEW-token admission. Existing admitted tokens remain
  followed until their frozen TTL expires.
* Each launch creates an acquisition epoch so downstream audits can separate
  data collected before/after infrastructure changes.
* Dynamic admission is recorded explicitly. Because system load can correlate
  with market activity, epoch/modulus are audit metadata and must not be hidden.

Research only. Never signs or submits transactions.
"""
from __future__ import annotations

import asyncio, hashlib, json, os, random, signal, sqlite3, time, uuid, zlib
from pathlib import Path
from urllib.parse import quote

try:
    import aiohttp, websockets
except ImportError:
    raise SystemExit('Missing dependency. Run: pip install aiohttp websockets')

import v51_spool_collector as base

ROOT=Path.home()/"memecoin_lab"
DB_PATH=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
KEY=(os.environ.get('HELIUS_API_KEY') or '').strip()
HTTP_BASE=os.environ.get('HELIUS_HTTP_BASE','https://mainnet.helius-rpc.com/')
WS_BASE=os.environ.get('HELIUS_WS_BASE','wss://mainnet.helius-rpc.com/')
RPC=os.environ.get('HELIUS_RPC_URL') or (f'{HTTP_BASE}?api-key={quote(KEY)}' if KEY else '')
COMMITMENT=os.environ.get('MEMECOIN_V5_COMMITMENT','confirmed')

HOT_TTL_S=float(os.environ.get('MEMECOIN_V515_HOT_TTL_S','180'))
BASE_SAMPLE_MOD=max(1,int(os.environ.get('MEMECOIN_V515_BASE_SAMPLE_MOD','16')))
SAMPLE_BUCKET=int(os.environ.get('MEMECOIN_V515_SAMPLE_BUCKET','0'))
MAX_HOT=max(1,int(os.environ.get('MEMECOIN_V515_MAX_HOT','64')))
HTTP_WORKERS=max(1,int(os.environ.get('MEMECOIN_V515_HTTP_WORKERS','6')))
BASE_RPS=max(.2,float(os.environ.get('MEMECOIN_V515_BASE_RPS','4.0')))
MAX_RPS=max(BASE_RPS,float(os.environ.get('MEMECOIN_V515_MAX_RPS','5.0')))
MAX_RETRIES=max(1,int(os.environ.get('MEMECOIN_V515_MAX_RETRIES','8')))
LEASE_S=float(os.environ.get('MEMECOIN_V515_LEASE_S','90'))
HTTP_TIMEOUT=float(os.environ.get('MEMECOIN_V5_HTTP_TIMEOUT','20'))

STOP=asyncio.Event(); RATE_LOCK=asyncio.Lock(); NEXT_HTTP_AT=0.0
CURRENT_RPS=BASE_RPS; RECENT_429=0; REQ_ID=515000
EPOCH_ID='A515_'+time.strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:8]
STARTED_AT=time.time()


def db():
    c=sqlite3.connect(DB_PATH,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000')
    return c


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
    ''')
    c.execute('INSERT INTO v515_acquisition_epochs VALUES(?,?,?,?,?,?,?,?,?)',(
      EPOCH_ID,STARTED_AT,None,HOT_TTL_S,BASE_SAMPLE_MOD,SAMPLE_BUCKET,BASE_RPS,MAX_RPS,
      'V5.1.5 durable queue + reconnect-safe HOT registry; dynamic admission by queue debt'))
    # A crashed previous process may have left leases behind. Release them.
    c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),))
    c.execute("UPDATE v515_hot_tokens SET status='EXPIRED',updated_at=? WHERE status='ACTIVE' AND expires_at<=?",(time.time(),time.time()))
    c.commit(); c.close()


def sample_hash(signature,mod):
    h=int.from_bytes(hashlib.sha256(str(signature).encode()).digest()[:8],'big')
    return h % int(mod) == SAMPLE_BUCKET % int(mod)


def queue_counts():
    c=db(); x={r['status']:r['n'] for r in c.execute('SELECT status,COUNT(*) n FROM v515_hot_queue GROUP BY status')}; c.close(); return x


def pending_count():
    c=db(); n=c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0]; c.close(); return int(n)


def debt_seconds(): return pending_count()/max(.2,CURRENT_RPS)


def admission_mod():
    debt=debt_seconds()
    if debt>=300:return None
    if debt>=120:return BASE_SAMPLE_MOD*4
    if debt>=60:return BASE_SAMPLE_MOD*2
    return BASE_SAMPLE_MOD


def persist_hot(mint,create_signature,source,mod):
    now=time.time(); exp=now+HOT_TTL_S; c=db()
    c.execute('''INSERT INTO v515_hot_tokens(mint,create_signature,source,admitted_at,expires_at,status,epoch_id,admission_mod,last_subscribed_at,updated_at)
      VALUES(?,?,?,?,?,'ACTIVE',?,?,NULL,?)
      ON CONFLICT(mint) DO UPDATE SET expires_at=MAX(v515_hot_tokens.expires_at,excluded.expires_at),status='ACTIVE',updated_at=excluded.updated_at''',
      (mint,create_signature,source,now,exp,EPOCH_ID,int(mod),now))
    c.commit(); c.close()


def active_hot():
    c=db(); rs=[dict(r) for r in c.execute("SELECT * FROM v515_hot_tokens WHERE status='ACTIVE' AND expires_at>? ORDER BY admitted_at",(time.time(),))]; c.close(); return rs


def expire_hot():
    c=db(); now=time.time(); cur=c.execute("UPDATE v515_hot_tokens SET status='EXPIRED',updated_at=? WHERE status='ACTIVE' AND expires_at<=?",(now,now)); c.commit(); n=cur.rowcount; c.close(); return n


def enqueue(signature,mint,kind,source,slot,logs):
    now=time.time(); c=db(); before=c.total_changes
    c.execute('''INSERT OR IGNORE INTO v515_hot_queue(signature,mint,kind,source,slot,logs_json,status,attempts,first_seen,updated_at,epoch_id)
      VALUES(?,?,?,?,?,?,'PENDING',0,?,?,?)''',(signature,mint,kind,source,slot,json.dumps(logs,separators=(',',':')),now,now,EPOCH_ID))
    inserted=c.total_changes-before; c.commit(); c.close(); return bool(inserted)


def claim_one():
    c=db(); now=time.time(); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND lease_until<?",(now,now))
        # CREATE is always first: discovering the mint unlocks the HOT subscription.
        r=c.execute("""SELECT * FROM v515_hot_queue WHERE status='PENDING' AND attempts<?
          ORDER BY CASE kind WHEN 'CREATE' THEN 0 ELSE 1 END, first_seen ASC LIMIT 1""",(MAX_RETRIES,)).fetchone()
        if not r:c.commit(); return None
        cur=c.execute("UPDATE v515_hot_queue SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+LEASE_S,now,r['signature']))
        if cur.rowcount!=1:c.rollback(); return None
        x=dict(r); x['attempts']=int(r['attempts'])+1; c.commit(); return x
    except BaseException:
        c.rollback(); raise
    finally:c.close()


def queue_done(sig):
    c=db(); c.execute("UPDATE v515_hot_queue SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),sig)); c.commit(); c.close()


def queue_retry(item,err,terminal=False):
    st='FAILED' if terminal or int(item.get('attempts',0))>=MAX_RETRIES else 'PENDING'
    c=db(); c.execute("UPDATE v515_hot_queue SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",(st,str(err)[-1500:],time.time(),item['signature'])); c.commit(); c.close()


def infer_source(tx,fallback='PUMP'):
    keys=set(base.account_keys(tx))
    if base.PUMP_AMM_PROGRAM in keys:return 'PUMPSWAP'
    if base.PUMP_PROGRAM in keys:return 'PUMP'
    return fallback


def store_tx(item,tx):
    logs=(tx.get('meta') or {}).get('logMessages') or json.loads(item['logs_json'] or '[]')
    event='CREATE' if item['kind']=='CREATE' else base.infer_event_hint(logs)
    source=infer_source(tx,item.get('source') or 'PUMP'); token,creator=base.hints(tx,source,event)
    payload={'signature':item['signature'],'slot':item.get('slot'),'logs':logs,'rpc_transaction':tx,
             'v515_transport':'durable_hot_logsSubscribe+getTransaction','v515_epoch':item.get('epoch_id')}
    raw=json.dumps(payload,separators=(',',':'),ensure_ascii=False).encode(); comp=zlib.compress(raw,3); now=time.time(); c=db(); before=c.total_changes
    pid=base.PUMP_PROGRAM if source=='PUMP' else base.PUMP_AMM_PROGRAM
    c.execute('''INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,
      observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
      (item['signature'],source,pid,None,item.get('slot'),None,now,event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
    inserted=c.total_changes-before
    c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=? AND status IN ('PENDING','FETCHING')",(now,item['signature']))
    c.commit(); c.close(); return int(inserted),token,source,event


async def pace():
    global NEXT_HTTP_AT
    async with RATE_LOCK:
        now=time.monotonic(); wait=max(0,NEXT_HTTP_AT-now)
        if wait:await asyncio.sleep(wait)
        NEXT_HTTP_AT=max(time.monotonic(),NEXT_HTTP_AT)+1.0/max(.2,CURRENT_RPS)


async def get_tx(session,sig):
    global RECENT_429
    await pace(); body={'jsonrpc':'2.0','id':sig[-12:],'method':'getTransaction','params':[sig,{'encoding':'jsonParsed','commitment':COMMITMENT,'maxSupportedTransactionVersion':0}]}
    async with session.post(RPC,json=body,timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as resp:
        if resp.status==429:RECENT_429+=1; return '429',None
        if resp.status in (401,403):return 'AUTH',None
        if resp.status!=200:return f'HTTP{resp.status}',None
        data=await resp.json(content_type=None)
    if data.get('error'):return 'RPCERR',None
    if data.get('result') is None:return 'NULL',None
    return 'OK',data['result']


async def worker(session,counters,wake_subscribe):
    while not STOP.is_set():
        item=await asyncio.to_thread(claim_one)
        if not item:
            await asyncio.sleep(.08); continue
        state=tx=None
        try:
            for attempt in range(MAX_RETRIES):
                state,tx=await get_tx(session,item['signature'])
                if state=='OK':break
                if state=='429': counters['429']+=1; await asyncio.sleep(.6+random.random()*.5); continue
                if state=='NULL':await asyncio.sleep(.15*(attempt+1)); continue
                if state=='AUTH':break
                await asyncio.sleep(.2*(attempt+1))
            if state!='OK' or tx is None:
                await asyncio.to_thread(queue_retry,item,state, state=='AUTH'); counters['fetch_fail']+=1; continue
            inserted,token,source,event=await asyncio.to_thread(store_tx,item,tx)
            await asyncio.to_thread(queue_done,item['signature']); counters['fetched']+=1; counters['inserted']+=inserted
            if item['kind']=='CREATE':
                counters['create_fetched']+=1
                if token:
                    await asyncio.to_thread(persist_hot,token,item['signature'],source,int(item.get('admission_mod') or BASE_SAMPLE_MOD))
                    counters['mints_found']+=1; wake_subscribe.set()
                else:counters['create_no_mint']+=1
            elif event in ('BUY','SELL'):counters['hot_swaps']+=1
        except Exception as e:
            await asyncio.to_thread(queue_retry,item,repr(e)); counters['worker_errors']+=1


async def rps_controller(counters):
    global CURRENT_RPS,RECENT_429
    while not STOP.is_set():
        await asyncio.sleep(30)
        q=await asyncio.to_thread(pending_count); debt=q/max(.2,CURRENT_RPS)
        n429=RECENT_429; RECENT_429=0
        if n429>0:CURRENT_RPS=max(BASE_RPS,CURRENT_RPS-.5)
        elif debt>60 and CURRENT_RPS<MAX_RPS:CURRENT_RPS=min(MAX_RPS,CURRENT_RPS+.25)
        elif debt<15 and CURRENT_RPS>BASE_RPS:CURRENT_RPS=max(BASE_RPS,CURRENT_RPS-.25)
        counters['rps_adjustments']+=1


async def websocket_loop(counters,wake_subscribe):
    global REQ_ID
    url=f'{WS_BASE}?api-key={quote(KEY)}'; backoff=1.0
    while not STOP.is_set():
      try:
        async with websockets.connect(url,ping_interval=20,ping_timeout=30,close_timeout=10,max_size=None,max_queue=32768) as ws:
            counters['reconnects']+=1; request_map={}; sub_map={}; sid_by_mint={}
            for source,pid in (('PUMP',base.PUMP_PROGRAM),('PUMPSWAP',base.PUMP_AMM_PROGRAM)):
                REQ_ID+=1; request_map[REQ_ID]={'type':'GLOBAL','source':source}
                await ws.send(json.dumps({'jsonrpc':'2.0','id':REQ_ID,'method':'logsSubscribe','params':[{'mentions':[pid]},{'commitment':COMMITMENT}]},separators=(',',':')))

            async def subscribe_active():
                active=await asyncio.to_thread(active_hot); existing=set(sid_by_mint)
                for h in active:
                    mint=h['mint']
                    if mint in existing:continue
                    if len(sid_by_mint)>=MAX_HOT:break
                    REQ_ID_local=None
                    globals()['REQ_ID']+=1; REQ_ID_local=globals()['REQ_ID']
                    request_map[REQ_ID_local]={'type':'HOT','mint':mint}
                    await ws.send(json.dumps({'jsonrpc':'2.0','id':REQ_ID_local,'method':'logsSubscribe','params':[{'mentions':[mint]},{'commitment':COMMITMENT}]},separators=(',',':')))

            await subscribe_active(); wake_subscribe.clear(); print(f'V5.1.5 connected | epoch={EPOCH_ID} | durable HOT restore={len(await asyncio.to_thread(active_hot))} | rps={CURRENT_RPS:.2f}',flush=True); backoff=1.0
            last_maint=time.monotonic()
            while not STOP.is_set():
                try: raw=await asyncio.wait_for(ws.recv(),timeout=.5)
                except asyncio.TimeoutError:
                    if wake_subscribe.is_set(): await subscribe_active(); wake_subscribe.clear()
                    if time.monotonic()-last_maint>=2:
                        await asyncio.to_thread(expire_hot); last_maint=time.monotonic()
                        # Unsubscribe locally expired mints. A reconnect would also clean them.
                        active_mints={x['mint'] for x in await asyncio.to_thread(active_hot)}
                        for mint,sid in list(sid_by_mint.items()):
                            if mint not in active_mints:
                                globals()['REQ_ID']+=1; rid=globals()['REQ_ID']; request_map[rid]={'type':'UNSUB','mint':mint,'sid':sid}
                                await ws.send(json.dumps({'jsonrpc':'2.0','id':rid,'method':'logsUnsubscribe','params':[sid]},separators=(',',':')))
                    continue
                msg=json.loads(raw)
                if 'id' in msg:
                    info=request_map.pop(msg.get('id'),None)
                    if not info:continue
                    if msg.get('error'):
                        counters['subscribe_errors']+=1; continue
                    if info['type']=='GLOBAL':sub_map[int(msg['result'])]={'type':'GLOBAL','source':info['source']}
                    elif info['type']=='HOT':
                        sid=int(msg['result']); mint=info['mint']; sid_by_mint[mint]=sid; sub_map[sid]={'type':'HOT','mint':mint}; counters['hot_subscribed']+=1
                        c=db(); c.execute('UPDATE v515_hot_tokens SET last_subscribed_at=?,updated_at=? WHERE mint=?',(time.time(),time.time(),mint)); c.commit(); c.close()
                    elif info['type']=='UNSUB':
                        mint=info['mint']; sid=info['sid']; sid_by_mint.pop(mint,None); sub_map.pop(sid,None); counters['hot_unsubscribed']+=1
                    continue
                if msg.get('method')!='logsNotification':continue
                p=msg.get('params') or {}; result=p.get('result') or {}; value=result.get('value') or {}; sid=p.get('subscription')
                sig=value.get('signature'); logs=value.get('logs') or []; slot=(result.get('context') or {}).get('slot')
                if not sig or value.get('err') is not None or sid is None:continue
                route=sub_map.get(int(sid));
                if not route:continue
                event=base.infer_event_hint(logs)
                if route['type']=='GLOBAL':
                    counters['global_logs']+=1
                    if event=='CREATE':
                        counters['creates_seen']+=1; mod=await asyncio.to_thread(admission_mod)
                        if mod is None:counters['admission_paused']+=1; continue
                        if sample_hash(sig,mod):
                            # Do not silently call this a 1/16 sample if backpressure widened it.
                            if enqueue(sig,None,'CREATE',route['source'],slot,logs):
                                c=db(); c.execute('UPDATE v515_hot_queue SET last_error=? WHERE signature=?',(f'admission_mod={mod}',sig)); c.commit(); c.close()
                                # admission_mod is recovered from last_error for CREATE workers below.
                                counters['creates_admitted']+=1
                else:
                    counters['hot_logs']+=1
                    if event in ('BUY','SELL'):
                        if enqueue(sig,route['mint'],'HOT','PUMP',slot,logs):counters['hot_enqueued']+=1
                total=counters['global_logs']+counters['hot_logs']
                if total and total%5000==0:
                    q=await asyncio.to_thread(pending_count); debt=q/max(.2,CURRENT_RPS); active=len(await asyncio.to_thread(active_hot));
                    print(f"global={counters['global_logs']:,} creates={counters['creates_seen']:,} admitted={counters['creates_admitted']:,} hot_active={active} hot_logs={counters['hot_logs']:,} hot_swaps={counters['hot_swaps']:,} fetched={counters['fetched']:,} q={q:,} debt={debt:.0f}s rps={CURRENT_RPS:.2f} 429={counters['429']:,} paused={counters['admission_paused']:,}",flush=True)
      except asyncio.CancelledError:raise
      except Exception as e:
        print(f'V5.1.5 WebSocket error: {e!r} | reconnect in {backoff:.0f}s',flush=True)
        try:await asyncio.wait_for(STOP.wait(),timeout=backoff)
        except asyncio.TimeoutError:pass
        backoff=min(30,backoff*2)


async def telemetry(counters):
    while not STOP.is_set():
        await asyncio.sleep(5)
        try:
            q=await asyncio.to_thread(pending_count); active=len(await asyncio.to_thread(active_hot)); debt=q/max(.2,CURRENT_RPS); mod=await asyncio.to_thread(admission_mod)
            c=db(); base.set_state(c,'v515_hot_lane',{
              'mode':'DURABLE_ADAPTIVE_HOT','epoch_id':EPOCH_ID,'started_at':STARTED_AT,'hot_ttl_s':HOT_TTL_S,
              'base_sample_mod':BASE_SAMPLE_MOD,'effective_sample_mod':mod,'sample_bucket':SAMPLE_BUCKET,
              'current_rps':CURRENT_RPS,'base_rps':BASE_RPS,'max_rps':MAX_RPS,'queue':q,'queue_debt_s':debt,
              'hot_active':active,**counters,'updated_at':time.time()}); c.commit(); c.close()
        except Exception:pass


async def main_async():
    if not KEY:raise SystemExit('HELIUS_API_KEY is not set in this terminal.')
    init_db(); wake=asyncio.Event(); counters={k:0 for k in ('global_logs','creates_seen','creates_admitted','admission_paused','create_fetched','create_no_mint','mints_found','hot_subscribed','hot_unsubscribed','hot_logs','hot_enqueued','hot_swaps','fetched','inserted','429','fetch_fail','worker_errors','subscribe_errors','reconnects','rps_adjustments')}
    connector=aiohttp.TCPConnector(limit=max(12,HTTP_WORKERS*2),ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        tasks=[asyncio.create_task(worker(session,counters,wake)) for _ in range(HTTP_WORKERS)]
        tasks += [asyncio.create_task(websocket_loop(counters,wake)),asyncio.create_task(telemetry(counters)),asyncio.create_task(rps_controller(counters))]
        try:await STOP.wait()
        finally:
            STOP.set()
            for t in tasks:t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)


def shutdown_epoch():
    try:
        c=db(); c.execute('UPDATE v515_acquisition_epochs SET stopped_at=? WHERE epoch_id=?',(time.time(),EPOCH_ID)); c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING'",(time.time(),)); c.commit(); c.close()
    except Exception:pass

if __name__=='__main__':
    print(f'V5.1.5 DURABLE HOT lane | epoch={EPOCH_ID} | sample base=1/{BASE_SAMPLE_MOD} | ttl={HOT_TTL_S:.0f}s | rps={BASE_RPS:.1f}-{MAX_RPS:.1f}',flush=True)
    try:asyncio.run(main_async())
    finally:shutdown_epoch()
