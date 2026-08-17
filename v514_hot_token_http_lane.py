#!/usr/bin/env python3
"""Memecoin Lab V5.1.4 — sampled HOT-TOKEN HTTP live lane.

Purpose
-------
Preserve 0-10s AND 10-20s swap coverage for a prospective, unbiased sample of
new Pump tokens without trying to HTTP-enrich the entire global firehose.

Architecture
------------
1. Global Pump/PumpSwap logsSubscribe is used ONLY to discover CREATE events.
2. CREATE signatures are deterministically sampled by signature hash BEFORE
   any token behaviour or price is observed.
3. Sampled CREATE is fetched immediately with the proven getTransaction RPC.
4. Once mint is known, a temporary logsSubscribe({mentions:[mint]}) is opened
   for HOT_TTL_S (default 35s).
5. BUY/SELL signatures seen on that mint subscription are fetched through a
   small dedicated HTTP priority queue and inserted directly into the existing
   v5_raw_transactions schema.
6. Historical/catch-up workers remain independent.

The sample decision depends only on the CREATE signature hash, not future price,
volume, activity or outcomes. It is therefore suitable for prospective science.

Research only. Never signs or submits a transaction.
"""
from __future__ import annotations

import asyncio, hashlib, json, os, random, signal, sqlite3, sys, time, zlib
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
KEY_SOURCE='HELIUS_API_KEY'
HTTP_BASE=os.environ.get('HELIUS_HTTP_BASE','https://mainnet.helius-rpc.com/')
WS_BASE=os.environ.get('HELIUS_WS_BASE','wss://mainnet.helius-rpc.com/')
RPC=os.environ.get('HELIUS_RPC_URL') or (f'{HTTP_BASE}?api-key={quote(KEY)}' if KEY else '')
COMMITMENT=os.environ.get('MEMECOIN_V5_COMMITMENT','confirmed')

HOT_TTL_S=float(os.environ.get('MEMECOIN_V514_HOT_TTL_S','35'))
CREATE_SAMPLE_MOD=max(1,int(os.environ.get('MEMECOIN_V514_CREATE_SAMPLE_MOD','16')))
CREATE_SAMPLE_BUCKET=int(os.environ.get('MEMECOIN_V514_CREATE_SAMPLE_BUCKET','0')) % CREATE_SAMPLE_MOD
MAX_HOT=int(os.environ.get('MEMECOIN_V514_MAX_HOT','48'))
HTTP_CONCURRENCY=max(1,int(os.environ.get('MEMECOIN_V514_HTTP_CONCURRENCY','6')))
TARGET_RPS=max(.2,float(os.environ.get('MEMECOIN_V514_TARGET_RPS','6.0')))
QUEUE_MAX=max(100,int(os.environ.get('MEMECOIN_V514_QUEUE_MAX','10000')))
MAX_RETRIES=max(1,int(os.environ.get('MEMECOIN_V514_MAX_RETRIES','5')))
HTTP_TIMEOUT=float(os.environ.get('MEMECOIN_V5_HTTP_TIMEOUT','20'))

STOP=asyncio.Event()
RATE_LOCK=asyncio.Lock(); NEXT_HTTP_AT=0.0
REQ_ID=514000


def stop(*_): STOP.set()


def db():
    c=sqlite3.connect(DB_PATH,timeout=30); c.row_factory=sqlite3.Row
    c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL'); c.execute('PRAGMA busy_timeout=30000')
    return c


def sampled(signature):
    h=hashlib.sha256(str(signature).encode()).digest()
    return int.from_bytes(h[:8],'big') % CREATE_SAMPLE_MOD == CREATE_SAMPLE_BUCKET


def infer_source(tx, fallback='PUMP'):
    keys=set(base.account_keys(tx))
    if base.PUMP_AMM_PROGRAM in keys:return 'PUMPSWAP'
    if base.PUMP_PROGRAM in keys:return 'PUMP'
    return fallback


def store_tx(signature,slot,logs,tx,source,event,sub_id=None):
    token,creator=base.hints(tx,source,event)
    raw=json.dumps({'signature':signature,'slot':slot,'logs':logs,'rpc_transaction':tx,
                    'v514_transport':'hot_token_logsSubscribe+getTransaction'},separators=(',',':'),ensure_ascii=False).encode()
    comp=zlib.compress(raw,3); c=db(); before=c.total_changes; now=time.time()
    pid=base.PUMP_PROGRAM if source=='PUMP' else base.PUMP_AMM_PROGRAM
    c.execute('''INSERT OR IGNORE INTO v5_raw_transactions(
      signature,source_program,source_program_id,subscription_id,slot,transaction_index,
      observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
      (signature,source,pid,sub_id,slot,None,now,event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
    inserted=c.total_changes-before
    # Avoid future duplicate HTTP work by the ordinary spool/catch-up lanes.
    c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=? AND status IN ('PENDING','FETCHING')",(now,signature))
    c.commit(); c.close(); return int(inserted),token


async def pace():
    global NEXT_HTTP_AT
    async with RATE_LOCK:
        now=time.monotonic(); wait=max(0.0,NEXT_HTTP_AT-now)
        if wait: await asyncio.sleep(wait)
        NEXT_HTTP_AT=max(time.monotonic(),NEXT_HTTP_AT)+1.0/TARGET_RPS


async def get_tx(session,signature):
    await pace()
    body={'jsonrpc':'2.0','id':signature[-12:],'method':'getTransaction','params':[signature,{
          'encoding':'jsonParsed','commitment':COMMITMENT,'maxSupportedTransactionVersion':0}]}
    async with session.post(RPC,json=body,timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)) as resp:
        if resp.status==429:return '429',None
        if resp.status in (401,403):return 'AUTH',None
        if resp.status!=200:return f'HTTP{resp.status}',None
        data=await resp.json(content_type=None)
    if data.get('error'):return 'RPCERR',None
    if data.get('result') is None:return 'NULL',None
    return 'OK',data['result']


async def fetch_worker(index,session,queue,subscribe_cmd,counters,inflight):
    while not STOP.is_set():
        try:item=await asyncio.wait_for(queue.get(),timeout=.5)
        except asyncio.TimeoutError:continue
        signature=item['signature']
        try:
            state=tx=None
            for attempt in range(MAX_RETRIES):
                state,tx=await get_tx(session,signature)
                if state=='OK':break
                if state=='429':
                    counters['429']+=1; await asyncio.sleep(min(3.0,.5*(attempt+1)+random.random()*.25)); continue
                if state=='NULL':
                    await asyncio.sleep(.15*(attempt+1)); continue
                if state=='AUTH':break
                await asyncio.sleep(.2*(attempt+1))
            if state!='OK' or tx is None:
                counters['fetch_fail']+=1; continue
            logs=(tx.get('meta') or {}).get('logMessages') or item.get('logs') or []
            event=base.infer_event_hint(logs)
            if item['kind']=='CREATE': event='CREATE'
            source=infer_source(tx,item.get('source','PUMP'))
            inserted,token=await asyncio.to_thread(store_tx,signature,item.get('slot'),logs,tx,source,event,item.get('sub_id'))
            counters['fetched']+=1; counters['inserted']+=inserted
            if item['kind']=='CREATE':
                counters['create_fetched']+=1
                if token:
                    counters['mints_found']+=1
                    await subscribe_cmd.put({'op':'ADD_HOT','mint':token,'created_sig':signature})
                else:counters['create_no_mint']+=1
            elif event in ('BUY','SELL'):
                counters['hot_swaps']+=1
        except Exception as exc:
            counters['worker_errors']+=1
            if counters['worker_errors']<=10:print(f'V5.1.4 worker error: {exc!r}',flush=True)
        finally:
            inflight.discard(signature); queue.task_done()


async def enqueue(queue,inflight,item,counters):
    sig=item.get('signature')
    if not sig or sig in inflight:return False
    inflight.add(sig)
    try:
        queue.put_nowait(item); return True
    except asyncio.QueueFull:
        inflight.discard(sig); counters['queue_drops']+=1; return False


async def websocket_loop(queue,inflight,subscribe_cmd,counters):
    global REQ_ID
    url=f'{WS_BASE}?api-key={quote(KEY)}'; backoff=1.0
    while not STOP.is_set():
      try:
        async with websockets.connect(url,ping_interval=20,ping_timeout=30,close_timeout=10,max_size=None,max_queue=32768) as ws:
            counters['reconnects']+=1
            request_map={}; sub_map={}; hot_by_mint={}
            # Global discovery subscriptions. We receive the firehose but only
            # enqueue deterministically sampled CREATE signatures.
            for source,pid in (('PUMP',base.PUMP_PROGRAM),('PUMPSWAP',base.PUMP_AMM_PROGRAM)):
                REQ_ID+=1; rid=REQ_ID; request_map[rid]={'type':'GLOBAL','source':source,'pid':pid}
                await ws.send(json.dumps({'jsonrpc':'2.0','id':rid,'method':'logsSubscribe','params':[{'mentions':[pid]},{'commitment':COMMITMENT}]},separators=(',',':')))
            print(f'V5.1.4 connected | sample=1/{CREATE_SAMPLE_MOD} bucket={CREATE_SAMPLE_BUCKET} | hot_ttl={HOT_TTL_S:.0f}s | target_rps={TARGET_RPS:.1f}',flush=True)
            backoff=1.0

            async def command_sender():
                global REQ_ID
                while not STOP.is_set():
                    try:cmd=await asyncio.wait_for(subscribe_cmd.get(),timeout=.5)
                    except asyncio.TimeoutError:
                        # TTL cleanup
                        now=time.time()
                        for mint,info in list(hot_by_mint.items()):
                            if info.get('sid') is not None and now>=info['expires']:
                                REQ_ID+=1; rid=REQ_ID; request_map[rid]={'type':'UNSUB','mint':mint,'sid':info['sid']}
                                await ws.send(json.dumps({'jsonrpc':'2.0','id':rid,'method':'logsUnsubscribe','params':[info['sid']]},separators=(',',':')))
                                info['unsub_pending']=True
                        continue
                    try:
                        if cmd['op']=='ADD_HOT':
                            mint=cmd['mint']; now=time.time()
                            if mint in hot_by_mint:
                                hot_by_mint[mint]['expires']=max(hot_by_mint[mint]['expires'],now+HOT_TTL_S); continue
                            active=sum(1 for x in hot_by_mint.values() if not x.get('unsub_pending'))
                            if active>=MAX_HOT:
                                counters['hot_capacity_reject']+=1; continue
                            REQ_ID+=1; rid=REQ_ID
                            hot_by_mint[mint]={'sid':None,'expires':now+HOT_TTL_S,'created_sig':cmd.get('created_sig'),'unsub_pending':False}
                            request_map[rid]={'type':'HOT','mint':mint}
                            await ws.send(json.dumps({'jsonrpc':'2.0','id':rid,'method':'logsSubscribe','params':[{'mentions':[mint]},{'commitment':COMMITMENT}]},separators=(',',':')))
                    finally:subscribe_cmd.task_done()

            sender=asyncio.create_task(command_sender())
            try:
              async for raw in ws:
                if STOP.is_set():break
                msg=json.loads(raw)
                if 'id' in msg:
                    info=request_map.pop(msg.get('id'),None)
                    if not info:continue
                    if msg.get('error'):
                        counters['subscribe_errors']+=1
                        if counters['subscribe_errors']<=10:print(f"V5.1.4 subscribe error {info}: {msg['error']}",flush=True)
                        if info.get('type')=='HOT':hot_by_mint.pop(info['mint'],None)
                        continue
                    if info['type']=='GLOBAL':
                        sub_map[int(msg['result'])]={'type':'GLOBAL','source':info['source']}
                    elif info['type']=='HOT':
                        sid=int(msg['result']); mint=info['mint']
                        if mint in hot_by_mint:
                            hot_by_mint[mint]['sid']=sid; sub_map[sid]={'type':'HOT','mint':mint}; counters['hot_subscribed']+=1
                    elif info['type']=='UNSUB':
                        sid=info['sid']; mint=info['mint']; sub_map.pop(sid,None); hot_by_mint.pop(mint,None); counters['hot_unsubscribed']+=1
                    continue
                if msg.get('method')!='logsNotification':continue
                params=msg.get('params') or {}; result=params.get('result') or {}; value=result.get('value') or {}; sid=params.get('subscription')
                sig=value.get('signature'); logs=value.get('logs') or []; slot=(result.get('context') or {}).get('slot')
                if not sig or value.get('err') is not None or sid is None:continue
                route=sub_map.get(int(sid))
                if not route:continue
                event=base.infer_event_hint(logs)
                if route['type']=='GLOBAL':
                    counters['global_logs']+=1
                    if event=='CREATE':
                        counters['creates_seen']+=1
                        if sampled(sig):
                            counters['creates_sampled']+=1
                            await enqueue(queue,inflight,{'kind':'CREATE','signature':sig,'slot':slot,'logs':logs,'source':route['source'],'sub_id':sid},counters)
                else:
                    counters['hot_logs']+=1
                    if event in ('BUY','SELL'):
                        await enqueue(queue,inflight,{'kind':'HOT','signature':sig,'slot':slot,'logs':logs,'source':'PUMP','sub_id':sid,'mint':route['mint']},counters)
                total=counters['global_logs']+counters['hot_logs']
                if total and total%5000==0:
                    active=sum(1 for x in hot_by_mint.values() if x.get('sid') is not None and not x.get('unsub_pending'))
                    print(f"global={counters['global_logs']:,} creates={counters['creates_seen']:,} sampled={counters['creates_sampled']:,} hot_active={active} hot_logs={counters['hot_logs']:,} hot_swaps={counters['hot_swaps']:,} fetched={counters['fetched']:,} inserted={counters['inserted']:,} q={queue.qsize():,} 429={counters['429']:,}",flush=True)
            finally:
                sender.cancel(); await asyncio.gather(sender,return_exceptions=True)
      except asyncio.CancelledError:raise
      except Exception as exc:
        print(f'V5.1.4 WebSocket error: {exc!r} | reconnect in {backoff:.0f}s',flush=True)
        try:await asyncio.wait_for(STOP.wait(),timeout=backoff)
        except asyncio.TimeoutError:pass
        backoff=min(30.0,backoff*2)


async def telemetry(counters,queue):
    while not STOP.is_set():
        await asyncio.sleep(5)
        try:
            c=db(); base.set_state(c,'v514_hot_lane',{
              'mode':'SAMPLED_HOT_TOKEN_HTTP','create_sample_mod':CREATE_SAMPLE_MOD,'sample_bucket':CREATE_SAMPLE_BUCKET,
              'hot_ttl_s':HOT_TTL_S,'target_rps':TARGET_RPS,'queue':queue.qsize(),**counters,'updated_at':time.time()}); c.commit(); c.close()
        except Exception:pass


async def main_async():
    if not KEY:raise SystemExit('HELIUS_API_KEY is not set in this terminal.')
    base.initialize(); queue=asyncio.Queue(maxsize=QUEUE_MAX); subscribe_cmd=asyncio.Queue(); inflight=set()
    counters={k:0 for k in ('global_logs','creates_seen','creates_sampled','create_fetched','create_no_mint','mints_found','hot_subscribed','hot_unsubscribed','hot_capacity_reject','hot_logs','hot_swaps','fetched','inserted','429','fetch_fail','queue_drops','worker_errors','subscribe_errors','reconnects')}
    connector=aiohttp.TCPConnector(limit=max(HTTP_CONCURRENCY*2,12),ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector,headers={'Content-Type':'application/json'}) as session:
        workers=[asyncio.create_task(fetch_worker(i,session,queue,subscribe_cmd,counters,inflight)) for i in range(HTTP_CONCURRENCY)]
        tasks=workers+[asyncio.create_task(websocket_loop(queue,inflight,subscribe_cmd,counters)),asyncio.create_task(telemetry(counters,queue))]
        try:await STOP.wait()
        finally:
            STOP.set()
            for t in tasks:t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)


def main():
    print(f'V5.1.4 HOT TOKEN lane | sample=1/{CREATE_SAMPLE_MOD} | ttl={HOT_TTL_S:.0f}s | HTTP={TARGET_RPS:.1f} rps',flush=True)
    try:asyncio.run(main_async())
    except KeyboardInterrupt:pass

if __name__=='__main__':main()
