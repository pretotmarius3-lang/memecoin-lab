#!/usr/bin/env python3
"""Memecoin Lab V5.1.3 — full-transaction WebSocket LIVE lane.

Removes HTTP getTransaction from the live critical path.

Helius transactionSubscribe streams full Pump / PumpSwap transactions directly
through WebSocket. Relevant CREATE/MIGRATE/BUY/SELL transactions are batched
into v5_raw_transactions and duplicate spool signatures are marked DONE.
Historical backlog remains the responsibility of V5.3.1.

Research only. No signing or transaction submission.
"""
from __future__ import annotations

import asyncio, json, os, signal, sqlite3, sys, time, zlib
from pathlib import Path
from urllib.parse import quote

try:
    import websockets
except ImportError:
    raise SystemExit("Missing dependency: pip install websockets")

import v51_spool_collector as base

ROOT=Path.home()/"memecoin_lab"
DB_PATH=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
API_KEY=(os.environ.get("HELIUS_API_KEY_2") or os.environ.get("HELIUS_API_KEY") or "").strip()
KEY_NAME="HELIUS_API_KEY_2" if os.environ.get("HELIUS_API_KEY_2") else "HELIUS_API_KEY"
WS_BASE=os.environ.get("HELIUS_WS_BASE","wss://mainnet.helius-rpc.com/")
COMMITMENT=os.environ.get("MEMECOIN_V513_COMMITMENT","confirmed")
QUEUE_MAX=int(os.environ.get("MEMECOIN_V513_QUEUE_MAX","30000"))
BATCH_SIZE=int(os.environ.get("MEMECOIN_V513_DB_BATCH","250"))
FLUSH_S=float(os.environ.get("MEMECOIN_V513_FLUSH_S","0.20"))

PROGRAMS={
    51301:("PUMP",base.PUMP_PROGRAM),
    51302:("PUMPSWAP",base.PUMP_AMM_PROGRAM),
}
STOP=asyncio.Event()


def stop(*_):
    STOP.set()


def normalize_result(result,source):
    """Return one v5_raw_transactions record or (None, reason)."""
    if not isinstance(result,dict): return None,"result_not_dict"
    signature=result.get("signature")
    slot=result.get("slot")
    tx=result.get("transaction")
    if not signature or not isinstance(tx,dict):
        return None,"non_jsonparsed_transaction"

    meta=tx.get("meta") or {}
    logs=meta.get("logMessages") or []
    event=base.infer_event_hint(logs)
    if event not in ("CREATE","MIGRATE","BUY","SELL"):
        return None,"other_event"

    try:
        token,creator=base.hints(tx,source,event)
    except Exception:
        token,creator=None,None

    now=time.time()
    payload={
        "signature":signature,
        "slot":slot,
        "logs":logs,
        "rpc_transaction":tx,
        "v513_receive_time":now,
        "v513_transport":"helius_transactionSubscribe_full",
    }
    raw=json.dumps(payload,separators=(",",":"),ensure_ascii=False).encode()
    comp=zlib.compress(raw,3)
    return {
        "signature":signature,"source_program":source,
        "source_program_id":PROGRAMS[51301][1] if source=="PUMP" else PROGRAMS[51302][1],
        "subscription_id":None,"slot":slot,"transaction_index":result.get("transactionIndex"),
        "observed_at":now,"event_hint":event,"token_hint":token,"creator_hint":creator,
        "payload_zlib":sqlite3.Binary(comp),"payload_bytes":len(raw),"compressed_bytes":len(comp),
    },None


def write_batch(items,counters):
    if not items:return
    db=base.open_db(); now=time.time()
    try:
        db.execute("BEGIN IMMEDIATE")
        inserted=0
        for x in items:
            before=db.total_changes
            db.execute("""INSERT OR IGNORE INTO v5_raw_transactions(
              signature,source_program,source_program_id,subscription_id,slot,transaction_index,
              observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
              x["signature"],x["source_program"],x["source_program_id"],x["subscription_id"],x["slot"],x["transaction_index"],
              x["observed_at"],x["event_hint"],x["token_hint"],x["creator_hint"],x["payload_zlib"],x["payload_bytes"],x["compressed_bytes"]))
            inserted += db.total_changes-before
            # If the old logs collector also saw this signature, prevent a later HTTP refetch.
            db.execute("""UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=?
                          WHERE signature=? AND status IN ('PENDING','FETCHING')""",(now,x["signature"]))
        counters["inserted"]+=inserted
        counters["duplicates"]+=len(items)-inserted
        try:
            base.set_state(db,"v513_live",{
                "transport":"transactionSubscribe_full",
                "key":KEY_NAME,"received":counters["received"],"relevant":counters["relevant"],
                "inserted":counters["inserted"],"duplicates":counters["duplicates"],"updated_at":now,
            })
            base.set_state(db,"last_write_at",now)
        except Exception: pass
        db.commit()
    except BaseException:
        db.rollback(); raise
    finally:
        db.close()


async def writer(queue,counters):
    batch=[]; deadline=time.monotonic()+FLUSH_S
    while not STOP.is_set() or not queue.empty() or batch:
        timeout=max(0.0,deadline-time.monotonic())
        try:
            item=await asyncio.wait_for(queue.get(),timeout=timeout)
            batch.append(item)
        except asyncio.TimeoutError:
            pass
        if len(batch)>=BATCH_SIZE or time.monotonic()>=deadline or (STOP.is_set() and queue.empty()):
            if batch:
                try:
                    await asyncio.to_thread(write_batch,batch,counters)
                except Exception as exc:
                    counters["db_errors"]+=1
                    print(f"V5.1.3 DB write error: {exc!r}",flush=True)
                    await asyncio.sleep(.25)
                batch=[]
            deadline=time.monotonic()+FLUSH_S


async def subscribe(queue,counters):
    url=f"{WS_BASE}?api-key={quote(API_KEY)}"
    req_map={rid:pair for rid,pair in PROGRAMS.items()}; subs={}
    async with websockets.connect(url,ping_interval=15,ping_timeout=30,close_timeout=10,max_size=None,max_queue=32768) as ws:
        print(f"V5.1.3 connected | provider={KEY_NAME} | full transactionSubscribe",flush=True)
        for rid,(source,pid) in PROGRAMS.items():
            req={
                "jsonrpc":"2.0","id":rid,"method":"transactionSubscribe",
                "params":[
                    {"failed":False,"vote":False,"accountInclude":[pid]},
                    {"commitment":COMMITMENT,"encoding":"jsonParsed","transactionDetails":"full",
                     "showRewards":False,"maxSupportedTransactionVersion":0}
                ]
            }
            await ws.send(json.dumps(req,separators=(",",":")))

        async for raw in ws:
            if STOP.is_set():break
            msg=json.loads(raw)
            if "id" in msg and msg.get("id") in req_map:
                if msg.get("error"): raise RuntimeError(f"subscribe {req_map[msg['id']][0]}: {msg['error']}")
                source,pid=req_map[msg["id"]]; sid=int(msg["result"]); subs[sid]=(source,pid)
                print(f"Subscribed FULLTX {source:<8} id={sid}",flush=True); continue
            if msg.get("method")!="transactionNotification":continue
            params=msg.get("params") or {}; sid=params.get("subscription"); result=params.get("result") or {}
            source,_=subs.get(int(sid),("UNKNOWN","UNKNOWN")) if sid is not None else ("UNKNOWN","UNKNOWN")
            counters["received"]+=1
            rec,reason=normalize_result(result,source)
            if rec is None:
                counters[reason]=counters.get(reason,0)+1
            else:
                counters["relevant"]+=1
                try: queue.put_nowait(rec)
                except asyncio.QueueFull:
                    counters["queue_drops"]+=1
            if counters["received"]%1000==0:
                print(f"fulltx={counters['received']:,} relevant={counters['relevant']:,} inserted={counters['inserted']:,} dup={counters['duplicates']:,} q={queue.qsize():,} shape_fail={counters.get('non_jsonparsed_transaction',0):,} drops={counters['queue_drops']:,}",flush=True)


async def main_async():
    if not API_KEY: raise SystemExit("No Helius key loaded (HELIUS_API_KEY_2 preferred).")
    base.initialize()
    q=asyncio.Queue(maxsize=QUEUE_MAX)
    counters={"received":0,"relevant":0,"inserted":0,"duplicates":0,"queue_drops":0,"db_errors":0}
    wt=asyncio.create_task(writer(q,counters)); backoff=1.0
    try:
        while not STOP.is_set():
            try:
                await subscribe(q,counters); backoff=1.0
                if not STOP.is_set():raise RuntimeError("WebSocket closed")
            except asyncio.CancelledError:raise
            except Exception as exc:
                print(f"V5.1.3 WebSocket error: {exc!r} | reconnect in {backoff:.0f}s",flush=True)
                try:await asyncio.wait_for(STOP.wait(),timeout=backoff)
                except asyncio.TimeoutError:pass
                backoff=min(30.0,backoff*2)
    finally:
        STOP.set(); await wt


def main():
    if sys.platform!="win32":
        loop=asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        for s in (signal.SIGINT,signal.SIGTERM):
            try: loop.add_signal_handler(s,STOP.set)
            except Exception: pass
        try: loop.run_until_complete(main_async())
        finally: loop.close()
    else:
        asyncio.run(main_async())

if __name__=="__main__":main()
