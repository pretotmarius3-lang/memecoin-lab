#!/usr/bin/env python3
"""Memecoin Lab V5.1.6 — Alchemy priority fetcher.

Uses Alchemy PAYG as the primary HTTP getTransaction engine for the existing
V5.1.5 durable HOT queue. Helius remains available to the discovery/streaming
lane and can be kept as fallback by the surrounding stack.

Scientific invariants:
- does not change sampling/freeze/rules/outcomes
- CREATE first, then HOT signatures FIFO
- writes only raw source-of-truth transactions and durable queue state
- records provider telemetry for cost/throughput dashboards

Research only. Never signs/submits transactions.
"""
from __future__ import annotations
import asyncio, json, os, random, sqlite3, time, zlib
from pathlib import Path
from urllib.parse import quote
import aiohttp
import v51_spool_collector as base

ROOT=Path.home()/"memecoin_lab"
DB=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
KEY=(os.environ.get("ALCHEMY_API_KEY") or "").strip()
RPC=os.environ.get("ALCHEMY_SOLANA_RPC_URL") or (f"https://solana-mainnet.g.alchemy.com/v2/{quote(KEY)}" if KEY else "")
RPS=max(.2,float(os.environ.get("MEMECOIN_V516_ALCHEMY_RPS","8")))
MAX_RPS=max(RPS,float(os.environ.get("MEMECOIN_V516_ALCHEMY_MAX_RPS","20")))
WORKERS=max(1,int(os.environ.get("MEMECOIN_V516_WORKERS","12")))
TIMEOUT=float(os.environ.get("MEMECOIN_V516_TIMEOUT","15"))
LEASE=float(os.environ.get("MEMECOIN_V516_LEASE","45"))
MAX_RETRIES=max(1,int(os.environ.get("MEMECOIN_V516_RETRIES","8")))
COMMITMENT=os.environ.get("MEMECOIN_V5_COMMITMENT","confirmed")
STOP=asyncio.Event(); RATE_LOCK=asyncio.Lock(); NEXT_AT=0.; CURRENT_RPS=RPS

def db():
 c=sqlite3.connect(DB,timeout=30);c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA busy_timeout=30000");return c

def init():
 if not KEY and not os.environ.get("ALCHEMY_SOLANA_RPC_URL"):raise SystemExit("Set ALCHEMY_API_KEY or ALCHEMY_SOLANA_RPC_URL")
 c=db();c.executescript('''
 CREATE TABLE IF NOT EXISTS v516_provider_stats(
   provider TEXT PRIMARY KEY, started_at REAL, updated_at REAL, requests INTEGER DEFAULT 0,
   ok INTEGER DEFAULT 0, nulls INTEGER DEFAULT 0, http_429 INTEGER DEFAULT 0,
   errors INTEGER DEFAULT 0, inserted INTEGER DEFAULT 0, last_error TEXT, current_rps REAL);
 INSERT OR IGNORE INTO v516_provider_stats(provider,started_at,updated_at,current_rps) VALUES('ALCHEMY',strftime('%s','now'),strftime('%s','now'),0);
 ''');
 # recover abandoned leases from earlier lane instances
 c.execute("UPDATE v515_hot_queue SET status='PENDING',lease_until=NULL,updated_at=? WHERE status='FETCHING' AND COALESCE(lease_until,0)<?",(time.time(),time.time()));c.commit();c.close()

def claim():
 c=db();now=time.time();c.execute("BEGIN IMMEDIATE")
 try:
  r=c.execute("""SELECT * FROM v515_hot_queue WHERE status='PENDING' AND attempts<?
  ORDER BY CASE kind WHEN 'CREATE' THEN 0 ELSE 1 END, first_seen ASC LIMIT 1""",(MAX_RETRIES,)).fetchone()
  if not r:c.commit();return None
  q=c.execute("UPDATE v515_hot_queue SET status='FETCHING',attempts=attempts+1,lease_until=?,updated_at=? WHERE signature=? AND status='PENDING'",(now+LEASE,now,r['signature']))
  if q.rowcount!=1:c.rollback();return None
  x=dict(r);x['attempts']=int(r['attempts'])+1;c.commit();return x
 finally:c.close()

def retry(x,e,terminal=False):
 st='FAILED' if terminal or x['attempts']>=MAX_RETRIES else 'PENDING';c=db();c.execute("UPDATE v515_hot_queue SET status=?,lease_until=NULL,last_error=?,updated_at=? WHERE signature=?",(st,str(e)[-1000:],time.time(),x['signature']));c.commit();c.close()
def done(sig):
 c=db();c.execute("UPDATE v515_hot_queue SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=?",(time.time(),sig));c.commit();c.close()
def stat(**kw):
 c=db();sets=["updated_at=?","current_rps=?"];vals=[time.time(),CURRENT_RPS]
 for k,v in kw.items():
  if k in ('requests','ok','nulls','http_429','errors','inserted'):sets.append(f"{k}={k}+?");vals.append(v)
  elif k=='last_error':sets.append("last_error=?");vals.append(str(v)[-1000:])
 vals.append('ALCHEMY');c.execute("UPDATE v516_provider_stats SET "+','.join(sets)+" WHERE provider=?",vals);c.commit();c.close()
def store(x,tx):
 logs=(tx.get('meta') or {}).get('logMessages') or json.loads(x.get('logs_json') or '[]');event='CREATE' if x.get('kind')=='CREATE' else base.infer_event_hint(logs)
 keys=set(base.account_keys(tx));source='PUMPSWAP' if base.PUMP_AMM_PROGRAM in keys else 'PUMP';token,creator=base.hints(tx,source,event)
 payload={'signature':x['signature'],'slot':x.get('slot'),'logs':logs,'rpc_transaction':tx,'v516_provider':'ALCHEMY','v515_epoch':x.get('epoch_id')};raw=json.dumps(payload,separators=(',',':'),ensure_ascii=False).encode();comp=zlib.compress(raw,3);now=time.time();c=db();before=c.total_changes
 pid=base.PUMP_AMM_PROGRAM if source=='PUMPSWAP' else base.PUMP_PROGRAM
 c.execute('''INSERT OR IGNORE INTO v5_raw_transactions(signature,source_program,source_program_id,subscription_id,slot,transaction_index,observed_at,event_hint,token_hint,creator_hint,payload_zlib,payload_bytes,compressed_bytes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',(x['signature'],source,pid,None,x.get('slot'),None,now,event,token,creator,sqlite3.Binary(comp),len(raw),len(comp)))
 ins=c.total_changes-before;c.execute("UPDATE v51_signature_spool SET status='DONE',lease_until=NULL,last_error=NULL,updated_at=? WHERE signature=? AND status IN ('PENDING','FETCHING')",(now,x['signature']));c.commit();c.close();return ins
async def pace():
 global NEXT_AT
 async with RATE_LOCK:
  w=max(0,NEXT_AT-time.monotonic())
  if w:await asyncio.sleep(w)
  NEXT_AT=max(time.monotonic(),NEXT_AT)+1/max(.2,CURRENT_RPS)
async def fetch(s,sig):
 await pace();body={'jsonrpc':'2.0','id':sig[-12:],'method':'getTransaction','params':[sig,{'encoding':'jsonParsed','commitment':COMMITMENT,'maxSupportedTransactionVersion':0}]};stat(requests=1)
 try:
  async with s.post(RPC,json=body,timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
   if r.status==429:stat(http_429=1,last_error='HTTP429');return '429',None
   if r.status in (401,403):stat(errors=1,last_error=f'HTTP{r.status}');return 'AUTH',None
   if r.status!=200:stat(errors=1,last_error=f'HTTP{r.status}');return 'ERR',None
   d=await r.json(content_type=None)
  if d.get('error'):stat(errors=1,last_error=d['error']);return 'ERR',None
  if d.get('result') is None:stat(nulls=1);return 'NULL',None
  stat(ok=1);return 'OK',d['result']
 except Exception as e:stat(errors=1,last_error=repr(e));return 'ERR',None
async def worker(s):
 while not STOP.is_set():
  x=await asyncio.to_thread(claim)
  if not x:await asyncio.sleep(.05);continue
  state=tx=None
  for n in range(4):
   state,tx=await fetch(s,x['signature'])
   if state=='OK':break
   if state=='AUTH':break
   await asyncio.sleep(.15*(n+1)+random.random()*.1)
  if state!='OK':await asyncio.to_thread(retry,x,state,state=='AUTH');continue
  ins=await asyncio.to_thread(store,x,tx);await asyncio.to_thread(done,x['signature']);stat(inserted=ins)
async def controller():
 global CURRENT_RPS
 last429=0
 while not STOP.is_set():
  await asyncio.sleep(20);c=db();q=c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0];s=c.execute("SELECT http_429 FROM v516_provider_stats WHERE provider='ALCHEMY'").fetchone();c.close();n429=int(s[0] or 0)
  if n429>last429:CURRENT_RPS=max(RPS,CURRENT_RPS*.75)
  elif q>100 and CURRENT_RPS<MAX_RPS:CURRENT_RPS=min(MAX_RPS,CURRENT_RPS+1)
  elif q<10 and CURRENT_RPS>RPS:CURRENT_RPS=max(RPS,CURRENT_RPS-1)
  last429=n429
async def reporter():
 while not STOP.is_set():
  await asyncio.sleep(10);c=db();q=c.execute("SELECT COUNT(*) FROM v515_hot_queue WHERE status='PENDING'").fetchone()[0];x=dict(c.execute("SELECT * FROM v516_provider_stats WHERE provider='ALCHEMY'").fetchone());c.close();print(f"ALCHEMY requests={x['requests']:,} ok={x['ok']:,} inserted={x['inserted']:,} null={x['nulls']:,} 429={x['http_429']:,} errors={x['errors']:,} q={q:,} debt={q/max(.2,CURRENT_RPS):.1f}s rps={CURRENT_RPS:.1f}",flush=True)
async def main():
 init();print(f"V5.1.6 ALCHEMY PRIORITY FETCHER | workers={WORKERS} | rps={RPS:.1f}->{MAX_RPS:.1f} | durable v515 queue",flush=True)
 async with aiohttp.ClientSession() as s:
  tasks=[asyncio.create_task(worker(s)) for _ in range(WORKERS)]+[asyncio.create_task(controller()),asyncio.create_task(reporter())]
  try:await asyncio.gather(*tasks)
  finally:
   STOP.set()
   for t in tasks:t.cancel()
if __name__=='__main__':asyncio.run(main())
