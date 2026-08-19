#!/usr/bin/env python3
"""MEMECOIN LAB — HIGH-CAPACITY CANONICAL DECODER V7.6.0

Infrastructure-only decoder. It preserves V5.2.2 canonical swap semantics and
price quarantine, but removes all feature/snapshot work from the hot path and
parallelizes payload decompression + canonical swap parsing.

Writes only existing canonical tables:
- v52_processed
- v52_swaps
- v52_token_events
- v52_state heartbeat

No causal snapshots, no strategy logic, no trading.
"""
from __future__ import annotations
import os, signal, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

os.environ.setdefault('MEMECOIN_V521_BATCH','10000')

import v52_decode_features as old
import v521_fast_decoder_features as fast
import v522_canonical_price_decoder as canon

ROOT=Path.home()/"memecoin_lab"
WORKERS=int(os.environ.get('MEMECOIN_V760_WORKERS','12'))
POLL=float(os.environ.get('MEMECOIN_V760_POLL_S','0.01'))
STOP=False

def stop(*_):
 global STOP; STOP=True

def raw_pending(db):
 try:
  db.execute('ATTACH DATABASE ? AS raw',(str(fast.V5_DB),))
 except Exception:pass
 try:
  return int(db.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions r
      LEFT JOIN main.v52_processed p ON p.signature=r.signature
      WHERE p.signature IS NULL''').fetchone()[0])
 except Exception:return -1

def prepare(row):
 """CPU-heavy decode/parse outside SQLite writer transaction."""
 status='IGNORED';reason=None;sw=None;event=None;evt=None
 try:
  payload=old.decode_payload(row);event=(row['event_hint'] or 'OTHER').upper();tx=payload.get('rpc_transaction') or {}
  ts=float(tx.get('blockTime') or row['observed_at'])
  if event in ('CREATE','MIGRATE'):
   evt=(row['signature'],row['token_hint'],ts,event,row['source_program'],row['creator_hint'])
  if event in ('BUY','SELL'):
   sw,reason=canon.canonical_swap(payload,row)
   status='SWAP_CANDIDATE' if sw else 'UNDECODED'
  elif event in ('CREATE','MIGRATE'):status=event
  else:status='OTHER'
 except Exception as e:
  status='ERROR';reason=repr(e)
 return row,status,reason,sw,evt

def decode_batch(pool):
 db=old.open_feature(); rows=fast.fetch_new(db)
 if not rows:
  rp=raw_pending(db);db.close();return 0,0,0,rp,0.0,0.0
 t0=time.perf_counter(); prepared=list(pool.map(prepare,rows)); t1=time.perf_counter()
 dec=quar=0;now=time.time();db.execute('BEGIN IMMEDIATE')
 try:
  for row,status,reason,sw,evt in prepared:
   if evt:
    db.execute('INSERT OR IGNORE INTO v52_token_events(signature,token_mint,timestamp,event_type,source_program,wallet_hint) VALUES(?,?,?,?,?,?)',evt)
   if sw is not None:
    ok,why=canon.gate(db,sw)
    if ok:
     db.execute('''INSERT OR IGNORE INTO v52_swaps(signature,token_mint,timestamp,slot,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence,observed_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',tuple(sw[k] for k in ('signature','token_mint','timestamp','slot','source_program','wallet','side','token_amount','quote_sol','price_sol','confidence','observed_at')))
     status='SWAP';dec+=1
    else:
     status='PRICE_QUARANTINE';reason=why;quar+=1
   db.execute('INSERT OR REPLACE INTO v52_processed(signature,processed_at,status,reason) VALUES(?,?,?,?)',(row['signature'],now,status,reason))
  old.state(db,'v760_decoder',{'at':now,'rows':len(rows),'swaps':dec,'quarantine':quar,'workers':WORKERS})
  db.commit()
 except BaseException:
  db.rollback();raise
 t2=time.perf_counter();rp=raw_pending(db);db.close()
 return len(rows),dec,quar,rp,t1-t0,t2-t1

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);old.initialize();canon.persist_activation_watermark()
 print(f'MEMECOIN LAB V7.6.0 HIGH-CAPACITY CANONICAL DECODER | workers={WORKERS} batch={fast.BATCH}',flush=True)
 total=swaps=0
 with ThreadPoolExecutor(max_workers=WORKERS,thread_name_prefix='v760decode') as pool:
  while not STOP:
   try:
    n,d,q,rp,cpu_s,db_s=decode_batch(pool);total+=n;swaps+=d
    if n or rp:
     rate=(n/(cpu_s+db_s)) if cpu_s+db_s>0 else 0
     print(f'V760 total={total} swaps={swaps} batch={n}/{d} quarantine={q} raw_pending={rp} | prep={cpu_s:.3f}s db={db_s:.3f}s rate={rate:.1f} rows/s',flush=True)
   except Exception as e:
    print('V760 error:',repr(e),flush=True);time.sleep(.2)
   time.sleep(POLL)

if __name__=='__main__':main()
