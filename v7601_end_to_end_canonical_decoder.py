#!/usr/bin/env python3
"""MEMECOIN LAB — END-TO-END CANONICAL DECODER V7.6.0.1

Purpose
-------
Replace V7.6.0 with a micro-batched, end-to-end instrumented canonical decoder.
It preserves V5.2.2 canonical swap semantics and price quarantine while:
- using smaller continuous batches to reduce long fetch/commit gaps;
- measuring fetch, prepare, DB write, pending-count and full-cycle latency;
- avoiding the expensive raw_pending anti-join on every single decode cycle;
- keeping feature/snapshot work fully outside the decoder hot path.

Writes only canonical decoder tables. Research infrastructure only.
"""
from __future__ import annotations
import os, signal, time
from concurrent.futures import ThreadPoolExecutor

os.environ.setdefault('MEMECOIN_V521_BATCH','2000')

import v52_decode_features as old
import v521_fast_decoder_features as fast
import v522_canonical_price_decoder as canon

WORKERS=int(os.environ.get('MEMECOIN_V7601_WORKERS','12'))
POLL=float(os.environ.get('MEMECOIN_V7601_POLL_S','0.002'))
PENDING_EVERY=float(os.environ.get('MEMECOIN_V7601_PENDING_EVERY','0.50'))
STOP=False

def stop(*_):
 global STOP; STOP=True

def raw_pending(db):
 try:
  db.execute('ATTACH DATABASE ? AS raw',(str(fast.V5_DB),))
 except Exception:
  pass
 return int(db.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions r
      LEFT JOIN main.v52_processed p ON p.signature=r.signature
      WHERE p.signature IS NULL''').fetchone()[0])

def prepare(row):
 status='IGNORED'; reason=None; sw=None; evt=None
 try:
  payload=old.decode_payload(row); event=(row['event_hint'] or 'OTHER').upper(); tx=payload.get('rpc_transaction') or {}
  ts=float(tx.get('blockTime') or row['observed_at'])
  if event in ('CREATE','MIGRATE'):
   evt=(row['signature'],row['token_hint'],ts,event,row['source_program'],row['creator_hint'])
  if event in ('BUY','SELL'):
   sw,reason=canon.canonical_swap(payload,row); status='SWAP_CANDIDATE' if sw else 'UNDECODED'
  elif event in ('CREATE','MIGRATE'):
   status=event
  else:
   status='OTHER'
 except Exception as e:
  status='ERROR'; reason=repr(e)
 return row,status,reason,sw,evt

def fetch_rows(db):
 t=time.perf_counter(); rows=fast.fetch_new(db); return rows,time.perf_counter()-t

def process_batch(pool):
 db=old.open_feature()
 cycle0=time.perf_counter()
 rows,fetch_s=fetch_rows(db)
 if not rows:
  db.close(); return {'n':0,'d':0,'q':0,'fetch':fetch_s,'prep':0.0,'db':0.0,'cycle':time.perf_counter()-cycle0}
 t0=time.perf_counter(); prepared=list(pool.map(prepare,rows)); prep_s=time.perf_counter()-t0
 dec=quar=0; now=time.time(); t1=time.perf_counter(); db.execute('BEGIN IMMEDIATE')
 try:
  for row,status,reason,sw,evt in prepared:
   if evt:
    db.execute('INSERT OR IGNORE INTO v52_token_events(signature,token_mint,timestamp,event_type,source_program,wallet_hint) VALUES(?,?,?,?,?,?)',evt)
   if sw is not None:
    ok,why=canon.gate(db,sw)
    if ok:
     db.execute('''INSERT OR IGNORE INTO v52_swaps(signature,token_mint,timestamp,slot,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence,observed_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',tuple(sw[k] for k in ('signature','token_mint','timestamp','slot','source_program','wallet','side','token_amount','quote_sol','price_sol','confidence','observed_at')))
     status='SWAP'; dec+=1
    else:
     status='PRICE_QUARANTINE'; reason=why; quar+=1
   db.execute('INSERT OR REPLACE INTO v52_processed(signature,processed_at,status,reason) VALUES(?,?,?,?)',(row['signature'],now,status,reason))
  old.state(db,'v7601_decoder',{'at':now,'rows':len(rows),'swaps':dec,'quarantine':quar,'workers':WORKERS,'batch':fast.BATCH})
  db.commit()
 except BaseException:
  db.rollback(); raise
 db_s=time.perf_counter()-t1; db.close(); cycle_s=time.perf_counter()-cycle0
 return {'n':len(rows),'d':dec,'q':quar,'fetch':fetch_s,'prep':prep_s,'db':db_s,'cycle':cycle_s}

def main():
 signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
 old.initialize(); canon.persist_activation_watermark()
 print(f'MEMECOIN LAB V7.6.0.1 END-TO-END CANONICAL DECODER | workers={WORKERS} batch={fast.BATCH}',flush=True)
 total=swaps=0; last_pending_at=0.0; last_pending=None
 with ThreadPoolExecutor(max_workers=WORKERS,thread_name_prefix='v7601decode') as pool:
  while not STOP:
   try:
    r=process_batch(pool); total+=r['n']; swaps+=r['d']; now=time.time(); pending_s=None
    if now-last_pending_at>=PENDING_EVERY:
     d=old.open_feature(); t=time.perf_counter(); last_pending=raw_pending(d); pending_s=time.perf_counter()-t; d.close(); last_pending_at=now
    eff=(r['n']/r['cycle']) if r['cycle']>0 else 0.0
    if r['n'] or (last_pending is not None and last_pending>0):
     ptxt=f'{last_pending}' if last_pending is not None else '?'; qtxt=f'{pending_s:.3f}s' if pending_s is not None else '-'
     print(f"V7601 total={total} swaps={swaps} batch={r['n']}/{r['d']} quarantine={r['q']} raw_pending={ptxt} | fetch={r['fetch']:.3f}s prep={r['prep']:.3f}s db={r['db']:.3f}s pending_q={qtxt} cycle={r['cycle']:.3f}s effective={eff:.1f} rows/s",flush=True)
   except Exception as e:
    print('V7601 error:',repr(e),flush=True); time.sleep(.1)
   time.sleep(POLL)

if __name__=='__main__':main()
