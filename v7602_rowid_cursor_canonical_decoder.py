#!/usr/bin/env python3
"""MEMECOIN LAB — ROWID-CURSOR CANONICAL DECODER V7.6.0.2

Fixes the dominant V7601 fetch bottleneck. Instead of repeatedly running a
cross-database anti-join + ORDER BY over the entire raw table, this decoder:
  * bootstraps once at the first unprocessed raw rowid;
  * then advances monotonically through raw.v5_raw_transactions by rowid;
  * filters already-processed signatures only inside each small fetched batch;
  * persists a cheap decoder state (cursor, max raw rowid, estimated backlog)
    for the causal scheduler to consume without running its own anti-join.

Canonical swap semantics and price quarantine remain V5.2.2-compatible.
No feature/snapshot work is performed here.
"""
from __future__ import annotations
import json, os, signal, sqlite3, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import v52_decode_features as old
import v521_fast_decoder_features as fast
import v522_canonical_price_decoder as canon

ROOT=Path.home()/"memecoin_lab"
RAW=Path(os.environ.get('MEMECOIN_V5_DB', ROOT/'v5_raw_events.db'))
BATCH=int(os.environ.get('MEMECOIN_V7602_BATCH','1500'))
WORKERS=int(os.environ.get('MEMECOIN_V7602_WORKERS','12'))
POLL=float(os.environ.get('MEMECOIN_V7602_POLL_S','0.002'))
STOP=False

def stop(*_):
 global STOP; STOP=True

def db():
 d=old.open_feature(); d.execute('PRAGMA busy_timeout=30000')
 try:d.execute('ATTACH DATABASE ? AS raw',(str(RAW),))
 except Exception:pass
 return d

def init_schema():
 d=db();d.executescript('''
 CREATE TABLE IF NOT EXISTS v7602_decoder_state(
  id INTEGER PRIMARY KEY CHECK(id=1), started_at REAL, heartbeat_at REAL,
  cursor_rowid INTEGER, max_raw_rowid INTEGER, backlog_est INTEGER,
  total_rows INTEGER, total_swaps INTEGER, last_fetch_s REAL,
  last_prep_s REAL, last_db_s REAL, last_cycle_s REAL, note TEXT);
 ''');d.commit();d.close()

def existing_state(d):
 return d.execute('SELECT * FROM v7602_decoder_state WHERE id=1').fetchone()

def bootstrap_cursor(d):
 """One expensive query only: find first raw row that has not been processed."""
 r=d.execute('''SELECT MIN(r.rowid) FROM raw.v5_raw_transactions r
                LEFT JOIN main.v52_processed p ON p.signature=r.signature
                WHERE p.signature IS NULL''').fetchone()
 first=r[0] if r else None
 if first is None:
  m=d.execute('SELECT COALESCE(MAX(rowid),0) FROM raw.v5_raw_transactions').fetchone()[0]
  return int(m)
 return max(0,int(first)-1)

def fetch_after(d,cursor):
 t=time.perf_counter()
 rows=d.execute('''SELECT rowid AS _rid,signature,source_program,slot,observed_at,event_hint,token_hint,creator_hint,payload_zlib
                  FROM raw.v5_raw_transactions WHERE rowid>? ORDER BY rowid LIMIT ?''',(cursor,BATCH)).fetchall()
 return rows,time.perf_counter()-t

def filter_processed(d,rows):
 if not rows:return rows
 sigs=[str(r['signature']) for r in rows]
 done=set()
 for i in range(0,len(sigs),500):
  ch=sigs[i:i+500];q=','.join('?' for _ in ch)
  done.update(str(x[0]) for x in d.execute(f'SELECT signature FROM v52_processed WHERE signature IN ({q})',ch).fetchall())
 return [r for r in rows if str(r['signature']) not in done]

def prepare(row):
 status='IGNORED';reason=None;sw=None;evt=None
 try:
  payload=old.decode_payload(row);event=(row['event_hint'] or 'OTHER').upper();tx=payload.get('rpc_transaction') or {}
  ts=float(tx.get('blockTime') or row['observed_at'])
  if event in ('CREATE','MIGRATE'):
   evt=(row['signature'],row['token_hint'],ts,event,row['source_program'],row['creator_hint'])
  if event in ('BUY','SELL'):
   sw,reason=canon.canonical_swap(payload,row);status='SWAP_CANDIDATE' if sw else 'UNDECODED'
  elif event in ('CREATE','MIGRATE'):status=event
  else:status='OTHER'
 except Exception as e:status='ERROR';reason=repr(e)
 return row,status,reason,sw,evt

def write_batch(d,prepared):
 dec=quar=0;now=time.time();t=time.perf_counter();d.execute('BEGIN IMMEDIATE')
 try:
  for row,status,reason,sw,evt in prepared:
   if evt:d.execute('INSERT OR IGNORE INTO v52_token_events(signature,token_mint,timestamp,event_type,source_program,wallet_hint) VALUES(?,?,?,?,?,?)',evt)
   if sw is not None:
    ok,why=canon.gate(d,sw)
    if ok:
     d.execute('''INSERT OR IGNORE INTO v52_swaps(signature,token_mint,timestamp,slot,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',tuple(sw[k] for k in ('signature','token_mint','timestamp','slot','source_program','wallet','side','token_amount','quote_sol','price_sol','confidence','observed_at')))
     status='SWAP';dec+=1
    else:status='PRICE_QUARANTINE';reason=why;quar+=1
   d.execute('INSERT OR REPLACE INTO v52_processed(signature,processed_at,status,reason) VALUES(?,?,?,?)',(row['signature'],now,status,reason))
  d.commit()
 except BaseException:d.rollback();raise
 return dec,quar,time.perf_counter()-t

def persist(d,started,cursor,total,swaps,fetch_s,prep_s,db_s,cycle_s):
 maxrid=int(d.execute('SELECT COALESCE(MAX(rowid),0) FROM raw.v5_raw_transactions').fetchone()[0])
 backlog=max(0,maxrid-int(cursor));now=time.time()
 d.execute('''INSERT OR REPLACE INTO v7602_decoder_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',(1,started,now,int(cursor),maxrid,backlog,total,swaps,fetch_s,prep_s,db_s,cycle_s,'rowid cursor; backlog=max_raw_rowid-cursor'))
 d.commit();return maxrid,backlog

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);old.initialize();canon.persist_activation_watermark();init_schema();started=time.time()
 d=db();s=existing_state(d);cursor=int(s['cursor_rowid']) if s and s['cursor_rowid'] is not None else bootstrap_cursor(d);d.close()
 print(f'MEMECOIN LAB V7.6.0.2 ROWID-CURSOR CANONICAL DECODER | workers={WORKERS} batch={BATCH} cursor={cursor}',flush=True)
 total=swaps=0
 with ThreadPoolExecutor(max_workers=WORKERS,thread_name_prefix='v7602decode') as pool:
  while not STOP:
   cycle0=time.perf_counter();d=db()
   try:
    rows,fetch_s=fetch_after(d,cursor)
    if rows:
     lastrid=int(rows[-1]['_rid']);todo=filter_processed(d,rows);t=time.perf_counter();prepared=list(pool.map(prepare,todo));prep_s=time.perf_counter()-t
     dec,quar,db_s=write_batch(d,prepared) if prepared else (0,0,0.0)
     cursor=lastrid;total+=len(todo);swaps+=dec
    else:
     prep_s=db_s=0.0;quar=dec=0
    cycle_s=time.perf_counter()-cycle0;maxrid,backlog=persist(d,started,cursor,total,swaps,fetch_s,prep_s,db_s,cycle_s)
    if rows or backlog:
     eff=(len(rows)/cycle_s) if cycle_s>0 else 0.0
     print(f'V7602 cursor={cursor}/{maxrid} backlog_est={backlog} fetched={len(rows)} todo={len(todo) if rows else 0} swaps={dec} quarantine={quar} | fetch={fetch_s:.4f}s prep={prep_s:.4f}s db={db_s:.4f}s cycle={cycle_s:.4f}s effective={eff:.1f} rawrows/s',flush=True)
   except Exception as e:
    print('V7602 error:',repr(e),flush=True);time.sleep(.1)
   finally:d.close()
   time.sleep(POLL)
if __name__=='__main__':main()
