#!/usr/bin/env python3
"""MEMECOIN LAB — ISOLATED CAUSAL FEATURE ENGINE V7.5.5

Purpose
-------
Run the canonical V5.2.2 decoder and the future-only causal snapshot scheduler
without any legacy mutable feature rebuild in the hot loop.

This removes the main source of self-inflicted latency observed in V7.5.3:
legacy upsert work could hold the SQLite writer and stall heartbeat/materialization.

Scientific invariants
---------------------
- brand-new table v755_causal_snapshots;
- activation cutoff frozen once on clean start;
- no pre-start token may enter the causal rail;
- each stage snapshot is INSERT ONLY and immutable;
- build_lag is measured against true first-swap + stage;
- no strategy thresholds are changed.

Research only. Never signs or submits transactions.
"""
from __future__ import annotations
import os, signal, time, sqlite3
from pathlib import Path

os.environ.setdefault('MEMECOIN_V522_BATCH','5000')
os.environ.setdefault('MEMECOIN_V522_POLL_S','0.02')

import v52_decode_features as old
import v522_canonical_price_decoder as canon

ROOT=Path.home()/"memecoin_lab"
V5=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
POLL=float(os.environ.get('MEMECOIN_V755_POLL_S','0.03'))
SCHED_EVERY=float(os.environ.get('MEMECOIN_V755_SCHED_S','0.10'))
HEALTH_EVERY=float(os.environ.get('MEMECOIN_V755_HEALTH_S','1.0'))
LOOKBACK=float(os.environ.get('MEMECOIN_V755_LOOKBACK_S','600'))
STAGES=tuple(sorted(set(old.SNAPSHOTS)))
STOP=False
ACTIVATION_CUTOFF=0.0
STARTED_AT=0.0

def stop(*_):
 global STOP; STOP=True

def db():
 d=old.open_feature(); d.execute('PRAGMA busy_timeout=30000'); return d

def init_schema():
 old.initialize(); d=db(); d.executescript('''
 CREATE TABLE IF NOT EXISTS v755_causal_snapshots(
   token_mint TEXT NOT NULL, stage_s INTEGER NOT NULL, first_ts REAL NOT NULL,
   cutoff_ts REAL NOT NULL, built_at REAL NOT NULL, build_lag_s REAL NOT NULL,
   source_swaps INTEGER NOT NULL, source_max_raw_observed_at REAL,
   source_max_processed_at REAL,
   swaps INTEGER NOT NULL, buys INTEGER NOT NULL, sells INTEGER NOT NULL,
   buy_ratio REAL, gross_sol REAL, net_sol REAL, unique_wallets INTEGER,
   repeat_wallet_ratio REAL, wallet_hhi REAL, wallet_top1_share REAL,
   avg_trade_sol REAL, max_trade_sol REAL, trade_hhi REAL, top1_trade_share REAL,
   return_pct REAL, range_pct REAL, flow_velocity REAL, flow_acceleration REAL,
   buy_ratio_delta REAL, price_velocity REAL,
   PRIMARY KEY(token_mint,stage_s));
 CREATE INDEX IF NOT EXISTS idx_v755_stage_cutoff ON v755_causal_snapshots(stage_s,cutoff_ts);
 CREATE INDEX IF NOT EXISTS idx_v755_built ON v755_causal_snapshots(built_at);
 CREATE TABLE IF NOT EXISTS v755_engine_state(
   id INTEGER PRIMARY KEY CHECK(id=1), started_at REAL, activation_cutoff REAL,
   heartbeat_at REAL, decoded_rows INTEGER, decoded_swaps INTEGER,
   causal_inserted INTEGER, raw_pending INTEGER, recent_n INTEGER,
   recent_p50_lag REAL, recent_p90_lag REAL, recent_p95_lag REAL, note TEXT);
 '''); d.commit(); d.close()

def freeze_activation():
 d=db(); n=d.execute('SELECT COUNT(*) FROM v755_causal_snapshots').fetchone()[0]
 s=d.execute('SELECT * FROM v755_engine_state WHERE id=1').fetchone()
 if s and n:
  cut=float(s['activation_cutoff']); started=float(s['started_at']); d.close(); return started,cut
 if n:
  d.close(); raise RuntimeError('v755 causal rows exist without durable activation state')
 z=d.execute('SELECT MAX(timestamp) FROM v52_swaps').fetchone()[0]
 cut=float(z) if z is not None else time.time(); started=time.time()
 d.execute('INSERT OR REPLACE INTO v755_engine_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
   (1,started,cut,started,0,0,0,0,0,None,None,None,'clean activation; isolated causal rail'))
 d.commit(); d.close(); return started,cut

def raw_pending():
 try:
  d=db(); d.execute('ATTACH DATABASE ? AS raw',(str(V5),))
  n=d.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions r
                 LEFT JOIN main.v52_processed p ON p.signature=r.signature
                 WHERE p.signature IS NULL''').fetchone()[0]
  d.close(); return int(n)
 except Exception:return -1

def due_tokens(now):
 d=db(); rs=d.execute('''SELECT token_mint,MIN(timestamp) first_ts
                          FROM v52_swaps
                          WHERE timestamp>? AND timestamp>=?
                          GROUP BY token_mint''',(ACTIVATION_CUTOFF,now-LOOKBACK)).fetchall(); d.close()
 return [(str(r['token_mint']),float(r['first_ts'])) for r in rs if r['first_ts'] is not None and float(r['first_ts'])>ACTIVATION_CUTOFF]

def insert_token(d,mint,now):
 first=d.execute('SELECT MIN(timestamp) FROM v52_swaps WHERE token_mint=?',(mint,)).fetchone()[0]
 if first is None or float(first)<=ACTIVATION_CUTOFF:return 0
 first=float(first)
 rows=d.execute('SELECT * FROM v52_swaps WHERE token_mint=? ORDER BY timestamp',(mint,)).fetchall()
 if not rows:return 0
 made=0
 cols=['token_mint','stage_s','cutoff_ts','swaps','buys','sells','buy_ratio','gross_sol','net_sol','unique_wallets','repeat_wallet_ratio','wallet_hhi','wallet_top1_share','avg_trade_sol','max_trade_sol','trade_hhi','top1_trade_share','return_pct','range_pct','flow_velocity','flow_acceleration','buy_ratio_delta','price_velocity']
 for st in STAGES:
  cut=first+st
  if now<cut:continue
  if d.execute('SELECT 1 FROM v755_causal_snapshots WHERE token_mint=? AND stage_s=?',(mint,st)).fetchone():continue
  m=old.snapshot_metrics(rows,first,st)
  if not m:continue
  used=[r for r in rows if float(r['timestamp'])<=cut]
  maxobs=max((float(r['observed_at']) for r in used if r['observed_at'] is not None),default=None)
  sigs=[str(r['signature']) for r in used]; maxproc=None
  for i in range(0,len(sigs),400):
   ch=sigs[i:i+400]
   if not ch:continue
   q=','.join('?' for _ in ch)
   z=d.execute(f'SELECT MAX(processed_at) FROM v52_processed WHERE signature IN ({q})',ch).fetchone()[0]
   if z is not None:maxproc=max(float(z),maxproc or float(z))
  vals=[m[c] for c in cols]
  d.execute(f'''INSERT OR IGNORE INTO v755_causal_snapshots(
     token_mint,stage_s,first_ts,cutoff_ts,built_at,build_lag_s,source_swaps,
     source_max_raw_observed_at,source_max_processed_at,{','.join(cols[3:])})
     VALUES({','.join('?' for _ in range(9+len(cols)-3))})''',
     [mint,st,first,cut,now,max(0.0,now-cut),len(used),maxobs,maxproc]+vals[3:])
  if d.execute('SELECT changes()').fetchone()[0]:made+=1
 return made

def schedule():
 now=time.time(); toks=due_tokens(now)
 if not toks:return 0
 d=db(); made=0; d.execute('BEGIN IMMEDIATE')
 try:
  for mint,_ in toks: made+=insert_token(d,mint,now)
  d.commit()
 except BaseException:d.rollback(); raise
 finally:d.close()
 return made

def pct(xs,q):
 if not xs:return None
 ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def heartbeat(decoded_rows,decoded_swaps,causal):
 now=time.time(); d=db(); xs=[float(r[0]) for r in d.execute('SELECT build_lag_s FROM v755_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-60,)).fetchall()]
 p50,p90,p95=pct(xs,.5),pct(xs,.9),pct(xs,.95); rp=raw_pending()
 d.execute('INSERT OR REPLACE INTO v755_engine_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
   (1,STARTED_AT,ACTIVATION_CUTOFF,now,decoded_rows,decoded_swaps,causal,rp,len(xs),p50,p90,p95,'isolated decode + causal scheduler; no legacy rebuild in hot loop'))
 d.commit(); d.close(); return rp,len(xs),p50,p90,p95

def main():
 global ACTIVATION_CUTOFF,STARTED_AT
 signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
 init_schema(); canon.persist_activation_watermark(); STARTED_AT,ACTIVATION_CUTOFF=freeze_activation()
 decoded_rows=decoded_swaps=causal=0; last_sched=last_health=0.0
 print('MEMECOIN LAB V7.5.5 ISOLATED CAUSAL FEATURE ENGINE',flush=True)
 print(f'activation_cutoff>{ACTIVATION_CUTOFF:.3f} | stages={STAGES} | table=v755_causal_snapshots',flush=True)
 while not STOP:
  try:
   n,sw,q,c=canon.decode_batch(); decoded_rows+=n; decoded_swaps+=sw; now=time.time()
   if now-last_sched>=SCHED_EVERY: causal+=schedule(); last_sched=now
   if now-last_health>=HEALTH_EVERY:
    rp,rn,p50,p90,p95=heartbeat(decoded_rows,decoded_swaps,causal); last_health=now
    print(f'V755 decoded={decoded_rows} swaps={decoded_swaps} raw_pending={rp} | causal={causal} recent20/30 n={rn} lag p50/p90/p95={p50}/{p90}/{p95}',flush=True)
  except Exception as e:
   print('V755 error:',repr(e),flush=True); time.sleep(.2)
  time.sleep(POLL)

if __name__=='__main__':main()
