#!/usr/bin/env python3
"""MEMECOIN LAB — DRAIN-THEN-FREEZE CAUSAL FEATURE ENGINE V7.5.7.1

Fixes two startup-contamination failure modes:

1) old raw rows decoded after feature-engine start could look post-activation;
2) waiting for *total* raw_pending==0 created a moving-target barrier because
   fresh acquisition keeps adding rows while the decoder drains.

V7.5.7.1 therefore freezes a STARTUP RAW WATERMARK at process start. Only raw
rows already present at that instant belong to the drain barrier. Fresh rows may
continue arriving and may even be decoded during warmup, but they do not prevent
the barrier from completing. When all startup-watermark rows are processed for
several consecutive checks, a second wall-clock activation freeze is taken.
Only tokens whose true first decoded swap observed_at is strictly after that
activation freeze can ever enter the insert-only causal rail.

No legacy feature rebuild is performed in the hot loop. Research only.
"""
from __future__ import annotations
import os, signal, sqlite3, time
from pathlib import Path

os.environ.setdefault('MEMECOIN_V522_BATCH','1000')
os.environ.setdefault('MEMECOIN_V522_POLL_S','0.02')

import v52_decode_features as old
import v522_canonical_price_decoder as canon

ROOT=Path.home()/"memecoin_lab"
V5=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
POLL=float(os.environ.get('MEMECOIN_V757_POLL_S','0.03'))
SCHED_EVERY=float(os.environ.get('MEMECOIN_V757_SCHED_S','0.10'))
HEALTH_EVERY=float(os.environ.get('MEMECOIN_V757_HEALTH_S','1.0'))
LOOKBACK=float(os.environ.get('MEMECOIN_V757_LOOKBACK_S','600'))
DRAIN_ZERO_STREAK=int(os.environ.get('MEMECOIN_V757_DRAIN_ZERO_STREAK','3'))
STAGES=tuple(sorted(set(old.SNAPSHOTS)))
STOP=False
STARTED_AT=0.0
STARTUP_RAW_CUTOFF=None
ACTIVATION_OBSERVED_AT=None
ACTIVATION_CHAIN_CUTOFF=None

def stop(*_):
 global STOP; STOP=True

def db():
 d=old.open_feature(); d.execute('PRAGMA busy_timeout=30000'); return d

def init_schema():
 old.initialize(); d=db(); d.executescript('''
 CREATE TABLE IF NOT EXISTS v757_causal_snapshots(
   token_mint TEXT NOT NULL, stage_s INTEGER NOT NULL, first_ts REAL NOT NULL,
   first_observed_at REAL NOT NULL, cutoff_ts REAL NOT NULL, built_at REAL NOT NULL,
   build_lag_s REAL NOT NULL, source_swaps INTEGER NOT NULL,
   source_max_raw_observed_at REAL, source_max_processed_at REAL,
   swaps INTEGER NOT NULL, buys INTEGER NOT NULL, sells INTEGER NOT NULL,
   buy_ratio REAL, gross_sol REAL, net_sol REAL, unique_wallets INTEGER,
   repeat_wallet_ratio REAL, wallet_hhi REAL, wallet_top1_share REAL,
   avg_trade_sol REAL, max_trade_sol REAL, trade_hhi REAL, top1_trade_share REAL,
   return_pct REAL, range_pct REAL, flow_velocity REAL, flow_acceleration REAL,
   buy_ratio_delta REAL, price_velocity REAL,
   PRIMARY KEY(token_mint,stage_s));
 CREATE INDEX IF NOT EXISTS idx_v757_stage_cutoff ON v757_causal_snapshots(stage_s,cutoff_ts);
 CREATE INDEX IF NOT EXISTS idx_v757_built ON v757_causal_snapshots(built_at);
 CREATE TABLE IF NOT EXISTS v757_engine_state(
   id INTEGER PRIMARY KEY CHECK(id=1), started_at REAL, phase TEXT,
   activation_observed_at REAL, activation_chain_cutoff REAL, heartbeat_at REAL,
   decoded_rows INTEGER, decoded_swaps INTEGER, causal_inserted INTEGER,
   raw_pending INTEGER, recent_n INTEGER, recent_p50_lag REAL,
   recent_p90_lag REAL, recent_p95_lag REAL, note TEXT);
 '''); d.commit(); d.close()

def freeze_startup_raw_cutoff():
 """Highest raw STORE timestamp already present when this process starts."""
 if not V5.exists(): return time.time()
 r=sqlite3.connect(f'file:{V5}?mode=ro',uri=True,timeout=30)
 try:
  z=r.execute('SELECT MAX(observed_at) FROM v5_raw_transactions').fetchone()[0]
  return float(z) if z is not None else time.time()
 finally:r.close()

def pending_counts():
 """Return (total_unprocessed, startup_watermark_unprocessed)."""
 try:
  d=db(); d.execute('ATTACH DATABASE ? AS raw',(str(V5),))
  total=d.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions r
                     LEFT JOIN main.v52_processed p ON p.signature=r.signature
                     WHERE p.signature IS NULL''').fetchone()[0]
  oldn=d.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions r
                    LEFT JOIN main.v52_processed p ON p.signature=r.signature
                    WHERE p.signature IS NULL AND r.observed_at<=?''',(float(STARTUP_RAW_CUTOFF),)).fetchone()[0]
  d.close(); return int(total),int(oldn)
 except Exception:return -1,-1

def persist_state(phase,decoded_rows,decoded_swaps,causal,rp,n=0,p50=None,p90=None,p95=None,note=''):
 d=db(); now=time.time(); d.execute('''INSERT OR REPLACE INTO v757_engine_state
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
 (1,STARTED_AT,phase,ACTIVATION_OBSERVED_AT,ACTIVATION_CHAIN_CUTOFF,now,
  decoded_rows,decoded_swaps,causal,rp,n,p50,p90,p95,note)); d.commit(); d.close()

def freeze_after_drain():
 global ACTIVATION_OBSERVED_AT,ACTIVATION_CHAIN_CUTOFF
 ACTIVATION_OBSERVED_AT=time.time()
 d=db(); z=d.execute('SELECT MAX(timestamp) FROM v52_swaps').fetchone()[0]; d.close()
 ACTIVATION_CHAIN_CUTOFF=float(z) if z is not None else ACTIVATION_OBSERVED_AT

def due_tokens(now):
 if ACTIVATION_OBSERVED_AT is None:return []
 d=db(); rs=d.execute('''SELECT token_mint,MIN(timestamp) first_ts,MIN(observed_at) first_obs
                          FROM v52_swaps
                          WHERE observed_at>? AND timestamp>=?
                          GROUP BY token_mint''',(ACTIVATION_OBSERVED_AT,now-LOOKBACK)).fetchall(); d.close()
 return [(str(r['token_mint']),float(r['first_ts']),float(r['first_obs'])) for r in rs if r['first_ts'] is not None and r['first_obs'] is not None]

def insert_token(d,mint,now):
 real=d.execute('SELECT MIN(timestamp),MIN(observed_at) FROM v52_swaps WHERE token_mint=?',(mint,)).fetchone()
 if not real or real[0] is None or real[1] is None:return 0
 first=float(real[0]); first_obs=float(real[1])
 if ACTIVATION_OBSERVED_AT is None or first_obs<=ACTIVATION_OBSERVED_AT:return 0
 rows=d.execute('SELECT * FROM v52_swaps WHERE token_mint=? ORDER BY timestamp',(mint,)).fetchall()
 if not rows:return 0
 made=0
 cols=['token_mint','stage_s','cutoff_ts','swaps','buys','sells','buy_ratio','gross_sol','net_sol','unique_wallets','repeat_wallet_ratio','wallet_hhi','wallet_top1_share','avg_trade_sol','max_trade_sol','trade_hhi','top1_trade_share','return_pct','range_pct','flow_velocity','flow_acceleration','buy_ratio_delta','price_velocity']
 for st in STAGES:
  cut=first+st
  if now<cut:continue
  if d.execute('SELECT 1 FROM v757_causal_snapshots WHERE token_mint=? AND stage_s=?',(mint,st)).fetchone():continue
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
  d.execute(f'''INSERT OR IGNORE INTO v757_causal_snapshots(
    token_mint,stage_s,first_ts,first_observed_at,cutoff_ts,built_at,build_lag_s,
    source_swaps,source_max_raw_observed_at,source_max_processed_at,{','.join(cols[3:])})
    VALUES({','.join('?' for _ in range(10+len(cols)-3))})''',
    [mint,st,first,first_obs,cut,now,max(0.0,now-cut),len(used),maxobs,maxproc]+vals[3:])
  if d.execute('SELECT changes()').fetchone()[0]:made+=1
 return made

def schedule():
 now=time.time(); toks=due_tokens(now)
 if not toks:return 0
 d=db(); made=0; d.execute('BEGIN IMMEDIATE')
 try:
  for mint,_,__ in toks: made+=insert_token(d,mint,now)
  d.commit()
 except BaseException:d.rollback(); raise
 finally:d.close()
 return made

def pct(xs,q):
 if not xs:return None
 ys=sorted(float(x) for x in xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
 return ys[lo]+(ys[hi]-ys[lo])*f

def recent_lags():
 d=db(); now=time.time(); xs=[float(r[0]) for r in d.execute('SELECT build_lag_s FROM v757_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-60,)).fetchall()]; d.close()
 return len(xs),pct(xs,.5),pct(xs,.9),pct(xs,.95)

def main():
 global STARTED_AT,STARTUP_RAW_CUTOFF
 signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop)
 init_schema(); canon.persist_activation_watermark(); STARTED_AT=time.time(); STARTUP_RAW_CUTOFF=freeze_startup_raw_cutoff()
 # A prior aborted V757 run never produced valid evidence while DRAINING. Keep the
 # existing causal table only if it is empty; otherwise require an explicit audit.
 d=db(); existing=d.execute('SELECT COUNT(*) FROM v757_causal_snapshots').fetchone()[0]; d.close()
 if existing: raise RuntimeError(f'v757 causal table already has {existing} rows; do not mix runs')
 decoded_rows=decoded_swaps=causal=0; zero_streak=0; phase='DRAINING'; last_sched=last_health=0.0
 print('MEMECOIN LAB V7.5.7.1 WATERMARK-DRAIN CAUSAL FEATURE ENGINE',flush=True)
 print(f'phase=DRAINING_STARTUP_WATERMARK | startup_raw_observed_at<={STARTUP_RAW_CUTOFF:.3f} | causal evidence DISABLED',flush=True)
 while not STOP:
  try:
   n,sw,q,c=canon.decode_batch(); decoded_rows+=n; decoded_swaps+=sw; now=time.time(); rp,startup_pending=pending_counts()
   if phase=='DRAINING':
    zero_streak=zero_streak+1 if startup_pending==0 else 0
    if zero_streak>=DRAIN_ZERO_STREAK:
     freeze_after_drain(); phase='LIVE_CAUSAL'
     print(f'V757 FREEZE | observed_at>{ACTIVATION_OBSERVED_AT:.3f} chain_cutoff>{ACTIVATION_CHAIN_CUTOFF:.3f} | startup backlog quarantined; fresh flow no longer blocks freeze',flush=True)
   elif now-last_sched>=SCHED_EVERY:
    causal+=schedule(); last_sched=now
   if now-last_health>=HEALTH_EVERY:
    rn,p50,p90,p95=recent_lags() if phase=='LIVE_CAUSAL' else (0,None,None,None)
    persist_state(phase,decoded_rows,decoded_swaps,causal,rp,rn,p50,p90,p95,f'startup_pending={startup_pending}; startup_raw_cutoff={STARTUP_RAW_CUTOFF}')
    print(f'V757 phase={phase} decoded={decoded_rows} swaps={decoded_swaps} raw_pending={rp} startup_pending={startup_pending} | causal={causal} recent20/30 n={rn} lag p50/p90/p95={p50}/{p90}/{p95}',flush=True)
    last_health=now
  except Exception as e:
   print('V757 error:',repr(e),flush=True); time.sleep(.2)
  time.sleep(POLL)

if __name__=='__main__':main()
