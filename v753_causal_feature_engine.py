#!/usr/bin/env python3
"""MEMECOIN LAB — CAUSAL LOW-LATENCY FEATURE ENGINE V7.5.3

Infrastructure-only replacement for the V5.2.2 decode/build loop.

V7.5.3 separates decoding from feature materialization, keeps legacy mutable
V52 snapshots available for discovery, and creates a second INSERT-ONLY causal
snapshot rail for future-only validation.

Critical invariant: the causal rail never backfills pre-start tokens. On first
process start it freezes an activation cutoff equal to the newest decoded swap
timestamp already present. Only tokens whose true first decoded swap is strictly
after that cutoff can enter v753_causal_snapshots.

Research only. Never signs or submits transactions.
"""
from __future__ import annotations

import os, signal, time
from pathlib import Path

os.environ.setdefault('MEMECOIN_V522_BATCH','5000')
os.environ.setdefault('MEMECOIN_V522_POLL_S','0.03')

import v52_decode_features as old
import v521_fast_decoder_features as fast
import v522_canonical_price_decoder as canon

ROOT=Path.home()/"memecoin_lab"
V5=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
V52=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
POLL=float(os.environ.get('MEMECOIN_V753_POLL_S','0.05'))
SCHED_EVERY=float(os.environ.get('MEMECOIN_V753_SCHED_S','0.20'))
LEGACY_FLUSH_EVERY=float(os.environ.get('MEMECOIN_V753_LEGACY_FLUSH_S','0.35'))
HEALTH_EVERY=float(os.environ.get('MEMECOIN_V753_HEALTH_S','1.0'))
LOOKBACK=float(os.environ.get('MEMECOIN_V753_LOOKBACK_S','900'))
STAGES=tuple(sorted(set(old.SNAPSHOTS)))
STOP=False
ACTIVATION_CUTOFF=0.0

def stop(*_):
 global STOP;STOP=True

def db():
 d=old.open_feature();d.execute('PRAGMA busy_timeout=30000');return d

def init_extra():
 old.initialize();d=db();d.executescript('''
 CREATE TABLE IF NOT EXISTS v753_causal_snapshots(
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
 CREATE INDEX IF NOT EXISTS idx_v753_cutoff ON v753_causal_snapshots(stage_s,cutoff_ts);
 CREATE INDEX IF NOT EXISTS idx_v753_built ON v753_causal_snapshots(built_at);
 CREATE TABLE IF NOT EXISTS v753_engine_state(
   id INTEGER PRIMARY KEY CHECK(id=1), started_at REAL, activation_cutoff REAL,
   heartbeat_at REAL, decoded_rows INTEGER, decoded_swaps INTEGER,
   legacy_tokens INTEGER, causal_inserted INTEGER, raw_pending INTEGER,
   recent_n INTEGER, recent_p50_lag REAL, recent_p90_lag REAL,
   recent_p95_lag REAL, note TEXT);
 ''');d.commit();d.close()

def freeze_activation_cutoff():
 """Freeze once per new causal rail; never infer causal history retroactively."""
 d=db()
 existing=d.execute('SELECT activation_cutoff FROM v753_engine_state WHERE id=1').fetchone()
 causal=d.execute('SELECT COUNT(*) FROM v753_causal_snapshots').fetchone()[0]
 if existing and existing[0] is not None and causal:
  cut=float(existing[0]);d.close();return cut
 z=d.execute('SELECT MAX(timestamp) FROM v52_swaps').fetchone()[0]
 cut=float(z) if z is not None else time.time()
 # If this is a clean start, remove no scientific history: table must be empty.
 if causal:
  d.close();raise RuntimeError('v753 causal table has rows but no durable activation cutoff')
 d.execute('''INSERT OR REPLACE INTO v753_engine_state
   (id,started_at,activation_cutoff,heartbeat_at,decoded_rows,decoded_swaps,legacy_tokens,causal_inserted,raw_pending,recent_n,recent_p50_lag,recent_p90_lag,recent_p95_lag,note)
   VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(time.time(),cut,time.time(),0,0,0,0,0,0,None,None,None,'activation frozen; no pre-start causal backfill'))
 d.commit();d.close();return cut

def raw_pending_count():
 try:
  d=db();d.execute('ATTACH DATABASE ? AS raw',(str(V5),))
  n=d.execute('''SELECT COUNT(*) FROM raw.v5_raw_transactions r
                 LEFT JOIN main.v52_processed p ON p.signature=r.signature
                 WHERE p.signature IS NULL''').fetchone()[0]
  d.close();return int(n)
 except Exception:return -1

def legacy_flush(tokens):
 if not tokens:return 0
 d=db();now=time.time();n=0;d.execute('BEGIN IMMEDIATE')
 try:
  for mint in sorted(tokens):
   try:fast.upsert_token(d,mint,now);n+=1
   except Exception:pass
  old.state(d,'v753_legacy_flush',{'tokens':n,'at':now,'engine':'v753'})
  d.commit()
 except BaseException:d.rollback();raise
 finally:d.close()
 return n

def due_tokens(now):
 d=db();rows=d.execute('''SELECT token_mint,MIN(timestamp) first_ts
                          FROM v52_swaps
                          WHERE timestamp>? AND timestamp>=?
                          GROUP BY token_mint''',(ACTIVATION_CUTOFF,now-LOOKBACK)).fetchall();d.close()
 return [(str(r['token_mint']),float(r['first_ts'])) for r in rows if r['first_ts'] is not None and float(r['first_ts'])>ACTIVATION_CUTOFF]

def insert_causal_for_token(d,mint,first,now):
 # Verify the real first decoded swap also lies after activation. This prevents a
 # token with an older first swap plus a new post-start swap from entering.
 real_first=d.execute('SELECT MIN(timestamp) FROM v52_swaps WHERE token_mint=?',(mint,)).fetchone()[0]
 if real_first is None or float(real_first)<=ACTIVATION_CUTOFF:return 0
 first=float(real_first)
 rows=d.execute('SELECT * FROM v52_swaps WHERE token_mint=? ORDER BY timestamp',(mint,)).fetchall()
 if not rows:return 0
 made=0
 cols=['token_mint','stage_s','cutoff_ts','swaps','buys','sells','buy_ratio','gross_sol','net_sol','unique_wallets','repeat_wallet_ratio','wallet_hhi','wallet_top1_share','avg_trade_sol','max_trade_sol','trade_hhi','top1_trade_share','return_pct','range_pct','flow_velocity','flow_acceleration','buy_ratio_delta','price_velocity']
 for st in STAGES:
  cut=first+st
  if now<cut:continue
  if d.execute('SELECT 1 FROM v753_causal_snapshots WHERE token_mint=? AND stage_s=?',(mint,st)).fetchone():continue
  m=old.snapshot_metrics(rows,first,st)
  if not m:continue
  used=[r for r in rows if float(r['timestamp'])<=cut]
  maxobs=max((float(r['observed_at']) for r in used if r['observed_at'] is not None),default=None)
  sigs=[str(r['signature']) for r in used];maxproc=None
  for i in range(0,len(sigs),500):
   chunk=sigs[i:i+500]
   if not chunk:continue
   q=','.join('?' for _ in chunk);z=d.execute(f'SELECT MAX(processed_at) FROM v52_processed WHERE signature IN ({q})',chunk).fetchone()[0]
   if z is not None:maxproc=max(float(z),maxproc or float(z))
  vals=[m[c] for c in cols]
  d.execute(f'''INSERT OR IGNORE INTO v753_causal_snapshots(
      token_mint,stage_s,first_ts,cutoff_ts,built_at,build_lag_s,source_swaps,
      source_max_raw_observed_at,source_max_processed_at,{','.join(cols[3:])})
      VALUES({','.join('?' for _ in range(9+len(cols)-3))})''',
      [mint,st,first,cut,now,max(0.0,now-cut),len(used),maxobs,maxproc]+vals[3:])
  if d.execute('SELECT changes()').fetchone()[0]:made+=1
 return made

def causal_schedule():
 now=time.time();tokens=due_tokens(now)
 if not tokens:return 0
 d=db();made=0;d.execute('BEGIN IMMEDIATE')
 try:
  for mint,first in tokens:made+=insert_causal_for_token(d,mint,first,now)
  d.commit()
 except BaseException:d.rollback();raise
 finally:d.close()
 return made

def recent_lags(window=60.0):
 d=db();now=time.time();xs=[float(r[0]) for r in d.execute('SELECT build_lag_s FROM v753_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-window,)).fetchall()];d.close()
 if not xs:return (0,None,None,None)
 ys=sorted(xs)
 def p(q):
  x=(len(ys)-1)*q;lo=int(x);hi=min(len(ys)-1,lo+1);f=x-lo;return ys[lo]+(ys[hi]-ys[lo])*f
 return len(xs),p(.5),p(.9),p(.95)

def heartbeat(started,decoded_rows,decoded_swaps,legacy_tokens,causal_total):
 n,p50,p90,p95=recent_lags();pending=raw_pending_count();d=db();now=time.time()
 d.execute('''INSERT OR REPLACE INTO v753_engine_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
   (1,started,ACTIVATION_CUTOFF,now,decoded_rows,decoded_swaps,legacy_tokens,causal_total,pending,n,p50,p90,p95,'canonical decode + changed-only legacy + future-only insert-only scheduler'))
 old.state(d,'v753_engine_state',{'heartbeat':now,'activation_cutoff':ACTIVATION_CUTOFF,'raw_pending':pending,'recent_n':n,'p90':p90,'p95':p95})
 d.commit();d.close();return pending,n,p50,p90,p95

def main():
 global STOP,ACTIVATION_CUTOFF
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
 init_extra();canon.persist_activation_watermark();ACTIVATION_CUTOFF=freeze_activation_cutoff();started=time.time()
 changed=set();decoded_rows=decoded_swaps=legacy_tokens=causal_total=0
 last_sched=last_legacy=last_health=0.0
 print('MEMECOIN LAB V7.5.3 CAUSAL LOW-LATENCY FEATURE ENGINE',flush=True)
 print(f'activation_cutoff>{ACTIVATION_CUTOFF:.3f} | stages={STAGES} | decode_batch={canon.BATCH} | causal=v753_causal_snapshots',flush=True)
 while not STOP:
  try:
   n,sw,q,c=canon.decode_batch();decoded_rows+=n;decoded_swaps+=sw;changed|=c;now=time.time()
   if changed and now-last_legacy>=LEGACY_FLUSH_EVERY:
    todo=set(changed);changed.clear();legacy_tokens+=legacy_flush(todo);last_legacy=now
   if now-last_sched>=SCHED_EVERY:causal_total+=causal_schedule();last_sched=now
   if now-last_health>=HEALTH_EVERY:
    pending,rn,p50,p90,p95=heartbeat(started,decoded_rows,decoded_swaps,legacy_tokens,causal_total);last_health=now
    print(f'V753 decoded={decoded_rows} swaps={decoded_swaps} raw_pending={pending} | causal={causal_total} recent20/30 n={rn} lag p50/p90/p95={p50}/{p90}/{p95}',flush=True)
  except Exception as e:print('V753 error:',repr(e),flush=True);time.sleep(.2)
  time.sleep(POLL)
 if changed:
  try:legacy_flush(changed)
  except Exception:pass

if __name__=='__main__':main()
