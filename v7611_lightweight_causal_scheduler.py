#!/usr/bin/env python3
"""MEMECOIN LAB — LIGHTWEIGHT CAUSAL SCHEDULER V7.6.1.1

Companion to V7602. Removes the expensive raw/processed anti-join from the
scheduler loop. Decoder health/backlog is read from v7602_decoder_state, so the
scheduler no longer competes with the decoder for the same full-table query.
Fresh insert-only causal snapshots only; no strategy logic.
"""
from __future__ import annotations
import os,signal,time
import v52_decode_features as old

POLL=float(os.environ.get('MEMECOIN_V7611_POLL_S','0.10'))
ZERO_STREAK_N=int(os.environ.get('MEMECOIN_V7611_ZERO_STREAK','5'))
LOOKBACK=float(os.environ.get('MEMECOIN_V7611_LOOKBACK_S','600'))
MAX_DECODER_HEARTBEAT=float(os.environ.get('MEMECOIN_V7611_MAX_DECODER_HEARTBEAT_S','3'))
STAGES=tuple(sorted(set(old.SNAPSHOTS)))
STOP=False;ACTIVATION=None;STARTED=0.0

def stop(*_):
 global STOP;STOP=True

def db():
 d=old.open_feature();d.execute('PRAGMA busy_timeout=30000');return d

def init_schema():
 d=db();d.executescript('''
 CREATE TABLE IF NOT EXISTS v7611_causal_snapshots(
  token_mint TEXT NOT NULL,stage_s INTEGER NOT NULL,first_ts REAL NOT NULL,first_observed_at REAL NOT NULL,
  cutoff_ts REAL NOT NULL,built_at REAL NOT NULL,build_lag_s REAL NOT NULL,source_swaps INTEGER NOT NULL,
  swaps INTEGER NOT NULL,buys INTEGER NOT NULL,sells INTEGER NOT NULL,buy_ratio REAL,gross_sol REAL,net_sol REAL,
  unique_wallets INTEGER,repeat_wallet_ratio REAL,wallet_hhi REAL,wallet_top1_share REAL,avg_trade_sol REAL,max_trade_sol REAL,
  trade_hhi REAL,top1_trade_share REAL,return_pct REAL,range_pct REAL,flow_velocity REAL,flow_acceleration REAL,buy_ratio_delta REAL,price_velocity REAL,
  PRIMARY KEY(token_mint,stage_s));
 CREATE INDEX IF NOT EXISTS idx_v7611_stage_cutoff ON v7611_causal_snapshots(stage_s,cutoff_ts);
 CREATE INDEX IF NOT EXISTS idx_v7611_built ON v7611_causal_snapshots(built_at);
 CREATE TABLE IF NOT EXISTS v7611_scheduler_state(
  id INTEGER PRIMARY KEY CHECK(id=1),started_at REAL,phase TEXT,activation_observed_at REAL,heartbeat_at REAL,
  decoder_backlog INTEGER,decoder_heartbeat_age REAL,causal_inserted INTEGER,recent_n INTEGER,recent_p50 REAL,recent_p90 REAL,recent_p95 REAL,note TEXT);
 ''');d.commit();d.close()

def decoder_health():
 d=db();r=d.execute('SELECT * FROM v7602_decoder_state WHERE id=1').fetchone();d.close()
 if not r:return {'ok':False,'backlog':None,'age':999.0}
 age=max(0.0,time.time()-float(r['heartbeat_at']));backlog=int(r['backlog_est']);return {'ok':age<=MAX_DECODER_HEARTBEAT,'backlog':backlog,'age':age}

def pct(xs,q):
 if not xs:return None
 ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo;return ys[lo]+(ys[hi]-ys[lo])*f

def recent():
 d=db();now=time.time();xs=[float(r[0]) for r in d.execute('SELECT build_lag_s FROM v7611_causal_snapshots WHERE built_at>=? AND stage_s IN (20,30)',(now-60,)).fetchall()];d.close();return len(xs),pct(xs,.5),pct(xs,.9),pct(xs,.95)

def due_tokens(now):
 if ACTIVATION is None:return []
 d=db();rows=d.execute('''SELECT token_mint,MIN(timestamp) first_ts,MIN(observed_at) first_obs FROM v52_swaps
 WHERE observed_at>? AND timestamp>=? GROUP BY token_mint''',(ACTIVATION,now-LOOKBACK)).fetchall();d.close()
 return [(str(r['token_mint']),float(r['first_ts']),float(r['first_obs'])) for r in rows if r['first_obs'] is not None and float(r['first_obs'])>ACTIVATION]

def insert_token(d,mint,now):
 real=d.execute('SELECT MIN(timestamp),MIN(observed_at) FROM v52_swaps WHERE token_mint=?',(mint,)).fetchone()
 if not real or real[0] is None or real[1] is None:return 0
 first=float(real[0]);first_obs=float(real[1])
 if first_obs<=ACTIVATION:return 0
 rows=d.execute('SELECT * FROM v52_swaps WHERE token_mint=? ORDER BY timestamp',(mint,)).fetchall();made=0
 cols=['token_mint','stage_s','cutoff_ts','swaps','buys','sells','buy_ratio','gross_sol','net_sol','unique_wallets','repeat_wallet_ratio','wallet_hhi','wallet_top1_share','avg_trade_sol','max_trade_sol','trade_hhi','top1_trade_share','return_pct','range_pct','flow_velocity','flow_acceleration','buy_ratio_delta','price_velocity']
 for st in STAGES:
  cut=first+st
  if now<cut:continue
  if d.execute('SELECT 1 FROM v7611_causal_snapshots WHERE token_mint=? AND stage_s=?',(mint,st)).fetchone():continue
  m=old.snapshot_metrics(rows,first,st)
  if not m:continue
  vals=[m[c] for c in cols]
  d.execute(f'''INSERT OR IGNORE INTO v7611_causal_snapshots(token_mint,stage_s,first_ts,first_observed_at,cutoff_ts,built_at,build_lag_s,source_swaps,{','.join(cols[3:])}) VALUES({','.join('?' for _ in range(8+len(cols)-3))})''',[mint,st,first,first_obs,cut,now,max(0.0,now-cut),sum(1 for r in rows if float(r['timestamp'])<=cut)]+vals[3:])
  if d.execute('SELECT changes()').fetchone()[0]:made+=1
 return made

def schedule():
 now=time.time();toks=due_tokens(now)
 if not toks:return 0
 d=db();made=0;d.execute('BEGIN IMMEDIATE')
 try:
  for mint,_,__ in toks:made+=insert_token(d,mint,now)
  d.commit()
 except BaseException:d.rollback();raise
 finally:d.close()
 return made

def persist(phase,total,h):
 n,p50,p90,p95=recent();d=db();now=time.time();d.execute('INSERT OR REPLACE INTO v7611_scheduler_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(1,STARTED,phase,ACTIVATION,now,h.get('backlog'),h.get('age'),total,n,p50,p90,p95,'uses V7602 state; no raw anti-join'));d.commit();d.close();return n,p50,p90,p95

def main():
 global ACTIVATION,STARTED
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);old.initialize();init_schema();STARTED=time.time();phase='WAIT_DECODER_DRAIN';zero=0;total=0;last=0
 print('MEMECOIN LAB V7.6.1.1 LIGHTWEIGHT CAUSAL SCHEDULER',flush=True)
 while not STOP:
  try:
   h=decoder_health();now=time.time()
   if phase=='WAIT_DECODER_DRAIN':
    clean=h['ok'] and h['backlog']==0;zero=zero+1 if clean else 0
    if zero>=ZERO_STREAK_N:
     ACTIVATION=time.time();phase='LIVE_CAUSAL';print(f'V7611 FREEZE | activation_observed_at>{ACTIVATION:.3f} | V7602 backlog clean',flush=True)
   elif h['ok']:
    total+=schedule()
   if now-last>=1:
    n,p50,p90,p95=persist(phase,total,h);print(f"V7611 phase={phase} decoder_backlog={h['backlog']} decoder_age={h['age']:.2f}s causal={total} recent20/30 n={n} lag p50/p90/p95={p50}/{p90}/{p95}",flush=True);last=now
  except Exception as e:print('V7611 error:',repr(e),flush=True);time.sleep(.2)
  time.sleep(POLL)
if __name__=='__main__':main()
