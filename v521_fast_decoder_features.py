#!/usr/bin/env python3
"""Memecoin Lab V5.2.1 — incremental fast decoder + feature factory.

Performance patch for V5.2. Scientific formulas/schema remain V5.2-compatible.
Key changes:
- SQL ATTACH anti-join fetches only unprocessed raw rows (no full-table Python scans)
- feature rebuild touches only tokens changed by the decode batch plus recently active tokens
- existing mature snapshots/outcomes are not pointlessly rebuilt every 2 seconds
- same v52_* tables, so V6.4 remains untouched
Research only.
"""
from __future__ import annotations
import json, os, signal, sqlite3, time, zlib
from pathlib import Path
import v52_decode_features as old

ROOT=Path.home()/"memecoin_lab"
V5_DB=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
FEATURE_DB=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
BATCH=int(os.environ.get("MEMECOIN_V521_BATCH","2000"))
POLL=float(os.environ.get("MEMECOIN_V521_POLL_S","0.15"))
ACTIVE_LOOKBACK=float(os.environ.get("MEMECOIN_V521_ACTIVE_LOOKBACK_S","1200"))
STOP=False

def stop(*_):
 global STOP;STOP=True

def fdb():
 d=old.open_feature();return d

def fetch_new(db):
 # Same connection can anti-join feature DB against attached raw DB efficiently.
 try: db.execute("DETACH DATABASE raw")
 except Exception: pass
 db.execute("ATTACH DATABASE ? AS raw",(str(V5_DB),))
 return db.execute("""SELECT r.signature,r.source_program,r.slot,r.observed_at,r.event_hint,r.token_hint,r.creator_hint,r.payload_zlib
 FROM raw.v5_raw_transactions r LEFT JOIN main.v52_processed p ON p.signature=r.signature
 WHERE p.signature IS NULL ORDER BY r.observed_at ASC LIMIT ?""",(BATCH,)).fetchall()

def decode_batch():
 if not V5_DB.exists():return (0,0,set())
 db=fdb();rows=fetch_new(db)
 if not rows: db.close();return (0,0,set())
 decoded=0;changed=set();db.execute("BEGIN IMMEDIATE")
 try:
  for row in rows:
   status="IGNORED";reason=None
   try:
    payload=old.decode_payload(row);event=(row["event_hint"] or "OTHER").upper();tx=payload.get("rpc_transaction") or {};ts=float(tx.get("blockTime") or row["observed_at"])
    if event in ("CREATE","MIGRATE"):
     db.execute("INSERT OR IGNORE INTO v52_token_events(signature,token_mint,timestamp,event_type,source_program,wallet_hint) VALUES(?,?,?,?,?,?)",(row["signature"],row["token_hint"],ts,event,row["source_program"],row["creator_hint"]))
     if row["token_hint"]:changed.add(str(row["token_hint"]))
    if event in ("BUY","SELL"):
     sw,reason=old.decode_swap(payload,row)
     if sw:
      db.execute("INSERT OR IGNORE INTO v52_swaps(signature,token_mint,timestamp,slot,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",tuple(sw[k] for k in ("signature","token_mint","timestamp","slot","source_program","wallet","side","token_amount","quote_sol","price_sol","confidence","observed_at")))
      status="SWAP";decoded+=1;changed.add(sw["token_mint"])
     else:status="UNDECODED"
    elif event in ("CREATE","MIGRATE"):status=event
    else:status="OTHER"
   except Exception as e:status="ERROR";reason=repr(e)
   db.execute("INSERT OR REPLACE INTO v52_processed(signature,processed_at,status,reason) VALUES(?,?,?,?)",(row["signature"],time.time(),status,reason))
  old.state(db,"last_decode_batch",{"rows":len(rows),"decoded_swaps":decoded,"engine":"v521"});db.commit()
 except BaseException:db.rollback();raise
 finally:db.close()
 return len(rows),decoded,changed

def upsert_token(db,mint,now):
 rows=db.execute("SELECT * FROM v52_swaps WHERE token_mint=? ORDER BY timestamp",(mint,)).fetchall()
 if not rows:return 0,0
 first=float(rows[0]["timestamp"]);built=outs=0
 for stage in old.SNAPSHOTS:
  cutoff=first+stage
  if now<cutoff:continue
  m=old.snapshot_metrics(rows,first,stage)
  if not m:continue
  cols=["token_mint","stage_s","cutoff_ts","swaps","buys","sells","buy_ratio","gross_sol","net_sol","unique_wallets","repeat_wallet_ratio","wallet_hhi","wallet_top1_share","avg_trade_sol","max_trade_sol","trade_hhi","top1_trade_share","return_pct","range_pct","flow_velocity","flow_acceleration","buy_ratio_delta","price_velocity"]
  vals=[m[c] for c in cols]
  db.execute(f"INSERT INTO v52_snapshots({','.join(cols)},built_at) VALUES({','.join('?' for _ in cols)},?) ON CONFLICT(token_mint,stage_s) DO UPDATE SET "+','.join(f"{c}=excluded.{c}" for c in cols[2:])+",built_at=excluded.built_at",vals+[now]);built+=1
  base=[float(r["price_sol"]) for r in rows if float(r["timestamp"])<=cutoff and r["price_sol"] is not None and float(r["price_sol"])>0];bp=base[-1] if base else None
  for h in old.HORIZONS:
   end=cutoff+h;ready=int(now>=end);future=[r for r in rows if cutoff<float(r["timestamp"])<=end and r["price_sol"] is not None and float(r["price_sol"])>0];rets=[100*(float(r["price_sol"])/bp-1) for r in future] if bp else []
   fr=rets[-1] if rets else None;mx=max(rets) if rets else None;mn=min(rets) if rets else None;mig=db.execute("SELECT 1 FROM v52_token_events WHERE token_mint=? AND event_type='MIGRATE' AND timestamp>? AND timestamp<=? LIMIT 1",(mint,cutoff,end)).fetchone() is not None
   db.execute("""INSERT INTO v52_outcomes(token_mint,stage_s,horizon_s,ready,future_return_pct,future_max_return_pct,future_min_return_pct,future_hit10,future_hit20,future_hit50,future_death50,future_migration,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(token_mint,stage_s,horizon_s) DO UPDATE SET ready=excluded.ready,future_return_pct=excluded.future_return_pct,future_max_return_pct=excluded.future_max_return_pct,future_min_return_pct=excluded.future_min_return_pct,future_hit10=excluded.future_hit10,future_hit20=excluded.future_hit20,future_hit50=excluded.future_hit50,future_death50=excluded.future_death50,future_migration=excluded.future_migration,updated_at=excluded.updated_at""",(mint,stage,h,ready,fr,mx,mn,int(mx is not None and mx>=10),int(mx is not None and mx>=20),int(mx is not None and mx>=50),int(mn is not None and mn<=-50),int(mig),now));outs+=1
 return built,outs

def build(changed):
 db=fdb();now=time.time();active={r[0] for r in db.execute("SELECT DISTINCT token_mint FROM v52_swaps WHERE timestamp>=?",(now-ACTIVE_LOOKBACK,))};tokens=set(changed)|active;b=o=0
 db.execute("BEGIN IMMEDIATE")
 try:
  for mint in tokens:
   x,y=upsert_token(db,mint,now);b+=x;o+=y
  old.state(db,"last_feature_build",{"snapshots_touched":b,"outcomes_touched":o,"tokens_touched":len(tokens),"engine":"v521"});db.commit()
 except BaseException:db.rollback();raise
 finally:db.close()
 return b,o,len(tokens)

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);old.initialize();last_build=0;ld=(0,0);lb=(0,0,0);changed=set();print("V5.2.1 FAST INCREMENTAL DECODER + FEATURE FACTORY",flush=True)
 while not STOP:
  n,d,c=decode_batch();ld=(n,d);changed|=c
  if changed or time.time()-last_build>=1:
   lb=build(changed);changed.clear();last_build=time.time()
  if n or lb[0]:print(f"V521 decoded={n}/{d} | feature_tokens={lb[2]} snapshots={lb[0]} outcomes={lb[1]}",flush=True)
  time.sleep(POLL)
if __name__=='__main__':main()
