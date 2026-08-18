#!/usr/bin/env python3
"""Memecoin Lab V5.2.2 — canonical price decoder + prospective consistency quarantine.

Future-only decoder patch. It never rewrites an existing v52_swaps row and therefore never
retroactively changes V6.4 observations. The raw RPC payload remains source of truth.

Main fixes:
- aggregate ALL token-account deltas owned by the selected wallet for the unique non-WSOL mint
  instead of trusting one largest token account delta;
- aggregate WSOL deltas across all wallet-owned WSOL accounts;
- preserve native SOL fallback;
- quarantine isolated price spikes prospectively before they enter v52_swaps when they are wildly
  inconsistent with the token's recent canonical prices.
"""
from __future__ import annotations
import math, os, signal, sqlite3, time
from pathlib import Path
import v52_decode_features as old
import v521_fast_decoder_features as fast

ROOT=Path.home()/"memecoin_lab"
V5_DB=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
BATCH=int(os.environ.get("MEMECOIN_V522_BATCH","2000"))
POLL=float(os.environ.get("MEMECOIN_V522_POLL_S","0.15"))
ACTIVE_LOOKBACK=float(os.environ.get("MEMECOIN_V522_ACTIVE_LOOKBACK_S","1200"))
PRICE_GATE_RATIO=float(os.environ.get("MEMECOIN_V522_PRICE_GATE_RATIO","8.0"))
PRICE_GATE_LOOKBACK=int(os.environ.get("MEMECOIN_V522_PRICE_GATE_LOOKBACK","5"))
MIN_QUOTE_SOL=float(os.environ.get("MEMECOIN_V522_MIN_QUOTE_SOL","0.00001"))
STOP=False

def stop(*_):
 global STOP;STOP=True

def median(xs):
 xs=sorted(float(x) for x in xs if x is not None and float(x)>0 and math.isfinite(float(x)))
 if not xs:return None
 n=len(xs);return xs[n//2] if n%2 else (xs[n//2-1]+xs[n//2])/2

def canonical_swap(payload,row):
 tx=payload.get("rpc_transaction") or {}
 if not tx or (tx.get("meta") or {}).get("err") is not None:return None,"rpc_error"
 ds=old.token_deltas(tx)
 non=sorted({d["mint"] for d in ds if d.get("mint") and d["mint"]!=old.WSOL and abs(float(d.get("delta") or 0))>0})
 if len(non)!=1:return None,f"mint_count={len(non)}"
 mint=non[0];keys=old.account_keys(tx);signers={k["pubkey"] for k in keys if k["signer"]}
 owners={d.get("owner") for d in ds if d.get("mint")==mint and d.get("owner") and abs(float(d.get("delta") or 0))>0}
 signer_owners=owners & signers
 # Prefer a signer; otherwise choose owner with largest NET mint delta across all of its token accounts.
 pool=signer_owners or owners
 if not pool:return None,"no_owner_delta"
 sums={o:sum(float(d["delta"]) for d in ds if d.get("mint")==mint and d.get("owner")==o) for o in pool}
 wallet=max(sums,key=lambda o:abs(sums[o]));token_delta=float(sums[wallet])
 if abs(token_delta)<=0:return None,"zero_net_token_delta"
 side="BUY" if token_delta>0 else "SELL";token_amount=abs(token_delta)
 q_wsol=old.wsol_delta_for_owner(ds,wallet);q_native=old.native_sol_delta(tx,wallet)
 quote=None;conf="MEDIUM"
 if q_wsol is not None and abs(float(q_wsol))>0:quote=abs(float(q_wsol));conf="HIGH"
 elif q_native is not None and abs(float(q_native))>0:quote=abs(float(q_native));conf="MEDIUM"
 if quote is None or quote<MIN_QUOTE_SOL:return None,"no_quote"
 price=quote/token_amount if token_amount>0 else None
 if price is None or not math.isfinite(price) or price<=0:return None,"bad_price"
 ts=tx.get("blockTime") or payload.get("blockTime") or row["observed_at"]
 return {"signature":row["signature"],"token_mint":mint,"timestamp":float(ts),"slot":row["slot"],"source_program":row["source_program"],"wallet":wallet,"side":side,"token_amount":token_amount,"quote_sol":quote,"price_sol":price,"confidence":conf,"observed_at":float(row["observed_at"])},None

def gate(db,sw):
 prev=db.execute("SELECT price_sol FROM v52_swaps WHERE token_mint=? AND price_sol IS NOT NULL AND price_sol>0 AND timestamp<=? ORDER BY timestamp DESC LIMIT ?",(sw['token_mint'],sw['timestamp'],PRICE_GATE_LOOKBACK)).fetchall()
 ref=median([r[0] for r in prev])
 if ref is None:return True,None
 ratio=max(sw['price_sol']/ref,ref/sw['price_sol'])
 if ratio>PRICE_GATE_RATIO:
  return False,f"price_quarantine ratio={ratio:.3f} ref={ref:.12g} price={sw['price_sol']:.12g}"
 return True,None

def decode_batch():
 db=old.open_feature();rows=fast.fetch_new(db)
 if not rows:db.close();return 0,0,0,set()
 dec=quar=0;changed=set();db.execute("BEGIN IMMEDIATE")
 try:
  for row in rows:
   status="IGNORED";reason=None
   try:
    payload=old.decode_payload(row);event=(row['event_hint'] or 'OTHER').upper();tx=payload.get('rpc_transaction') or {};ts=float(tx.get('blockTime') or row['observed_at'])
    if event in ('CREATE','MIGRATE'):
     db.execute("INSERT OR IGNORE INTO v52_token_events(signature,token_mint,timestamp,event_type,source_program,wallet_hint) VALUES(?,?,?,?,?,?)",(row['signature'],row['token_hint'],ts,event,row['source_program'],row['creator_hint']))
     if row['token_hint']:changed.add(str(row['token_hint']))
    if event in ('BUY','SELL'):
     sw,reason=canonical_swap(payload,row)
     if sw:
      ok,why=gate(db,sw)
      if ok:
       db.execute("INSERT OR IGNORE INTO v52_swaps(signature,token_mint,timestamp,slot,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",tuple(sw[k] for k in ('signature','token_mint','timestamp','slot','source_program','wallet','side','token_amount','quote_sol','price_sol','confidence','observed_at')))
       status='SWAP';dec+=1;changed.add(sw['token_mint'])
      else:status='PRICE_QUARANTINE';reason=why;quar+=1
     else:status='UNDECODED'
    elif event in ('CREATE','MIGRATE'):status=event
    else:status='OTHER'
   except Exception as e:status='ERROR';reason=repr(e)
   db.execute("INSERT OR REPLACE INTO v52_processed(signature,processed_at,status,reason) VALUES(?,?,?,?)",(row['signature'],time.time(),status,reason))
  old.state(db,'last_decode_batch',{'rows':len(rows),'decoded_swaps':dec,'price_quarantine':quar,'engine':'v522'});db.commit()
 except BaseException:db.rollback();raise
 finally:db.close()
 return len(rows),dec,quar,changed

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);old.initialize();last=0;changed=set();print(f"V5.2.2 CANONICAL PRICE DECODER | gate={PRICE_GATE_RATIO}x lookback={PRICE_GATE_LOOKBACK}",flush=True)
 while not STOP:
  n,d,q,c=decode_batch();changed|=c
  if changed or time.time()-last>=1:
   b,o,t=fast.build(changed);changed.clear();last=time.time()
  else:b=o=t=0
  if n or b:print(f"V522 decoded={n}/{d} quarantine={q} | feature_tokens={t} snapshots={b} outcomes={o}",flush=True)
  time.sleep(POLL)
if __name__=='__main__':main()
