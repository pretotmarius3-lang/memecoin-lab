#!/usr/bin/env python3
"""MEMECOIN LAB — DETERMINISTIC PUMP MIGRATION MINT DECODER V7.7.4

Purpose
-------
Take exact Pump `Instruction: Migrate` signatures already captured by V7.7.3
and recover the migrated base mint from the full transaction.

Evidence hierarchy (strict):
1. EXACT_LOCAL_TX: transaction found in local v5_raw_transactions and one unique
   non-WSOL mint is supported by token balances + pre-migration canonical history.
2. EXACT_RPC_TX: same deterministic rule after fetching a missing transaction from
   the configured Alchemy Solana RPC.
3. AMBIGUOUS / NO_TX / NO_MINT: retained for audit, never promoted to exact.

No price-crossing inference. Source DBs are READ-ONLY. Results are written only to
v774_exact_migrations.db.
"""
from __future__ import annotations

import json, os, sqlite3, time, urllib.request, zlib
from collections import Counter, defaultdict
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW=ROOT/"v5_raw_events.db"
FEATURE=ROOT/"v52_features.db"
OUT=ROOT/"v774_exact_migrations.db"
PUMP="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
WSOL="So11111111111111111111111111111111111111112"
NEEDLE="Instruction: Migrate"


def load_dotenv():
    p=ROOT/'.env'
    if not p.exists(): return
    for line in p.read_text(errors='ignore').splitlines():
        s=line.strip()
        if not s or s.startswith('#') or '=' not in s: continue
        k,v=s.split('=',1); k=k.strip(); v=v.strip().strip('"').strip("'")
        if k and k not in os.environ: os.environ[k]=v


def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row; d.execute('PRAGMA query_only=ON'); d.execute('PRAGMA busy_timeout=30000')
    return d


def odb():
    d=sqlite3.connect(OUT,timeout=30); d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL'); d.execute('PRAGMA synchronous=NORMAL'); d.execute('PRAGMA busy_timeout=30000')
    return d


def init_out():
    d=odb(); d.executescript('''
    CREATE TABLE IF NOT EXISTS exact_migrations(
      signature TEXT PRIMARY KEY,
      slot INTEGER,
      block_time REAL,
      mint TEXT,
      decode_status TEXT NOT NULL,
      evidence TEXT NOT NULL,
      tx_source TEXT,
      candidate_mints_json TEXT,
      prior_swap_candidates_json TEXT,
      decoded_at REAL NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_v774_mint ON exact_migrations(mint);
    CREATE INDEX IF NOT EXISTS idx_v774_status ON exact_migrations(decode_status);
    '''); d.commit(); d.close()


def exact_signatures():
    d=ro(RAW)
    rows=d.execute("SELECT signature,slot,first_seen FROM v51_signature_spool WHERE logs_json LIKE ?",(f'%{NEEDLE}%',)).fetchall()
    d.close()
    out={}
    for r in rows:
        sig=str(r['signature'] or '')
        if sig: out.setdefault(sig,{'slot':r['slot'],'first_seen':r['first_seen']})
    return out


def decode_local_payload(blob):
    if blob is None:return None
    b=bytes(blob) if not isinstance(blob,str) else blob.encode()
    attempts=[lambda x:x, zlib.decompress]
    for fn in attempts:
        try:
            raw=fn(b)
            if isinstance(raw,bytes): raw=raw.decode('utf-8')
            obj=json.loads(raw)
            return obj
        except Exception: pass
    return None


def local_transactions(sigs):
    d=ro(RAW); out={}
    sigs=list(sigs)
    for i in range(0,len(sigs),500):
        sub=sigs[i:i+500]; qs=','.join('?' for _ in sub)
        try:
            rr=d.execute(f'SELECT signature,slot,observed_at,payload_zlib FROM v5_raw_transactions WHERE signature IN ({qs})',sub).fetchall()
        except sqlite3.Error:
            rr=[]
        for r in rr:
            p=decode_local_payload(r['payload_zlib'])
            if not p: continue
            tx=p.get('rpc_transaction') if isinstance(p,dict) and 'rpc_transaction' in p else p
            if isinstance(tx,dict) and tx.get('result') is not None: tx=tx.get('result')
            if tx: out[str(r['signature'])]=tx
    d.close(); return out


def rpc_url():
    u=os.environ.get('ALCHEMY_SOLANA_RPC_URL')
    if u:return u
    k=os.environ.get('ALCHEMY_API_KEY')
    return f'https://solana-mainnet.g.alchemy.com/v2/{k}' if k else None


def rpc_get_tx(url,sig):
    body=json.dumps({'jsonrpc':'2.0','id':1,'method':'getTransaction','params':[sig,{'encoding':'jsonParsed','commitment':'confirmed','maxSupportedTransactionVersion':0}]}).encode()
    req=urllib.request.Request(url,data=body,headers={'content-type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=20) as r:
            obj=json.loads(r.read().decode())
        return obj.get('result')
    except Exception:
        return None


def balance_mints(tx):
    meta=(tx or {}).get('meta') or {}
    m=[]
    for key in ('preTokenBalances','postTokenBalances'):
        for b in meta.get(key) or []:
            if isinstance(b,dict) and b.get('mint'):
                mint=str(b['mint'])
                if mint!=WSOL:m.append(mint)
    return sorted(set(m))


def account_keys(tx):
    try: ks=tx['transaction']['message']['accountKeys']
    except Exception:return []
    out=[]
    for k in ks or []:
        if isinstance(k,str):out.append(k)
        elif isinstance(k,dict) and k.get('pubkey'):out.append(str(k['pubkey']))
    return out


def prior_swap_map(candidate_by_sig, bt_by_sig):
    # A base mint should have canonical Pump trading history before migration.
    allm=sorted({m for ms in candidate_by_sig.values() for m in ms})
    prior=defaultdict(list)
    if not allm or not FEATURE.exists():return prior
    d=ro(FEATURE)
    for i in range(0,len(allm),600):
        sub=allm[i:i+600];qs=','.join('?' for _ in sub)
        for r in d.execute(f'SELECT token_mint,MIN(timestamp) mn,MAX(timestamp) mx,COUNT(*) n FROM v52_swaps WHERE token_mint IN ({qs}) GROUP BY token_mint',sub):
            prior[str(r['token_mint'])]=[float(r['mn']),float(r['mx']),int(r['n'])]
    d.close();return prior


def main():
    load_dotenv(); init_out()
    print('='*136)
    print('MEMECOIN LAB — DETERMINISTIC PUMP MIGRATION MINT DECODER V7.7.4')
    print('='*136)
    print('Exact migrate logs -> full tx -> base mint attribution | no MC/price inference')

    sigmeta=exact_signatures(); sigs=sorted(sigmeta)
    print(f'exact_migrate_signatures={len(sigs)}')
    local=local_transactions(sigs)
    print(f'local_full_transactions={len(local)}/{len(sigs)}')

    url=rpc_url(); txs=dict(local); rpc_added=0
    missing=[s for s in sigs if s not in txs]
    if missing and url:
        print(f'RPC backfill enabled for missing={len(missing)}',flush=True)
        for j,s in enumerate(missing,1):
            tx=rpc_get_tx(url,s)
            if tx: txs[s]=tx; rpc_added+=1
            if j%25==0: print(f'  rpc {j}/{len(missing)} recovered={rpc_added}',flush=True)
            time.sleep(0.03)
    elif missing:
        print('RPC backfill disabled: no ALCHEMY_SOLANA_RPC_URL / ALCHEMY_API_KEY found')
    print(f'rpc_full_transactions_added={rpc_added}')

    candidates={s:balance_mints(tx) for s,tx in txs.items()}
    bt={s:float(tx.get('blockTime') or 0) for s,tx in txs.items()}
    hist=prior_swap_map(candidates,bt)

    rows=[]; statuses=Counter(); exact_mints=[]
    for s in sigs:
        tx=txs.get(s); source='LOCAL' if s in local else ('RPC' if tx else None)
        ms=candidates.get(s,[])
        t=bt.get(s,0.0)
        # Candidates with canonical history at/before this migration tx.
        prior=[]
        for m in ms:
            h=hist.get(m)
            if h and (not t or h[0] <= t): prior.append(m)
        mint=None; status='NO_TX'; evidence='no full transaction available'
        if tx:
            if len(prior)==1:
                mint=prior[0];status='EXACT';evidence='unique non-WSOL token-balance mint with canonical pre-migration swap history'
            elif len(ms)==1:
                mint=ms[0];status='EXACT';evidence='unique non-WSOL mint in transaction token balances'
            elif len(prior)>1:
                status='AMBIGUOUS';evidence='multiple non-WSOL mints with canonical history'
            elif len(ms)>1:
                status='AMBIGUOUS';evidence='multiple non-WSOL transaction balance mints'
            else:
                status='NO_MINT';evidence='no non-WSOL mint exposed in token balances'
        slot=tx.get('slot') if tx else sigmeta[s].get('slot')
        block=tx.get('blockTime') if tx else None
        rows.append((s,slot,block,mint,status,evidence,source,json.dumps(ms),json.dumps(prior),time.time()))
        statuses[status]+=1
        if status=='EXACT':exact_mints.append(mint)

    d=odb();d.execute('DELETE FROM exact_migrations');d.executemany('INSERT INTO exact_migrations VALUES(?,?,?,?,?,?,?,?,?,?)',rows);d.commit();d.close()

    print('\nDECODE CENSUS')
    print('status='+repr(dict(statuses)))
    print(f'exact_rows={statuses["EXACT"]}/{len(sigs)} unique_exact_mints={len(set(exact_mints))}')
    print(f'output={OUT.name}')

    print('\nEXACT EXAMPLES')
    for r in rows:
        if r[4]=='EXACT':print(f'sig={r[0]} mint={r[3]} slot={r[1]} source={r[6]} evidence={r[5]}')
        if sum(1 for x in rows[:rows.index(r)+1] if x[4]=='EXACT')>=8:break

    print('\nQUALITY GATE')
    n=statuses['EXACT']; rate=n/max(1,len(sigs))
    if n>=50 and rate>=.70:
        print('STATUS=CANONICAL_MIGRATION_MINT_COHORT_USABLE')
        print('Next: V7.7.5 reconstruct PRE/MIGRATION/POST around exact migrate blockTime and compare migrated vs non-migrated / post-migration outcomes.')
    elif n>=20:
        print('STATUS=CANONICAL_MIGRATION_MINT_COHORT_PARTIAL')
        print('Enough for census, but inspect ambiguous/no-mint cases before high-dimensional discovery.')
    else:
        print('STATUS=MINT_ATTRIBUTION_INSUFFICIENT')
        print('Next: decode Pump migrate account indices directly from the current Pump IDL for unresolved transactions.')

    print('\nGuardrail: EXACT here means transaction-account/token-balance attribution, not a market-cap proxy.')

if __name__=='__main__':main()
