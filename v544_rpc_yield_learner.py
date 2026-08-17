#!/usr/bin/env python3
"""Memecoin Lab V5.4.4 — adaptive RPC yield learner.

Research-only acquisition control plane.
Learns which admission tiers produce useful decoded science and publishes a dynamic
A/B/C/D RPC budget. It never deletes backlog and never trades/signs transactions.

Reward is derived from existing V5.2 decoder labels:
  SWAP=1.00, CREATE=1.00, MIGRATE=1.20, OTHER=0.05, UNDECODED=0.00.
A small exploration floor prevents starvation of any tier.
"""
from __future__ import annotations
import json, math, os, sqlite3, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW=Path(os.environ.get("MEMECOIN_V5_DB",ROOT/"v5_raw_events.db"))
FEAT=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
LOOP=float(os.environ.get("MEMECOIN_V544_LOOP_S","10"))
WINDOW=int(os.environ.get("MEMECOIN_V544_WINDOW","5000"))
FLOOR=float(os.environ.get("MEMECOIN_V544_FLOOR","0.05"))
TEMP=float(os.environ.get("MEMECOIN_V544_TEMP","0.20"))
PRIOR_N=float(os.environ.get("MEMECOIN_V544_PRIOR_N","25"))
PRIOR_Y=float(os.environ.get("MEMECOIN_V544_PRIOR_YIELD","0.35"))
REWARD={"SWAP":1.0,"CREATE":1.0,"MIGRATE":1.2,"OTHER":.05,"UNDECODED":0.0}

def db(path,ro=False):
    if ro: c=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=30)
    else:
        c=sqlite3.connect(path,timeout=30); c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=NORMAL")
    c.row_factory=sqlite3.Row; c.execute("PRAGMA busy_timeout=30000"); return c

def init():
    if not RAW.exists(): raise SystemExit(f"missing {RAW}")
    if not FEAT.exists(): raise SystemExit(f"missing {FEAT}")
    c=db(RAW); c.executescript('''
    CREATE TABLE IF NOT EXISTS v544_rpc_yield(
      tier TEXT PRIMARY KEY,
      n INTEGER NOT NULL,
      reward REAL NOT NULL,
      mean_yield REAL NOT NULL,
      budget REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v544_state(
      key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); c.commit(); c.close()

def observations():
    r=db(RAW,True); f=db(FEAT,True)
    # Recent enriched signatures that have an admission decision.
    rows=r.execute('''SELECT x.signature,a.tier,x.observed_at
                      FROM v5_raw_transactions x JOIN v542_admission a USING(signature)
                      ORDER BY x.observed_at DESC LIMIT ?''',(WINDOW,)).fetchall()
    sigs=[x['signature'] for x in rows]
    status={}
    # Avoid SQLite variable limits.
    for i in range(0,len(sigs),500):
        chunk=sigs[i:i+500]
        if not chunk: continue
        q=','.join('?' for _ in chunk)
        for z in f.execute(f"SELECT signature,status FROM v52_processed WHERE signature IN ({q})",chunk): status[z['signature']]=z['status']
    r.close(); f.close()
    stats={k:{'n':0,'reward':0.0,'labels':{}} for k in 'ABCD'}
    for x in rows:
        st=status.get(x['signature'])
        if st is None: continue
        t=x['tier']; rew=REWARD.get(st,0.0); s=stats[t]; s['n']+=1; s['reward']+=rew; s['labels'][st]=s['labels'].get(st,0)+1
    return stats

def budgets(stats):
    # Bayesian shrinkage toward a neutral prior, then softmax + exploration floor.
    score={}
    for t,s in stats.items():
        score[t]=(s['reward']+PRIOR_N*PRIOR_Y)/(s['n']+PRIOR_N)
    m=max(score.values()); ex={t:math.exp((v-m)/max(.03,TEMP)) for t,v in score.items()}; den=sum(ex.values()) or 1
    free=max(0.0,1.0-4*FLOOR)
    b={t:FLOOR+free*ex[t]/den for t in 'ABCD'}
    return score,b

def cycle():
    stats=observations(); score,b=budgets(stats); now=time.time(); c=db(RAW)
    for t in 'ABCD':
        s=stats[t]; c.execute('''INSERT INTO v544_rpc_yield(tier,n,reward,mean_yield,budget,updated_at) VALUES(?,?,?,?,?,?)
          ON CONFLICT(tier) DO UPDATE SET n=excluded.n,reward=excluded.reward,mean_yield=excluded.mean_yield,budget=excluded.budget,updated_at=excluded.updated_at''',
          (t,s['n'],s['reward'],score[t],b[t],now))
    payload={'window':WINDOW,'scores':score,'budgets':b,'stats':stats}
    c.execute('''INSERT INTO v544_state(key,value_json,updated_at) VALUES('latest',?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at''',(json.dumps(payload,separators=(',',':')),now)); c.commit(); c.close()
    parts=[]
    for t in 'ABCD':
        s=stats[t]; labels=','.join(f'{k}:{v}' for k,v in sorted(s['labels'].items())) or '-'
        parts.append(f"{t}[n={s['n']} y={score[t]:.3f} budget={100*b[t]:.1f}% {labels}]")
    print('V5.4.4 YIELD | '+' '.join(parts),flush=True)

def main():
    init()
    while True:
        try: cycle()
        except KeyboardInterrupt: break
        except Exception as e: print('V5.4.4 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__': main()
