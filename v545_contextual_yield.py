#!/usr/bin/env python3
"""Memecoin Lab V5.4.5 — contextual scientific-yield scorer.

Learns expected scientific value from information available BEFORE an RPC call:
source program, event hint, priority bucket, age bucket, log length and log
fingerprints. Scores pending spool rows individually and stores expected_yield.
Research-only; no signing or transaction submission.
"""
from __future__ import annotations
import hashlib,json,math,os,re,sqlite3,time
from collections import defaultdict
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
FEAT=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
LOOP=float(os.environ.get('MEMECOIN_V545_LOOP_S','12'))
TRAIN=int(os.environ.get('MEMECOIN_V545_TRAIN','12000'))
SCORE_N=int(os.environ.get('MEMECOIN_V545_SCORE_N','60000'))
PRIOR_N=float(os.environ.get('MEMECOIN_V545_PRIOR_N','20'))
PRIOR_Y=float(os.environ.get('MEMECOIN_V545_PRIOR_Y','0.35'))
REWARD={'SWAP':1.0,'CREATE':1.0,'MIGRATE':1.2,'OTHER':.05,'UNDECODED':0.0,'ERROR':0.0}
PATTERNS=['instruction: buy','instruction: sell','instruction: create','instruction: migrate','swap','transfer','mintto','closeaccount']

def db(path,ro=False):
    if ro:c=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    else:
        c=sqlite3.connect(path,timeout=30); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL')
    c.row_factory=sqlite3.Row; c.execute('PRAGMA busy_timeout=30000'); return c

def init():
    if not RAW.exists(): raise SystemExit(f'missing {RAW}')
    if not FEAT.exists(): raise SystemExit(f'missing {FEAT}')
    c=db(RAW); c.executescript('''
    CREATE TABLE IF NOT EXISTS v545_context_model(
      feature_key TEXT PRIMARY KEY,n INTEGER NOT NULL,reward REAL NOT NULL,
      mean_yield REAL NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v545_signature_score(
      signature TEXT PRIMARY KEY,expected_yield REAL NOT NULL,confidence REAL NOT NULL,
      context_json TEXT NOT NULL,scored_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v545_score ON v545_signature_score(expected_yield DESC);
    CREATE TABLE IF NOT EXISTS v545_state(key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); c.commit(); c.close()

def age_bucket(age):
    if age<180:return '<3m'
    if age<900:return '3-15m'
    if age<3600:return '15-60m'
    return '>60m'

def pri_bucket(p):
    if p<=2:return 'P0'
    if p<=12:return 'P1'
    return 'P2'

def log_features(logs):
    text='\n'.join(logs or []).lower(); n=len(logs or [])
    out=[f'LOGN:{min(20,(n//5)*5)}']
    for p in PATTERNS:
        if p in text:out.append('PAT:'+p)
    return out

def context(row,now):
    try:logs=json.loads(row['logs_json'] or '[]')
    except Exception:logs=[]
    age=max(0,now-float(row['first_seen'] or now))
    feats=[f"SRC:{row['source_program']}",f"EV:{row['event_hint']}",f"PRI:{pri_bucket(int(row['priority'] or 99))}",f"AGE:{age_bucket(age)}"]+log_features(logs)
    return feats,{'source':row['source_program'],'event':row['event_hint'],'priority':int(row['priority'] or 99),'age_bucket':age_bucket(age),'log_n':len(logs)}

def train():
    r=db(RAW,True); f=db(FEAT,True)
    rows=r.execute('''SELECT s.signature,s.source_program,s.event_hint,s.priority,s.first_seen,s.logs_json,x.observed_at
                      FROM v5_raw_transactions x JOIN v51_signature_spool s USING(signature)
                      ORDER BY x.observed_at DESC LIMIT ?''',(TRAIN,)).fetchall()
    sigs=[x['signature'] for x in rows]; status={}
    for i in range(0,len(sigs),500):
        ch=sigs[i:i+500]
        if not ch:continue
        q=','.join('?' for _ in ch)
        for z in f.execute(f'SELECT signature,status FROM v52_processed WHERE signature IN ({q})',ch):status[z['signature']]=z['status']
    r.close();f.close(); now=time.time(); agg=defaultdict(lambda:[0,0.0])
    for x in rows:
        st=status.get(x['signature'])
        if st is None:continue
        rew=REWARD.get(st,0.0); feats,_=context(x,now)
        for key in feats:agg[key][0]+=1;agg[key][1]+=rew
    c=db(RAW); c.execute('BEGIN IMMEDIATE')
    try:
        c.execute('DELETE FROM v545_context_model')
        for key,(n,rw) in agg.items():
            mean=(rw+PRIOR_N*PRIOR_Y)/(n+PRIOR_N)
            c.execute('INSERT INTO v545_context_model VALUES(?,?,?,?,?)',(key,n,rw,mean,now))
        c.commit()
    except:c.rollback();raise
    finally:c.close()
    return len(rows),len(agg)

def load_model():
    c=db(RAW,True); m={r['feature_key']:(int(r['n']),float(r['mean_yield'])) for r in c.execute('SELECT * FROM v545_context_model')};c.close();return m

def score_pending(model):
    now=time.time(); c=db(RAW); rows=c.execute('''SELECT signature,source_program,event_hint,priority,first_seen,logs_json
      FROM v51_signature_spool WHERE status='PENDING' ORDER BY first_seen DESC LIMIT ?''',(SCORE_N,)).fetchall(); vals=[]
    for r in rows:
        feats,meta=context(r,now); weighted=[]; totaln=0
        for k in feats:
            n,y=model.get(k,(0,PRIOR_Y)); w=min(1.0,math.log1p(n)/5.0); weighted.append((w,y)); totaln+=n
        if weighted and sum(w for w,_ in weighted)>0: pred=sum(w*y for w,y in weighted)/sum(w for w,_ in weighted)
        else: pred=PRIOR_Y
        # structural events retain a floor so contextual learning cannot starve them
        if (r['event_hint'] or '').upper() in ('CREATE','MIGRATE'): pred=max(pred,.75)
        conf=min(1.0,math.log1p(totaln)/8.0)
        vals.append((r['signature'],float(pred),float(conf),json.dumps({'features':feats,'meta':meta},separators=(',',':')),now))
    c.execute('BEGIN IMMEDIATE')
    try:
        c.executemany('''INSERT INTO v545_signature_score VALUES(?,?,?,?,?) ON CONFLICT(signature) DO UPDATE SET
          expected_yield=excluded.expected_yield,confidence=excluded.confidence,context_json=excluded.context_json,scored_at=excluded.scored_at''',vals)
        state={'scored':len(vals),'top':[]}
        for r in c.execute('SELECT signature,expected_yield,confidence FROM v545_signature_score ORDER BY expected_yield DESC LIMIT 8'):
            state['top'].append(dict(r))
        c.execute("INSERT INTO v545_state VALUES('latest',?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",(json.dumps(state,separators=(',',':')),now));c.commit()
    except:c.rollback();raise
    finally:c.close()
    return len(vals)

def main():
    init()
    while True:
        try:
            ntrain,nfeat=train(); model=load_model(); ns=score_pending(model)
            top=sorted(model.items(),key=lambda kv:kv[1][1],reverse=True)[:6]
            print('V5.4.5 CONTEXT | train=%s features=%s scored=%s top=%s'%(ntrain,nfeat,ns,' | '.join(f'{k}={v[1]:.3f}(n={v[0]})' for k,v in top)),flush=True)
        except KeyboardInterrupt:break
        except Exception as e:print('V5.4.5 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__':main()
