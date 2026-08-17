#!/usr/bin/env python3
"""Memecoin Lab V5.4.2 — Scientific Admission Controller.

Assigns admission tiers to pending spool signatures without deleting backlog.
Compatible with spool schemas that do not contain token_hint.
"""
from __future__ import annotations
import hashlib, json, os, sqlite3, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW_DB=Path(os.environ.get('MEMECOIN_V5_DB',ROOT/'v5_raw_events.db'))
FEAT_DB=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
LOOP=float(os.environ.get('MEMECOIN_V542_LOOP_S','10'))
RECENT_S=float(os.environ.get('MEMECOIN_V542_RECENT_S','900'))
EXPLORE_RATE=float(os.environ.get('MEMECOIN_V542_EXPLORE_RATE','0.03'))
BATCH=int(os.environ.get('MEMECOIN_V542_BATCH','50000'))

def db(path,ro=False):
    if ro: c=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    else:
        c=sqlite3.connect(path,timeout=30); c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA synchronous=NORMAL')
    c.row_factory=sqlite3.Row; c.execute('PRAGMA busy_timeout=30000'); return c

def init():
    if not RAW_DB.exists(): raise SystemExit(f'missing {RAW_DB}')
    c=db(RAW_DB); c.executescript('''
    CREATE TABLE IF NOT EXISTS v542_admission(
      signature TEXT PRIMARY KEY,
      tier TEXT NOT NULL,
      score REAL NOT NULL,
      reason TEXT NOT NULL,
      decided_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v542_tier_score ON v542_admission(tier,score DESC);
    CREATE TABLE IF NOT EXISTS v542_state(
      key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); c.commit(); c.close()

def deterministic_sample(sig,rate):
    h=int(hashlib.blake2b(sig.encode(),digest_size=8).hexdigest(),16)
    return (h/2**64)<rate

def known_tokens():
    if not FEAT_DB.exists(): return set()
    c=db(FEAT_DB,True)
    try: out={r[0] for r in c.execute('SELECT DISTINCT token_mint FROM v52_swaps WHERE token_mint IS NOT NULL')}
    except sqlite3.Error: out=set()
    c.close(); return out

def classify(r,known,now):
    ev=(r['event_hint'] or 'OTHER').upper(); pri=int(r['priority'] or 99); age=max(0,now-float(r['first_seen'] or now))
    # token_hint is optional in the live spool schema.  Do not invent it.
    token=r['token_hint'] if 'token_hint' in r.keys() else None
    if ev in ('CREATE','MIGRATE') or pri<=1:
        return 'A',1000-age/60,'structural/high-priority'
    if age<=RECENT_S and token and token in known:
        return 'B',700-age/60,'known-token recent follow-up'
    if age<=RECENT_S and deterministic_sample(r['signature'],EXPLORE_RATE):
        return 'C',400-age/60,'deterministic exploration sample'
    return 'D',100-min(age/3600,99),'preserved backfill'

def cycle():
    now=time.time(); known=known_tokens(); c=db(RAW_DB)
    cols={r[1] for r in c.execute('PRAGMA table_info(v51_signature_spool)')}
    token_expr='token_hint' if 'token_hint' in cols else 'NULL AS token_hint'
    rows=c.execute(f"""SELECT signature,event_hint,priority,first_seen,{token_expr}
                      FROM v51_signature_spool
                      WHERE status='PENDING'
                      ORDER BY first_seen DESC LIMIT ?""",(BATCH,)).fetchall()
    vals=[]; counts={'A':0,'B':0,'C':0,'D':0}
    for r in rows:
        tier,score,reason=classify(r,known,now); counts[tier]+=1
        vals.append((r['signature'],tier,float(score),reason,now))
    c.execute('BEGIN IMMEDIATE')
    try:
        c.executemany("""INSERT INTO v542_admission(signature,tier,score,reason,decided_at)
                         VALUES(?,?,?,?,?) ON CONFLICT(signature) DO UPDATE SET
                         tier=excluded.tier,score=excluded.score,reason=excluded.reason,decided_at=excluded.decided_at""",vals)
        state={'classified':len(vals),'known_tokens':len(known),'tiers':counts,'explore_rate':EXPLORE_RATE,'recent_s':RECENT_S,'spool_has_token_hint':('token_hint' in cols)}
        c.execute("""INSERT INTO v542_state(key,value_json,updated_at) VALUES('latest',?,?)
                     ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
                  (json.dumps(state,separators=(',',':')),now))
        c.commit()
    except: c.rollback(); raise
    finally:c.close()
    print(f"V5.4.2 ADMISSION | classified={len(vals):,} known_tokens={len(known):,} A={counts['A']:,} B={counts['B']:,} C={counts['C']:,} D={counts['D']:,} explore={EXPLORE_RATE*100:.1f}% token_hint={'yes' if 'token_hint' in cols else 'no'}",flush=True)

def main():
    init()
    while True:
        try: cycle()
        except KeyboardInterrupt: break
        except Exception as e: print('V5.4.2 error:',repr(e),flush=True)
        time.sleep(LOOP)
if __name__=='__main__': main()
