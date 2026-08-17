#!/usr/bin/env python3
"""Memecoin Lab V5.4 — Adaptive Pipeline Governor.

Observability + scheduling policy for the V5 data factory.
It does not fetch RPC data and does not decode transactions. Instead it measures
where lag actually is, classifies the signature spool into live / signal /
background lanes, and writes a compact policy snapshot that other workers and
visuals can consume.

Important design rule: never delete or silently discard backlog. Low-value old
rows remain available for later backfill; the governor only decides what should
be serviced first.
"""
from __future__ import annotations
import json, math, os, sqlite3, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
RAW_DB=Path(os.environ.get("MEMECOIN_V5_DB", ROOT/"v5_raw_events.db"))
FEAT_DB=Path(os.environ.get("MEMECOIN_V52_DB", ROOT/"v52_features.db"))
LOOP=float(os.environ.get("MEMECOIN_V54_LOOP_S","5"))
LIVE_S=float(os.environ.get("MEMECOIN_V54_LIVE_S","180"))
SIGNAL_S=float(os.environ.get("MEMECOIN_V54_SIGNAL_S","3600"))
STOP=False

def db(path,ro=False):
    if ro:
        c=sqlite3.connect(f"file:{path}?mode=ro",uri=True,timeout=20)
    else:
        c=sqlite3.connect(path,timeout=30)
        c.execute("PRAGMA journal_mode=WAL"); c.execute("PRAGMA synchronous=NORMAL")
    c.row_factory=sqlite3.Row; c.execute("PRAGMA busy_timeout=30000"); return c

def one(c,sql,args=()):
    r=c.execute(sql,args).fetchone(); return None if r is None else r[0]

def initialize():
    if not RAW_DB.exists(): raise SystemExit(f"missing {RAW_DB}")
    c=db(RAW_DB); c.executescript('''
    CREATE TABLE IF NOT EXISTS v54_governor_state(
      key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v54_lane_snapshot(
      captured_at REAL NOT NULL,lane TEXT NOT NULL,pending INTEGER NOT NULL,
      oldest_age_s REAL,newest_age_s REAL,PRIMARY KEY(captured_at,lane));
    '''); c.commit(); c.close()

def feature_stats():
    if not FEAT_DB.exists(): return {"processed":0,"swaps":0,"tokens":0,"snapshots":0,"outcomes_ready":0}
    c=db(FEAT_DB,True)
    out={
      "processed":one(c,"SELECT COUNT(*) FROM v52_processed") or 0,
      "swaps":one(c,"SELECT COUNT(*) FROM v52_swaps") or 0,
      "tokens":one(c,"SELECT COUNT(DISTINCT token_mint) FROM v52_swaps") or 0,
      "snapshots":one(c,"SELECT COUNT(*) FROM v52_snapshots") or 0,
      "outcomes_ready":one(c,"SELECT COUNT(*) FROM v52_outcomes WHERE ready=1") or 0,
    }; c.close(); return out

def raw_stats():
    now=time.time(); c=db(RAW_DB,True)
    raw=one(c,"SELECT COUNT(*) FROM v5_raw_transactions") or 0
    newest=one(c,"SELECT MAX(observed_at) FROM v5_raw_transactions")
    spool=one(c,"SELECT COUNT(*) FROM v51_signature_spool") or 0
    counts={r['status']:r['n'] for r in c.execute("SELECT status,COUNT(*) n FROM v51_signature_spool GROUP BY status")}
    lanes={}
    specs={
      "LIVE":("status='PENDING' AND first_seen>=?",(now-LIVE_S,)),
      "SIGNAL":("status='PENDING' AND first_seen<? AND (event_hint IN ('CREATE','MIGRATE') OR priority<=1)",(now-LIVE_S,)),
      "RECENT":("status='PENDING' AND first_seen<? AND first_seen>=? AND event_hint NOT IN ('CREATE','MIGRATE') AND priority>1",(now-LIVE_S,now-SIGNAL_S)),
      "BACKFILL":("status='PENDING' AND first_seen<? AND event_hint NOT IN ('CREATE','MIGRATE') AND priority>1",(now-SIGNAL_S,)),
    }
    for lane,(where,args) in specs.items():
        r=c.execute(f"SELECT COUNT(*) n,MIN(first_seen) mn,MAX(first_seen) mx FROM v51_signature_spool WHERE {where}",args).fetchone()
        lanes[lane]={"pending":int(r['n'] or 0),"oldest_age_s":None if r['mn'] is None else max(0,now-r['mn']),"newest_age_s":None if r['mx'] is None else max(0,now-r['mx'])}
    c.close(); return {"raw":raw,"raw_age_s":None if newest is None else max(0,now-newest),"spool":spool,"status":counts,"lanes":lanes}

def policy(r,f):
    raw=r['raw']; processed=f['processed']; decode_gap=max(0,raw-processed); pending=r['status'].get('PENDING',0)
    decode_ratio=(processed/raw) if raw else 1.0
    live=r['lanes']['LIVE']['pending']; signal=r['lanes']['SIGNAL']['pending']
    if decode_gap>max(5000,raw*.10): bottleneck='DECODE'
    elif pending>max(10000,raw*2): bottleneck='RPC_ENRICHMENT'
    else: bottleneck='BALANCED'
    # Budget fractions are recommendations for a future scheduler; sum=1.
    if live>5000: budget={"LIVE":.70,"SIGNAL":.20,"RECENT":.08,"BACKFILL":.02}
    elif signal>5000: budget={"LIVE":.45,"SIGNAL":.35,"RECENT":.15,"BACKFILL":.05}
    else: budget={"LIVE":.40,"SIGNAL":.25,"RECENT":.20,"BACKFILL":.15}
    return {"bottleneck":bottleneck,"decode_gap":decode_gap,"decode_coverage":decode_ratio,"recommended_budget":budget}

def persist(r,f,p):
    now=time.time(); c=db(RAW_DB); c.execute('BEGIN IMMEDIATE')
    try:
        obj={"raw":r,"features":f,"policy":p}
        c.execute("INSERT INTO v54_governor_state(key,value_json,updated_at) VALUES('latest',?,?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",(json.dumps(obj,separators=(',',':')),now))
        for lane,x in r['lanes'].items():
            c.execute("INSERT INTO v54_lane_snapshot(captured_at,lane,pending,oldest_age_s,newest_age_s) VALUES(?,?,?,?,?)",(now,lane,x['pending'],x['oldest_age_s'],x['newest_age_s']))
        c.execute("DELETE FROM v54_lane_snapshot WHERE captured_at<?",(now-86400,)); c.commit()
    except: c.rollback(); raise
    finally:c.close()

def age(x):
    if x is None:return '—'
    if x<60:return f'{x:.0f}s'
    if x<3600:return f'{x/60:.1f}m'
    return f'{x/3600:.1f}h'

def emit(r,f,p):
    print('\033[2J\033[H',end='')
    print('='*112); print('MEMECOIN LAB — V5.4 ADAPTIVE PIPELINE GOVERNOR'); print('='*112)
    print(f"BOTTLENECK {p['bottleneck']:<16} RAW {r['raw']:,} | PROCESSED {f['processed']:,} | GAP {p['decode_gap']:,} | DECODE {p['decode_coverage']*100:5.2f}% | RAW AGE {age(r['raw_age_s'])}")
    print(f"SPOOL pending={r['status'].get('PENDING',0):,} done={r['status'].get('DONE',0):,} fetching={r['status'].get('FETCHING',0):,} failed={r['status'].get('FAILED',0):,}")
    print('\nLANES')
    for lane in ('LIVE','SIGNAL','RECENT','BACKFILL'):
        x=r['lanes'][lane]; b=p['recommended_budget'][lane]
        print(f"  {lane:<9} pending={x['pending']:>9,} oldest={age(x['oldest_age_s']):>7} newest={age(x['newest_age_s']):>7} recommended_rpc={b*100:5.1f}%")
    print(f"\nSCIENCE swaps={f['swaps']:,} tokens={f['tokens']:,} snapshots={f['snapshots']:,} ready_outcomes={f['outcomes_ready']:,}")
    print('\nNo backlog is deleted. Governor only measures and recommends service priority.')

def main():
    initialize()
    while True:
        try:
            r=raw_stats(); f=feature_stats(); p=policy(r,f); persist(r,f,p); emit(r,f,p)
        except KeyboardInterrupt: break
        except Exception as e: print('V5.4 error:',repr(e))
        time.sleep(LOOP)
if __name__=='__main__': main()
