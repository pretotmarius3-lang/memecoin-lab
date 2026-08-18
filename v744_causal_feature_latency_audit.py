#!/usr/bin/env python3
"""MEMECOIN LAB — CAUSAL FEATURE LATENCY AUDIT V7.4.4

READ-ONLY diagnostic layer over V7.4.2.3 + V5.2.
It does not change strategies, snapshots, swaps, cutoffs, or verdicts.

Purpose
-------
Separate three latency mechanisms that can make a paper rule non-executable:
1) source/decoder lag: chain timestamp -> swap observed_at in V52;
2) feature/snapshot availability lag: strategy cutoff -> first observation by V7423;
3) intrinsic feature availability: feature NULL at first observation even though snapshot row exists.

Important limitation
--------------------
v52_snapshots.built_at is mutable because the active feature factory rebuilds rows.
Therefore this audit deliberately does NOT treat current built_at as first-build time.
V7423.first_observed_at is the prospective first-seen timestamp used for causal timing.

Paper research only. No live trading/signing.
"""
from __future__ import annotations
import math, sqlite3, statistics, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
ARENA=ROOT/'v7423_snapshot_first_future.db'
V52=ROOT/'v52_features.db'
OUT=ROOT/'v744_latency_audit.db'
FILL_WINDOW=15.0


def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def odb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=30000');return d

def sf(x,d=None):
    try:
        v=float(x);return v if math.isfinite(v) else d
    except:return d

def pct(xs,q):
    if not xs:return None
    ys=sorted(xs);p=(len(ys)-1)*q;lo=int(math.floor(p));hi=int(math.ceil(p))
    return ys[lo] if lo==hi else ys[lo]+(ys[hi]-ys[lo])*(p-lo)

def init():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS audit_run(
      run_id TEXT PRIMARY KEY,created_at REAL,arena_id TEXT,common_cutoff REAL,note TEXT);
    CREATE TABLE IF NOT EXISTS family_latency(
      run_id TEXT,family TEXT,stage_s INTEGER,observed INTEGER,feature_ok INTEGER,feature_null INTEGER,
      late_snapshot INTEGER,signals INTEGER,done INTEGER,
      snapshot_lag_p50 REAL,snapshot_lag_p90 REAL,snapshot_lag_p95 REAL,snapshot_lag_max REAL,
      pct_seen_le15 REAL,pct_seen_le30 REAL,pct_seen_le60 REAL,
      swap_transport_n INTEGER,swap_transport_p50 REAL,swap_transport_p90 REAL,swap_transport_p95 REAL,swap_transport_max REAL,
      diagnosis TEXT,PRIMARY KEY(run_id,family));
    CREATE TABLE IF NOT EXISTS sample_event(
      run_id TEXT,family TEXT,token_mint TEXT,cutoff_ts REAL,first_observed_at REAL,snapshot_lag_s REAL,
      feature_available INTEGER,locked_feature REAL,state TEXT,nearest_swap_ts REAL,nearest_swap_observed_at REAL,
      swap_transport_lag_s REAL);
    ''');d.commit();d.close()

def main():
    if not ARENA.exists():raise SystemExit(f'Missing {ARENA}')
    if not V52.exists():raise SystemExit(f'Missing {V52}')
    init()
    a=ro(ARENA);v=ro(V52);o=odb()
    ar=a.execute('SELECT * FROM arena LIMIT 1').fetchone()
    if not ar:raise SystemExit('V7423 arena row missing')
    run_id=f"L744_{int(time.time())}"
    o.execute('INSERT INTO audit_run VALUES(?,?,?,?,?)',(run_id,time.time(),ar['arena_id'],ar['common_cutoff'],'V7423 first-seen timing; current V52 built_at intentionally ignored'))
    rules=[dict(r) for r in a.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()]
    print('='*178);print('MEMECOIN LAB — CAUSAL FEATURE LATENCY AUDIT V7.4.4');print('='*178)
    print(f"arena={ar['arena_id']} cutoff>{float(ar['common_cutoff']):.3f} | execution window={FILL_WINDOW:.0f}s | V7423/V52 READ-ONLY")
    print('Timing truth: snapshot first-seen comes from V7423.first_observed_at; mutable v52_snapshots.built_at is NOT used as first-build evidence.\n')
    for r in rules:
        ev=[dict(x) for x in a.execute('SELECT * FROM events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(r['rule_id'],)).fetchall()]
        lags=[max(0.0,float(e['first_observed_at'])-float(e['cutoff_ts'])) for e in ev]
        feature_ok=sum(int(e['feature_available'])==1 for e in ev);feature_null=len(ev)-feature_ok
        late=sum(e['state']=='LATE_SNAPSHOT' for e in ev);signals=sum(int(e.get('signal_decision') or 0)==1 for e in ev);done=sum(e['state']=='DONE' for e in ev)
        transport=[];samples=[]
        for e in ev:
            sw=v.execute('''SELECT timestamp,observed_at FROM v52_swaps WHERE token_mint=? AND timestamp<=? AND observed_at IS NOT NULL ORDER BY timestamp DESC LIMIT 1''',(e['token_mint'],e['cutoff_ts'])).fetchone()
            st=so=sl=None
            if sw:
                st=sf(sw['timestamp']);so=sf(sw['observed_at']);sl=(so-st) if st is not None and so is not None else None
                if sl is not None and -1<=sl<3600:transport.append(max(0.0,sl))
            samples.append((e,st,so,sl))
        seen15=100*sum(x<=15 for x in lags)/len(lags) if lags else 0.0
        seen30=100*sum(x<=30 for x in lags)/len(lags) if lags else 0.0
        seen60=100*sum(x<=60 for x in lags)/len(lags) if lags else 0.0
        null_pct=100*feature_null/len(ev) if ev else 0.0
        if not ev:diag='NO_DATA_YET'
        elif null_pct>=80:diag='FEATURE_INTRINSIC_OR_UPSTREAM_UNAVAILABLE_AT_FIRST_SEEN'
        elif seen15<20 and (pct(transport,.50) or 0)>15:diag='RAW_SOURCE_OR_DECODER_LATENCY_DOMINANT'
        elif seen15<20:diag='SNAPSHOT_BUILD_OR_DELIVERY_LATENCY_DOMINANT'
        elif seen15<70:diag='MIXED_LATENCY_EXECUTION_CONSTRAINED'
        else:diag='CAUSAL_TIMING_MOSTLY_WITHIN_15S'
        vals=(run_id,r['family'],r['stage_s'],len(ev),feature_ok,feature_null,late,signals,done,
              pct(lags,.50),pct(lags,.90),pct(lags,.95),max(lags) if lags else None,seen15,seen30,seen60,
              len(transport),pct(transport,.50),pct(transport,.90),pct(transport,.95),max(transport) if transport else None,diag)
        o.execute('INSERT INTO family_latency VALUES('+','.join('?'*22)+')',vals)
        for e,st,so,sl in samples[-20:]:
            o.execute('INSERT INTO sample_event VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(run_id,r['family'],e['token_mint'],e['cutoff_ts'],e['first_observed_at'],float(e['first_observed_at'])-float(e['cutoff_ts']),e['feature_available'],e['locked_feature'],e['state'],st,so,sl))
        print(f"{r['family']:<20} stage={r['stage_s']:>2}s observed={len(ev):4d} feature_ok={feature_ok:4d} NULL={feature_null:4d} ({null_pct:5.1f}%) late={late:4d} signals={signals:3d} DONE={done:3d}")
        print(f"  snapshot first-seen lag: p50={sf(pct(lags,.50),0):6.1f}s p90={sf(pct(lags,.90),0):6.1f}s p95={sf(pct(lags,.95),0):6.1f}s max={sf(max(lags) if lags else None,0):7.1f}s | <=15/30/60s = {seen15:5.1f}/{seen30:5.1f}/{seen60:5.1f}%")
        print(f"  input swap transport lag: n={len(transport):4d} p50={sf(pct(transport,.50),0):5.1f}s p90={sf(pct(transport,.90),0):5.1f}s p95={sf(pct(transport,.95),0):5.1f}s max={sf(max(transport) if transport else None,0):6.1f}s")
        print(f"  DIAGNOSIS: {diag}\n")
    o.commit();o.close();a.close();v.close()
    print('INTERPRETATION')
    print('  - High swap transport lag => data/RPC/decoder arrives too late before feature construction.')
    print('  - Low swap lag but high snapshot first-seen lag => feature factory / rebuild / DB delivery is the bottleneck.')
    print('  - High first-seen NULL rate => the feature itself is not causally available when the snapshot first exists.')
    print('  - Do NOT change strategy thresholds or fill windows from this audit; infrastructure fixes require a fresh arena.')
    print(f'OUTPUT={OUT}')
    print('Guardrail: diagnostic evidence about timing infrastructure only; no strategy promotion or capital decision.')

if __name__=='__main__':main()
