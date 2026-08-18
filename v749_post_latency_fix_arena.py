#!/usr/bin/env python3
"""MEMECOIN LAB — POST-LATENCY-FIX FRESH CAUSAL ARENA V7.4.9

Fresh prospective re-test of the exact V7.4.1 frozen CAPITAL/FLOW/WALLET rules
using the V7.4.2.3 snapshot-first append-only state machine, but only after a
healthy V7.4.7 acquisition epoch has been observed.

Scientific guardrails
---------------------
- brand-new DB and common cutoff on first launch;
- exact V7.4.1 rules, no retuning;
- inherits V7.4.2.3 first-seen/NULL/late/fill/maturity invariants;
- requires a healthy current V7.4.7 epoch before freezing;
- records the acquisition epoch and latency health at freeze;
- if the acquisition epoch changes later, evidence collection PAUSES rather
  than silently spanning an acquisition restart;
- old V7.4.2/.1/.2.2/.2.3 evidence is never imported.

Paper research only. Never signs or submits transactions.
"""
from __future__ import annotations
import signal, sqlite3, statistics, time
from pathlib import Path

import v7423_snapshot_first_causal_arena as base

ROOT=Path.home()/"memecoin_lab"
TRACE=ROOT/'v747_acquisition_trace.db'
OUT=ROOT/'v749_post_latency_future.db'
MIN_HOT=100
MAX_HOT_P90_STORE_S=1.0

# Redirect the proven V7423 state machine into a completely fresh DB.
base.OUT=OUT


def tdb():
    d=sqlite3.connect(f'file:{TRACE}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d


def pct(xs,q):
    if not xs:return None
    ys=sorted(float(x) for x in xs);p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def acquisition_health():
    if not TRACE.exists():raise RuntimeError('Missing v747_acquisition_trace.db; V747 must be running first')
    d=tdb()
    z=d.execute("SELECT epoch_id,MAX(updated_at) AS newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1").fetchone()
    if not z:d.close();raise RuntimeError('No V747 acquisition epoch found')
    epoch=str(z['epoch_id'])
    rows=d.execute("SELECT enqueue_at,raw_store_at FROM trace WHERE epoch_id=? AND kind='HOT' AND enqueue_at IS NOT NULL AND raw_store_at IS NOT NULL ORDER BY raw_store_at DESC LIMIT 2000",(epoch,)).fetchall()
    q=d.execute("SELECT pending_total,pending_hot,fetching,oldest_pending_age_s,current_rps,sampled_at FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1",(epoch,)).fetchone()
    d.close()
    lags=[max(0.0,float(r['raw_store_at'])-float(r['enqueue_at'])) for r in rows]
    n=len(lags);p50=pct(lags,.5);p90=pct(lags,.9)
    pending=int(q['pending_total']) if q else 0;ph=int(q['pending_hot']) if q else 0;fetching=int(q['fetching']) if q else 0;old=float(q['oldest_pending_age_s']) if q else 0.0;rps=float(q['current_rps']) if q else 0.0
    healthy=(n>=MIN_HOT and p90 is not None and p90<=MAX_HOT_P90_STORE_S and pending==0 and ph==0 and old<5.0)
    return {'epoch_id':epoch,'hot_n':n,'hot_p50':p50,'hot_p90':p90,'pending':pending,'pending_hot':ph,'fetching':fetching,'oldest_pending':old,'rps':rps,'healthy':healthy}


def init_meta():
    d=base.odb();d.execute('''CREATE TABLE IF NOT EXISTS infrastructure_freeze(
      id INTEGER PRIMARY KEY CHECK(id=1), frozen_at REAL, acquisition_epoch TEXT,
      hot_n INTEGER,hot_enqueue_store_p50 REAL,hot_enqueue_store_p90 REAL,
      pending_total INTEGER,pending_hot INTEGER,oldest_pending_s REAL,current_rps REAL,
      note TEXT)''');d.commit();d.close()


def freeze_once_strict():
    d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone();d.close()
    if a:return
    h=acquisition_health()
    if not h['healthy']:
        raise RuntimeError(f"V747 acquisition not stable enough to freeze: HOT n={h['hot_n']} p90={h['hot_p90']} pending={h['pending']} oldest={h['oldest_pending']:.2f}s")
    # This creates the fresh common cutoff using the current maximum V52 cutoff.
    base.freeze_once()
    d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone()
    d.execute("UPDATE arena SET method='POST_LATENCY_FIX_SNAPSHOT_FIRST_V1' WHERE arena_id=?",(a['arena_id'],))
    d.execute('INSERT OR REPLACE INTO infrastructure_freeze VALUES(?,?,?,?,?,?,?,?,?,?,?)',(
      1,time.time(),h['epoch_id'],h['hot_n'],h['hot_p50'],h['hot_p90'],h['pending'],h['pending_hot'],h['oldest_pending'],h['rps'],
      'V747 healthy acquisition required before fresh cutoff; pause on acquisition epoch change'))
    d.commit();d.close()


def frozen_infra():
    d=base.odb();z=d.execute('SELECT * FROM infrastructure_freeze WHERE id=1').fetchone();d.close();return dict(z) if z else None


def current_epoch():
    try:return acquisition_health()['epoch_id']
    except Exception:return None


def display_header_infra():
    f=frozen_infra();cur=current_epoch()
    if not f:return
    state='HEALTHY_CONTINUITY' if cur==f['acquisition_epoch'] else 'PAUSED_EPOCH_CHANGED'
    print(f"INFRA {state} | frozen_epoch={f['acquisition_epoch']} current_epoch={cur} | HOT freeze n={f['hot_n']} enqueue->store p50/p90={f['hot_enqueue_store_p50']:.3f}/{f['hot_enqueue_store_p90']:.3f}s")


def cycle():
    freeze_once_strict()
    f=frozen_infra();cur=current_epoch()
    if not f:raise RuntimeError('Infrastructure freeze missing')
    if cur!=f['acquisition_epoch']:
        print('\033[2J\033[H',end='')
        print('='*184);print('MEMECOIN LAB — POST-LATENCY-FIX FRESH CAUSAL ARENA V7.4.9');print('='*184)
        print(f"PAUSED: acquisition epoch changed | frozen={f['acquisition_epoch']} current={cur}")
        print('No new evidence is ingested. Start a new future-only arena after acquisition continuity is restored.')
        return
    o=base.odb();a=dict(o.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(z) for z in o.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()];o.close()
    for r in rules:
        base.ingest_all_visible_snapshots(a,r)
        base.progress_signal_locked(a,r)
        base.progress_fill_locked(a,r)
        base.audit_invariants(r)
        base.summarize(r)
    # Native V7423 display, then explicit infra continuity line.
    base.display(a,rules)
    display_header_infra()
    print('V7.4.9 guardrail: fresh post-latency-fix cohort only; exact frozen rules; no retuning; no old evidence imported.')


if __name__=='__main__':
    signal.signal(signal.SIGINT,base.stop);signal.signal(signal.SIGTERM,base.stop)
    base.init_schema();init_meta()
    print('V7.4.9 boot: validating V747 acquisition health before creating any cutoff...',flush=True)
    while not base.STOP:
        try:cycle()
        except Exception as e:print('V7.4.9 error:',repr(e),flush=True)
        time.sleep(max(.1,base.LOOP))
