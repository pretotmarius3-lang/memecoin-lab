#!/usr/bin/env python3
"""MEMECOIN LAB — POST-BURST FRESH CAUSAL ARENA V7.5.2

Fresh prospective re-test of the exact V7.4.1 frozen CAPITAL/FLOW/WALLET rules,
after V7.5.0 burst-resilient acquisition has returned to a stable low-latency state.

Guardrails
----------
- brand-new DB and common cutoff;
- exact frozen rules, no retuning;
- inherits V7.4.2.3 snapshot-first causal state machine;
- V7.5.0 acquisition epoch is frozen with the arena;
- live acquisition health is checked every cycle;
- evidence ingestion PAUSES if epoch changes, queue degrades, or recent HOT latency
  breaches limits;
- old V7.4.x/V7.4.9 evidence is never imported.

Paper research only. Never signs or submits transactions.
"""
from __future__ import annotations

import signal, sqlite3, time
from pathlib import Path

import v7423_snapshot_first_causal_arena as base

ROOT = Path.home()/"memecoin_lab"
TRACE = ROOT/'v750_acquisition_trace.db'
OUT = ROOT/'v752_post_burst_future.db'
base.OUT = OUT

# Strict operational health gates.
MIN_RECENT_HOT = 50
RECENT_WINDOW_S = 60.0
MAX_P90_S = 2.0
MAX_P95_S = 3.0
MAX_PENDING = 0
MAX_OLDEST_S = 2.0
MAX_SAMPLE_AGE_S = 20.0


def tdb():
    d=sqlite3.connect(f'file:{TRACE}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def pct(xs,q):
    if not xs:return None
    ys=sorted(float(x) for x in xs)
    p=(len(ys)-1)*q;lo=int(p);hi=min(len(ys)-1,lo+1);f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def live_health():
    if not TRACE.exists():
        return {'healthy':False,'reason':'missing_v750_trace'}
    d=tdb();now=time.time()
    ep=d.execute("SELECT epoch_id,MAX(updated_at) newest FROM trace WHERE epoch_id IS NOT NULL GROUP BY epoch_id ORDER BY newest DESC LIMIT 1").fetchone()
    if not ep:
        d.close();return {'healthy':False,'reason':'no_epoch'}
    epoch=str(ep['epoch_id'])
    q=d.execute("SELECT * FROM queue_sample WHERE epoch_id=? ORDER BY sampled_at DESC LIMIT 1",(epoch,)).fetchone()
    rows=d.execute("""SELECT enqueue_at,raw_store_at FROM trace
                      WHERE epoch_id=? AND kind='HOT' AND enqueue_at IS NOT NULL
                        AND raw_store_at IS NOT NULL AND raw_store_at>=?
                      ORDER BY raw_store_at DESC""",(epoch,now-RECENT_WINDOW_S)).fetchall()
    d.close()
    lags=[max(0.0,float(r['raw_store_at'])-float(r['enqueue_at'])) for r in rows]
    p50=pct(lags,.50);p90=pct(lags,.90);p95=pct(lags,.95)
    if q:
        pending=int(q['pending_total']);ph=int(q['pending_hot']);fetching=int(q['fetching'])
        oldest=float(q['oldest_pending_age_s']);sample_age=max(0.0,now-float(q['sampled_at']))
        rps=float(q['current_rps'])
    else:
        pending=ph=fetching=0;oldest=0.0;sample_age=9999.0;rps=0.0
    healthy=(len(lags)>=MIN_RECENT_HOT and p90 is not None and p95 is not None
             and p90<=MAX_P90_S and p95<=MAX_P95_S
             and pending<=MAX_PENDING and ph<=MAX_PENDING
             and oldest<MAX_OLDEST_S and sample_age<=MAX_SAMPLE_AGE_S)
    reason='OK' if healthy else 'HEALTH_GATE_FAILED'
    return {'healthy':healthy,'reason':reason,'epoch_id':epoch,'n':len(lags),'p50':p50,'p90':p90,'p95':p95,
            'pending':pending,'pending_hot':ph,'fetching':fetching,'oldest':oldest,'sample_age':sample_age,'rps':rps}


def init_meta():
    d=base.odb();d.execute('''CREATE TABLE IF NOT EXISTS infrastructure_freeze(
      id INTEGER PRIMARY KEY CHECK(id=1),frozen_at REAL,acquisition_epoch TEXT,
      recent_hot_n INTEGER,p50 REAL,p90 REAL,p95 REAL,pending INTEGER,pending_hot INTEGER,
      oldest_pending_s REAL,current_rps REAL,note TEXT)''')
    d.commit();d.close()


def freeze_once_strict():
    d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone();d.close()
    if a:return
    h=live_health()
    if not h.get('healthy'):
        raise RuntimeError(f"V750 not healthy enough to freeze: {h}")
    base.freeze_once()
    d=base.odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone()
    d.execute("UPDATE arena SET method='POST_BURST_V750_SNAPSHOT_FIRST_V1' WHERE arena_id=?",(a['arena_id'],))
    d.execute('INSERT OR REPLACE INTO infrastructure_freeze VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(
        1,time.time(),h['epoch_id'],h['n'],h['p50'],h['p90'],h['p95'],h['pending'],h['pending_hot'],h['oldest'],h['rps'],
        'Frozen only after V750 recent path stable; pause immediately on live health breach or epoch change'))
    d.commit();d.close()


def frozen_infra():
    d=base.odb();z=d.execute('SELECT * FROM infrastructure_freeze WHERE id=1').fetchone();d.close()
    return dict(z) if z else None


def print_pause(f,h,why):
    print('\033[2J\033[H',end='')
    print('='*184)
    print('MEMECOIN LAB — POST-BURST FRESH CAUSAL ARENA V7.5.2')
    print('='*184)
    print(f'PAUSED: {why}')
    print(f"frozen_epoch={f.get('acquisition_epoch')} current_epoch={h.get('epoch_id')} | recent HOT n={h.get('n',0)} p90={h.get('p90')} p95={h.get('p95')} pending={h.get('pending')} oldest={h.get('oldest')}s")
    print('No new evidence is ingested while infrastructure health is outside the frozen regime.')


def display_infra(f,h):
    print(f"INFRA LIVE_HEALTHY | epoch={h['epoch_id']} | recent60s HOT n={h['n']} total p50/p90/p95={h['p50']:.3f}/{h['p90']:.3f}/{h['p95']:.3f}s | pending={h['pending']} oldest={h['oldest']:.2f}s rps={h['rps']:.1f}")
    print(f"INFRA FREEZE | epoch={f['acquisition_epoch']} HOT n={f['recent_hot_n']} p50/p90/p95={f['p50']:.3f}/{f['p90']:.3f}/{f['p95']:.3f}s")
    print('V7.5.2 guardrail: exact rules, fresh post-burst cohort only, pause on live infrastructure degradation.')


def cycle():
    freeze_once_strict()
    f=frozen_infra();h=live_health()
    if not f:raise RuntimeError('infrastructure freeze missing')
    if h.get('epoch_id')!=f['acquisition_epoch']:
        print_pause(f,h,'ACQUISITION_EPOCH_CHANGED');return
    if not h.get('healthy'):
        print_pause(f,h,'LIVE_INFRA_UNHEALTHY');return

    o=base.odb();a=dict(o.execute('SELECT * FROM arena LIMIT 1').fetchone())
    rules=[dict(z) for z in o.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()];o.close()
    for r in rules:
        base.ingest_all_visible_snapshots(a,r)
        base.progress_signal_locked(a,r)
        base.progress_fill_locked(a,r)
        base.audit_invariants(r)
        base.summarize(r)
    base.display(a,rules)
    display_infra(f,h)


if __name__=='__main__':
    signal.signal(signal.SIGINT,base.stop);signal.signal(signal.SIGTERM,base.stop)
    base.init_schema();init_meta()
    print('V7.5.2 boot: requiring live-stable V750 before creating any cutoff...',flush=True)
    while not base.STOP:
        try:cycle()
        except Exception as e:print('V7.5.2 error:',repr(e),flush=True)
        time.sleep(max(.1,base.LOOP))
