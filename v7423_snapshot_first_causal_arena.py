#!/usr/bin/env python3
"""MEMECOIN LAB — SNAPSHOT-FIRST CAUSAL ARENA V7.4.2.3

Fresh prospective re-test of the exact V7.4.1 frozen proposals.

Why this exists
---------------
V7.4.2 reused mutable feature values. V7.4.2.2 fixed that, but its MAX(cutoff_ts)
watermark could silently miss out-of-order snapshot arrivals. This version removes
that watermark entirely and observes *every* post-cutoff snapshot row with
INSERT OR IGNORE semantics.

Causal invariants
-----------------
- brand-new common cutoff on first launch;
- exact V7.4.1 rules, no retuning;
- every post-cutoff snapshot is first-seen exactly once, even if it arrives out of order;
- snapshot rows are recorded even when the strategy feature is NULL;
- NULL at first observation => FEATURE_UNAVAILABLE terminal forever;
- non-NULL feature is locked forever and signal decision is made once;
- snapshot first observed after its fill deadline => LATE_SNAPSHOT terminal;
- SIGNAL_LOCKED can fill only while wall-clock now <= cutoff + fill_window;
- no late/backfilled fill can rescue an expired signal;
- FILL_LOCKED terminalizes exactly once at first post-maturity observation;
- terminal states are never rewritten;
- every transition is append-only journaled;
- integrity audits abort the cycle on invariant violations.

Paper evidence only. No live trading/signing.
"""
from __future__ import annotations
import hashlib, math, os, signal, sqlite3, statistics, time
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59
import v60_economic_edge_discovery_engine as v60
import v63_next_fill_economic_edge_engine as v63

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/'v52_features.db'
DESIGN=ROOT/'v741_shortlist_design.db'
OUT=ROOT/'v7423_snapshot_first_future.db'
LOOP=float(os.environ.get('MEMECOIN_V7423_LOOP_S','1.0'))
CONFIRM=int(os.environ.get('MEMECOIN_V7423_CONFIRM','30'))
STOP=False
TERMINAL={'NO_SIGNAL','NO_FILL','FEATURE_UNAVAILABLE','LATE_SNAPSHOT','SPARSE','ANOMALY','DONE'}


def stop(*_):
    global STOP; STOP=True

def sf(x,d=None):
    try:
        v=float(x);return v if math.isfinite(v) else d
    except:return d

def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def odb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=FULL');d.execute('PRAGMA busy_timeout=30000');return d

def v52():return ro(V52) if V52.exists() else None


def init_schema():
    d=odb();d.executescript('''
    CREATE TABLE IF NOT EXISTS arena(
      arena_id TEXT PRIMARY KEY,created_at REAL,design_id TEXT,common_cutoff REAL,
      cost_pct REAL,fill_window_s REAL,confirm_done INTEGER,method TEXT);
    CREATE TABLE IF NOT EXISTS frozen_rule(
      rule_id TEXT PRIMARY KEY,arena_id TEXT,family TEXT,experiment_id TEXT,feature TEXT,
      stage_s INTEGER,horizon_s INTEGER,tp_pct REAL,sl_pct REAL,direction REAL,threshold REAL,
      source_ho INTEGER,source_exp REAL,source_pf REAL,source_fill REAL);
    CREATE TABLE IF NOT EXISTS events(
      rule_id TEXT,token_mint TEXT,cutoff_ts REAL,
      first_observed_at REAL NOT NULL,feature_available INTEGER NOT NULL,locked_feature REAL,
      signal_decision INTEGER NOT NULL,state TEXT NOT NULL,
      signal_locked_at REAL,fill_price REAL,fill_ts REAL,fill_observed_at REAL,fill_delay_s REAL,
      maturity_ts REAL,path_points INTEGER,raw_return REAL,net_return REAL,hit INTEGER,exit_reason TEXT,mfe REAL,mae REAL,
      terminal_at REAL,updated_at REAL,
      PRIMARY KEY(rule_id,token_mint,cutoff_ts));
    CREATE TABLE IF NOT EXISTS transition_log(
      transition_id INTEGER PRIMARY KEY AUTOINCREMENT,rule_id TEXT,token_mint TEXT,cutoff_ts REAL,
      from_state TEXT,to_state TEXT,observed_at REAL,note TEXT);
    CREATE TABLE IF NOT EXISTS integrity_violation(
      violation_id INTEGER PRIMARY KEY AUTOINCREMENT,created_at REAL,rule_id TEXT,token_mint TEXT,cutoff_ts REAL,
      code TEXT,detail TEXT);
    CREATE TABLE IF NOT EXISTS summary(
      rule_id TEXT PRIMARY KEY,observed INTEGER,feature_available INTEGER,feature_unavailable INTEGER,late_snapshot INTEGER,
      signals INTEGER,no_signal INTEGER,signal_locked INTEGER,fill_locked INTEGER,no_fill INTEGER,sparse INTEGER,anomaly INTEGER,
      done INTEGER,fill_rate REAL,delay_med REAL,expectancy REAL,pf REAL,win_rate REAL,raw_dd REAL,true_dd_050 REAL,status TEXT,updated_at REAL);
    ''');d.commit();d.close()


def max_cutoff():
    x=v52()
    if not x:return 0.0
    z=x.execute('SELECT MAX(cutoff_ts) FROM v52_snapshots').fetchone();x.close();return sf(z[0],0.0) or 0.0


def freeze_once():
    d=odb();a=d.execute('SELECT * FROM arena LIMIT 1').fetchone()
    if a:d.close();return
    if not DESIGN.exists():d.close();raise RuntimeError('Missing v741_shortlist_design.db')
    s=ro(DESIGN);run=s.execute('SELECT * FROM design_run ORDER BY created_at DESC LIMIT 1').fetchone()
    rules=s.execute("SELECT * FROM proposed_freeze WHERE design_id=? AND status='PROPOSED_ONLY' ORDER BY family",(run['design_id'],)).fetchall()
    if len(rules)!=3:s.close();d.close();raise RuntimeError(f'Expected 3 frozen proposals, got {len(rules)}')
    cut=max_cutoff();now=time.time();aid='A7423_'+hashlib.sha256(f"{run['design_id']}|{cut}|snapshot_first_v1".encode()).hexdigest()[:20]
    d.execute('INSERT INTO arena VALUES(?,?,?,?,?,?,?,?)',(aid,now,run['design_id'],cut,float(v59.total_cost_pct()),float(v63.MAX_FILL_DELAY_S),CONFIRM,'SNAPSHOT_FIRST_SCAN_ALL_INSERT_IGNORE_V1'))
    for r in rules:
        rid='R7423_'+hashlib.sha256(f"{aid}|{r['freeze_id']}".encode()).hexdigest()[:20]
        d.execute('INSERT INTO frozen_rule VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(rid,aid,r['family'],r['experiment_id'],r['feature'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['direction'],r['threshold'],r['selected_holdout'],r['holdout_expectancy'],r['holdout_pf'],r['fill_rate']))
    d.commit();d.close();s.close()


def log_transition(o,r,token,cut,frm,to,note=''):
    o.execute('INSERT INTO transition_log(rule_id,token_mint,cutoff_ts,from_state,to_state,observed_at,note) VALUES(?,?,?,?,?,?,?)',(r['rule_id'],token,cut,frm,to,time.time(),note))

def violation(o,r,token,cut,code,detail):
    o.execute('INSERT INTO integrity_violation(created_at,rule_id,token_mint,cutoff_ts,code,detail) VALUES(?,?,?,?,?,?)',(time.time(),r['rule_id'],token,cut,code,detail));o.commit()
    raise RuntimeError(f'INTEGRITY {code} {r["family"]} {token} {cut}: {detail}')


def ingest_all_visible_snapshots(a,r):
    """No timestamp watermark. Scan all post-cutoff source rows every poll; PK dedupes."""
    x=v52()
    if not x:return 0
    rows=x.execute(f'''SELECT token_mint,cutoff_ts,{r['feature']} AS val
        FROM v52_snapshots
        WHERE stage_s=? AND cutoff_ts>?
        ORDER BY cutoff_ts,token_mint''',(int(r['stage_s']),float(a['common_cutoff']))).fetchall()
    o=odb();now=time.time();made=0
    for z in rows:
        token=str(z['token_mint']);cut=float(z['cutoff_ts']);raw=z['val'];val=sf(raw)
        # already first-seen => immutable forever, even if V52 row changed later
        if o.execute('SELECT 1 FROM events WHERE rule_id=? AND token_mint=? AND cutoff_ts=?',(r['rule_id'],token,cut)).fetchone():continue
        deadline=cut+float(a['fill_window_s'])
        if val is None:
            fa=0;sig=0;state='FEATURE_UNAVAILABLE';terminal=now;locked=None;note='feature=NULL at first observation'
        elif now>deadline:
            fa=1;sig=0;state='LATE_SNAPSHOT';terminal=now;locked=val;note=f'first observed after deadline by {now-deadline:.3f}s'
        else:
            fa=1;locked=val;qual=(float(r['direction'])*val>=float(r['threshold']));sig=int(qual)
            state='SIGNAL_LOCKED' if qual else 'NO_SIGNAL';terminal=None if qual else now
            note=f'locked_feature={val:.12g}'
        o.execute('''INSERT OR IGNORE INTO events(rule_id,token_mint,cutoff_ts,first_observed_at,feature_available,locked_feature,
          signal_decision,state,signal_locked_at,terminal_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
          (r['rule_id'],token,cut,now,fa,locked,sig,state,now if state=='SIGNAL_LOCKED' else None,terminal,now))
        if o.execute('SELECT changes()').fetchone()[0]:log_transition(o,r,token,cut,'NEW',state,note);made+=1
    o.commit();o.close();x.close();return made


def progress_signal_locked(a,r):
    o=odb();rows=[dict(z) for z in o.execute("SELECT * FROM events WHERE rule_id=? AND state='SIGNAL_LOCKED' ORDER BY cutoff_ts,token_mint",(r['rule_id'],)).fetchall()]
    if not rows:o.close();return
    x=v52();now=time.time()
    for e in rows:
        token=e['token_mint'];cut=float(e['cutoff_ts']);val=sf(e['locked_feature']);deadline=cut+float(a['fill_window_s'])
        if val is None or float(r['direction'])*val<float(r['threshold']):violation(o,r,token,cut,'SIGNAL_THRESHOLD','locked signal violates threshold')
        if now>deadline:
            o.execute("UPDATE events SET state='NO_FILL',terminal_at=?,updated_at=? WHERE rule_id=? AND token_mint=? AND cutoff_ts=? AND state='SIGNAL_LOCKED'",(now,now,r['rule_id'],token,cut));log_transition(o,r,token,cut,'SIGNAL_LOCKED','NO_FILL','deadline passed before observed fill');continue
        f=x.execute('''SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol>0 ORDER BY timestamp LIMIT 1''',(token,cut,deadline)).fetchone()
        if not f:continue
        ft=float(f['timestamp']);delay=ft-cut
        if delay<0 or delay>float(a['fill_window_s']):violation(o,r,token,cut,'BAD_FILL_DELAY',f'delay={delay}')
        maturity=ft+int(r['horizon_s'])
        o.execute("""UPDATE events SET state='FILL_LOCKED',fill_price=?,fill_ts=?,fill_observed_at=?,fill_delay_s=?,maturity_ts=?,updated_at=?
          WHERE rule_id=? AND token_mint=? AND cutoff_ts=? AND state='SIGNAL_LOCKED'""",(float(f['price_sol']),ft,now,delay,maturity,now,r['rule_id'],token,cut));log_transition(o,r,token,cut,'SIGNAL_LOCKED','FILL_LOCKED',f'fill_ts={ft:.3f}')
    o.commit();o.close();x.close()


def progress_fill_locked(a,r):
    o=odb();rows=[dict(z) for z in o.execute("SELECT * FROM events WHERE rule_id=? AND state='FILL_LOCKED' ORDER BY cutoff_ts,token_mint",(r['rule_id'],)).fetchall()]
    if not rows:o.close();return
    x=v52();now=time.time()
    for e in rows:
        if now<float(e['maturity_ts']):continue
        token=e['token_mint'];cut=float(e['cutoff_ts']);entry=float(e['fill_price']);ft=float(e['fill_ts']);end=float(e['maturity_ts'])
        if int(e['signal_decision'])!=1:violation(o,r,token,cut,'FILL_WITHOUT_SIGNAL','signal_decision != 1')
        rs=x.execute('''SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol>0 ORDER BY timestamp''',(token,ft,end)).fetchall();n=len(rs)
        q={'path_points':n}
        if n<int(v63.MIN_PATH_POINTS):q['state']='SPARSE'
        else:
            prices=[float(z['price_sol']) for z in rs];allp=[entry]+prices
            steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))];rets=[100*(p/entry-1) for p in prices]
            if any(abs(z)>v60.MAX_ABS_STEP_PCT for z in steps) or any(abs(z)>v60.MAX_ABS_PATH_RETURN_PCT for z in rets):q['state']='ANOMALY'
            else:
                raw=rets[-1];reason='TIME_EXIT'
                for z in rets:
                    if z>=float(r['tp_pct']):raw=float(r['tp_pct']);reason='TP_FIRST';break
                    if z<=-float(r['sl_pct']):raw=-float(r['sl_pct']);reason='SL_FIRST';break
                q.update(state='DONE',raw_return=raw,net_return=raw-float(a['cost_pct']),hit=int(reason=='TP_FIRST'),exit_reason=reason,mfe=max(rets),mae=min(rets))
        o.execute('''UPDATE events SET state=?,path_points=?,raw_return=?,net_return=?,hit=?,exit_reason=?,mfe=?,mae=?,terminal_at=?,updated_at=?
          WHERE rule_id=? AND token_mint=? AND cutoff_ts=? AND state='FILL_LOCKED' ''',(q['state'],q.get('path_points'),q.get('raw_return'),q.get('net_return'),q.get('hit'),q.get('exit_reason'),q.get('mfe'),q.get('mae'),now,now,r['rule_id'],token,cut));log_transition(o,r,token,cut,'FILL_LOCKED',q['state'],'terminalized once')
    o.commit();o.close();x.close()


def audit_invariants(r):
    o=odb()
    bad=o.execute('''SELECT COUNT(*) FROM events WHERE rule_id=? AND signal_decision=1 AND (locked_feature IS NULL OR ?*locked_feature<?)''',(r['rule_id'],float(r['direction']),float(r['threshold']))).fetchone()[0]
    if bad:violation(o,r,'*',0,'SIGNAL_THRESHOLD_AUDIT',f'{bad} invalid locked signals')
    bad=o.execute("SELECT COUNT(*) FROM events WHERE rule_id=? AND state IN ('SIGNAL_LOCKED','FILL_LOCKED','DONE','SPARSE','ANOMALY','NO_FILL') AND signal_decision<>1",(r['rule_id'],)).fetchone()[0]
    if bad:violation(o,r,'*',0,'STATE_SIGNAL_AUDIT',f'{bad} post-signal states without signal')
    bad=o.execute("SELECT COUNT(*) FROM events WHERE rule_id=? AND state='FEATURE_UNAVAILABLE' AND feature_available<>0",(r['rule_id'],)).fetchone()[0]
    if bad:violation(o,r,'*',0,'NULL_FEATURE_AUDIT',f'{bad} inconsistent NULL feature states')
    o.close()


def pf(xs):
    g=sum(z for z in xs if z>0);l=-sum(z for z in xs if z<0);return g/l if l>0 else (999.0 if g>0 else 0.0)
def rawdd(xs):
    eq=peak=0.;dd=0.
    for z in xs:eq+=z;peak=max(peak,eq);dd=min(dd,eq-peak)
    return dd
def true_dd(xs,risk=.005):
    eq=peak=1.;dd=0.
    for z in xs:eq*=max(.000001,1+risk*(z/13.));peak=max(peak,eq);dd=min(dd,eq/peak-1)
    return 100*dd


def summarize(r):
    o=odb();rows=[dict(z) for z in o.execute('SELECT * FROM events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(r['rule_id'],)).fetchall()];c={}
    for z in rows:c[z['state']]=c.get(z['state'],0)+1
    done=[z for z in rows if z['state']=='DONE'];xs=[float(z['net_return']) for z in done];signals=sum(int(z['signal_decision']) for z in rows);filled=[z for z in rows if z['fill_ts'] is not None];delays=[float(z['fill_delay_s']) for z in filled if z['fill_delay_s'] is not None]
    n=len(xs);exp=statistics.mean(xs) if xs else None;p=pf(xs) if xs else None;wr=sum(z>0 for z in xs)/n if n else None
    status='WAITING'
    if n>=10:status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
    if n>=CONFIRM:status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
    vals=(r['rule_id'],len(rows),sum(int(z['feature_available']) for z in rows),c.get('FEATURE_UNAVAILABLE',0),c.get('LATE_SNAPSHOT',0),signals,c.get('NO_SIGNAL',0),c.get('SIGNAL_LOCKED',0),c.get('FILL_LOCKED',0),c.get('NO_FILL',0),c.get('SPARSE',0),c.get('ANOMALY',0),n,len(filled)/signals if signals else None,statistics.median(delays) if delays else None,exp,p,wr,rawdd(xs) if xs else None,true_dd(xs) if xs else None,status,time.time())
    o.execute('INSERT OR REPLACE INTO summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals);o.commit();o.close()


def r64():
    try:
        d=core.open_research();z=d.execute('SELECT * FROM v64_forward_summary ORDER BY updated_at DESC LIMIT 1').fetchone();d.close();return dict(z) if z else {}
    except:return {}


def display(a,rules):
    print('\033[2J\033[H',end='');print('='*184);print('MEMECOIN LAB — SNAPSHOT-FIRST CAUSAL ARENA V7.4.2.3');print('='*184)
    print(f"arena={a['arena_id']} common_cutoff>{a['common_cutoff']:.3f} | method={a['method']} | confirm={a['confirm_done']} DONE | poll={LOOP:.1f}s")
    print('No MAX(cutoff) watermark. Every visible post-cutoff snapshot is first-seen once, including NULL features and out-of-order arrivals.\n')
    b=r64();print(f"EXTERNAL R64  DONE={int(b.get('done',0) or 0):4d} exp={sf(b.get('expectancy'),0):+6.2f}% PF={sf(b.get('profit_factor'),0):.2f} fill={100*sf(b.get('fill_rate'),0):5.1f}%\n")
    o=odb()
    for r in rules:
        s=o.execute('SELECT * FROM summary WHERE rule_id=?',(r['rule_id'],)).fetchone();s=dict(s) if s else {}
        print(f"{r['family']:<20} {s.get('status','WAITING'):<14} observed={int(s.get('observed',0) or 0):4d} feat_ok={int(s.get('feature_available',0) or 0):4d} feat_NULL={int(s.get('feature_unavailable',0) or 0):4d} late={int(s.get('late_snapshot',0) or 0):3d}")
        print(f"  signals={int(s.get('signals',0) or 0):3d} DONE={int(s.get('done',0) or 0):3d}/{CONFIRM} no_fill={int(s.get('no_fill',0) or 0):3d} exp={sf(s.get('expectancy'),0):+6.2f}% PF={sf(s.get('pf'),0):.2f} fill={100*sf(s.get('fill_rate'),0):5.1f}% TRUE_DD@0.50={sf(s.get('true_dd_050'),0):+5.2f}%")
        print(f"  {r['feature']} stage={r['stage_s']} h={r['horizon_s']} dir={r['direction']:+g} th={r['threshold']:.10g}\n")
    v=o.execute('SELECT COUNT(*) FROM integrity_violation').fetchone()[0];t=o.execute('SELECT COUNT(*) FROM transition_log').fetchone()[0];o.close()
    print(f'INTEGRITY violations={v} | append-only transitions={t}')
    print('Guardrail: fresh prospective paper evidence only. V7.4.2/.1/.2.2 remain audit artifacts and are not imported.')


def cycle():
    freeze_once();o=odb();a=dict(o.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(z) for z in o.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()];o.close()
    for r in rules:
        ingest_all_visible_snapshots(a,r);progress_signal_locked(a,r);progress_fill_locked(a,r);audit_invariants(r);summarize(r)
    display(a,rules)


if __name__=='__main__':
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init_schema()
    while not STOP:
        try:cycle()
        except Exception as e:print('V7.4.2.3 error:',repr(e),flush=True)
        time.sleep(max(.1,LOOP))
