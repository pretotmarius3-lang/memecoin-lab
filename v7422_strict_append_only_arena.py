#!/usr/bin/env python3
"""MEMECOIN LAB — STRICT APPEND-ONLY COMMON FUTURE ARENA V7.4.2.2

Fresh causal re-test of the exact V7.4.1 proposals.

This arena intentionally does NOT repair or reuse V7.4.2 outcomes. It starts a
brand-new common cutoff and locks each event as it is first observed by this
process.

Causal invariants:
- exact V7.4.1 feature/stage/horizon/TP/SL/direction/threshold are immutable;
- one new common cutoff is frozen on first launch;
- a snapshot's feature value is locked on FIRST OBSERVATION and never updated;
- signal decision is made exactly once from that locked value;
- NO_SIGNAL is terminal and can never later become a signal;
- SIGNAL_LOCKED may become FILL_LOCKED only while wall-clock now <= fill deadline;
- once the fill deadline passes without an observed fill, NO_FILL is terminal;
- FILL_LOCKED may become DONE/SPARSE/ANOMALY exactly once after maturity;
- terminal states are never rewritten;
- every state transition is journaled append-only;
- integrity assertions abort the cycle on threshold/state violations.

Important limitation: this is strict relative to what this process observes in
V52 while it is running. A swap that appears in V52 only after its deadline is
never accepted retroactively even if its exchange timestamp is earlier.

Paper evidence only. No live trading/signing.
"""
from __future__ import annotations
import hashlib, json, math, os, signal, sqlite3, statistics, time
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59
import v60_economic_edge_discovery_engine as v60
import v63_next_fill_economic_edge_engine as v63

ROOT=Path.home()/"memecoin_lab"
V52=ROOT/'v52_features.db'
DESIGN=ROOT/'v741_shortlist_design.db'
OUT=ROOT/'v7422_append_only_future.db'
LOOP=float(os.environ.get('MEMECOIN_V7422_LOOP_S','1.0'))
CONFIRM=int(os.environ.get('MEMECOIN_V7422_CONFIRM','30'))
TERMINAL={'NO_SIGNAL','NO_FILL','SPARSE','ANOMALY','DONE'}
STOP=False


def stop(*_):
    global STOP; STOP=True

def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def ro(path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30); d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON'); d.execute('PRAGMA busy_timeout=30000'); return d

def odb():
    d=sqlite3.connect(OUT,timeout=30); d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL'); d.execute('PRAGMA synchronous=FULL'); d.execute('PRAGMA busy_timeout=30000'); return d

def v52(): return ro(V52) if V52.exists() else None

def init_schema():
    d=odb(); d.executescript('''
    CREATE TABLE IF NOT EXISTS arena(
      arena_id TEXT PRIMARY KEY,created_at REAL,design_id TEXT,common_cutoff REAL,
      cost_pct REAL,fill_window_s REAL,confirm_done INTEGER,method TEXT);
    CREATE TABLE IF NOT EXISTS frozen_rule(
      rule_id TEXT PRIMARY KEY,arena_id TEXT,family TEXT,experiment_id TEXT,feature TEXT,
      stage_s INTEGER,horizon_s INTEGER,tp_pct REAL,sl_pct REAL,direction REAL,threshold REAL,
      source_ho INTEGER,source_exp REAL,source_pf REAL,source_fill REAL);
    CREATE TABLE IF NOT EXISTS events(
      rule_id TEXT,token_mint TEXT,cutoff_ts REAL,
      locked_feature REAL NOT NULL,first_observed_at REAL NOT NULL,
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
      rule_id TEXT PRIMARY KEY,eligible INTEGER,signals INTEGER,no_signal INTEGER,signal_locked INTEGER,fill_locked INTEGER,
      no_fill INTEGER,sparse INTEGER,anomaly INTEGER,done INTEGER,fill_rate REAL,delay_med REAL,
      expectancy REAL,pf REAL,win_rate REAL,raw_dd REAL,true_dd_050 REAL,status TEXT,updated_at REAL);
    '''); d.commit(); d.close()

def max_cutoff():
    x=v52()
    if not x:return 0.0
    r=x.execute('SELECT MAX(cutoff_ts) FROM v52_snapshots').fetchone(); x.close(); return sf(r[0],0.0) or 0.0

def freeze_once():
    d=odb(); a=d.execute('SELECT * FROM arena LIMIT 1').fetchone()
    if a:d.close();return
    if not DESIGN.exists():d.close();raise RuntimeError('Missing v741_shortlist_design.db')
    s=ro(DESIGN); run=s.execute('SELECT * FROM design_run ORDER BY created_at DESC LIMIT 1').fetchone()
    rules=s.execute("SELECT * FROM proposed_freeze WHERE design_id=? AND status='PROPOSED_ONLY' ORDER BY family",(run['design_id'],)).fetchall()
    if len(rules)!=3:s.close();d.close();raise RuntimeError(f'Expected 3 proposed freezes, got {len(rules)}')
    cut=max_cutoff(); now=time.time(); aid='A7422_'+hashlib.sha256(f"{run['design_id']}|{cut}|append_only_v1".encode()).hexdigest()[:20]
    d.execute('INSERT INTO arena VALUES(?,?,?,?,?,?,?,?)',(aid,now,run['design_id'],cut,float(v59.total_cost_pct()),float(v63.MAX_FILL_DELAY_S),CONFIRM,'STRICT_FIRST_OBSERVED_APPEND_ONLY_V1'))
    for r in rules:
        rid='R7422_'+hashlib.sha256(f"{aid}|{r['freeze_id']}".encode()).hexdigest()[:20]
        d.execute('INSERT INTO frozen_rule VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(rid,aid,r['family'],r['experiment_id'],r['feature'],r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['direction'],r['threshold'],r['selected_holdout'],r['holdout_expectancy'],r['holdout_pf'],r['fill_rate']))
    d.commit();d.close();s.close()

def violation(o,r,token,cutoff,code,detail):
    o.execute('INSERT INTO integrity_violation(created_at,rule_id,token_mint,cutoff_ts,code,detail) VALUES(?,?,?,?,?,?)',(time.time(),r['rule_id'],token,cutoff,code,detail));o.commit()
    raise RuntimeError(f'INTEGRITY {code} {r["family"]} {token} {cutoff}: {detail}')

def log_transition(o,r,token,cutoff,frm,to,note=''):
    o.execute('INSERT INTO transition_log(rule_id,token_mint,cutoff_ts,from_state,to_state,observed_at,note) VALUES(?,?,?,?,?,?,?)',(r['rule_id'],token,cutoff,frm,to,time.time(),note))

def ingest_new_snapshots(a,r):
    x=v52()
    if not x:return 0
    o=odb(); last=o.execute('SELECT COALESCE(MAX(cutoff_ts),?) FROM events WHERE rule_id=?',(a['common_cutoff'],r['rule_id'])).fetchone()[0]
    rows=x.execute(f'''SELECT token_mint,cutoff_ts,{r['feature']} AS val FROM v52_snapshots
      WHERE stage_s=? AND cutoff_ts>? AND {r['feature']} IS NOT NULL ORDER BY cutoff_ts,token_mint''',(int(r['stage_s']),float(last))).fetchall()
    made=0; now=time.time()
    for z in rows:
        token=str(z['token_mint']);cut=float(z['cutoff_ts']);val=sf(z['val'])
        if cut<=float(a['common_cutoff']) or val is None:continue
        qualifies=(float(r['direction'])*val>=float(r['threshold']))
        state='SIGNAL_LOCKED' if qualifies else 'NO_SIGNAL';terminal=now if state=='NO_SIGNAL' else None
        o.execute('''INSERT OR IGNORE INTO events(rule_id,token_mint,cutoff_ts,locked_feature,first_observed_at,signal_decision,state,signal_locked_at,terminal_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?)''',(r['rule_id'],token,cut,val,now,int(qualifies),state,now if qualifies else None,terminal,now))
        if o.execute('SELECT changes()').fetchone()[0]:
            log_transition(o,r,token,cut,'NEW',state,f'locked_feature={val:.12g}');made+=1
    o.commit();o.close();x.close();return made

def progress_signal_locked(a,r):
    o=odb();rows=[dict(z) for z in o.execute("SELECT * FROM events WHERE rule_id=? AND state='SIGNAL_LOCKED' ORDER BY cutoff_ts,token_mint",(r['rule_id'],)).fetchall()]
    if not rows:o.close();return
    x=v52();now=time.time()
    for e in rows:
        token=e['token_mint'];cut=float(e['cutoff_ts']);val=float(e['locked_feature']);deadline=cut+float(a['fill_window_s'])
        if float(r['direction'])*val<float(r['threshold']):violation(o,r,token,cut,'SIGNAL_BELOW_THRESHOLD',f'locked={val} threshold={r["threshold"]} dir={r["direction"]}')
        # Strict wall-clock deadline: after it passes, no later/backfilled swap can rescue the event.
        if now>deadline:
            o.execute("UPDATE events SET state='NO_FILL',terminal_at=?,updated_at=? WHERE rule_id=? AND token_mint=? AND cutoff_ts=? AND state='SIGNAL_LOCKED'",(now,now,r['rule_id'],token,cut));log_transition(o,r,token,cut,'SIGNAL_LOCKED','NO_FILL','deadline passed before observed fill');continue
        f=x.execute('''SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp LIMIT 1''',(token,cut,deadline)).fetchone()
        if not f:continue
        ft=float(f['timestamp']);delay=ft-cut
        if not (0<=delay<=float(a['fill_window_s'])):violation(o,r,token,cut,'BAD_FILL_DELAY',f'delay={delay}')
        maturity=ft+int(r['horizon_s'])
        o.execute("""UPDATE events SET state='FILL_LOCKED',fill_price=?,fill_ts=?,fill_observed_at=?,fill_delay_s=?,maturity_ts=?,updated_at=?
          WHERE rule_id=? AND token_mint=? AND cutoff_ts=? AND state='SIGNAL_LOCKED'""",(float(f['price_sol']),ft,now,delay,maturity,now,r['rule_id'],token,cut));log_transition(o,r,token,cut,'SIGNAL_LOCKED','FILL_LOCKED',f'fill_ts={ft:.3f} observed_at={now:.3f}')
    o.commit();o.close();x.close()

def progress_fill_locked(a,r):
    o=odb();rows=[dict(z) for z in o.execute("SELECT * FROM events WHERE rule_id=? AND state='FILL_LOCKED' ORDER BY cutoff_ts,token_mint",(r['rule_id'],)).fetchall()]
    if not rows:o.close();return
    x=v52();now=time.time()
    for e in rows:
        if now<float(e['maturity_ts']):continue
        token=e['token_mint'];cut=float(e['cutoff_ts']);entry=float(e['fill_price']);ft=float(e['fill_ts']);end=float(e['maturity_ts'])
        if int(e['signal_decision'])!=1 or float(r['direction'])*float(e['locked_feature'])<float(r['threshold']):violation(o,r,token,cut,'FILL_WITH_INVALID_SIGNAL','locked signal invariant broken')
        rs=x.execute('''SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp''',(token,ft,end)).fetchall();n=len(rs)
        q={'path_points':n}
        if n<int(v63.MIN_PATH_POINTS):q.update(state='SPARSE')
        else:
            prices=[float(z['price_sol']) for z in rs];allp=[entry]+prices
            steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))];rets=[100*(p/entry-1) for p in prices]
            if any(abs(z)>v60.MAX_ABS_STEP_PCT for z in steps) or any(abs(z)>v60.MAX_ABS_PATH_RETURN_PCT for z in rets):q.update(state='ANOMALY')
            else:
                raw=rets[-1];reason='TIME_EXIT'
                for z in rets:
                    if z>=float(r['tp_pct']):raw=float(r['tp_pct']);reason='TP_FIRST';break
                    if z<=-float(r['sl_pct']):raw=-float(r['sl_pct']);reason='SL_FIRST';break
                q.update(state='DONE',raw_return=raw,net_return=raw-float(a['cost_pct']),hit=int(reason=='TP_FIRST'),exit_reason=reason,mfe=max(rets),mae=min(rets))
        o.execute('''UPDATE events SET state=?,path_points=?,raw_return=?,net_return=?,hit=?,exit_reason=?,mfe=?,mae=?,terminal_at=?,updated_at=?
          WHERE rule_id=? AND token_mint=? AND cutoff_ts=? AND state='FILL_LOCKED' ''',(q['state'],q.get('path_points'),q.get('raw_return'),q.get('net_return'),q.get('hit'),q.get('exit_reason'),q.get('mfe'),q.get('mae'),now,now,r['rule_id'],token,cut));log_transition(o,r,token,cut,'FILL_LOCKED',q['state'],'terminalized once at first post-maturity observation')
    o.commit();o.close();x.close()

def audit_invariants(r):
    o=odb()
    bad=o.execute('''SELECT COUNT(*) FROM events WHERE rule_id=? AND signal_decision=1 AND ?*locked_feature<?''',(r['rule_id'],float(r['direction']),float(r['threshold']))).fetchone()[0]
    if bad:violation(o,r,'*',0,'SIGNAL_THRESHOLD_AUDIT',f'{bad} locked signals violate threshold')
    bad2=o.execute("SELECT COUNT(*) FROM events WHERE rule_id=? AND state IN ('FILL_LOCKED','DONE','SPARSE','ANOMALY','NO_FILL') AND signal_decision<>1",(r['rule_id'],)).fetchone()[0]
    if bad2:violation(o,r,'*',0,'STATE_SIGNAL_AUDIT',f'{bad2} post-signal states without signal_decision=1')
    o.close()
def pf(xs):
    g=sum(z for z in xs if z>0);l=-sum(z for z in xs if z<0);return g/l if l>0 else (999.0 if g>0 else 0.0)
def rawdd(xs):
    eq=peak=0.;dd=0.
    for z in xs:eq+=z;peak=max(peak,eq);dd=min(dd,eq-peak)
    return dd
def true_dd(xs,risk=.005):
    eq=peak=1.;dd=0.
    for z in xs:eq*=max(0.000001,1+risk*(z/13.));peak=max(peak,eq);dd=min(dd,eq/peak-1)
    return 100*dd

def summarize(r):
    o=odb();rows=[dict(z) for z in o.execute('SELECT * FROM events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(r['rule_id'],)).fetchall()];c={}
    for z in rows:c[z['state']]=c.get(z['state'],0)+1
    done=[z for z in rows if z['state']=='DONE'];xs=[float(z['net_return']) for z in done];signals=sum(int(z['signal_decision']) for z in rows);filled=[z for z in rows if z['fill_ts'] is not None];delays=[float(z['fill_delay_s']) for z in filled if z['fill_delay_s'] is not None];n=len(xs);exp=statistics.mean(xs) if xs else None;p=pf(xs) if xs else None;wr=sum(z>0 for z in xs)/n if n else None
    status='WAITING'
    if n>=10:status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
    if n>=CONFIRM:status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
    vals=(r['rule_id'],len(rows),signals,c.get('NO_SIGNAL',0),c.get('SIGNAL_LOCKED',0),c.get('FILL_LOCKED',0),c.get('NO_FILL',0),c.get('SPARSE',0),c.get('ANOMALY',0),n,len(filled)/signals if signals else None,statistics.median(delays) if delays else None,exp,p,wr,rawdd(xs) if xs else None,true_dd(xs) if xs else None,status,time.time())
    o.execute('INSERT OR REPLACE INTO summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals);o.commit();o.close()
def r64():
    try:
        d=core.open_research();z=d.execute('SELECT * FROM v64_forward_summary ORDER BY updated_at DESC LIMIT 1').fetchone();d.close();return dict(z) if z else {}
    except:return {}
def display(a,rules):
    print('\033[2J\033[H',end='');print('='*180);print('MEMECOIN LAB — STRICT APPEND-ONLY COMMON FUTURE ARENA V7.4.2.2');print('='*180)
    print(f"arena={a['arena_id']} common_cutoff>{a['common_cutoff']:.3f} | method={a['method']} | confirm={a['confirm_done']} DONE | poll={LOOP:.1f}s")
    print('Locked feature + one-way state machine. Terminal states never rewritten. Late/backfilled fills cannot rescue expired signals.\n')
    b=r64();print(f"EXTERNAL R64  DONE={int(b.get('done',0) or 0):4d} exp={sf(b.get('expectancy'),0):+6.2f}% PF={sf(b.get('profit_factor'),0):.2f} fill={100*sf(b.get('fill_rate'),0):5.1f}%")
    o=odb()
    for r in rules:
        s=o.execute('SELECT * FROM summary WHERE rule_id=?',(r['rule_id'],)).fetchone();s=dict(s) if s else {}
        print(f"{r['family']:<20} {s.get('status','WAITING'):<14} DONE={int(s.get('done',0) or 0):3d}/{CONFIRM} signals={int(s.get('signals',0) or 0):3d} locked={int(s.get('signal_locked',0) or 0):2d} fill_locked={int(s.get('fill_locked',0) or 0):2d} no_fill={int(s.get('no_fill',0) or 0):3d}")
        print(f"  exp={sf(s.get('expectancy'),0):+6.2f}% PF={sf(s.get('pf'),0):.2f} fill={100*sf(s.get('fill_rate'),0):5.1f}% TRUE_DD@0.50={sf(s.get('true_dd_050'),0):+5.2f}% | {r['feature']} stage={r['stage_s']} h={r['horizon_s']} dir={r['direction']:+g} th={r['threshold']:.10g}")
    vio=o.execute('SELECT COUNT(*) FROM integrity_violation').fetchone()[0];trans=o.execute('SELECT COUNT(*) FROM transition_log').fetchone()[0];o.close()
    print(f"\nINTEGRITY violations={vio} | append-only transitions={trans}")
    print('Guardrail: fresh prospective paper evidence only. Original V7.4.2/repair DBs remain audit artifacts, not evidence for this arena.')
def cycle():
    freeze_once();o=odb();a=dict(o.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(z) for z in o.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()];o.close()
    for r in rules:
        ingest_new_snapshots(a,r);progress_signal_locked(a,r);progress_fill_locked(a,r);audit_invariants(r);summarize(r)
    display(a,rules)

if __name__=='__main__':
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init_schema()
    while not STOP:
        try:cycle()
        except Exception as e:print('V7.4.2.2 ERROR:',repr(e),flush=True)
        end=time.time()+LOOP
        while not STOP and time.time()<end:time.sleep(min(.2,max(0,end-time.time())))
