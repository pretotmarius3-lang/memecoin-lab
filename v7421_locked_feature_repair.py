#!/usr/bin/env python3
"""MEMECOIN LAB — V7.4.2.1 LOCKED FIRST-SEEN FEATURE REPAIR

Corrective integrity layer for V7.4.2.

Root issue in V7.4.2: event.feature_value was inserted once, but subsequent cycles
re-classified the same (rule_id, token_mint, cutoff_ts) using the CURRENT mutable
v52_snapshots feature value. ON CONFLICT updated state/outcome but not feature_value.
That can produce DONE/NO_FILL/SPARSE rows whose stored first-seen feature never
qualified the frozen threshold.

This repair NEVER changes the frozen arena/rules/cutoff. It writes a NEW DB and
rebuilds every existing V7.4.2 event using the event's stored first-seen feature
as the immutable signal decision. Future processing must also keep that feature
locked for each event.

Original v742_common_future.db remains untouched for audit.
"""
from __future__ import annotations
import json, math, sqlite3, statistics, time
from pathlib import Path
import v60_economic_edge_discovery_engine as v60
import v63_next_fill_economic_edge_engine as v63

ROOT=Path.home()/"memecoin_lab"
SRC=ROOT/'v742_common_future.db'
V52=ROOT/'v52_features.db'
OUT=ROOT/'v7421_locked_feature_future.db'

def sf(x,d=None):
    try:
        v=float(x);return v if math.isfinite(v) else d
    except:return d

def ro(p):
    d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA query_only=ON');d.execute('PRAGMA busy_timeout=30000');return d

def outdb():
    d=sqlite3.connect(OUT,timeout=30);d.row_factory=sqlite3.Row;d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA busy_timeout=30000');return d

def init():
    if not SRC.exists():raise SystemExit('Missing v742_common_future.db')
    d=outdb();d.executescript('''
    CREATE TABLE IF NOT EXISTS arena(arena_id TEXT PRIMARY KEY,created_at REAL,design_id TEXT,common_cutoff REAL,excluded_tokens_json TEXT,cost_pct REAL,fill_window_s REAL,confirm_done INTEGER,repair_note TEXT);
    CREATE TABLE IF NOT EXISTS frozen_rule(rule_id TEXT PRIMARY KEY,arena_id TEXT,family TEXT,experiment_id TEXT,feature TEXT,stage_s INTEGER,horizon_s INTEGER,tp_pct REAL,sl_pct REAL,direction REAL,threshold REAL,source_ho INTEGER,source_exp REAL,source_pf REAL,source_fill REAL);
    CREATE TABLE IF NOT EXISTS events(rule_id TEXT,token_mint TEXT,cutoff_ts REAL,feature_value REAL,state TEXT,fill_price REAL,fill_ts REAL,fill_delay_s REAL,path_points INTEGER,raw_return REAL,net_return REAL,hit INTEGER,exit_reason TEXT,mfe REAL,mae REAL,updated_at REAL,PRIMARY KEY(rule_id,token_mint,cutoff_ts));
    CREATE TABLE IF NOT EXISTS summary(rule_id TEXT PRIMARY KEY,eligible INTEGER,signals INTEGER,no_signal INTEGER,wait_fill INTEGER,no_fill INTEGER,wait_maturity INTEGER,sparse INTEGER,anomaly INTEGER,done INTEGER,fill_rate REAL,delay_med REAL,expectancy REAL,pf REAL,win_rate REAL,raw_dd REAL,true_dd_050 REAL,status TEXT,updated_at REAL);
    CREATE TABLE IF NOT EXISTS repair_audit(rule_id TEXT PRIMARY KEY,family TEXT,total_old INTEGER,invalid_old_non_signal INTEGER,old_done INTEGER,new_done INTEGER,removed_done INTEGER,created_at REAL);
    ''');d.commit();d.close()

def classify(x,r,a,token,cutoff,val,now):
    # FIRST-SEEN FEATURE IS THE IMMUTABLE SIGNAL DECISION.
    if float(r['direction'])*float(val)<float(r['threshold']):return {'state':'NO_SIGNAL'}
    f=x.execute('SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND price_sol>0 ORDER BY timestamp LIMIT 1',(token,cutoff)).fetchone()
    deadline=cutoff+float(a['fill_window_s'])
    if not f:return {'state':'WAIT_FILL' if now<=deadline else 'NO_FILL'}
    ft=float(f['timestamp']);delay=ft-cutoff
    if delay<0 or delay>float(a['fill_window_s']):return {'state':'NO_FILL','fill_delay_s':delay}
    entry=float(f['price_sol']);end=ft+int(r['horizon_s'])
    if now<end:return {'state':'WAIT_MATURITY','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay}
    rows=x.execute('SELECT price_sol,timestamp FROM v52_swaps WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol>0 ORDER BY timestamp',(token,ft,end)).fetchall();n=len(rows)
    if n<int(v63.MIN_PATH_POINTS):return {'state':'SPARSE','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay,'path_points':n}
    prices=[float(z['price_sol']) for z in rows];allp=[entry]+prices
    steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))];rets=[100*(p/entry-1) for p in prices]
    if any(abs(z)>v60.MAX_ABS_STEP_PCT for z in steps) or any(abs(z)>v60.MAX_ABS_PATH_RETURN_PCT for z in rets):return {'state':'ANOMALY','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay,'path_points':n}
    raw=rets[-1];reason='TIME_EXIT'
    for z in rets:
        if z>=float(r['tp_pct']):raw=float(r['tp_pct']);reason='TP_FIRST';break
        if z<=-float(r['sl_pct']):raw=-float(r['sl_pct']);reason='SL_FIRST';break
    return {'state':'DONE','fill_price':entry,'fill_ts':ft,'fill_delay_s':delay,'path_points':n,'raw_return':raw,'net_return':raw-float(a['cost_pct']),'hit':int(reason=='TP_FIRST'),'exit_reason':reason,'mfe':max(rets),'mae':min(rets)}
def pf(xs):
    g=sum(z for z in xs if z>0);l=-sum(z for z in xs if z<0);return g/l if l>0 else (999 if g>0 else 0)
def rawdd(xs):
    eq=peak=0.;dd=0.
    for z in xs:eq+=z;peak=max(peak,eq);dd=min(dd,eq-peak)
    return dd
def true_dd(xs,risk=.005):
    eq=peak=1.;dd=0.
    for z in xs:
        eq*=max(0.000001,1+risk*(z/13.));peak=max(peak,eq);dd=min(dd,eq/peak-1)
    return 100*dd

def summarize(o,r,confirm):
    rows=[dict(z) for z in o.execute('SELECT * FROM events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(r['rule_id'],)).fetchall()];c={}
    for z in rows:c[z['state']]=c.get(z['state'],0)+1
    done=[z for z in rows if z['state']=='DONE'];xs=[float(z['net_return']) for z in done];signals=len(rows)-c.get('NO_SIGNAL',0);filled=[z for z in rows if z['fill_ts'] is not None and z['state']!='NO_SIGNAL'];delays=[float(z['fill_delay_s']) for z in filled if z['fill_delay_s'] is not None]
    n=len(xs);exp=statistics.mean(xs) if xs else None;p=pf(xs) if xs else None;wr=sum(z>0 for z in xs)/n if n else None;status='WAITING'
    if n>=10:status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
    if n>=confirm:status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
    o.execute('INSERT OR REPLACE INTO summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(r['rule_id'],len(rows),signals,c.get('NO_SIGNAL',0),c.get('WAIT_FILL',0),c.get('NO_FILL',0),c.get('WAIT_MATURITY',0),c.get('SPARSE',0),c.get('ANOMALY',0),n,len(filled)/signals if signals else None,statistics.median(delays) if delays else None,exp,p,wr,rawdd(xs) if xs else None,true_dd(xs) if xs else None,status,time.time()))

def main():
    init();s=ro(SRC);x=ro(V52);o=outdb();a=dict(s.execute('SELECT * FROM arena LIMIT 1').fetchone());rules=[dict(z) for z in s.execute('SELECT * FROM frozen_rule ORDER BY family').fetchall()]
    o.execute('DELETE FROM arena');o.execute('DELETE FROM frozen_rule');o.execute('DELETE FROM events');o.execute('DELETE FROM summary');o.execute('DELETE FROM repair_audit')
    o.execute('INSERT INTO arena VALUES(?,?,?,?,?,?,?,?,?)',(a['arena_id'],a['created_at'],a['design_id'],a['common_cutoff'],a['excluded_tokens_json'],a['cost_pct'],a['fill_window_s'],a['confirm_done'],'V7.4.2.1 locked first-seen feature corrective reconstruction'))
    for r in rules:o.execute('INSERT INTO frozen_rule VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',tuple(r[k] for k in ['rule_id','arena_id','family','experiment_id','feature','stage_s','horizon_s','tp_pct','sl_pct','direction','threshold','source_ho','source_exp','source_pf','source_fill']))
    now=time.time()
    for r in rules:
        old=[dict(z) for z in s.execute('SELECT * FROM events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(r['rule_id'],)).fetchall()]
        old_done=sum(z['state']=='DONE' for z in old);invalid=sum(z['state']!='NO_SIGNAL' and float(r['direction'])*float(z['feature_value'])<float(r['threshold']) for z in old)
        for z in old:
            q=classify(x,r,a,str(z['token_mint']),float(z['cutoff_ts']),float(z['feature_value']),now)
            o.execute('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(r['rule_id'],z['token_mint'],z['cutoff_ts'],z['feature_value'],q['state'],q.get('fill_price'),q.get('fill_ts'),q.get('fill_delay_s'),q.get('path_points'),q.get('raw_return'),q.get('net_return'),q.get('hit'),q.get('exit_reason'),q.get('mfe'),q.get('mae'),now))
        summarize(o,r,int(a['confirm_done']));new_done=o.execute('SELECT done FROM summary WHERE rule_id=?',(r['rule_id'],)).fetchone()[0]
        o.execute('INSERT INTO repair_audit VALUES(?,?,?,?,?,?,?)',(r['rule_id'],r['family'],len(old),invalid,old_done,new_done,old_done-new_done,time.time()))
    o.commit()
    print('='*150);print('MEMECOIN LAB — V7.4.2.1 LOCKED FIRST-SEEN FEATURE REPAIR');print('='*150)
    print(f"arena={a['arena_id']} common_cutoff>{a['common_cutoff']:.3f} | ORIGINAL DB UNTOUCHED | repaired={OUT.name}\n")
    for r in rules:
        au=dict(o.execute('SELECT * FROM repair_audit WHERE rule_id=?',(r['rule_id'],)).fetchone());sm=dict(o.execute('SELECT * FROM summary WHERE rule_id=?',(r['rule_id'],)).fetchone())
        print(f"{r['family']:<20} invalid_old={au['invalid_old_non_signal']:3d} old_DONE={au['old_done']:3d} repaired_DONE={au['new_done']:3d} removed_DONE={au['removed_done']:3d} | status={sm['status']:<14} exp={sf(sm['expectancy'],0):+6.2f}% PF={sf(sm['pf'],0):.2f} TRUE_DD@0.50={sf(sm['true_dd_050'],0):+5.2f}%")
    print('\nGuardrail: corrective reconstruction only. Frozen rules/cutoff unchanged; original V7.4.2 DB preserved for audit.')
    s.close();x.close();o.close()
if __name__=='__main__':main()
