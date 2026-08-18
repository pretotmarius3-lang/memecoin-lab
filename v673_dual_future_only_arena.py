#!/usr/bin/env python3
"""Memecoin Lab V6.7.3 — dual strict future-only challenger arena.

Runs exactly two immutable challengers selected before this arena:
1) WALLET_STRUCTURE from V6.7.2
2) FLOW_DYNAMICS_CORRECTED from V6.7.2.1

For each challenger:
- only snapshots strictly after its own frozen cutoff are eligible;
- exact feature/stage/horizon/TP/SL/direction/threshold are immutable;
- source research tokens are excluded;
- next observed post-signal price is fill, <= frozen fill window;
- TP/SL/horizon start at actual fill;
- V6.4/R64 and all V6.7 research tables are untouched.

Paper/research only. No live trading or signing.
"""
from __future__ import annotations

import json, math, os, signal, statistics, time
from pathlib import Path
import sqlite3

import v41_core as core
import v64_next_fill_future_only_arena as v64

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
LOOP=float(os.environ.get('MEMECOIN_V673_LOOP_S','10'))
MIN_SURVIVE=int(os.environ.get('MEMECOIN_V673_MIN_SURVIVE_TRADES','10'))
MIN_CONFIRM=int(os.environ.get('MEMECOIN_V673_MIN_CONFIRM_TRADES','30'))
STOP=False

def stop(*_):
    global STOP; STOP=True

def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def open_v52():
    if not V52.exists():return None
    d=sqlite3.connect(f'file:{V52}?mode=ro',uri=True,timeout=30); d.row_factory=sqlite3.Row; d.execute('PRAGMA busy_timeout=30000'); return d

def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v673_forward_events(
      challenger_id TEXT NOT NULL,label TEXT NOT NULL,token_mint TEXT NOT NULL,cutoff_ts REAL NOT NULL,
      feature_value REAL NOT NULL,state TEXT NOT NULL,
      fill_price REAL,fill_ts REAL,fill_delay_s REAL,path_points INTEGER,
      raw_return REAL,net_return REAL,hit INTEGER,exit_reason TEXT,mfe REAL,mae REAL,
      first_seen_at REAL NOT NULL,updated_at REAL NOT NULL,
      PRIMARY KEY(challenger_id,token_mint,cutoff_ts));
    CREATE TABLE IF NOT EXISTS v673_forward_summary(
      challenger_id TEXT PRIMARY KEY,label TEXT NOT NULL,
      eligible INTEGER NOT NULL,signals INTEGER NOT NULL,no_signal INTEGER NOT NULL,
      waiting_fill INTEGER NOT NULL,no_fill INTEGER NOT NULL,waiting_maturity INTEGER NOT NULL,
      sparse_path INTEGER NOT NULL,anomaly INTEGER NOT NULL,done INTEGER NOT NULL,
      fill_rate REAL,median_fill_delay REAL,expectancy REAL,median_net REAL,win_rate REAL,
      profit_factor REAL,hit_rate REAL,max_drawdown REAL,status TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()

def load_rules():
    d=core.open_research()
    w=d.execute("SELECT * FROM v672_frozen_challengers WHERE family='WALLET_STRUCTURE' LIMIT 1").fetchone()
    f=d.execute("SELECT * FROM v6721_corrected_freezes WHERE label='FLOW_DYNAMICS_CORRECTED' LIMIT 1").fetchone()
    d.close()
    if not w or not f:raise SystemExit('Missing required V6.7.2/V6.7.2.1 freezes')
    out=[]
    for row,label in ((w,'WALLET_STRUCTURE'),(f,'FLOW_DYNAMICS_CORRECTED')):
        r=dict(row);r['label']=label;out.append(r)
    return out

def classify(db,rule,token,cutoff,x,now):
    # Reuse the already-audited V6.4 state machine with the same field contract.
    return v64.classify(db,rule,token,cutoff,x,now)

def upsert(rule,token,cutoff,x,res):
    now=time.time();d=core.open_research()
    d.execute('''INSERT INTO v673_forward_events(
      challenger_id,label,token_mint,cutoff_ts,feature_value,state,fill_price,fill_ts,fill_delay_s,path_points,
      raw_return,net_return,hit,exit_reason,mfe,mae,first_seen_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(challenger_id,token_mint,cutoff_ts) DO UPDATE SET
      state=excluded.state,fill_price=excluded.fill_price,fill_ts=excluded.fill_ts,fill_delay_s=excluded.fill_delay_s,
      path_points=excluded.path_points,raw_return=excluded.raw_return,net_return=excluded.net_return,hit=excluded.hit,
      exit_reason=excluded.exit_reason,mfe=excluded.mfe,mae=excluded.mae,updated_at=excluded.updated_at''',(
      rule['challenger_id'],rule['label'],token,float(cutoff),float(x),res['state'],res.get('fill_price'),res.get('fill_ts'),
      res.get('fill_delay_s'),res.get('path_points'),res.get('raw_return'),res.get('net_return'),res.get('hit'),
      res.get('exit_reason'),res.get('mfe'),res.get('mae'),now,now));d.commit();d.close()

def process(rule):
    db=open_v52()
    if db is None:return 0
    excluded=set(json.loads(rule['excluded_tokens_json'])); feature=str(rule['feature'])
    rows=db.execute(f'''SELECT token_mint,cutoff_ts,{feature} AS feature FROM v52_snapshots
      WHERE stage_s=? AND cutoff_ts>? AND {feature} IS NOT NULL ORDER BY cutoff_ts,token_mint''',
      (int(rule['stage_s']),float(rule['frozen_max_cutoff_ts']))).fetchall()
    now=time.time();made=0
    for r in rows:
        token=str(r['token_mint'])
        if token in excluded:continue
        x=sf(r['feature'])
        if x is None:continue
        upsert(rule,token,float(r['cutoff_ts']),x,classify(db,rule,token,float(r['cutoff_ts']),x,now));made+=1
    db.close();return made

def pf(xs):
    g=sum(x for x in xs if x>0);l=-sum(x for x in xs if x<0)
    return g/l if l>0 else (999.0 if g>0 else 0.0)

def max_dd(xs):
    eq=peak=0.0;worst=0.0
    for x in xs:
        eq+=x;peak=max(peak,eq);worst=min(worst,eq-peak)
    return worst

def summarize(rule):
    d=core.open_research();rows=[dict(r) for r in d.execute('SELECT * FROM v673_forward_events WHERE challenger_id=? ORDER BY cutoff_ts,token_mint',(rule['challenger_id'],)).fetchall()]
    counts={}
    for r in rows:counts[r['state']]=counts.get(r['state'],0)+1
    done=[r for r in rows if r['state']=='DONE' and r['net_return'] is not None];xs=[float(r['net_return']) for r in done]
    signals=len(rows)-counts.get('NO_SIGNAL',0);filled=[r for r in rows if r['fill_ts'] is not None and r['state']!='NO_SIGNAL']
    delays=[float(r['fill_delay_s']) for r in filled if r['fill_delay_s'] is not None and 0<=float(r['fill_delay_s'])<=float(rule['fill_window_s'])]
    n=len(xs);exp=statistics.mean(xs) if xs else None;med=statistics.median(xs) if xs else None;wr=sum(x>0 for x in xs)/n if n else None;p=pf(xs) if xs else None
    status='WAITING'
    if n>=MIN_SURVIVE:status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
    if n>=MIN_CONFIRM:status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
    vals=(rule['challenger_id'],rule['label'],len(rows),signals,counts.get('NO_SIGNAL',0),counts.get('WAIT_FILL',0),counts.get('NO_FILL',0),
      counts.get('WAIT_MATURITY',0),counts.get('SPARSE_PATH',0),counts.get('ANOMALY',0),n,
      len(filled)/signals if signals else None,statistics.median(delays) if delays else None,exp,med,wr,p,
      sum(int(r['hit']) for r in done)/n if n else None,max_dd(xs) if xs else None,status,time.time())
    d.execute('''INSERT INTO v673_forward_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(challenger_id) DO UPDATE SET label=excluded.label,eligible=excluded.eligible,signals=excluded.signals,
      no_signal=excluded.no_signal,waiting_fill=excluded.waiting_fill,no_fill=excluded.no_fill,waiting_maturity=excluded.waiting_maturity,
      sparse_path=excluded.sparse_path,anomaly=excluded.anomaly,done=excluded.done,fill_rate=excluded.fill_rate,
      median_fill_delay=excluded.median_fill_delay,expectancy=excluded.expectancy,median_net=excluded.median_net,
      win_rate=excluded.win_rate,profit_factor=excluded.profit_factor,hit_rate=excluded.hit_rate,max_drawdown=excluded.max_drawdown,
      status=excluded.status,updated_at=excluded.updated_at''',vals);d.commit();d.close()

def r64_benchmark():
    d=core.open_research();r=d.execute('SELECT * FROM v64_forward_summary LIMIT 1').fetchone();d.close();return dict(r) if r else {}

def display(rules):
    d=core.open_research();sums={r['challenger_id']:dict(x) for r in rules for x in [d.execute('SELECT * FROM v673_forward_summary WHERE challenger_id=?',(r['challenger_id'],)).fetchone()] if x};d.close()
    b=r64_benchmark();print('\033[2J\033[H',end='');print('='*188);print('MEMECOIN LAB — DUAL FUTURE-ONLY CHALLENGER ARENA V6.7.3');print('='*188)
    print(f"R64 CONTROL: status={b.get('status','?')} DONE={b.get('done',0)} exp={sf(b.get('expectancy'),0):+.2f}% PF={sf(b.get('profit_factor'),0):.2f} DD={sf(b.get('max_drawdown'),0):+.2f}%")
    print(f"Each challenger confirms at {MIN_CONFIRM} DONE. Rules and cutoffs are immutable; source tokens excluded.\n")
    for r in rules:
        s=sums.get(r['challenger_id'],{})
        print(f"{r['label']}  id={r['challenger_id']}  status={s.get('status','WAITING')}")
        print(f"  RULE {r['feature']} stage={r['stage_s']} h={r['horizon_s']} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f} dir={r['direction']:+.0f} th={r['threshold']:.12g} cutoff>{r['frozen_max_cutoff_ts']:.0f}")
        print(f"  SOURCE HO_n={r['source_holdout_n']} exp={sf(r['source_expectancy'],0):+.2f}% PF={sf(r['source_pf'],0):.2f} fill={100*sf(r['source_fill_rate'],0):.1f}%")
        print(f"  FORWARD eligible={s.get('eligible',0)} signals={s.get('signals',0)} DONE={s.get('done',0)} no_fill={s.get('no_fill',0)} sparse={s.get('sparse_path',0)} anomaly={s.get('anomaly',0)}")
        print(f"          fill={100*sf(s.get('fill_rate'),0):.1f}% delay={sf(s.get('median_fill_delay'),0):.2f}s exp={sf(s.get('expectancy'),0):+.2f}% med={sf(s.get('median_net'),0):+.2f}% win={100*sf(s.get('win_rate'),0):.1f}% PF={sf(s.get('profit_factor'),0):.2f} DD={sf(s.get('max_drawdown'),0):+.2f}%\n")
    print('Guardrail: prospective paper evidence only. No challenger may be retuned from these outcomes.')

def cycle(rules):
    for r in rules:process(r);summarize(r)
    display(rules)

def main():
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop);init();rules=load_rules()
    while not STOP:
        try:cycle(rules)
        except Exception as e:print('V6.7.3 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
