#!/usr/bin/env python3
"""Memecoin Lab V6.3 — NEXT-EXECUTABLE-FILL economic research lane.

Scientific cohort B. V6.2 remains untouched.

Execution model:
- decision occurs at snapshot cutoff_ts
- entry is the FIRST observed swap price strictly AFTER cutoff_ts
- entry must arrive within MAX_FILL_DELAY_S (default 15s)
- TP/SL/time horizon start from the actual fill timestamp
- explicit V5.9 costs remain applied
- deterministic 75/25 token train/holdout split
- threshold/direction selected on TRAIN only
- HOLDOUT never tunes the rule

Research/paper only. No live trading or mutation of V6.2.
"""
from __future__ import annotations

import hashlib, json, math, os, signal, sqlite3, statistics, time
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59
import v60_economic_edge_discovery_engine as v60

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get("MEMECOIN_V52_DB",ROOT/"v52_features.db"))
LOOP=float(os.environ.get("MEMECOIN_V63_LOOP_S","10"))
BATCH=int(os.environ.get("MEMECOIN_V63_BATCH","8"))
MAX_FILL_DELAY_S=float(os.environ.get("MEMECOIN_V63_MAX_FILL_DELAY_S","15"))
MIN_PATH_POINTS=int(os.environ.get("MEMECOIN_V63_MIN_PATH_POINTS",str(v60.MIN_PATH_POINTS)))
MIN_TRAIN=int(os.environ.get("MEMECOIN_V63_MIN_TRAIN",str(v60.MIN_TRAIN)))
MIN_HOLDOUT=int(os.environ.get("MEMECOIN_V63_MIN_HOLDOUT",str(v60.MIN_HOLDOUT)))
STAGES=v60.STAGES
HORIZONS=v60.HORIZONS
BARRIERS=v60.BARRIERS
FEATURES=v60.FEATURES
STOP=False


def stop(*_):
    global STOP; STOP=True


def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d


def open_v52():
    if not V52.exists(): return None
    d=sqlite3.connect(f"file:{V52}?mode=ro",uri=True,timeout=30); d.row_factory=sqlite3.Row; d.execute("PRAGMA busy_timeout=30000"); return d


def holdout(token): return v60.holdout(token)


def init():
    d=core.open_research(); d.executescript("""
    CREATE TABLE IF NOT EXISTS v63_experiments(
      experiment_id TEXT PRIMARY KEY,
      stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,
      feature TEXT NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL,
      UNIQUE(stage_s,horizon_s,tp_pct,sl_pct,feature));
    CREATE TABLE IF NOT EXISTS v63_results(
      experiment_id TEXT PRIMARY KEY,
      train_n INTEGER NOT NULL,holdout_n INTEGER NOT NULL,
      train_selected INTEGER NOT NULL,holdout_selected INTEGER NOT NULL,
      direction REAL,threshold REAL,threshold_q REAL,
      train_expectancy REAL,train_median REAL,train_win REAL,train_pf REAL,train_hit REAL,
      holdout_expectancy REAL,holdout_median REAL,holdout_win REAL,holdout_pf REAL,holdout_hit REAL,
      baseline_expectancy REAL,baseline_hit REAL,expectancy_lift REAL,hit_lift REAL,
      median_fill_delay REAL,fill_rate REAL,verdict TEXT NOT NULL,metrics_json TEXT NOT NULL,updated_at REAL NOT NULL);
    """)
    now=time.time()
    for st in STAGES:
      for hz in HORIZONS:
       for tp,sl in BARRIERS:
        for feat in FEATURES:
         eid='E63_'+hashlib.sha256(f'{st}|{hz}|{tp}|{sl}|{feat}|nextfill15'.encode()).hexdigest()[:22]
         d.execute("INSERT OR IGNORE INTO v63_experiments VALUES(?,?,?,?,?,?,'READY',?,?)",(eid,st,hz,tp,sl,feat,now,now))
    d.commit(); d.close()


def next_fill(db,token,decision_ts):
    r=db.execute("""SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp>? AND price_sol IS NOT NULL AND price_sol>0
      ORDER BY timestamp ASC LIMIT 1""",(token,float(decision_ts))).fetchone()
    if not r:return None
    delay=float(r['timestamp'])-float(decision_ts)
    if delay<0 or delay>MAX_FILL_DELAY_S:return None
    return float(r['price_sol']),float(r['timestamp']),delay


def economic_path(db,token,decision_ts,horizon_s,tp,sl):
    fill=next_fill(db,token,decision_ts)
    if not fill:return None
    entry,fill_ts,delay=fill
    end=fill_ts+int(horizon_s)
    rs=db.execute("""SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp""",
      (token,fill_ts,end)).fetchall()
    if len(rs)<MIN_PATH_POINTS:return None
    prices=[float(r['price_sol']) for r in rs]
    if any(p<=0 or not math.isfinite(p) for p in prices):return None
    allp=[entry]+prices
    steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]
    rets=[100*(p/entry-1) for p in prices]
    if any(abs(x)>v60.MAX_ABS_STEP_PCT for x in steps):return None
    if any(abs(x)>v60.MAX_ABS_PATH_RETURN_PCT for x in rets):return None
    raw=rets[-1]; reason='TIME_EXIT'
    for x in rets:
        if x>=tp: raw=tp; reason='TP_FIRST'; break
        if x<=-sl: raw=-sl; reason='SL_FIRST'; break
    return {'net':raw-v59.total_cost_pct(),'raw':raw,'hit':int(reason=='TP_FIRST'),'reason':reason,'mfe':max(rets),'mae':min(rets),'fill_delay':delay}


def dataset(stage,horizon,tp,sl,feature):
    db=open_v52()
    if db is None:return [],0
    rows=db.execute(f"SELECT token_mint,cutoff_ts,{feature} AS feature FROM v52_snapshots WHERE stage_s=? AND {feature} IS NOT NULL ORDER BY cutoff_ts,token_mint",(int(stage),)).fetchall()
    out=[]; signalable=len(rows)
    for r in rows:
        x=sf(r['feature'])
        if x is None:continue
        econ=economic_path(db,str(r['token_mint']),float(r['cutoff_ts']),horizon,tp,sl)
        if not econ:continue
        out.append({'token_mint':str(r['token_mint']),'feature':x,'net':econ['net'],'hit':econ['hit'],'fill_delay':econ['fill_delay']})
    db.close(); return out,signalable


def metrics(rows):
    nets=[r['net'] for r in rows]
    return {'n':len(rows),'expectancy':statistics.mean(nets) if nets else None,'median':statistics.median(nets) if nets else None,
            'win':sum(x>0 for x in nets)/len(nets) if nets else None,'pf':v60.profit_factor(nets),'hit':sum(r['hit'] for r in rows)/len(rows) if rows else None}


def evaluate(data,total_rows):
    train=[r for r in data if not holdout(r['token_mint'])]; test=[r for r in data if holdout(r['token_mint'])]
    base={'train_n':len(train),'holdout_n':len(test),'fill_rate':len(data)/max(1,total_rows),'median_fill_delay':statistics.median([r['fill_delay'] for r in data]) if data else None}
    if len(train)<MIN_TRAIN or len(test)<MIN_HOLDOUT:return 'COLLECT_MORE',base
    rho=v60.spearman([r['feature'] for r in train],[r['net'] for r in train])
    if rho is None:return 'REJECT',base
    direction=1.0 if rho>=0 else -1.0
    candidates=[]
    for q in (.60,.70,.75,.80,.85,.90):
        th=v60.quantile([direction*r['feature'] for r in train],q)
        sel=[r for r in train if direction*r['feature']>=th] if th is not None else []
        m=metrics(sel)
        if m['n']<max(12,MIN_TRAIN//3):continue
        score=sf(m['expectancy'],-1e9)+2*min(sf(m['pf'],0),3)+.03*math.sqrt(m['n'])
        candidates.append((score,th,q,m))
    if not candidates:return 'REJECT',dict(base,direction=direction)
    _,th,qsel,trm=max(candidates,key=lambda x:x[0])
    test_sel=[r for r in test if direction*r['feature']>=th]; hom=metrics(test_sel); hob=metrics(test)
    exp_lift=None if hom['expectancy'] is None or hob['expectancy'] is None else hom['expectancy']-hob['expectancy']
    hit_lift=None if hom['hit'] is None or hob['hit'] in (None,0) else hom['hit']/hob['hit']
    verdict='REJECT'
    if hom['n']<8: verdict='COLLECT_MORE'
    elif sf(hom['expectancy'],-1)<=0 or sf(hom['pf'],0)<=1: verdict='REJECT'
    elif sf(exp_lift,-1)<=0: verdict='WEAK'
    elif sf(hom['pf'],0)>=1.25 and sf(hom['expectancy'],0)>=1: verdict='PROMISING'
    else: verdict='WEAK'
    return verdict,dict(base,direction=direction,threshold=th,threshold_q=qsel,train_selected=trm,holdout_selected=hom,holdout_baseline=hob,expectancy_lift=exp_lift,hit_lift=hit_lift)


def run_one(exp):
    data,total=dataset(exp['stage_s'],exp['horizon_s'],exp['tp_pct'],exp['sl_pct'],exp['feature']); verdict,m=evaluate(data,total)
    tr=m.get('train_selected',{}); ho=m.get('holdout_selected',{}); ba=m.get('holdout_baseline',{}); now=time.time(); d=core.open_research()
    d.execute("""INSERT OR REPLACE INTO v63_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (exp['experiment_id'],int(m.get('train_n',0)),int(m.get('holdout_n',0)),int(tr.get('n',0)),int(ho.get('n',0)),m.get('direction'),m.get('threshold'),m.get('threshold_q'),
       tr.get('expectancy'),tr.get('median'),tr.get('win'),tr.get('pf'),tr.get('hit'),ho.get('expectancy'),ho.get('median'),ho.get('win'),ho.get('pf'),ho.get('hit'),
       ba.get('expectancy'),ba.get('hit'),m.get('expectancy_lift'),m.get('hit_lift'),m.get('median_fill_delay'),m.get('fill_rate'),verdict,core.canonical_json(m),now))
    d.execute("UPDATE v63_experiments SET status='DONE',updated_at=? WHERE experiment_id=?",(now,exp['experiment_id'])); d.commit(); d.close()


def display(ran):
    d=core.open_research(); counts={r[0]:r[1] for r in d.execute("SELECT status,COUNT(*) FROM v63_experiments GROUP BY status")}; rows=[dict(r) for r in d.execute("""SELECT e.*,r.* FROM v63_results r JOIN v63_experiments e USING(experiment_id)
      WHERE r.verdict IN ('PROMISING','WEAK') ORDER BY CASE r.verdict WHEN 'PROMISING' THEN 0 ELSE 1 END, r.holdout_expectancy DESC LIMIT 15""").fetchall()]; d.close()
    print('\033[2J\033[H',end=''); print('='*184); print('MEMECOIN LAB — NEXT-EXECUTABLE-FILL ECONOMIC EDGE ENGINE V6.3'); print('='*184)
    print(f"READY={counts.get('READY',0)} DONE={counts.get('DONE',0)} RAN={ran} | fill_window<={MAX_FILL_DELAY_S:.0f}s | costs={v59.total_cost_pct():.2f}%")
    print('Decision at cutoff; first observed post-signal price is the fill. TP/SL clock starts at actual fill. V6.2 is untouched.\n')
    if not rows: print('No PROMISING/WEAK V6.3 edge yet.')
    for r in rows:
      print(f"{r['verdict']:<10} stage={r['stage_s']:<3} h={r['horizon_s']:<3} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f} {r['feature']:<22} th={sf(r['threshold'],0):.4g} dir={sf(r['direction'],0):+.0f} HO_n={r['holdout_selected']:<3} exp={sf(r['holdout_expectancy'],0):+.2f}% PF={sf(r['holdout_pf'],0):.2f} lift={sf(r['expectancy_lift'],0):+.2f}% fill={100*sf(r['fill_rate'],0):.1f}% delay_med={sf(r['median_fill_delay'],0):.1f}s")


def cycle():
    d=core.open_research(); exps=[dict(r) for r in d.execute("SELECT * FROM v63_experiments WHERE status='READY' ORDER BY created_at LIMIT ?",(BATCH,)).fetchall()]; d.close()
    for e in exps: run_one(e)
    display(len(exps)); return len(exps)


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try: cycle()
        except Exception as e: print('V6.3 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
