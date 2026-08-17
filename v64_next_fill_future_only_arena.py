#!/usr/bin/env python3
"""Memecoin Lab V6.4 — strict NEXT-FILL future-only arena.

Freezes the best completed V6.3 PRICE_DYNAMICS rule exactly as learned on V6.3
and evaluates only snapshots created beyond a fresh V6.4 cutoff watermark.

Non-negotiable scientific rules:
- V6.3 direction/threshold/stage/horizon/TP/SL are copied exactly; never retuned.
- first observed price strictly AFTER the signal is the simulated fill.
- fill must occur within the frozen <=15s window.
- TP/SL/time horizon start at the actual fill timestamp.
- tokens used by the source V6.3 experiment are explicitly excluded.
- every future observation is auditable through an explicit state machine.
- no live trading/signing; paper evidence only.
"""
from __future__ import annotations

import json, math, os, signal, sqlite3, statistics, time
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59
import v60_economic_edge_discovery_engine as v60
import v63_next_fill_economic_edge_engine as v63

ROOT=Path.home()/"memecoin_lab"
V52=Path(os.environ.get('MEMECOIN_V52_DB',ROOT/'v52_features.db'))
LOOP=float(os.environ.get('MEMECOIN_V64_LOOP_S','10'))
MIN_SURVIVE=int(os.environ.get('MEMECOIN_V64_MIN_SURVIVE_TRADES','10'))
MIN_CONFIRM=int(os.environ.get('MEMECOIN_V64_MIN_CONFIRM_TRADES','30'))
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
    CREATE TABLE IF NOT EXISTS v64_frozen_rule(
      rule_id TEXT PRIMARY KEY,
      source_experiment_id TEXT NOT NULL,
      feature TEXT NOT NULL,stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,
      tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,direction REAL NOT NULL,threshold REAL NOT NULL,
      fill_window_s REAL NOT NULL,cost_pct REAL NOT NULL,
      source_holdout_n INTEGER,source_expectancy REAL,source_pf REAL,source_fill_rate REAL,
      frozen_at REAL NOT NULL,frozen_max_cutoff_ts REAL NOT NULL,
      excluded_tokens_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS v64_forward_events(
      rule_id TEXT NOT NULL,token_mint TEXT NOT NULL,cutoff_ts REAL NOT NULL,
      feature_value REAL NOT NULL,state TEXT NOT NULL,
      fill_price REAL,fill_ts REAL,fill_delay_s REAL,path_points INTEGER,
      raw_return REAL,net_return REAL,hit INTEGER,exit_reason TEXT,mfe REAL,mae REAL,
      first_seen_at REAL NOT NULL,updated_at REAL NOT NULL,
      PRIMARY KEY(rule_id,token_mint,cutoff_ts));
    CREATE TABLE IF NOT EXISTS v64_forward_summary(
      rule_id TEXT PRIMARY KEY,eligible INTEGER NOT NULL,signals INTEGER NOT NULL,
      no_signal INTEGER NOT NULL,waiting_fill INTEGER NOT NULL,no_fill INTEGER NOT NULL,
      waiting_maturity INTEGER NOT NULL,sparse_path INTEGER NOT NULL,anomaly INTEGER NOT NULL,
      done INTEGER NOT NULL,fill_rate REAL,median_fill_delay REAL,
      expectancy REAL,median_net REAL,win_rate REAL,profit_factor REAL,hit_rate REAL,max_drawdown REAL,
      status TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()


def current_max_cutoff():
    db=open_v52()
    if db is None:return 0.0
    r=db.execute('SELECT MAX(cutoff_ts) FROM v52_snapshots').fetchone(); db.close(); return sf(r[0],0.0) or 0.0


def source_tokens(exp):
    data,_=v63.dataset(exp['stage_s'],exp['horizon_s'],exp['tp_pct'],exp['sl_pct'],exp['feature'])
    return sorted({str(r['token_mint']) for r in data})


def freeze_once():
    d=core.open_research(); existing=d.execute('SELECT COUNT(*) FROM v64_frozen_rule').fetchone()[0]
    if existing:d.close(); return 0
    r=d.execute('''SELECT e.*,r.* FROM v63_experiments e JOIN v63_results r USING(experiment_id)
      WHERE e.status='DONE' AND r.verdict='PROMISING' AND e.feature='price_velocity'
      ORDER BY r.holdout_expectancy DESC,r.holdout_pf DESC,r.holdout_selected DESC LIMIT 1''').fetchone()
    if not r:d.close(); return 0
    x=dict(r); excluded=source_tokens(x); freeze_ts=time.time(); max_cut=current_max_cutoff()
    rid='R64_'+core.fingerprint({'source':x['experiment_id'],'freeze':freeze_ts},'v64:')[:22]
    d.execute('''INSERT INTO v64_frozen_rule VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(
      rid,x['experiment_id'],x['feature'],int(x['stage_s']),int(x['horizon_s']),float(x['tp_pct']),float(x['sl_pct']),
      float(x['direction']),float(x['threshold']),float(v63.MAX_FILL_DELAY_S),float(v59.total_cost_pct()),
      int(x['holdout_selected']),x['holdout_expectancy'],x['holdout_pf'],x['fill_rate'],freeze_ts,max_cut,
      core.canonical_json(excluded)))
    d.commit(); d.close(); return 1


def classify(db,rule,token,cutoff,x,now):
    directed=float(rule['direction'])*float(x)
    if directed<float(rule['threshold']):
        return {'state':'NO_SIGNAL'}
    fill=db.execute('''SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp>? AND price_sol IS NOT NULL AND price_sol>0
      ORDER BY timestamp ASC LIMIT 1''',(token,float(cutoff))).fetchone()
    deadline=float(cutoff)+float(rule['fill_window_s'])
    if not fill:
        return {'state':'WAIT_FILL' if now<=deadline else 'NO_FILL'}
    fill_ts=float(fill['timestamp']); delay=fill_ts-float(cutoff)
    if delay<0 or delay>float(rule['fill_window_s']):
        return {'state':'NO_FILL','fill_delay_s':delay}
    entry=float(fill['price_sol']); end=fill_ts+int(rule['horizon_s'])
    if now<end:
        return {'state':'WAIT_MATURITY','fill_price':entry,'fill_ts':fill_ts,'fill_delay_s':delay}
    rs=db.execute('''SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp>? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0 ORDER BY timestamp''',
      (token,fill_ts,end)).fetchall()
    n=len(rs)
    if n<int(v63.MIN_PATH_POINTS):
        return {'state':'SPARSE_PATH','fill_price':entry,'fill_ts':fill_ts,'fill_delay_s':delay,'path_points':n}
    prices=[float(r['price_sol']) for r in rs]; allp=[entry]+prices
    steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]; rets=[100*(p/entry-1) for p in prices]
    if any(abs(z)>v60.MAX_ABS_STEP_PCT for z in steps) or any(abs(z)>v60.MAX_ABS_PATH_RETURN_PCT for z in rets):
        return {'state':'ANOMALY','fill_price':entry,'fill_ts':fill_ts,'fill_delay_s':delay,'path_points':n}
    raw=rets[-1]; reason='TIME_EXIT'
    for z in rets:
        if z>=float(rule['tp_pct']):raw=float(rule['tp_pct']); reason='TP_FIRST'; break
        if z<=-float(rule['sl_pct']):raw=-float(rule['sl_pct']); reason='SL_FIRST'; break
    return {'state':'DONE','fill_price':entry,'fill_ts':fill_ts,'fill_delay_s':delay,'path_points':n,
            'raw_return':raw,'net_return':raw-float(rule['cost_pct']),'hit':int(reason=='TP_FIRST'),'exit_reason':reason,
            'mfe':max(rets),'mae':min(rets)}


def upsert_event(rule,token,cutoff,x,res):
    now=time.time(); d=core.open_research()
    d.execute('''INSERT INTO v64_forward_events(
      rule_id,token_mint,cutoff_ts,feature_value,state,fill_price,fill_ts,fill_delay_s,path_points,
      raw_return,net_return,hit,exit_reason,mfe,mae,first_seen_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(rule_id,token_mint,cutoff_ts) DO UPDATE SET
      state=excluded.state,fill_price=excluded.fill_price,fill_ts=excluded.fill_ts,fill_delay_s=excluded.fill_delay_s,
      path_points=excluded.path_points,raw_return=excluded.raw_return,net_return=excluded.net_return,hit=excluded.hit,
      exit_reason=excluded.exit_reason,mfe=excluded.mfe,mae=excluded.mae,updated_at=excluded.updated_at''',(
      rule['rule_id'],token,float(cutoff),float(x),res['state'],res.get('fill_price'),res.get('fill_ts'),res.get('fill_delay_s'),
      res.get('path_points'),res.get('raw_return'),res.get('net_return'),res.get('hit'),res.get('exit_reason'),res.get('mfe'),res.get('mae'),now,now))
    d.commit(); d.close()


def process(rule):
    db=open_v52()
    if db is None:return 0
    excluded=set(json.loads(rule['excluded_tokens_json'])); rows=db.execute(f'''SELECT token_mint,cutoff_ts,{rule['feature']} AS feature
      FROM v52_snapshots WHERE stage_s=? AND cutoff_ts>? AND {rule['feature']} IS NOT NULL ORDER BY cutoff_ts,token_mint''',
      (int(rule['stage_s']),float(rule['frozen_max_cutoff_ts']))).fetchall()
    made=0; now=time.time()
    for r in rows:
        token=str(r['token_mint'])
        if token in excluded:continue
        x=sf(r['feature'])
        if x is None:continue
        res=classify(db,rule,token,float(r['cutoff_ts']),x,now)
        upsert_event(rule,token,float(r['cutoff_ts']),x,res); made+=1
    db.close(); return made


def pf(xs):
    g=sum(x for x in xs if x>0); l=-sum(x for x in xs if x<0)
    return g/l if l>0 else (999.0 if g>0 else None)


def max_dd(xs):
    eq=peak=0.0; dd=0.0
    for x in xs:eq+=x; peak=max(peak,eq); dd=min(dd,eq-peak)
    return dd


def summarize(rule):
    d=core.open_research(); rows=[dict(r) for r in d.execute('SELECT * FROM v64_forward_events WHERE rule_id=? ORDER BY cutoff_ts,token_mint',(rule['rule_id'],)).fetchall()]
    counts={}
    for r in rows:counts[r['state']]=counts.get(r['state'],0)+1
    done=[r for r in rows if r['state']=='DONE' and r['net_return'] is not None]; xs=[float(r['net_return']) for r in done]
    signals=len(rows)-counts.get('NO_SIGNAL',0); filled=[r for r in rows if r['fill_ts'] is not None and r['state']!='NO_SIGNAL']
    delays=[float(r['fill_delay_s']) for r in filled if r['fill_delay_s'] is not None and 0<=float(r['fill_delay_s'])<=float(rule['fill_window_s'])]
    n=len(done); exp=statistics.mean(xs) if xs else None; med=statistics.median(xs) if xs else None
    wr=sum(x>0 for x in xs)/n if n else None; p=pf(xs); hr=sum(int(r['hit']) for r in done)/n if n else None
    status='WAITING'
    if n>=MIN_SURVIVE:status='SURVIVING' if sf(exp,-1)>0 and sf(p,0)>1 else 'DECAYING'
    if n>=MIN_CONFIRM:status='CONFIRMED' if sf(exp,-1)>0 and sf(p,0)>1 else 'FAILED_FORWARD'
    vals=(rule['rule_id'],len(rows),signals,counts.get('NO_SIGNAL',0),counts.get('WAIT_FILL',0),counts.get('NO_FILL',0),
          counts.get('WAIT_MATURITY',0),counts.get('SPARSE_PATH',0),counts.get('ANOMALY',0),n,
          len(filled)/signals if signals else None,statistics.median(delays) if delays else None,exp,med,wr,p,hr,max_dd(xs) if xs else None,status,time.time())
    d.execute('''INSERT INTO v64_forward_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(rule_id) DO UPDATE SET eligible=excluded.eligible,signals=excluded.signals,no_signal=excluded.no_signal,
      waiting_fill=excluded.waiting_fill,no_fill=excluded.no_fill,waiting_maturity=excluded.waiting_maturity,sparse_path=excluded.sparse_path,
      anomaly=excluded.anomaly,done=excluded.done,fill_rate=excluded.fill_rate,median_fill_delay=excluded.median_fill_delay,
      expectancy=excluded.expectancy,median_net=excluded.median_net,win_rate=excluded.win_rate,profit_factor=excluded.profit_factor,
      hit_rate=excluded.hit_rate,max_drawdown=excluded.max_drawdown,status=excluded.status,updated_at=excluded.updated_at''',vals)
    d.commit(); d.close()


def display(rule):
    d=core.open_research(); s=d.execute('SELECT * FROM v64_forward_summary WHERE rule_id=?',(rule['rule_id'],)).fetchone(); d.close(); s=dict(s) if s else {}
    print('\033[2J\033[H',end=''); print('='*184); print('MEMECOIN LAB — NEXT-FILL FUTURE-ONLY ARENA V6.4'); print('='*184)
    print(f"SOURCE={rule['source_experiment_id']} | excluded_source_tokens={len(json.loads(rule['excluded_tokens_json']))} | confirm_at={MIN_CONFIRM} DONE | costs={rule['cost_pct']:.2f}%")
    print('Fresh V6.4 watermark. Exact V6.3 rule is immutable; no pre-freeze token may validate this arena.\n')
    print(f"RULE feature={rule['feature']} stage={rule['stage_s']} h={rule['horizon_s']} TP/SL={rule['tp_pct']:.0f}/{rule['sl_pct']:.0f} dir={rule['direction']:+.0f} threshold={rule['threshold']:.12g} fill<={rule['fill_window_s']:.0f}s")
    print(f"SOURCE HOLDOUT n={rule['source_holdout_n']} exp={sf(rule['source_expectancy'],0):+.2f}% PF={sf(rule['source_pf'],0):.2f} fill={100*sf(rule['source_fill_rate'],0):.1f}%")
    print(f"\nFORWARD status={s.get('status','WAITING')} eligible={s.get('eligible',0)} signals={s.get('signals',0)} DONE={s.get('done',0)}")
    print(f"  no_signal={s.get('no_signal',0)} wait_fill={s.get('waiting_fill',0)} no_fill={s.get('no_fill',0)} wait_maturity={s.get('waiting_maturity',0)} sparse={s.get('sparse_path',0)} anomaly={s.get('anomaly',0)}")
    print(f"  fill_rate={100*sf(s.get('fill_rate'),0):.1f}% delay_med={sf(s.get('median_fill_delay'),0):.2f}s | exp={sf(s.get('expectancy'),0):+.2f}% med={sf(s.get('median_net'),0):+.2f}% win={100*sf(s.get('win_rate'),0):.1f}% PF={sf(s.get('profit_factor'),0):.2f} DD={sf(s.get('max_drawdown'),0):+.2f}%")
    print('\nSTATE MACHINE: ELIGIBLE -> NO_SIGNAL | WAIT_FILL -> NO_FILL / WAIT_MATURITY -> SPARSE / ANOMALY / DONE')
    print('Guardrail: CONFIRMED means prospective paper evidence under the frozen NEXT-FILL model, not authorization for live capital.')


def cycle():
    freeze_once(); d=core.open_research(); r=d.execute('SELECT * FROM v64_frozen_rule LIMIT 1').fetchone(); d.close()
    if not r:
        print('V6.4 waiting: no qualifying completed V6.3 PRICE_DYNAMICS rule.'); return
    rule=dict(r); process(rule); summarize(rule); display(rule)


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:cycle()
        except Exception as e:print('V6.4 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
