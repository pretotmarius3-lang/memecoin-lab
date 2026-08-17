#!/usr/bin/env python3
"""Memecoin Lab V5.9.1.1 — hotfix for V5.9.1 SQL insert arity.

Reuses every V5.9.1 calculation unchanged and overrides only audit_policy so the
18-column v591_policy_audit row is written with 18 placeholders.
"""
from __future__ import annotations

import json
import signal
import statistics
import time
from collections import Counter, defaultdict

import v41_core as core
import v59_champion_exploitation_engine as v59
import v591_execution_reality_engine as base


def audit_policy(p):
    spec=json.loads(p['spec_json'])
    rows=v59.score_rows(spec,before_ts=float(p['freeze_cutoff_ts']))
    selected=[]
    for r in rows:
        score=float(p['direction'])*r['feature_value']
        if score>=float(p['threshold']):
            selected.append((r,score))

    rejects=Counter(); trades=[]
    for r,score in selected:
        path,reason=base.load_path(r['token_mint'],r['decision_ts'],int(p['holding_s']))
        if reason!='OK':
            rejects[reason]+=1
            continue
        trades.append({
            'token':r['token_mint'],
            'score':score,
            'path':path,
            'fixed_net':path['fixed_raw']-v59.total_cost_pct(),
        })

    fixed=[t['fixed_net'] for t in trades]
    mfes=[t['path']['mfe'] for t in trades]
    maes=[t['path']['mae'] for t in trades]

    barriers=[]
    for tp,sl in base.barrier_grid(spec):
        nets=[]; outcomes=Counter()
        for t in trades:
            raw,outcome=base.first_touch(t['path'],tp,sl)
            outcomes[outcome]+=1
            nets.append(raw-v59.total_cost_pct())
        barriers.append({
            'tp':tp,'sl':sl,'n':len(nets),
            'tp_first':outcomes['TP_FIRST'],
            'sl_first':outcomes['SL_FIRST'],
            'time_exit':outcomes['TIME_EXIT'],
            'avg':base.trimmed_mean(nets),
            'median':statistics.median(nets) if nets else None,
            'win':sum(x>0 for x in nets)/len(nets) if nets else None,
            'pf':base.profit_factor(nets),
            'worst':min(nets) if nets else None,
            'best':max(nets) if nets else None,
        })

    best=max(
        barriers,
        key=lambda x:(
            x['median'] if x['median'] is not None else -1e99,
            x['win'] if x['win'] is not None else -1,
        ),
    ) if barriers else None

    d=core.open_research()
    d.execute("DELETE FROM v591_barrier_results WHERE policy_id=?",(p['policy_id'],))
    d.execute("DELETE FROM v591_score_buckets WHERE policy_id=?",(p['policy_id'],))

    for b in barriers:
        d.execute(
            "INSERT INTO v591_barrier_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                p['policy_id'],b['tp'],b['sl'],b['n'],b['tp_first'],
                b['sl_first'],b['time_exit'],b['avg'],b['median'],b['win'],
                b['pf'],b['worst'],b['best'],
            ),
        )

    grouped=defaultdict(list)
    for bucket,t in base.quartile_buckets(trades):
        grouped[bucket].append(t)

    for bucket,ts in grouped.items():
        nets=[t['fixed_net'] for t in ts]
        scores=[t['score'] for t in ts]
        bm=[t['path']['mfe'] for t in ts]
        ba=[t['path']['mae'] for t in ts]
        d.execute(
            "INSERT INTO v591_score_buckets VALUES(?,?,?,?,?,?,?,?)",
            (
                p['policy_id'],bucket,len(ts),statistics.median(scores),
                statistics.median(nets),sum(x>0 for x in nets)/len(nets),
                statistics.median(bm),statistics.median(ba),
            ),
        )

    details={
        'cost_pct':v59.total_cost_pct(),
        'quality_gate':{
            'max_entry_gap_s':base.MAX_GAP_S,
            'min_path_points':base.MIN_PATH_POINTS,
            'max_abs_step_pct':base.MAX_ABS_STEP_PCT,
            'max_abs_path_return_pct':base.MAX_ABS_PATH_RETURN_PCT,
        },
        'warning':'Barrier grid is historical diagnostic only; best historical barrier is NOT promoted to the frozen forward policy.',
    }

    # 18 table columns => 18 values/placeholders.
    d.execute(
        """INSERT INTO v591_policy_audit VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(policy_id) DO UPDATE SET
          raw_signals=excluded.raw_signals,
          executable=excluded.executable,
          rejected_no_entry=excluded.rejected_no_entry,
          rejected_stale_entry=excluded.rejected_stale_entry,
          rejected_no_path=excluded.rejected_no_path,
          rejected_sparse_path=excluded.rejected_sparse_path,
          rejected_price_anomaly=excluded.rejected_price_anomaly,
          fixed_median_net=excluded.fixed_median_net,
          fixed_trimmed_mean_net=excluded.fixed_trimmed_mean_net,
          fixed_win_rate=excluded.fixed_win_rate,
          median_mfe=excluded.median_mfe,
          median_mae=excluded.median_mae,
          best_barrier_label=excluded.best_barrier_label,
          best_barrier_median_net=excluded.best_barrier_median_net,
          best_barrier_win_rate=excluded.best_barrier_win_rate,
          details_json=excluded.details_json,
          updated_at=excluded.updated_at""",
        (
            p['policy_id'],
            len(selected),
            len(trades),
            rejects['NO_ENTRY'],
            rejects['STALE_ENTRY'],
            rejects['NO_PATH'],
            rejects['SPARSE_PATH'],
            rejects['PRICE_ANOMALY'],
            statistics.median(fixed) if fixed else None,
            base.trimmed_mean(fixed),
            sum(x>0 for x in fixed)/len(fixed) if fixed else None,
            statistics.median(mfes) if mfes else None,
            statistics.median(maes) if maes else None,
            f"TP{best['tp']:.0f}/SL{best['sl']:.0f}" if best else None,
            best['median'] if best else None,
            best['win'] if best else None,
            core.canonical_json(details),
            time.time(),
        ),
    )
    d.commit(); d.close()


def cycle():
    v59.init()
    policies=v59.refresh()
    for p in policies:
        audit_policy(p)
    base.display(policies)


def main():
    signal.signal(signal.SIGINT,base.stop)
    signal.signal(signal.SIGTERM,base.stop)
    base.init()
    while not base.STOP:
        try:
            cycle()
        except Exception as e:
            print('V5.9.1.1 error:',repr(e),flush=True)
        time.sleep(base.LOOP)


if __name__=='__main__':
    main()
