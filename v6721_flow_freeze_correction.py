#!/usr/bin/env python3
"""V6.7.2.1 — corrective immutable FLOW_DYNAMICS freeze.

The original V6.7.2 selector prioritized HOLDOUT count before strength and froze
a weak FLOW_DYNAMICS instance. This corrective step preserves that old freeze
for auditability, but creates a NEW immutable FLOW_DYNAMICS_CORRECTED challenger
using only already-consumed V6.7 evidence and a fresh future cutoff.

Selection rule (predeclared here, deterministic): PROMISING only, HO_n>=8,
then highest holdout expectancy, highest PF, highest fill rate, experiment id.
No threshold/direction retuning. No forward evaluation in this script.
"""
from __future__ import annotations
import json, math, time
import v41_core as core
import v63_next_fill_economic_edge_engine as v63
import v64_next_fill_future_only_arena as v64
import v59_champion_exploitation_engine as v59

FAMILY='FLOW_DYNAMICS'
LABEL='FLOW_DYNAMICS_CORRECTED'
MIN_HO_N=8

def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v6721_corrected_freezes(
      challenger_id TEXT PRIMARY KEY,label TEXT NOT NULL UNIQUE,family TEXT NOT NULL,
      source_epoch_id TEXT NOT NULL,source_experiment_id TEXT NOT NULL,
      feature TEXT NOT NULL,stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,
      tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,direction REAL NOT NULL,threshold REAL NOT NULL,
      fill_window_s REAL NOT NULL,cost_pct REAL NOT NULL,
      source_holdout_n INTEGER,source_expectancy REAL,source_pf REAL,source_fill_rate REAL,
      frozen_at REAL NOT NULL,frozen_max_cutoff_ts REAL NOT NULL,
      excluded_tokens_json TEXT NOT NULL,selection_policy TEXT NOT NULL);
    '''); d.commit(); d.close()

def source_tokens(r):
    # Use the same frozen V6.7 epoch dataset, not all later data.
    d=core.open_research(); ep=d.execute('SELECT * FROM v67_epoch WHERE epoch_id=?',(r['epoch_id'],)).fetchone(); d.close()
    if not ep:return []
    import v67_postcanonical_challenger_engine as v67
    data,_=v67.dataset(dict(ep),int(r['stage_s']),int(r['horizon_s']),float(r['tp_pct']),float(r['sl_pct']),str(r['feature']))
    return sorted({str(x['token_mint']) for x in data})

def choose():
    d=core.open_research(); rows=[dict(r) for r in d.execute('''SELECT * FROM v671_edge_instances
      WHERE family=? AND verdict='PROMISING' AND holdout_selected>=?
        AND holdout_expectancy>0 AND holdout_pf>1 AND expectancy_lift>0
      ORDER BY holdout_expectancy DESC,holdout_pf DESC,fill_rate DESC,experiment_id ASC''',(FAMILY,MIN_HO_N)).fetchall()]; d.close()
    return rows[0] if rows else None

def main():
    init(); d=core.open_research(); old=d.execute('SELECT * FROM v6721_corrected_freezes WHERE label=?',(LABEL,)).fetchone(); d.close()
    if old:r=dict(old); created=False
    else:
        r0=choose()
        if not r0:raise SystemExit('No PROMISING FLOW_DYNAMICS candidate with HO_n>=8')
        toks=source_tokens(r0); now=time.time(); cutoff=v64.current_max_cutoff()
        cid='C6721_'+core.fingerprint({'label':LABEL,'source':r0['experiment_id'],'freeze':now},'v6721:')[:22]
        policy='PROMISING only; HO_n>=8; max expectancy, PF, fill; exact V6.7 rule; fresh cutoff; no retuning'
        vals=(cid,LABEL,FAMILY,r0['epoch_id'],r0['experiment_id'],r0['feature'],int(r0['stage_s']),int(r0['horizon_s']),float(r0['tp_pct']),float(r0['sl_pct']),float(r0['direction']),float(r0['threshold']),float(v63.MAX_FILL_DELAY_S),float(v59.total_cost_pct()),int(r0['holdout_selected']),r0['holdout_expectancy'],r0['holdout_pf'],r0['fill_rate'],now,cutoff,core.canonical_json(toks),policy)
        d=core.open_research(); d.execute('INSERT INTO v6721_corrected_freezes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals); d.commit(); r=dict(d.execute('SELECT * FROM v6721_corrected_freezes WHERE challenger_id=?',(cid,)).fetchone()); d.close(); created=True
    print('='*176); print('MEMECOIN LAB — CORRECTIVE FLOW CHALLENGER FREEZE V6.7.2.1'); print('='*176)
    print(f"{r['label']} {'FROZEN_NOW' if created else 'ALREADY_FROZEN'} id={r['challenger_id']}")
    print(f"source={r['source_experiment_id']} feature={r['feature']} stage={r['stage_s']} h={r['horizon_s']} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f}")
    print(f"dir={r['direction']:+.0f} threshold={r['threshold']:.12g} | HO_n={r['source_holdout_n']} exp={sf(r['source_expectancy'],0):+.2f}% PF={sf(r['source_pf'],0):.2f} fill={100*sf(r['source_fill_rate'],0):.1f}%")
    print(f"future_cutoff>{r['frozen_max_cutoff_ts']:.3f} | excluded_source_tokens={len(json.loads(r['excluded_tokens_json']))}")
    print('Original weak V6.7.2 FLOW freeze is preserved for audit but must not be used in the challenger arena.')

if __name__=='__main__':main()
