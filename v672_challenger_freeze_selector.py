#!/usr/bin/env python3
"""Memecoin Lab V6.7.2 — immutable challenger freeze selector.

Selects exactly one WALLET_STRUCTURE and one FLOW_DYNAMICS candidate from the
completed V6.7 epoch, using only already-observed V6.7/V6.7.1 evidence. It then
freezes their exact feature/stage/horizon/TP/SL/direction/threshold behind a
fresh cutoff watermark. No future observations are evaluated here.

Selection policy is deterministic and intentionally conservative:
- candidate must already be a positive V6.7 edge instance with HO_n >= 8;
- family must be REPLICATED in V6.7.1;
- prefer larger HOLDOUT, then expectancy, PF, fill rate;
- exact selected rule is copied without threshold/direction retuning;
- source V6.7 dataset tokens are excluded from later prospective validation;
- freeze is write-once: reruns never replace an existing frozen candidate.
"""
from __future__ import annotations

import json, math, time
from pathlib import Path

import v41_core as core
import v63_next_fill_economic_edge_engine as v63
import v64_next_fill_future_only_arena as v64

TARGET_FAMILIES=("WALLET_STRUCTURE","FLOW_DYNAMICS")
MIN_HO_N=8


def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d


def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v672_frozen_challengers(
      challenger_id TEXT PRIMARY KEY,
      family TEXT NOT NULL UNIQUE,
      source_epoch_id TEXT NOT NULL,
      source_experiment_id TEXT NOT NULL,
      feature TEXT NOT NULL,stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,
      tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,direction REAL NOT NULL,threshold REAL NOT NULL,
      fill_window_s REAL NOT NULL,cost_pct REAL NOT NULL,
      source_holdout_n INTEGER,source_expectancy REAL,source_pf REAL,source_fill_rate REAL,
      frozen_at REAL NOT NULL,frozen_max_cutoff_ts REAL NOT NULL,
      excluded_tokens_json TEXT NOT NULL,selection_policy TEXT NOT NULL);
    '''); d.commit(); d.close()


def source_tokens(r):
    # Exclude every token visible to the source experiment's frozen research dataset.
    data,_=v63.dataset(int(r['stage_s']),int(r['horizon_s']),float(r['tp_pct']),float(r['sl_pct']),str(r['feature']))
    return sorted({str(x['token_mint']) for x in data})


def choose(family):
    d=core.open_research()
    fam=d.execute('SELECT * FROM v671_family_champions WHERE family=?',(family,)).fetchone()
    if not fam or str(fam['status']) not in ('REPLICATED','CHALLENGER_READY'):
        d.close(); return None
    rows=[dict(r) for r in d.execute('''SELECT * FROM v671_edge_instances
      WHERE family=? AND holdout_selected>=? AND holdout_expectancy>0 AND holdout_pf>1 AND expectancy_lift>0
      ORDER BY holdout_selected DESC,holdout_expectancy DESC,holdout_pf DESC,fill_rate DESC,experiment_id ASC''',(family,MIN_HO_N)).fetchall()]
    d.close()
    return rows[0] if rows else None


def freeze_family(family):
    d=core.open_research(); old=d.execute('SELECT * FROM v672_frozen_challengers WHERE family=?',(family,)).fetchone(); d.close()
    if old:return dict(old),False
    r=choose(family)
    if not r:return None,False
    toks=source_tokens(r); frozen_at=time.time(); max_cut=v64.current_max_cutoff()
    cid='C672_'+core.fingerprint({'family':family,'source':r['experiment_id'],'freeze':frozen_at},'v672:')[:22]
    policy='largest_HO_then_expectancy_PF_fill; exact V6.7 rule; no retuning'
    vals=(cid,family,r['epoch_id'],r['experiment_id'],r['feature'],int(r['stage_s']),int(r['horizon_s']),
          float(r['tp_pct']),float(r['sl_pct']),float(r['direction']),float(r['threshold']),float(v63.MAX_FILL_DELAY_S),
          float(__import__('v59_champion_exploitation_engine').total_cost_pct()),int(r['holdout_selected']),
          r['holdout_expectancy'],r['holdout_pf'],r['fill_rate'],frozen_at,max_cut,core.canonical_json(toks),policy)
    d=core.open_research(); d.execute('INSERT INTO v672_frozen_challengers VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals); d.commit()
    out=dict(d.execute('SELECT * FROM v672_frozen_challengers WHERE challenger_id=?',(cid,)).fetchone()); d.close(); return out,True


def display(rows):
    print('='*176); print('MEMECOIN LAB — IMMUTABLE CHALLENGER FREEZE SELECTOR V6.7.2'); print('='*176)
    print('Two families only. Exact V6.7 parameters are copied once; fresh cutoff is frozen before prospective evaluation.\n')
    for r,created in rows:
        if not r:
            print('NOT FROZEN — required replicated family evidence missing'); continue
        print(f"{r['family']:<18} {'FROZEN_NOW' if created else 'ALREADY_FROZEN'}  id={r['challenger_id']}")
        print(f"  source={r['source_experiment_id']} feature={r['feature']} stage={r['stage_s']} h={r['horizon_s']} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f}")
        print(f"  dir={r['direction']:+.0f} threshold={r['threshold']:.12g} | HO_n={r['source_holdout_n']} exp={sf(r['source_expectancy'],0):+.2f}% PF={sf(r['source_pf'],0):.2f} fill={100*sf(r['source_fill_rate'],0):.1f}%")
        print(f"  future_cutoff>{r['frozen_max_cutoff_ts']:.3f} | excluded_source_tokens={len(json.loads(r['excluded_tokens_json']))}\n")
    print('NEXT: prospective arena must consume only snapshots strictly after each frozen cutoff. This script performs no forward evaluation.')


def main():
    init(); rows=[freeze_family(f) for f in TARGET_FAMILIES]; display(rows)

if __name__=='__main__':main()
