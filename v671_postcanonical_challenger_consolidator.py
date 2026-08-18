#!/usr/bin/env python3
"""Memecoin Lab V6.7.1 — post-canonical challenger consolidator.

Consumes completed V6.7 results without retuning thresholds. Reconstructs exact
selected HOLDOUT tokens inside the frozen V6.7 epoch, controls token overlap,
checks cross-regime replication, threshold/direction stability, and compares
families against the clean POST-V5.2.2 R64 benchmark.

Read-only with respect to V6.4/R64 and V6.7 experiments/results. Writes only
v671_* consolidation tables. No future-only challenger freeze is created here.
"""
from __future__ import annotations

import json, math, statistics, time
from collections import defaultdict

import v41_core as core
import v61_economic_champion_consolidator as v61
import v67_postcanonical_challenger_engine as v67
import v643_v522_cohort_audit as cohort

MIN_REGIMES=2
MIN_UNIQUE_TOKENS=12
MAX_OVERLAP=0.70
MIN_FILL_RATE=0.10
MIN_HO_N=8


def sf(x,d=None):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def jaccard(a,b):
    a=set(a); b=set(b); u=len(a|b)
    return len(a&b)/u if u else 0.0

def pf(xs):
    g=sum(x for x in xs if x>0); l=-sum(x for x in xs if x<0)
    return g/l if l>0 else (999.0 if g>0 else 0.0)

def dd(xs):
    eq=peak=0.0; worst=0.0
    for x in xs:
        eq+=x; peak=max(peak,eq); worst=min(worst,eq-peak)
    return worst

def epoch():
    d=core.open_research(); r=d.execute('SELECT * FROM v67_epoch ORDER BY created_at LIMIT 1').fetchone(); d.close()
    if not r: raise SystemExit('No V6.7 epoch found')
    return dict(r)

def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v671_edge_instances(
      experiment_id TEXT PRIMARY KEY,epoch_id TEXT NOT NULL,family TEXT NOT NULL,feature TEXT NOT NULL,
      stage_s INTEGER NOT NULL,horizon_s INTEGER NOT NULL,tp_pct REAL NOT NULL,sl_pct REAL NOT NULL,
      direction REAL NOT NULL,threshold REAL NOT NULL,threshold_q REAL,
      holdout_selected INTEGER NOT NULL,selected_tokens_json TEXT NOT NULL,
      holdout_expectancy REAL,holdout_pf REAL,holdout_win REAL,expectancy_lift REAL,
      fill_rate REAL,median_fill_delay REAL,verdict TEXT NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v671_family_champions(
      family TEXT PRIMARY KEY,status TEXT NOT NULL,positive_instances INTEGER NOT NULL,
      positive_regimes INTEGER NOT NULL,independent_regimes INTEGER NOT NULL,unique_holdout_tokens INTEGER NOT NULL,
      median_expectancy REAL,worst_expectancy REAL,median_pf REAL,worst_pf REAL,median_win REAL,
      median_lift REAL,median_fill_rate REAL,worst_fill_rate REAL,max_pair_overlap REAL,
      direction_consistency REAL,threshold_cv REAL,representative_experiment_id TEXT,
      benchmark_expectancy REAL,benchmark_pf REAL,benchmark_done INTEGER,
      evidence_json TEXT NOT NULL,updated_at REAL NOT NULL);
    '''); d.commit(); d.close()

def load_rows(ep):
    d=core.open_research(); rows=[dict(r) for r in d.execute('''
      SELECT e.*,r.* FROM v67_experiments e JOIN v67_results r USING(experiment_id)
      WHERE e.epoch_id=? AND e.status='DONE' AND r.verdict IN ('PROMISING','WEAK')
    ''',(ep['epoch_id'],)).fetchall()]; d.close(); return rows

def selected_holdout_tokens(ep,r):
    if r.get('threshold') is None or r.get('direction') is None:return []
    data,_=v67.dataset(ep,r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct'],r['feature'])
    out=[]; direction=float(r['direction']); th=float(r['threshold'])
    for x in data:
        if not v67.v63.holdout(x['token_mint']):continue
        if direction*float(x['feature'])>=th:out.append(str(x['token_mint']))
    return sorted(set(out))

def rebuild_instances(ep):
    rows=load_rows(ep); now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        d.execute('DELETE FROM v671_edge_instances')
        for r in rows:
            toks=selected_holdout_tokens(ep,r)
            d.execute('''INSERT INTO v671_edge_instances VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(
              r['experiment_id'],ep['epoch_id'],v61.FAMILY.get(r['feature'],'OTHER'),r['feature'],r['stage_s'],r['horizon_s'],
              r['tp_pct'],r['sl_pct'],r['direction'],r['threshold'],r['threshold_q'],len(toks),core.canonical_json(toks),
              r['holdout_expectancy'],r['holdout_pf'],r['holdout_win'],r['expectancy_lift'],r['fill_rate'],r['median_fill_delay'],r['verdict'],now))
        d.commit()
    except BaseException:
        d.rollback(); raise
    finally:d.close()
    return len(rows)

def independent_subset(rows):
    ordered=sorted(rows,key=lambda r:(sf(r['holdout_expectancy'],-1e9),sf(r['holdout_pf'],0),r['holdout_selected']),reverse=True)
    kept=[]
    for r in ordered:
        toks=json.loads(r['selected_tokens_json'])
        if all(jaccard(toks,json.loads(k['selected_tokens_json']))<MAX_OVERLAP for k in kept): kept.append(r)
    return kept

def r64_post_benchmark():
    wm,_=cohort.infer_watermark(); d=core.open_research()
    rule=d.execute('SELECT rule_id FROM v64_frozen_rule LIMIT 1').fetchone()
    if not rule: d.close(); return {'done':0,'expectancy':None,'pf':None}
    xs=[float(r[0]) for r in d.execute('''SELECT net_return FROM v64_forward_events
      WHERE rule_id=? AND cutoff_ts>? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts,token_mint''',(rule[0],wm)).fetchall()]
    d.close(); return {'done':len(xs),'expectancy':statistics.mean(xs) if xs else None,'pf':pf(xs) if xs else None,'dd':dd(xs) if xs else None}

def threshold_cv(rows):
    vals=[abs(float(r['threshold'])) for r in rows if sf(r['threshold']) is not None]
    if len(vals)<2:return 0.0
    m=statistics.mean(vals)
    return statistics.pstdev(vals)/m if m>0 else 0.0

def rebuild_families(bench):
    d=core.open_research(); rows=[dict(r) for r in d.execute('SELECT * FROM v671_edge_instances').fetchall()]; d.close()
    by=defaultdict(list)
    for r in rows:by[r['family']].append(r)
    now=time.time(); out=[]; d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        d.execute('DELETE FROM v671_family_champions')
        for fam,rs in by.items():
            pos=[r for r in rs if r['holdout_selected']>=MIN_HO_N and sf(r['holdout_expectancy'],-1)>0 and sf(r['holdout_pf'],0)>1 and sf(r['expectancy_lift'],-1)>0 and sf(r['fill_rate'],0)>=MIN_FILL_RATE]
            indep=independent_subset(pos); toks=set()
            for r in indep:toks.update(json.loads(r['selected_tokens_json']))
            exps=[float(r['holdout_expectancy']) for r in indep]; pfs=[float(r['holdout_pf']) for r in indep]
            wins=[sf(r['holdout_win']) for r in indep if sf(r['holdout_win']) is not None]
            lifts=[sf(r['expectancy_lift']) for r in indep if sf(r['expectancy_lift']) is not None]
            fills=[sf(r['fill_rate']) for r in indep if sf(r['fill_rate']) is not None]
            dirs=[1 if float(r['direction'])>0 else -1 for r in indep]
            dir_cons=abs(sum(dirs))/len(dirs) if dirs else 0.0
            maxov=0.0
            for i in range(len(pos)):
                a=json.loads(pos[i]['selected_tokens_json'])
                for j in range(i+1,len(pos)):maxov=max(maxov,jaccard(a,json.loads(pos[j]['selected_tokens_json'])))
            regimes={(r['stage_s'],r['horizon_s'],r['tp_pct'],r['sl_pct']) for r in pos}
            status='DISCOVERY'
            if len(indep)>=MIN_REGIMES and len(toks)>=MIN_UNIQUE_TOKENS and exps and min(exps)>0 and pfs and min(pfs)>1:
                status='REPLICATED'
            medexp=statistics.median(exps) if exps else None; medpf=statistics.median(pfs) if pfs else None
            beats=(medexp is not None and bench['expectancy'] is not None and medexp>bench['expectancy'] and medpf is not None and bench['pf'] is not None and medpf>bench['pf'])
            if status=='REPLICATED' and len(indep)>=3 and len(toks)>=18 and dir_cons>=0.67 and beats:
                status='CHALLENGER_READY'
            rep=max(pos,key=lambda r:(sf(r['holdout_expectancy'],-1e9),sf(r['holdout_pf'],0),r['holdout_selected']))['experiment_id'] if pos else None
            ev={'instances':[r['experiment_id'] for r in pos],'independent_instances':[r['experiment_id'] for r in indep],
                'regimes':[list(x) for x in sorted(regimes)],'unique_tokens':len(toks),'max_overlap':maxov,
                'direction_consistency':dir_cons,'threshold_cv':threshold_cv(indep),'beats_r64_post_median':beats}
            vals=(fam,status,len(pos),len(regimes),len(indep),len(toks),medexp,min(exps) if exps else None,
                  medpf,min(pfs) if pfs else None,statistics.median(wins) if wins else None,statistics.median(lifts) if lifts else None,
                  statistics.median(fills) if fills else None,min(fills) if fills else None,maxov,dir_cons,threshold_cv(indep),rep,
                  bench['expectancy'],bench['pf'],bench['done'],core.canonical_json(ev),now)
            d.execute('INSERT INTO v671_family_champions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',vals); out.append(vals)
        d.commit()
    except BaseException:
        d.rollback(); raise
    finally:d.close()
    return len(by)

def display(ep,ninst,nfam,bench):
    d=core.open_research(); rows=[dict(r) for r in d.execute('''SELECT * FROM v671_family_champions ORDER BY
      CASE status WHEN 'CHALLENGER_READY' THEN 0 WHEN 'REPLICATED' THEN 1 ELSE 2 END,
      median_expectancy DESC,median_pf DESC''').fetchall()]; d.close()
    print('='*188); print('MEMECOIN LAB — POST-CANONICAL CHALLENGER CONSOLIDATOR V6.7.1'); print('='*188)
    print(f"EPOCH={ep['epoch_id']} | edge_instances={ninst} families={nfam}")
    print(f"R64 POST benchmark: DONE={bench['done']} exp={sf(bench['expectancy'],0):+.2f}% PF={sf(bench['pf'],0):.2f} DD={sf(bench.get('dd'),0):+.2f}%")
    print(f"Gates: HO_n>={MIN_HO_N} overlap<{MAX_OVERLAP:.0%} independent_regimes>={MIN_REGIMES} unique_tokens>={MIN_UNIQUE_TOKENS}; no threshold retuning.\n")
    for i,r in enumerate(rows,1):
        print(f"#{i:02d} {r['status']:<17} {r['family']:<22} pos={r['positive_instances']:<3} regimes={r['positive_regimes']:<3} independent={r['independent_regimes']:<2} unique={r['unique_holdout_tokens']:<3}")
        print(f"     exp med={sf(r['median_expectancy'],0):+.2f}% worst={sf(r['worst_expectancy'],0):+.2f}% | PF med={sf(r['median_pf'],0):.2f} worst={sf(r['worst_pf'],0):.2f} | fill med={100*sf(r['median_fill_rate'],0):.1f}%")
        print(f"     dir_cons={100*sf(r['direction_consistency'],0):.0f}% threshold_cv={100*sf(r['threshold_cv'],0):.1f}% overlap={100*sf(r['max_pair_overlap'],0):.1f}% | rep={r['representative_experiment_id']}")
    print('\nGuardrail: CHALLENGER_READY means historical post-canonical replication beating the R64 POST benchmark on median expectancy/PF. It still requires a fresh future-only arena.')

def main():
    init(); ep=epoch(); n=rebuild_instances(ep); bench=r64_post_benchmark(); f=rebuild_families(bench); display(ep,n,f,bench)

if __name__=='__main__':main()
