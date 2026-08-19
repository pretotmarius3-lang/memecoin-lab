#!/usr/bin/env python3
"""MEMECOIN LAB — MIGRATION CENSUS V7.7.0

READ-ONLY census for PRE-MIGRATION / MIGRATION / POST-MIGRATION research.

Goals:
- inventory what migration-like evidence already exists in v5_raw_events.db
- count CREATE tokens and candidate migration markers
- estimate CREATE->migration delays where a conservative marker can be matched
- report how many tokens have causal T10/T20/T30 snapshots and post-event coverage

IMPORTANT:
- This is a discovery / data-coverage tool, NOT a trading rule.
- Migration detection here is conservative and explicitly labeled HEURISTIC unless the
  local dataset exposes an exact migration event/type field.
- No DB mutation, no queue mutation, no strategy retuning.
"""
from __future__ import annotations

import json
import math
import sqlite3
import statistics
from pathlib import Path

ROOT = Path.home()/"memecoin_lab"
RAW = ROOT/'v5_raw_events.db'
FEATURE = ROOT/'v52_features.db'

MIGRATION_TERMS = (
    'migrate','migration','migrated','complete','completed',
    'pumpswap','raydium','bonding curve complete','bonding_curve_complete',
    'initialize2','create_pool','pool created','pool_created'
)


def ro(path: Path):
    d=sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def tables(d):
    return [r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]


def cols(d,t):
    return [r[1] for r in d.execute(f'PRAGMA table_info("{t}")')]


def sf(x):
    try:
        z=float(x)
        return z if math.isfinite(z) else None
    except Exception:
        return None


def pct(xs,q):
    if not xs:return None
    ys=sorted(xs); p=(len(ys)-1)*q; lo=int(p); hi=min(len(ys)-1,lo+1); f=p-lo
    return ys[lo]+(ys[hi]-ys[lo])*f


def fmt(x):
    return 'None' if x is None else f'{x:.2f}'


def inventory():
    d=ro(RAW)
    out=[]
    for t in tables(d):
        c=cols(d,t)
        lc={x.lower() for x in c}
        score=0
        if {'mint','token_mint'} & lc: score+=2
        if {'kind','type','event','event_type','source'} & lc: score+=2
        if {'logs_json','logs','raw_json','payload','data'} & lc: score+=2
        if {'timestamp','block_time','first_seen','observed_at','created_at','updated_at'} & lc: score+=1
        if score>=3:
            out.append((score,t,c))
    d.close()
    return sorted(out,reverse=True)


def create_cohort():
    d=ro(RAW)
    ts=tables(d)
    rows=[]
    if 'v515_hot_queue' in ts:
        c=cols(d,'v515_hot_queue')
        if all(x in c for x in ('mint','kind','first_seen')):
            q="SELECT mint,MIN(first_seen) AS ts FROM v515_hot_queue WHERE kind='CREATE' AND mint IS NOT NULL GROUP BY mint"
            rows=[(str(r['mint']),float(r['ts'])) for r in d.execute(q).fetchall() if r['ts'] is not None]
    d.close()
    return rows


def exact_field_candidates():
    """Find rows in explicit event/type/kind/source fields that contain migration terms."""
    d=ro(RAW); hits=[]
    for t in tables(d):
        c=cols(d,t); lc={x.lower():x for x in c}
        mintcol=lc.get('mint') or lc.get('token_mint')
        timecol=next((lc[x] for x in ('timestamp','block_time','observed_at','first_seen','created_at','updated_at') if x in lc),None)
        for key in ('event_type','event','type','kind','source'):
            col=lc.get(key)
            if not col: continue
            try:
                vals=d.execute(f'SELECT "{col}" v,COUNT(*) n FROM "{t}" WHERE "{col}" IS NOT NULL GROUP BY "{col}" ORDER BY n DESC LIMIT 200').fetchall()
            except sqlite3.Error:
                continue
            for r in vals:
                v=str(r['v']); low=v.lower()
                if any(term in low for term in MIGRATION_TERMS):
                    hits.append({'table':t,'field':col,'value':v,'n':int(r['n']),'mintcol':mintcol,'timecol':timecol})
    d.close(); return hits


def heuristic_log_candidates(limit_per_table=5000):
    """Search JSON/log text only after explicit-field search; labels remain heuristic."""
    d=ro(RAW); out=[]
    for _,t,c in inventory():
        lc={x.lower():x for x in c}
        mintcol=lc.get('mint') or lc.get('token_mint')
        timecol=next((lc[x] for x in ('timestamp','block_time','observed_at','first_seen','created_at','updated_at') if x in lc),None)
        textcols=[lc[x] for x in ('logs_json','logs','raw_json','payload','data') if x in lc]
        if not mintcol or not timecol or not textcols: continue
        for tc in textcols:
            clauses=' OR '.join([f'LOWER(CAST("{tc}" AS TEXT)) LIKE ?' for _ in MIGRATION_TERMS])
            params=[f'%{term}%' for term in MIGRATION_TERMS]
            q=f'''SELECT "{mintcol}" mint,"{timecol}" ts,"{tc}" txt FROM "{t}"
                  WHERE "{mintcol}" IS NOT NULL AND "{timecol}" IS NOT NULL AND ({clauses})
                  ORDER BY "{timecol}" DESC LIMIT {int(limit_per_table)}'''
            try: rs=d.execute(q,params).fetchall()
            except sqlite3.Error: continue
            for r in rs:
                out.append({'table':t,'field':tc,'mint':str(r['mint']),'ts':sf(r['ts']),'txt':str(r['txt'])[:500]})
    d.close(); return out


def migration_events(exact_hits, log_hits):
    events=[]
    d=ro(RAW)
    for h in exact_hits:
        if not h['mintcol'] or not h['timecol']: continue
        try:
            rs=d.execute(f'''SELECT "{h['mintcol']}" mint,"{h['timecol']}" ts
                             FROM "{h['table']}" WHERE "{h['field']}"=? AND "{h['mintcol']}" IS NOT NULL AND "{h['timecol']}" IS NOT NULL''',(h['value'],)).fetchall()
        except sqlite3.Error: continue
        for r in rs:
            ts=sf(r['ts'])
            if ts is not None: events.append((str(r['mint']),ts,'EXACT_FIELD',f"{h['table']}.{h['field']}={h['value']}"))
    d.close()
    for r in log_hits:
        if r['ts'] is not None: events.append((r['mint'],float(r['ts']),'HEURISTIC_LOG',f"{r['table']}.{r['field']}"))
    # earliest candidate event per mint, preferring exact if tied/available
    by={}
    for mint,ts,quality,src in events:
        z=by.get(mint)
        rank=0 if quality=='EXACT_FIELD' else 1
        if z is None or rank<z[2] or (rank==z[2] and ts<z[0]): by[mint]=(ts,quality,rank,src)
    return {m:(v[0],v[1],v[3]) for m,v in by.items()}


def feature_coverage(mints):
    if not mints or not FEATURE.exists(): return {'t10':0,'t20':0,'t30':0,'all':0}
    d=ro(FEATURE); qs=','.join('?' for _ in mints)
    try:
        rows=d.execute(f'''SELECT token_mint,stage_s FROM v7611_causal_snapshots
                           WHERE token_mint IN ({qs}) AND stage_s IN (10,20,30)''',mints).fetchall()
    except sqlite3.Error:
        d.close(); return {'t10':0,'t20':0,'t30':0,'all':0}
    d.close(); by={}
    for r in rows: by.setdefault(str(r['token_mint']),set()).add(int(r['stage_s']))
    return {
        't10':sum(10 in s for s in by.values()),
        't20':sum(20 in s for s in by.values()),
        't30':sum(30 in s for s in by.values()),
        'all':sum({10,20,30}.issubset(s) for s in by.values()),
    }


def main():
    print('='*132)
    print('MEMECOIN LAB — MIGRATION CENSUS V7.7.0')
    print('='*132)
    print('READ-ONLY | migration markers are EXACT only when found in explicit event/type fields; log matches remain HEURISTIC')

    inv=inventory()
    print('\nCANDIDATE RAW TABLES')
    for score,t,c in inv[:20]:
        print(f'{t:<42} score={score} cols={",".join(c[:14])}{"..." if len(c)>14 else ""}')

    creates=create_cohort()
    print(f'\nCREATE COHORT unique_tokens={len(creates)}')

    exact=exact_field_candidates()
    print('\nEXPLICIT MIGRATION-LIKE FIELD VALUES')
    if not exact: print('  none found')
    for h in exact[:30]:
        print(f"  {h['table']}.{h['field']}={h['value']!r} n={h['n']} mint={h['mintcol']} time={h['timecol']}")

    logs=heuristic_log_candidates()
    print(f'\nHEURISTIC LOG MATCHES rows={len(logs)}')
    bysrc={}
    for r in logs: bysrc[(r['table'],r['field'])]=bysrc.get((r['table'],r['field']),0)+1
    for (t,f),n in sorted(bysrc.items(),key=lambda x:-x[1])[:20]: print(f'  {t}.{f}: {n}')

    mig=migration_events(exact,logs)
    create_map=dict(creates)
    matched=[]
    for mint,(mts,q,src) in mig.items():
        cts=create_map.get(mint)
        if cts is None: continue
        dt=mts-cts
        if dt>=0: matched.append((mint,dt,q,src))
    delays=[x[1] for x in matched]
    exact_n=sum(x[2]=='EXACT_FIELD' for x in matched)
    heur_n=sum(x[2]=='HEURISTIC_LOG' for x in matched)
    print('\nCREATE -> MIGRATION CANDIDATE MATCHING')
    print(f'matched={len(matched)} exact={exact_n} heuristic={heur_n} migration_rate_candidate={(100*len(matched)/len(creates)) if creates else 0:.2f}%')
    if delays:
        print(f'delay_s p10/p50/p90/p95={fmt(pct(delays,.10))}/{fmt(pct(delays,.50))}/{fmt(pct(delays,.90))}/{fmt(pct(delays,.95))} min/max={fmt(min(delays))}/{fmt(max(delays))}')

    cov=feature_coverage([x[0] for x in matched])
    print('\nCAUSAL SNAPSHOT COVERAGE AMONG MATCHED')
    print(f"T10={cov['t10']} T20={cov['t20']} T30={cov['t30']} all_T10_T20_T30={cov['all']}")

    print('\nMIGRATION MARKER QUALITY GATE')
    if exact_n>=20:
        print('STATUS=EXACT_MARKER_AVAILABLE')
        print('Next: build V7.7.1 exact pre/post migration path profiler using ONLY exact migration rows.')
    elif len(matched)>=20:
        print('STATUS=HEURISTIC_MARKER_ONLY')
        print('Do NOT use as alpha evidence yet. Next: inspect signatures/log patterns and promote only a verified exact marker.')
    else:
        print('STATUS=INSUFFICIENT_MIGRATION_MARKERS')
        print('Next: add/identify a canonical migration detector before any pre/post alpha study.')

if __name__=='__main__':
    main()
