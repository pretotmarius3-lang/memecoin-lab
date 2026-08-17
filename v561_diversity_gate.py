#!/usr/bin/env python3
"""Memecoin Lab V5.6.1 — Diversity Gate.

Prevents future SIDE research capacity from being wasted on scientifically
redundant hypotheses while preserving all completed/frozen evidence.

The gate NEVER edits V5.5 frozen rules, observations, beliefs, or verdicts.
It only changes *READY* V4.9 experiments to SKIPPED_DIVERSITY when an equivalent
hypothesis is already adequately represented.

Policy:
- same scientific hypothesis already frozen in V5.5 -> block new READY duplicate
- same hypothesis already DONE in V4.9 -> block unless watermark advanced enough
  to qualify as a genuine temporal refresh
- several simultaneous READY duplicates -> keep one representative, skip the rest
- materially newer watermark -> allow as REFRESH_RETEST

Research-only. No trading/signing.
"""
from __future__ import annotations

import json, math, os, signal, time
from collections import defaultdict

import v41_core as core

LOOP=float(os.environ.get('MEMECOIN_V561_LOOP_S','0.5'))
MIN_ABS_ADVANCE=int(os.environ.get('MEMECOIN_V561_MIN_WATERMARK_ADVANCE','250'))
MIN_REL_ADVANCE=float(os.environ.get('MEMECOIN_V561_MIN_WATERMARK_REL_ADVANCE','0.20'))
STOP=False


def stop(*_):
    global STOP; STOP=True


def canonical_spec(kind,s):
    """Scientific identity independent of parent lineage and data watermark."""
    out={
        'kind':str(kind),
        'stage':s.get('stage'),
        'stage1':s.get('stage1'),
        'stage2':s.get('stage2'),
        'horizon':s.get('horizon'),
        'target':s.get('target'),
        'feature':s.get('feature'),
        'weak':s.get('weak'),
        'gate':s.get('gate'),
    }
    fs=s.get('features')
    if fs is not None:
        out['features']=sorted(str(x) for x in fs)
    return out


def scientific_key(kind,spec):
    return core.fingerprint(canonical_spec(kind,spec),'v561science:')[:28]


def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v561_diversity_decisions(
      experiment_id TEXT PRIMARY KEY,
      scientific_key TEXT NOT NULL,
      decision TEXT NOT NULL,
      representative_id TEXT,
      reason TEXT NOT NULL,
      watermark_n INTEGER NOT NULL,
      representative_watermark INTEGER,
      estimated_rows_avoided INTEGER NOT NULL DEFAULT 0,
      decided_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v561_key ON v561_diversity_decisions(scientific_key);
    CREATE INDEX IF NOT EXISTS idx_v561_decision ON v561_diversity_decisions(decision);

    CREATE TABLE IF NOT EXISTS v561_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    '''); d.commit(); d.close()


def frozen_keys(d):
    names={r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'v55_candidates' not in names:return {}
    out={}
    for r in d.execute("SELECT candidate_id,kind,spec_json,state FROM v55_candidates"):
        try:k=scientific_key(r['kind'],json.loads(r['spec_json']))
        except Exception:continue
        # One frozen candidate is enough to stop a parallel exact duplicate.
        out.setdefault(k,(r['candidate_id'],r['state']))
    return out


def done_representatives(d):
    names={r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'v49_side_experiments' not in names:return {}
    has_results='v49_side_results' in names
    if has_results:
        rows=d.execute('''SELECT e.*,r.holdout_rho,r.comparison
                          FROM v49_side_experiments e
                          LEFT JOIN v49_side_results r USING(experiment_id)
                          WHERE e.status='DONE' ''').fetchall()
    else:
        rows=d.execute("SELECT e.*,NULL holdout_rho,NULL comparison FROM v49_side_experiments e WHERE e.status='DONE'").fetchall()
    groups=defaultdict(list)
    for r in rows:
        try:k=scientific_key(r['kind'],json.loads(r['spec_json']))
        except Exception:continue
        groups[k].append(dict(r))
    reps={}
    for k,g in groups.items():
        # Prefer the most recent/largest data watermark; tie-break on useful result.
        reps[k]=max(g,key=lambda x:(int(x['watermark_n']), x.get('comparison')=='IMPROVED', float(x.get('holdout_rho') or -9)))
    return reps


def enough_new_data(old_wm,new_wm):
    old=max(0,int(old_wm or 0)); new=max(0,int(new_wm or 0)); delta=new-old
    need=max(MIN_ABS_ADVANCE,int(math.ceil(old*MIN_REL_ADVANCE)))
    return delta>=need,delta,need


def record(d,eid,key,decision,rep,reason,wm,repwm,avoided,now):
    d.execute('''INSERT INTO v561_diversity_decisions(
      experiment_id,scientific_key,decision,representative_id,reason,watermark_n,
      representative_watermark,estimated_rows_avoided,decided_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(experiment_id) DO UPDATE SET scientific_key=excluded.scientific_key,
      decision=excluded.decision,representative_id=excluded.representative_id,
      reason=excluded.reason,watermark_n=excluded.watermark_n,
      representative_watermark=excluded.representative_watermark,
      estimated_rows_avoided=excluded.estimated_rows_avoided,updated_at=excluded.updated_at''',
      (eid,key,decision,rep,reason,int(wm),repwm,int(avoided),now,now))


def cycle():
    d=core.open_research(); names={r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'v49_side_experiments' not in names:
        d.close(); return {'ready':0,'skipped':0,'allowed':0,'refresh':0,'frozen_block':0,'rows_avoided':0}

    fkeys=frozen_keys(d); done=done_representatives(d)
    ready=[dict(r) for r in d.execute("SELECT * FROM v49_side_experiments WHERE status='READY' ORDER BY created_at,experiment_id").fetchall()]

    # Group simultaneous READY experiments by scientific identity.
    rgroups=defaultdict(list)
    for r in ready:
        try:k=scientific_key(r['kind'],json.loads(r['spec_json']))
        except Exception:continue
        r['_key']=k; rgroups[k].append(r)

    decisions=[]; now=time.time(); stats={'ready':len(ready),'skipped':0,'allowed':0,'refresh':0,'frozen_block':0,'rows_avoided':0}
    for key,g in rgroups.items():
        # Keep the highest-watermark READY item as the simultaneous representative.
        simultaneous=max(g,key=lambda x:(int(x['watermark_n']),-float(x['created_at'])))
        for r in g:
            eid=r['experiment_id']; wm=int(r['watermark_n'])
            if key in fkeys:
                rep,state=fkeys[key]
                decisions.append((r,'BLOCK_FROZEN',rep,f'equivalent hypothesis already frozen in V5.5 ({state})',None,wm))
                stats['frozen_block']+=1
                continue
            old=done.get(key)
            if old is not None:
                ok,delta,need=enough_new_data(old['watermark_n'],wm)
                if ok:
                    decisions.append((r,'REFRESH_RETEST',old['experiment_id'],f'new watermark +{delta} >= refresh requirement {need}',int(old['watermark_n']),0))
                    stats['refresh']+=1; stats['allowed']+=1
                else:
                    decisions.append((r,'BLOCK_DONE_DUPLICATE',old['experiment_id'],f'equivalent DONE experiment; watermark advance +{delta} < {need}',int(old['watermark_n']),wm))
                continue
            if eid!=simultaneous['experiment_id']:
                decisions.append((r,'BLOCK_PARALLEL_DUPLICATE',simultaneous['experiment_id'],'parallel READY experiment has identical scientific identity',int(simultaneous['watermark_n']),wm))
            else:
                decisions.append((r,'ALLOW_NOVEL',None,'no frozen or adequately tested equivalent found',None,0)); stats['allowed']+=1

    d.execute('BEGIN IMMEDIATE')
    try:
        for r,decision,rep,reason,repwm,avoided in decisions:
            if decision.startswith('BLOCK_'):
                cur=d.execute("UPDATE v49_side_experiments SET status='SKIPPED_DIVERSITY',updated_at=? WHERE experiment_id=? AND status='READY'",(now,r['experiment_id']))
                if cur.rowcount:
                    stats['skipped']+=1; stats['rows_avoided']+=int(avoided)
            record(d,r['experiment_id'],r['_key'],decision,rep,reason,r['watermark_n'],repwm,avoided if decision.startswith('BLOCK_') else 0,now)

        # Global diversity metrics over all side experiments.
        allrows=d.execute("SELECT kind,spec_json FROM v49_side_experiments").fetchall()
        keys=set()
        for x in allrows:
            try:keys.add(scientific_key(x['kind'],json.loads(x['spec_json'])))
            except Exception:pass
        total=len(allrows); diversity=(len(keys)/total) if total else 1.0
        historical_skipped=d.execute("SELECT COUNT(*) FROM v49_side_experiments WHERE status='SKIPPED_DIVERSITY'").fetchone()[0]
        saved_rows=d.execute("SELECT COALESCE(SUM(estimated_rows_avoided),0) FROM v561_diversity_decisions WHERE decision LIKE 'BLOCK_%'").fetchone()[0]
        state=dict(stats,total_experiments=total,scientific_identities=len(keys),diversity_ratio=diversity,historical_skipped=int(historical_skipped),estimated_rows_avoided_total=int(saved_rows))
        d.execute('''INSERT INTO v561_state(key,value_json,updated_at) VALUES('latest',?,?)
                     ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at''',(core.canonical_json(state),now))
        d.commit()
    except BaseException:
        d.rollback(); raise
    finally:d.close()
    return state


def display(s):
    d=core.open_research()
    top=[dict(r) for r in d.execute('''SELECT scientific_key,COUNT(*) n,
          SUM(CASE WHEN decision LIKE 'BLOCK_%' THEN 1 ELSE 0 END) blocked,
          MAX(representative_id) representative
          FROM v561_diversity_decisions GROUP BY scientific_key
          HAVING COUNT(*)>1 ORDER BY blocked DESC,n DESC LIMIT 8''').fetchall()]
    recent=[dict(r) for r in d.execute("SELECT * FROM v561_diversity_decisions ORDER BY updated_at DESC LIMIT 8").fetchall()]
    d.close()
    print('\033[2J\033[H',end='')
    print('='*132)
    print('MEMECOIN LAB — DIVERSITY GATE V5.6.1')
    print('='*132)
    print(f"READY_SEEN={s.get('ready',0):,} | ALLOWED={s.get('allowed',0):,} | SKIPPED_NOW={s.get('skipped',0):,} | REFRESH_RETEST={s.get('refresh',0):,} | FROZEN_BLOCK={s.get('frozen_block',0):,}")
    print(f"TOTAL_SIDE={s.get('total_experiments',0):,} | SCIENTIFIC_IDENTITIES={s.get('scientific_identities',0):,} | DIVERSITY={100*s.get('diversity_ratio',0):.1f}% | SKIPPED_TOTAL={s.get('historical_skipped',0):,}")
    print(f"ESTIMATED_ROWS_OF_RESEARCH_AVOIDED={s.get('estimated_rows_avoided_total',0):,}")
    print('\nRECENT GATE DECISIONS')
    for r in recent:
        print(f"{r['decision']:<26} wm={r['watermark_n']:<6} exp={r['experiment_id'][:16]} rep={(r['representative_id'] or '—')[:16]}  {r['reason'][:64]}")
    print('\nMOST CROWDED SCIENTIFIC IDENTITIES')
    for r in top:
        print(f"{r['scientific_key']}  observations={r['n']:<4} blocked={r['blocked'] or 0:<4} representative={(r['representative'] or '—')[:18]}")
    print('\nGuardrail: only READY exploratory SIDE experiments can be skipped. DONE results and every V5.5 frozen/prospective record remain untouched.')


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:display(cycle())
        except Exception as e:print('V5.6.1 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
