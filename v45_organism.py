#!/usr/bin/env python3
"""Memecoin Lab V4.5 — scientific learning organism.

Extends V4.4 with a real evidence-to-hypothesis loop:
- aggregates repeated LIVE_RESEARCH results across epochs
- estimates per-result significance from holdout Spearman rho
- applies Benjamini-Hochberg FDR correction inside each context/epoch
- writes explicit scientific conclusions
- detects stable signals, weak candidates, contradictions, and no-edge features
- generates learned pairwise hypotheses from stable signals
- generates regime hypotheses when a signal changes sign across epochs
- runs those follow-up experiments through the normal worker pool

Frozen historical candidates are never modified here. LIVE_SCIENCE remains exploratory
until a dedicated prospective scorer validates hypotheses on genuinely unseen tokens.
Research-only. No signing. No trading.
"""
from __future__ import annotations

import itertools
import json
import math
import multiprocessing as mp
import os
import signal
import statistics
import time
import traceback
from collections import defaultdict

import v41_core as core
import v41_engine as base
import v41_organism as old
import v42_organism as v42
import v43_organism as v43
import v44_organism as v44

CPU = os.cpu_count() or 4
WORKERS = int(os.environ.get("MEMECOIN_V45_WORKERS", str(min(10, max(4, CPU // 2)))))
IDLE_SLEEP = 0.25
LOOP_SLEEP = 1.0
MAX_PAIRWISE_PER_TICK = int(os.environ.get("MEMECOIN_V45_MAX_PAIRWISE", "18"))
MAX_REGIME_PER_TICK = int(os.environ.get("MEMECOIN_V45_MAX_REGIME", "8"))
MIN_EPOCHS_CANDIDATE = int(os.environ.get("MEMECOIN_V45_MIN_EPOCHS", "2"))
STOP = False


def stop_handler(*_):
    global STOP
    STOP = True


def median(xs):
    xs = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return statistics.median(xs) if xs else None


def approx_corr_p(rho, n):
    """Two-sided Fisher-z normal approximation; adequate for ranking/FDR gates."""
    try:
        r = max(-0.999999, min(0.999999, float(rho)))
        n = int(n)
    except Exception:
        return None
    if n < 8:
        return None
    z = math.atanh(r) * math.sqrt(max(1.0, n - 3.0))
    return math.erfc(abs(z) / math.sqrt(2.0))


def bh_adjust(items):
    """items=[(key,p)] -> dict key:q using Benjamini-Hochberg."""
    valid = [(k, float(p)) for k, p in items if p is not None and math.isfinite(float(p))]
    m = len(valid)
    if not m:
        return {}
    ordered = sorted(valid, key=lambda x: x[1])
    raw = [min(1.0, p * m / i) for i, (_, p) in enumerate(ordered, 1)]
    for i in range(m - 2, -1, -1):
        raw[i] = min(raw[i], raw[i + 1])
    return {ordered[i][0]: raw[i] for i in range(m)}


def init_v45():
    db = core.open_research()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v45_evidence (
      result_id TEXT PRIMARY KEY,
      context_key TEXT NOT NULL,
      epoch INTEGER NOT NULL,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      target TEXT NOT NULL,
      feature TEXT NOT NULL,
      rho REAL,
      effect REAL,
      holdout_n INTEGER,
      approx_p REAL,
      fdr_q REAL,
      verdict TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v45_ev_context ON v45_evidence(context_key,feature,epoch);

    CREATE TABLE IF NOT EXISTS v45_conclusions (
      conclusion_key TEXT PRIMARY KEY,
      context_key TEXT NOT NULL,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      target TEXT NOT NULL,
      feature TEXT NOT NULL,
      classification TEXT NOT NULL,
      epochs INTEGER NOT NULL,
      median_rho REAL,
      sign_rate REAL,
      median_effect REAL,
      q_pass_rate REAL,
      latest_rho REAL,
      latest_q REAL,
      confidence REAL NOT NULL,
      conclusion TEXT NOT NULL,
      evidence_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v45_conclusion_class ON v45_conclusions(classification,confidence DESC);

    CREATE TABLE IF NOT EXISTS v45_science_state (
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL);
    """)
    db.commit(); db.close()


def refresh_evidence():
    db = core.open_research()
    rows = db.execute("""SELECT r.result_id,r.verdict,r.primary_metric,r.effect_size,r.holdout_n,r.created_at,
                                h.spec_json
                         FROM v41_results r
                         JOIN v41_hypotheses h ON h.hypothesis_id=r.hypothesis_id
                         WHERE h.branch='LIVE_RESEARCH' AND r.stage='EXPLORATORY_LIVE'
                         ORDER BY r.created_at""").fetchall()
    parsed = []
    by_epoch = defaultdict(list)
    for row in rows:
        try:
            spec = json.loads(row['spec_json'])
            stage = int(spec['stage_s']); horizon = int(spec['horizon_s'])
            target = str(spec['target']); feature = str(spec['feature']); epoch = int(spec.get('epoch',0))
        except Exception:
            continue
        context = f"{stage}:{horizon}:{target}"
        rho = row['primary_metric']; n = row['holdout_n']; p = approx_corr_p(rho,n) if rho is not None else None
        item = dict(result_id=row['result_id'], verdict=row['verdict'], rho=rho, effect=row['effect_size'],
                    holdout_n=n, created_at=float(row['created_at']), stage=stage,horizon=horizon,target=target,
                    feature=feature,epoch=epoch,context=context,p=p)
        parsed.append(item); by_epoch[(context,epoch)].append(item)

    qmap = {}
    for _, items in by_epoch.items():
        qmap.update(bh_adjust([(x['result_id'],x['p']) for x in items]))

    now = time.time(); db.execute('BEGIN IMMEDIATE')
    try:
        for x in parsed:
            db.execute("""INSERT INTO v45_evidence(result_id,context_key,epoch,stage_s,horizon_s,target,feature,rho,effect,holdout_n,approx_p,fdr_q,verdict,created_at,updated_at)
                          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ON CONFLICT(result_id) DO UPDATE SET approx_p=excluded.approx_p,fdr_q=excluded.fdr_q,updated_at=excluded.updated_at""",
                       (x['result_id'],x['context'],x['epoch'],x['stage'],x['horizon'],x['target'],x['feature'],x['rho'],x['effect'],x['holdout_n'],x['p'],qmap.get(x['result_id']),x['verdict'],x['created_at'],now))
        db.commit()
    except BaseException:
        db.rollback(); raise
    finally:
        db.close()
    return len(parsed)


def classify_feature(items):
    items = sorted(items, key=lambda x: (x['epoch'], x['created_at']))
    rhos = [float(x['rho']) for x in items if x['rho'] is not None]
    effects = [float(x['effect']) for x in items if x['effect'] is not None]
    qs = [float(x['fdr_q']) for x in items if x['fdr_q'] is not None]
    epochs = len({int(x['epoch']) for x in items})
    if not rhos:
        return 'INSUFFICIENT', 0.0, {}
    med = statistics.median(rhos)
    sign_rate = sum(r > 0 for r in rhos) / len(rhos)
    med_eff = statistics.median(effects) if effects else 0.0
    q_pass = sum(q <= 0.10 for q in qs) / len(qs) if qs else 0.0
    latest = items[-1]
    latest_rho = float(latest['rho']) if latest['rho'] is not None else 0.0
    latest_q = float(latest['fdr_q']) if latest['fdr_q'] is not None else 1.0

    if epochs >= 3 and sign_rate >= 0.80 and med >= 0.10 and q_pass >= 0.50 and latest_rho >= 0.05:
        cls = 'CONFIRMED_SIGNAL'
    elif epochs >= MIN_EPOCHS_CANDIDATE and sign_rate >= 0.67 and med >= 0.07 and med_eff >= 2.0:
        cls = 'CANDIDATE_SIGNAL'
    elif epochs >= 3 and 0.25 < sign_rate < 0.75 and max(rhos) >= 0.08 and min(rhos) <= -0.05:
        cls = 'CONTRADICTORY'
    elif epochs >= 3 and sign_rate <= 0.20 and med <= -0.08:
        cls = 'NEGATIVE_SIGNAL'
    else:
        cls = 'NO_EDGE'

    consistency = abs(2.0 * sign_rate - 1.0)
    strength = min(1.0, abs(med) / 0.25)
    repeat = min(1.0, epochs / 5.0)
    multiplicity = min(1.0, q_pass + 0.25)
    confidence = max(0.0, min(1.0, 0.30*consistency + 0.30*strength + 0.20*repeat + 0.20*multiplicity))
    metrics = dict(epochs=epochs,median_rho=med,sign_rate=sign_rate,median_effect=med_eff,q_pass_rate=q_pass,
                   latest_rho=latest_rho,latest_q=latest_q,confidence=confidence)
    return cls, confidence, metrics


def derive_conclusions():
    db = core.open_research()
    rows = [dict(r) for r in db.execute("SELECT * FROM v45_evidence ORDER BY context_key,feature,epoch,created_at").fetchall()]
    groups = defaultdict(list)
    for r in rows: groups[(r['context_key'],r['feature'])].append(r)
    now = time.time(); counts = defaultdict(int)
    db.execute('BEGIN IMMEDIATE')
    try:
        for (context,feature), items in groups.items():
            cls, conf, m = classify_feature(items); counts[cls] += 1
            first = items[0]
            ck = f"{context}:{feature}"
            conclusion = (f"{feature} on {first['target']} at stage={first['stage_s']}s horizon={first['horizon_s']}s: "
                          f"{cls}; epochs={m.get('epochs',0)}, median_rho={m.get('median_rho')}, "
                          f"sign_rate={m.get('sign_rate')}, q_pass={m.get('q_pass_rate')}")
            evidence = {'result_ids':[x['result_id'] for x in items[-12:]],'epochs':[x['epoch'] for x in items[-12:]],
                        'rhos':[x['rho'] for x in items[-12:]],'q':[x['fdr_q'] for x in items[-12:]]}
            db.execute("""INSERT INTO v45_conclusions(conclusion_key,context_key,stage_s,horizon_s,target,feature,classification,epochs,median_rho,sign_rate,median_effect,q_pass_rate,latest_rho,latest_q,confidence,conclusion,evidence_json,updated_at)
                          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                          ON CONFLICT(conclusion_key) DO UPDATE SET classification=excluded.classification,epochs=excluded.epochs,
                            median_rho=excluded.median_rho,sign_rate=excluded.sign_rate,median_effect=excluded.median_effect,
                            q_pass_rate=excluded.q_pass_rate,latest_rho=excluded.latest_rho,latest_q=excluded.latest_q,
                            confidence=excluded.confidence,conclusion=excluded.conclusion,evidence_json=excluded.evidence_json,updated_at=excluded.updated_at""",
                       (ck,context,first['stage_s'],first['horizon_s'],first['target'],feature,cls,m.get('epochs',0),m.get('median_rho'),m.get('sign_rate'),m.get('median_effect'),m.get('q_pass_rate'),m.get('latest_rho'),m.get('latest_q'),conf,conclusion,core.canonical_json(evidence),now))
        db.commit()
    except BaseException:
        db.rollback(); raise
    finally: db.close()
    return dict(counts)


def live_multifeature_dataset(stage_s,horizon_s,target,features,max_rows=None):
    db = v44.open_v52()
    if db is None: return []
    cols = ','.join(f"s.{f} AS {f}" for f in features)
    notnull = ' AND '.join(f"s.{f} IS NOT NULL" for f in features)
    sql = f"""SELECT s.token_mint,s.cutoff_ts,{cols},o.{target} target
              FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
              WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND o.{target} IS NOT NULL AND {notnull}
              ORDER BY s.cutoff_ts,s.token_mint"""
    rows = db.execute(sql,(stage_s,horizon_s)).fetchall(); db.close()
    if max_rows is not None: rows = rows[:int(max_rows)]
    return [dict(r) | {'token_mint':str(r['token_mint']),'target':int(r['target'])} for r in rows]


def regime_test(rows,signal_feature,regime_feature):
    rows = [r for r in rows if old.valid(r.get(signal_feature)) and old.valid(r.get(regime_feature))]
    if len(rows) < 80:
        return 'COLLECT_MORE', {'n':len(rows),'signal_feature':signal_feature,'regime_feature':regime_feature}
    cut = statistics.median(float(r[regime_feature]) for r in rows)
    low = [dict(r, feature=r[signal_feature]) for r in rows if float(r[regime_feature]) <= cut]
    high = [dict(r, feature=r[signal_feature]) for r in rows if float(r[regime_feature]) > cut]
    vl, ml = base.evaluate_univariate(low,'feature'); vh, mh = base.evaluate_univariate(high,'feature')
    rl = ml.get('holdout_signed_rho'); rh = mh.get('holdout_signed_rho')
    gap = None if rl is None or rh is None else abs(float(rh)-float(rl))
    if gap is not None and gap >= 0.15 and max(float(rl),float(rh)) >= 0.10:
        verdict = 'PROMISING_REGIME'
    elif gap is not None and gap >= 0.08:
        verdict = 'WEAK_REGIME'
    else:
        verdict = 'REJECT_REGIME'
    return verdict, {'n':len(rows),'split_value':cut,'signal_feature':signal_feature,'regime_feature':regime_feature,
                     'low_verdict':vl,'high_verdict':vh,'low_rho':rl,'high_rho':rh,'rho_gap':gap,
                     'holdout_signed_rho':max([x for x in (rl,rh) if x is not None],default=None),
                     'qdiff_pp':max([x for x in (ml.get('qdiff_pp'),mh.get('qdiff_pp')) if x is not None],default=None)}


def run_payload(payload):
    adapter = payload.get('adapter')
    if adapter == 'v52_learned_pairwise':
        features = tuple(payload['features'])
        rows = live_multifeature_dataset(int(payload['stage_s']),int(payload['horizon_s']),payload['target'],features,payload.get('watermark_n'))
        verdict, metrics = old.evaluate_once(rows,features)
        metrics['research_mode']='LEARNED_PAIRWISE'; metrics['parent_conclusions']=payload.get('parent_conclusions',[])
        return verdict, metrics
    if adapter == 'v52_regime_probe':
        features=(payload['signal_feature'],payload['regime_feature'])
        rows=live_multifeature_dataset(int(payload['stage_s']),int(payload['horizon_s']),payload['target'],features,payload.get('watermark_n'))
        verdict,metrics=regime_test(rows,payload['signal_feature'],payload['regime_feature']); metrics['research_mode']='LEARNED_REGIME'
        return verdict,metrics
    return v44.run_payload(payload)


def finish_job(job,verdict,metrics):
    adapter=job['payload'].get('adapter')
    if adapter in ('v52_learned_pairwise','v52_regime_probe'):
        core.finish_job(job,verdict,'SCIENTIFIC_FOLLOWUP',metrics,
                        discovery_n=metrics.get('discovery_n'),holdout_n=metrics.get('holdout_n'),positives=metrics.get('positives'),
                        primary_metric=metrics.get('holdout_signed_rho'),effect_size=metrics.get('qdiff_pp'),
                        coverage={'n':metrics.get('n'),'prospective':False,'learned_hypothesis':True})
    else: v44.finish_job(job,verdict,metrics)


def worker_main(index):
    wid=f"ORG45-{index:02d}-{os.getpid()}"; core.worker_heartbeat(wid,'RUNNING')
    while True:
        job=base.safe_claim(wid)
        if job is None:
            core.worker_heartbeat(wid,'IDLE'); time.sleep(IDLE_SLEEP); continue
        core.worker_heartbeat(wid,'BUSY',job['job_id'])
        try:
            verdict,metrics=run_payload(job['payload']); finish_job(job,verdict,metrics); core.worker_heartbeat(wid,'RUNNING',done_inc=1)
        except KeyboardInterrupt: return
        except Exception:
            core.fail_job(job,traceback.format_exc()); core.worker_heartbeat(wid,'RUNNING',failed_inc=1)


def current_watermark(stage,horizon,target):
    n=v44.ready_count(stage,horizon,target)
    if n < v44.MIN_READY: return 0
    epoch=(n-v44.MIN_READY)//v44.EPOCH_STEP
    return v44.MIN_READY + epoch*v44.EPOCH_STEP


def seed_learned_hypotheses():
    db=core.open_research(); conclusions=[dict(r) for r in db.execute("SELECT * FROM v45_conclusions ORDER BY context_key,confidence DESC").fetchall()]; db.close()
    byctx=defaultdict(list)
    for c in conclusions: byctx[c['context_key']].append(c)
    made_pair=made_regime=0
    for context,items in byctx.items():
        stable=[x for x in items if x['classification'] in ('CONFIRMED_SIGNAL','CANDIDATE_SIGNAL')]
        contrad=[x for x in items if x['classification']=='CONTRADICTORY']
        stable=sorted(stable,key=lambda x:float(x['confidence']),reverse=True)[:6]
        if stable:
            first=stable[0]; watermark=current_watermark(first['stage_s'],first['horizon_s'],first['target'])
            if watermark:
                for a,b in itertools.combinations(stable,2):
                    if made_pair>=MAX_PAIRWISE_PER_TICK: break
                    features=sorted([a['feature'],b['feature']])
                    spec={'adapter':'v52_learned_pairwise','branch':'LIVE_SCIENCE','stage_s':a['stage_s'],'horizon_s':a['horizon_s'],
                          'target':a['target'],'features':features,'watermark_n':watermark,
                          'parent_conclusions':[a['conclusion_key'],b['conclusion_key']]}
                    hid,_=core.create_hypothesis('LIVE_SCIENCE','LEARNED_PAIRWISE',spec,
                        {'lane':'scientific_followup','derived_from_evidence':True,'fdr_screened':True,'not_for_freezing':True},generation=1000+watermark)
                    _,created=core.enqueue_job(hid,'SCIENTIFIC_FOLLOWUP',spec,priority=20); made_pair+=int(created)
        if contrad and stable:
            for sig in contrad[:4]:
                for reg in stable[:3]:
                    if sig['feature']==reg['feature'] or made_regime>=MAX_REGIME_PER_TICK: continue
                    watermark=current_watermark(sig['stage_s'],sig['horizon_s'],sig['target'])
                    if not watermark: continue
                    spec={'adapter':'v52_regime_probe','branch':'LIVE_SCIENCE','stage_s':sig['stage_s'],'horizon_s':sig['horizon_s'],
                          'target':sig['target'],'signal_feature':sig['feature'],'regime_feature':reg['feature'],'watermark_n':watermark,
                          'parent_conclusions':[sig['conclusion_key'],reg['conclusion_key']]}
                    hid,_=core.create_hypothesis('LIVE_SCIENCE','LEARNED_REGIME',spec,
                        {'lane':'scientific_followup','reason':'sign_instability','not_for_freezing':True},generation=2000+watermark)
                    _,created=core.enqueue_job(hid,'SCIENTIFIC_FOLLOWUP',spec,priority=22); made_regime+=int(created)
    return made_pair,made_regime


def conclusion_counts():
    db=core.open_research(); out={r['classification']:r['n'] for r in db.execute("SELECT classification,COUNT(*) n FROM v45_conclusions GROUP BY classification")}; db.close(); return out


def display(last_director,sw,si,sr,evidence,conclusions,learned):
    db=core.open_research(); jobs={r['status']:r['n'] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}; frozen=db.execute("SELECT COUNT(*) FROM v41_candidates WHERE status='FROZEN'").fetchone()[0]
    branches=db.execute("""SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) h,
      SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) q,SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) r,
      SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) d,SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) f
      FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id GROUP BY h.branch ORDER BY h DESC""").fetchall(); db.close()
    swaps,tokens,ready,latest=v44.v52_stats(); age='—' if latest is None else f"{max(0,time.time()-float(latest)):.1f}s"
    cc=conclusion_counts()
    print('\033[2J\033[H',end=''); print('='*132); print('MEMECOIN LAB — SCIENTIFIC LEARNING ORGANISM V4.5'); print('='*132)
    print(f"WORKERS={WORKERS} | QUEUED={jobs.get('QUEUED',0)} | RUNNING={jobs.get('RUNNING',0)} | DONE={jobs.get('DONE',0)} | FAILED={jobs.get('FAILED',0)} | FROZEN={frozen}")
    print(f"V52 SWAPS={swaps:,} | TOKENS={tokens:,} | READY={ready:,} | AGE={age} | PRESET JOBS={sr} | LEARNED PAIR/REGIME={learned}")
    print(f"EVIDENCE={evidence:,} | CONCLUSIONS={sum(cc.values()):,} | CONFIRMED={cc.get('CONFIRMED_SIGNAL',0)} | CANDIDATE={cc.get('CANDIDATE_SIGNAL',0)} | CONTRADICTORY={cc.get('CONTRADICTORY',0)} | NO_EDGE={cc.get('NO_EDGE',0)}")
    print(f"DIRECTOR={last_director} | WALLET={sw} | LIVE_INGEST={si}")
    print(); print(f"{'BRANCH':<20}{'HYP':>8}{'Q':>8}{'RUN':>8}{'DONE':>8}{'FAIL':>8}")
    for x in branches: print(f"{x['branch']:<20}{x['h'] or 0:>8}{x['q'] or 0:>8}{x['r'] or 0:>8}{x['d'] or 0:>8}{x['f'] or 0:>8}")
    print('\nLearning loop: evidence -> FDR -> conclusions -> learned hypotheses -> follow-up experiments. Frozen candidates unchanged. Research-only.')


def main():
    global STOP
    signal.signal(signal.SIGINT,stop_handler); signal.signal(signal.SIGTERM,stop_handler)
    core.initialize(); v43.init_v43(); v44.init_v44(); init_v45(); v42.seed_wallet_history(); old.seed_discovery_if_needed()
    workers=[mp.Process(target=worker_main,args=(i+1,),daemon=True) for i in range(WORKERS)]
    for p in workers: p.start()
    try:
        while not STOP:
            core.reclaim_expired_jobs(); old.seed_discovery_if_needed(); sw=v42.seed_wallet_history(); si=v43.seed_live_ingest(); sr=v44.seed_live_research()
            evidence=refresh_evidence(); conclusions=derive_conclusions(); learned=seed_learned_hypotheses(); d=v42.auto_director_tick()
            display(d,sw,si,sr,evidence,conclusions,learned); time.sleep(LOOP_SLEEP)
    finally:
        for p in workers:
            if p.is_alive(): p.terminate()
        for p in workers: p.join(timeout=3)
        print('V4.5 organism stopped cleanly')

if __name__=='__main__':
    mp.set_start_method('spawn',force=True); main()
