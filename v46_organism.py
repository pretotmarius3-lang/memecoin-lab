#!/usr/bin/env python3
"""Memecoin Lab V4.6 — prospective scientific validation organism.

Extends V4.5 with:
- immutable prospective candidate freezing from confirmed live conclusions
- fixed point-in-time thresholds and data cutoffs
- prospective-only scoring on tokens whose snapshot cutoff is strictly after freeze
- belief states: WAITING / WATCH / PASS / FAIL
- automatic belief memory updates without mutating historical Frozen candidates

Research-only. No signing. No trading.
"""
from __future__ import annotations

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
import v45_organism as v45

CPU = os.cpu_count() or 4
WORKERS = int(os.environ.get("MEMECOIN_V46_WORKERS", str(min(10, max(4, CPU // 2)))))
IDLE_SLEEP = 0.25
LOOP_SLEEP = 1.0
MIN_FREEZE_CONF = float(os.environ.get("MEMECOIN_V46_MIN_FREEZE_CONF", "0.62"))
MIN_FREEZE_EPOCHS = int(os.environ.get("MEMECOIN_V46_MIN_FREEZE_EPOCHS", "3"))
MIN_PROSPECTIVE_N = int(os.environ.get("MEMECOIN_V46_MIN_PROSPECTIVE_N", "40"))
PASS_N = int(os.environ.get("MEMECOIN_V46_PASS_N", "100"))
FAIL_N = int(os.environ.get("MEMECOIN_V46_FAIL_N", "100"))
STOP = False

def stop_handler(*_):
    global STOP
    STOP = True

def percentile(xs, q):
    xs = sorted(float(x) for x in xs if x is not None and math.isfinite(float(x)))
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    p = (len(xs)-1)*q
    lo, hi = math.floor(p), math.ceil(p)
    if lo == hi:
        return xs[lo]
    w = p-lo
    return xs[lo]*(1-w)+xs[hi]*w

def init_v46():
    db = core.open_research()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v46_prospective_candidates (
      candidate_id TEXT PRIMARY KEY,
      source_conclusion_key TEXT NOT NULL UNIQUE,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      target TEXT NOT NULL,
      feature TEXT NOT NULL,
      direction REAL NOT NULL,
      threshold REAL NOT NULL,
      data_cutoff REAL NOT NULL,
      train_n INTEGER NOT NULL,
      train_positive_rate REAL,
      source_confidence REAL NOT NULL,
      source_median_rho REAL,
      status TEXT NOT NULL,
      frozen_at REAL NOT NULL,
      updated_at REAL NOT NULL);

    CREATE TABLE IF NOT EXISTS v46_prospective_observations (
      candidate_id TEXT NOT NULL,
      token_mint TEXT NOT NULL,
      cutoff_ts REAL NOT NULL,
      feature_value REAL NOT NULL,
      directional_score REAL NOT NULL,
      predicted_positive INTEGER NOT NULL,
      actual INTEGER NOT NULL,
      observed_at REAL NOT NULL,
      PRIMARY KEY(candidate_id,token_mint),
      FOREIGN KEY(candidate_id) REFERENCES v46_prospective_candidates(candidate_id));

    CREATE INDEX IF NOT EXISTS idx_v46_obs_candidate ON v46_prospective_observations(candidate_id,cutoff_ts);

    CREATE TABLE IF NOT EXISTS v46_beliefs (
      candidate_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      n INTEGER NOT NULL,
      positives INTEGER NOT NULL,
      predicted_n INTEGER NOT NULL,
      predicted_hits INTEGER NOT NULL,
      baseline_rate REAL,
      precision REAL,
      lift REAL,
      prospective_rho REAL,
      confidence REAL NOT NULL,
      statement TEXT NOT NULL,
      metrics_json TEXT NOT NULL,
      updated_at REAL NOT NULL,
      FOREIGN KEY(candidate_id) REFERENCES v46_prospective_candidates(candidate_id));

    CREATE TABLE IF NOT EXISTS v46_brain_memory (
      memory_key TEXT PRIMARY KEY,
      memory_type TEXT NOT NULL,
      subject TEXT NOT NULL,
      statement TEXT NOT NULL,
      evidence_json TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    """)
    db.commit()
    db.close()

def source_rows(stage_s,horizon_s,target,feature,cutoff=None):
    db = v44.open_v52()
    if db is None:
        return []
    sql = f"""SELECT s.token_mint,s.cutoff_ts,s.{feature} feature,o.{target} target
              FROM v52_snapshots s
              JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
              WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1
                AND s.{feature} IS NOT NULL AND o.{target} IS NOT NULL"""
    params=[stage_s,horizon_s]
    if cutoff is not None:
        sql += " AND s.cutoff_ts<=?"
        params.append(float(cutoff))
    sql += " ORDER BY s.cutoff_ts,s.token_mint"
    rows = db.execute(sql, tuple(params)).fetchall()
    db.close()
    return [dict(r) for r in rows]

def latest_cutoff(stage_s,horizon_s,target,feature):
    rows = source_rows(stage_s,horizon_s,target,feature)
    return max((float(r["cutoff_ts"]) for r in rows), default=None)

def freeze_candidates():
    db = core.open_research()
    conclusions=[dict(r) for r in db.execute("""
      SELECT * FROM v45_conclusions
      WHERE classification='CONFIRMED_SIGNAL' AND epochs>=? AND confidence>=?
      ORDER BY confidence DESC
    """,(MIN_FREEZE_EPOCHS,MIN_FREEZE_CONF)).fetchall()]
    existing={r[0] for r in db.execute("SELECT source_conclusion_key FROM v46_prospective_candidates").fetchall()}
    db.close()
    made=0
    for c in conclusions:
        if c["conclusion_key"] in existing:
            continue
        cutoff=latest_cutoff(c["stage_s"],c["horizon_s"],c["target"],c["feature"])
        if cutoff is None:
            continue
        rows=source_rows(c["stage_s"],c["horizon_s"],c["target"],c["feature"],cutoff)
        if len(rows)<60:
            continue
        direction=1.0 if float(c["median_rho"] or 0)>=0 else -1.0
        scores=[direction*float(r["feature"]) for r in rows]
        threshold=percentile(scores,0.75)
        if threshold is None:
            continue
        base_rate=sum(int(r["target"]) for r in rows)/len(rows)
        cid="P_"+core.fingerprint({
            "conclusion":c["conclusion_key"],"cutoff":cutoff,"threshold":threshold,"direction":direction
        },"prospective:")[:22]
        now=time.time()
        db=core.open_research()
        db.execute("""INSERT OR IGNORE INTO v46_prospective_candidates(
          candidate_id,source_conclusion_key,stage_s,horizon_s,target,feature,direction,threshold,
          data_cutoff,train_n,train_positive_rate,source_confidence,source_median_rho,status,frozen_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'FROZEN',?,?)""",
          (cid,c["conclusion_key"],c["stage_s"],c["horizon_s"],c["target"],c["feature"],direction,threshold,
           cutoff,len(rows),base_rate,c["confidence"],c["median_rho"],now,now))
        made += db.total_changes>0
        db.commit(); db.close()
    return int(made)

def refresh_prospective():
    db=core.open_research()
    candidates=[dict(r) for r in db.execute("SELECT * FROM v46_prospective_candidates").fetchall()]
    db.close()
    inserted=0
    for c in candidates:
        vdb=v44.open_v52()
        if vdb is None:
            continue
        feature=c["feature"]; target=c["target"]
        rows=vdb.execute(f"""SELECT s.token_mint,s.cutoff_ts,s.{feature} feature,o.{target} target
                             FROM v52_snapshots s
                             JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
                             WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1
                               AND s.cutoff_ts>? AND s.{feature} IS NOT NULL AND o.{target} IS NOT NULL
                             ORDER BY s.cutoff_ts,s.token_mint""",
                          (c["stage_s"],c["horizon_s"],c["data_cutoff"])).fetchall()
        vdb.close()
        db=core.open_research()
        db.execute("BEGIN IMMEDIATE")
        try:
            for r in rows:
                score=float(c["direction"])*float(r["feature"])
                pred=int(score>=float(c["threshold"]))
                before=db.total_changes
                db.execute("""INSERT OR IGNORE INTO v46_prospective_observations(
                    candidate_id,token_mint,cutoff_ts,feature_value,directional_score,predicted_positive,actual,observed_at)
                    VALUES(?,?,?,?,?,?,?,?)""",
                    (c["candidate_id"],str(r["token_mint"]),float(r["cutoff_ts"]),float(r["feature"]),score,pred,int(r["target"]),time.time()))
                inserted += db.total_changes-before
            db.commit()
        except BaseException:
            db.rollback(); raise
        finally:
            db.close()
    return inserted

def evaluate_belief(candidate_id):
    db=core.open_research()
    c=dict(db.execute("SELECT * FROM v46_prospective_candidates WHERE candidate_id=?",(candidate_id,)).fetchone())
    rows=[dict(r) for r in db.execute("""SELECT * FROM v46_prospective_observations
                                        WHERE candidate_id=? ORDER BY cutoff_ts""",(candidate_id,)).fetchall()]
    db.close()
    n=len(rows)
    positives=sum(int(r["actual"]) for r in rows)
    pred=[r for r in rows if int(r["predicted_positive"])]
    predicted_n=len(pred)
    predicted_hits=sum(int(r["actual"]) for r in pred)
    baseline=positives/n if n else None
    precision=predicted_hits/predicted_n if predicted_n else None
    lift=(precision/baseline) if precision is not None and baseline and baseline>0 else None
    rho=base.spearman([float(r["directional_score"]) for r in rows],[int(r["actual"]) for r in rows]) if n>=8 else None

    status="WAITING"
    if n>=MIN_PROSPECTIVE_N:
        status="WATCH"
    if n>=PASS_N and rho is not None and rho>=0.08 and lift is not None and lift>=1.20 and predicted_n>=10:
        status="PASS"
    elif n>=FAIL_N and ((rho is not None and rho<=0.01) or (lift is not None and lift<=1.05)):
        status="FAIL"

    sample=min(1.0,n/max(1,PASS_N))
    strength=0.0 if rho is None else min(1.0,max(0.0,rho)/0.20)
    lift_strength=0.0 if lift is None else min(1.0,max(0.0,lift-1.0)/0.50)
    confidence=max(0.0,min(1.0,0.45*sample+0.30*strength+0.25*lift_strength))
    statement=(f"{c['feature']} -> {c['target']} at {c['stage_s']}s/{c['horizon_s']}s: "
               f"{status}; prospective n={n}, rho={rho}, lift={lift}, precision={precision}, baseline={baseline}")
    metrics=dict(n=n,positives=positives,predicted_n=predicted_n,predicted_hits=predicted_hits,
                 baseline_rate=baseline,precision=precision,lift=lift,prospective_rho=rho,
                 data_cutoff=c["data_cutoff"],threshold=c["threshold"],direction=c["direction"])
    return status,confidence,statement,metrics

def refresh_beliefs():
    db=core.open_research()
    ids=[r[0] for r in db.execute("SELECT candidate_id FROM v46_prospective_candidates").fetchall()]
    db.close()
    counts=defaultdict(int)
    now=time.time()
    for cid in ids:
        status,conf,statement,m=evaluate_belief(cid)
        counts[status]+=1
        db=core.open_research()
        db.execute("""INSERT INTO v46_beliefs(candidate_id,status,n,positives,predicted_n,predicted_hits,
                     baseline_rate,precision,lift,prospective_rho,confidence,statement,metrics_json,updated_at)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(candidate_id) DO UPDATE SET status=excluded.status,n=excluded.n,positives=excluded.positives,
                       predicted_n=excluded.predicted_n,predicted_hits=excluded.predicted_hits,baseline_rate=excluded.baseline_rate,
                       precision=excluded.precision,lift=excluded.lift,prospective_rho=excluded.prospective_rho,
                       confidence=excluded.confidence,statement=excluded.statement,metrics_json=excluded.metrics_json,updated_at=excluded.updated_at""",
                  (cid,status,m["n"],m["positives"],m["predicted_n"],m["predicted_hits"],m["baseline_rate"],
                   m["precision"],m["lift"],m["prospective_rho"],conf,statement,core.canonical_json(m),now))
        mem_key="belief:"+cid+":"+status
        db.execute("""INSERT INTO v46_brain_memory(memory_key,memory_type,subject,statement,evidence_json,created_at,updated_at)
                      VALUES(?,?,?,?,?,?,?)
                      ON CONFLICT(memory_key) DO UPDATE SET statement=excluded.statement,evidence_json=excluded.evidence_json,updated_at=excluded.updated_at""",
                   (mem_key,"PROSPECTIVE_BELIEF",cid,statement,core.canonical_json(m),now,now))
        db.commit(); db.close()
    return dict(counts)

def worker_main(index):
    wid=f"ORG46-{index:02d}-{os.getpid()}"
    core.worker_heartbeat(wid,"RUNNING")
    while True:
        job=base.safe_claim(wid)
        if job is None:
            core.worker_heartbeat(wid,"IDLE"); time.sleep(IDLE_SLEEP); continue
        core.worker_heartbeat(wid,"BUSY",job["job_id"])
        try:
            verdict,metrics=v45.run_payload(job["payload"])
            v45.finish_job(job,verdict,metrics)
            core.worker_heartbeat(wid,"RUNNING",done_inc=1)
        except KeyboardInterrupt:
            return
        except Exception:
            core.fail_job(job,traceback.format_exc())
            core.worker_heartbeat(wid,"RUNNING",failed_inc=1)

def display(sw,si,sr,evidence,conclusions,learned,new_frozen,new_obs,beliefs):
    db=core.open_research()
    jobs={r["status"]:r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
    pc=db.execute("SELECT COUNT(*) FROM v46_prospective_candidates").fetchone()[0]
    branches=db.execute("""SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) h,
      SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) q,
      SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) r,
      SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) d,
      SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) f
      FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
      GROUP BY h.branch ORDER BY h DESC""").fetchall()
    db.close()
    swaps,tokens,ready,latest=v44.v52_stats()
    age="—" if latest is None else f"{max(0,time.time()-float(latest)):.1f}s"
    print("\033[2J\033[H",end="")
    print("="*138)
    print("MEMECOIN LAB — PROSPECTIVE SCIENTIFIC BRAIN V4.6")
    print("="*138)
    print(f"WORKERS={WORKERS} | QUEUED={jobs.get('QUEUED',0)} | RUNNING={jobs.get('RUNNING',0)} | DONE={jobs.get('DONE',0)} | FAILED={jobs.get('FAILED',0)}")
    print(f"V52 SWAPS={swaps:,} | TOKENS={tokens:,} | READY={ready:,} | AGE={age}")
    print(f"EVIDENCE={evidence:,} | CONCLUSIONS={sum(conclusions.values()):,} | LEARNED={learned} | PROSPECTIVE CANDIDATES={pc} | NEW FREEZE={new_frozen} | NEW OBS={new_obs}")
    print(f"BELIEFS WAITING={beliefs.get('WAITING',0)} WATCH={beliefs.get('WATCH',0)} PASS={beliefs.get('PASS',0)} FAIL={beliefs.get('FAIL',0)}")
    print(f"WALLET={sw} | LIVE_INGEST={si} | PRESET_RESEARCH={sr}")
    print()
    print(f"{'BRANCH':<20}{'HYP':>8}{'Q':>8}{'RUN':>8}{'DONE':>8}{'FAIL':>8}")
    for x in branches:
        print(f"{x['branch']:<20}{x['h'] or 0:>8}{x['q'] or 0:>8}{x['r'] or 0:>8}{x['d'] or 0:>8}{x['f'] or 0:>8}")
    print("\nLoop: live data -> FDR conclusions -> learned hypotheses -> immutable freeze -> unseen-token prospective validation -> beliefs.")
    print("Historical Frozen candidates remain untouched. Research-only. No trading.")

def main():
    global STOP
    signal.signal(signal.SIGINT,stop_handler)
    signal.signal(signal.SIGTERM,stop_handler)
    core.initialize(); v43.init_v43(); v44.init_v44(); v45.init_v45(); init_v46()
    v42.seed_wallet_history(); old.seed_discovery_if_needed()
    workers=[mp.Process(target=worker_main,args=(i+1,),daemon=True) for i in range(WORKERS)]
    for p in workers: p.start()
    try:
        while not STOP:
            core.reclaim_expired_jobs()
            old.seed_discovery_if_needed()
            sw=v42.seed_wallet_history()
            si=v43.seed_live_ingest()
            sr=v44.seed_live_research()
            evidence=v45.refresh_evidence()
            conclusions=v45.derive_conclusions()
            learned=v45.seed_learned_hypotheses()
            new_frozen=freeze_candidates()
            new_obs=refresh_prospective()
            beliefs=refresh_beliefs()
            v42.auto_director_tick()
            display(sw,si,sr,evidence,conclusions,learned,new_frozen,new_obs,beliefs)
            time.sleep(LOOP_SLEEP)
    finally:
        for p in workers:
            if p.is_alive(): p.terminate()
        for p in workers: p.join(timeout=3)
        print("V4.6 organism stopped cleanly")

if __name__=="__main__":
    mp.set_start_method("spawn",force=True)
    main()
