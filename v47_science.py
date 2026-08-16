#!/usr/bin/env python3
"""Memecoin Lab V4.7 — prospective pass audit + independence + ensemble forge.

Purpose
-------
V4.6 already freezes single-signal candidates and scores them only on unseen tokens.
V4.7 does NOT change those candidates. It runs beside V4.6 and:

1. audits every prospective PASS for temporal stability;
2. measures redundancy between PASS signals in the same prediction context;
3. selects cross-family, low-redundancy PASS signals;
4. freezes a deterministic majority-vote ensemble at a NEW data cutoff;
5. evaluates that ensemble only on tokens whose snapshot is strictly after that cutoff.

Important scientific guardrail: an ensemble may be *selected* using evidence available up
to its freeze time, but once frozen its members, thresholds, directions and voting rule
never change. Its reported performance is therefore forward-only from the ensemble cutoff.

Research-only. No signing. No trading.
"""
from __future__ import annotations

import json
import math
import os
import signal
import sqlite3
import statistics
import time
from collections import defaultdict
from pathlib import Path

import v41_core as core
import v41_engine as base
import v44_organism as v44

ROOT = Path.home() / "memecoin_lab"
V52 = Path(os.environ.get("MEMECOIN_V52_DB", ROOT / "v52_features.db"))
LOOP_S = float(os.environ.get("MEMECOIN_V47_LOOP_S", "3"))
MIN_PASS_N = int(os.environ.get("MEMECOIN_V47_MIN_PASS_N", "100"))
MIN_ENSEMBLE_MEMBERS = int(os.environ.get("MEMECOIN_V47_MIN_ENSEMBLE_MEMBERS", "2"))
MAX_ENSEMBLE_MEMBERS = int(os.environ.get("MEMECOIN_V47_MAX_ENSEMBLE_MEMBERS", "4"))
MAX_REDUNDANCY = float(os.environ.get("MEMECOIN_V47_MAX_REDUNDANCY", "0.78"))
WATCH_N = int(os.environ.get("MEMECOIN_V47_ENSEMBLE_WATCH_N", "40"))
PASS_N = int(os.environ.get("MEMECOIN_V47_ENSEMBLE_PASS_N", "100"))
STOP = False


def stop_handler(*_):
    global STOP
    STOP = True


def family(feature: str) -> str:
    f = (feature or "").lower()
    if any(k in f for k in ("wallet", "hhi", "repeat")):
        return "WALLET_CONCENTRATION"
    if any(k in f for k in ("price", "return", "range")):
        return "PRICE_MOMENTUM"
    if any(k in f for k in ("flow", "buy_ratio", "net_sol", "gross_sol")):
        return "FLOW_IMBALANCE"
    if any(k in f for k in ("swap", "trade", "activity")):
        return "ACTIVITY_TRADING"
    return "OTHER"


def safe_float(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def mean(xs):
    xs = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(xs) / len(xs) if xs else None


def init_v47():
    db = core.open_research()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v47_pass_audit (
      candidate_id TEXT PRIMARY KEY,
      family TEXT NOT NULL,
      feature TEXT NOT NULL,
      target TEXT NOT NULL,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      n INTEGER NOT NULL,
      rho REAL,
      lift REAL,
      precision REAL,
      baseline REAL,
      early_n INTEGER,
      early_rho REAL,
      early_lift REAL,
      late_n INTEGER,
      late_rho REAL,
      late_lift REAL,
      rho_drift REAL,
      lift_drift REAL,
      prediction_rate REAL,
      stability_score REAL NOT NULL,
      audit_class TEXT NOT NULL,
      updated_at REAL NOT NULL,
      FOREIGN KEY(candidate_id) REFERENCES v46_prospective_candidates(candidate_id));

    CREATE TABLE IF NOT EXISTS v47_pair_redundancy (
      candidate_a TEXT NOT NULL,
      candidate_b TEXT NOT NULL,
      context_key TEXT NOT NULL,
      shared_n INTEGER NOT NULL,
      score_rho REAL,
      prediction_jaccard REAL,
      prediction_agreement REAL,
      redundancy REAL,
      independence REAL,
      updated_at REAL NOT NULL,
      PRIMARY KEY(candidate_a,candidate_b));

    CREATE TABLE IF NOT EXISTS v47_ensembles (
      ensemble_id TEXT PRIMARY KEY,
      context_key TEXT NOT NULL,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      target TEXT NOT NULL,
      member_ids_json TEXT NOT NULL,
      member_features_json TEXT NOT NULL,
      member_families_json TEXT NOT NULL,
      member_count INTEGER NOT NULL,
      min_votes INTEGER NOT NULL,
      data_cutoff REAL NOT NULL,
      selection_note TEXT NOT NULL,
      status TEXT NOT NULL,
      frozen_at REAL NOT NULL,
      updated_at REAL NOT NULL,
      UNIQUE(context_key,member_ids_json));

    CREATE TABLE IF NOT EXISTS v47_ensemble_observations (
      ensemble_id TEXT NOT NULL,
      token_mint TEXT NOT NULL,
      cutoff_ts REAL NOT NULL,
      vote_count INTEGER NOT NULL,
      member_count INTEGER NOT NULL,
      vote_fraction REAL NOT NULL,
      predicted_positive INTEGER NOT NULL,
      actual INTEGER NOT NULL,
      observed_at REAL NOT NULL,
      PRIMARY KEY(ensemble_id,token_mint),
      FOREIGN KEY(ensemble_id) REFERENCES v47_ensembles(ensemble_id));

    CREATE INDEX IF NOT EXISTS idx_v47_ens_obs ON v47_ensemble_observations(ensemble_id,cutoff_ts);

    CREATE TABLE IF NOT EXISTS v47_ensemble_beliefs (
      ensemble_id TEXT PRIMARY KEY,
      status TEXT NOT NULL,
      n INTEGER NOT NULL,
      positives INTEGER NOT NULL,
      predicted_n INTEGER NOT NULL,
      predicted_hits INTEGER NOT NULL,
      baseline REAL,
      precision REAL,
      lift REAL,
      vote_rho REAL,
      confidence REAL NOT NULL,
      statement TEXT NOT NULL,
      updated_at REAL NOT NULL,
      FOREIGN KEY(ensemble_id) REFERENCES v47_ensembles(ensemble_id));

    CREATE TABLE IF NOT EXISTS v47_science_memory (
      memory_key TEXT PRIMARY KEY,
      memory_type TEXT NOT NULL,
      subject TEXT NOT NULL,
      statement TEXT NOT NULL,
      evidence_json TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL);
    """)
    db.commit(); db.close()


def candidate_rows(status="PASS"):
    db = core.open_research()
    rows = [dict(r) for r in db.execute("""
      SELECT c.*,b.status belief_status,b.n,b.baseline_rate,b.precision,b.lift,b.prospective_rho,b.confidence belief_confidence
      FROM v46_prospective_candidates c
      JOIN v46_beliefs b ON b.candidate_id=c.candidate_id
      WHERE b.status=?
      ORDER BY b.confidence DESC,b.n DESC
    """, (status,)).fetchall()]
    db.close()
    return rows


def obs(candidate_id):
    db = core.open_research()
    rows = [dict(r) for r in db.execute("""
      SELECT token_mint,cutoff_ts,directional_score,predicted_positive,actual
      FROM v46_prospective_observations
      WHERE candidate_id=? ORDER BY cutoff_ts,token_mint
    """, (candidate_id,)).fetchall()]
    db.close()
    return rows


def slice_metrics(rows):
    n = len(rows)
    if not rows:
        return dict(n=0, rho=None, lift=None, precision=None, baseline=None)
    y = [int(r["actual"]) for r in rows]
    s = [float(r["directional_score"]) for r in rows]
    rho = base.spearman(s, y) if n >= 8 else None
    baseline = sum(y) / n
    pred = [r for r in rows if int(r["predicted_positive"])]
    precision = sum(int(r["actual"]) for r in pred) / len(pred) if pred else None
    lift = precision / baseline if precision is not None and baseline > 0 else None
    return dict(n=n, rho=rho, lift=lift, precision=precision, baseline=baseline)


def stability_score(full, early, late):
    rho = max(0.0, safe_float(full.get("rho")) or 0.0)
    lift = max(0.0, (safe_float(full.get("lift")) or 1.0) - 1.0)
    er = safe_float(early.get("rho")); lr = safe_float(late.get("rho"))
    el = safe_float(early.get("lift")); ll = safe_float(late.get("lift"))
    rho_stab = 0.0 if er is None or lr is None else max(0.0, 1.0 - abs(er-lr)/0.30)
    lift_stab = 0.0 if el is None or ll is None else max(0.0, 1.0 - abs(el-ll)/2.0)
    return max(0.0, min(1.0, 0.30*min(1.0,rho/0.25) + 0.25*min(1.0,lift/1.5) + 0.25*rho_stab + 0.20*lift_stab))


def audit_passes():
    now = time.time(); count = 0
    for c in candidate_rows("PASS"):
        rows = obs(c["candidate_id"])
        if not rows:
            continue
        mid = len(rows)//2
        full = slice_metrics(rows); early = slice_metrics(rows[:mid]); late = slice_metrics(rows[mid:])
        score = stability_score(full,early,late)
        if score >= .75 and len(rows) >= 140:
            cls = "ELITE_STABLE"
        elif score >= .55:
            cls = "STABLE"
        elif score >= .35:
            cls = "MIXED"
        else:
            cls = "FRAGILE"
        pr = sum(int(r["predicted_positive"]) for r in rows)/len(rows)
        er,lr = safe_float(early["rho"]),safe_float(late["rho"])
        el,ll = safe_float(early["lift"]),safe_float(late["lift"])
        db=core.open_research()
        db.execute("""INSERT INTO v47_pass_audit(candidate_id,family,feature,target,stage_s,horizon_s,n,rho,lift,precision,baseline,
          early_n,early_rho,early_lift,late_n,late_rho,late_lift,rho_drift,lift_drift,prediction_rate,stability_score,audit_class,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(candidate_id) DO UPDATE SET n=excluded.n,rho=excluded.rho,lift=excluded.lift,precision=excluded.precision,
          baseline=excluded.baseline,early_n=excluded.early_n,early_rho=excluded.early_rho,early_lift=excluded.early_lift,
          late_n=excluded.late_n,late_rho=excluded.late_rho,late_lift=excluded.late_lift,rho_drift=excluded.rho_drift,
          lift_drift=excluded.lift_drift,prediction_rate=excluded.prediction_rate,stability_score=excluded.stability_score,
          audit_class=excluded.audit_class,updated_at=excluded.updated_at""",
          (c["candidate_id"],family(c["feature"]),c["feature"],c["target"],c["stage_s"],c["horizon_s"],len(rows),full["rho"],full["lift"],
           full["precision"],full["baseline"],early["n"],early["rho"],early["lift"],late["n"],late["rho"],late["lift"],
           None if er is None or lr is None else lr-er,None if el is None or ll is None else ll-el,pr,score,cls,now))
        db.commit(); db.close(); count += 1
    return count


def prediction_jaccard(a,b):
    pa={k for k,v in a.items() if int(v["predicted_positive"])}
    pb={k for k,v in b.items() if int(v["predicted_positive"])}
    union=pa|pb
    return len(pa&pb)/len(union) if union else None


def measure_redundancy():
    passes=candidate_rows("PASS"); groups=defaultdict(list)
    for c in passes:
        key=f"{c['stage_s']}:{c['horizon_s']}:{c['target']}"
        groups[key].append(c)
    now=time.time(); made=0
    for context,items in groups.items():
        maps={c["candidate_id"]:{r["token_mint"]:r for r in obs(c["candidate_id"])} for c in items}
        for i in range(len(items)):
            for j in range(i+1,len(items)):
                a,b=items[i],items[j]; aid,bid=sorted([a["candidate_id"],b["candidate_id"]])
                ma,mb=maps[aid],maps[bid]; shared=sorted(set(ma)&set(mb))
                if len(shared)<8:
                    continue
                sa=[float(ma[t]["directional_score"]) for t in shared]
                sb=[float(mb[t]["directional_score"]) for t in shared]
                rho=base.spearman(sa,sb)
                jac=prediction_jaccard({t:ma[t] for t in shared},{t:mb[t] for t in shared})
                agree=sum(int(ma[t]["predicted_positive"])==int(mb[t]["predicted_positive"]) for t in shared)/len(shared)
                rscore=min(1.0,0.55*abs(float(rho or 0))+0.30*float(jac or 0)+0.15*agree)
                db=core.open_research()
                db.execute("""INSERT INTO v47_pair_redundancy(candidate_a,candidate_b,context_key,shared_n,score_rho,prediction_jaccard,prediction_agreement,redundancy,independence,updated_at)
                  VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(candidate_a,candidate_b) DO UPDATE SET shared_n=excluded.shared_n,score_rho=excluded.score_rho,
                  prediction_jaccard=excluded.prediction_jaccard,prediction_agreement=excluded.prediction_agreement,redundancy=excluded.redundancy,
                  independence=excluded.independence,updated_at=excluded.updated_at""",
                  (aid,bid,context,len(shared),rho,jac,agree,rscore,1-rscore,now))
                db.commit(); db.close(); made += 1
    return made


def latest_context_cutoff(stage,horizon,target):
    db=v44.open_v52()
    if db is None:
        return None
    row=db.execute(f"""SELECT MAX(s.cutoff_ts) mx FROM v52_snapshots s
      JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
      WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND o.{target} IS NOT NULL""",(stage,horizon)).fetchone()
    db.close(); return None if row is None or row["mx"] is None else float(row["mx"])


def redundancy(a,b):
    aid,bid=sorted([a,b]); db=core.open_research()
    row=db.execute("SELECT redundancy FROM v47_pair_redundancy WHERE candidate_a=? AND candidate_b=?",(aid,bid)).fetchone(); db.close()
    return None if row is None else safe_float(row[0])


def audit_row(cid):
    db=core.open_research(); row=db.execute("SELECT * FROM v47_pass_audit WHERE candidate_id=?",(cid,)).fetchone(); db.close()
    return dict(row) if row else None


def forge_ensembles():
    passes=[c for c in candidate_rows("PASS") if int(c.get("n") or 0)>=MIN_PASS_N]
    groups=defaultdict(list)
    for c in passes:
        groups[(int(c["stage_s"]),int(c["horizon_s"]),str(c["target"]))].append(c)
    made=0
    for (stage,horizon,target),items in groups.items():
        # Keep the strongest audited PASS per coarse family first.
        ranked=[]
        for c in items:
            a=audit_row(c["candidate_id"])
            if not a: continue
            ranked.append((float(a["stability_score"]),float(c.get("belief_confidence") or 0),c,a))
        ranked.sort(key=lambda x:(x[0],x[1]),reverse=True)
        pool=[]; used_families=set()
        for _,_,c,a in ranked:
            fam=a["family"]
            if fam in used_families: continue
            if any((redundancy(c["candidate_id"],x["candidate_id"]) or 0)>=MAX_REDUNDANCY for x in pool):
                continue
            pool.append(c); used_families.add(fam)
            if len(pool)>=MAX_ENSEMBLE_MEMBERS: break
        if len(pool)<MIN_ENSEMBLE_MEMBERS:
            continue
        members=sorted(c["candidate_id"] for c in pool)
        member_json=core.canonical_json(members)
        db=core.open_research()
        exists=db.execute("SELECT 1 FROM v47_ensembles WHERE context_key=? AND member_ids_json=?",(f"{stage}:{horizon}:{target}",member_json)).fetchone(); db.close()
        if exists: continue
        cutoff=latest_context_cutoff(stage,horizon,target)
        if cutoff is None: continue
        features=[next(c["feature"] for c in pool if c["candidate_id"]==m) for m in members]
        families=[family(f) for f in features]
        min_votes=max(1,math.ceil(len(members)/2))
        eid="E_"+core.fingerprint({"members":members,"context":[stage,horizon,target],"cutoff":cutoff},"v47ensemble:")[:22]
        note=f"Frozen majority vote from {len(members)} prior prospective PASS signals across {len(set(families))} families; no post-freeze tuning."
        now=time.time(); db=core.open_research()
        db.execute("""INSERT OR IGNORE INTO v47_ensembles(ensemble_id,context_key,stage_s,horizon_s,target,member_ids_json,member_features_json,
          member_families_json,member_count,min_votes,data_cutoff,selection_note,status,frozen_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'FROZEN',?,?)""",
          (eid,f"{stage}:{horizon}:{target}",stage,horizon,target,member_json,core.canonical_json(features),core.canonical_json(families),len(members),min_votes,cutoff,note,now,now))
        made += db.total_changes>0; db.commit(); db.close()
    return int(made)


def snapshot_columns():
    db=v44.open_v52()
    if db is None: return set()
    cols={r[1] for r in db.execute("PRAGMA table_info(v52_snapshots)").fetchall()}; db.close(); return cols


def refresh_ensemble_observations():
    cols=snapshot_columns(); rdb=core.open_research(); ensembles=[dict(r) for r in rdb.execute("SELECT * FROM v47_ensembles").fetchall()]; rdb.close(); inserted=0
    for e in ensembles:
        members=json.loads(e["member_ids_json"]); candidates={}
        rdb=core.open_research()
        for cid in members:
            row=rdb.execute("SELECT candidate_id,feature,direction,threshold FROM v46_prospective_candidates WHERE candidate_id=?",(cid,)).fetchone()
            if row: candidates[cid]=dict(row)
        rdb.close()
        if len(candidates)!=len(members): continue
        features=[candidates[cid]["feature"] for cid in members]
        if any(f not in cols for f in features): continue
        select_cols=",".join(f"s.{f} AS '{f}'" for f in features)
        notnull=" AND ".join(f"s.{f} IS NOT NULL" for f in features)
        vdb=v44.open_v52()
        if vdb is None: continue
        rows=vdb.execute(f"""SELECT s.token_mint,s.cutoff_ts,{select_cols},o.{e['target']} actual
          FROM v52_snapshots s JOIN v52_outcomes o ON o.token_mint=s.token_mint AND o.stage_s=s.stage_s
          WHERE s.stage_s=? AND o.horizon_s=? AND o.ready=1 AND s.cutoff_ts>? AND o.{e['target']} IS NOT NULL AND {notnull}
          ORDER BY s.cutoff_ts,s.token_mint""",(e["stage_s"],e["horizon_s"],e["data_cutoff"])).fetchall(); vdb.close()
        db=core.open_research(); db.execute("BEGIN IMMEDIATE")
        try:
            for r in rows:
                votes=0
                for cid in members:
                    c=candidates[cid]; score=float(c["direction"])*float(r[c["feature"]]); votes += int(score>=float(c["threshold"]))
                pred=int(votes>=int(e["min_votes"])); before=db.total_changes
                db.execute("""INSERT OR IGNORE INTO v47_ensemble_observations(ensemble_id,token_mint,cutoff_ts,vote_count,member_count,vote_fraction,predicted_positive,actual,observed_at)
                  VALUES(?,?,?,?,?,?,?,?,?)""",(e["ensemble_id"],str(r["token_mint"]),float(r["cutoff_ts"]),votes,len(members),votes/len(members),pred,int(r["actual"]),time.time()))
                inserted += db.total_changes-before
            db.commit()
        except BaseException:
            db.rollback(); raise
        finally: db.close()
    return inserted


def evaluate_ensemble(eid):
    db=core.open_research(); e=dict(db.execute("SELECT * FROM v47_ensembles WHERE ensemble_id=?",(eid,)).fetchone())
    rows=[dict(r) for r in db.execute("SELECT * FROM v47_ensemble_observations WHERE ensemble_id=? ORDER BY cutoff_ts",(eid,)).fetchall()]; db.close()
    n=len(rows); positives=sum(int(r["actual"]) for r in rows); baseline=positives/n if n else None
    pred=[r for r in rows if int(r["predicted_positive"])]; pn=len(pred); ph=sum(int(r["actual"]) for r in pred)
    precision=ph/pn if pn else None; lift=precision/baseline if precision is not None and baseline and baseline>0 else None
    rho=base.spearman([float(r["vote_fraction"]) for r in rows],[int(r["actual"]) for r in rows]) if n>=8 else None
    status="WAITING"
    if n>=WATCH_N: status="WATCH"
    if n>=PASS_N and rho is not None and rho>=.08 and lift is not None and lift>=1.25 and pn>=10: status="PASS"
    elif n>=PASS_N and ((rho is not None and rho<=.01) or (lift is not None and lift<=1.05)): status="FAIL"
    sample=min(1.0,n/max(1,PASS_N)); strength=min(1.0,max(0.0,float(rho or 0))/.20); ls=min(1.0,max(0.0,float(lift or 1)-1)/.75)
    conf=max(0.0,min(1.0,.45*sample+.30*strength+.25*ls))
    statement=f"ensemble {eid} {e['target']} {e['stage_s']}s/{e['horizon_s']}s: {status}; unseen n={n}, rho={rho}, lift={lift}, precision={precision}, baseline={baseline}"
    return status,conf,statement,dict(n=n,positives=positives,predicted_n=pn,predicted_hits=ph,baseline=baseline,precision=precision,lift=lift,vote_rho=rho)


def refresh_ensemble_beliefs():
    db=core.open_research(); ids=[r[0] for r in db.execute("SELECT ensemble_id FROM v47_ensembles").fetchall()]; db.close(); counts=defaultdict(int); now=time.time()
    for eid in ids:
        status,conf,statement,m=evaluate_ensemble(eid); counts[status]+=1; db=core.open_research()
        db.execute("""INSERT INTO v47_ensemble_beliefs(ensemble_id,status,n,positives,predicted_n,predicted_hits,baseline,precision,lift,vote_rho,confidence,statement,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(ensemble_id) DO UPDATE SET status=excluded.status,n=excluded.n,positives=excluded.positives,
          predicted_n=excluded.predicted_n,predicted_hits=excluded.predicted_hits,baseline=excluded.baseline,precision=excluded.precision,lift=excluded.lift,
          vote_rho=excluded.vote_rho,confidence=excluded.confidence,statement=excluded.statement,updated_at=excluded.updated_at""",
          (eid,status,m["n"],m["positives"],m["predicted_n"],m["predicted_hits"],m["baseline"],m["precision"],m["lift"],m["vote_rho"],conf,statement,now))
        key=f"ensemble_belief:{eid}:{status}"
        db.execute("""INSERT INTO v47_science_memory(memory_key,memory_type,subject,statement,evidence_json,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?) ON CONFLICT(memory_key) DO UPDATE SET statement=excluded.statement,evidence_json=excluded.evidence_json,updated_at=excluded.updated_at""",
          (key,"ENSEMBLE_BELIEF",eid,statement,core.canonical_json(m),now,now))
        db.commit(); db.close()
    return dict(counts)


def display(audits,pairs,new_ens,new_obs,beliefs):
    db=core.open_research()
    ac={r["audit_class"]:r["n"] for r in db.execute("SELECT audit_class,COUNT(*) n FROM v47_pass_audit GROUP BY audit_class")}
    ens=db.execute("SELECT COUNT(*) FROM v47_ensembles").fetchone()[0]
    passn=db.execute("SELECT COUNT(*) FROM v46_beliefs WHERE status='PASS'").fetchone()[0]
    top=[dict(r) for r in db.execute("""SELECT e.ensemble_id,e.target,e.stage_s,e.horizon_s,e.member_count,b.status,b.n,b.lift,b.vote_rho
      FROM v47_ensembles e LEFT JOIN v47_ensemble_beliefs b ON b.ensemble_id=e.ensemble_id
      ORDER BY CASE b.status WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 ELSE 2 END,b.confidence DESC LIMIT 6""").fetchall()]
    db.close()
    print("\033[2J\033[H",end="")
    print("="*126); print("MEMECOIN LAB — V4.7 PROSPECTIVE EDGE SCIENCE"); print("="*126)
    print(f"SINGLE PASS={passn} | AUDITED={audits} | ELITE={ac.get('ELITE_STABLE',0)} | STABLE={ac.get('STABLE',0)} | MIXED={ac.get('MIXED',0)} | FRAGILE={ac.get('FRAGILE',0)}")
    print(f"PAIR REDUNDANCY={pairs} | ENSEMBLES={ens} | NEW ENSEMBLES={new_ens} | NEW FUTURE OBS={new_obs} | BELIEFS={beliefs}")
    print("\nTOP FROZEN ENSEMBLES")
    for x in top:
        print(f"{x['ensemble_id']:<26} {str(x['status'] or 'WAITING'):<8} {x['target']:<18} {x['stage_s']:>3}s/{x['horizon_s']:<4}s members={x['member_count']} n={x['n'] or 0} rho={x['vote_rho']} lift={x['lift']}")
    print("\nGuardrail: ensemble membership/rules frozen BEFORE these observations. Research-only; no live trading.")


def main():
    global STOP
    signal.signal(signal.SIGINT,stop_handler); signal.signal(signal.SIGTERM,stop_handler)
    core.initialize(); init_v47()
    while not STOP:
        try:
            audits=audit_passes(); pairs=measure_redundancy(); new_ens=forge_ensembles(); new_obs=refresh_ensemble_observations(); beliefs=refresh_ensemble_beliefs()
            display(audits,pairs,new_ens,new_obs,beliefs)
        except Exception as exc:
            print(f"V4.7 cycle error: {exc!r}")
        end=time.time()+LOOP_S
        while not STOP and time.time()<end: time.sleep(.2)
    print("V4.7 science stopped cleanly")


if __name__=="__main__":
    main()
