#!/usr/bin/env python3
"""Memecoin Lab V6.0 — Economic Edge Discovery Engine.

New generation objective: discover signals directly against executable economic
outcomes, not against abstract hit labels first.

Core principles
---------------
- Universe is filtered for point-in-time executability before research.
- Targets are net economic outcomes after explicit costs.
- Train/holdout split is deterministic by token.
- Signal direction and threshold are learned on DISCOVERY only.
- HOLDOUT is untouched until the rule is frozen.
- A candidate is PROMISING only if HOLDOUT expectancy > 0 and PF > 1.
- No live trading/signing; no mutation of V5.x frozen champions.

This first V6.0 lane is intentionally simple and falsifiable: univariate features
from v52_snapshots are tested across stage/horizon/barrier regimes.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import signal
import sqlite3
import statistics
import time
from pathlib import Path

import v41_core as core
import v59_champion_exploitation_engine as v59

ROOT = Path.home() / "memecoin_lab"
V52 = Path(os.environ.get("MEMECOIN_V52_DB", ROOT / "v52_features.db"))
LOOP = float(os.environ.get("MEMECOIN_V60_LOOP_S", "8"))
BATCH = int(os.environ.get("MEMECOIN_V60_BATCH", "8"))
MAX_ENTRY_GAP_S = float(os.environ.get("MEMECOIN_V60_MAX_ENTRY_GAP_S", "15"))
MIN_PATH_POINTS = int(os.environ.get("MEMECOIN_V60_MIN_PATH_POINTS", "3"))
MIN_TRAIN = int(os.environ.get("MEMECOIN_V60_MIN_TRAIN", "30"))
MIN_HOLDOUT = int(os.environ.get("MEMECOIN_V60_MIN_HOLDOUT", "12"))
MAX_ABS_STEP_PCT = float(os.environ.get("MEMECOIN_V60_MAX_ABS_STEP_PCT", "500"))
MAX_ABS_PATH_RETURN_PCT = float(os.environ.get("MEMECOIN_V60_MAX_ABS_PATH_RETURN_PCT", "10000"))

STAGES = (20, 30, 60, 120)
HORIZONS = (120, 300, 600)
BARRIERS = ((10.0, 10.0), (10.0, 15.0), (20.0, 10.0))
FEATURES = (
    "swaps",
    "buy_ratio",
    "gross_sol",
    "net_sol",
    "unique_wallets",
    "repeat_wallet_ratio",
    "wallet_hhi",
    "wallet_top1_share",
    "avg_trade_sol",
    "max_trade_sol",
    "trade_hhi",
    "top1_trade_share",
    "return_pct",
    "range_pct",
    "flow_velocity",
    "flow_acceleration",
    "buy_ratio_delta",
    "price_velocity",
)
STOP = False


def stop(*_):
    global STOP
    STOP = True


def sf(x, d=None):
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def open_v52():
    if not V52.exists():
        return None
    d = sqlite3.connect(f"file:{V52}?mode=ro", uri=True, timeout=30)
    d.row_factory = sqlite3.Row
    d.execute("PRAGMA busy_timeout=30000")
    return d


def holdout(token):
    h = hashlib.sha256(str(token).encode()).digest()
    return int.from_bytes(h[:4], "big") % 100 >= 75


def init():
    d = core.open_research()
    d.executescript("""
    CREATE TABLE IF NOT EXISTS v60_experiments(
      experiment_id TEXT PRIMARY KEY,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      tp_pct REAL NOT NULL,
      sl_pct REAL NOT NULL,
      feature TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL,
      UNIQUE(stage_s,horizon_s,tp_pct,sl_pct,feature)
    );

    CREATE TABLE IF NOT EXISTS v60_results(
      experiment_id TEXT PRIMARY KEY,
      train_n INTEGER NOT NULL,
      holdout_n INTEGER NOT NULL,
      train_executable INTEGER NOT NULL,
      holdout_executable INTEGER NOT NULL,
      direction REAL,
      threshold REAL,
      train_selected INTEGER NOT NULL,
      holdout_selected INTEGER NOT NULL,
      train_expectancy REAL,
      train_median_net REAL,
      train_win_rate REAL,
      train_profit_factor REAL,
      train_hit_rate REAL,
      holdout_expectancy REAL,
      holdout_median_net REAL,
      holdout_win_rate REAL,
      holdout_profit_factor REAL,
      holdout_hit_rate REAL,
      holdout_baseline_expectancy REAL,
      holdout_baseline_hit_rate REAL,
      expectancy_lift REAL,
      hit_rate_lift REAL,
      verdict TEXT NOT NULL,
      metrics_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v60_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    """)
    now = time.time()
    for st in STAGES:
        for hz in HORIZONS:
            for tp, sl in BARRIERS:
                for feat in FEATURES:
                    eid = "E60_" + hashlib.sha256(f"{st}|{hz}|{tp}|{sl}|{feat}".encode()).hexdigest()[:22]
                    d.execute("""
                      INSERT OR IGNORE INTO v60_experiments(
                        experiment_id,stage_s,horizon_s,tp_pct,sl_pct,feature,status,created_at,updated_at
                      ) VALUES(?,?,?,?,?,?,'READY',?,?)
                    """, (eid,st,hz,tp,sl,feat,now,now))
    d.commit(); d.close()


def entry_quote(db, token, decision_ts):
    r = db.execute("""
      SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0
      ORDER BY timestamp DESC LIMIT 1
    """, (token,float(decision_ts))).fetchone()
    if not r:
        return None
    gap = float(decision_ts) - float(r["timestamp"])
    if gap > MAX_ENTRY_GAP_S:
        return None
    return float(r["price_sol"])


def economic_path(db, token, decision_ts, horizon_s, tp, sl):
    entry = entry_quote(db, token, decision_ts)
    if entry is None or entry <= 0:
        return None
    end = float(decision_ts) + int(horizon_s)
    rs = db.execute("""
      SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp>? AND timestamp<=?
        AND price_sol IS NOT NULL AND price_sol>0
      ORDER BY timestamp
    """, (token,float(decision_ts),end)).fetchall()
    if len(rs) < MIN_PATH_POINTS:
        return None
    prices = [float(r["price_sol"]) for r in rs]
    if any((p<=0 or not math.isfinite(p)) for p in prices):
        return None
    allp = [entry] + prices
    steps = [100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]
    rets = [100*(p/entry-1) for p in prices]
    if any(abs(x)>MAX_ABS_STEP_PCT for x in steps):
        return None
    if any(abs(x)>MAX_ABS_PATH_RETURN_PCT for x in rets):
        return None

    exit_raw = rets[-1]
    reason = "TIME_EXIT"
    for r in rets:
        if r >= tp:
            exit_raw = tp; reason = "TP_FIRST"; break
        if r <= -sl:
            exit_raw = -sl; reason = "SL_FIRST"; break
    net = exit_raw - v59.total_cost_pct()
    return {
        "net": net,
        "raw": exit_raw,
        "hit": int(reason == "TP_FIRST"),
        "reason": reason,
        "mfe": max(rets),
        "mae": min(rets),
    }


def dataset(stage, horizon, tp, sl, feature):
    db = open_v52()
    if db is None:
        return []
    rows = db.execute(f"""
      SELECT token_mint,cutoff_ts,{feature} AS feature
      FROM v52_snapshots
      WHERE stage_s=? AND {feature} IS NOT NULL
      ORDER BY cutoff_ts,token_mint
    """, (int(stage),)).fetchall()
    out = []
    for r in rows:
        x = sf(r["feature"])
        if x is None:
            continue
        econ = economic_path(db,str(r["token_mint"]),float(r["cutoff_ts"]),horizon,tp,sl)
        if not econ:
            continue
        out.append({
            "token_mint": str(r["token_mint"]),
            "feature": x,
            "net": econ["net"],
            "hit": econ["hit"],
        })
    db.close()
    return out


def ranks(values):
    ordered=sorted(enumerate(values),key=lambda z:z[1]); out=[0.0]*len(values); i=0
    while i<len(ordered):
        j=i
        while j+1<len(ordered) and ordered[j+1][1]==ordered[i][1]: j+=1
        r=(i+j+2)/2.0
        for k in range(i,j+1): out[ordered[k][0]]=r
        i=j+1
    return out


def pearson(x,y):
    if len(x)<3:return None
    mx=sum(x)/len(x); my=sum(y)/len(y)
    num=sum((a-mx)*(b-my) for a,b in zip(x,y))
    dx=math.sqrt(sum((a-mx)**2 for a in x)); dy=math.sqrt(sum((b-my)**2 for b in y))
    return None if dx==0 or dy==0 else num/(dx*dy)


def spearman(x,y):
    if len(x)<3:return None
    return pearson(ranks(x),ranks(y))


def quantile(vals,q):
    xs=sorted(vals)
    if not xs:return None
    if len(xs)==1:return xs[0]
    pos=(len(xs)-1)*q; lo=int(math.floor(pos)); hi=int(math.ceil(pos)); w=pos-lo
    return xs[lo]*(1-w)+xs[hi]*w


def profit_factor(xs):
    g=sum(x for x in xs if x>0); l=-sum(x for x in xs if x<0)
    if l>0:return g/l
    return 999.0 if g>0 else None


def metrics(rows):
    nets=[r["net"] for r in rows]
    return {
        "n":len(rows),
        "expectancy":statistics.mean(nets) if nets else None,
        "median":statistics.median(nets) if nets else None,
        "win":sum(x>0 for x in nets)/len(nets) if nets else None,
        "pf":profit_factor(nets),
        "hit":sum(r["hit"] for r in rows)/len(rows) if rows else None,
    }


def evaluate(data):
    train=[r for r in data if not holdout(r["token_mint"])]
    test=[r for r in data if holdout(r["token_mint"])]
    if len(train)<MIN_TRAIN or len(test)<MIN_HOLDOUT:
        return "COLLECT_MORE", {"train_n":len(train),"holdout_n":len(test)}

    rho=spearman([r["feature"] for r in train],[r["net"] for r in train])
    if rho is None:
        return "REJECT", {"train_n":len(train),"holdout_n":len(test)}
    direction=1.0 if rho>=0 else -1.0
    directed=[direction*r["feature"] for r in train]

    # Threshold is selected on TRAIN only from predeclared quantiles.
    candidates=[]
    for q in (.60,.70,.75,.80,.85,.90):
        th=quantile(directed,q)
        if th is None:continue
        selected=[r for r in train if direction*r["feature"]>=th]
        m=metrics(selected)
        if m["n"]<max(12,MIN_TRAIN//3):continue
        score=sf(m["expectancy"],-1e9)+2.0*min(sf(m["pf"],0),3.0)+0.03*math.sqrt(m["n"])
        candidates.append((score,th,q,m))
    if not candidates:
        return "REJECT", {"train_n":len(train),"holdout_n":len(test),"direction":direction}

    _,threshold,qsel,train_sel=max(candidates,key=lambda x:x[0])
    test_sel=[r for r in test if direction*r["feature"]>=threshold]
    hold_sel=metrics(test_sel)
    hold_base=metrics(test)

    exp_lift=None
    if hold_sel["expectancy"] is not None and hold_base["expectancy"] is not None:
        exp_lift=hold_sel["expectancy"]-hold_base["expectancy"]
    hit_lift=None
    if hold_sel["hit"] is not None and hold_base["hit"] not in (None,0):
        hit_lift=hold_sel["hit"]/hold_base["hit"]

    verdict="REJECT"
    if hold_sel["n"]<8:
        verdict="COLLECT_MORE"
    elif sf(hold_sel["expectancy"],-1)<=0 or sf(hold_sel["pf"],0)<=1.0:
        verdict="REJECT"
    elif sf(exp_lift,-1)<=0:
        verdict="WEAK"
    elif sf(hold_sel["pf"],0)>=1.25 and sf(hold_sel["expectancy"],0)>=1.0:
        verdict="PROMISING"
    else:
        verdict="WEAK"

    return verdict, {
        "train_n":len(train),"holdout_n":len(test),"direction":direction,
        "threshold":threshold,"threshold_q":qsel,
        "train_selected":train_sel,"holdout_selected":hold_sel,
        "holdout_baseline":hold_base,"expectancy_lift":exp_lift,"hit_rate_lift":hit_lift,
    }


def run_one(exp):
    data=dataset(exp["stage_s"],exp["horizon_s"],exp["tp_pct"],exp["sl_pct"],exp["feature"])
    verdict,m=evaluate(data)
    ts=time.time(); d=core.open_research()
    train_sel=m.get("train_selected",{}) if isinstance(m,dict) else {}
    hold_sel=m.get("holdout_selected",{}) if isinstance(m,dict) else {}
    base=m.get("holdout_baseline",{}) if isinstance(m,dict) else {}
    d.execute("""
      INSERT OR REPLACE INTO v60_results(
       experiment_id,train_n,holdout_n,train_executable,holdout_executable,direction,threshold,
       train_selected,holdout_selected,train_expectancy,train_median_net,train_win_rate,train_profit_factor,train_hit_rate,
       holdout_expectancy,holdout_median_net,holdout_win_rate,holdout_profit_factor,holdout_hit_rate,
       holdout_baseline_expectancy,holdout_baseline_hit_rate,expectancy_lift,hit_rate_lift,verdict,metrics_json,updated_at
      ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(
      exp["experiment_id"],int(m.get("train_n",0)),int(m.get("holdout_n",0)),int(m.get("train_n",0)),int(m.get("holdout_n",0)),
      m.get("direction"),m.get("threshold"),int(train_sel.get("n",0)),int(hold_sel.get("n",0)),
      train_sel.get("expectancy"),train_sel.get("median"),train_sel.get("win"),train_sel.get("pf"),train_sel.get("hit"),
      hold_sel.get("expectancy"),hold_sel.get("median"),hold_sel.get("win"),hold_sel.get("pf"),hold_sel.get("hit"),
      base.get("expectancy"),base.get("hit"),m.get("expectancy_lift"),m.get("hit_rate_lift"),verdict,core.canonical_json(m),ts
    ))
    d.execute("UPDATE v60_experiments SET status='DONE',updated_at=? WHERE experiment_id=?",(ts,exp["experiment_id"]))
    d.commit();d.close()


def cycle_batch():
    d=core.open_research(); rows=[dict(r) for r in d.execute("SELECT * FROM v60_experiments WHERE status='READY' ORDER BY stage_s,horizon_s,tp_pct,sl_pct,feature LIMIT ?",(BATCH,)).fetchall()]; d.close()
    for r in rows:
        try:run_one(r)
        except Exception as e:
            d=core.open_research(); d.execute("UPDATE v60_experiments SET status='ERROR',updated_at=? WHERE experiment_id=?",(time.time(),r["experiment_id"])); d.commit();d.close(); print("V6 experiment error",r["experiment_id"],repr(e),flush=True)
    return len(rows)


def display(ran):
    d=core.open_research()
    counts={r["status"]:r["n"] for r in d.execute("SELECT status,COUNT(*) n FROM v60_experiments GROUP BY status")}
    verdicts={r["verdict"]:r["n"] for r in d.execute("SELECT verdict,COUNT(*) n FROM v60_results GROUP BY verdict")}
    top=[dict(r) for r in d.execute("""
      SELECT e.stage_s,e.horizon_s,e.tp_pct,e.sl_pct,e.feature,r.*
      FROM v60_results r JOIN v60_experiments e USING(experiment_id)
      WHERE r.verdict IN ('PROMISING','WEAK')
      ORDER BY CASE r.verdict WHEN 'PROMISING' THEN 0 ELSE 1 END,
               r.holdout_expectancy DESC,r.holdout_profit_factor DESC
      LIMIT 12
    """).fetchall()]
    d.close()

    print("\033[2J\033[H",end="")
    print("="*174)
    print("MEMECOIN LAB — ECONOMIC EDGE DISCOVERY ENGINE V6.0")
    print("="*174)
    print(f"READY={counts.get('READY',0)} DONE={counts.get('DONE',0)} ERROR={counts.get('ERROR',0)} RAN={ran} | PROMISING={verdicts.get('PROMISING',0)} WEAK={verdicts.get('WEAK',0)} REJECT={verdicts.get('REJECT',0)} COLLECT={verdicts.get('COLLECT_MORE',0)}")
    print(f"Universe: executable price age <= {MAX_ENTRY_GAP_S:g}s | costs={v59.total_cost_pct():.2f}% | deterministic train/holdout 75/25")
    print("A result is PROMISING only if the frozen HOLDOUT rule has positive expectancy, PF>1, and positive lift over executable baseline.\n")
    print("TOP ECONOMIC EDGES")
    if not top:
        print("No positive holdout economic edge yet.")
    for r in top:
        print(
          f"{r['verdict']:<10} stage={r['stage_s']:<3} h={r['horizon_s']:<3} TP/SL={r['tp_pct']:.0f}/{r['sl_pct']:.0f} "
          f"{r['feature']:<22} th={sf(r['threshold'],0):.4g} dir={sf(r['direction'],0):+g} "
          f"HO_n={r['holdout_selected']:<3} exp={sf(r['holdout_expectancy'],0):+.2f}% med={sf(r['holdout_median_net'],0):+.2f}% "
          f"win={100*sf(r['holdout_win_rate'],0):.1f}% PF={sf(r['holdout_profit_factor'],0):.2f} lift_exp={sf(r['expectancy_lift'],0):+.2f}% hit_lift={sf(r['hit_rate_lift'],0):.2f}x"
        )
    print("\nGuardrail: V6.0 searches economic outcomes directly. Nothing here becomes a live strategy without independent future-only paper confirmation.")


def cycle():
    ran=cycle_batch(); display(ran)
    d=core.open_research(); d.execute("""INSERT INTO v60_state(key,value_json,updated_at) VALUES('latest',?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
      (core.canonical_json({"ran":ran,"updated_at":time.time()}),time.time())); d.commit();d.close()


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:cycle()
        except Exception as e:print("V6.0 error:",repr(e),flush=True)
        time.sleep(LOOP)


if __name__=="__main__":main()
