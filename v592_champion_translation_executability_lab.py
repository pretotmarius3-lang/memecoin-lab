#!/usr/bin/env python3
"""Memecoin Lab V5.9.2 — Champion Translation & Executability Lab.

Goal: translate scientific champions into actually tradable frozen rules.

Protocol
--------
1. Start from the top unique V5.7.2 scientific winners.
2. Reconstruct each champion score at the true decision time.
3. Apply a point-in-time executability gate (recent price <= MAX_ENTRY_GAP_S).
4. Split tokens deterministically into DISCOVERY (75%) and HOLDOUT (25%).
5. Search entry thresholds on DISCOVERY ONLY.
6. Use one pre-declared execution policy for threshold selection:
      target-aligned TP, fixed SL, chronological first touch, explicit costs.
7. Freeze the selected threshold.
8. Evaluate that immutable rule on HOLDOUT without tuning.
9. Freeze a forward policy after all currently observed decision timestamps and
   collect future paper signals only.

The holdout never selects thresholds. Historical barrier/threshold search is not
prospective proof. No live orders/signing.
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
LOOP = float(os.environ.get("MEMECOIN_V592_LOOP_S", "8"))
TOP = int(os.environ.get("MEMECOIN_V592_TOP", "3"))
MAX_ENTRY_GAP_S = float(os.environ.get("MEMECOIN_V592_MAX_ENTRY_GAP_S", "15"))
MIN_TRAIN_TRADES = int(os.environ.get("MEMECOIN_V592_MIN_TRAIN_TRADES", "20"))
SELECTION_SL_PCT = float(os.environ.get("MEMECOIN_V592_SELECTION_SL_PCT", "10"))
MIN_PATH_POINTS = int(os.environ.get("MEMECOIN_V592_MIN_PATH_POINTS", "3"))
MAX_ABS_STEP_PCT = float(os.environ.get("MEMECOIN_V592_MAX_ABS_STEP_PCT", "500"))
MAX_ABS_PATH_RETURN_PCT = float(os.environ.get("MEMECOIN_V592_MAX_ABS_PATH_RETURN_PCT", "10000"))
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


def init():
    d = core.open_research()
    d.executescript("""
    CREATE TABLE IF NOT EXISTS v592_translation_policies(
      policy_id TEXT PRIMARY KEY,
      challenger_scientific_key TEXT NOT NULL UNIQUE,
      scientific_duel_key TEXT NOT NULL,
      mutation_label TEXT NOT NULL,
      spec_json TEXT NOT NULL,
      direction REAL NOT NULL,
      selected_threshold REAL NOT NULL,
      tp_pct REAL NOT NULL,
      sl_pct REAL NOT NULL,
      holding_s INTEGER NOT NULL,
      train_n INTEGER NOT NULL,
      train_trades INTEGER NOT NULL,
      train_expectancy REAL,
      train_median_net REAL,
      train_win_rate REAL,
      train_profit_factor REAL,
      holdout_n INTEGER NOT NULL,
      holdout_trades INTEGER NOT NULL,
      holdout_expectancy REAL,
      holdout_median_net REAL,
      holdout_win_rate REAL,
      holdout_profit_factor REAL,
      holdout_lift_hit REAL,
      freeze_decision_ts REAL NOT NULL,
      state TEXT NOT NULL,
      details_json TEXT NOT NULL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v592_threshold_sweep(
      policy_id TEXT NOT NULL,
      threshold REAL NOT NULL,
      train_candidates INTEGER NOT NULL,
      train_executable INTEGER NOT NULL,
      train_trades INTEGER NOT NULL,
      hit_rate REAL,
      expectancy REAL,
      median_net REAL,
      win_rate REAL,
      profit_factor REAL,
      score REAL,
      PRIMARY KEY(policy_id,threshold)
    );

    CREATE TABLE IF NOT EXISTS v592_forward_signals(
      signal_id TEXT PRIMARY KEY,
      policy_id TEXT NOT NULL,
      token_mint TEXT NOT NULL,
      decision_ts REAL NOT NULL,
      directed_score REAL NOT NULL,
      threshold REAL NOT NULL,
      entry_price REAL,
      status TEXT NOT NULL,
      exit_reason TEXT,
      raw_return REAL,
      net_return REAL,
      mfe REAL,
      mae REAL,
      settled_at REAL,
      created_at REAL NOT NULL,
      updated_at REAL NOT NULL,
      UNIQUE(policy_id,token_mint)
    );
    CREATE INDEX IF NOT EXISTS idx_v592_forward_policy_status
      ON v592_forward_signals(policy_id,status);

    CREATE TABLE IF NOT EXISTS v592_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    """)
    d.commit(); d.close()


def holdout(token):
    h = hashlib.sha256(str(token).encode()).digest()
    return int.from_bytes(h[:4], "big") % 100 >= 75


def target_tp(spec):
    t = str(spec.get("target", ""))
    for n in (10, 20, 50):
        if f"hit{n}" in t:
            return float(n)
    return 10.0


def top_winners():
    d = core.open_research()
    names = {r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "v572_scientific_duels" not in names:
        d.close(); return []
    rows = [dict(r) for r in d.execute("""
      SELECT * FROM v572_scientific_duels
      WHERE outcome='CHALLENGER_WINS'
      ORDER BY score DESC, unique_evidence_count DESC, challenger_rho DESC
    """).fetchall()]
    d.close()
    seen=set(); out=[]
    for r in rows:
        if r["challenger_scientific_key"] in seen:
            continue
        seen.add(r["challenger_scientific_key"]); out.append(r)
        if len(out) >= TOP:
            break
    return out


def evidence_direction(duel):
    try:
        ids=json.loads(duel["evidence_ids_json"])
    except Exception:
        ids=[]
    d=core.open_research()
    for eid in ids:
        r=d.execute("SELECT metrics_json FROM v49_side_results WHERE experiment_id=?",(eid,)).fetchone()
        if not r:
            continue
        try:
            direction=sf(json.loads(r["metrics_json"]).get("direction"))
            if direction in (-1.0,1.0):
                d.close(); return direction
        except Exception:
            pass
    d.close(); return 1.0


def decision_rows(spec):
    rows=v59.score_rows(spec)
    return [dict(r) for r in rows]


def entry_quote(token, decision_ts):
    db=open_v52()
    if db is None:
        return None, "NO_DB"
    r=db.execute("""
      SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp<=? AND price_sol IS NOT NULL AND price_sol>0
      ORDER BY timestamp DESC LIMIT 1
    """,(token,float(decision_ts))).fetchone()
    db.close()
    if not r:
        return None,"NO_ENTRY"
    gap=float(decision_ts)-float(r["timestamp"])
    if gap > MAX_ENTRY_GAP_S:
        return None,"STALE_ENTRY"
    return {"price":float(r["price_sol"]),"timestamp":float(r["timestamp"]),"gap_s":gap},"OK"


def path(token, decision_ts, holding_s):
    eq,reason=entry_quote(token,decision_ts)
    if reason!="OK":
        return None,reason
    db=open_v52()
    end=float(decision_ts)+int(holding_s)
    rs=db.execute("""
      SELECT price_sol,timestamp FROM v52_swaps
      WHERE token_mint=? AND timestamp>? AND timestamp<=?
        AND price_sol IS NOT NULL AND price_sol>0
      ORDER BY timestamp
    """,(token,float(decision_ts),end)).fetchall()
    db.close()
    if not rs:
        return None,"NO_PATH"
    if len(rs)<MIN_PATH_POINTS:
        return None,"SPARSE_PATH"
    entry=eq["price"]
    prices=[float(r["price_sol"]) for r in rs]
    if entry<=0 or any((not math.isfinite(p) or p<=0) for p in prices):
        return None,"PRICE_ANOMALY"
    allp=[entry]+prices
    steps=[100*(allp[i]/allp[i-1]-1) for i in range(1,len(allp))]
    rets=[100*(p/entry-1) for p in prices]
    if any(abs(x)>MAX_ABS_STEP_PCT for x in steps) or any(abs(x)>MAX_ABS_PATH_RETURN_PCT for x in rets):
        return None,"PRICE_ANOMALY"
    return {
        "entry_price":entry,
        "rets":rets,
        "mfe":max(rets),
        "mae":min(rets),
        "fixed_raw":rets[-1],
    },"OK"


def first_touch(p, tp, sl):
    for r in p["rets"]:
        if r>=tp:
            return tp,"TP_FIRST"
        if r<=-sl:
            return -sl,"SL_FIRST"
    return p["fixed_raw"],"TIME_EXIT"


def profit_factor(xs):
    gains=sum(x for x in xs if x>0)
    losses=-sum(x for x in xs if x<0)
    if losses>0:
        return gains/losses
    return 999.0 if gains>0 else None


def quantile(vals,q):
    xs=sorted(float(x) for x in vals if sf(x) is not None)
    if not xs:return None
    if len(xs)==1:return xs[0]
    pos=(len(xs)-1)*q; lo=int(math.floor(pos)); hi=int(math.ceil(pos)); w=pos-lo
    return xs[lo]*(1-w)+xs[hi]*w


def threshold_candidates(scores):
    vals=sorted(set(float(x) for x in scores if sf(x) is not None))
    if not vals:return []
    c=set()
    for q in (.50,.60,.70,.75,.80,.85,.90,.95):
        x=quantile(vals,q)
        if x is not None:c.add(float(x))
    # Count-like trajectory champions frequently have a mass at zero.
    for x in (1,2,3,5,10,20,40,60,100):
        if vals[0] <= x <= vals[-1]:c.add(float(x))
    return sorted(c)


def prepare(duel):
    spec=json.loads(duel["challenger_spec_json"])
    direction=evidence_direction(duel)
    holding=int(spec.get("horizon",300))
    tp=target_tp(spec)
    rows=[]
    for r in decision_rows(spec):
        score=direction*float(r["feature_value"])
        eq,er=entry_quote(r["token_mint"],r["decision_ts"])
        rows.append({
            **r,
            "score":score,
            "lane":"HOLDOUT" if holdout(r["token_mint"]) else "DISCOVERY",
            "entry_executable":er=="OK",
            "entry_reason":er,
            "entry_price":eq["price"] if eq else None,
        })
    return spec,direction,holding,tp,rows


def evaluate_subset(rows, threshold, holding, tp, sl):
    selected=[r for r in rows if r["score"]>=threshold]
    executable=[r for r in selected if r["entry_executable"]]
    trades=[]
    hit=0
    for r in executable:
        p,reason=path(r["token_mint"],r["decision_ts"],holding)
        if reason!="OK":
            continue
        raw,exit_reason=first_touch(p,tp,sl)
        net=raw-v59.total_cost_pct()
        hit += int(p["mfe"]>=tp)
        trades.append({"net":net,"raw":raw,"exit_reason":exit_reason,"mfe":p["mfe"],"mae":p["mae"]})
    nets=[t["net"] for t in trades]
    return {
        "candidates":len(selected),
        "executable":len(executable),
        "trades":len(trades),
        "hit_rate":hit/len(trades) if trades else None,
        "expectancy":statistics.mean(nets) if nets else None,
        "median_net":statistics.median(nets) if nets else None,
        "win_rate":sum(x>0 for x in nets)/len(nets) if nets else None,
        "profit_factor":profit_factor(nets),
        "nets":nets,
    }


def selection_score(m):
    # Predeclared economic objective. No holdout information enters here.
    n=m["trades"]
    exp=sf(m["expectancy"],-1e9)
    pf=sf(m["profit_factor"],0)
    if n < MIN_TRAIN_TRADES:
        return -1e12
    # Prefer positive expectancy; PF > 1 helps, sample size is only a soft reward.
    return exp + 2.0*min(pf,3.0) + 0.05*math.sqrt(n)


def build_policy(duel):
    spec,direction,holding,tp,rows=prepare(duel)
    train=[r for r in rows if r["lane"]=="DISCOVERY"]
    test=[r for r in rows if r["lane"]=="HOLDOUT"]
    thresholds=threshold_candidates([r["score"] for r in train if r["entry_executable"]])
    if not thresholds:
        return None

    sweeps=[]
    for th in thresholds:
        m=evaluate_subset(train,th,holding,tp,SELECTION_SL_PCT)
        sweeps.append((th,m,selection_score(m)))
    viable=[x for x in sweeps if x[2]>-1e11]
    if not viable:
        return None
    best=max(viable,key=lambda x:(x[2],x[1]["expectancy"],x[1]["trades"]))
    th,train_m,score=best
    hold_m=evaluate_subset(test,th,holding,tp,SELECTION_SL_PCT)

    # Baseline hit rate on executable HOLDOUT before thresholding.
    base=evaluate_subset(test,-1e99,holding,tp,SELECTION_SL_PCT)
    hold_lift=None
    if hold_m["hit_rate"] is not None and base["hit_rate"] not in (None,0):
        hold_lift=hold_m["hit_rate"]/base["hit_rate"]

    freeze=max((r["decision_ts"] for r in rows),default=time.time())
    key=duel["challenger_scientific_key"]
    pid="P592_"+hashlib.sha256(key.encode()).hexdigest()[:20]
    details={
        "protocol":"threshold trained on DISCOVERY only; deterministic token HOLDOUT untouched until threshold frozen",
        "selection_objective":"expectancy + 2*min(PF,3) + 0.05*sqrt(n)",
        "selection_exit":{"tp_pct":tp,"sl_pct":SELECTION_SL_PCT,"holding_s":holding,"cost_pct":v59.total_cost_pct()},
        "executability":{"max_entry_gap_s":MAX_ENTRY_GAP_S},
        "source_rho":duel["challenger_rho"],
        "source_delta_rho":duel["delta_rho"],
        "holdout_baseline_hit_rate":base["hit_rate"],
    }
    now=time.time()
    d=core.open_research()
    d.execute("DELETE FROM v592_threshold_sweep WHERE policy_id=?",(pid,))
    for t,m,s in sweeps:
        d.execute("""INSERT INTO v592_threshold_sweep(policy_id,threshold,train_candidates,train_executable,train_trades,hit_rate,expectancy,median_net,win_rate,profit_factor,score)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                  (pid,t,m["candidates"],m["executable"],m["trades"],m["hit_rate"],m["expectancy"],m["median_net"],m["win_rate"],m["profit_factor"],s))
    d.execute("""INSERT INTO v592_translation_policies(
      policy_id,challenger_scientific_key,scientific_duel_key,mutation_label,spec_json,direction,selected_threshold,tp_pct,sl_pct,holding_s,
      train_n,train_trades,train_expectancy,train_median_net,train_win_rate,train_profit_factor,
      holdout_n,holdout_trades,holdout_expectancy,holdout_median_net,holdout_win_rate,holdout_profit_factor,holdout_lift_hit,
      freeze_decision_ts,state,details_json,created_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'FROZEN_FORWARD',?,?,?)
      ON CONFLICT(policy_id) DO UPDATE SET
       train_n=excluded.train_n,train_trades=excluded.train_trades,train_expectancy=excluded.train_expectancy,train_median_net=excluded.train_median_net,
       train_win_rate=excluded.train_win_rate,train_profit_factor=excluded.train_profit_factor,holdout_n=excluded.holdout_n,
       holdout_trades=excluded.holdout_trades,holdout_expectancy=excluded.holdout_expectancy,holdout_median_net=excluded.holdout_median_net,
       holdout_win_rate=excluded.holdout_win_rate,holdout_profit_factor=excluded.holdout_profit_factor,holdout_lift_hit=excluded.holdout_lift_hit,
       details_json=excluded.details_json,updated_at=excluded.updated_at""",
      (pid,key,duel["scientific_duel_key"],duel["mutation_label"],core.canonical_json(spec),direction,th,tp,SELECTION_SL_PCT,holding,
       len(train),train_m["trades"],train_m["expectancy"],train_m["median_net"],train_m["win_rate"],train_m["profit_factor"],
       len(test),hold_m["trades"],hold_m["expectancy"],hold_m["median_net"],hold_m["win_rate"],hold_m["profit_factor"],hold_lift,
       freeze,core.canonical_json(details),now,now))
    d.commit();d.close()
    return pid


def seed():
    made=0
    for duel in top_winners():
        d=core.open_research(); old=d.execute("SELECT 1 FROM v592_translation_policies WHERE challenger_scientific_key=?",(duel["challenger_scientific_key"],)).fetchone(); d.close()
        if old:
            continue
        if build_policy(duel):
            made+=1
    return made


def forward_path(token,decision_ts,holding,tp,sl,complete=False):
    if complete and time.time()<float(decision_ts)+holding:
        return None,"NOT_READY"
    return path(token,decision_ts,holding)


def scan_forward(p):
    spec=json.loads(p["spec_json"]); now=time.time()
    rows=v59.score_rows(spec,after_ts=float(p["freeze_decision_ts"]))
    for r in rows:
        score=float(p["direction"])*float(r["feature_value"])
        if score<float(p["selected_threshold"]):
            continue
        eq,reason=entry_quote(r["token_mint"],r["decision_ts"])
        if reason!="OK":
            continue
        sid="F592_"+hashlib.sha256((p["policy_id"]+"|"+r["token_mint"]).encode()).hexdigest()[:22]
        d=core.open_research(); d.execute("""INSERT OR IGNORE INTO v592_forward_signals(
          signal_id,policy_id,token_mint,decision_ts,directed_score,threshold,entry_price,status,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,'OPEN',?,?)""",
          (sid,p["policy_id"],r["token_mint"],r["decision_ts"],score,p["selected_threshold"],eq["price"],now,now)); d.commit();d.close()


def settle_forward():
    d=core.open_research(); rows=[dict(r) for r in d.execute("""SELECT s.*,p.tp_pct,p.sl_pct,p.holding_s
      FROM v592_forward_signals s JOIN v592_translation_policies p USING(policy_id) WHERE s.status='OPEN'""").fetchall()]; d.close(); now=time.time()
    for r in rows:
        if now<float(r["decision_ts"])+int(r["holding_s"]):
            continue
        p,reason=path(r["token_mint"],r["decision_ts"],int(r["holding_s"]))
        d=core.open_research()
        if reason!="OK":
            d.execute("UPDATE v592_forward_signals SET status=?,updated_at=? WHERE signal_id=?",("NO_PATH_"+reason,now,r["signal_id"]))
        else:
            raw,exit_reason=first_touch(p,float(r["tp_pct"]),float(r["sl_pct"])); net=raw-v59.total_cost_pct()
            d.execute("""UPDATE v592_forward_signals SET status='DONE',exit_reason=?,raw_return=?,net_return=?,mfe=?,mae=?,settled_at=?,updated_at=? WHERE signal_id=?""",
                      (exit_reason,raw,net,p["mfe"],p["mae"],now,now,r["signal_id"]))
        d.commit();d.close()


def refresh_forward():
    d=core.open_research(); ps=[dict(r) for r in d.execute("SELECT * FROM v592_translation_policies WHERE state='FROZEN_FORWARD' ORDER BY holdout_expectancy DESC").fetchall()]; d.close()
    for p in ps:
        scan_forward(p)
    settle_forward()
    return ps


def pc(x):
    return "NA" if x is None else f"{100*float(x):.1f}%"


def display(ps,new):
    d=core.open_research()
    print("\033[2J\033[H",end="")
    print("="*174)
    print("MEMECOIN LAB — CHAMPION TRANSLATION & EXECUTABILITY LAB V5.9.2")
    print("="*174)
    print(f"POLICIES={len(ps)} NEW={new} | TRAIN/HOLDOUT=75/25 deterministic by token | ENTRY GAP<={MAX_ENTRY_GAP_S:g}s | COST={v59.total_cost_pct():.2f}%")
    print(f"Threshold is selected on DISCOVERY only using TP=target, SL={SELECTION_SL_PCT:g}%, chronological first-touch. HOLDOUT never tunes the rule.\n")
    for i,p in enumerate(ps,1):
        spec=json.loads(p["spec_json"])
        f=d.execute("""SELECT COUNT(*) n,SUM(status='OPEN') o,SUM(status='DONE') done,
          AVG(CASE WHEN status='DONE' THEN net_return END) avg FROM v592_forward_signals WHERE policy_id=?""",(p["policy_id"],)).fetchone()
        done=[float(r[0]) for r in d.execute("SELECT net_return FROM v592_forward_signals WHERE policy_id=? AND status='DONE' AND net_return IS NOT NULL",(p["policy_id"],)).fetchall()]
        print(f"#{i} {p['mutation_label']}  policy={p['policy_id']} target={spec.get('target')} shape={spec.get('stage1',spec.get('stage'))}->{spec.get('stage2','—')}")
        print(f"   FROZEN RULE score*dir >= {p['selected_threshold']:.4g} | TP={p['tp_pct']:.0f}% SL={p['sl_pct']:.0f}% hold={p['holding_s']}s")
        print(f"   TRAIN   N={p['train_n']} trades={p['train_trades']} expectancy={sf(p['train_expectancy'],0):+.2f}% med={sf(p['train_median_net'],0):+.2f}% win={pc(p['train_win_rate'])} PF={sf(p['train_profit_factor'],0):.2f}")
        print(f"   HOLDOUT N={p['holdout_n']} trades={p['holdout_trades']} expectancy={sf(p['holdout_expectancy'],0):+.2f}% med={sf(p['holdout_median_net'],0):+.2f}% win={pc(p['holdout_win_rate'])} PF={sf(p['holdout_profit_factor'],0):.2f} hit_lift={sf(p['holdout_lift_hit'],0):.2f}x")
        top=d.execute("SELECT * FROM v592_threshold_sweep WHERE policy_id=? ORDER BY score DESC LIMIT 4",(p["policy_id"],)).fetchall()
        print("   TRAIN THRESHOLD FRONTIER")
        for r in top:
            print(f"      th>={r['threshold']:.4g} trades={r['train_trades']:<4} hit={pc(r['hit_rate'])} exp={sf(r['expectancy'],0):+.2f}% med={sf(r['median_net'],0):+.2f}% win={pc(r['win_rate'])} PF={sf(r['profit_factor'],0):.2f}")
        print(f"   TRUE FORWARD signals={int(f['n'] or 0)} open={int(f['o'] or 0)} done={int(f['done'] or 0)} avg={sf(f['avg'],0):+.2f}% med={(statistics.median(done) if done else 0):+.2f}% win={(100*sum(x>0 for x in done)/len(done) if done else 0):.1f}%")
        print()
    d.close()
    print("Guardrail: a good TRAIN threshold is irrelevant if HOLDOUT fails. Only frozen post-cutoff forward trades can become clean economic evidence.")


def cycle():
    init(); new=seed(); ps=refresh_forward(); display(ps,new)
    d=core.open_research(); d.execute("""INSERT INTO v592_state(key,value_json,updated_at) VALUES('latest',?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at""",
      (core.canonical_json({"policies":len(ps),"new":new,"updated_at":time.time()}),time.time())); d.commit();d.close()


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try:cycle()
        except Exception as e:print("V5.9.2 error:",repr(e),flush=True)
        time.sleep(LOOP)


if __name__=="__main__":main()
