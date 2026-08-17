#!/usr/bin/env python3
"""Memecoin Lab V5.8 — Champion Observatory.

Makes champions observable as living research objects instead of static labels.

V5.8 never changes frozen rules or champion admission logic. It records periodic
snapshots of V5.6.2 champions and combines them with V5.7.x arena pressure and
V5.7.3 replication status to show:
- prospective evidence growth (N/rho/lift/confidence);
- recent direction and stability;
- descriptive proof/health bars (visual only, never a verdict gate);
- challenger pressure and defenses;
- whether winning descendants are only discoveries or temporally replicated;
- champion age and lifecycle state.

Research-only. No trading/signing.
"""
from __future__ import annotations

import json
import math
import os
import signal
import statistics
import time
from collections import defaultdict

import v41_core as core
import v561_diversity_gate as diversity
import v562_research_portfolio as registry

LOOP = float(os.environ.get("MEMECOIN_V58_LOOP_S", "5"))
SNAPSHOT_MIN_S = float(os.environ.get("MEMECOIN_V58_SNAPSHOT_MIN_S", "60"))
HISTORY_POINTS = int(os.environ.get("MEMECOIN_V58_HISTORY_POINTS", "16"))
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


def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, sf(x, lo)))


def tables(d):
    return {r[0] for r in d.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def init():
    registry.init()
    d = core.open_research()
    d.executescript("""
    CREATE TABLE IF NOT EXISTS v58_champion_snapshots(
      snapshot_id TEXT PRIMARY KEY,
      candidate_id TEXT NOT NULL,
      ts REAL NOT NULL,
      champion_state TEXT NOT NULL,
      n INTEGER NOT NULL,
      prospective_rho REAL,
      lift REAL,
      precision REAL,
      baseline_rate REAL,
      confidence REAL,
      proof_score REAL NOT NULL,
      arena_scientific_duels INTEGER NOT NULL DEFAULT 0,
      arena_challenger_wins INTEGER NOT NULL DEFAULT 0,
      arena_control_wins INTEGER NOT NULL DEFAULT 0,
      replicated_descendants INTEGER NOT NULL DEFAULT 0,
      strong_descendants INTEGER NOT NULL DEFAULT 0,
      future_ready_descendants INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_v58_snap_candidate_ts
      ON v58_champion_snapshots(candidate_id,ts);

    CREATE TABLE IF NOT EXISTS v58_champion_state(
      candidate_id TEXT PRIMARY KEY,
      lifecycle TEXT NOT NULL,
      proof_score REAL NOT NULL,
      delta_n INTEGER NOT NULL,
      delta_rho REAL,
      delta_lift REAL,
      delta_confidence REAL,
      scientific_duels INTEGER NOT NULL,
      challenger_wins INTEGER NOT NULL,
      control_wins INTEGER NOT NULL,
      ties INTEGER NOT NULL,
      replicated_descendants INTEGER NOT NULL,
      strong_descendants INTEGER NOT NULL,
      future_ready_descendants INTEGER NOT NULL,
      last_snapshot_at REAL,
      updated_at REAL NOT NULL,
      details_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS v58_state(
      key TEXT PRIMARY KEY,
      value_json TEXT NOT NULL,
      updated_at REAL NOT NULL
    );
    """)
    d.commit()
    d.close()


def candidate_specs():
    d = core.open_research()
    names = tables(d)
    if "v55_candidates" not in names:
        d.close()
        return {}
    out = {}
    for r in d.execute("SELECT candidate_id,kind,spec_json FROM v55_candidates").fetchall():
        try:
            spec = json.loads(r["spec_json"])
        except Exception:
            spec = {}
        out[r["candidate_id"]] = {
            "kind": r["kind"],
            "spec": spec,
            "scientific_key": diversity.scientific_key(r["kind"], spec),
        }
    d.close()
    return out


def proof_score(ch):
    """Visual evidence score only; never used as a scientific promotion gate."""
    n_score = clamp(int(ch.get("n") or 0) / 220.0)
    rho = sf(ch.get("prospective_rho"), 0.0)
    lift = sf(ch.get("lift"), 1.0)
    conf = clamp(ch.get("confidence"), 0.0, 1.0)
    rho_score = clamp((rho - 0.03) / 0.15)
    lift_score = clamp((lift - 1.00) / 0.50)
    return 100.0 * (0.35*n_score + 0.25*rho_score + 0.20*lift_score + 0.20*conf)


def arena_by_science():
    d = core.open_research()
    names = tables(d)
    out = defaultdict(lambda: {"duels":0,"wins":0,"losses":0,"ties":0})
    if "v572_scientific_duels" in names:
        for r in d.execute("""
          SELECT control_scientific_key,outcome,COUNT(*) n
          FROM v572_scientific_duels
          GROUP BY control_scientific_key,outcome
        """).fetchall():
            x = out[r["control_scientific_key"]]
            n = int(r["n"])
            x["duels"] += n
            if r["outcome"] == "CHALLENGER_WINS": x["wins"] += n
            elif r["outcome"] == "CONTROL_WINS": x["losses"] += n
            else: x["ties"] += n
    d.close()
    return out


def descendants_by_control():
    d = core.open_research()
    names = tables(d)
    out = defaultdict(lambda: {"replicated":0,"strong":0,"ready":0,"discoveries":0})
    if "v573_replication_status" in names:
        for r in d.execute("""
          SELECT control_scientific_key,status,COUNT(*) n
          FROM v573_replication_status
          GROUP BY control_scientific_key,status
        """).fetchall():
            x = out[r["control_scientific_key"]]
            n = int(r["n"])
            if r["status"] == "READY_FOR_FUTURE_ONLY": x["ready"] += n
            elif r["status"] == "STRONG_REPLICATION": x["strong"] += n
            elif r["status"] == "REPLICATED": x["replicated"] += n
            else: x["discoveries"] += n
    d.close()
    return out


def latest_two(candidate_id):
    d = core.open_research()
    rows = [dict(r) for r in d.execute("""
      SELECT * FROM v58_champion_snapshots
      WHERE candidate_id=? ORDER BY ts DESC LIMIT 2
    """, (candidate_id,)).fetchall()]
    d.close()
    return rows


def should_snapshot(candidate_id, now):
    d = core.open_research()
    r = d.execute("SELECT MAX(ts) t FROM v58_champion_snapshots WHERE candidate_id=?", (candidate_id,)).fetchone()
    d.close()
    last = sf(r["t"] if r else None)
    return last is None or now-last >= SNAPSHOT_MIN_S


def lifecycle(ch, delta_n, delta_rho, pressure, descendants):
    cs = str(ch.get("champion_state") or "CHAMPION")
    n = int(ch.get("n") or 0)
    rho = sf(ch.get("prospective_rho"), 0.0)

    if cs == "RETIRED": return "RETIRED"
    if cs == "DECAYING": return "DECAYING"
    if descendants.get("ready",0) > 0: return "EVOLUTION_READY"
    if pressure.get("wins",0) > pressure.get("losses",0) and pressure.get("wins",0) >= 2:
        return "UNDER_PRESSURE"
    if cs == "STABLE":
        if delta_n > 0 and (delta_rho is None or delta_rho >= -0.01): return "STABLE_GROWING"
        return "STABLE"
    if n < 80: return "YOUNG"
    if delta_n > 0 and (delta_rho is None or delta_rho >= 0): return "GROWING"
    if rho >= 0.08: return "CONFIRMING"
    return "CHAMPION"


def snapshot_and_refresh():
    registry.refresh_registry()
    specs = candidate_specs()
    pressure = arena_by_science()
    descendants = descendants_by_control()

    d = core.open_research()
    champs = [dict(r) for r in d.execute("SELECT * FROM v562_champion_registry ORDER BY confidence DESC,n DESC").fetchall()]
    d.close()
    now = time.time()

    for ch in champs:
        cid = ch["candidate_id"]
        sk = specs.get(cid,{}).get("scientific_key")
        p = pressure.get(sk,{"duels":0,"wins":0,"losses":0,"ties":0})
        ds = descendants.get(sk,{"replicated":0,"strong":0,"ready":0,"discoveries":0})
        ps = proof_score(ch)

        if should_snapshot(cid, now):
            sid = "S58_" + core.fingerprint({"c":cid,"bucket":int(now//SNAPSHOT_MIN_S)}, "v58snap:")[:22]
            d = core.open_research()
            d.execute("""
              INSERT OR IGNORE INTO v58_champion_snapshots(
                snapshot_id,candidate_id,ts,champion_state,n,prospective_rho,lift,
                precision,baseline_rate,confidence,proof_score,arena_scientific_duels,
                arena_challenger_wins,arena_control_wins,replicated_descendants,
                strong_descendants,future_ready_descendants
              ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                sid,cid,now,ch["champion_state"],int(ch["n"]),ch["prospective_rho"],ch["lift"],
                ch["precision"],ch["baseline_rate"],ch["confidence"],ps,p["duels"],p["wins"],p["losses"],
                ds["replicated"],ds["strong"],ds["ready"]
            ))
            d.commit(); d.close()

        hist = latest_two(cid)
        cur = hist[0] if hist else None
        prev = hist[1] if len(hist)>1 else None
        dn = (int(cur["n"])-int(prev["n"])) if cur and prev else 0
        dr = (sf(cur["prospective_rho"],0)-sf(prev["prospective_rho"],0)) if cur and prev else None
        dl = (sf(cur["lift"],0)-sf(prev["lift"],0)) if cur and prev else None
        dc = (sf(cur["confidence"],0)-sf(prev["confidence"],0)) if cur and prev else None
        lc = lifecycle(ch,dn,dr,p,ds)

        details = {
            "family": ch.get("family"), "kind": ch.get("kind"), "generation": ch.get("generation"),
            "redundancy_role": ch.get("redundancy_role"), "scientific_key": sk,
            "arena": p, "descendants": ds, "spec": specs.get(cid,{}).get("spec",{}),
        }
        d = core.open_research()
        d.execute("""
          INSERT INTO v58_champion_state(
            candidate_id,lifecycle,proof_score,delta_n,delta_rho,delta_lift,delta_confidence,
            scientific_duels,challenger_wins,control_wins,ties,replicated_descendants,
            strong_descendants,future_ready_descendants,last_snapshot_at,updated_at,details_json
          ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(candidate_id) DO UPDATE SET
            lifecycle=excluded.lifecycle,proof_score=excluded.proof_score,delta_n=excluded.delta_n,
            delta_rho=excluded.delta_rho,delta_lift=excluded.delta_lift,delta_confidence=excluded.delta_confidence,
            scientific_duels=excluded.scientific_duels,challenger_wins=excluded.challenger_wins,
            control_wins=excluded.control_wins,ties=excluded.ties,replicated_descendants=excluded.replicated_descendants,
            strong_descendants=excluded.strong_descendants,future_ready_descendants=excluded.future_ready_descendants,
            last_snapshot_at=excluded.last_snapshot_at,updated_at=excluded.updated_at,details_json=excluded.details_json
        """, (cid,lc,ps,dn,dr,dl,dc,p["duels"],p["wins"],p["losses"],p["ties"],ds["replicated"],ds["strong"],ds["ready"],cur["ts"] if cur else None,now,core.canonical_json(details)))
        d.commit(); d.close()

    return champs


def bar(value, width=18):
    v = clamp(value/100.0)
    n = int(round(v*width))
    return "█"*n + "░"*(width-n)


def spark(values):
    blocks = "▁▂▃▄▅▆▇█"
    vals = [sf(x) for x in values]
    vals = [x for x in vals if x is not None]
    if not vals: return "—"
    lo,hi=min(vals),max(vals)
    if abs(hi-lo)<1e-12: return blocks[3]*len(vals)
    return "".join(blocks[min(7,max(0,int(round((x-lo)/(hi-lo)*7))))] for x in vals)


def histories(cid):
    d=core.open_research()
    rows=[dict(r) for r in d.execute("""
      SELECT * FROM v58_champion_snapshots WHERE candidate_id=?
      ORDER BY ts DESC LIMIT ?
    """,(cid,HISTORY_POINTS)).fetchall()]
    d.close(); rows.reverse(); return rows


def display():
    d=core.open_research()
    rows=[dict(r) for r in d.execute("""
      SELECT r.*,s.lifecycle,s.proof_score,s.delta_n,s.delta_rho,s.delta_lift,
             s.scientific_duels,s.challenger_wins,s.control_wins,s.ties,
             s.replicated_descendants,s.strong_descendants,s.future_ready_descendants,
             s.details_json
      FROM v562_champion_registry r JOIN v58_champion_state s USING(candidate_id)
      ORDER BY s.proof_score DESC,r.confidence DESC,r.n DESC
    """).fetchall()]
    d.close()

    states=defaultdict(int)
    for r in rows: states[r["lifecycle"]]+=1

    print("\033[2J\033[H",end="")
    print("="*164)
    print("MEMECOIN LAB — CHAMPION OBSERVATORY V5.8")
    print("="*164)
    print(f"CHAMPIONS={len(rows)} | " + " | ".join(f"{k}={v}" for k,v in sorted(states.items())))
    print("Visual proof score is descriptive only: N + prospective rho + lift + confidence. It never promotes a rule.\n")

    print("CHAMPION LIFECYCLE BOARD")
    print("-"*164)
    for i,r in enumerate(rows[:12],1):
        h=histories(r["candidate_id"])
        rho_hist=[x["prospective_rho"] for x in h]
        n_hist=[x["n"] for x in h]
        try: det=json.loads(r["details_json"])
        except Exception: det={}
        spec=det.get("spec",{})
        shape=(f"{spec.get('stage1')}→{spec.get('stage2')}" if 'stage1' in spec else str(spec.get('stage','?')))
        print(f"#{i:02d} {r['lifecycle']:<18} {r['family']:<20} G{r['generation']} {r['kind']:<18} id={r['candidate_id'][:12]}")
        print(f"    PROOF {r['proof_score']:5.1f} [{bar(r['proof_score'])}]  N={r['n']:<4} ΔN={r['delta_n']:+4d}  rho={sf(r['prospective_rho'],0):+.3f} Δrho={sf(r['delta_rho'],0):+.3f}  lift={sf(r['lift'],0):.2f} Δlift={sf(r['delta_lift'],0):+.2f}  conf={sf(r['confidence'],0):.2f}")
        print(f"    GROWTH N:{spark(n_hist):<18} rho:{spark(rho_hist):<18}  shape={shape} horizon={spec.get('horizon','?')} target={spec.get('target','?')} feature={spec.get('feature',spec.get('weak','?'))}")
        print(f"    ARENA unique_duels={r['scientific_duels']}  challengers_win={r['challenger_wins']}  control_defends={r['control_wins']}  ties={r['ties']}  | DESCENDANTS replicated={r['replicated_descendants']} strong={r['strong_descendants']} future_ready={r['future_ready_descendants']}")

    print("\nLIFECYCLE LEGEND")
    print("YOUNG → GROWING → CONFIRMING → STABLE_GROWING/STABLE; UNDER_PRESSURE when scientific challengers repeatedly beat it; EVOLUTION_READY when a descendant earns future-only testing; DECAYING/RETIRED remain governed by V5.6.2.")
    print("\nGuardrail: V5.8 observes and visualizes champions. Admission, freezing, retirement and future-only validation remain owned by their original pipelines.")


def cycle():
    champs=snapshot_and_refresh()
    display()
    d=core.open_research(); state={"champions":len(champs),"updated_at":time.time()}
    d.execute("""
      INSERT INTO v58_state(key,value_json,updated_at) VALUES('latest',?,?)
      ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at
    """,(core.canonical_json(state),time.time())); d.commit(); d.close()


def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); init()
    while not STOP:
        try: cycle()
        except Exception as e: print("V5.8 error:",repr(e),flush=True)
        time.sleep(LOOP)


if __name__=='__main__': main()
