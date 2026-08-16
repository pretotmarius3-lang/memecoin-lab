#!/usr/bin/env python3
"""Memecoin Lab — unified technical master dashboard.

One read-only control plane for V5.1/V5.2 data, V4.6 prospective science,
V4.7 ensembles, V4.8 meta-learning and V4.9 recursive side research.

No mutation. No trading. No signing.
"""
from __future__ import annotations

import html
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
RDB = Path(os.environ.get("MEMECOIN_RESEARCH_V41_DB", ROOT / "research_v4_1.db"))
V5 = Path(os.environ.get("MEMECOIN_V5_DB", ROOT / "v5_raw_events.db"))
V52 = Path(os.environ.get("MEMECOIN_V52_DB", ROOT / "v52_features.db"))
HOST = os.environ.get("MEMECOIN_MASTER_DASH_HOST", "127.0.0.1")
PORT = int(os.environ.get("MEMECOIN_MASTER_DASH_PORT", "8780"))


def dbopen(path):
    if not path.exists():
        return None
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")
    return db


def table(db, name):
    return bool(db and db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone())


def rows(db, sql, args=()):
    try:
        return [dict(r) for r in db.execute(sql, args).fetchall()]
    except Exception:
        return []


def scalar(db, sql, args=(), default=0):
    try:
        r = db.execute(sql, args).fetchone()
        return default if r is None or r[0] is None else r[0]
    except Exception:
        return default


def sf(x, default=None):
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def age(ts):
    if ts is None:
        return None
    try:
        return max(0.0, time.time() - float(ts))
    except Exception:
        return None


def family(feature):
    f = (feature or "").lower()
    if any(k in f for k in ("wallet", "hhi", "repeat")):
        return "WALLET"
    if any(k in f for k in ("price", "return", "range")):
        return "PRICE"
    if any(k in f for k in ("flow", "buy_ratio", "net_sol", "gross_sol")):
        return "FLOW"
    if any(k in f for k in ("swap", "trade", "activity")):
        return "ACTIVITY"
    return "OTHER"


def collect():
    out = {
        "now": time.time(), "paths": {"research": str(RDB), "raw": str(V5), "features": str(V52)},
        "jobs": {}, "workers": [], "roles": {}, "beliefs": {}, "conclusions": {},
        "signals": [], "passes": [], "ensembles": [], "families": [], "agenda": [],
        "side": {}, "side_results": [], "pipeline": {}, "branches": [],
    }

    # -------- raw acquisition --------
    d = dbopen(V5)
    if d:
        if table(d, "v5_raw_transactions"):
            out["pipeline"]["raw_tx"] = int(scalar(d, "SELECT COUNT(*) FROM v5_raw_transactions"))
            out["pipeline"]["raw_age"] = age(scalar(d, "SELECT MAX(observed_at) FROM v5_raw_transactions", default=None))
            out["pipeline"]["raw_sources"] = rows(d, "SELECT source_program source,COUNT(*) n FROM v5_raw_transactions GROUP BY source_program ORDER BY n DESC")
        if table(d, "v51_signature_spool"):
            ss = rows(d, "SELECT status,COUNT(*) n FROM v51_signature_spool GROUP BY status")
            out["pipeline"]["spool"] = {r["status"]: int(r["n"]) for r in ss}
            out["pipeline"]["spool_total"] = int(sum(r["n"] for r in ss))
            out["pipeline"]["spool_latest"] = age(scalar(d, "SELECT MAX(first_seen) FROM v51_signature_spool", default=None))
            out["pipeline"]["spool_oldest_pending"] = age(scalar(d, "SELECT MIN(first_seen) FROM v51_signature_spool WHERE status='PENDING'", default=None))
        d.close()

    # -------- decoded features --------
    d = dbopen(V52)
    if d:
        if table(d, "v52_swaps"):
            out["pipeline"]["swaps"] = int(scalar(d, "SELECT COUNT(*) FROM v52_swaps"))
            out["pipeline"]["tokens"] = int(scalar(d, "SELECT COUNT(DISTINCT token_mint) FROM v52_swaps"))
            out["pipeline"]["decode_age"] = age(scalar(d, "SELECT MAX(timestamp) FROM v52_swaps", default=None))
        if table(d, "v52_outcomes"):
            out["pipeline"]["ready"] = int(scalar(d, "SELECT COUNT(*) FROM v52_outcomes WHERE ready=1"))
        if table(d, "v52_snapshots"):
            out["pipeline"]["snapshots"] = int(scalar(d, "SELECT COUNT(*) FROM v52_snapshots"))
        d.close()

    # -------- research brain --------
    d = dbopen(RDB)
    if d:
        if table(d, "v41_jobs"):
            out["jobs"] = {r["status"]: int(r["n"]) for r in rows(d, "SELECT status,COUNT(*) n FROM v41_jobs GROUP BY status")}
        if table(d, "v41_workers"):
            out["workers"] = rows(d, "SELECT * FROM v41_workers ORDER BY last_heartbeat DESC LIMIT 40")
        if table(d, "v41_hypotheses") and table(d, "v41_jobs"):
            out["branches"] = rows(d, """SELECT h.branch,COUNT(DISTINCT h.hypothesis_id) hyp,
                SUM(CASE WHEN j.status='QUEUED' THEN 1 ELSE 0 END) queued,
                SUM(CASE WHEN j.status='RUNNING' THEN 1 ELSE 0 END) running,
                SUM(CASE WHEN j.status='DONE' THEN 1 ELSE 0 END) done,
                SUM(CASE WHEN j.status='FAILED' THEN 1 ELSE 0 END) failed
                FROM v41_hypotheses h LEFT JOIN v41_jobs j ON j.hypothesis_id=h.hypothesis_id
                GROUP BY h.branch ORDER BY done DESC""")
        if table(d, "v45_conclusions"):
            out["conclusions"] = {r["classification"]: int(r["n"]) for r in rows(d, "SELECT classification,COUNT(*) n FROM v45_conclusions GROUP BY classification")}
        if table(d, "v46_beliefs"):
            out["beliefs"] = {r["status"]: int(r["n"]) for r in rows(d, "SELECT status,COUNT(*) n FROM v46_beliefs GROUP BY status")}
        if table(d, "v48_signal_rankings"):
            out["signals"] = rows(d, "SELECT * FROM v48_signal_rankings ORDER BY live_score DESC LIMIT 80")
            out["roles"] = {r["role"]: int(r["n"]) for r in rows(d, "SELECT role,COUNT(*) n FROM v48_signal_rankings GROUP BY role")}
        if table(d, "v46_prospective_candidates") and table(d, "v46_beliefs"):
            out["passes"] = rows(d, """SELECT c.candidate_id,c.feature,c.stage_s,c.horizon_s,c.target,c.direction,c.threshold,c.data_cutoff,
                b.status,b.n,b.baseline_rate,b.precision,b.lift,b.prospective_rho,b.confidence
                FROM v46_prospective_candidates c JOIN v46_beliefs b ON b.candidate_id=c.candidate_id
                WHERE b.status IN ('PASS','WATCH','FAIL')
                ORDER BY CASE b.status WHEN 'PASS' THEN 0 WHEN 'WATCH' THEN 1 ELSE 2 END,b.confidence DESC,b.n DESC LIMIT 160""")
        if table(d, "v47_ensemble_rankings"):
            out["ensembles"] = rows(d, "SELECT * FROM v48_ensemble_rankings ORDER BY live_score DESC LIMIT 50") if table(d, "v48_ensemble_rankings") else []
        elif table(d, "v48_ensemble_rankings"):
            out["ensembles"] = rows(d, "SELECT * FROM v48_ensemble_rankings ORDER BY live_score DESC LIMIT 50")
        if table(d, "v48_family_budget"):
            out["families"] = rows(d, "SELECT * FROM v48_family_budget ORDER BY budget_weight DESC")
        if table(d, "v48_research_agenda"):
            out["agenda"] = rows(d, "SELECT * FROM v48_research_agenda WHERE state='OPEN' ORDER BY priority,updated_at DESC LIMIT 40")
        if table(d, "v49_side_experiments"):
            out["side"]["experiments"] = int(scalar(d, "SELECT COUNT(*) FROM v49_side_experiments"))
            out["side"]["states"] = {r["status"]: int(r["n"]) for r in rows(d, "SELECT status,COUNT(*) n FROM v49_side_experiments GROUP BY status")}
            out["side"]["kinds"] = rows(d, "SELECT kind,COUNT(*) n FROM v49_side_experiments GROUP BY kind ORDER BY n DESC")
        if table(d, "v49_side_results"):
            out["side"]["results"] = int(scalar(d, "SELECT COUNT(*) FROM v49_side_results"))
            out["side"]["comparisons"] = {r["comparison"]: int(r["n"]) for r in rows(d, "SELECT comparison,COUNT(*) n FROM v49_side_results GROUP BY comparison")}
            out["side_results"] = rows(d, """SELECT r.*,e.parent_feature,e.spec_json
                FROM v49_side_results r LEFT JOIN v49_side_experiments e ON e.experiment_id=r.experiment_id
                ORDER BY CASE r.comparison WHEN 'IMPROVED' THEN 0 WHEN 'SAME' THEN 1 ELSE 2 END,
                         COALESCE(r.delta_rho,-999) DESC, r.updated_at DESC LIMIT 60""")
        d.close()

    now = time.time()
    out["live_workers"] = sum(1 for w in out["workers"] if now - float(w.get("last_heartbeat") or 0) < 15)
    out["failed_jobs"] = int(out["jobs"].get("FAILED", 0))

    # operating state
    da = out["pipeline"].get("decode_age")
    ra = out["pipeline"].get("raw_age")
    pending = int(out["pipeline"].get("spool", {}).get("PENDING", 0))
    if da is None:
        state, reason = "DOWN", "NO DECODE DATA"
    elif da > 600:
        state, reason = "STALE", "DECODER STALE"
    elif out["live_workers"] <= 0:
        state, reason = "WARN", "NO RESEARCH WORKERS"
    elif ra is not None and ra > 180:
        state, reason = "WARN", "RAW FEED LAG"
    elif pending > 50000:
        state, reason = "WARN", "SPOOL PRESSURE"
    else:
        state, reason = "OK", "SYSTEM NOMINAL"
    out["state"] = state
    out["state_reason"] = reason

    # feature family live summary from ranked signals
    fam = defaultdict(lambda: {"signals": 0, "champions": 0, "passes": 0, "score": [], "rho": [], "lift": []})
    pass_ids = {p["candidate_id"] for p in out["passes"] if p.get("status") == "PASS"}
    for s in out["signals"]:
        f = s.get("family") or family(s.get("feature"))
        x = fam[f]
        x["signals"] += 1
        x["champions"] += int(s.get("role") == "CHAMPION")
        x["passes"] += int(s.get("candidate_id") in pass_ids)
        if sf(s.get("live_score")) is not None: x["score"].append(float(s["live_score"]))
        if sf(s.get("rho")) is not None: x["rho"].append(float(s["rho"]))
        if sf(s.get("lift")) is not None: x["lift"].append(float(s["lift"]))
    out["family_live"] = []
    for name, x in fam.items():
        out["family_live"].append({
            "family": name, "signals": x["signals"], "champions": x["champions"], "passes": x["passes"],
            "score": sum(x["score"])/len(x["score"]) if x["score"] else None,
            "rho": sum(x["rho"])/len(x["rho"]) if x["rho"] else None,
            "lift": sum(x["lift"])/len(x["lift"]) if x["lift"] else None,
        })
    out["family_live"].sort(key=lambda x: (x["champions"], x["passes"], x["score"] or 0), reverse=True)
    return out


PAGE = r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Memecoin Lab — Master Terminal</title>
<style>
:root{--bg:#07090d;--p:#0b0f15;--p2:#0e131b;--line:#202936;--text:#e7edf5;--muted:#778395;--soft:#a9b3c2;--green:#48d597;--cyan:#56c7e8;--amber:#e0af68;--red:#e56b7a;--blue:#7097e8;--purple:#a58ce8}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:13px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}button{font:inherit}.shell{max-width:1900px;margin:auto;padding:18px}.top{display:flex;align-items:flex-start;justify-content:space-between;border-bottom:1px solid var(--line);padding-bottom:13px;margin-bottom:13px}.title{font:700 21px -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;letter-spacing:-.3px}.sub{color:var(--muted);margin-top:4px;font-size:11px}.status{display:flex;gap:8px;align-items:center;color:var(--soft);font-size:11px}.dot{width:8px;height:8px;border-radius:50%;background:var(--muted)}.dot.OK{background:var(--green)}.dot.WARN,.dot.STALE{background:var(--amber)}.dot.DOWN{background:var(--red)}
.tabs{display:flex;gap:4px;margin-bottom:12px}.tab{border:1px solid var(--line);background:transparent;color:var(--muted);padding:7px 10px;border-radius:5px;cursor:pointer}.tab.on{background:var(--p2);color:var(--text);border-color:#334050}.view{display:none}.view.on{display:block}
.metrics{display:grid;grid-template-columns:repeat(12,minmax(85px,1fr));gap:7px}.m{background:var(--p);border:1px solid var(--line);border-radius:7px;padding:10px;min-height:70px}.mk{font-size:9px;color:var(--muted);letter-spacing:.8px;text-transform:uppercase}.mv{font:700 22px -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;margin-top:6px}.ms{font-size:9px;color:var(--muted);margin-top:4px}.good{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}.cyan{color:var(--cyan)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:9px}.grid3{display:grid;grid-template-columns:1.2fr 1fr .8fr;gap:9px}.panel{background:var(--p);border:1px solid var(--line);border-radius:7px;padding:12px;margin-top:9px;overflow:hidden}.ph{display:flex;justify-content:space-between;align-items:center;margin-bottom:9px}.pt{font-size:10px;color:var(--soft);font-weight:700;letter-spacing:.8px;text-transform:uppercase}.meta{font-size:9px;color:var(--muted)}
table{width:100%;border-collapse:collapse}th{font-size:9px;color:var(--muted);font-weight:500;text-transform:uppercase;letter-spacing:.5px;text-align:left;padding:7px 6px;border-bottom:1px solid var(--line)}td{font-size:11px;padding:8px 6px;border-bottom:1px solid #151c26;vertical-align:middle}tr:last-child td{border-bottom:0}.mono{font-family:inherit;font-weight:700}.pill{display:inline-block;border:1px solid var(--line);border-radius:99px;padding:2px 6px;font-size:9px}.CHAMPION,.PASS,.IMPROVED,.ELITE_STABLE,.OK{color:var(--green)}.CONTENDER,.WATCH,.STABLE{color:var(--cyan)}.DECAYING,.MIXED,.WAITING{color:var(--amber)}.RETIRE,.FAIL,.WORSE,.FRAGILE{color:var(--red)}
.bar{height:3px;background:#17202b;border-radius:5px;overflow:hidden;margin-top:5px}.bar i{display:block;height:100%;background:var(--cyan)}.kv{display:grid;grid-template-columns:1fr auto;gap:5px;padding:6px 0;border-bottom:1px solid #151c26}.kv:last-child{border-bottom:0}.k2{color:var(--muted)}.v2{color:var(--text)}.agenda{border-left:2px solid var(--line);padding:7px 9px;margin:5px 0;background:#0a0e14}.agenda b{font-size:10px}.agenda div{font-size:10px;color:var(--muted);margin-top:3px;line-height:1.35}.familyrow{display:grid;grid-template-columns:1fr 55px 55px 70px;gap:7px;padding:7px 0;border-bottom:1px solid #151c26}.familyrow:last-child{border-bottom:0}.sidekind{display:grid;grid-template-columns:1fr auto;gap:10px;padding:6px 0;border-bottom:1px solid #151c26}.muted{color:var(--muted)}.right{text-align:right}.nowrap{white-space:nowrap}.scroll{max-height:525px;overflow:auto}.pipeline{display:grid;grid-template-columns:repeat(5,1fr);gap:0;border:1px solid var(--line);border-radius:7px;overflow:hidden;margin-top:9px}.node{background:var(--p);padding:12px;border-right:1px solid var(--line)}.node:last-child{border-right:0}.node .n{font-size:9px;color:var(--muted);text-transform:uppercase}.node .big{font-size:18px;font-weight:700;margin-top:5px}.arrow{color:var(--muted);float:right}
@media(max-width:1250px){.metrics{grid-template-columns:repeat(6,1fr)}.grid3{grid-template-columns:1fr}.grid2{grid-template-columns:1fr}.pipeline{grid-template-columns:1fr}.node{border-right:0;border-bottom:1px solid var(--line)}}
</style></head><body><div class="shell">
<div class="top"><div><div class="title">MEMECOIN LAB / MASTER TERMINAL</div><div class="sub">one read-only view · acquisition → decode → research → prospective → meta-learning → recursive lab</div></div><div class="status"><span id="clock">--:--:--</span><span class="dot" id="sysdot"></span><b id="system">LOADING</b></div></div>
<div class="tabs"><button class="tab on" data-v="overview">OVERVIEW</button><button class="tab" data-v="signals">SIGNALS</button><button class="tab" data-v="research">RESEARCH</button><button class="tab" data-v="pipeline">PIPELINE</button></div>
<div id="overview" class="view on"><div id="metrics" class="metrics"></div><div id="pipebar" class="pipeline"></div><div class="grid3"><div class="panel"><div class="ph"><div class="pt">Live edge ranking</div><div class="meta">current forward evidence</div></div><div id="topSignals"></div></div><div class="panel"><div class="ph"><div class="pt">Prospective state</div><div class="meta">frozen rules only</div></div><div id="prospective"></div></div><div class="panel"><div class="ph"><div class="pt">Research allocation</div><div class="meta">meta-brain budget</div></div><div id="familyBudget"></div></div></div><div class="grid2"><div class="panel"><div class="ph"><div class="pt">Recursive side lab</div><div class="meta">V4.9 exploration</div></div><div id="sideSummary"></div></div><div class="panel"><div class="ph"><div class="pt">Autonomous agenda</div><div class="meta">next research actions</div></div><div id="agenda"></div></div></div></div>
<div id="signals" class="view"><div class="grid2"><div class="panel"><div class="ph"><div class="pt">Champion / contender board</div></div><div class="scroll"><table><thead><tr><th>#</th><th>feature</th><th>family</th><th>target</th><th>n</th><th>rho</th><th>lift</th><th>trend</th><th>score</th><th>role</th></tr></thead><tbody id="signalTable"></tbody></table></div></div><div class="panel"><div class="ph"><div class="pt">Prospective vault</div></div><div class="scroll"><table><thead><tr><th>state</th><th>feature</th><th>context</th><th>n</th><th>rho</th><th>lift</th><th>precision</th><th>base</th></tr></thead><tbody id="passTable"></tbody></table></div></div></div><div class="panel"><div class="ph"><div class="pt">Ensemble frontier</div></div><table><thead><tr><th>role</th><th>context</th><th>members</th><th>n</th><th>rho</th><th>lift</th><th>precision</th><th>score</th></tr></thead><tbody id="ensembleTable"></tbody></table></div></div>
<div id="research" class="view"><div class="grid2"><div class="panel"><div class="ph"><div class="pt">Recursive experiment results</div><div class="meta">child vs parent</div></div><div class="scroll"><table><thead><tr><th>comparison</th><th>kind</th><th>parent</th><th>n</th><th>rho</th><th>Δrho</th><th>verdict</th></tr></thead><tbody id="sideTable"></tbody></table></div></div><div class="panel"><div class="ph"><div class="pt">Research branches</div></div><table><thead><tr><th>branch</th><th>hyp</th><th>q</th><th>run</th><th>done</th><th>fail</th></tr></thead><tbody id="branchTable"></tbody></table></div></div><div class="panel"><div class="ph"><div class="pt">Family live map</div></div><div id="familyLive"></div></div></div>
<div id="pipeline" class="view"><div class="grid2"><div class="panel"><div class="ph"><div class="pt">Data factory</div></div><div id="pipelineDetail"></div></div><div class="panel"><div class="ph"><div class="pt">Worker / queue health</div></div><div id="workerDetail"></div></div></div><div class="panel"><div class="ph"><div class="pt">Database paths</div></div><div id="paths"></div></div></div>
</div><script>
const $=id=>document.getElementById(id);const n=(x,d=3)=>x===null||x===undefined||Number.isNaN(Number(x))?'—':Number(x).toFixed(d);const ni=x=>x===null||x===undefined?'—':Number(x).toLocaleString();const pc=x=>x===null||x===undefined?'—':(100*Number(x)).toFixed(1)+'%';const ag=x=>x===null||x===undefined?'—':Number(x)<60?Number(x).toFixed(0)+'s':Number(x)<3600?(Number(x)/60).toFixed(1)+'m':(Number(x)/3600).toFixed(1)+'h';const cls=x=>String(x||'').replace(/[^A-Za-z0-9_-]/g,'');
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab').forEach(x=>x.classList.remove('on'));document.querySelectorAll('.view').forEach(x=>x.classList.remove('on'));b.classList.add('on');$(b.dataset.v).classList.add('on')});
function metric(k,v,s='',c=''){return `<div class="m"><div class="mk">${k}</div><div class="mv ${c}">${v}</div><div class="ms">${s}</div></div>`}
function bar(v){let w=Math.max(0,Math.min(100,100*Number(v||0)));return `<div class="bar"><i style="width:${w}%"></i></div>`}
function render(d){$('clock').textContent=new Date().toLocaleTimeString();$('sysdot').className='dot '+d.state;$('system').textContent=d.state_reason;
let p=d.pipeline||{},r=d.roles||{},b=d.beliefs||{},c=d.conclusions||{},s=d.side||{},jobs=d.jobs||{};
$('metrics').innerHTML=[metric('workers',ni(d.live_workers),'active research',d.live_workers?'good':'bad'),metric('queue',ni(jobs.QUEUED||0),'pending jobs'),metric('done',ni(jobs.DONE||0),'completed jobs'),metric('champions',ni(r.CHAMPION||0),'meta-ranked','good'),metric('prospective pass',ni(b.PASS||0),'unseen-token wins','good'),metric('watch',ni(b.WATCH||0),'maturing','cyan'),metric('confirmed',ni(c.CONFIRMED_SIGNAL||0),'historical stable'),metric('side improved',ni((s.comparisons||{}).IMPROVED||0),'recursive wins','good'),metric('tokens',ni(p.tokens||0),'decoded population'),metric('swaps',ni(p.swaps||0),'decoded trades'),metric('decode age',ag(p.decode_age),'feature freshness',p.decode_age>600?'bad':p.decode_age>120?'warn':'good'),metric('raw age',ag(p.raw_age),'collector freshness',p.raw_age>300?'bad':p.raw_age>120?'warn':'good')].join('');
let spool=p.spool||{};$('pipebar').innerHTML=`<div class="node"><div class="n">01 / Helius ingest <span class="arrow">→</span></div><div class="big">${ni(p.spool_total||0)}</div><div class="ms">spooled · pending ${ni(spool.PENDING||0)}</div></div><div class="node"><div class="n">02 / Raw RPC <span class="arrow">→</span></div><div class="big">${ni(p.raw_tx||0)}</div><div class="ms">latest ${ag(p.raw_age)}</div></div><div class="node"><div class="n">03 / Decode <span class="arrow">→</span></div><div class="big">${ni(p.swaps||0)}</div><div class="ms">${ni(p.tokens||0)} tokens · age ${ag(p.decode_age)}</div></div><div class="node"><div class="n">04 / Outcomes <span class="arrow">→</span></div><div class="big">${ni(p.ready||0)}</div><div class="ms">mature labels</div></div><div class="node"><div class="n">05 / Science</div><div class="big">${ni(b.PASS||0)} PASS</div><div class="ms">${ni(r.CHAMPION||0)} champions · ${ni((s.comparisons||{}).IMPROVED||0)} side improvements</div></div>`;
let sig=(d.signals||[]).slice(0,10);$('topSignals').innerHTML='<table><thead><tr><th>#</th><th>feature</th><th>rho</th><th>lift</th><th>trend</th><th>score</th></tr></thead><tbody>'+sig.map((x,i)=>`<tr><td>${String(i+1).padStart(2,'0')}</td><td><span class="mono">${x.feature}</span><div class="ms">${x.family} · ${x.target}</div></td><td>${n(x.rho)}</td><td>${n(x.lift,2)}x</td><td class="${cls(x.trend)}">${x.trend}</td><td>${n(x.live_score)}${bar(x.live_score)}</td></tr>`).join('')+'</tbody></table>';
$('prospective').innerHTML=['PASS','WATCH','WAITING','FAIL'].map(k=>`<div class="kv"><div class="k2">${k}</div><div class="v2 ${k}">${ni(b[k]||0)}</div></div>`).join('')+`<div class="kv"><div class="k2">confirmed signals</div><div>${ni(c.CONFIRMED_SIGNAL||0)}</div></div><div class="kv"><div class="k2">contradictory</div><div>${ni(c.CONTRADICTORY||0)}</div></div>`;
$('familyBudget').innerHTML=(d.families||[]).map(x=>`<div class="familyrow"><div><b>${x.family}</b><div class="ms">${x.action}</div>${bar(x.budget_weight)}</div><div class="right">${pc(x.budget_weight)}</div><div class="right">${x.champion_count} C</div><div class="right">${x.pass_count} pass</div></div>`).join('')||'<span class="muted">V4.8 budget not available.</span>';
$('sideSummary').innerHTML=`<div class="kv"><div class="k2">experiments</div><div>${ni(s.experiments||0)}</div></div><div class="kv"><div class="k2">results</div><div>${ni(s.results||0)}</div></div><div class="kv"><div class="k2">improved</div><div class="IMPROVED">${ni((s.comparisons||{}).IMPROVED||0)}</div></div><div class="kv"><div class="k2">same</div><div>${ni((s.comparisons||{}).SAME||0)}</div></div><div class="kv"><div class="k2">worse</div><div class="WORSE">${ni((s.comparisons||{}).WORSE||0)}</div></div>`+(s.kinds||[]).slice(0,8).map(x=>`<div class="sidekind"><span>${x.kind}</span><b>${ni(x.n)}</b></div>`).join('');
$('agenda').innerHTML=(d.agenda||[]).slice(0,12).map(x=>`<div class="agenda"><b>P${x.priority} · ${x.agenda_type}</b><div class="mono">${x.subject}</div><div>${x.rationale}</div></div>`).join('')||'<span class="muted">No open agenda.</span>';
$('signalTable').innerHTML=(d.signals||[]).map((x,i)=>`<tr><td>${i+1}</td><td class="mono">${x.feature}</td><td>${x.family}</td><td>${x.target}<div class="ms">${x.stage_s}s/${x.horizon_s}s</div></td><td>${ni(x.n)}</td><td>${n(x.rho)}</td><td>${n(x.lift,2)}x</td><td class="${cls(x.trend)}">${x.trend}</td><td>${n(x.live_score)}</td><td class="${cls(x.role)}">${x.role}</td></tr>`).join('');
$('passTable').innerHTML=(d.passes||[]).map(x=>`<tr><td class="${cls(x.status)}">${x.status}</td><td class="mono">${x.feature}</td><td>${x.target}<div class="ms">${x.stage_s}s/${x.horizon_s}s</div></td><td>${ni(x.n)}</td><td>${n(x.prospective_rho)}</td><td>${n(x.lift,2)}x</td><td>${pc(x.precision)}</td><td>${pc(x.baseline_rate)}</td></tr>`).join('');
$('ensembleTable').innerHTML=(d.ensembles||[]).map(x=>`<tr><td class="${cls(x.role)}">${x.role}</td><td>${x.context_key}</td><td>${x.member_count}</td><td>${ni(x.n)}</td><td>${n(x.rho)}</td><td>${n(x.lift,2)}x</td><td>${pc(x.precision)}</td><td>${n(x.live_score)}</td></tr>`).join('')||'<tr><td colspan="8" class="muted">No ensemble ranking yet.</td></tr>';
$('sideTable').innerHTML=(d.side_results||[]).map(x=>`<tr><td class="${cls(x.comparison)}">${x.comparison}</td><td>${x.kind}</td><td class="mono">${x.parent_feature||'—'}</td><td>${ni(x.n)}</td><td>${n(x.holdout_rho)}</td><td class="${Number(x.delta_rho||0)>0?'good':Number(x.delta_rho||0)<0?'bad':''}">${n(x.delta_rho)}</td><td>${x.verdict}</td></tr>`).join('')||'<tr><td colspan="7" class="muted">No V4.9 results yet.</td></tr>';
$('branchTable').innerHTML=(d.branches||[]).map(x=>`<tr><td class="mono">${x.branch}</td><td>${ni(x.hyp)}</td><td>${ni(x.queued)}</td><td>${ni(x.running)}</td><td>${ni(x.done)}</td><td>${ni(x.failed)}</td></tr>`).join('');
$('familyLive').innerHTML=(d.family_live||[]).map(x=>`<div class="familyrow"><div><b>${x.family}</b>${bar(x.score)}</div><div class="right">${x.champions} C</div><div class="right">${x.passes} P</div><div class="right">ρ ${n(x.rho,2)}</div></div>`).join('')||'<span class="muted">No live family ranking.</span>';
$('pipelineDetail').innerHTML=`<div class="kv"><div class="k2">signature spool</div><div>${ni(p.spool_total||0)}</div></div><div class="kv"><div class="k2">pending</div><div>${ni(spool.PENDING||0)}</div></div><div class="kv"><div class="k2">oldest pending</div><div>${ag(p.spool_oldest_pending)}</div></div><div class="kv"><div class="k2">raw transactions</div><div>${ni(p.raw_tx||0)}</div></div><div class="kv"><div class="k2">decoded swaps</div><div>${ni(p.swaps||0)}</div></div><div class="kv"><div class="k2">tokens</div><div>${ni(p.tokens||0)}</div></div><div class="kv"><div class="k2">snapshots</div><div>${ni(p.snapshots||0)}</div></div><div class="kv"><div class="k2">ready outcomes</div><div>${ni(p.ready||0)}</div></div>`;
$('workerDetail').innerHTML=`<div class="kv"><div class="k2">live workers</div><div>${ni(d.live_workers)}</div></div><div class="kv"><div class="k2">queued</div><div>${ni(jobs.QUEUED||0)}</div></div><div class="kv"><div class="k2">running</div><div>${ni(jobs.RUNNING||0)}</div></div><div class="kv"><div class="k2">done</div><div>${ni(jobs.DONE||0)}</div></div><div class="kv"><div class="k2">failed</div><div class="${jobs.FAILED?'bad':''}">${ni(jobs.FAILED||0)}</div></div>`;
$('paths').innerHTML=Object.entries(d.paths||{}).map(([k,v])=>`<div class="kv"><div class="k2">${k}</div><div class="mono">${v}</div></div>`).join('')}
async function tick(){try{let r=await fetch('/api/state',{cache:'no-store'});render(await r.json())}catch(e){$('system').textContent='DASHBOARD READ ERROR';$('sysdot').className='dot DOWN'}}tick();setInterval(tick,2000);
</script></body></html>'''


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/state":
            body = json.dumps(collect(), default=str, separators=(",", ":")).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path not in ("/", "/index.html"):
            self.send_response(404); self.end_headers(); return
        body = PAGE.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    print("=" * 92)
    print("MEMECOIN LAB — MASTER TERMINAL")
    print("=" * 92)
    print(f"Dashboard : http://{HOST}:{PORT}")
    print(f"Research  : {RDB}")
    print(f"Raw data  : {V5}")
    print(f"Features  : {V52}")
    print("Read-only. CTRL+C stops dashboard only.")
    ThreadingHTTPServer((HOST, PORT), H).serve_forever()
