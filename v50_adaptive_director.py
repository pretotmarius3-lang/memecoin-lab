#!/usr/bin/env python3
"""V5.0 Adaptive Research Director.

Turns V4.8 rankings + V4.9 side results into an autonomous research portfolio.
It NEVER promotes exploratory evidence directly into PASS. It only creates
promotion proposals with a fresh freeze cutoff; V4.6/V4.7 remain the forward
validation authority. Research-only; no trading/signing.
"""
from __future__ import annotations
import json, math, os, signal, sqlite3, time
from collections import defaultdict
import v41_core as core

LOOP=float(os.environ.get('MEMECOIN_V50_DIRECTOR_LOOP','5')); STOP=False

def stop(*_):
    global STOP; STOP=True

def sf(x,d=0.0):
    try:
        v=float(x); return v if math.isfinite(v) else d
    except Exception:return d

def init():
    d=core.open_research(); d.executescript('''
    CREATE TABLE IF NOT EXISTS v50_director_state(key TEXT PRIMARY KEY,value_json TEXT NOT NULL,updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v50_promotions(
      promotion_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL, kind TEXT NOT NULL,
      parent_key TEXT, spec_json TEXT NOT NULL, evidence_n INTEGER NOT NULL,
      holdout_rho REAL, delta_rho REAL, priority REAL NOT NULL,
      freeze_cutoff REAL NOT NULL, state TEXT NOT NULL, rationale TEXT NOT NULL,
      created_at REAL NOT NULL, updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v50_family_policy(
      family TEXT PRIMARY KEY, evidence_score REAL NOT NULL, side_yield REAL NOT NULL,
      forward_score REAL NOT NULL, allocation REAL NOT NULL, action TEXT NOT NULL,
      updated_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS v50_director_actions(
      action_id TEXT PRIMARY KEY, action_type TEXT NOT NULL, subject TEXT NOT NULL,
      priority REAL NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL,
      rationale TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL);
    '''); d.commit(); d.close()

def family_for_feature(f):
    if f in ('repeat_wallet_ratio','wallet_hhi','wallet_top1_share','trade_hhi','top1_trade_share','unique_wallets'): return 'WALLET_CONCENTRATION'
    if f in ('net_sol','flow_velocity','flow_acceleration','buy_ratio','buy_ratio_delta'): return 'FLOW_IMBALANCE'
    if f in ('return_pct','range_pct','price_velocity'): return 'PRICE_MOMENTUM'
    return 'ACTIVITY_TRADING'

def refresh_policy():
    d=core.open_research(); ranks=[dict(r) for r in d.execute('SELECT * FROM v48_signal_rankings').fetchall()] if d.execute("SELECT 1 FROM sqlite_master WHERE name='v48_signal_rankings'").fetchone() else []
    side=[dict(r) for r in d.execute("SELECT r.*,e.parent_feature FROM v49_side_results r JOIN v49_side_experiments e USING(experiment_id)").fetchall()] if d.execute("SELECT 1 FROM sqlite_master WHERE name='v49_side_results'").fetchone() else []; d.close()
    fams=defaultdict(lambda:{'rank':[],'side':[]})
    for r in ranks:fams[r['family']]['rank'].append(r)
    for r in side:fams[family_for_feature(r.get('parent_feature') or '')]['side'].append(r)
    raw={}
    for fam,g in fams.items():
        rr=g['rank']; ss=g['side']; forward=sum(sf(x.get('live_score')) for x in rr)/max(1,len(rr)); improved=sum(x.get('comparison')=='IMPROVED' for x in ss); sideyield=improved/max(1,len(ss)); champions=sum(x.get('role')=='CHAMPION' for x in rr)
        raw[fam]=.55*forward+.30*sideyield+.15*min(1,champions/2)
    total=sum(raw.values()) or 1; now=time.time(); d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for fam,g in fams.items():
            rr=g['rank']; ss=g['side']; forward=sum(sf(x.get('live_score')) for x in rr)/max(1,len(rr)); sideyield=sum(x.get('comparison')=='IMPROVED' for x in ss)/max(1,len(ss)); score=raw[fam]; alloc=score/total; action='EXPAND' if alloc>=.32 else ('MAINTAIN' if alloc>=.15 else 'COOLDOWN')
            d.execute("INSERT INTO v50_family_policy VALUES(?,?,?,?,?,?,?) ON CONFLICT(family) DO UPDATE SET evidence_score=excluded.evidence_score,side_yield=excluded.side_yield,forward_score=excluded.forward_score,allocation=excluded.allocation,action=excluded.action,updated_at=excluded.updated_at",(fam,score,sideyield,forward,alloc,action,now))
        d.commit()
    except: d.rollback(); raise
    finally:d.close()

def propose_promotions():
    d=core.open_research();
    if not d.execute("SELECT 1 FROM sqlite_master WHERE name='v49_side_results'").fetchone(): d.close(); return 0
    rows=[dict(r) for r in d.execute("SELECT r.*,e.parent_key,e.spec_json FROM v49_side_results r JOIN v49_side_experiments e USING(experiment_id) WHERE r.comparison='IMPROVED' AND r.n>=90 AND COALESCE(r.holdout_rho,0)>=0.08 ORDER BY COALESCE(r.delta_rho,0) DESC,COALESCE(r.holdout_rho,0) DESC LIMIT 200").fetchall()]; d.close(); made=0; now=time.time()
    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for r in rows:
            priority=min(1,.45*min(1,sf(r['holdout_rho'])/.30)+.35*min(1,max(0,sf(r['delta_rho']))/.20)+.20*min(1,int(r['n'])/250))
            if priority<.42: continue
            pid='P_'+core.fingerprint({'x':r['experiment_id']},'v50:')[:24]; rationale=f"Side experiment improved parent: rho={sf(r['holdout_rho']):.3f}, delta={sf(r['delta_rho']):+.3f}, n={r['n']}. Requires fresh future-only validation."
            before=d.total_changes; d.execute("INSERT OR IGNORE INTO v50_promotions VALUES(?,?,?,?,?,?,?,?,?,'PROPOSED',?,?,?)",(pid,r['experiment_id'],r['kind'],r['parent_key'],r['spec_json'],int(r['n']),r['holdout_rho'],r['delta_rho'],priority,now,rationale,now,now)); made+=int(d.total_changes>before)
        d.commit()
    except: d.rollback(); raise
    finally:d.close()
    return made

def actions():
    d=core.open_research(); pol=[dict(r) for r in d.execute('SELECT * FROM v50_family_policy ORDER BY allocation DESC').fetchall()]; ranks=[dict(r) for r in d.execute("SELECT * FROM v48_signal_rankings WHERE trend IN ('DECAYING','BROKEN') ORDER BY live_score DESC LIMIT 20").fetchall()] if d.execute("SELECT 1 FROM sqlite_master WHERE name='v48_signal_rankings'").fetchone() else []; d.close(); now=time.time(); items=[]
    for p in pol:
        typ='EXPAND_FAMILY' if p['action']=='EXPAND' else ('COOLDOWN_FAMILY' if p['action']=='COOLDOWN' else 'MAINTAIN_FAMILY'); items.append((typ,p['family'],p['allocation'],{'allocation':p['allocation']},f"Adaptive allocation {p['allocation']:.1%}; side yield {p['side_yield']:.1%}; forward score {p['forward_score']:.3f}"))
    for r in ranks: items.append(('DIAGNOSE_DECAY',r['feature'],.8,{'candidate_id':r['candidate_id'],'family':r['family']},f"{r['feature']} is {r['trend']}; preserve frozen rule and investigate regime/time/source side branches."))
    d=core.open_research(); d.execute('BEGIN IMMEDIATE')
    try:
        for typ,sub,pri,pay,why in items:
            aid='D_'+core.fingerprint({'t':typ,'s':sub},'v50a:')[:24]; d.execute("INSERT INTO v50_director_actions VALUES(?,?,?,?,?,'OPEN',?,?,?) ON CONFLICT(action_id) DO UPDATE SET priority=excluded.priority,payload_json=excluded.payload_json,rationale=excluded.rationale,updated_at=excluded.updated_at",(aid,typ,sub,pri,core.canonical_json(pay),why,now,now))
        d.commit()
    except:d.rollback(); raise
    finally:d.close()

def display(new):
    d=core.open_research(); p=[dict(r) for r in d.execute("SELECT * FROM v50_promotions WHERE state='PROPOSED' ORDER BY priority DESC LIMIT 8").fetchall()]; f=[dict(r) for r in d.execute('SELECT * FROM v50_family_policy ORDER BY allocation DESC').fetchall()]; a=d.execute("SELECT COUNT(*) FROM v50_director_actions WHERE state='OPEN'").fetchone()[0]; d.close()
    print('\033[2J\033[H'+'='*118); print('MEMECOIN LAB — ADAPTIVE RESEARCH DIRECTOR V5.0'); print('='*118); print(f'NEW PROMOTIONS={new} | PROPOSED={len(p)} shown | OPEN ACTIONS={a}')
    print('\nRESEARCH CAPITAL')
    for x in f: print(f"{x['family']:<24} {x['allocation']*100:5.1f}%  {x['action']:<9} forward={x['forward_score']:.3f} side_yield={x['side_yield']*100:5.1f}%")
    print('\nPROMOTION QUEUE — fresh freeze required')
    for x in p: print(f"{x['kind']:<20} priority={x['priority']:.2f} rho={sf(x['holdout_rho']):.3f} delta={sf(x['delta_rho']):+.3f} n={x['evidence_n']}")
    print('\nExploration may adapt. Prospective validation remains immutable and future-only.')

def main():
    init()
    while not STOP:
        try: refresh_policy(); new=propose_promotions(); actions(); display(new)
        except Exception as e: print('director error:',repr(e))
        time.sleep(LOOP)
if __name__=='__main__':
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); main()
