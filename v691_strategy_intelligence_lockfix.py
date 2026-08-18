#!/usr/bin/env python3
"""V6.9.1 — lock-safe Strategy Intelligence Layer.

Fixes V6.9's self-deadlock: V6.9 held one write connection open and then opened
another write connection from journal(), which can produce 'database is locked'.
V6.9.1 performs intelligence + journal writes on ONE connection/transaction per
cycle, with busy_timeout and retry. Frozen research/forward tables remain read-only.
"""
from __future__ import annotations
import hashlib, json, os, signal, sqlite3, time
import v41_core as core
import v69_strategy_intelligence_layer as v69

LOOP=float(os.environ.get('MEMECOIN_V691_LOOP_S','20'))
STOP=False

def stop(*_):
    global STOP; STOP=True

def write_cycle():
    rr,b=v69.r64(); r64id=str(rr.get('rule_id') or 'R64')
    rt=v69.tokens('v64_forward_events','rule_id',r64id)
    rs=v69.tokens('v64_forward_events','rule_id',r64id,True)
    allrows=[(rr,b,'R64 // PRICE_VELOCITY','CONTROL')]+[(r,s,s['label'],'CHALLENGER') for r,s in v69.challengers()]
    now=time.time(); payload=[]
    for r,s,label,role in allrows:
        sid=str(r.get('challenger_id') or r.get('rule_id') or label); ov=sv=None
        if role!='CONTROL':
            ov=v69.overlap(v69.tokens('v673_forward_events','challenger_id',sid),rt)
            sv=v69.overlap(v69.tokens('v673_forward_events','challenger_id',sid,True),rs)
        ev=v69.evidence(int(s.get('done') or 0),role,str(s.get('status') or ''))
        verdict,action=v69.verdict(label,role,s,b,ov,sv)
        vals=(sid,label,role,ev,int(s.get('done') or 0),int(s.get('signals') or 0),s.get('expectancy'),s.get('profit_factor'),s.get('win_rate'),s.get('fill_rate'),s.get('max_drawdown'),v69.sf(s.get('expectancy'))-v69.sf(b.get('expectancy')) if role!='CONTROL' else 0,v69.sf(s.get('profit_factor'))-v69.sf(b.get('profit_factor')) if role!='CONTROL' else 0,ov,sv,verdict,action,now)
        m=v69.milestone(role,s)
        snap={'done':s.get('done'),'signals':s.get('signals'),'expectancy':s.get('expectancy'),'profit_factor':s.get('profit_factor'),'fill_rate':s.get('fill_rate'),'max_drawdown':s.get('max_drawdown'),'status':s.get('status')}
        eid=hashlib.sha256(f'{sid}|{m}'.encode()).hexdigest()[:24]
        jvals=(eid,now,sid,label,m,ev,verdict,action,json.dumps(snap,sort_keys=True))
        payload.append((vals,jvals))

    last=None
    for attempt in range(8):
        d=None
        try:
            d=core.open_research(); d.execute('PRAGMA busy_timeout=15000'); d.execute('BEGIN IMMEDIATE')
            for vals,jvals in payload:
                d.execute('''INSERT INTO v69_strategy_intelligence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(strategy_id) DO UPDATE SET label=excluded.label,role=excluded.role,evidence=excluded.evidence,
                  done=excluded.done,signals=excluded.signals,expectancy=excluded.expectancy,profit_factor=excluded.profit_factor,
                  win_rate=excluded.win_rate,fill_rate=excluded.fill_rate,max_drawdown=excluded.max_drawdown,
                  alpha_vs_r64=excluded.alpha_vs_r64,pf_vs_r64=excluded.pf_vs_r64,
                  token_overlap_r64=excluded.token_overlap_r64,signal_overlap_r64=excluded.signal_overlap_r64,
                  verdict=excluded.verdict,action=excluded.action,updated_at=excluded.updated_at''',vals)
                d.execute('INSERT OR IGNORE INTO v69_research_journal VALUES(?,?,?,?,?,?,?,?,?)',jvals)
            d.commit(); d.close(); return
        except sqlite3.OperationalError as e:
            last=e
            try:
                if d:d.rollback();d.close()
            except:pass
            if 'locked' not in str(e).lower():raise
            time.sleep(0.25*(attempt+1))
    raise last or RuntimeError('V6.9.1 write failed')

def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); v69.init()
    while not STOP:
        try:
            write_cycle(); v69.display()
        except Exception as e:
            print('V6.9.1 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__':main()
