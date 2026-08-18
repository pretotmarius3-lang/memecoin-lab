#!/usr/bin/env python3
"""Memecoin Lab V6.9.4 — Creative Research Sandbox schema fix.

Fixes V6.9.3 reading the old in-research table name (v69_strategy_intelligence)
while isolated V6.9.2 writes to strategy_intelligence in v69_intelligence.db.
Reads science DB read-only and writes hypotheses only to v693_creative.db.
"""
from __future__ import annotations
import os, signal, sqlite3, time
import v693_creative_research_sandbox as base

LOOP=float(os.environ.get('MEMECOIN_V694_LOOP_S','30'))
STOP=False

def stop(*_):
    global STOP; STOP=True

def scientific_rows_fixed():
    if not base.SCI_DB.exists(): return []
    d=base.ro(base.SCI_DB)
    try:
        tables={str(x[0]) for x in d.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if 'strategy_intelligence' in tables:
            q='SELECT * FROM strategy_intelligence ORDER BY role,label'
        elif 'v69_strategy_intelligence' in tables:
            q='SELECT * FROM v69_strategy_intelligence ORDER BY role,label'
        else:
            return []
        return [dict(x) for x in d.execute(q).fetchall()]
    finally:
        d.close()

def main():
    signal.signal(signal.SIGINT,stop); signal.signal(signal.SIGTERM,stop); base.init()
    print(f'V6.9.4 started | science={base.SCI_DB} READ-ONLY | creative={base.CREATIVE_DB}',flush=True)
    while not STOP:
        try:
            rs=scientific_rows_fixed(); hs=base.write_cycle(rs); base.display(rs,hs)
        except Exception as e:
            print('V6.9.4 error:',repr(e),flush=True)
        time.sleep(LOOP)

if __name__=='__main__': main()
