#!/usr/bin/env python3
"""MEMECOIN LAB — CANONICAL MIGRATION GROWTH COLLECTOR V7.8.1

Continuously refreshes the frozen V7.7.9 canonical migration table and V7.8.0
PRE/POST reconstruction as new locally captured migration transactions/swaps arrive.
Research infrastructure only: no alpha search and no evidence-rule retuning.
"""
from __future__ import annotations
import sqlite3, subprocess, sys, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
MIG=ROOT/'v779_canonical_migrations.db'
PATHS=ROOT/'v780_canonical_migration_paths.db'
POLL=30.0


def scalar(db,sql):
    if not db.exists(): return 0
    try:
        d=sqlite3.connect(f'file:{db}?mode=ro',uri=True,timeout=10)
        x=d.execute(sql).fetchone()[0]; d.close(); return int(x or 0)
    except Exception:return 0


def snapshot():
    return (
        scalar(MIG,'SELECT COUNT(*) FROM canonical_migrations'),
        scalar(PATHS,'SELECT COUNT(*) FROM migration_paths'),
        *[scalar(PATHS,f'SELECT COUNT(*) FROM migration_paths WHERE ret{h} IS NOT NULL') for h in (5,10,30,60,120,300)]
    )


def run(script):
    try:
        p=subprocess.run([sys.executable,'-u',str(ROOT/script)],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=180)
    except Exception as e:
        print(f'V781 WARN {script} exception={e!r}',flush=True); return False
    if p.returncode:
        print(f"V781 WARN {script} rc={p.returncode}\n"+'\n'.join((p.stdout or '').splitlines()[-12:]),flush=True)
        return False
    return True


def main():
    print('MEMECOIN LAB V7.8.1 CANONICAL MIGRATION GROWTH COLLECTOR',flush=True)
    print('frozen_truth=V7.7.9 | reconstruction=V7.8.0 | no alpha tuning | poll=30s',flush=True)
    previous=snapshot()
    print('V781 start canonical=%d paths=%d post5/10/30/60/120/300=%d/%d/%d/%d/%d/%d'%previous,flush=True)
    while True:
        t0=time.time(); ok1=run('v779_canonical_migration_table.py'); ok2=run('v780_canonical_migration_path_reconstruction.py') if ok1 else False
        now=snapshot(); dc=now[0]-previous[0]; dp=now[1]-previous[1]
        print(f'V781 heartbeat canonical={now[0]} ({dc:+d}) paths={now[1]} ({dp:+d}) post5/10/30/60/120/300={now[2]}/{now[3]}/{now[4]}/{now[5]}/{now[6]}/{now[7]} refresh={time.time()-t0:.1f}s status={"OK" if ok1 and ok2 else "WARN"}',flush=True)
        previous=now
        time.sleep(max(1.0,POLL-(time.time()-t0)))

if __name__=='__main__':main()
