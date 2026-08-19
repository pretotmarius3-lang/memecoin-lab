#!/usr/bin/env python3
"""MEMECOIN LAB — PREREGISTERED TAIL-DIRECTION VALIDATOR V7.6.9.2

Future-only confirmation of ONE simple survivor from V7.6.9.1.

Frozen parent tail rule (unchanged):
  gross_growth >= 1.142 AND wallet_top1_share <= 0.451

Frozen direction feature chosen BEFORE this validator:
  D10_30_sells = T30 sells - T10 sells
  direction = LOWER is better
  threshold = 13.75

Why 13.75: fixed midpoint of discovery medians (WIN=5, CRASH=22.5).
It is hard-coded here and MUST NOT be recomputed or retuned.

Confirmation compares the directional subset ONLY against the parent tail cohort
inside a brand-new post-cutoff future cohort.
Research only. No capital decision.
"""
from __future__ import annotations
import math, sqlite3, statistics, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
OUT=ROOT/'v7692_tail_direction_validation.db'

GROSS_TH=1.142
TOP1_MAX=0.451
SELL_DELTA_MAX=13.75
WINNER=25.0
DOWNSIDE=-50.0
TARGET_DIRECTION=20
TARGET_TAIL=40


def sf(x):
    try:
        z=float(x); return z if math.isfinite(z) else None
    except Exception:
        return None


def ro():
    d=sqlite3.connect(f'file:{FEATURE}?mode=ro',uri=True,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA query_only=ON')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def odb():
    d=sqlite3.connect(OUT,timeout=30)
    d.row_factory=sqlite3.Row
    d.execute('PRAGMA journal_mode=WAL')
    d.execute('PRAGMA synchronous=FULL')
    d.execute('PRAGMA busy_timeout=30000')
    return d


def init():
    d=odb()
    d.executescript('''
    CREATE TABLE IF NOT EXISTS run(
      id INTEGER PRIMARY KEY CHECK(id=1),created_at REAL,activation REAL,cutoff_t30 REAL,
      parent_rule TEXT,direction_rule TEXT,gross_th REAL,top1_max REAL,sell_delta_max REAL,
      winner_th REAL,downside_th REAL,target_direction INTEGER,target_tail INTEGER
    );
    CREATE TABLE IF NOT EXISTS future_obs(
      token_mint TEXT PRIMARY KEY,t30 REAL,gross_growth REAL,wallet_top1_share REAL,
      sell_delta_10_30 REAL,parent_tail INTEGER,direction_selected INTEGER,
      future REAL,first_recorded_at REAL
    );
    ''')
    r=d.execute('SELECT * FROM run WHERE id=1').fetchone()
    if not r:
        x=ro()
        a=x.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone()
        activation=sf(a[0]) if a else None
        if activation is None: raise SystemExit('No current V7611 activation')
        z=x.execute('SELECT MAX(first_ts+30) FROM v7611_causal_snapshots WHERE stage_s=30 AND first_observed_at>?',(activation,)).fetchone()
        x.close()
        cutoff=sf(z[0]) if z and z[0] is not None else time.time()
        d.execute('INSERT INTO run VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(
          1,time.time(),activation,cutoff,
          'gross_growth>=1.142 AND wallet_top1_share<=0.451',
          'D10_30_sells<=13.75',GROSS_TH,TOP1_MAX,SELL_DELTA_MAX,
          WINNER,DOWNSIDE,TARGET_DIRECTION,TARGET_TAIL))
        d.commit()
    d.close()


def samples():
    d=odb(); r=d.execute('SELECT * FROM run WHERE id=1').fetchone(); d.close()
    activation=float(r['activation'])
    x=ro()
    rows=x.execute('''
      SELECT token_mint,stage_s,first_ts,first_observed_at,gross_sol,wallet_top1_share,sells,return_pct
      FROM v7611_causal_snapshots
      WHERE first_observed_at>?
      ORDER BY first_ts,token_mint,stage_s
    ''',(activation,)).fetchall(); x.close()
    by={}
    for rr in rows: by.setdefault(str(rr['token_mint']),{})[int(rr['stage_s'])]=dict(rr)
    out=[]
    for mint,s in by.items():
        if not all(k in s for k in (10,30)): continue
        g10,g30=sf(s[10]['gross_sol']),sf(s[30]['gross_sol'])
        top1=sf(s[30]['wallet_top1_share'])
        sells10,sells30=sf(s[10]['sells']),sf(s[30]['sells'])
        r30=sf(s[30]['return_pct'])
        if None in (g10,g30,top1,sells10,sells30,r30): continue
        fut=None
        for h in (120,60):
            if h in s and sf(s[h]['return_pct']) is not None:
                fut=sf(s[h]['return_pct'])-r30; break
        if fut is None: continue
        out.append({'mint':mint,'t30':float(s[30]['first_ts'])+30.0,
                    'gross_growth':g30-g10,'top1':top1,'sell_delta':sells30-sells10,'future':fut})
    out.sort(key=lambda z:(z['t30'],z['mint']))
    return out


def ingest():
    d=odb(); r=d.execute('SELECT * FROM run WHERE id=1').fetchone(); cut=float(r['cutoff_t30'])
    known={x[0] for x in d.execute('SELECT token_mint FROM future_obs')}; made=0
    for z in samples():
        if z['t30']<=cut or z['mint'] in known: continue
        tail=int(z['gross_growth']>=float(r['gross_th']) and z['top1']<=float(r['top1_max']))
        directional=int(tail and z['sell_delta']<=float(r['sell_delta_max']))
        d.execute('INSERT OR IGNORE INTO future_obs VALUES(?,?,?,?,?,?,?,?,?)',(
          z['mint'],z['t30'],z['gross_growth'],z['top1'],z['sell_delta'],tail,directional,z['future'],time.time()))
        made+=d.execute('SELECT changes()').fetchone()[0]
    d.commit(); d.close(); return made


def stats(xs,w,dn):
    if not xs:return None
    cap=[max(-100,min(100,x)) for x in xs]
    return {'n':len(xs),'mean':statistics.mean(xs),'median':statistics.median(xs),
            'cap':statistics.mean(cap),'winner':sum(x>=w for x in xs)/len(xs),
            'down':sum(x<=dn for x in xs)/len(xs)}


def display():
    d=odb(); r=d.execute('SELECT * FROM run WHERE id=1').fetchone()
    rows=[dict(x) for x in d.execute('SELECT * FROM future_obs ORDER BY t30').fetchall()]; d.close()
    tail=[float(x['future']) for x in rows if int(x['parent_tail'])==1]
    direction=[float(x['future']) for x in rows if int(x['direction_selected'])==1]
    a=stats(direction,float(r['winner_th']),float(r['downside_th']))
    b=stats(tail,float(r['winner_th']),float(r['downside_th']))
    print('='*140)
    print('MEMECOIN LAB — PREREGISTERED TAIL-DIRECTION VALIDATOR V7.6.9.2')
    print('='*140)
    print(f"activation>{float(r['activation']):.3f} cutoff_t30>{float(r['cutoff_t30']):.3f} all_future={len(rows)} tail={len(tail)} direction_selected={len(direction)}")
    print('FIXED parent: gross_growth>=1.142 & top1<=0.451')
    print('FIXED direction: D10_30_sells<=13.75 | threshold frozen from discovery median midpoint; NO RETUNING')
    print(f"targets direction>={int(r['target_direction'])} tail>={int(r['target_tail'])} | winner>=+25% downside<=-50%")
    if a and b:
        print(f"DIRECTION n={a['n']} mean/med/cap={a['mean']:+7.2f}/{a['median']:+7.2f}/{a['cap']:+7.2f}% winner={100*a['winner']:5.1f}% downside={100*a['down']:5.1f}%")
        print(f"TAIL BASE n={b['n']} mean/med/cap={b['mean']:+7.2f}/{b['median']:+7.2f}/{b['cap']:+7.2f}% winner={100*b['winner']:5.1f}% downside={100*b['down']:5.1f}%")
        print(f"UPLIFT cap={a['cap']-b['cap']:+7.2f}% winner={100*(a['winner']-b['winner']):+5.1f}pp downside={100*(a['down']-b['down']):+5.1f}pp")
    enough=len(direction)>=int(r['target_direction']) and len(tail)>=int(r['target_tail'])
    if not enough: print('STATUS=ACCUMULATING_FUTURE_EVIDENCE')
    else:
        passed=bool(a and b and a['cap']>b['cap'] and a['winner']>b['winner'] and a['down']<=b['down'])
        print('STATUS=' + ('TAIL_DIRECTION_SURVIVES' if passed else 'TAIL_DIRECTION_FAILS'))


def main():
    init(); print(f'new_future_rows={ingest()}'); display()

if __name__=='__main__': main()
