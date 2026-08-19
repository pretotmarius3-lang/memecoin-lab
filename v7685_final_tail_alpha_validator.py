#!/usr/bin/env python3
"""MEMECOIN LAB — FINAL CLEAN PREREGISTERED TAIL-ALPHA VALIDATOR V7.6.8.5

Binds the already-frozen tail-alpha rule to the CURRENT V7611 activation.
Creates a brand-new validation DB and cutoff once, then only ingests future rows.
No historical validation rows are imported. No retuning.

Fixed hypothesis inherited unchanged from V7.6.8.3:
  gross_growth >= 1.142 AND wallet_top1_share <= 0.451
  winner >= +25%, downside <= -50%
  gate: selected >= 30 AND total >= 60

Research only. No capital decision.
"""
from __future__ import annotations
import math, sqlite3, statistics, time
from pathlib import Path

ROOT=Path.home()/"memecoin_lab"
FEATURE=ROOT/'v52_features.db'
OUT=ROOT/'v7685_final_tail_alpha_validation.db'
GROSS_TH=1.142
TOP1_MAX=0.451
WINNER=25.0
DOWNSIDE=-50.0
TARGET_SELECTED=30
TARGET_TOTAL=60


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


def sf(x):
    try:
        z=float(x)
        return z if math.isfinite(z) else None
    except Exception:
        return None


def init():
    d=odb()
    d.executescript('''
    CREATE TABLE IF NOT EXISTS run(
      id INTEGER PRIMARY KEY CHECK(id=1),
      created_at REAL,
      activation REAL,
      cutoff_t30 REAL,
      hypothesis TEXT,
      gross_th REAL,
      top1_max REAL,
      winner_th REAL,
      downside_th REAL,
      target_selected INTEGER,
      target_total INTEGER
    );
    CREATE TABLE IF NOT EXISTS future_obs(
      token_mint TEXT PRIMARY KEY,
      t30 REAL,
      gross_growth REAL,
      wallet_top1_share REAL,
      selected INTEGER,
      future REAL,
      first_recorded_at REAL
    );
    ''')
    r=d.execute('SELECT * FROM run WHERE id=1').fetchone()
    if not r:
        x=ro()
        a=x.execute('SELECT activation_observed_at FROM v7611_scheduler_state WHERE id=1').fetchone()
        activation=sf(a[0]) if a else None
        if activation is None:
            raise SystemExit('No current V7611 activation')
        z=x.execute('SELECT MAX(first_ts+30) FROM v7611_causal_snapshots WHERE stage_s=30 AND first_observed_at>?',(activation,)).fetchone()
        x.close()
        cut=sf(z[0]) if z and z[0] is not None else time.time()
        d.execute('INSERT INTO run VALUES(?,?,?,?,?,?,?,?,?,?,?)',(
            1,time.time(),activation,cut,
            'gross_growth>=1.142 AND wallet_top1_share<=0.451',
            GROSS_TH,TOP1_MAX,WINNER,DOWNSIDE,TARGET_SELECTED,TARGET_TOTAL
        ))
        d.commit()
    d.close()


def samples():
    d=odb(); r=d.execute('SELECT * FROM run WHERE id=1').fetchone(); d.close()
    activation=float(r['activation'])
    x=ro()
    rows=x.execute('''
      SELECT token_mint,stage_s,first_ts,first_observed_at,gross_sol,wallet_top1_share,return_pct
      FROM v7611_causal_snapshots
      WHERE first_observed_at>?
      ORDER BY first_ts,token_mint,stage_s
    ''',(activation,)).fetchall()
    x.close()
    by={}
    for rr in rows:
        by.setdefault(str(rr['token_mint']),{})[int(rr['stage_s'])]=dict(rr)
    out=[]
    for mint,s in by.items():
        if not all(k in s for k in (10,30)):
            continue
        g10,g30=sf(s[10]['gross_sol']),sf(s[30]['gross_sol'])
        top1=sf(s[30]['wallet_top1_share'])
        r30=sf(s[30]['return_pct'])
        if None in (g10,g30,top1,r30):
            continue
        fut=None
        for h in (120,60):
            if h in s and sf(s[h]['return_pct']) is not None:
                fut=sf(s[h]['return_pct'])-r30
                break
        if fut is None:
            continue
        out.append({
            'mint':mint,
            't30':float(s[30]['first_ts'])+30.0,
            'gross_growth':g30-g10,
            'top1':top1,
            'future':fut
        })
    out.sort(key=lambda z:(z['t30'],z['mint']))
    return out


def ingest():
    d=odb(); r=d.execute('SELECT * FROM run WHERE id=1').fetchone()
    cut=float(r['cutoff_t30'])
    known={x[0] for x in d.execute('SELECT token_mint FROM future_obs')}
    made=0
    for z in samples():
        if z['t30']<=cut or z['mint'] in known:
            continue
        sel=int(z['gross_growth']>=float(r['gross_th']) and z['top1']<=float(r['top1_max']))
        d.execute('INSERT OR IGNORE INTO future_obs VALUES(?,?,?,?,?,?,?)',(
            z['mint'],z['t30'],z['gross_growth'],z['top1'],sel,z['future'],time.time()
        ))
        made+=d.execute('SELECT changes()').fetchone()[0]
    d.commit(); d.close()
    return made


def stats(xs,w,dn):
    if not xs:
        return None
    cap=[max(-100,min(100,x)) for x in xs]
    return {
        'n':len(xs),
        'mean':statistics.mean(xs),
        'median':statistics.median(xs),
        'cap':statistics.mean(cap),
        'hit':sum(x>=w for x in xs)/len(xs),
        'down':sum(x<=dn for x in xs)/len(xs),
    }


def display():
    d=odb(); r=d.execute('SELECT * FROM run WHERE id=1').fetchone()
    rows=[dict(x) for x in d.execute('SELECT * FROM future_obs ORDER BY t30').fetchall()]
    d.close()
    sel=[float(x['future']) for x in rows if int(x['selected'])==1]
    base=[float(x['future']) for x in rows]
    a=stats(sel,float(r['winner_th']),float(r['downside_th']))
    b=stats(base,float(r['winner_th']),float(r['downside_th']))
    print('='*132)
    print('MEMECOIN LAB — FINAL CLEAN PREREGISTERED TAIL-ALPHA VALIDATOR V7.6.8.5')
    print('='*132)
    print(f"activation>{float(r['activation']):.3f} cutoff_t30>{float(r['cutoff_t30']):.3f} total_future={len(rows)} selected={len(sel)}")
    print(f"FIXED rule gross_growth>={float(r['gross_th']):.3f} & top1<={float(r['top1_max']):.3f} | winner>={float(r['winner_th']):+.0f}% downside<={float(r['downside_th']):+.0f}%")
    print(f"targets selected>={int(r['target_selected'])} total>={int(r['target_total'])} | future-only; no retuning")
    if a and b:
        print(f"SELECTED n={a['n']} mean/med/cap={a['mean']:+7.2f}/{a['median']:+7.2f}/{a['cap']:+7.2f}% winner={100*a['hit']:5.1f}% downside={100*a['down']:5.1f}%")
        print(f"BASE     n={b['n']} mean/med/cap={b['mean']:+7.2f}/{b['median']:+7.2f}/{b['cap']:+7.2f}% winner={100*b['hit']:5.1f}% downside={100*b['down']:5.1f}%")
        print(f"UPLIFT cap={a['cap']-b['cap']:+7.2f}% winner={100*(a['hit']-b['hit']):+5.1f}pp downside={100*(a['down']-b['down']):+5.1f}pp")
    enough=len(rows)>=int(r['target_total']) and len(sel)>=int(r['target_selected'])
    if not enough:
        print('STATUS=ACCUMULATING_FUTURE_EVIDENCE')
    else:
        passed=bool(a and b and a['hit']>b['hit'] and a['cap']>b['cap'] and a['down']<=b['down'])
        print('STATUS=' + ('TAIL_ALPHA_SURVIVES' if passed else 'TAIL_ALPHA_FAILS'))


def main():
    init()
    print(f'new_future_rows={ingest()}')
    display()


if __name__=='__main__':
    main()
