#!/usr/bin/env python3
"""MEMECOIN LAB — PORTFOLIO REALITY AUDIT V7.2

Read-only audit of the frozen V7.1 R64+WALLET portfolio cohort.
Builds a common temporal exposure view without altering V7.1.
Primary allocation remains the predeclared 50/50 equal-risk portfolio.
"""
from __future__ import annotations
import math, sqlite3, statistics, time
from pathlib import Path

ROOT=Path(__file__).resolve().parent
RDB=ROOT/'research_v4_1.db'
OUT=ROOT/'v72_portfolio_reality.db'
WINDOW_S=120.0
LOSS_UNIT=13.0
RISK_PCT=0.50

def ro(p):
 d=sqlite3.connect(f'file:{p}?mode=ro',uri=True,timeout=10); d.row_factory=sqlite3.Row; d.execute('PRAGMA query_only=ON'); d.execute('PRAGMA busy_timeout=10000'); return d

def init():
 d=sqlite3.connect(OUT,timeout=10); d.execute('PRAGMA journal_mode=WAL'); d.execute('PRAGMA busy_timeout=10000')
 d.executescript('''
 CREATE TABLE IF NOT EXISTS v72_summary(
  created_at REAL, arena_id TEXT, allocation TEXT, r64_done INTEGER, wallet_done INTEGER,
  paired_temporal INTEGER, overlap_rate REAL, return_corr REAL, both_loss_rate REAL,
  portfolio_return REAL, portfolio_true_dd REAL, r64_return REAL, r64_true_dd REAL,
  wallet_return REAL, wallet_true_dd REAL, marginal_return_vs_r64 REAL, marginal_dd_vs_r64 REAL,
  verdict TEXT, method TEXT);
 '''); d.commit(); d.close()

def step(net,w): return 1.0+(RISK_PCT/100.0)*(w*float(net)/LOSS_UNIT)
def path(events):
 eq=peak=100.0; dd=0.0
 for _,net,w in sorted(events,key=lambda x:x[0]):
  eq*=step(net,w); peak=max(peak,eq); dd=min(dd,100*(eq/peak-1))
 return eq-100,dd

def corr(a,b):
 if len(a)<2:return None
 ma=statistics.mean(a);mb=statistics.mean(b);sa=sum((x-ma)**2 for x in a);sb=sum((x-mb)**2 for x in b)
 return sum((x-ma)*(y-mb) for x,y in zip(a,b))/math.sqrt(sa*sb) if sa>0 and sb>0 else None

def cycle():
 d=ro(RDB); fr=d.execute('SELECT * FROM v71_freeze LIMIT 1').fetchone()
 if not fr: d.close(); raise RuntimeError('V7.1 freeze missing')
 aid=fr['arena_id']
 rs=[dict(x) for x in d.execute("SELECT label,token_mint,cutoff_ts,net_return FROM v71_events WHERE arena_id=? AND state='DONE' AND net_return IS NOT NULL ORDER BY cutoff_ts",(aid,))]
 d.close()
 r=[x for x in rs if x['label']=='R64']; w=[x for x in rs if x['label']=='WALLET']
 # temporal matching: nearest opposite-strategy DONE within +/- WINDOW_S, one-to-one greedy
 pairs=[];used=set()
 for i,a in enumerate(r):
  best=None
  for j,b in enumerate(w):
   if j in used: continue
   gap=abs(float(a['cutoff_ts'])-float(b['cutoff_ts']))
   if gap<=WINDOW_S and (best is None or gap<best[0]): best=(gap,j,b)
  if best:
   used.add(best[1]);pairs.append((a,best[2]))
 ra=[float(a['net_return']) for a,b in pairs]; wa=[float(b['net_return']) for a,b in pairs]
 both=sum(1 for a,b in pairs if float(a['net_return'])<0 and float(b['net_return'])<0)
 ov=len(pairs)/max(1,min(len(r),len(w)))
 # exact chronology for standalone and 50/50 portfolio over V7.1 cohort
 pr=[(float(x['cutoff_ts']),float(x['net_return']),1.0) for x in r]
 pw=[(float(x['cutoff_ts']),float(x['net_return']),1.0) for x in w]
 pp=[(float(x['cutoff_ts']),float(x['net_return']),0.5) for x in r]+[(float(x['cutoff_ts']),float(x['net_return']),0.5) for x in w]
 rr,rdd=path(pr);wr,wdd=path(pw);pret,pdd=path(pp)
 marg=pret-rr; mdd=abs(pdd)-abs(rdd)
 if pret>rr and abs(pdd)<=abs(rdd):ver='DOMINATES_R64'
 elif pret>=0 and abs(pdd)<abs(rdd):ver='DIVERSIFICATION_BENEFIT'
 elif pret<rr and abs(pdd)>=abs(rdd):ver='DILUTES_R64'
 else:ver='MIXED'
 row=(time.time(),aid,'PRIMARY_50_50',len(r),len(w),len(pairs),ov,corr(ra,wa),both/len(pairs) if pairs else None,pret,pdd,rr,rdd,wr,wdd,marg,mdd,ver,f'TEMPORAL_NEAREST_PAIR_WINDOW_{int(WINDOW_S)}S')
 o=sqlite3.connect(OUT,timeout=10);o.execute('INSERT INTO v72_summary VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',row);o.commit();o.close()
 print('\n'+'='*178);print('MEMECOIN LAB — PORTFOLIO REALITY AUDIT V7.2');print('='*178)
 print(f'arena={aid} | temporal pairing window=±{WINDOW_S:.0f}s | risk={RISK_PCT:.2f}% per full loss unit | V7.1 READ-ONLY\n')
 print(f'R64    DONE={len(r):>3} return={rr:+6.2f}% TRUE_DD={rdd:+6.2f}%')
 print(f'WALLET DONE={len(w):>3} return={wr:+6.2f}% TRUE_DD={wdd:+6.2f}%')
 print(f'50/50  events={len(r)+len(w):>3} return={pret:+6.2f}% TRUE_DD={pdd:+6.2f}% marginal_return_vs_R64={marg:+6.2f}pp marginal_DD={mdd:+6.2f}pp')
 print(f'paired_temporal={len(pairs)} overlap={100*ov:.1f}% corr={corr(ra,wa) if pairs else None} both_loss={100*(both/len(pairs) if pairs else 0):.1f}%')
 print(f'VERDICT={ver}')
 print('\nGuardrail: audit only. Does not alter V7.1 or select the secondary 75/25 allocation post hoc.',flush=True)

def main():
 init();print(f'V7.2 started | read={RDB} | write={OUT}',flush=True)
 while True:
  try:cycle()
  except Exception as e:print('V7.2 error:',repr(e),flush=True)
  time.sleep(30)
if __name__=='__main__':main()
