#!/usr/bin/env python3
"""MEMECOIN LAB V7.6.8.4 — CLEAN TAIL-ALPHA COLLECTOR
Continuously ingests only future rows into V7683. Does not alter activation, cutoff or rule.
"""
from __future__ import annotations
import os,time,traceback
import v7683_clean_tail_alpha_validator as v
POLL=float(os.environ.get('MEMECOIN_V7684_POLL_S','5'))

def main():
 v.init(); total=0
 print(f'MEMECOIN LAB V7.6.8.4 CLEAN TAIL-ALPHA COLLECTOR | poll={POLL:.1f}s',flush=True)
 while True:
  try:
   n=v.ingest(); total+=n; print(f'V7684 heartbeat new={n} total_new={total}',flush=True)
   if n: v.display()
  except KeyboardInterrupt: break
  except Exception as e:
   print('V7684 error:',repr(e),flush=True); traceback.print_exc()
  time.sleep(POLL)
if __name__=='__main__': main()
