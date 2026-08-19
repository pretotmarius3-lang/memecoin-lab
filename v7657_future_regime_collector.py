#!/usr/bin/env python3
"""MEMECOIN LAB — FUTURE REGIME COLLECTOR V7.6.5.7

Continuous companion for V7.6.5.6. The preregistered validator itself is a
one-shot ingest/report command; this daemon calls its fixed ingest logic on a
small cadence so future-only evidence accumulates automatically.

Guardrails:
- Does not alter the V7656 cutoff or hypothesis.
- Does not use historical outcomes as validation evidence.
- Only appends newly matured post-cutoff rows through V7656.ingest().
"""
from __future__ import annotations
import os, signal, time
import v7656_preregistered_future_regime_validator as v

POLL=float(os.environ.get('MEMECOIN_V7657_POLL_S','5'))
REPORT=float(os.environ.get('MEMECOIN_V7657_REPORT_S','30'))
STOP=False

def stop(*_):
 global STOP; STOP=True

def main():
 signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
 v.init();last=0.0;total_new=0
 print(f'MEMECOIN LAB V7.6.5.7 FUTURE REGIME COLLECTOR | poll={POLL:.1f}s',flush=True)
 while not STOP:
  try:
   made=v.ingest();total_new+=made;now=time.time()
   if made or now-last>=REPORT:
    print(f'V7657 heartbeat new={made} total_new={total_new}',flush=True)
    v.display();last=now
  except Exception as e:
   print('V7657 error:',repr(e),flush=True)
  time.sleep(POLL)
 print('V7657 stopped cleanly',flush=True)

if __name__=='__main__': main()
