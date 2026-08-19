#!/usr/bin/env python3
"""MEMECOIN LAB — TAIL-ALPHA COLLECTOR V7.6.8.1

Continuous companion for V7.6.8. The validator is intentionally one-shot:
it initializes/fixes its cutoff, ingests currently matured future-only rows,
prints a report, and exits. This daemon calls that exact fixed ingest path on a
small cadence so the preregistered tail-alpha evidence accumulates automatically.

Guardrails:
- Does not alter the V768 cutoff, thresholds, winner/downside gates or targets.
- Does not import V767 discovery outcomes into validation evidence.
- Only appends rows through v768_preregistered_tail_alpha_validator.ingest().
"""
from __future__ import annotations
import os, signal, time
import v768_preregistered_tail_alpha_validator as v

POLL=float(os.environ.get('MEMECOIN_V7681_POLL_S','5'))
REPORT=float(os.environ.get('MEMECOIN_V7681_REPORT_S','30'))
STOP=False

def stop(*_):
 global STOP; STOP=True

def main():
 signal.signal(signal.SIGINT,stop)
 signal.signal(signal.SIGTERM,stop)
 v.init()
 last=0.0
 total_new=0
 print(f'MEMECOIN LAB V7.6.8.1 TAIL-ALPHA COLLECTOR | poll={POLL:.1f}s',flush=True)
 while not STOP:
  try:
   made=v.ingest()
   total_new+=made
   now=time.time()
   if made or now-last>=REPORT:
    print(f'V7681 heartbeat new={made} total_new={total_new}',flush=True)
    v.display()
    last=now
  except Exception as e:
   print('V7681 error:',repr(e),flush=True)
  time.sleep(POLL)
 print('V7681 stopped cleanly',flush=True)

if __name__=='__main__':
 main()
