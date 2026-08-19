#!/usr/bin/env python3
"""MEMECOIN LAB V7.6.9.3 — continuous collector for V7.6.9.2.
Calls the frozen future-only validator ingest loop every few seconds.
Does not alter thresholds, cutoff, or hypothesis.
"""
from __future__ import annotations
import os,time,traceback
import v7692_preregistered_tail_direction_validator as val

POLL=float(os.environ.get('MEMECOIN_V7693_POLL_S','5.0'))

def main():
    val.init()
    total=0
    print(f'MEMECOIN LAB V7.6.9.3 TAIL-DIRECTION COLLECTOR | poll={POLL:.1f}s',flush=True)
    while True:
        try:
            n=val.ingest(); total+=n
            print(f'V7693 heartbeat new={n} total_new={total}',flush=True)
            if n: val.display()
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f'V7693 error: {e!r}',flush=True)
            traceback.print_exc()
        time.sleep(POLL)

if __name__=='__main__': main()
