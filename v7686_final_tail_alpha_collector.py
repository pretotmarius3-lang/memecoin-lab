#!/usr/bin/env python3
"""MEMECOIN LAB — FINAL CLEAN TAIL-ALPHA COLLECTOR V7.6.8.6

Continuously calls the immutable V7.6.8.5 validator ingest/display loop.
Does not change activation, cutoff, thresholds, or historical evidence.
"""
from __future__ import annotations
import os, signal, time
import v7685_final_tail_alpha_validator as v

POLL=float(os.environ.get('MEMECOIN_V7686_POLL_S','5'))
STOP=False

def stop(*_):
    global STOP
    STOP=True

def main():
    signal.signal(signal.SIGINT,stop)
    signal.signal(signal.SIGTERM,stop)
    v.init()
    total=0
    print(f'MEMECOIN LAB V7.6.8.6 FINAL CLEAN TAIL-ALPHA COLLECTOR | poll={POLL:.1f}s',flush=True)
    while not STOP:
        try:
            n=v.ingest(); total+=n
            print(f'V7686 heartbeat new={n} total_new={total}',flush=True)
            if n:
                v.display()
        except Exception as e:
            print(f'V7686 error: {e!r}',flush=True)
        time.sleep(POLL)

if __name__=='__main__':
    main()
