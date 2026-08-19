#!/usr/bin/env python3
"""MEMECOIN LAB V7.6.4 — LEAN BURST ACQUISITION

Purpose
-------
Run the V7.5.0 burst-capacity acquisition policy WITHOUT the synchronous trace
writes that were installed on enqueue/rpc/store. This isolates the live WebSocket
and HTTP path from instrumentation-induced event-loop blocking.

Keeps:
- V7.5.0 capacity settings (64 workers, 30->60 RPS)
- burst-aware claim priority
- burst-aware admission policy
- fast 2s controller

Removes from the hot path:
- per-event v750_acquisition_trace.db writes on enqueue
- per-request trace writes before/after HTTP
- per-store trace writes
- V7.5.0 trace reporter

Research infrastructure only. Never signs or submits transactions.
"""
from __future__ import annotations
import asyncio, time

# Importing v750 applies its capacity settings and exposes the original base
# functions saved before trace instrumentation.
import v750_burst_resilient_alchemy_engine as v750

base = v750.base

# Restore the uninstrumented hot-path functions while keeping burst policies.
base.enqueue = v750._orig_enqueue
base.rpc_get_tx = v750._orig_rpc
base.store_tx = v750._orig_store
base.claim_one = v750.claim_one_burst
base.admission_mod = v750.admission_mod_burst
base.controller = v750.controller_burst

async def lightweight_reporter():
    while not base.STOP.is_set():
        await asyncio.sleep(10)
        try:
            q, pc, ph, fetching, age = await asyncio.to_thread(v750.queue_snapshot)
            mod = await asyncio.to_thread(v750.admission_mod_burst)
            print('\n===== V7.6.4 LEAN BURST ACQUISITION =====', flush=True)
            print(
                f'epoch={base.EPOCH_ID} rps={base.CURRENT_RPS:.1f} pending={q} '
                f'CREATE={pc} HOT={ph} fetching={fetching} oldest={age:.2f}s '
                f'admission={"PAUSE" if mod is None else "1/"+str(mod)}',
                flush=True,
            )
        except Exception as e:
            print(f'V764 reporter error: {e!r}', flush=True)

async def main():
    print('MEMECOIN LAB V7.6.4 lean burst acquisition | trace-free hot path', flush=True)
    print(
        f'epoch={base.EPOCH_ID} commitment={base.COMMITMENT} '
        f'workers={base.WORKERS} rps={base.BASE_RPS}->{base.MAX_RPS}',
        flush=True,
    )
    await asyncio.gather(base.main(), lightweight_reporter())

if __name__ == '__main__':
    asyncio.run(main())
