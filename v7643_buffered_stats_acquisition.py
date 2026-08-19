#!/usr/bin/env python3
"""MEMECOIN LAB V7.6.4.3 — BUFFERED-STATS LOCK-EFFICIENT ACQUISITION

Successor to V7.6.4.2.

Diagnosis addressed
-------------------
The queue claimer was made lock-efficient in V7.6.4.2, but the base Alchemy
engine still called bump() synchronously on the asyncio event loop for nearly
every WebSocket message and HTTP request/result. Each bump() opened SQLite,
updated v517_provider_stats, committed, and closed the connection. During HOT
bursts that turns observability into event-loop / SQLite backpressure and can
also starve WS keepalive handling.

This version keeps the V7642 claim path unchanged and replaces per-event stats
DB writes with in-memory counters flushed to SQLite once per second.

Scientific guardrail: infrastructure-only change. Any strategy confirmation
requires a fresh post-stability cutoff. Never run alongside another acquisition
engine sharing v5_raw_events.db.
"""
from __future__ import annotations

import asyncio
import threading
import time

import v7642_lock_efficient_lean_acquisition as v7642

base = v7642.base

_ALLOWED = {
    'ws_messages','creates_seen','creates_admitted','hot_logs','hot_enqueued',
    'requests','ok','inserted','nulls','http_429','errors','reconnects'
}
_LOCK = threading.Lock()
_PENDING = {k: 0 for k in _ALLOWED}
_LAST_ERROR = None


def buffered_bump(field, n=1, error=None):
    """Hot-path stats update: memory only; no SQLite and no blocking I/O."""
    global _LAST_ERROR
    with _LOCK:
        if field in _ALLOWED:
            _PENDING[field] += int(n)
        if error is not None:
            _LAST_ERROR = str(error)[-1200:]


def _take_pending():
    global _LAST_ERROR
    with _LOCK:
        snap = dict(_PENDING)
        err = _LAST_ERROR
        for k in _PENDING:
            _PENDING[k] = 0
        _LAST_ERROR = None
    return snap, err


def _restore_pending(snap, err):
    global _LAST_ERROR
    with _LOCK:
        for k, v in snap.items():
            _PENDING[k] += int(v)
        if err is not None:
            _LAST_ERROR = err


def flush_stats_once():
    snap, err = _take_pending()
    if not any(snap.values()) and err is None:
        return
    try:
        c = base.db()
        sets = ['updated_at=?', 'current_rps=?']
        vals = [time.time(), float(base.CURRENT_RPS)]
        for k, v in snap.items():
            if v:
                sets.append(f'{k}={k}+?')
                vals.append(int(v))
        if err is not None:
            sets.append('last_error=?')
            vals.append(err)
        vals.append(base.EPOCH_ID)
        c.execute('UPDATE v517_provider_stats SET ' + ','.join(sets) + ' WHERE epoch_id=?', vals)
        c.commit()
        c.close()
    except Exception:
        _restore_pending(snap, err)
        raise


async def stats_flusher():
    # base.main() creates the epoch row during init_db(); give it a moment first.
    await asyncio.sleep(1.0)
    while not base.STOP.is_set():
        try:
            await asyncio.to_thread(flush_stats_once)
        except Exception as e:
            print(f'V7643 stats flush error: {e!r}', flush=True)
        await asyncio.sleep(1.0)
    try:
        await asyncio.to_thread(flush_stats_once)
    except Exception:
        pass


# Module-global lookups inside v517_alchemy_prospective_engine resolve this
# patched attribute, so HTTP/WS hot-path bump() calls become memory-only.
base.bump = buffered_bump


async def main():
    print('MEMECOIN LAB V7.6.4.3 buffered-stats lock-efficient acquisition', flush=True)
    print('hot-path observability: in-memory counters | SQLite stats flush=1Hz', flush=True)
    await asyncio.gather(v7642.main(), stats_flusher())


if __name__ == '__main__':
    asyncio.run(main())
