#!/usr/bin/env python3
"""Hotfix launcher for the Helius Free collector.

Fixes two issues in v5_helius_collector_free.py:
1. pack() called hints() without the required logs argument, which killed every
   fetch worker on its first successful getTransaction response.
2. fetch workers had no outer exception guard, so an unexpected parser/storage
   error could silently kill a worker and leave the WebSocket filling the queue.

This launcher reuses the existing collector implementation and monkey-patches the
faulty functions before starting it. No API key is stored here.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
import traceback

import v5_helius_collector_free as base


def fixed_pack(source, pid, sub_id, signature, slot, logs, tx):
    event, token, creator = base.hints(tx, source, logs)
    raw = base.json.dumps(
        {
            "signature": signature,
            "slot": slot or tx.get("slot"),
            "logs": logs,
            "rpc_transaction": tx,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    comp = base.zlib.compress(raw, 3)
    return (
        signature,
        source,
        pid,
        sub_id,
        slot or tx.get("slot"),
        None,
        time.time(),
        event,
        token,
        creator,
        sqlite3.Binary(comp),
        len(raw),
        len(comp),
    )


async def fixed_fetch_worker(index, q, counters, session_id):
    """Worker that cannot disappear silently after one malformed transaction."""
    while not base.STOP.is_set() or not q.empty():
        try:
            item = await asyncio.wait_for(q.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        source, pid, sub_id, signature, slot, logs = item
        try:
            tx = None
            last_err = None
            attempts = 0

            for attempt in range(1, base.MAX_FETCH_RETRIES + 1):
                attempts = attempt
                try:
                    tx = await base.get_transaction(signature)
                    if tx is not None:
                        break
                    last_err = "getTransaction returned null"
                except base.urllib.error.HTTPError as exc:
                    body = ""
                    try:
                        body = exc.read().decode("utf-8", errors="replace")[:500]
                    except Exception:
                        pass
                    last_err = f"HTTP {exc.code}: {exc.reason} {body}".strip()
                    if exc.code == 401:
                        raise RuntimeError("Helius API key rejected (HTTP 401)") from exc
                    if exc.code == 429:
                        counters["http_429"] = counters.get("http_429", 0) + 1
                except Exception as exc:
                    last_err = repr(exc)

                await asyncio.sleep(min(5.0, 0.25 * (2 ** (attempt - 1))))

            if tx is None:
                db = base.open_db()
                db.execute(
                    """INSERT INTO v5_fetch_failures(signature,source_program,slot,attempts,last_error,updated_at)
                       VALUES(?,?,?,?,?,?)
                       ON CONFLICT(signature) DO UPDATE SET
                         attempts=excluded.attempts,last_error=excluded.last_error,updated_at=excluded.updated_at""",
                    (signature, source, slot, attempts, last_err, time.time()),
                )
                base.set_state(db, "last_fetch_error", last_err)
                db.commit()
                db.close()
                counters["fetch_failed"] += 1
                if counters["fetch_failed"] <= 5 or counters["fetch_failed"] % 100 == 0:
                    print(
                        f"fetch failed #{counters['fetch_failed']:,} "
                        f"sig={signature[:10]}... error={last_err}"
                    )
                continue

            row = fixed_pack(source, pid, sub_id, signature, slot, logs, tx)
            await base.store_row(row, counters, session_id)
            counters["fetched"] += 1

            if counters["fetched"] <= 5 or counters["fetched"] % 100 == 0:
                print(
                    f"fetched={counters['fetched']:,} "
                    f"inserted={counters['inserted']:,} "
                    f"failed={counters['fetch_failed']:,} "
                    f"429={counters.get('http_429',0):,} "
                    f"backlog={q.qsize():,} "
                    f"db={base.db_size_bytes()/1024**2:,.1f}MB"
                )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            counters["worker_errors"] = counters.get("worker_errors", 0) + 1
            msg = f"WORKER-{index} ERROR on {signature[:12]}...: {exc!r}"
            print(msg)
            if counters["worker_errors"] <= 3:
                traceback.print_exc()
            try:
                db = base.open_db()
                base.set_state(db, "last_worker_error", msg)
                db.commit()
                db.close()
            except Exception:
                pass
        finally:
            q.task_done()


# Patch globals used by base.main_async().
base.pack = fixed_pack
base.fetch_worker = fixed_fetch_worker


if __name__ == "__main__":
    base.main()
