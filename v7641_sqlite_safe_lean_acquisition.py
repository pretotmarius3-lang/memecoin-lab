#!/usr/bin/env python3
"""MEMECOIN LAB V7.6.4.1 — SQLITE-SAFE LEAN ACQUISITION

Same lean burst acquisition policy as V7.6.4, but avoids executing
PRAGMA journal_mode=WAL on every short-lived worker connection.
The raw DB is already persistently in WAL mode; worker connections only set
synchronous=NORMAL and busy_timeout.

Research infrastructure only. Never signs or submits transactions.
"""
from __future__ import annotations
import asyncio, sqlite3

import v764_lean_burst_acquisition as v764

base=v764.base


def safe_db():
    c=sqlite3.connect(base.DB_PATH,timeout=30)
    c.row_factory=sqlite3.Row
    c.execute('PRAGMA synchronous=NORMAL')
    c.execute('PRAGMA busy_timeout=30000')
    return c

# The database was verified to already be in WAL mode. Avoid repeatedly
# renegotiating journal mode from many worker threads.
base.db=safe_db

async def main():
    print('MEMECOIN LAB V7.6.4.1 sqlite-safe lean burst acquisition',flush=True)
    print('worker db(): no per-connection PRAGMA journal_mode=WAL',flush=True)
    await v764.main()

if __name__=='__main__':
    asyncio.run(main())
