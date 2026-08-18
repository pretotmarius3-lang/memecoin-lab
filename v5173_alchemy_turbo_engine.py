#!/usr/bin/env python3
"""V5.1.7.3 — Alchemy high-throughput prospective epoch.

Same acquisition/scientific logic as V5.1.7.1, with deterministic 1/4 CREATE
admission. A fresh acquisition epoch is created automatically by the base engine.
V6.4's frozen rule is untouched.
"""
import os
os.environ.setdefault('MEMECOIN_V517_BASE_SAMPLE_MOD','4')
os.environ.setdefault('MEMECOIN_V517_HOT_TTL_S','180')
os.environ.setdefault('MEMECOIN_V517_BASE_RPS','12')
os.environ.setdefault('MEMECOIN_V517_MAX_RPS','30')
os.environ.setdefault('MEMECOIN_V517_WORKERS','16')
os.environ.setdefault('MEMECOIN_V517_MAX_HOT','64')
import asyncio
import v5171_alchemy_prospective_engine as engine

if __name__=='__main__':
    asyncio.run(engine.base.main())
