#!/usr/bin/env python3
"""V5.1.7.5 — full-stream Alchemy prospective epoch.

Same scientific/runtime logic as V5.1.7.1, but deterministic admission is 1/1.
This maximizes prospective token throughput while preserving the V6.4 freeze
and the adaptive HTTP queue/rate protections.
"""
import os
os.environ.setdefault('MEMECOIN_V517_BASE_SAMPLE_MOD','1')
os.environ.setdefault('MEMECOIN_V517_HOT_TTL_S','180')
os.environ.setdefault('MEMECOIN_V517_BASE_RPS','12')
os.environ.setdefault('MEMECOIN_V517_MAX_RPS','30')
os.environ.setdefault('MEMECOIN_V517_WORKERS','16')
os.environ.setdefault('MEMECOIN_V517_MAX_HOT','64')

import asyncio
import v5171_alchemy_prospective_engine as patched

if __name__ == '__main__':
    asyncio.run(patched.base.main())
