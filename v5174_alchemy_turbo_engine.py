#!/usr/bin/env python3
"""V5.1.7.4 — Alchemy half-sampling turbo prospective epoch.

Same scientific/runtime logic as v5171, but defaults to deterministic 1/2
CREATE admission to increase prospective token throughput. Creates a fresh
acquisition epoch automatically through the imported base engine.
"""
import os
os.environ.setdefault('MEMECOIN_V517_BASE_SAMPLE_MOD','2')
os.environ.setdefault('MEMECOIN_V517_HOT_TTL_S','180')
os.environ.setdefault('MEMECOIN_V517_BASE_RPS','12')
os.environ.setdefault('MEMECOIN_V517_MAX_RPS','30')
os.environ.setdefault('MEMECOIN_V517_WORKERS','16')
import runpy

if __name__ == '__main__':
    runpy.run_module('v5171_alchemy_prospective_engine', run_name='__main__')
