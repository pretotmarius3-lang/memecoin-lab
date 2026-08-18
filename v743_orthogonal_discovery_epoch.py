#!/usr/bin/env python3
"""MEMECOIN LAB — ORTHOGONAL FRESH DISCOVERY EPOCH V7.4.3

Parallel research rail while V7.4.2.2 collects prospective evidence.
Starts a brand-new post-start discovery epoch in its own DB and deliberately
excludes features already occupying the validation/control rail.

This script reuses the V7.4 robust discovery machinery without modifying its DB,
rules, or any frozen arena. Discovery output is screening evidence only.
"""
from pathlib import Path
import os
import v74_fresh_robust_discovery_epoch as lab

ROOT=Path.home()/"memecoin_lab"
lab.OUT=ROOT/'v743_orthogonal_discovery.db'

# Quarantine features already used by the control / active validation rail.
# We want genuinely new candidate mechanisms, not parameter clones of R64 or
# the three V7.4.2.x challengers.
QUARANTINED={
    'price_velocity',       # R64 control
    'gross_sol',            # CAPITAL_FLOW validator
    'buy_ratio_delta',      # FLOW_DYNAMICS validator
    'repeat_wallet_ratio',  # WALLET_STRUCTURE validator / failed evidence
    'flow_acceleration',    # prior failed-forward FLOW evidence
}
lab.FEATURES=tuple(f for f in lab.FEATURES if f not in QUARANTINED)

# Slightly stricter family diversity for the new research rail. These remain
# discovery gates only; any survivor still needs a new immutable future arena.
lab.MIN_FAMILY_INDEP=max(lab.MIN_FAMILY_INDEP,2)
lab.MIN_FAMILY_UNIQUE=max(lab.MIN_FAMILY_UNIQUE,18)
lab.MAX_PAIR_OVERLAP=min(lab.MAX_PAIR_OVERLAP,0.60)


def banner():
    print('\n'+'='*170)
    print('MEMECOIN LAB — ORTHOGONAL FRESH DISCOVERY EPOCH V7.4.3')
    print('='*170)
    print('OUTPUT='+str(lab.OUT))
    print('Parallel rail: V7.4.2.2 validation remains untouched.')
    print('Quarantined active/known features: '+', '.join(sorted(QUARANTINED)))
    print('Goal: new families/features with independent replication; no frozen-rule retuning.')
    print()


def main():
    banner()
    # lab.main() creates a fresh immutable epoch boundary because OUT is a new DB.
    lab.main()


if __name__=='__main__':
    main()
