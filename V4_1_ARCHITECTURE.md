# Memecoin Lab V4.1 — Autonomous Research Engine

## Objective

Turn the existing V4 research scripts into a durable research organism that can run many independent experiments concurrently without reintroducing the V2/V3 single-writer/socket failure mode.

V4.1 is research-only. Live trading remains disabled.

## Findings from the current codebase

### Keep

- `autonomous_lab_v4.py`: good minimal foundation. It deliberately uses one orchestrator, no research IPC/socket, a read-only market DB, WAL research DB, and explicit research tables.
- `autonomous_lab_v4_fast.py`: useful statistical primitives and deterministic token holdout. Its resurrection and migration loaders are a useful starting point.
- `autonomous_lab_v4_robust.py`: robustness stage should remain separate from discovery.
- `autonomous_lab_v4_flow_dynamics.py`: feature-family laboratory, but its output should become jobs/results rather than a standalone terminal workflow.
- Existing V4 on-chain/migration/wallet research scripts: treat as research adapters, not supervisors.

### Do not revive

- V2/V3 socket-based central writer architecture.
- Process supervisors that repeatedly restart crashing research components.
- Unlimited hypothesis generation disconnected from available data.
- A queue that can create jobs faster than workers can drain them without backpressure.
- Reusing the same holdout for iterative threshold tuning.

## Data reality

The historical swap table is an event sample, not a complete lifecycle dataset. The coverage audit found 8,227 swaps over 1,587 tokens (5.18 swaps/token average); 45.37% of tokens have only one observed swap and only 66 tokens have >=20 swaps over >=300 seconds.

However, wallet/signature/price/side/SOL fields are complete in the observed rows, with 5,123 unique wallets and 1,041 wallets observed on multiple tokens. Therefore V4.1 should distinguish cross-sectional research from trajectory research and give wallet-history research its own branch.

## Architecture

```text
validation_v090.db (READ ONLY)
          |
          v
  Dataset Registry / Coverage Gate
          |
          v
  Hypothesis Director ---- Scientific Memory
          |
          v
       Job Queue
          |
   +------+------+------+------+
   |      |      |      |      |
 worker worker worker worker ... adaptive pool
   |      |      |      |      |
   +------+------+------+------+
          |
          v
       Results
          |
          +--> reject/archive
          +--> refine
          +--> robustness arena
          +--> freeze candidate
          +--> prospective registry

research_v4_1.db owns research state only.
No Unix socket. No central writer process. No live money.
```

## SQLite concurrency policy

1. Every worker opens its own short-lived SQLite connection.
2. WAL + `busy_timeout` on research DB.
3. Workers claim jobs with a short `BEGIN IMMEDIATE` transaction and commit immediately.
4. Expensive market-data reads and statistical work happen outside a write transaction.
5. Result writes are short transactions.
6. Jobs have leases; stale RUNNING jobs can be reclaimed after a crash.
7. Queue generation has hard backpressure.

This keeps the useful simplicity of V4 while permitting parallel workers.

## Core tables

### `v41_hypotheses`
One scientific idea, deduplicated by a canonical fingerprint.

Important fields: branch, family, spec_json, data_requirement_json, status, parent_hypothesis_id, generation, created_at.

### `v41_jobs`
Executable research work.

States: `QUEUED`, `RUNNING`, `DONE`, `FAILED`, `CANCELLED`.

Important fields: priority, worker_id, lease_until, attempts, created_at, started_at, finished_at.

### `v41_results`
Immutable experiment output. Store sample sizes, discovery metrics, holdout metrics, multiple-testing metadata, coverage diagnostics, and full JSON result.

### `v41_memory`
Scientific lessons, including negative results. This is what prevents the machine from rediscovering the same failed idea forever.

### `v41_candidates`
Promoted candidates only. A candidate records the exact frozen feature set/model/threshold and the data boundary used to create it.

### `v41_dataset_registry`
Tracks what data a research family is actually allowed to use. A trajectory experiment cannot silently pretend 1,587 tokens have deep histories.

## Research lanes

### Cross-sectional lane
Can use the broad token population when features are observable at the decision timestamp.

Branches:
- migration
- early success/failure
- wallet structure
- recurring-wallet history
- creator/account structure when available

### Deep-trajectory lane
Requires explicit coverage gates such as >=N swaps and >=T observed seconds.

Branches:
- flow acceleration
- imbalance dynamics
- reversal/continuation
- sequence motifs
- post-dump resurrection

### Wallet-history lane
All wallet reputation features must be point-in-time safe. At timestamp T, wallet history may use only events strictly before T.

Candidate features:
- previous tokens touched
- previous migration count/rate
- prior success rate
- prior median outcome
- repeat-wallet breadth
- known-wallet concentration
- weighted historical wallet quality
- independent-wallet count
- wallet novelty rate

## Research funnel

`DISCOVERY -> REFINEMENT -> ROBUSTNESS -> FROZEN -> PROSPECTIVE -> SHADOW`

Discovery is allowed to be cheap and broad. Holdout/prospective data are not allowed to mutate hypotheses.

### Discovery
Generate many scientifically distinct candidates, but only while queue depth is below the backpressure target.

### Refinement
Only promising families get feature ablations, stage/horizon variants, interactions, and nearby parameter variants.

### Robustness
Use repeated deterministic/time-aware splits, bootstrap/stability tests, permutation/null tests, minimum positive counts, and multiple-testing correction. Report uncertainty rather than a single score.

### Frozen candidate
Persist exact specification and data cutoff. No mutation after freeze.

### Prospective
Only new observations after the freeze boundary may score the candidate.

## Multiple-testing control

V4.1 must count the number of hypotheses attempted. A low nominal p-value after thousands of searches is not sufficient evidence. Store experiment family, generation, and search count, then apply FDR-style correction within sensible families and require effect-size/stability criteria in addition to significance.

## Backpressure

The director should target a queue measured in minutes of work, not thousands of jobs.

Example policy:

- target queued jobs = `workers * 8`
- stop generation above `workers * 16`
- resume below `workers * 4`
- prefer refinement of promising families over random expansion when backlog is high

## Worker pool

Start conservatively at roughly half the logical CPU count, bounded by configuration. Adapt based on throughput and failure/lock rates, not merely CPU load.

Workers are generic. Research logic lives in adapters so adding a new family does not require another supervisor process.

## V5 data factory interface

V5 should be a separate collector subsystem and should not write research decisions. It should append raw, reconstructable events.

Minimum raw event contract:

- token mint
- signature
- slot/block time
- transaction index/order when available
- wallet / relevant account identities
- side
- SOL amount/delta
- token amount/delta
- executable price or sufficient reserves/deltas to reconstruct it
- program / venue / curve or pool identity
- migration event and pool identity
- raw payload/reference for later parser upgrades

Store raw events once; derive 1s/5s/30s/1m/event/volume/wallet bars later.

## Immediate build order

1. Implement V4.1 DB schema and job leasing.
2. Implement generic worker pool with graceful shutdown.
3. Port existing V4 fast experiments into adapters.
4. Add dataset coverage gates.
5. Add scientific memory + hypothesis fingerprints.
6. Add queue backpressure/director.
7. Add robustness promotion and frozen candidate registry.
8. Add point-in-time wallet-history adapter.
9. Build V5 collector separately.
10. Use new V5 observations as prospective data rather than repeatedly mining the historical holdout.
