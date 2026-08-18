#!/usr/bin/env python3
"""V5.1.7.1 — hotfix for V5.1.7 queue INSERT cardinality.

The original V5.1.7 enqueue statement supplied 13 VALUES for 12 columns when a
CREATE was admitted. This wrapper preserves every V5.1.7 scientific/runtime
rule and replaces only enqueue().
"""
import asyncio, json, time
import v517_alchemy_prospective_engine as base


def enqueue_fixed(sig,mint,kind,source,slot,logs,mod=None):
    now=time.time()
    c=base.db()
    before=c.total_changes
    c.execute('''INSERT OR IGNORE INTO v515_hot_queue(
        signature,mint,kind,source,slot,logs_json,status,attempts,
        first_seen,updated_at,epoch_id,admission_mod
      ) VALUES(?,?,?,?,?,?,'PENDING',0,?,?,?,?)''',(
        sig,mint,kind,source,slot,
        json.dumps(logs,separators=(',',':')),
        now,now,base.EPOCH_ID,mod
    ))
    added=c.total_changes-before
    c.commit(); c.close()
    return bool(added)


base.enqueue=enqueue_fixed

if __name__=='__main__':
    asyncio.run(base.main())
