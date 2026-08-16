#!/usr/bin/env python3

import os
import time

import research_client as rc


MIN_WORKERS = 4
DEFAULT_WORKERS = 6
MAX_WORKERS = 12


def loadavg():

    try:
        return os.getloadavg()[0]
    except Exception:
        return 0.0


def queue_count():

    db = rc.readonly()

    n = db.execute("""
    SELECT COUNT(*)
    FROM jobs
    WHERE
        job_type='DISCOVERY_V3'
        AND status='QUEUED'
    """).fetchone()[0]

    db.close()

    return n


def recommendation():

    q = queue_count()
    load = loadavg()

    cpu = (
        os.cpu_count()
        or 4
    )

    normalized = (
        load/cpu
        if cpu
        else 0
    )

    workers = DEFAULT_WORKERS

    if q > 1000 and normalized < 0.70:
        workers = 12

    elif q > 500 and normalized < 0.70:
        workers = 10

    elif q > 250 and normalized < 0.75:
        workers = 8

    elif q < 60:
        workers = 4

    workers = max(
        MIN_WORKERS,
        min(
            MAX_WORKERS,
            workers
        )
    )

    return {
        "queue":q,
        "load1":load,
        "normalized_load":normalized,
        "recommended_workers":workers,
    }


def main():

    while True:

        r = recommendation()

        rc.execute("""
        INSERT INTO factory_state (
            key,
            value,
            updated_at
        )
        VALUES (
            'recommended_workers',
            ?,
            ?
        )
        ON CONFLICT(key)
        DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,(
            str(
                r[
                    "recommended_workers"
                ]
            ),
            time.time(),
        ))

        rc.execute("""
        INSERT INTO factory_state (
            key,
            value,
            updated_at
        )
        VALUES (
            'governor_snapshot',
            ?,
            ?
        )
        ON CONFLICT(key)
        DO UPDATE SET
            value=excluded.value,
            updated_at=excluded.updated_at
        """,(
            str(r),
            time.time(),
        ))

        time.sleep(5)


if __name__ == "__main__":
    main()
