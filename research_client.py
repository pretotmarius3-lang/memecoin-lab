#!/usr/bin/env python3

import json
import socket
import sqlite3
import time
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"

RESEARCH_DB = ROOT / "research_lab.db"
SOCKET_PATH = ROOT / ".research_writer.sock"


def request(payload, timeout=10.0, retries=40):

    data = (
        json.dumps(payload, separators=(",", ":"))
        + "\n"
    ).encode()

    last_error = None

    for attempt in range(retries):
        try:
            with socket.socket(
                socket.AF_UNIX,
                socket.SOCK_STREAM
            ) as sock:

                sock.settimeout(timeout)
                sock.connect(str(SOCKET_PATH))
                sock.sendall(data)

                buf = b""

                while b"\n" not in buf:
                    chunk = sock.recv(65536)

                    if not chunk:
                        break

                    buf += chunk

            if not buf:
                raise RuntimeError(
                    "research_writer returned no response"
                )

            raw = buf.split(b"\n", 1)[0]
            response = json.loads(raw.decode())

            if not response.get("ok"):
                raise RuntimeError(
                    response.get(
                        "error",
                        "unknown writer error"
                    )
                )

            return response

        except (
            FileNotFoundError,
            ConnectionRefusedError,
            ConnectionResetError,
            BrokenPipeError,
            socket.timeout,
        ) as exc:

            last_error = exc
            time.sleep(min(0.10 * (attempt + 1), 1.0))

    raise RuntimeError(
        f"research_writer unavailable after {retries} attempts: "
        f"{last_error!r}"
    )


def ping():
    return request({
        "op": "ping"
    })


def execute(sql, params=()):
    return request({
        "op": "execute",
        "sql": sql,
        "params": list(params),
    })


def event(
    event_type,
    source,
    payload=None,
    severity="INFO"
):
    return request({
        "op": "event",
        "event_type": event_type,
        "source": source,
        "severity": severity,
        "payload": payload or {},
    })


def readonly():

    db = sqlite3.connect(
        f"file:{RESEARCH_DB}?mode=ro",
        uri=True,
        timeout=10,
    )

    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=10000")

    return db
