#!/usr/bin/env python3
"""Memecoin Lab V5.2 — live swap decoder + feature factory.

Reads enriched rows from v5_raw_events.db, reconstructs user-level BUY/SELL executions
from token/native-balance deltas, and continuously builds point-in-time token snapshots.

Important:
- raw compressed RPC payload remains source of truth
- this v1 decoder is balance-delta based; it does NOT claim byte-perfect Anchor event decoding
- only decoded trades with a unique non-WSOL mint and a plausible wallet token delta are stored
- no trading/signing
"""
from __future__ import annotations

import json
import math
import os
import signal
import sqlite3
import time
import zlib
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
V5_DB = Path(os.environ.get("MEMECOIN_V5_DB", ROOT / "v5_raw_events.db"))
FEATURE_DB = Path(os.environ.get("MEMECOIN_V52_DB", ROOT / "v52_features.db"))
POLL_S = float(os.environ.get("MEMECOIN_V52_POLL_S", "0.5"))
BATCH = int(os.environ.get("MEMECOIN_V52_BATCH", "500"))
SNAPSHOTS = tuple(int(x) for x in os.environ.get("MEMECOIN_V52_SNAPSHOTS", "5,10,20,30,60,120,300").split(","))
HORIZONS = tuple(int(x) for x in os.environ.get("MEMECOIN_V52_HORIZONS", "60,120,300,600,900").split(","))
WSOL = "So11111111111111111111111111111111111111112"
STOP = False


def stop_handler(*_):
    global STOP
    STOP = True


def open_v5():
    if not V5_DB.exists():
        return None
    db = sqlite3.connect(f"file:{V5_DB}?mode=ro", uri=True, timeout=20)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=20000")
    return db


def open_feature():
    db = sqlite3.connect(FEATURE_DB, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def initialize():
    FEATURE_DB.parent.mkdir(parents=True, exist_ok=True)
    db = open_feature()
    db.executescript("""
    CREATE TABLE IF NOT EXISTS v52_processed (
      signature TEXT PRIMARY KEY,
      processed_at REAL NOT NULL,
      status TEXT NOT NULL,
      reason TEXT);

    CREATE TABLE IF NOT EXISTS v52_swaps (
      signature TEXT PRIMARY KEY,
      token_mint TEXT NOT NULL,
      timestamp REAL NOT NULL,
      slot INTEGER,
      source_program TEXT NOT NULL,
      wallet TEXT NOT NULL,
      side TEXT NOT NULL,
      token_amount REAL NOT NULL,
      quote_sol REAL,
      price_sol REAL,
      confidence TEXT NOT NULL,
      observed_at REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_v52_swaps_token_ts ON v52_swaps(token_mint,timestamp);
    CREATE INDEX IF NOT EXISTS idx_v52_swaps_wallet_ts ON v52_swaps(wallet,timestamp);

    CREATE TABLE IF NOT EXISTS v52_token_events (
      signature TEXT PRIMARY KEY,
      token_mint TEXT,
      timestamp REAL NOT NULL,
      event_type TEXT NOT NULL,
      source_program TEXT,
      wallet_hint TEXT);
    CREATE INDEX IF NOT EXISTS idx_v52_events_token_ts ON v52_token_events(token_mint,timestamp);

    CREATE TABLE IF NOT EXISTS v52_snapshots (
      token_mint TEXT NOT NULL,
      stage_s INTEGER NOT NULL,
      cutoff_ts REAL NOT NULL,
      built_at REAL NOT NULL,
      swaps INTEGER NOT NULL,
      buys INTEGER NOT NULL,
      sells INTEGER NOT NULL,
      buy_ratio REAL,
      gross_sol REAL,
      net_sol REAL,
      unique_wallets INTEGER,
      repeat_wallet_ratio REAL,
      wallet_hhi REAL,
      wallet_top1_share REAL,
      avg_trade_sol REAL,
      max_trade_sol REAL,
      trade_hhi REAL,
      top1_trade_share REAL,
      return_pct REAL,
      range_pct REAL,
      flow_velocity REAL,
      flow_acceleration REAL,
      buy_ratio_delta REAL,
      price_velocity REAL,
      PRIMARY KEY(token_mint,stage_s));

    CREATE TABLE IF NOT EXISTS v52_outcomes (
      token_mint TEXT NOT NULL,
      stage_s INTEGER NOT NULL,
      horizon_s INTEGER NOT NULL,
      ready INTEGER NOT NULL,
      future_return_pct REAL,
      future_max_return_pct REAL,
      future_min_return_pct REAL,
      future_hit10 INTEGER,
      future_hit20 INTEGER,
      future_hit50 INTEGER,
      future_death50 INTEGER,
      future_migration INTEGER,
      updated_at REAL NOT NULL,
      PRIMARY KEY(token_mint,stage_s,horizon_s));

    CREATE TABLE IF NOT EXISTS v52_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at REAL NOT NULL);
    """)
    db.commit(); db.close()


def state(db, key, value):
    db.execute("""INSERT INTO v52_state(key,value,updated_at) VALUES(?,?,?)
                  ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
               (key, json.dumps(value, separators=(",", ":"), default=str), time.time()))


def parse_amount(tb):
    try:
        ui = tb.get("uiTokenAmount") or {}
        raw = ui.get("amount")
        dec = int(ui.get("decimals") or 0)
        return float(raw) / (10 ** dec), dec
    except Exception:
        return 0.0, 0


def account_keys(tx):
    try:
        keys = tx["transaction"]["message"]["accountKeys"]
    except Exception:
        return []
    out = []
    for k in keys or []:
        if isinstance(k, str):
            out.append({"pubkey": k, "signer": False, "writable": False})
        elif isinstance(k, dict) and k.get("pubkey"):
            out.append({"pubkey": str(k["pubkey"]), "signer": bool(k.get("signer")), "writable": bool(k.get("writable"))})
    return out


def token_deltas(tx):
    meta = tx.get("meta") or {}
    pre = {}
    post = {}
    info = {}
    for b in meta.get("preTokenBalances") or []:
        if not isinstance(b, dict) or b.get("accountIndex") is None or not b.get("mint"):
            continue
        amt, _ = parse_amount(b)
        idx = int(b["accountIndex"])
        pre[idx] = amt
        info[idx] = {"mint": str(b["mint"]), "owner": str(b.get("owner") or "")}
    for b in meta.get("postTokenBalances") or []:
        if not isinstance(b, dict) or b.get("accountIndex") is None or not b.get("mint"):
            continue
        amt, _ = parse_amount(b)
        idx = int(b["accountIndex"])
        post[idx] = amt
        info[idx] = {"mint": str(b["mint"]), "owner": str(b.get("owner") or info.get(idx, {}).get("owner") or "")}
    out = []
    for idx in set(pre) | set(post):
        meta_i = info.get(idx) or {}
        out.append({"index": idx, "mint": meta_i.get("mint"), "owner": meta_i.get("owner"), "delta": post.get(idx, 0.0) - pre.get(idx, 0.0)})
    return out


def native_sol_delta(tx, wallet):
    keys = account_keys(tx)
    meta = tx.get("meta") or {}
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    for i, k in enumerate(keys):
        if k["pubkey"] == wallet and i < len(pre) and i < len(post):
            lamports = float(post[i]) - float(pre[i])
            # fee is paid by account 0 in normal Solana transactions; put it back
            # so quote estimate is less biased for BUYs.
            if i == 0:
                lamports += float(meta.get("fee") or 0)
            return lamports / 1e9
    return None


def wsol_delta_for_owner(deltas, wallet):
    vals = [d["delta"] for d in deltas if d.get("mint") == WSOL and d.get("owner") == wallet]
    return sum(vals) if vals else None


def decode_swap(payload, row):
    tx = payload.get("rpc_transaction") or {}
    if not tx or (tx.get("meta") or {}).get("err") is not None:
        return None, "rpc_error"
    deltas = token_deltas(tx)
    non_wsol = sorted({d["mint"] for d in deltas if d.get("mint") and d["mint"] != WSOL and abs(d["delta"]) > 0})
    if len(non_wsol) != 1:
        return None, f"mint_count={len(non_wsol)}"
    mint = non_wsol[0]
    candidates = [d for d in deltas if d.get("mint") == mint and d.get("owner") and abs(d["delta"]) > 0]
    if not candidates:
        return None, "no_owner_delta"
    signers = {k["pubkey"] for k in account_keys(tx) if k["signer"]}
    signer_candidates = [d for d in candidates if d["owner"] in signers]
    chosen = max(signer_candidates or candidates, key=lambda d: abs(d["delta"]))
    wallet = chosen["owner"]
    token_delta = float(chosen["delta"])
    if token_delta == 0:
        return None, "zero_token_delta"
    side = "BUY" if token_delta > 0 else "SELL"
    token_amount = abs(token_delta)

    q_wsol = wsol_delta_for_owner(deltas, wallet)
    q_native = native_sol_delta(tx, wallet)
    confidence = "MEDIUM"
    quote = None
    if q_wsol is not None and abs(q_wsol) > 0:
        quote = abs(q_wsol)
        confidence = "HIGH"
    elif q_native is not None and abs(q_native) > 0:
        quote = abs(q_native)
        confidence = "MEDIUM"
    price = quote / token_amount if quote is not None and token_amount > 0 else None
    ts = tx.get("blockTime") or payload.get("blockTime") or row["observed_at"]
    return {
        "signature": row["signature"],
        "token_mint": mint,
        "timestamp": float(ts),
        "slot": row["slot"],
        "source_program": row["source_program"],
        "wallet": wallet,
        "side": side,
        "token_amount": token_amount,
        "quote_sol": quote,
        "price_sol": price,
        "confidence": confidence,
        "observed_at": float(row["observed_at"]),
    }, None


def decode_payload(row):
    try:
        raw = zlib.decompress(row["payload_zlib"])
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"payload_decode: {exc!r}")


def process_new_rows():
    v5 = open_v5()
    if v5 is None:
        return 0, 0
    fdb = open_feature()
    rows = v5.execute("""SELECT signature,source_program,slot,observed_at,event_hint,token_hint,creator_hint,payload_zlib
                         FROM v5_raw_transactions
                         WHERE signature NOT IN (SELECT signature FROM main.v5_raw_transactions LIMIT 0)
                         ORDER BY observed_at ASC""").fetchall()
    # SQLite cannot reference the feature DB from this read-only connection, so filter locally.
    processed = {r[0] for r in fdb.execute("SELECT signature FROM v52_processed").fetchall()}
    rows = [r for r in rows if r["signature"] not in processed][:BATCH]
    v5.close()
    if not rows:
        fdb.close(); return 0, 0

    decoded = 0
    fdb.execute("BEGIN IMMEDIATE")
    try:
        for row in rows:
            status = "IGNORED"; reason = None
            try:
                payload = decode_payload(row)
                event = (row["event_hint"] or "OTHER").upper()
                ts = float((payload.get("rpc_transaction") or {}).get("blockTime") or row["observed_at"])
                if event in ("CREATE", "MIGRATE"):
                    fdb.execute("INSERT OR IGNORE INTO v52_token_events(signature,token_mint,timestamp,event_type,source_program,wallet_hint) VALUES(?,?,?,?,?,?)",
                                (row["signature"], row["token_hint"], ts, event, row["source_program"], row["creator_hint"]))
                if event in ("BUY", "SELL"):
                    swap, reason = decode_swap(payload, row)
                    if swap:
                        fdb.execute("""INSERT OR IGNORE INTO v52_swaps(signature,token_mint,timestamp,slot,source_program,wallet,side,token_amount,quote_sol,price_sol,confidence,observed_at)
                                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                                    tuple(swap[k] for k in ("signature","token_mint","timestamp","slot","source_program","wallet","side","token_amount","quote_sol","price_sol","confidence","observed_at")))
                        status = "SWAP"; decoded += 1
                    else:
                        status = "UNDECODED"
                elif event in ("CREATE", "MIGRATE"):
                    status = event
                else:
                    status = "OTHER"
            except Exception as exc:
                status = "ERROR"; reason = repr(exc)
            fdb.execute("INSERT OR REPLACE INTO v52_processed(signature,processed_at,status,reason) VALUES(?,?,?,?)",
                        (row["signature"], time.time(), status, reason))
        state(fdb, "last_decode_batch", {"rows": len(rows), "decoded_swaps": decoded})
        fdb.commit()
    except BaseException:
        fdb.rollback(); raise
    finally:
        fdb.close()
    return len(rows), decoded


def hhi(values):
    total = sum(values)
    if total <= 0:
        return None
    return sum((v / total) ** 2 for v in values)


def snapshot_metrics(rows, first_ts, stage_s):
    cutoff = first_ts + stage_s
    rs = [r for r in rows if float(r["timestamp"]) <= cutoff]
    if not rs:
        return None
    buys = [r for r in rs if r["side"] == "BUY"]
    sells = [r for r in rs if r["side"] == "SELL"]
    sol_sizes = [abs(float(r["quote_sol"])) for r in rs if r["quote_sol"] is not None and float(r["quote_sol"]) > 0]
    buy_sol = sum(abs(float(r["quote_sol"])) for r in buys if r["quote_sol"] is not None)
    sell_sol = sum(abs(float(r["quote_sol"])) for r in sells if r["quote_sol"] is not None)
    gross = buy_sol + sell_sol
    wc = Counter(str(r["wallet"]) for r in rs)
    wallet_sol = defaultdict(float)
    for r in rs:
        if r["quote_sol"] is not None:
            wallet_sol[str(r["wallet"])] += abs(float(r["quote_sol"]))
    prices = [float(r["price_sol"]) for r in rs if r["price_sol"] is not None and float(r["price_sol"]) > 0]
    ret = 100.0 * (prices[-1] / prices[0] - 1.0) if len(prices) >= 2 and prices[0] > 0 else None
    rng = 100.0 * (max(prices) / min(prices) - 1.0) if prices and min(prices) > 0 else None

    mid = first_ts + stage_s / 2.0
    a = [r for r in rs if float(r["timestamp"]) <= mid]
    b = [r for r in rs if float(r["timestamp"]) > mid]
    def net(x):
        bb = sum(abs(float(r["quote_sol"])) for r in x if r["side"] == "BUY" and r["quote_sol"] is not None)
        ss = sum(abs(float(r["quote_sol"])) for r in x if r["side"] == "SELL" and r["quote_sol"] is not None)
        return bb - ss
    half = max(stage_s / 2.0, 1.0)
    vel_a, vel_b = net(a) / half, net(b) / half
    br_a = sum(r["side"] == "BUY" for r in a) / len(a) if a else None
    br_b = sum(r["side"] == "BUY" for r in b) / len(b) if b else None
    p_a = [float(r["price_sol"]) for r in a if r["price_sol"] is not None and float(r["price_sol"]) > 0]
    p_b = [float(r["price_sol"]) for r in b if r["price_sol"] is not None and float(r["price_sol"]) > 0]
    price_vel = None
    if p_a and p_b and p_a[-1] > 0:
        price_vel = 100.0 * (p_b[-1] / p_a[-1] - 1.0) / half

    vals = sorted(wallet_sol.values(), reverse=True)
    return {
        "token_mint": str(rs[0]["token_mint"]), "stage_s": stage_s, "cutoff_ts": cutoff,
        "swaps": len(rs), "buys": len(buys), "sells": len(sells),
        "buy_ratio": len(buys) / len(rs), "gross_sol": gross, "net_sol": buy_sol - sell_sol,
        "unique_wallets": len(wc), "repeat_wallet_ratio": sum(v > 1 for v in wc.values()) / len(wc) if wc else None,
        "wallet_hhi": hhi(vals), "wallet_top1_share": vals[0] / sum(vals) if vals and sum(vals) > 0 else None,
        "avg_trade_sol": sum(sol_sizes) / len(sol_sizes) if sol_sizes else None,
        "max_trade_sol": max(sol_sizes) if sol_sizes else None,
        "trade_hhi": hhi(sol_sizes), "top1_trade_share": max(sol_sizes) / sum(sol_sizes) if sol_sizes and sum(sol_sizes) > 0 else None,
        "return_pct": ret, "range_pct": rng,
        "flow_velocity": (buy_sol - sell_sol) / max(stage_s, 1),
        "flow_acceleration": vel_b - vel_a,
        "buy_ratio_delta": (br_b - br_a) if br_a is not None and br_b is not None else None,
        "price_velocity": price_vel,
    }


def build_features():
    db = open_feature()
    token_rows = db.execute("SELECT DISTINCT token_mint FROM v52_swaps").fetchall()
    built = outcomes = 0
    now = time.time()
    for tr in token_rows:
        mint = tr["token_mint"]
        rows = db.execute("SELECT * FROM v52_swaps WHERE token_mint=? ORDER BY timestamp", (mint,)).fetchall()
        if not rows:
            continue
        first_ts = float(rows[0]["timestamp"])
        for stage_s in SNAPSHOTS:
            if now < first_ts + stage_s:
                continue
            m = snapshot_metrics(rows, first_ts, stage_s)
            if not m:
                continue
            cols = ["token_mint","stage_s","cutoff_ts","swaps","buys","sells","buy_ratio","gross_sol","net_sol","unique_wallets","repeat_wallet_ratio","wallet_hhi","wallet_top1_share","avg_trade_sol","max_trade_sol","trade_hhi","top1_trade_share","return_pct","range_pct","flow_velocity","flow_acceleration","buy_ratio_delta","price_velocity"]
            vals = [m[c] for c in cols]
            db.execute(f"""INSERT INTO v52_snapshots({','.join(cols)},built_at) VALUES({','.join('?' for _ in cols)},?)
                           ON CONFLICT(token_mint,stage_s) DO UPDATE SET
                           cutoff_ts=excluded.cutoff_ts,built_at=excluded.built_at,swaps=excluded.swaps,buys=excluded.buys,sells=excluded.sells,buy_ratio=excluded.buy_ratio,
                           gross_sol=excluded.gross_sol,net_sol=excluded.net_sol,unique_wallets=excluded.unique_wallets,repeat_wallet_ratio=excluded.repeat_wallet_ratio,
                           wallet_hhi=excluded.wallet_hhi,wallet_top1_share=excluded.wallet_top1_share,avg_trade_sol=excluded.avg_trade_sol,max_trade_sol=excluded.max_trade_sol,
                           trade_hhi=excluded.trade_hhi,top1_trade_share=excluded.top1_trade_share,return_pct=excluded.return_pct,range_pct=excluded.range_pct,
                           flow_velocity=excluded.flow_velocity,flow_acceleration=excluded.flow_acceleration,buy_ratio_delta=excluded.buy_ratio_delta,price_velocity=excluded.price_velocity""",
                       vals + [now])
            built += 1

            cutoff = first_ts + stage_s
            base_prices = [float(r["price_sol"]) for r in rows if float(r["timestamp"]) <= cutoff and r["price_sol"] is not None and float(r["price_sol"]) > 0]
            base_price = base_prices[-1] if base_prices else None
            for horizon_s in HORIZONS:
                end = cutoff + horizon_s
                ready = int(now >= end)
                future = [r for r in rows if cutoff < float(r["timestamp"]) <= end and r["price_sol"] is not None and float(r["price_sol"]) > 0]
                frets = []
                if base_price and base_price > 0:
                    frets = [100.0 * (float(r["price_sol"]) / base_price - 1.0) for r in future]
                future_return = frets[-1] if frets else None
                fmax = max(frets) if frets else None
                fmin = min(frets) if frets else None
                mig = db.execute("SELECT 1 FROM v52_token_events WHERE token_mint=? AND event_type='MIGRATE' AND timestamp>? AND timestamp<=? LIMIT 1", (mint, cutoff, end)).fetchone() is not None
                db.execute("""INSERT INTO v52_outcomes(token_mint,stage_s,horizon_s,ready,future_return_pct,future_max_return_pct,future_min_return_pct,
                              future_hit10,future_hit20,future_hit50,future_death50,future_migration,updated_at)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                              ON CONFLICT(token_mint,stage_s,horizon_s) DO UPDATE SET ready=excluded.ready,future_return_pct=excluded.future_return_pct,
                              future_max_return_pct=excluded.future_max_return_pct,future_min_return_pct=excluded.future_min_return_pct,future_hit10=excluded.future_hit10,
                              future_hit20=excluded.future_hit20,future_hit50=excluded.future_hit50,future_death50=excluded.future_death50,future_migration=excluded.future_migration,updated_at=excluded.updated_at""",
                           (mint,stage_s,horizon_s,ready,future_return,fmax,fmin,
                            int(fmax is not None and fmax >= 10), int(fmax is not None and fmax >= 20), int(fmax is not None and fmax >= 50),
                            int(fmin is not None and fmin <= -50), int(mig), now))
                outcomes += 1
    state(db, "last_feature_build", {"snapshots_touched": built, "outcomes_touched": outcomes})
    db.commit(); db.close()
    return built, outcomes


def stats():
    db = open_feature()
    p = db.execute("SELECT COUNT(*) FROM v52_processed").fetchone()[0]
    s = db.execute("SELECT COUNT(*) FROM v52_swaps").fetchone()[0]
    t = db.execute("SELECT COUNT(DISTINCT token_mint) FROM v52_swaps").fetchone()[0]
    sn = db.execute("SELECT COUNT(*) FROM v52_snapshots").fetchone()[0]
    ready = db.execute("SELECT COUNT(*) FROM v52_outcomes WHERE ready=1").fetchone()[0]
    latest = db.execute("SELECT MAX(timestamp) FROM v52_swaps").fetchone()[0]
    statuses = {r["status"]: r["n"] for r in db.execute("SELECT status,COUNT(*) n FROM v52_processed GROUP BY status")}
    db.close()
    return p,s,t,sn,ready,latest,statuses


def display(last_decode, last_build):
    p,s,t,sn,ready,latest,statuses = stats()
    age = "—" if latest is None else f"{max(0,time.time()-latest):.1f}s"
    print("\033[2J\033[H", end="")
    print("="*118)
    print("MEMECOIN LAB — V5.2 SWAP DECODER + FEATURE FACTORY")
    print("="*118)
    print(f"PROCESSED={p:,} | DECODED SWAPS={s:,} | TOKENS={t:,} | SNAPSHOTS={sn:,} | READY OUTCOMES={ready:,} | LIVE AGE={age}")
    print(f"LAST DECODE rows/swaps={last_decode} | LAST BUILD snapshots/outcomes={last_build}")
    print("STATUS:", " ".join(f"{k}={v:,}" for k,v in sorted(statuses.items())))
    print("Research-only | balance-delta decoder v1 | raw RPC payload remains source of truth")


def main():
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    initialize()
    last_decode = (0,0); last_build=(0,0); last_feature_at=0.0
    while not STOP:
        last_decode = process_new_rows()
        if time.time() - last_feature_at >= 2.0:
            last_build = build_features(); last_feature_at=time.time()
        display(last_decode,last_build)
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
