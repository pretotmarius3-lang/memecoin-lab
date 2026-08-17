#!/usr/bin/env python3
"""Memecoin Lab V5.1.5.1 — low-usage durable HOT lane.

Thin runtime patch over v515_durable_hot_lane.py.

Changes only acquisition transport / audit plumbing:
- ONE global discovery subscription: Pump program only (CREATE discovery).
- HOT mint subscriptions remain unchanged and follow admitted tokens for TTL.
- Durable SQLite queue/registry, HTTP workers, backpressure and acquisition
  epochs are inherited from V5.1.5.
- Dynamic admission_mod is recovered into claimed CREATE work so the admitted
  token records the actual 1/N gate used under load.
- Helius "usage cap exceeded / max usage reached" enters a long cooldown rather
  than reconnect-hammering the provider.

No feature, threshold, V6.4 freeze or scientific decision rule is modified.
Research only; never signs/submits transactions.
"""
from __future__ import annotations

import asyncio, json, os, time
import websockets

import v515_durable_hot_lane as v

USAGE_CAP_BACKOFF_S=float(os.environ.get('MEMECOIN_V5151_USAGE_CAP_BACKOFF_S','300'))
NORMAL_MAX_BACKOFF_S=float(os.environ.get('MEMECOIN_V5151_MAX_RECONNECT_S','30'))

# Preserve the exact durable claim implementation but recover the dynamic
# admission modulus written by V5.1.5 into CREATE queue audit metadata.
_original_claim_one=v.claim_one

def claim_one_with_admission_mod():
    item=_original_claim_one()
    if item and item.get('kind')=='CREATE':
        marker=str(item.get('last_error') or '')
        if marker.startswith('admission_mod='):
            try:item['admission_mod']=int(marker.split('=',1)[1])
            except Exception:item['admission_mod']=v.BASE_SAMPLE_MOD
    return item

v.claim_one=claim_one_with_admission_mod


def usage_cap_error(exc:BaseException)->bool:
    s=repr(exc).lower()
    return ('usage cap exceeded' in s or 'max usage reached' in s or
            ('status_code=429' in s and '-32429' in s))


async def low_usage_websocket_loop(counters,wake_subscribe):
    url=f'{v.WS_BASE}?api-key={v.quote(v.KEY)}'
    backoff=1.0
    while not v.STOP.is_set():
        try:
            async with websockets.connect(
                url,ping_interval=20,ping_timeout=30,close_timeout=10,
                max_size=None,max_queue=32768
            ) as ws:
                counters['reconnects']+=1
                request_map={}; sub_map={}; sid_by_mint={}

                # LOW-USAGE CHANGE: Pump only for CREATE discovery.
                v.REQ_ID+=1
                request_map[v.REQ_ID]={'type':'GLOBAL','source':'PUMP'}
                await ws.send(json.dumps({
                    'jsonrpc':'2.0','id':v.REQ_ID,'method':'logsSubscribe',
                    'params':[{'mentions':[v.base.PUMP_PROGRAM]},
                              {'commitment':v.COMMITMENT}]
                },separators=(',',':')))

                async def subscribe_active():
                    active=await asyncio.to_thread(v.active_hot)
                    existing=set(sid_by_mint)
                    for h in active:
                        mint=h['mint']
                        if mint in existing:continue
                        if len(sid_by_mint)>=v.MAX_HOT:break
                        v.REQ_ID+=1; rid=v.REQ_ID
                        request_map[rid]={'type':'HOT','mint':mint}
                        await ws.send(json.dumps({
                            'jsonrpc':'2.0','id':rid,'method':'logsSubscribe',
                            'params':[{'mentions':[mint]},
                                      {'commitment':v.COMMITMENT}]
                        },separators=(',',':')))

                await subscribe_active(); wake_subscribe.clear()
                restored=len(await asyncio.to_thread(v.active_hot))
                print(
                    f'V5.1.5.1 connected | epoch={v.EPOCH_ID} | '
                    f'global=PUMP_ONLY | durable HOT restore={restored} | '
                    f'rps={v.CURRENT_RPS:.2f}',flush=True
                )
                backoff=1.0; last_maint=time.monotonic()

                while not v.STOP.is_set():
                    try:
                        raw=await asyncio.wait_for(ws.recv(),timeout=.5)
                    except asyncio.TimeoutError:
                        if wake_subscribe.is_set():
                            await subscribe_active(); wake_subscribe.clear()
                        if time.monotonic()-last_maint>=2:
                            await asyncio.to_thread(v.expire_hot)
                            last_maint=time.monotonic()
                            active_mints={x['mint'] for x in await asyncio.to_thread(v.active_hot)}
                            for mint,sid in list(sid_by_mint.items()):
                                if mint not in active_mints:
                                    v.REQ_ID+=1; rid=v.REQ_ID
                                    request_map[rid]={'type':'UNSUB','mint':mint,'sid':sid}
                                    await ws.send(json.dumps({
                                        'jsonrpc':'2.0','id':rid,
                                        'method':'logsUnsubscribe','params':[sid]
                                    },separators=(',',':')))
                        continue

                    msg=json.loads(raw)
                    if 'id' in msg:
                        info=request_map.pop(msg.get('id'),None)
                        if not info:continue
                        if msg.get('error'):
                            counters['subscribe_errors']+=1
                            err=str(msg['error']).lower()
                            if 'max usage' in err or 'usage cap' in err:
                                raise RuntimeError(f"Helius usage cap: {msg['error']}")
                            continue
                        if info['type']=='GLOBAL':
                            sub_map[int(msg['result'])]={'type':'GLOBAL','source':'PUMP'}
                        elif info['type']=='HOT':
                            sid=int(msg['result']); mint=info['mint']
                            sid_by_mint[mint]=sid
                            sub_map[sid]={'type':'HOT','mint':mint}
                            counters['hot_subscribed']+=1
                            c=v.db(); now=time.time()
                            c.execute('UPDATE v515_hot_tokens SET last_subscribed_at=?,updated_at=? WHERE mint=?',(now,now,mint)); c.commit(); c.close()
                        elif info['type']=='UNSUB':
                            mint=info['mint']; sid=info['sid']
                            sid_by_mint.pop(mint,None); sub_map.pop(sid,None)
                            counters['hot_unsubscribed']+=1
                        continue

                    if msg.get('method')!='logsNotification':continue
                    p=msg.get('params') or {}; result=p.get('result') or {}
                    value=result.get('value') or {}; sid=p.get('subscription')
                    sig=value.get('signature'); logs=value.get('logs') or []
                    slot=(result.get('context') or {}).get('slot')
                    if not sig or value.get('err') is not None or sid is None:continue
                    route=sub_map.get(int(sid))
                    if not route:continue
                    event=v.base.infer_event_hint(logs)

                    if route['type']=='GLOBAL':
                        counters['global_logs']+=1
                        if event=='CREATE':
                            counters['creates_seen']+=1
                            mod=await asyncio.to_thread(v.admission_mod)
                            if mod is None:
                                counters['admission_paused']+=1; continue
                            if v.sample_hash(sig,mod):
                                if v.enqueue(sig,None,'CREATE','PUMP',slot,logs):
                                    c=v.db()
                                    c.execute('UPDATE v515_hot_queue SET last_error=? WHERE signature=?',(f'admission_mod={mod}',sig)); c.commit(); c.close()
                                    counters['creates_admitted']+=1
                    else:
                        counters['hot_logs']+=1
                        if event in ('BUY','SELL'):
                            if v.enqueue(sig,route['mint'],'HOT','PUMP',slot,logs):
                                counters['hot_enqueued']+=1

                    total=counters['global_logs']+counters['hot_logs']
                    if total and total%5000==0:
                        q=await asyncio.to_thread(v.pending_count)
                        debt=q/max(.2,v.CURRENT_RPS)
                        active=len(await asyncio.to_thread(v.active_hot))
                        print(
                            f"global={counters['global_logs']:,} creates={counters['creates_seen']:,} "
                            f"admitted={counters['creates_admitted']:,} hot_active={active} "
                            f"hot_logs={counters['hot_logs']:,} hot_swaps={counters['hot_swaps']:,} "
                            f"fetched={counters['fetched']:,} q={q:,} debt={debt:.0f}s "
                            f"rps={v.CURRENT_RPS:.2f} 429={counters['429']:,} "
                            f"paused={counters['admission_paused']:,}",flush=True
                        )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if usage_cap_error(e):
                wait=USAGE_CAP_BACKOFF_S
                print(f'V5.1.5.1 Helius usage cap reached | cooldown {wait:.0f}s | durable queue preserved',flush=True)
            else:
                wait=backoff
                print(f'V5.1.5.1 WebSocket error: {e!r} | reconnect in {wait:.0f}s',flush=True)
                backoff=min(NORMAL_MAX_BACKOFF_S,backoff*2)
            try:await asyncio.wait_for(v.STOP.wait(),timeout=wait)
            except asyncio.TimeoutError:pass


# v.main_async resolves this module-global function at runtime, so swapping it
# retains all V5.1.5 durable queue workers/controller/telemetry unchanged.
v.websocket_loop=low_usage_websocket_loop

if __name__=='__main__':
    print(
        f'V5.1.5.1 LOW-USAGE DURABLE HOT | epoch={v.EPOCH_ID} | '
        f'global=PUMP_ONLY | sample base=1/{v.BASE_SAMPLE_MOD} | '
        f'ttl={v.HOT_TTL_S:.0f}s | rps={v.BASE_RPS:.1f}-{v.MAX_RPS:.1f}',
        flush=True
    )
    try:asyncio.run(v.main_async())
    finally:v.shutdown_epoch()
