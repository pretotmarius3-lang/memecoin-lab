#!/usr/bin/env python3
"""Memecoin Lab V6.5.1 — command center adapter for durable HOT telemetry.

Keeps the V6.5 UI/read-only cache, but makes its HOT panel prefer the current
v515_hot_lane state. Falls back to V5.1.4/V5.1.1 only when V5.1.5 telemetry is
absent. No scientific state mutation.
"""
from __future__ import annotations

import v65_visual_command_center as dash

_original_state_json=dash.state_json

def current_hot_state(d,key):
    if key=='v514_hot_lane':
        x=_original_state_json(d,'v515_hot_lane')
        if x:return x
        x=_original_state_json(d,'v514_hot_lane')
        if x:return x
        return _original_state_json(d,'v511_scheduler')
    return _original_state_json(d,key)

dash.state_json=current_hot_state

if __name__=='__main__':
    print(f'MEMECOIN LAB V6.5.1 COMMAND CENTER → http://{dash.HOST}:{dash.PORT}',flush=True)
    print('HOT telemetry priority: v515_hot_lane → v514_hot_lane → v511_scheduler | read-only',flush=True)
    dash.ThreadingHTTPServer((dash.HOST,dash.PORT),dash.H).serve_forever()
