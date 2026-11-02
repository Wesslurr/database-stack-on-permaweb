#!/usr/bin/env python3
import time
import urllib.request
import json

req_data = json.dumps({'action': 'Replicate', 'id': 'direct_test', 'x': 1, 'y': 2, 'tick': 999}).encode('utf-8')
for i in range(5):
    t0 = time.perf_counter()
    req = urllib.request.Request(
        'http://127.0.0.1:8735/~node-process@1.0/sd1/push/output~message@1.0/last_tick',
        data=req_data,
        headers={'Content-Type': 'application/json', 'Cache-Control': 'no-store'}
    )
    r = urllib.request.urlopen(req).read()
    print(f'SD1 push #{i+1} took {(time.perf_counter() - t0)*1000:.1f}ms: {r.decode()[:40]}')
