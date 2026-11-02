#!/usr/bin/env python3
import time
import json
import urllib.request

req_data = json.dumps({"action": "Write-Point", "id": "bench_test", "x": 10.5, "y": 20.5}).encode("utf-8")
headers = {"Content-Type": "application/json", "Cache-Control": "no-store"}

latencies = []
for i in range(30):
    t0 = time.perf_counter()
    req = urllib.request.Request(
        "http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0/body",
        data=req_data,
        headers=headers
    )
    resp = urllib.request.urlopen(req, timeout=5).read()
    latencies.append((time.perf_counter() - t0) * 1000)

min_l = min(latencies)
max_l = max(latencies)
avg_l = sum(latencies) / len(latencies)
p95_l = sorted(latencies)[int(len(latencies) * 0.95)]
print(f"30 Writes Benchmark: Min={min_l:.1f}ms | Avg={avg_l:.1f}ms | p95={p95_l:.1f}ms | Max={max_l:.1f}ms")
