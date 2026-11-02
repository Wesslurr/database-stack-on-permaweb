import sys
import time
import urllib.request
import json

headers = {"Content-Type": "application/json", "Accept": "application/json", "Cache-Control": "no-cache"}

# 1. Benchmark direct push to SD1
t0 = time.perf_counter()
payload_sd = json.dumps({"action": "Replicate", "id": "bench_test", "x": "10", "y": "20", "tick": 9999, "relayed_by_signer": "test", "assignment_id": "test"}).encode()
req = urllib.request.Request("http://127.0.0.1:8735/~node-process@1.0/sd1/push/output~message@1.0/last_tick", data=payload_sd, headers=headers)
with urllib.request.urlopen(req) as resp:
    data = resp.read()
t_sd1 = (time.perf_counter() - t0) * 1000

# 2. Benchmark direct push to Primary
t0 = time.perf_counter()
payload_p = json.dumps({"action": "Write-Point", "id": "bench_test", "x": 10.0, "y": 20.0}).encode()
req = urllib.request.Request("http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0/body", data=payload_p, headers=headers)
with urllib.request.urlopen(req) as resp:
    data = resp.read()
t_p = (time.perf_counter() - t0) * 1000

print(f"Direct push to SD1: {t_sd1:.2f} ms")
print(f"End-to-end push to Primary (with dual SD replication): {t_p:.2f} ms")
