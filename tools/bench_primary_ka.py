import http.client
import json
import time

conn = http.client.HTTPConnection("127.0.0.1", 8734)
headers = {"Content-Type": "application/json", "Accept": "application/json", "Cache-Control": "no-cache"}

rtts = []
for i in range(10):
    payload = json.dumps({"action": "Write-Point", "id": f"bench_ka_{i}", "x": float(i), "y": float(i*2)})
    t0 = time.perf_counter()
    conn.request("POST", "/~node-process@1.0/primary/push/output~message@1.0/body", payload, headers)
    resp = conn.getresponse()
    body = resp.read()
    rtt = (time.perf_counter() - t0) * 1000
    rtts.append(rtt)
conn.close()

print(f"Keep-Alive RTTs to Primary: min={min(rtts):.2f}ms, avg={sum(rtts)/len(rtts):.2f}ms, max={max(rtts):.2f}ms")
print(f"Values: {[round(x, 1) for x in rtts]}")
