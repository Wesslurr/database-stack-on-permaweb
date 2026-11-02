import http.client
import json
import time

conn = http.client.HTTPConnection("127.0.0.1", 8735)
headers = {"Content-Type": "application/json", "Accept": "application/json"}
payload = json.dumps({"action": "Replicate", "id": "bench_ka", "x": "10", "y": "20", "tick": 99999, "relayed_by_signer": "test", "assignment_id": "test"})

rtts = []
for i in range(10):
    t0 = time.perf_counter()
    conn.request("POST", "/~node-process@1.0/sd1/push/output~message@1.0/last_tick", payload, headers)
    resp = conn.getresponse()
    resp.read()
    rtt = (time.perf_counter() - t0) * 1000
    rtts.append(rtt)
conn.close()

print(f"Keep-Alive RTTs to SD1: min={min(rtts):.2f}ms, avg={sum(rtts)/len(rtts):.2f}ms, max={max(rtts):.2f}ms")
print(f"First (with TCP handshake): {rtts[0]:.2f}ms, Warm keep-alive (subsequent): {[round(x, 1) for x in rtts[1:]]}")
