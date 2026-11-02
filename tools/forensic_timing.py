import time
import http.client
import json

# Profile direct requests to SD1 vs SD2 vs Primary
def time_request(host, port, path, payload):
    conn = http.client.HTTPConnection(host, port, timeout=5)
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    
    # Warm up connection
    conn.request("GET", "/~meta@1.0/info/address")
    conn.getresponse().read()
    
    timings = []
    for i in range(5):
        t0 = time.perf_counter()
        conn.request("POST", path, json.dumps(payload), headers)
        resp = conn.getresponse()
        resp.read()
        timings.append((time.perf_counter() - t0) * 1000)
    conn.close()
    return timings

print("1. Timing direct push to SD1 (Scheduler + Ed25519 Sign + Lua Lean ACK + LMDB Disk Write):")
sd1_times = time_request("127.0.0.1", 8735, "/~node-process@1.0/sd1/push/output~message@1.0/last_tick",
    {"action": "Replicate", "id": "prof_test", "x": 10, "y": 20, "tick": 1, "relayed_by_signer": "test", "assignment_id": "test"})
print(f"   SD1: {[round(t, 2) for t in sd1_times]} -> avg={sum(sd1_times)/len(sd1_times):.2f} ms")

print("2. Timing direct push to SD2 (Scheduler + Ed25519 Sign + Lua Lean ACK + LMDB Disk Write):")
sd2_times = time_request("127.0.0.1", 8736, "/~node-process@1.0/sd2/push/output~message@1.0/last_tick",
    {"action": "Replicate", "id": "prof_test", "x": 10, "y": 20, "tick": 1, "relayed_by_signer": "test", "assignment_id": "test"})
print(f"   SD2: {[round(t, 2) for t in sd2_times]} -> avg={sum(sd2_times)/len(sd2_times):.2f} ms")

print("3. Timing end-to-end push to Primary (Primary + SD1 + SD2):")
p_times = time_request("127.0.0.1", 8734, "/~node-process@1.0/primary/push/output~message@1.0/body",
    {"action": "Write-Point", "id": "prof_test", "x": 10, "y": 20})
print(f"   Primary: {[round(t, 2) for t in p_times]} -> avg={sum(p_times)/len(p_times):.2f} ms")

p_avg = sum(p_times)/len(p_times)
sd1_avg = sum(sd1_times)/len(sd1_times)
sd2_avg = sum(sd2_times)/len(sd2_times)
print(f"\n--- BREAKDOWN ---")
print(f"SD1 Replication alone      : {sd1_avg:.2f} ms")
print(f"SD2 Replication alone      : {sd2_avg:.2f} ms")
print(f"Combined SD1 + SD2 (Sum)   : {sd1_avg + sd2_avg:.2f} ms  ({(sd1_avg+sd2_avg)/p_avg*100:.1f}% of total latency!)")
print(f"Primary's own overhead     : {p_avg - (sd1_avg + sd2_avg):.2f} ms")
