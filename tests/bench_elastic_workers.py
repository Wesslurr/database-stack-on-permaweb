import urllib.request
import json
import time
import concurrent.futures

BASE_URL = "http://127.0.0.1:8734/~node-process@1.0"

def send_batch(worker_id, items, seq_base):
    now_ts = int(time.time() * 1000)
    packed = ";".join([f"{it['id']}|{it['x']}|{it['y']}|{seq_base + idx}|{now_ts + idx}|worker_{worker_id}" for idx, it in enumerate(items)])
    payload = json.dumps({"action": "Write-Batch", "packed_inputs": packed, "ts": now_ts, "flush": True}).encode()
    req = urllib.request.Request(f"{BASE_URL}/{worker_id}/push/output~message@1.0", data=payload, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=5) as resp:
        rtt = (time.perf_counter() - t0) * 1000
        data = json.loads(resp.read().decode())
        return rtt, data

def run_benchmark(num_workers=4, num_bots=100, rounds=10, batch_size=20):
    print(f"\n=======================================================")
    print(f"BENCHMARK: {num_workers} Active Workers | {num_bots} Bots | {rounds} Rounds")
    print(f"=======================================================")
    
    total_writes = 0
    start_time = time.perf_counter()
    rtts = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers * 2) as executor:
        for r in range(rounds):
            futures = []
            for w in range(1, num_workers + 1):
                worker_id = f"primary_{w}"
                items = [{"id": f"bot_{w}_{i}", "x": (r * 10 + i) % 400, "y": (r * 10 - i) % 250} for i in range(batch_size)]
                futures.append(executor.submit(send_batch, worker_id, items, r * 100 + 1))
                total_writes += len(items)
            
            for f in concurrent.futures.as_completed(futures):
                rtt, data = f.result()
                rtts.append(rtt)

    total_time = time.perf_counter() - start_time
    wps = total_writes / total_time
    avg_rtt = sum(rtts) / len(rtts)
    p95_rtt = sorted(rtts)[int(len(rtts) * 0.95)]

    print(f"Total Writes ACKed : {total_writes} writes in {total_time:.2f} s")
    print(f"Achieved Throughput: {wps:.1f} writes/second (w/s)")
    print(f"Average L0 RTT     : {avg_rtt:.1f} ms")
    print(f"P95 L0 Latency     : {p95_rtt:.1f} ms")
    return wps, avg_rtt

if __name__ == "__main__":
    # Test 1 Worker baseline
    wps1, rtt1 = run_benchmark(num_workers=1, num_bots=20, rounds=6, batch_size=30)
    # Test 4 Workers
    wps4, rtt4 = run_benchmark(num_workers=4, num_bots=80, rounds=8, batch_size=25)
    # Test 10 Workers
    wps10, rtt10 = run_benchmark(num_workers=10, num_bots=100, rounds=12, batch_size=25)
    
    print("\n================== SCALING SUMMARY ==================")
    print(f" 1 Worker  : {wps1:.1f} w/s  (Avg RTT: {rtt1:.1f} ms)")
    print(f" 4 Workers : {wps4:.1f} w/s  (Avg RTT: {rtt4:.1f} ms)")
    print(f"10 Workers : {wps10:.1f} w/s  (Avg RTT: {rtt10:.1f} ms)")
    print("=====================================================")
