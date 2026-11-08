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

def run_test(num_workers, batch_size=50, rounds=10):
    total_writes = 0
    start_time = time.perf_counter()
    rtts = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
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
    print(f"Workers: {num_workers:2d} | Batch: {batch_size:3d} | Total: {total_writes:4d} | Time: {total_time:.2f}s | WPS: {wps:7.1f} | Avg RTT: {avg_rtt:5.1f}ms")

for nw in [1, 2, 3, 4]:
    for bs in [30, 60, 100]:
        run_test(nw, batch_size=bs, rounds=6)
