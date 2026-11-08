import urllib.request
import json
import time
import concurrent.futures
import threading

BASE_URL = "http://127.0.0.1:8734/~node-process@1.0"

def worker_stream(worker_id, num_batches=15, batch_size=80):
    total_writes = 0
    rtts = []
    w_name = f"primary_{worker_id}"
    for b in range(num_batches):
        now_ts = int(time.time() * 1000)
        items = [f"bot_{worker_id}_{i}|{(b*10+i)%400}|{(b*10-i)%250}|{b*100+i+1}|{now_ts+i}|worker_{worker_id}" for i in range(batch_size)]
        packed = ";".join(items)
        payload = json.dumps({"action": "Write-Batch", "packed_inputs": packed, "ts": now_ts, "flush": True}).encode()
        req = urllib.request.Request(f"{BASE_URL}/{w_name}/push/output~message@1.0", data=payload, headers={"Content-Type": "application/json"})
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                rtt = (time.perf_counter() - t0) * 1000
                rtts.append(rtt)
                total_writes += batch_size
        except Exception as e:
            pass
    return total_writes, rtts

def test_streaming(num_workers, batch_size=80, batches_per_worker=10):
    print(f"\n---> Testing Streaming with {num_workers} Workers | Batch Size: {batch_size} ent/batch | Batches/Worker: {batches_per_worker}")
    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker_stream, w, batches_per_worker, batch_size) for w in range(1, num_workers + 1)]
        results = [f.result() for f in futures]
    total_time = time.perf_counter() - t0
    total_writes = sum(r[0] for r in results)
    all_rtts = [rtt for r in results for rtt in r[1]]
    wps = total_writes / total_time
    avg_rtt = sum(all_rtts) / len(all_rtts) if all_rtts else 0
    print(f"Result: {total_writes} writes in {total_time:.2f}s | Throughput: {wps:.1f} w/s | Avg RTT: {avg_rtt:.1f} ms")
    return wps, avg_rtt

if __name__ == "__main__":
    for nw in [1, 2, 4, 8, 10]:
        test_streaming(num_workers=nw, batch_size=60, batches_per_worker=10)
