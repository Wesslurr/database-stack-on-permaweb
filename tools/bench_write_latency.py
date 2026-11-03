import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tools.client import StateEngineClient

def main():
    client = StateEngineClient()
    primary_url = client.discover_primary()
    print(f"Discovered Primary at {primary_url}")

    # Warmup
    client.write_point("warmup_pt", 0.0, 0.0)

    times = []
    for i in range(20):
        t0 = time.perf_counter()
        res = client.write_point(f"bench_pt_{i}", float(i), float(i))
        dt = (time.perf_counter() - t0) * 1000
        times.append(dt)
        print(f"Write {i:2d}: {dt:6.2f} ms | status: {res.get('status')} | sd_ack: {res.get('sd_ack')} | acked: {res.get('sd_acked_nodes')}")

    avg = sum(times) / len(times)
    p95 = sorted(times)[int(len(times) * 0.95)]
    print("-" * 65)
    print(f"Average Write Feedback Latency: {avg:.2f} ms")
    print(f"95th Percentile Latency       : {p95:.2f} ms")
    print("-" * 65)

if __name__ == "__main__":
    main()
