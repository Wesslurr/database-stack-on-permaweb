import time
import json
import urllib.request

def profile():
    # 1. GET now output body
    times_get = []
    for _ in range(5):
        t0 = time.perf_counter()
        req = urllib.request.Request("http://127.0.0.1:8734/~node-process@1.0/primary/now/results~message@1.0/output/body")
        with urllib.request.urlopen(req) as r:
            _ = r.read()
        times_get.append((time.perf_counter() - t0) * 1000)
    print(f"GET /now/results/output/body: Avg = {sum(times_get[1:])/len(times_get[1:]):.1f} ms | Min = {min(times_get[1:]):.1f} ms")

    # 2. POST Read-Point
    times_read = []
    for _ in range(5):
        t0 = time.perf_counter()
        req = urllib.request.Request(
            "http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0/body",
            data=json.dumps({"action": "Read-Point", "id": "p_test"}).encode(),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as r:
            _ = r.read()
        times_read.append((time.perf_counter() - t0) * 1000)
    print(f"POST push Read-Point        : Avg = {sum(times_read[1:])/len(times_read[1:]):.1f} ms | Min = {min(times_read[1:]):.1f} ms")

    # 3. POST Write-Point
    times_write = []
    for i in range(5):
        t0 = time.perf_counter()
        req = urllib.request.Request(
            "http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0/body",
            data=json.dumps({"action": "Write-Point", "id": "p_test", "x": i, "y": i}).encode(),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as r:
            _ = r.read()
        times_write.append((time.perf_counter() - t0) * 1000)
    print(f"POST push Write-Point       : Avg = {sum(times_write[1:])/len(times_write[1:]):.1f} ms | Min = {min(times_write[1:]):.1f} ms")

if __name__ == "__main__":
    profile()
