import time
import json
import urllib.request

def run():
    print("--- Testing Write-Point RTT Breakdown ---")
    times = []
    for i in range(1, 11):
        t0 = time.perf_counter()
        req = urllib.request.Request(
            "http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0/body",
            data=json.dumps({"action": "Write-Point", "id": "p_test", "x": i*5, "y": i*5}).encode(),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
        dt = (time.perf_counter() - t0) * 1000
        times.append(dt)
        parsed = json.loads(data)
        me_sealed = parsed.get("micro_epoch_sealed")
        tick = parsed.get("point", {}).get("tick")
        print(f"Move #{i:2d} (Tick #{tick}): RTT = {dt:.1f} ms | micro_epoch_sealed = {me_sealed}")

    warm = times[1:]
    print("-----------------------------------------")
    print(f"Average (warm): {sum(warm)/len(warm):.1f} ms")
    print(f"Min RTT       : {min(warm):.1f} ms")
    print(f"Max RTT       : {max(warm):.1f} ms")

if __name__ == "__main__":
    run()
