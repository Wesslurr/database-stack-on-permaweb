import urllib.request
import json
import time

def ping_primary():
    t0 = time.perf_counter()
    p = {
        "action": "Write-Point",
        "id": "player_red",
        "x": 200,
        "y": 100,
        "client_signer": "player_red_signer",
        "target_tick": 700,
        "ts": int(time.time() * 1000),
        "flush": True
    }
    req = urllib.request.Request(
        "http://127.0.0.1:8734/~node-process@1.0/primary_1/push/output~message@1.0",
        data=json.dumps(p).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as resp:
        rtt = (time.perf_counter() - t0) * 1000
        body = json.loads(resp.read().decode())
        print(f"RTT: {rtt:.2f} ms | Status: {resp.status} | Body Status: {body.get('status')} | Batch: {body.get('batch_id')}")

if __name__ == "__main__":
    for i in range(5):
        ping_primary()
        time.sleep(0.1)
