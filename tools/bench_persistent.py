import time
import http.client
import json

def main():
    conn = http.client.HTTPConnection("127.0.0.1", 8734, timeout=5)
    headers = {"Content-Type": "application/json", "Connection": "keep-alive"}

    rtts = []
    for i in range(1, 21):
        payload = json.dumps({"action": "Write-Point", "id": "player_red", "x": i * 10, "y": i * 10})
        t0 = time.perf_counter()
        conn.request("POST", "/~node-process@1.0/primary/push/output~message@1.0/body", payload, headers)
        resp = conn.getresponse()
        data = resp.read()
        dt = (time.perf_counter() - t0) * 1000.0
        rtts.append(dt)
        parsed = json.loads(data)
        pt = parsed.get("point", {})
        print(f"Move #{i:2d} -> Tick #{pt.get('tick')} RTT={dt:.1f} ms")

    conn.close()
    warm = rtts[1:]
    print("\n================ Persistent Connection Summary ================")
    print(f"Cold Start : {rtts[0]:.1f} ms")
    print(f"Warm Avg   : {sum(warm)/len(warm):.1f} ms")
    print(f"Min RTT    : {min(warm):.1f} ms")
    print(f"Max RTT    : {max(warm):.1f} ms")
    print("==============================================================")

if __name__ == "__main__":
    main()
