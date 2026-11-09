import urllib.request
import json
import time

def test_relog():
    # 1. Player moves to (140, 80)
    print("[1] Moving player_red to (140, 80)...")
    write_req = urllib.request.Request(
        'http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0',
        data=json.dumps({
            'action': 'Write-Point',
            'id': 'player_red',
            'x': 140,
            'y': 80,
            'client_signer': 'client_cefbd9818ad459af',
            'flush': True
        }).encode('utf-8'),
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
    )
    with urllib.request.urlopen(write_req) as resp:
        raw = resp.read().decode('utf-8')
        w = json.loads(raw)
        if 'body' in w and isinstance(w['body'], str):
            w = json.loads(w['body'])
        print("  Move ACK received: tick =", w.get("tick") or w.get("head_tick"), "batch =", w.get("batch_id"))

    time.sleep(0.2)

    # 2. Simulate Client Relog (Page Refresh):
    print("\n[2] Simulating Page Refresh / Relog...")
    # Step A: Client discovers Primary from Genesis
    with urllib.request.urlopen('http://127.0.0.1:8733/~node-process@1.0/genesis/now/results~message@1.0/output/body') as resp:
        gen = json.loads(resp.read().decode('utf-8'))
        if 'body' in gen and isinstance(gen['body'], str):
            gen = json.loads(gen['body'])
        primary_url = gen.get('primary_url')
        print("  Discovered Primary URL from Genesis:", primary_url)

    # Step B: Client fetches existing position from Primary RAM via Read-Point
    read_req = urllib.request.Request(
        f'{primary_url}/~node-process@1.0/primary/push/output/body',
        data=json.dumps({'action': 'Read-Point', 'id': 'player_red'}).encode('utf-8'),
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
    )
    with urllib.request.urlopen(read_req) as resp:
        raw = resp.read().decode('utf-8')
        r = json.loads(raw)
        if 'body' in r and isinstance(r['body'], str):
            r = json.loads(r['body'])
        print("  Read-Point response:", r.get("status"))
        pt = r.get("point")
        print("  Restored Coordinates:", f"({pt['x']}, {pt['y']}) [Tick #{pt['tick']}]")
        assert pt['x'] == 140 and pt['y'] == 80, "Position mismatch on relog!"
        print("\n[SUCCESS] Relog correctly retrieved latest player coordinates without resetting to (0, 0)!")

if __name__ == '__main__':
    test_relog()
