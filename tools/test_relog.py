import sys
sys.path.insert(0, '/home/weslur/database')
from tools.client import StateEngineClient
import urllib.request
import json

c = StateEngineClient()
prim_url = c.discover_primary()

# 1. Write coordinate (120, 80) for player_red and await L1 quorum confirmation
res = c.write_point_confirmed("player_red", 120.0, 80.0)
print(f"Write player_red confirmed: tick={res.get('tick')} batch_id={res.get('batch_id')}")

# 2. Simulate client reload: query /now from Primary (same as fetchProcessBody)
req = urllib.request.Request(
    f"{prim_url}/~node-process@1.0/primary/now/results~message@1.0/output/body",
    headers={"Accept": "application/json", "Cache-Control": "no-cache"}
)
with urllib.request.urlopen(req) as resp:
    raw = resp.read().decode('utf-8')
    data = json.loads(raw)
    if isinstance(data, dict) and "body" in data and isinstance(data["body"], str):
        data = json.loads(data["body"])

pts = data.get("points", []) or data.get("head_points", [])
found = next((p for p in pts if (p[0] if isinstance(p, list) else p.get("id")) == "player_red"), None)
print(f"Reload queried /now -> Found player_red: {found}")
assert found is not None
px = found[1] if isinstance(found, list) else found["x"]
py = found[2] if isinstance(found, list) else found["y"]
assert px == 120.0 and py == 80.0
print("SUCCESS: Player position perfectly restored on relog without resetting to (0, 0)!")
