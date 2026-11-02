import sys
sys.path.insert(0, '/home/weslur/database')
from tools.client import StateEngineClient
import urllib.request
import json

c = StateEngineClient()
prim_url = c.discover_primary()

# 1. Write coordinate (120, 80) for player_red
res = c.write_point("player_red", 120.0, 80.0)
print(f"Write player_red: tick={res['assignment_slot']} pt={res['point']}")

# 2. Simulate client reload: query /now from Primary (same as fetchProcessBody)
req = urllib.request.Request(
    f"{prim_url}/~node-process@1.0/primary/now/results~message@1.0/output/body",
    headers={"Accept": "application/json", "Cache-Control": "no-cache"}
)
with urllib.request.urlopen(req) as resp:
    data = json.loads(resp.read().decode('utf-8'))
    if isinstance(data, dict) and data.get("ao-result") == "body":
        data = json.loads(data["body"])

pts = data.get("points", [])
found = next((p for p in pts if p.get("id") == "player_red"), None)
print(f"Reload queried /now -> Found player_red: {found}")
assert found is not None
assert found["x"] == 120.0 and found["y"] == 80.0
print("SUCCESS: Player position perfectly restored on relog without resetting to (0, 0)!")
