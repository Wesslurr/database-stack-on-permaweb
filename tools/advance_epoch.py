import urllib.request
import json
import sys

epoch = int(sys.argv[1]) if len(sys.argv) > 1 else None
payload = {"action": "Advance-Epoch", "admin_key": "hyperstack_admin_genesis_key"}
if epoch:
    payload["epoch"] = epoch

req = urllib.request.Request(
    'http://127.0.0.1:8733/~node-process@1.0/genesis/push/output~message@1.0',
    data=json.dumps(payload).encode('utf-8'),
    headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
)
resp = urllib.request.urlopen(req).read().decode('utf-8')
res = json.loads(resp)
print("Epoch from header:", res.get("epoch"))
inner = json.loads(res.get("body", "{}"))
print("Epoch from inner body:", inner.get("epoch"))

