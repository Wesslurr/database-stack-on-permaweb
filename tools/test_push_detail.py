import http.client
import json
import time

conn = http.client.HTTPConnection("127.0.0.1", 8734)
payload = json.dumps({"action": "Write-Point", "id": "test_p", "x": 1.0, "y": 2.0})
headers = {"Content-Type": "application/json", "Accept": "application/json"}

t0 = time.perf_counter()
conn.request("POST", "/~node-process@1.0/primary/push/output~message@1.0/body", payload, headers)
resp = conn.getresponse()
body = resp.read().decode()
dt = (time.perf_counter() - t0) * 1000

print(f"Status: {resp.status} in {dt:.2f} ms")
print(f"Body: {body}")
conn.close()
