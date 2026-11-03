import urllib.request
import json

def check_status():
    req = urllib.request.Request(
        'http://127.0.0.1:8734/~node-process@1.0/primary/push/output/body',
        data=json.dumps({'action': 'Read-All'}).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode('utf-8'))
        pts = data.get('points', [])
        print("Total count:", len(pts))
        print("Point IDs:", [p.get('id') for p in pts])

if __name__ == '__main__':
    check_status()
