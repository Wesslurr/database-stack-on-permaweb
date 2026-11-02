import urllib.request
import json

def check_status():
    req = urllib.request.Request(
        'http://127.0.0.1:8734/~node-process@1.0/primary/push/output~message@1.0/body',
        data=json.dumps({'action': 'Read-Point', 'id': 'player_red'}).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req) as resp:
        print("Status code:", resp.status)
        print("Headers:", dict(resp.headers))
        print("Body:", resp.read().decode('utf-8'))

if __name__ == '__main__':
    check_status()
