import urllib.request
import json

def check(url, name):
    try:
        req = urllib.request.urlopen(f"{url}/~node-process@1.0/{name}/now/results~message@1.0/output/body")
        data = json.loads(req.read().decode("utf-8"))
        print(f"[{name.upper()} @ {url}]")
        print(f"  Last Tick      : {data.get('last_tick') or data.get('official_tick') or data.get('tick')}")
        print(f"  Points Count   : {data.get('count') or data.get('total')}")
        print(f"  Signer         : {data.get('sd_signer') or data.get('primary_signer') or data.get('arbit_signer')}")
        print(f"  Relayed Signer : {data.get('relayed_by_signer')}")
    except Exception as e:
        print(f"[{name.upper()} @ {url}] ERROR: {e}")

def main():
    print("=== HYPERBEAM CLUSTER LIVE STATUS ===")
    check("http://127.0.0.1:8734", "primary")
    check("http://127.0.0.1:8735", "sd1")
    check("http://127.0.0.1:8736", "sd2")
    check("http://127.0.0.1:8737", "arbit")

if __name__ == "__main__":
    main()
