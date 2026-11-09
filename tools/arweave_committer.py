#!/usr/bin/env python3
import json
import base64
import hashlib
import time
import os
import sys
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes

PORT = int(os.environ.get("COMMITTER_PORT", "8738"))
ARLOCAL_URL = os.environ.get("ARLOCAL_URL", "http://127.0.0.1:1984")
WALLET_PATH = os.environ.get("ARBIT_WALLET_PATH", "/home/weslur/database/engine/data/arbit/wallet.json")

def b64u_dec(s):
    if isinstance(s, bytes):
        s = s.decode('ascii')
    s += '=' * ((4 - len(s) % 4) % 4)
    return base64.urlsafe_b64decode(s)

def b64u_enc(b):
    if isinstance(b, str):
        b = b.encode('utf-8')
    return base64.urlsafe_b64encode(b).decode('ascii').rstrip('=')

def to_int(s):
    return int.from_bytes(b64u_dec(s), 'big')

# Load and construct RSA Private Key
def load_wallet():
    if not os.path.exists(WALLET_PATH):
        raise FileNotFoundError(f"Wallet not found at {WALLET_PATH}")
    with open(WALLET_PATH, "r", encoding="utf-8") as f:
        jwk = json.load(f)
    
    priv_numbers = rsa.RSAPrivateNumbers(
        p=to_int(jwk['p']),
        q=to_int(jwk['q']),
        d=to_int(jwk['d']),
        dmp1=to_int(jwk['dp']),
        dmq1=to_int(jwk['dq']),
        iqmp=to_int(jwk['qi']),
        public_numbers=rsa.RSAPublicNumbers(e=to_int(jwk['e']), n=to_int(jwk['n']))
    )
    priv_key = priv_numbers.private_key()
    owner = jwk['n']
    address = b64u_enc(hashlib.sha256(b64u_dec(owner)).digest())
    return priv_key, owner, address

PRIV_KEY, OWNER, WALLET_ADDRESS = load_wallet()

def ensure_wallet_funded():
    try:
        bal_raw = urllib.request.urlopen(f"{ARLOCAL_URL}/wallet/{WALLET_ADDRESS}/balance", timeout=3).read().decode()
        balance = int(bal_raw) if bal_raw.isdigit() else 0
        if balance < 100000000:
            urllib.request.urlopen(f"{ARLOCAL_URL}/mint/{WALLET_ADDRESS}/1000000000000", timeout=3)
    except Exception as e:
        print(f"[Committer] Warning funding wallet: {e}", file=sys.stderr)

def deep_hash(data):
    if isinstance(data, (bytes, bytearray)):
        tag = b'blob' + str(len(data)).encode('ascii')
        tag_hash = hashlib.sha384(tag).digest()
        blob_hash = hashlib.sha384(data).digest()
        return hashlib.sha384(tag_hash + blob_hash).digest()
    elif isinstance(data, list):
        tag = b'list' + str(len(data)).encode('ascii')
        acc = hashlib.sha384(tag).digest()
        for item in data:
            acc = hashlib.sha384(acc + deep_hash(item)).digest()
        return acc
    else:
        raise ValueError('Unsupported deep_hash data type: ' + str(type(data)))

def commit_checkpoint_to_arweave(payload):
    ensure_wallet_funded()
    
    # 1. Fetch latest tx_anchor from ArLocal
    anchor_req = urllib.request.urlopen(f"{ARLOCAL_URL}/tx_anchor", timeout=5)
    anchor_str = anchor_req.read().decode('ascii').strip()
    
    official_tick = str(payload.get("official_tick", 0))
    state_root = str(payload.get("state_root", ""))
    point_count = str(payload.get("point_count", 0))
    commitment_seq = str(payload.get("commitment_seq", 0))
    diff_seq = str(payload.get("diff_seq", 0))
    epoch = str(payload.get("epoch", 1))
    shard = str(payload.get("shard", "sd1_1"))
    now_ts = str(int(time.time()))
    
    # 2. Build structured tags
    tags = [
        (b'App-Name', b'HyperStack'),
        (b'Type', b'Checkpoint'),
        (b'Official-Tick', official_tick.encode('ascii')),
        (b'State-Root', state_root.encode('ascii')),
        (b'Point-Count', point_count.encode('ascii')),
        (b'Commitment-Seq', commitment_seq.encode('ascii')),
        (b'Diff-Seq', diff_seq.encode('ascii')),
        (b'Epoch', epoch.encode('ascii')),
        (b'Shard', shard.encode('ascii')),
        (b'Timestamp', now_ts.encode('ascii'))
    ]
    tags_deep = [[t[0], t[1]] for t in tags]
    
    # 3. Format 2 signature list
    sig_list = [
        b'2',
        b64u_dec(OWNER),
        b'', # target
        b'0', # quantity
        b'1000000', # reward
        b64u_dec(anchor_str),
        tags_deep,
        b'0', # data_size
        b''   # data_root
    ]
    sig_data = deep_hash(sig_list)
    signature = PRIV_KEY.sign(
        sig_data,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32),
        hashes.SHA256()
    )
    tx_id = b64u_enc(hashlib.sha256(signature).digest())
    
    # 4. Arweave Transaction Object
    tx = {
        'format': 2,
        'id': tx_id,
        'last_tx': anchor_str,
        'owner': OWNER,
        'tags': [{'name': b64u_enc(t[0]), 'value': b64u_enc(t[1])} for t in tags],
        'target': '',
        'quantity': '0',
        'data': '',
        'data_size': '0',
        'data_root': '',
        'reward': '1000000',
        'signature': b64u_enc(signature)
    }
    
    # 5. POST /tx to ArLocal
    req = urllib.request.Request(
        f"{ARLOCAL_URL}/tx",
        data=json.dumps(tx).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        if resp.status != 200:
            raise RuntimeError(f"POST /tx failed with status {resp.status}")
    
    # 6. Mine the block immediately on ArLocal
    mine_req = urllib.request.urlopen(f"{ARLOCAL_URL}/mine", timeout=5)
    mine_data = json.loads(mine_req.read().decode('utf-8'))
    height = mine_data.get("height", 0)
    block_hash = mine_data.get("current", "")
    
    return {
        "status": "finalized",
        "tx_id": tx_id,
        "height": height,
        "block_hash": block_hash,
        "official_tick": int(official_tick),
        "state_root": state_root,
        "commitment_seq": int(commitment_seq),
        "shard": shard,
        "owner": WALLET_ADDRESS
    }

class CommitterHTTPHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Quiet stdout logging

    def do_GET(self):
        if self.path in ("/health", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            resp = {
                "status": "online",
                "service": "HyperStack Arweave L1 Committer",
                "wallet": WALLET_ADDRESS,
                "arlocal_gateway": ARLOCAL_URL
            }
            self.wfile.write(json.dumps(resp).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/commit_checkpoint":
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            try:
                payload = json.loads(body)
                res = commit_checkpoint_to_arweave(payload)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(res).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                err = {"status": "error", "message": str(e)}
                self.wfile.write(json.dumps(err).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

def run_server():
    server = HTTPServer(("0.0.0.0", PORT), CommitterHTTPHandler)
    print(f"[Arweave Committer] Listening on http://0.0.0.0:{PORT} (Wallet: {WALLET_ADDRESS})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print(f"Testing Arweave L1 Committer (Address: {WALLET_ADDRESS})...")
        res = commit_checkpoint_to_arweave({
            "official_tick": 100,
            "point_count": 50,
            "state_root": "0x1234567890abcdef",
            "commitment_seq": 1,
            "diff_seq": 100,
            "epoch": 1,
            "shard": "sd1_1"
        })
        print("Success! Result:", json.dumps(res, indent=2))
    else:
        run_server()
