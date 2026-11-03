#!/usr/bin/env python3
import os
import glob
import subprocess

PROJECT_ROOT = "/home/weslur/database"
DATA_DIR = os.path.join(PROJECT_ROOT, "engine", "data")
NODES = ["genesis", "primary", "sd1", "sd2", "arbit"]

def get_erl_ebin_args():
    ebins = glob.glob(f"{PROJECT_ROOT}/HyperBEAM/_build/default/lib/*/ebin")
    args = []
    for e in ebins:
        args.extend(["-pz", e])
    return args

def generate_wallets():
    print("[RSA Generator] Generating standard Arweave RSA wallets for all nodes...")
    ebin_args = get_erl_ebin_args()

    for node in NODES:
        node_dir = os.path.join(DATA_DIR, node)
        os.makedirs(node_dir, exist_ok=True)
        wallet_path = os.path.join(node_dir, "wallet.json")

        eval_cmd = (
            f"W = ar_wallet:new(), "
            f"JSON = ar_wallet:to_json(W), "
            f"ok = file:write_file(\"{wallet_path}\", JSON), "
            f"Addr = hb_util:human_id(ar_wallet:to_address(W)), "
            f"io:format(\"~s\", [Addr]), "
            f"init:stop()."
        )

        res = subprocess.run([
            "erl", "-noshell",
            *ebin_args,
            "-eval", eval_cmd
        ], capture_output=True, text=True)

        if res.returncode != 0:
            print(f"Error generating wallet for {node}: {res.stderr}")
            raise RuntimeError(f"Failed to generate RSA wallet for {node}")
        
        addr = res.stdout.strip()
        print(f"  - [{node}] Generated RSA Wallet -> Address: {addr} ({wallet_path})")

    print("[RSA Generator] All 5 node wallets successfully generated with RSA!")

if __name__ == "__main__":
    generate_wallets()
