#!/usr/bin/env python3
"""
Forge Dynamic Device Deployer (Zero-Restart SpecID Hot-Load)
=============================================================
Packages and verifies `engine/devices/l1_checkpoint/src` using HyperBEAM Forge,
publishes the signed ANS-104 `Device-Specification` and `Device-Implementation`
to live `hb-genesis`, registers `SpecID` inside `genesis_process.lua`, syncs
`arbit_process.lua`, and warms/verifies `hb-arbit` (`:8737`) hot-loads the
custom device over `/~<SpecID>/info` without restarting any node.
"""

import argparse
import http.client
import json
import os
import shutil
import subprocess
import sys
import urllib.parse

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HB_ROOT = os.path.join(PROJECT_ROOT, "HyperBEAM")
DEVICE_SRC = os.path.join(PROJECT_ROOT, "engine", "devices", "l1_checkpoint", "src")
DEVICE_PKG_DIR = os.path.join(PROJECT_ROOT, "engine", "devices", "l1_checkpoint", "_build", "device-packages")
GENESIS_WALLET = os.path.join(PROJECT_ROOT, "engine", "data", "genesis", "wallet.json")
PUBLISH_ESCRIPT = os.path.join(PROJECT_ROOT, "tools", "publish_forge_device.escript")


def http_req(host, port, method, path, body=None, headers=None, timeout=15.0):
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, raw
    finally:
        conn.close()


def run_step(cmd, cwd, label):
    res = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if res.returncode != 0:
        print(f"  [FAIL] {label} exited {res.returncode}")
        if res.stdout:
            print(res.stdout)
        if res.stderr:
            print(res.stderr)
        sys.exit(res.returncode)
    return res


def parse_target(url):
    p = urllib.parse.urlparse(url)
    return (p.hostname or "127.0.0.1", p.port or 80)


def main():
    parser = argparse.ArgumentParser(description="Deploy external Forge l1-checkpoint device to hb-genesis and hot-load on hb-arbit.")
    parser.add_argument("--skip-verify", action="store_true", help="Skip rebar3 package/verify CLI (used when already verified before cluster boot so live HyperBEAM/_build is untouched).")
    args = parser.parse_args()

    genesis_url = os.environ.get("GENESIS_URL", "http://127.0.0.1:8733")
    arbit_url = os.environ.get("ARBIT_URL", "http://127.0.0.1:8737")
    g_host, g_port = parse_target(genesis_url)
    a_host, a_port = parse_target(arbit_url)

    rebar3_bin = shutil.which("rebar3") or os.path.expanduser("~/.local/bin/rebar3")
    escript_bin = shutil.which("escript") or "escript"

    print("=== [Forge] 1/5 Preparing external l1-checkpoint device ===")
    rel_src = os.path.relpath(DEVICE_SRC, HB_ROOT)
    rel_pkg = os.path.relpath(DEVICE_PKG_DIR, HB_ROOT)
    if not args.skip_verify:
        run_step(
            [rebar3_bin, "device", "package", "--device-src", rel_src, "--output-dir", rel_pkg],
            HB_ROOT,
            "rebar3 device package",
        )
        run_step(
            [rebar3_bin, "device", "verify", "--device-src", rel_src, "--output-dir", rel_pkg],
            HB_ROOT,
            "rebar3 device verify",
        )
        print("  [OK] External device archive packaged and verified cleanly.")
    else:
        print("  [OK] Using pre-verified external device source (packaging in-memory via hb_packager).")

    print("=== [Forge] 2/5 Publishing signed Spec & Impl to live hb-genesis ===")
    pub_res = run_step(
        [escript_bin, PUBLISH_ESCRIPT, DEVICE_SRC, GENESIS_WALLET, genesis_url, HB_ROOT],
        PROJECT_ROOT,
        "publish_forge_device.escript",
    )
    pub_lines = [ln.strip() for ln in pub_res.stdout.splitlines() if ln.strip()]
    pub_json = json.loads(pub_lines[-1])
    spec_id = pub_json["spec_id"]
    impl_id = pub_json["impl_id"]
    signer = pub_json["signer"]
    print(f"  [OK] Published SpecID={spec_id}")
    print(f"       ImplID={impl_id} Signer={signer}")

    print("=== [Forge] 3/5 Verifying GraphQL discovery & archive body on hb-genesis ===")
    status_body, raw_zip = http_req(g_host, g_port, "GET", f"/{impl_id}/body")
    status_raw, raw_ans = http_req(
        g_host, g_port, "GET", f"/~local-name@1.0/lookup?key=arweave-raw-{impl_id}"
    )
    if status_body != 200 or not raw_zip.startswith(b"PK") or status_raw != 200 or len(raw_ans) == 0:
        print(
            f"  [FAIL] GET /{impl_id}/body -> status={status_body}, len={len(raw_zip)}; "
            f"GET /~local-name@1.0/lookup?key=arweave-raw-{impl_id} -> status={status_raw}, len={len(raw_ans)}"
        )
        sys.exit(1)
    gql_payload = json.dumps({
        "query": (
            "query($owners: [String!]!, $spec: [String!]!) { "
            "transactions(owners: $owners, tags: [{ name: \"implements-device\", values: $spec }]) { "
            "edges { node { id bundledIn { id } fee { winston } } } } }"
        ),
        "variables": {"owners": [signer], "spec": [spec_id]},
    })
    status_gql, raw_gql = http_req(
        g_host, g_port, "POST", "/~query@1.0/graphql",
        body=gql_payload,
        headers={"Content-Type": "application/json"},
    )
    gql_data = json.loads(raw_gql.decode("utf-8", errors="replace")) if status_gql == 200 else {}
    edges = gql_data.get("data", {}).get("transactions", {}).get("edges", [])
    found_ids = [e.get("node", {}).get("id") for e in edges]
    if status_gql != 200 or impl_id not in found_ids:
        print(f"  [FAIL] GraphQL query on hb-genesis failed: status={status_gql}, body={raw_gql[:400]}")
        sys.exit(1)
    print(f"  [OK] hb-genesis serving ZIP ({len(raw_zip)} bytes) + GraphQL match.")

    print("=== [Forge] 4/5 Registering SpecID on Genesis & syncing Arbit ===")
    reg_payload = json.dumps({
        "action": "Register-Forge-Device",
        "admin_key": "hyperstack_admin_genesis_key",
        "spec_id": spec_id,
    })
    status_reg, raw_reg = http_req(
        g_host, g_port, "POST", "/~node-process@1.0/genesis/push/output~message@1.0",
        body=reg_payload,
        headers={"Content-Type": "application/json"},
    )
    reg_parsed = json.loads(raw_reg.decode("utf-8", errors="replace")) if status_reg == 200 else {}
    reg_body = reg_parsed.get("body")
    reg_json = json.loads(reg_body) if isinstance(reg_body, str) and reg_body.startswith("{") else reg_parsed
    if status_reg != 200 or reg_json.get("l1_checkpoint_spec_id") != spec_id:
        print(f"  [FAIL] Genesis Register-Forge-Device failed: status={status_reg}, body={raw_reg.decode('utf-8', errors='replace')}")
        sys.exit(1)

    sync_payload = json.dumps({"action": "Sync-Topology"})
    status_sync, raw_sync = http_req(
        a_host, a_port, "POST", "/~node-process@1.0/arbit/push/output~message@1.0",
        body=sync_payload,
        headers={"Content-Type": "application/json"},
    )
    if status_sync != 200:
        print(f"  [FAIL] Arbit Sync-Topology failed: status={status_sync}, body={raw_sync[:400]}")
        sys.exit(1)
    print("  [OK] Genesis SpecID registered & Arbit topology synced.")

    print("=== [Forge] 5/5 Hot-loading device on live hb-arbit (no restart) ===")
    status_info, raw_info = http_req(
        a_host,
        a_port,
        "GET",
        f"/~{spec_id}/info",
        headers={"Accept": "application/json"},
        timeout=30.0,
    )
    if status_info != 200:
        print(f"  [FAIL] GET /~{spec_id}/info -> status={status_info}, body={raw_info[:400]}")
        sys.exit(1)
    info_text = raw_info.decode("utf-8", errors="replace")
    try:
        info_data = json.loads(info_text)
    except Exception:
        info_data = {}
    if info_data.get("device") != "l1-checkpoint@1.0":
        print(f"  [FAIL] info missing device='l1-checkpoint@1.0': {info_text[:300]}")
        sys.exit(1)
    print(f"  [OK] hb-arbit dynamically hot-loaded /~{spec_id} -> device={info_data.get('device')}")


if __name__ == "__main__":
    main()
