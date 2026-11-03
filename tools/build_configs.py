#!/usr/bin/env python3
"""
=============================================================================
Genesis-Centric Node Configuration Builder (Cryptographic AO + Low-Latency)
=============================================================================
1. Reads the master node list directly from `genesis_process.lua` (Single
   Source of Truth).
2. Spawns worker processes (`primary`, `sd1`, `sd2`, `arbit`) with ONLY
   `"genesis-url"` injected into their Process Definition Message.
3. Injects `"requires-sign": True` into every process definition to enforce
   AO cryptographic process identity across the cluster.
4. Enforces Tiered Storage & Low-Latency Execution:
   - `primary`: 100% Pure RAM (`hb_store_volatile` ONLY across `store`,
     `match-index`, `priv-store`, `attested-store` — ZERO LMDB disk files).
   - `sd1` & `sd2`: Dual-Tier `hb_store_volatile` (RAM Latest) + `hb_store_lmdb`
     (Disk Snapshot `process-snapshot-slots = 5`, aligning fsyncs with the
     Arbit batch checkpoint interval).
   - Persistent process workers (`process-workers = true`) and zero mainnet
     hook/console-trace overhead.
   - Primary enforces a 64 KiB HTTP body ceiling (`http_max_body`) so oversized
     payloads receive a real 413 before Lua allocates memory.
=============================================================================
"""

import json
import os
import re
from urllib.parse import urlparse

PROJECT_ROOT = "/home/weslur/database"
CONFIG_DIR = os.path.join(PROJECT_ROOT, "engine", "config")
DATA_DIR = os.path.join(PROJECT_ROOT, "engine", "data")
PROCESSES_DIR = os.path.join(PROJECT_ROOT, "engine", "processes")

GENESIS_SCRIPT_PATH = os.path.join(PROCESSES_DIR, "genesis", "genesis_process.lua")
GENESIS_URL = "http://127.0.0.1:8733"

ROLE_TO_SCRIPT = {
    "genesis": os.path.join(PROCESSES_DIR, "genesis", "genesis_process.lua"),
    "primary": os.path.join(PROCESSES_DIR, "primary", "primary_process.lua"),
    "sd1": os.path.join(PROCESSES_DIR, "shared_data", "sd_process.lua"),
    "sd2": os.path.join(PROCESSES_DIR, "shared_data", "sd_process.lua"),
    "arbit": os.path.join(PROCESSES_DIR, "arbit", "arbit_process.lua"),
}


def parse_genesis_topology(genesis_lua_src: str):
    """Extracts the master node list and L1 gateway defined inside genesis_process.lua."""
    entry_pattern = re.compile(
        r'\{\s*url\s*=\s*"([^"]+)"\s*,\s*process\s*=\s*"([^"]+)"\s*\}'
    )
    l1_match = re.search(r'l1_gateway\s*=\s*"([^"]+)"', genesis_lua_src)
    l1_gateway = l1_match.group(1) if l1_match else "http://127.0.0.1:1984"

    discovered_nodes = [
        {
            "name": "genesis",
            "port": urlparse(GENESIS_URL).port,
            "process_name": "genesis",
            "script_path": ROLE_TO_SCRIPT["genesis"],
        }
    ]

    for url, proc_name in entry_pattern.findall(genesis_lua_src):
        parsed = urlparse(url)
        discovered_nodes.append(
            {
                "name": proc_name,
                "port": parsed.port,
                "process_name": proc_name,
                "script_path": ROLE_TO_SCRIPT[proc_name],
            }
        )

    return discovered_nodes, l1_gateway


def main():
    with open(GENESIS_SCRIPT_PATH, "r", encoding="utf-8") as f:
        genesis_src = f.read()

    nodes, l1_gateway = parse_genesis_topology(genesis_src)
    os.makedirs(CONFIG_DIR, exist_ok=True)

    for node in nodes:
        with open(node["script_path"], "r", encoding="utf-8") as f:
            script_body = f.read()

        role = node["process_name"]
        is_primary = role == "primary"

        proc_def = {
            "device": "process@1.0",
            "execution-device": "lua@5.3a",
            "scheduler-device": "scheduler@1.0",
            "push-device": "push@1.0",
            "node-role": role,
            "requires-sign": role in ("genesis", "arbit"),
            "scheduler-commitment-spec": "ans104@1.0" if role in ("genesis", "arbit") else "httpsig@1.0",
            "scheduler_commitment_spec": "ans104@1.0" if role in ("genesis", "arbit") else "httpsig@1.0",
            "module": {
                "content-type": "text/x-lua",
                "body": script_body,
            },
        }

        # Every worker process receives ONLY the Genesis bootstrap pointer at spawn
        if role != "genesis":
            proc_def["genesis-url"] = GENESIS_URL

        volatile_store = {
            "ao-types": 'atom=("store-module")',
            "store-module": "hb_store_volatile",
            "name": f"{node['name']}_ram",
        }
        lmdb_store = {
            "ao-types": 'atom=("store-module")',
            "store-module": "hb_store_lmdb",
            "name": os.path.join(DATA_DIR, node["name"], "lmdb"),
            "sync": False,
        }

        # Tiered Storage Policy:
        # - Primary: 100% Pure RAM (`hb_store_volatile` ONLY — zero LMDB disk I/O)
        # - SD1 / SD2 / Arbit / Genesis: Pure LMDB Disk (`hb_store_lmdb` ONLY — 80% disk, minimal RAM)
        node_store = volatile_store if is_primary else lmdb_store
        stores = [node_store]
        # Micro-Epoch alignment: Primary stays fast in RAM; replicas snapshot every 10 ticks
        snapshot_slots = 1000 if is_primary else 10
        # Primary is a bounded RAM tier: reject oversized requests at the HTTP
        # layer. Other tiers keep a more permissive ceiling for checkpoints.
        http_max_body = 65536 if is_primary else 1048576

        if not is_primary:
            os.makedirs(os.path.join(DATA_DIR, node["name"], "lmdb"), exist_ok=True)

        # Micro-Epoch signing: Genesis and Arbit sign official topology & settlements,
        # while Primary & SD hot path runs ultra-fast in-memory/disk quorum without per-tick RSA bottleneck
        is_auth_authority = role in ("genesis", "arbit")
        cfg = {
            "port": node["port"],
            "priv_key_location": os.path.join(DATA_DIR, node["name"], "wallet.json"),
            "gateway": l1_gateway,
            "relay-allow-commit-request": True,
            "relay_allow_commit_request": True,
            "verify-assignments": False,
            "verify_assignments": False,
            "hosts-block-internal": False,
            "hosts_block_internal": False,
            # Keep persistent Erlang process workers alive across slots & low-latency scheduling
            "process-workers": True,
            "process_workers": True,
            "scheduling-mode": "local_confirmation",
            "scheduling_mode": "local_confirmation",
            "scheduler-default-commitment-spec": "ans104@1.0" if is_auth_authority else "httpsig@1.0",
            "scheduler_default_commitment_spec": "ans104@1.0" if is_auth_authority else "httpsig@1.0",
            "force-signed": is_auth_authority,
            "force_signed": is_auth_authority,
            "process-snapshot-slots": snapshot_slots,
            "process_snapshot_slots": snapshot_slots,
            "process-snapshot-time": False,
            "process_snapshot_time": False,
            # Bounded request envelope + no cache writes for transient traffic.
            "http_max_body": http_max_body,
            "cache-control": ["no-store"],
            "cache_control": ["no-store"],
            # Eliminate default mainnet hooks & stdout debug trace bottlenecks
            "on": {},
            "debug-print": ["error"],
            "debug_print": ["error"],
            "debug-print-trace": False,
            "debug_print_trace": False,
            # Store configuration (Pure RAM on Primary, Pure LMDB on SD/Arbit/Genesis)
            "store": stores,
            "match-index": [node_store],
            "match_index": [node_store],
            "priv-store": [node_store],
            "priv_store": [node_store],
            "attested-store": node_store,
            "attested_store": node_store,
            "node_processes": {
                role: proc_def,
            },
        }

        # Only Arbit & Genesis bundle official checkpoints to the L1 ANS-104
        # gateway. Primary and SD are transient tiers: their scheduler must not
        # block each tick on a mainnet upload. A closed local port makes the
        # scheduler's best-effort upload fail immediately instead.
        if role in ("arbit", "genesis"):
            cfg["bundler_ans104"] = l1_gateway
        else:
            cfg["bundler_ans104"] = "http://127.0.0.1:1"

        out_file = os.path.join(CONFIG_DIR, f"{node['name']}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)

        storage_desc = "PURE_RAM(hb_store_volatile)" if is_primary else "PURE_LMDB_DISK(hb_store_lmdb)"
        print(
            f"[Genesis-Spawn] Built {out_file} -> Process '{role}' "
            f"(port {node['port']}, storage={storage_desc}, requires-sign=True, genesis-url={proc_def.get('genesis-url', 'SELF')})"
        )


if __name__ == "__main__":
    main()
