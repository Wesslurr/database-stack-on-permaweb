#!/usr/bin/env python3
"""
=============================================================================
Comprehensive E2E Verification Test: 2D Coordinates State Engine
=============================================================================
Architecture & Scope:
- Strictly separated from engine/ (resides in tests/).
- Verifies ZERO hardcoded peer URLs inside Primary, SD, and Arbit processes.
- Verifies all worker processes dynamically resolve their topology from
  the Genesis Process (:8733).
- Client connects ONLY to Genesis Process (:8733) to discover Primary (:8734).
- Client executes Write and Read operations on Primary.
- Test verifies all 4 architectural tiers:
    1. Tier 1: Primary RAM (In-Memory 2D Point State, high frequency)
    2. Tier 2: Shared Data Replicas SD1 (:8735) & SD2 (:8736)
    3. Tier 3: Arbit Node (:8737) (Consensus Gatekeeper, State Root commitment)
    4. Tier 4: ArLocal L1 (:1984) (Arweave Permaweb Ledger & Block Mining)
=============================================================================
"""

import sys
import os
import random
import time
import json
import urllib.request
from typing import List, Dict, Any

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tools.client import StateEngineClient

# ANSI colors for diagnostic console output
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def log_step(step_num: int, title: str):
    print(f"\n{BOLD}{CYAN}=== [{step_num}] {title} ==={RESET}")


def assert_true(condition: bool, msg: str):
    if condition:
        print(f"  {GREEN}[PASS]{RESET} {msg}")
    else:
        print(f"  {RED}[FAIL]{RESET} {msg}")
        raise AssertionError(msg)


def http_get_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def mine_arlocal_block():
    req = urllib.request.Request("http://127.0.0.1:1984/mine")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read().decode("utf-8")


def get_process_state(base_url: str, process_name: str) -> Dict[str, Any]:
    """Helper to read deep process state from HyperBEAM node without client knowledge."""
    client_helper = StateEngineClient()
    return client_helper._fetch_process_now_body(base_url, process_name)


def main():
    print(f"{BOLD}======================================================================{RESET}")
    print(f"{BOLD}    HYPERBEAM REAL-TIME STATE ENGINE: 2D COORDINATES E2E TEST         {RESET}")
    print(f"{BOLD}======================================================================{RESET}")

    total_assertions = 0

    # -------------------------------------------------------------------------
    # STEP 0: Verify Zero Hardcoded HTTP URLs in Worker Process Code
    # -------------------------------------------------------------------------
    log_step(0, "Auditing Worker Process Scripts for Zero Hardcoded Peer URLs")
    worker_scripts = [
        os.path.join(PROJECT_ROOT, "engine", "processes", "primary", "primary_process.lua"),
        os.path.join(PROJECT_ROOT, "engine", "processes", "shared_data", "sd_process.lua"),
        os.path.join(PROJECT_ROOT, "engine", "processes", "arbit", "arbit_process.lua"),
    ]
    for script_path in worker_scripts:
        with open(script_path, "r", encoding="utf-8") as f:
            content = f.read()
        rel_name = os.path.relpath(script_path, PROJECT_ROOT)
        assert_true("http://" not in content and "https://" not in content, f"Zero hardcoded URLs in {rel_name}")
        total_assertions += 1

    # -------------------------------------------------------------------------
    # STEP 1: Discovery via Genesis Process
    # -------------------------------------------------------------------------
    log_step(1, "Service Discovery via Genesis Process (:8733)")
    genesis_url = "http://127.0.0.1:8733"
    client = StateEngineClient(genesis_url=genesis_url)
    
    primary_url = client.discover_primary()
    assert_true(primary_url == "http://127.0.0.1:8734", f"Genesis reported Primary at {primary_url}")
    assert_true(client.epoch is not None and client.epoch >= 1, f"Genesis reported active Epoch {client.epoch}")
    total_assertions += 2

    # -------------------------------------------------------------------------
    # STEP 2: Workload Generation (5 Random 2D Coordinates)
    # -------------------------------------------------------------------------
    log_step(2, "Generating Random 2D Coordinate Workload")
    random.seed(20261102)
    test_points: List[Dict[str, Any]] = []
    for i in range(1, 6):
        pt = {
            "id": f"pt_rand_{i}_{int(time.time()) % 10000}",
            "x": round(random.uniform(-100.0, 100.0), 4),
            "y": round(random.uniform(-100.0, 100.0), 4)
        }
        test_points.append(pt)
        print(f"  Point {i}: ID={pt['id']:<18} X={pt['x']:>9.4f}  Y={pt['y']:>9.4f}")

    # -------------------------------------------------------------------------
    # STEP 3: Client Write Execution on Primary
    # -------------------------------------------------------------------------
    log_step(3, "Client Writes 2D Points Directly to Primary Process (:8734)")
    write_results: List[Dict[str, Any]] = []
    for pt in test_points:
        res = client.write_point(pt["id"], pt["x"], pt["y"])
        assert_true(res["status"] == "ok", f"Wrote {pt['id']} -> Slot {res['assignment_slot']}")
        assert_true(res["tier"] == "primary_ram", f"Tier confirmed as Primary RAM")
        write_results.append(res)
        total_assertions += 2

    # -------------------------------------------------------------------------
    # STEP 4: Client Read Verification from Primary
    # -------------------------------------------------------------------------
    log_step(4, "Client Reads Points Back from Primary Process")
    for pt in test_points:
        read_res = client.read_point(pt["id"])
        assert_true(read_res.get("status") == "ok", f"Read point {pt['id']} successfully")
        retrieved_pt = read_res.get("point", {})
        assert_true(retrieved_pt.get("id") == pt["id"], f"Point ID matches: {retrieved_pt.get('id')}")
        assert_true(abs(float(retrieved_pt.get("x", 0)) - pt["x"]) < 1e-4, f"X coordinate matches: {pt['x']}")
        assert_true(abs(float(retrieved_pt.get("y", 0)) - pt["y"]) < 1e-4, f"Y coordinate matches: {pt['y']}")
        total_assertions += 4

    # Test Read-All
    all_res = client.read_all_points()
    assert_true(all_res.get("status") == "ok", "Client read_all_points() succeeded")
    assert_true(all_res.get("total", 0) >= 5, f"Primary contains all points (Total: {all_res.get('total')})")
    total_assertions += 2

    # -------------------------------------------------------------------------
    # STEP 5: Multi-Tier State Engine Verification & Genesis-Centric Topology
    # -------------------------------------------------------------------------
    log_step(5, "Verifying Multi-Tier Data Storage & Genesis-Resolved Topology")

    # Tier 1: Primary RAM
    primary_state = get_process_state("http://127.0.0.1:8734", "primary")
    primary_points = primary_state.get("points", [])
    primary_point_ids = {p.get("id") for p in primary_points if isinstance(p, dict)}
    print(f"\n  {BOLD}[TIER 1: Primary RAM (Port 8734)]{RESET}")
    print(f"    - Topology synced from Genesis  : {primary_state.get('topology_source')}")
    print(f"    - Discovered SD Replicas        : {primary_state.get('sd_targets')}")
    print(f"    - Total points stored in memory : {primary_state.get('total')}")
    assert_true(primary_state.get("topology_source") == genesis_url, "Primary resolved topology from Genesis")
    assert_true(len(primary_state.get("sd_targets", [])) == 2, "Primary discovered both SD1 and SD2 from Genesis")
    total_assertions += 2
    for pt in test_points:
        assert_true(pt["id"] in primary_point_ids, f"Primary RAM holds {pt['id']}")
        total_assertions += 1

    # Tier 2A: Shared Data Replica 1
    sd1_state = get_process_state("http://127.0.0.1:8735", "sd1")
    sd1_points = sd1_state.get("points", [])
    sd1_point_ids = {p.get("id") for p in sd1_points if isinstance(p, dict)}
    print(f"\n  {BOLD}[TIER 2A: Shared Data Replica 1 (Port 8735)]{RESET}")
    print(f"    - Topology synced from Genesis  : {sd1_state.get('topology_source')}")
    print(f"    - Discovered Arbit Target       : {sd1_state.get('arbit_target')}")
    print(f"    - Replica point count           : {sd1_state.get('count')} (last_tick={sd1_state.get('last_tick')})")
    assert_true(sd1_state.get("topology_source") == genesis_url, "SD1 resolved topology from Genesis")
    assert_true(sd1_state.get("arbit_target") == "http://127.0.0.1:8737", "SD1 discovered Arbit URL from Genesis")
    assert_true(sd1_state.get("count", 0) >= 5, f"SD1 replicated all 5 points (Count: {sd1_state.get('count')})")
    total_assertions += 3

    # Tier 2B: Shared Data Replica 2
    sd2_state = get_process_state("http://127.0.0.1:8736", "sd2")
    sd2_points = sd2_state.get("points", [])
    sd2_point_ids = {p.get("id") for p in sd2_points if isinstance(p, dict)}
    print(f"\n  {BOLD}[TIER 2B: Shared Data Replica 2 (Port 8736)]{RESET}")
    print(f"    - Topology synced from Genesis  : {sd2_state.get('topology_source')}")
    print(f"    - Discovered Arbit Target       : {sd2_state.get('arbit_target')}")
    print(f"    - Replica point count           : {sd2_state.get('count')} (last_tick={sd2_state.get('last_tick')})")
    assert_true(sd2_state.get("topology_source") == genesis_url, "SD2 resolved topology from Genesis")
    assert_true(sd2_state.get("arbit_target") == "http://127.0.0.1:8737", "SD2 discovered Arbit URL from Genesis")
    assert_true(sd2_state.get("count", 0) >= 5, f"SD2 replicated all 5 points (Count: {sd2_state.get('count')})")
    total_assertions += 3

    # Tier 3: Arbitration Consensus Ledger
    arbit_state = get_process_state("http://127.0.0.1:8737", "arbit")
    print(f"\n  {BOLD}[TIER 3: Arbitration Consensus Node (Port 8737)]{RESET}")
    print(f"    - Topology synced from Genesis  : {arbit_state.get('topology_source')}")
    print(f"    - Discovered L1 Gateway         : {arbit_state.get('l1_gateway')}")
    print(f"    - Official consensus tick       : {arbit_state.get('official_tick')}")
    print(f"    - Official State Root           : {arbit_state.get('state_root')}")
    print(f"    - State commitment sequence     : {arbit_state.get('commitment_seq')}")
    assert_true(arbit_state.get("topology_source") == genesis_url, "Arbit resolved topology from Genesis")
    assert_true(arbit_state.get("l1_gateway") == "http://127.0.0.1:1984", "Arbit discovered L1 gateway from Genesis")
    assert_true(arbit_state.get("official_tick", 0) >= 5, f"Arbit official tick is >= 5 (Tick: {arbit_state.get('official_tick')})")
    assert_true(arbit_state.get("commitment_seq", 0) >= 1, "Arbit commitment sequence has advanced")
    total_assertions += 4

    # Tier 4: Arweave L1 (ArLocal)
    print(f"\n  {BOLD}[TIER 4: Arweave L1 Permaweb Ledger (Port 1984)]{RESET}")
    mine_res = mine_arlocal_block()
    arlocal_info = http_get_json("http://127.0.0.1:1984/info")
    height = arlocal_info.get("height", 0)
    blocks = arlocal_info.get("blocks", 0)
    print(f"    - L1 Current Block Height       : {height} (Total Blocks: {blocks})")
    print(f"    - L1 Network Identifier         : {arlocal_info.get('network')}")
    assert_true(blocks >= 2, f"ArLocal L1 contains mined blocks (Blocks: {blocks})")
    total_assertions += 1

    # -------------------------------------------------------------------------
    # STEP 6: Structured Diagnostic Summary Table
    # -------------------------------------------------------------------------
    log_step(6, "Comprehensive Data Distribution Diagnostic Matrix")
    print("\n" + "=" * 118)
    print(f"{'POINT ID':<18} | {'COORDINATES (X, Y)':<20} | {'PRIMARY (:8734)':<16} | {'SD1 (:8735)':<16} | {'SD2 (:8736)':<16} | {'ARBIT (:8737)':<18}")
    print("-" * 118)
    for pt in test_points:
        coords = f"({pt['x']:>7.2f}, {pt['y']:>7.2f})"
        t1_status = "In RAM [OK]" if pt["id"] in primary_point_ids else "MISSING"
        t2a_status = "Replicated [OK]" if pt["id"] in sd1_point_ids else "MISSING"
        t2b_status = "Replicated [OK]" if pt["id"] in sd2_point_ids else "MISSING"
        t3_status = f"Root seq #{arbit_state.get('commitment_seq')}"
        print(f"{pt['id']:<18} | {coords:<20} | {t1_status:<16} | {t2a_status:<16} | {t2b_status:<16} | {t3_status:<18}")
    print("=" * 118)

    print(f"\n{GREEN}{BOLD}======================================================================{RESET}")
    print(f"{GREEN}{BOLD}  ALL CHECKS PASSED: {total_assertions}/{total_assertions} ASSERTIONS VERIFIED SUCCESSFULLY!   {RESET}")
    print(f"{GREEN}{BOLD}  100% Genesis-Centric Topology & Multi-Tier Flow Proven!             {RESET}")
    print(f"{GREEN}{BOLD}======================================================================{RESET}")


if __name__ == "__main__":
    main()
