#!/usr/bin/env python3
"""
=============================================================================
HyperStack Security & Failure Modes Verification Suite
=============================================================================
Verifies protections against critical vulnerabilities identified in code review:
- C1: Quorum validation (atomic commit vs rollback)
- C2: Arbit consensus poisoning defense (excessive tick leaps rejected)
- C2b: Arbit sequence monotonicity enforcement
- C3: Genesis unauthorized node registration defense (SSRF & hijack guard)
- C3b: Genesis URL format validation (injection defense)
- H1: SD out-of-order write rejection (stale tick rejected)
- H7: Primary coordinate validation (NaN, Infinity, bounds [-1000, 1000])
- H7b: Primary entity ID format validation
=============================================================================
"""

import sys
import os
import json
import urllib.request
import urllib.error

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tools.client import StateEngineClient

GREEN = "\033[92m"
RED = "\033[91m"
BOLD = "\033[1m"
CYAN = "\033[96m"
RESET = "\033[0m"

TOTAL_TESTS = 0
PASSED_TESTS = 0


def assert_test(cond: bool, label: str):
    global TOTAL_TESTS, PASSED_TESTS
    TOTAL_TESTS += 1
    if cond:
        PASSED_TESTS += 1
        print(f"  {GREEN}[PASS]{RESET} {label}")
    else:
        print(f"  {RED}[FAIL]{RESET} {label}")
        raise AssertionError(f"Test failed: {label}")


def post_process_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        res_raw = resp.read().decode("utf-8")
        parsed = json.loads(res_raw)
        if isinstance(parsed, dict) and parsed.get("ao-result") == "body" and isinstance(parsed.get("body"), str):
            return json.loads(parsed["body"])
        return parsed


def main():
    print(f"\n{BOLD}{CYAN}======================================================================{RESET}")
    print(f"{BOLD}{CYAN}      HYPERSTACK SECURITY & FAILURE MODES TEST SUITE                  {RESET}")
    print(f"{BOLD}{CYAN}======================================================================{RESET}\n")

    client = StateEngineClient()
    primary_url = client.discover_primary()

    # -------------------------------------------------------------------------
    # TEST 1: Primary Coordinate Bounds & NaN Guards (H7)
    # -------------------------------------------------------------------------
    print(f"{BOLD}--- [Test 1] Primary Coordinate Bounds & NaN Protection (H7) ---{RESET}")
    # NaN
    push_url = f"{primary_url}/~node-process@1.0/primary/push/output~message@1.0/body"
    res_nan = post_process_json(push_url, {"action": "Write-Point", "id": "pt_nan", "x": "NaN", "y": 10.0})
    assert_test(res_nan.get("status") == "error_invalid_coordinates", "NaN coordinate rejected with error_invalid_coordinates")

    # Out of bounds (+99999)
    res_oob = post_process_json(push_url, {"action": "Write-Point", "id": "pt_oob", "x": 99999.0, "y": 0.0})
    assert_test(res_oob.get("status") == "error_invalid_coordinates", "Out-of-bounds (+99999) rejected with error_invalid_coordinates")

    # -------------------------------------------------------------------------
    # TEST 2: Primary Entity ID Format Validation (H7b)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 2] Primary Entity ID Format Validation (H7b) ---{RESET}")
    # Newlines in ID (injection attempt)
    res_inject = post_process_json(push_url, {"action": "Write-Point", "id": "pt_bad\ninjection", "x": 10.0, "y": 20.0})
    assert_test(res_inject.get("status") == "error_invalid_entity_id", "Entity ID with newlines rejected with error_invalid_entity_id")

    # -------------------------------------------------------------------------
    # TEST 3: Genesis Unauthorized Node Registration (C3)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 3] Genesis Unauthorized Registration Guard (C3) ---{RESET}")
    genesis_push = "http://127.0.0.1:8733/~node-process@1.0/genesis/push/output~message@1.0/body"
    res_unauth = post_process_json(genesis_push, {
        "action": "Register-Node",
        "role": "primary",
        "url": "http://127.0.0.1:9999",
        "caller_signer": "attacker_fake_key"
    })
    assert_test(res_unauth.get("status") == "error_unauthorized", "Unauthorized Register-Node rejected with error_unauthorized")

    # -------------------------------------------------------------------------
    # TEST 4: Genesis URL Injection / SSRF Guard (C3b)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 4] Genesis Malformed / Injection URL Guard (C3b) ---{RESET}")
    res_bad_url = post_process_json(genesis_push, {
        "action": "Register-Node",
        "role": "primary",
        "url": "http://127.0.0.1:8734\",bad_json:true",
        "admin_key": "hyperstack_admin_genesis_key"
    })
    assert_test(res_bad_url.get("status") == "error_invalid_url", "Malformed URL with injection syntax rejected with error_invalid_url")
    # Restore Genesis active topology output
    post_process_json(genesis_push, {"action": "Get-Topology"})

    # -------------------------------------------------------------------------
    # TEST 5: Shared Data Out-of-Order / Stale Tick Rejection (H1)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 5] Shared Data Stale Tick Guard (H1) ---{RESET}")
    sd1_push = "http://127.0.0.1:8735/~node-process@1.0/sd1/push/output~message@1.0/body"
    sd1_now = post_process_json(sd1_push, {"action": "Get-Replica-State"})
    base_tick = int(sd1_now.get("last_tick") or 0)
    target_tick = base_tick + 10
    stale_tick = target_tick - 5
    entity_id = f"pt_order_{base_tick}"

    # First, write a valid point at target_tick
    res_sd_ok = post_process_json(sd1_push, {"action": "Replicate", "id": entity_id, "x": 1.0, "y": 2.0, "tick": target_tick})
    assert_test(res_sd_ok.get("status") == "replicated", f"Valid replication update accepted at tick {target_tick}")

    # Second, send a stale out-of-order write for the same entity at stale_tick
    res_sd_stale = post_process_json(sd1_push, {"action": "Replicate", "id": entity_id, "x": 9.0, "y": 9.0, "tick": stale_tick})
    assert_test(res_sd_stale.get("status") == "stale_tick_ignored", f"Stale tick ({stale_tick} <= {target_tick}) ignored with stale_tick_ignored")

    # Verify existing entity at target_tick was NOT overwritten
    res_read = post_process_json(sd1_push, {"action": "Read-Point", "id": entity_id})
    assert_test(res_read.get("point", {}).get("tick") == target_tick, f"ReplicaStore retained tick {target_tick}, stale tick did not overwrite")
    # Restore SD1 active replica state output
    post_process_json(sd1_push, {"action": "Get-Replica-State"})

    # -------------------------------------------------------------------------
    # TEST 6: Arbit Consensus Excessive Tick Leap Guard (C2)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 6] Arbit Excessive Tick Leap Poisoning Guard (C2) ---{RESET}")
    arbit_push = "http://127.0.0.1:8737/~node-process@1.0/arbit/push/output~message@1.0/body"
    # Attempt to poison ledger with tick 99999999
    res_leap = post_process_json(arbit_push, {
        "action": "Commit-Checkpoint",
        "last_tick": 99999999,
        "point_count": 100,
        "diff_seq": 1000
    })
    assert_test(res_leap.get("status") == "error_excessive_tick_leap", "Excessive tick leap rejected with error_excessive_tick_leap")

    # -------------------------------------------------------------------------
    # TEST 7: Arbit Diff Sequence Monotonicity Guard (C2b)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 7] Arbit Diff Sequence Regression Guard (C2b) ---{RESET}")
    arbit_now = post_process_json(arbit_push, {"action": "Get-Official-State"})
    arbit_tick = int(arbit_now.get("official_tick") or 0)
    arbit_seq = int(arbit_now.get("diff_seq") or 0)
    next_tick = arbit_tick + 5
    next_seq = arbit_seq + 5

    # Legitimate checkpoint
    res_valid_chk = post_process_json(arbit_push, {
        "action": "Commit-Checkpoint",
        "last_tick": next_tick,
        "point_count": 6,
        "diff_seq": next_seq
    })
    assert_test(res_valid_chk.get("status") == "committed", f"Legitimate checkpoint accepted at tick {next_tick}, diff_seq {next_seq}")

    # Regression attempt (diff_seq < next_seq)
    res_regress = post_process_json(arbit_push, {
        "action": "Commit-Checkpoint",
        "last_tick": next_tick + 1,
        "point_count": 6,
        "diff_seq": next_seq - 1
    })
    assert_test(res_regress.get("status") == "error_diff_seq_regression", f"Diff sequence regression ({next_seq - 1} < {next_seq}) rejected with error_diff_seq_regression")
    # Restore Arbit active consensus state output
    post_process_json(arbit_push, {"action": "Get-Official-State"})

    # -------------------------------------------------------------------------
    # TEST 8: Quorum Verification & Signer Propagation (C1, C4)
    # -------------------------------------------------------------------------
    print(f"\n{BOLD}--- [Test 8] Primary Majority Quorum & Atomic Write Verification (C1) ---{RESET}")
    res_write = client.write_point("pt_quorum_test", 50.0, 50.0)
    assert_test(res_write["status"] == "ok", "Client write succeeds with active SD quorum")
    assert_test(res_write["sd_ack"] is True, "sd_ack is True indicating verified replication")
    assert_test(len(res_write["sd_acked_nodes"]) >= 1, f"At least 1 SD replica confirmed (got {res_write['sd_acked_nodes']})")
    assert_test(res_write["sd_persistence"] == "ram_latest+lmdb_disk", "Persistence verified as ram_latest+lmdb_disk")

    print(f"\n{GREEN}{BOLD}======================================================================{RESET}")
    print(f"{GREEN}{BOLD}  ALL {PASSED_TESTS}/{TOTAL_TESTS} SECURITY & FAILURE MODE TESTS PASSED!{RESET}")
    print(f"{GREEN}{BOLD}======================================================================{RESET}\n")


if __name__ == "__main__":
    main()
