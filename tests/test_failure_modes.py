#!/usr/bin/env python3
"""Failure-injection test for the honest ACK contract.

DESTRUCTIVE: this test kills and restarts replicas with tmux.

    python3 tests/test_failure_modes.py --yes
"""
import json
import sys
import time

from _harness import (
    Checker,
    Contract,
    PORTS,
    RED,
    RESET,
    StateEngineClient,
    kill_node,
    require_cluster,
    start_node,
)


def main():
    if "--yes" not in sys.argv:
        print("%sThis test kills and restarts hb-sd-1 / hb-sd-2.%s" % (RED, RESET))
        print("Re-run with: python3 tests/test_failure_modes.py --yes")
        return 2

    require_cluster()
    checker = Checker("test_failure_modes")
    client = StateEngineClient()
    contract = Contract(client, checker)
    client.discover_primary()
    stamp = int(time.time() * 1000) % 10_000_000

    # Make sure the cluster is healthy before breaking it.
    base = client.write_point_confirmed("fm_base_%d" % stamp, 1.0, 1.0, ts=stamp, client_signer="tester")
    checker.check(base.get("http_status") == 200, "baseline write is replicated", str(base)[:300])
    contract.assert_roots_equal("baseline roots agree")

    print("\n[1] Both replicas down: 503, state untouched, no false 200")
    checker.check(kill_node("sd1"), "sd1 stopped")
    checker.check(kill_node("sd2"), "sd2 stopped")
    before = contract.state()
    accepted = client.write_point("fm_dead_%d" % stamp, 2.0, 2.0, ts=stamp + 1, client_signer="tester")
    checker.check(accepted["http_status"] == 202, "write is accepted while quorum is down",
                  str(accepted["http_status"]))
    checker.check(accepted["ack"] == "accepted", "accepted is not claimed as replicated")
    bid = accepted["batch_id"]
    final = None
    for _ in range(40):
        final = client.batch_status(bid)
        if final.get("http_status") == 503:
            break
        time.sleep(0.05)
    checker.check(final and final.get("http_status") == 503,
                  "unresolved batch reports 503 quorum_unavailable", str(final)[:300])
    checker.check(final.get("error") == "quorum_unavailable", "503 names the failure honestly")
    after = contract.state()
    checker.check(after.get("tick") == before.get("tick"), "tick did not move on quorum failure",
                  "%s -> %s" % (before.get("tick"), after.get("tick")))
    checker.check(after.get("state_root") == before.get("state_root"),
                  "root did not move on quorum failure")
    checker.check(client.read_point("fm_dead_%d" % stamp).get("status") == "not_found",
                  "the failed write was never applied")

    print("\n[2] A forged Ack is rejected (no 200 without root+tick verification)")
    forged_root = "0x" + "00" * 32
    code, body, _ = client._push(client.primary_url, "primary", {
        "action": "Ack", "batch_id": bid, "tick": accepted.get("tick_to"),
        "state_root": forged_root, "role": "forged_sd"})
    checker.check(code == 409 and body.get("status") == "ack_mismatch",
                  "Ack with a wrong root is 409 ack_mismatch", "%s %s" % (code, body.get("status")))
    code, body, _ = client._push(client.primary_url, "primary", {
        "action": "Ack", "batch_id": bid, "tick": int(accepted.get("tick_to")) + 100,
        "state_root": accepted.get("target_root"), "role": "forged_sd"})
    checker.check(code == 409, "Ack with a wrong tick is 409", str(code))
    code, body, _ = client._push(client.primary_url, "primary", {
        "action": "Ack", "batch_id": bid, "tick": accepted.get("tick_to"),
        "state_root": accepted.get("target_root"), "role": "forged_sd", "epoch": 999})
    checker.check(code == 409 and body.get("status") == "epoch_mismatch",
                  "Ack from a foreign epoch is 409 epoch_mismatch",
                  "%s %s" % (code, body.get("status")))
    checker.check(client.read_all_points().get("tick") == before.get("tick"),
                  "forged Acks changed nothing")

    print("\n[3] Replicas return: the SAME batch is retried and commits once")
    checker.check(start_node("sd1"), "sd1 restarted")
    checker.check(start_node("sd2"), "sd2 restarted")
    retry = client.write_point("fm_dead_%d" % stamp, 2.0, 2.0, ts=stamp + 1, client_signer="tester")
    checker.check(retry["http_status"] == 202, "retry is accepted", str(retry["http_status"]))
    checker.check(retry["batch_id"] == bid, "retry reuses the original batch_id",
                  str(retry["batch_id"]))
    confirmed = client.wait_for_batch(bid, timeout=30)
    checker.check(confirmed.get("http_status") == 200, "retried batch is replicated",
                  str(confirmed)[:300])
    checker.check(confirmed.get("ack") == "replicated", "confirmed ack is replicated")
    checker.check(client.read_point("fm_dead_%d" % stamp).get("status") == "ok", "the retried write is applied")
    contract.assert_roots_equal("all three nodes agree after recovery")

    print("\n[4] Failover: sd1 alone down still reaches 200 via sd2")
    before = contract.state()
    checker.check(kill_node("sd1"), "sd1 stopped again")
    failover = client.write_point("fm_failover_%d" % stamp, 3.0, 3.0, ts=stamp + 2, client_signer="tester")
    checker.check(failover["http_status"] == 202, "write accepted with sd1 down")
    confirmed = client.wait_for_batch(failover["batch_id"], timeout=30)
    checker.check(confirmed.get("http_status") == 200,
                  "sd2 alone confirms the batch", str(confirmed)[:300])
    checker.check(confirmed.get("tick", 0) > before.get("tick"), "tick advanced")
    checker.check("sd2" in (confirmed.get("replicas") or []), "the ack names sd2",
                  str(confirmed.get("replicas")))

    print("\n[5] sd1 rejoins and catches up from the delta log")
    checker.check(start_node("sd1"), "sd1 restarted")
    deadline = time.time() + 60
    equal = False
    while time.time() < deadline:
        roots = contract.roots()
        if len(set(roots.values())) == 1 and None not in roots.values():
            equal = True
            break
        time.sleep(2)
    checker.check(equal, "roots converge after the replica rejoins", str(contract.roots()))

    print("\n[6] Out-of-order and stale commits at the replica")
    code, body = contract_state_sd(client, "sd1")
    last = body.get("last_tick")
    root = body.get("state_root")
    code, body, _ = client.request(
        "POST",
        "http://127.0.0.1:%d/~node-process@1.0/sd1/push/output~message@1.0" % PORTS["sd1"],
        {"action": "Commit", "batch_id": "fm-future", "tick_from": last + 1000,
         "tick_to": last + 1002, "state_root": root, "delta": []})
    checker.check(code == 202 and body.get("status") == "deferred",
                  "future commit is buffered (202 deferred), not dropped",
                  "%s %s" % (code, body.get("status")))
    code, body = contract_state_sd(client, "sd1")
    checker.check(body.get("last_tick") == last, "buffering a future commit does not apply it")
    code, body, _ = client.request(
        "POST",
        "http://127.0.0.1:%d/~node-process@1.0/sd1/push/output~message@1.0" % PORTS["sd1"],
        {"action": "Commit", "batch_id": "fm-stale", "tick_from": max(0, last - 5),
         "tick_to": max(0, last - 3), "state_root": root, "delta": []})
    checker.check(code == 409 and body.get("status") == "error_tick_mismatch",
                  "stale commit is rejected (409 tick mismatch)",
                  "%s %s" % (code, body.get("status")))

    print("\n[7] A duplicate confirmed write is applied exactly once")
    before = contract.state()
    dup1 = client.write_point("fm_dup_%d" % stamp, 7.0, 7.0, ts=stamp + 3, client_signer="tester")
    ok1 = client.wait_for_batch(dup1["batch_id"], timeout=30)
    dup2 = client.write_point("fm_dup_%d" % stamp, 7.0, 7.0, ts=stamp + 3, client_signer="tester")
    ok2 = client.wait_for_batch(dup2["batch_id"], timeout=30)
    checker.check(dup1["batch_id"] == dup2["batch_id"], "duplicate maps to one batch_id")
    checker.check(ok1.get("tick") == ok2.get("tick"), "duplicate does not advance the tick")
    after = contract.state()
    checker.check(after.get("tick") == ok1.get("tick"), "state settled on the single apply",
                  "%s vs %s" % (after.get("tick"), ok1.get("tick")))

    return 0 if checker.summary() else 1


def contract_state_sd(client, proc):
    code, body, _ = client.request(
        "POST",
        "http://127.0.0.1:%d/~node-process@1.0/%s/push/output~message@1.0" % (PORTS[proc], proc),
        {"action": "Hydrate"})
    return code, body


if __name__ == "__main__":
    sys.exit(main())
