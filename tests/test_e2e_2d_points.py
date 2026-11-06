#!/usr/bin/env python3
"""End-to-end contract test for the 2D point engine (happy path).

Run against a live cluster:

    ./tools/cluster_up.sh
    FRESH=1 ./tools/apply_engine.sh
    python3 tests/test_e2e_2d_points.py

Every assertion here is about externally observable behaviour: the 202/200/503
contract, canonical roots, idempotency, ownership, and input guards.
"""
import json
import sys
import time

from _harness import (
    Checker,
    Contract,
    PORTS,
    RESET,
    YELLOW,
    StateEngineClient,
    kill_node,
    require_cluster,
    start_node,
)


def main():
    require_cluster()
    checker = Checker("test_e2e_2d_points")
    client = StateEngineClient()
    contract = Contract(client, checker)
    client.discover_primary()
    stamp = int(time.time() * 1000) % 10_000_000

    print("\n[1] L0 accept (202) never claims replication")
    accepted = client.write_point("e2e_a", 1.0, 2.0, ts=stamp, client_signer="alice")
    checker.check(accepted["http_status"] == 202, "write returns 202",
                  str(accepted))
    checker.check(accepted["ack"] == "accepted", "202 ack is 'accepted'", str(accepted["ack"]))
    checker.check(bool(accepted["batch_id"]), "202 carries a batch_id")
    checker.check("replicated" not in json.dumps(accepted["raw"]),
                  "202 body never contains a replicated claim")

    print("\n[2] L1 confirmation (200) is verified and carries the root")
    final = client.wait_for_batch(accepted["batch_id"], timeout=20)
    checker.check(final.get("http_status") == 200, "batch resolves to 200", str(final)[:300])
    checker.check(final.get("ack") == "replicated", "200 ack is 'replicated'")
    checker.check(bool(final.get("replicas")), "200 lists the replica(s) that acked",
                  str(final.get("replicas")))
    checker.check(isinstance(final.get("tick"), int), "200 carries an integer tick")
    root_a = final.get("state_root")
    checker.check(isinstance(root_a, str) and root_a.startswith("0x") and len(root_a) == 66,
                  "200 carries a canonical 32-byte root", str(root_a))

    print("\n[3] The point is readable with the owner that was supplied")
    point = client.read_point("e2e_a")
    checker.check(point.get("point", {}).get("owner") == "alice", "owner is alice",
                  str(point.get("point")))
    checker.check(point.get("point", {}).get("x") == 1.0, "x == 1.0")

    print("\n[4] Primary and both replicas converge on the canonical root")
    roots = contract.assert_roots_equal()
    checker.note("converged roots: %s" % json.dumps(roots))

    print("\n[5] Idempotency: the same input twice is accepted once")
    tick_before = contract.state().get("tick")
    replay = client.write_point("e2e_a", 1.0, 2.0, ts=stamp, client_signer="alice")
    checker.check(replay["batch_id"] == accepted["batch_id"],
                  "replay returns the original batch_id", str(replay["batch_id"]))
    checker.check(replay["http_status"] in (200, 202), "replay is answered without error",
                  str(replay["http_status"]))
    time.sleep(0.3)
    contract.assert_tick_stable(tick_before, "replay did not advance the tick")
    checker.check(contract.roots()["primary"] == root_a, "replay did not change the root")

    print("\n[6] A real update (new timestamp) does advance state")
    moved = client.write_point_confirmed("e2e_a", 4.0, 5.0, ts=stamp + 1, client_signer="alice")
    checker.check(moved.get("http_status") == 200, "update is replicated", str(moved)[:300])
    checker.check(moved.get("tick", 0) > tick_before, "tick advanced",
                  "%s -> %s" % (tick_before, moved.get("tick")))
    checker.check(moved.get("state_root") != root_a, "root changed with the update")
    point = client.read_point("e2e_a")
    checker.check(point.get("point", {}).get("x") == 4.0, "new x visible")

    print("\n[7] Ownership cannot be forged with a different client_signer")
    tick_before = contract.state().get("tick")
    forged = client.write_point("e2e_a", 9.0, 9.0, ts=stamp + 2, client_signer="mallory")
    checker.check(forged["http_status"] == 403, "forged owner is rejected with 403",
                  str(forged["http_status"]))
    checker.check(contract.state().get("tick") == tick_before,
                  "rejected write did not move the tick")
    checker.check(client.read_point("e2e_a").get("point", {}).get("x") == 4.0,
                  "rejected write did not change the point")

    print("\n[8] Input guards: coordinates, ids and payload size")
    bad = client.write_point("e2e_bad", 5_000.0, 0.0, ts=stamp + 3)
    checker.check(bad["http_status"] == 400, "out-of-range coordinates rejected", str(bad["http_status"]))
    bad = client.write_point("e2e\nbad", 1.0, 1.0, ts=stamp + 4)
    checker.check(bad["http_status"] == 400, "id with a newline rejected", str(bad["http_status"]))
    huge = client.write_point("e2e_huge", 1.0, 1.0, ts=stamp + 5,
                              idempotency_key="x" * 70000)
    checker.check(huge["http_status"] in (413, 400), "oversized field rejected",
                  str(huge["http_status"]))

    print("\n[9] Unknown batch ids are 404, never a fake success")
    unknown = client.batch_status("b-does-not-exist")
    checker.check(unknown.get("http_status") == 404, "unknown batch -> 404",
                  str(unknown.get("http_status")))

    print("\n[10] Read paths agree")
    read_all = client.read_all_points()
    snap = client.snapshot()
    checker.check(read_all.get("tick") == snap.get("tick"), "Read-All and Snapshot see the same tick")
    checker.check(read_all.get("state_root") == snap.get("state_root"),
                  "Read-All and Snapshot see the same root")
    checker.check(len(snap.get("points", [])) == snap.get("active_entity_count"),
                  "snapshot count matches its point array")

    return 0 if checker.summary() else 1


if __name__ == "__main__":
    sys.exit(main())
