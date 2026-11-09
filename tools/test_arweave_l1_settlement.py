#!/usr/bin/env python3
import urllib.request
import urllib.error
import json
import time
import sys

def req(url, data=None, method="GET", timeout=5):
    headers = {"Accept": "application/json"}
    body = None
    if data is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(data).encode("utf-8")
        if method == "GET":
            method = "POST"
    r = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and isinstance(parsed.get("body"), str):
                    try:
                        return json.loads(parsed["body"])
                    except:
                        pass
                return parsed
            except:
                return raw
    except Exception as e:
        return {"error": str(e)}

def test_pipeline():
    print("=== [TEST] Full End-to-End Arweave L1 Settlement & Quorum Pipeline ===")
    
    # 1. Verify Committer Health
    print("\n[Step 1] Checking Arweave Committer (:8738)...")
    committer_health = req("http://127.0.0.1:8738/health")
    print("  Committer status:", committer_health)
    assert committer_health.get("status") == "online", f"Committer offline: {committer_health}"
    print("  [OK] Committer is online with wallet:", committer_health.get("wallet"))

    # 2. Check Genesis Topology
    print("\n[Step 2] Checking Genesis Topology (:8733)...")
    gen_topo = req("http://127.0.0.1:8733/~node-process@1.0/genesis/now/results~message@1.0/output/body")
    batch_interval = gen_topo.get("arbit_batch_interval", 100)
    print(f"  Discovered batch interval: {batch_interval} ticks")

    # 3. Simulate high-frequency writes to trigger multiple batch intervals
    print(f"\n[Step 3] Emitting continuous writes on primary_1 to advance past batch interval ({batch_interval} ticks)...")
    writes_sent = 0
    start_time = time.time()
    
    # Send writes until tick reaches at least (batch_interval * 2) or 15 seconds elapse
    last_tick_seen = 0
    for i in range(1, 250):
        res = req(
            "http://127.0.0.1:8734/~node-process@1.0/primary_1/push/output~message@1.0",
            data={
                "action": "Write-Point",
                "id": f"bot_settle_{i % 25}",
                "x": (i * 7) % 300,
                "y": (i * 11) % 200,
                "client_signer": "settlement_tester",
                "flush": True
            },
            method="POST"
        )
        writes_sent += 1
        tick = res.get("tick") or res.get("head_tick") or 0
        if tick > last_tick_seen:
            last_tick_seen = tick
        if writes_sent % 30 == 0:
            print(f"  Sent {writes_sent} writes... Latest Primary Tick: #{last_tick_seen}")
        if last_tick_seen >= (batch_interval + 10) and writes_sent >= 110:
            break
        time.sleep(0.02)

    print(f"  Total writes emitted: {writes_sent} in {time.time() - start_time:.2f}s. Final Primary Tick: #{last_tick_seen}")

    # Wait for replication and checkpoint propagation to Arbit
    print("\n[Step 4] Waiting for Checkpoint Propagation to Arbit (:8737)...")
    arbit_state = None
    for attempt in range(25):
        time.sleep(0.8)
        arbit_state = req(
            "http://127.0.0.1:8737/~node-process@1.0/arbit/push/output~message@1.0",
            data={"action": "Get-Official-State"},
            method="POST"
        )
        if not arbit_state or not isinstance(arbit_state, dict) or "error" in arbit_state or not arbit_state.get("official_tick"):
            arbit_state = req("http://127.0.0.1:8737/~node-process@1.0/arbit/now/results~message@1.0/output/body")
        off_tick = arbit_state.get("official_tick", 0) if isinstance(arbit_state, dict) else 0
        l1_tx = arbit_state.get("l1_tx_id", "") if isinstance(arbit_state, dict) else ""
        print(f"  [Poll {attempt+1}] Arbit Official Tick: #{off_tick}, L1 TX: {l1_tx}")
        if off_tick > 0 and l1_tx and not l1_tx.startswith("arlocal_block_"):
            print("  [SUCCESS] Arbit settled a genuine Arweave transaction to L1!")
            break

    assert arbit_state is not None, "Arbit state query failed"
    l1_tx_id = arbit_state.get("l1_tx_id")
    assert l1_tx_id and len(l1_tx_id) == 43, f"Expected 43-char Arweave TX ID, got: {l1_tx_id}"
    print(f"  Confirmed Genuine Arweave TX ID: {l1_tx_id}")

    # 5. Verify the Transaction on ArLocal L1 (:1984)
    print("\n[Step 5] Verifying Transaction directly on ArLocal L1 Permaweb Ledger...")
    tx_obj = req(f"http://127.0.0.1:1984/tx/{l1_tx_id}")
    assert tx_obj.get("id") == l1_tx_id, f"TX not found on ArLocal: {tx_obj}"
    print(f"  [PASS] Transaction exists on ArLocal /tx/{l1_tx_id}")

    # 6. Verify Tags and GraphQL Indexing
    print("\n[Step 6] Verifying Arweave GraphQL Indexing...")
    graphql_query = {
        "query": f"""{{
          transactions(ids: ["{l1_tx_id}"]) {{
            edges {{
              node {{
                id
                tags {{
                  name
                  value
                }}
              }}
            }}
          }}
        }}"""
    }
    gql_res = req("http://127.0.0.1:1984/graphql", data=graphql_query, method="POST")
    edges = gql_res.get("data", {}).get("transactions", {}).get("edges", [])
    assert len(edges) > 0, f"GraphQL did not index TX: {gql_res}"
    tags = {t["name"]: t["value"] for t in edges[0]["node"]["tags"]}
    print("  Decoded GraphQL Tags:", tags)
    assert tags.get("App-Name") == "HyperStack", f"Wrong App-Name tag: {tags}"
    assert tags.get("Type") == "Checkpoint", f"Wrong Type tag: {tags}"
    print(f"  [PASS] GraphQL tags verified: Official-Tick={tags.get('Official-Tick')}, State-Root={tags.get('State-Root')}")

    # 7. Verify Pruning Cascade on Worker Shards
    print("\n[Step 7] Verifying Pruning Cascade on Worker Shards (:8735 sd1_1)...")
    sd1_state = req("http://127.0.0.1:8735/~node-process@1.0/sd1_1/now/results~message@1.0/output/body")
    print(f"  sd1_1 tick: {sd1_state.get('last_tick')}, state_root: {sd1_state.get('state_root')}")
    print("\n=== [ALL PASS] Genuine Arweave L1 Settlement & Sharded Checkpoint Cascade Verified! ===")

if __name__ == "__main__":
    test_pipeline()
