#!/usr/bin/env python3
"""
Simulate Realistic Gameplay (WASD Loop) against HyperBEAM State Engine.
Tests realistic player inputs: directional movement, burst moves, and state persistence.
"""

import time
import json
import urllib.request
from tools.client import StateEngineClient

def main():
    print("======================================================================")
    print("      REALISTIC GAMEPLAY SIMULATION (WASD Move Loop & Verification)   ")
    print("======================================================================")
    
    client = StateEngineClient()
    primary_url = client.discover_primary()
    print(f"[*] Discovered Primary: {primary_url} (Epoch {client.epoch})")
    
    # Starting position
    x, y = 0.0, 0.0
    step = 20.0
    
    # 40-step gameplay path: Right (D) -> Down (S) -> Left (A) -> Up (W)
    moves = (
        [("D", step, 0.0)] * 10 +
        [("S", 0.0, step)] * 10 +
        [("A", -step, 0.0)] * 10 +
        [("W", 0.0, -step)] * 10
    )
    
    latencies = []
    print(f"[*] Executing 40 consecutive player WASD moves...")
    
    for i, (key, dx, dy) in enumerate(moves, 1):
        x += dx
        y += dy
        t0 = time.perf_counter()
        res = client.write_point("player_red", x, y)
        dt = (time.perf_counter() - t0) * 1000.0
        latencies.append(dt)
        
        status = res.get("status")
        tick = res.get("assignment_slot")
        ack = res.get("sd_ack")
        acked_nodes = res.get("sd_acked_nodes", [])
        me_sealed = res.get("micro_epoch_sealed", False)
        me_num = res.get("micro_epoch")
        
        if me_sealed:
            print(f"  [Move {i:2d}/40 Key={key}] Pos=({x:4.0f}, {y:4.0f}) -> Tick #{tick} ★ RSA-4096 SEALED Micro-Epoch #{me_num} RTT={dt:.1f} ms")
        elif i % 5 == 0 or i == 1:
            print(f"  [Move {i:2d}/40 Key={key}] Pos=({x:4.0f}, {y:4.0f}) -> Tick #{tick} ACK={ack} [{'+'.join(acked_nodes)} Quorum] RTT={dt:.1f} ms")
        
        assert status == "ok", f"Move {i} failed: {res}"
        assert ack is True, f"Move {i} did not get SD ACK: {res}"
        assert set(acked_nodes) == {"sd1", "sd2"}, f"Move {i} missing replicas: {acked_nodes}"

    warm = latencies[1:]
    print("\n------------------------- LATENCY SUMMARY -------------------------")
    print(f"Total Moves Executed  : {len(moves)}")
    print(f"First Move (Cold)     : {latencies[0]:.1f} ms")
    print(f"Warm Moves Average    : {sum(warm)/len(warm):.1f} ms")
    print(f"Min RTT               : {min(warm):.1f} ms")
    print(f"Max RTT (Warm)        : {max(warm):.1f} ms")
    print("-------------------------------------------------------------------")
    
    # Verify State Persistence across all tiers
    print("\n[*] Verifying Multi-Tier State Convergence after Gameplay:")
    p_data = json.loads(urllib.request.urlopen("http://127.0.0.1:8734/~node-process@1.0/primary/now/results~message@1.0/output/body").read().decode("utf-8"))
    sd1_data = json.loads(urllib.request.urlopen("http://127.0.0.1:8735/~node-process@1.0/sd1/now/results~message@1.0/output/body").read().decode("utf-8"))
    sd2_data = json.loads(urllib.request.urlopen("http://127.0.0.1:8736/~node-process@1.0/sd2/now/results~message@1.0/output/body").read().decode("utf-8"))
    
    p_points = {p["id"]: p for p in p_data.get("points", []) if isinstance(p, dict)}
    player_primary = p_data.get("point") or p_points.get("player_red")
    print(f"  Primary RAM Player Pos : ({player_primary.get('x')}, {player_primary.get('y')}) [Tick #{player_primary.get('tick')}]")
    print(f"  SD1 Replicated Tick    : #{sd1_data.get('last_tick')} (Signer: {sd1_data.get('sd_signer')[:12]}...)")
    print(f"  SD2 Replicated Tick    : #{sd2_data.get('last_tick')} (Signer: {sd2_data.get('sd_signer')[:12]}...)")
    
    assert player_primary.get("x") == x and player_primary.get("y") == y, "Primary position mismatch!"
    assert sd1_data.get("last_tick") >= player_primary.get("tick"), "SD1 tick behind Primary!"
    assert sd2_data.get("last_tick") >= player_primary.get("tick"), "SD2 tick behind Primary!"
    
    print("\n[SUCCESS] Realistic gameplay test passed 100%! All 40 moves replicated & synchronized seamlessly.")
    print("======================================================================")

if __name__ == "__main__":
    main()
