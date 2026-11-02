import time
from tools.client import StateEngineClient

def main():
    c = StateEngineClient()
    c.discover_primary()

    rtts = []
    for i in range(1, 21):
        t0 = time.perf_counter()
        res = c.write_point("player_red", i * 10, i * 10)
        dt = (time.perf_counter() - t0) * 1000.0
        rtts.append(dt)
        slot = res.get("assignment_slot")
        ack = res.get("sd_ack")
        print(f"Move #{i:2d} -> Tick #{slot} ACK={ack} RTT={dt:.1f} ms")

    warm = rtts[1:]
    print("\n================ Latency Summary (20 moves) ================")
    print(f"First Move (Cold) : {rtts[0]:.1f} ms")
    print(f"Warm Moves Avg    : {sum(warm)/len(warm):.1f} ms")
    print(f"Min RTT           : {min(warm):.1f} ms")
    print(f"Max RTT (Warm)    : {max(warm):.1f} ms")
    print("=============================================================")

if __name__ == "__main__":
    main()
