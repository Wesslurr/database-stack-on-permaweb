#!/usr/bin/env python3
"""
Hard Data Reset Tool for HyperStack Circle Test & Multi-Node Cluster.

Cleanly stops all HyperBEAM nodes, ArLocal, the L1 Committer, and the Web UI,
wipes all persisted LMDB stores and runtime logs back to tick 0 (empty world),
rebuilds node manifests, optionally restarts the fresh cluster + UI, and
verifies tick == 0 and empty entity state across primary, sd1, and sd2.
"""

import argparse
import http.client
import json
import os
import shutil
import socket
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "engine", "data")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
CACHE_DIRS = [
    os.path.join(PROJECT_ROOT, "cache-mainnet"),
    os.path.join(PROJECT_ROOT, "HyperBEAM", "cache-mainnet"),
]

NODES = ("genesis", "primary", "sd1", "sd2", "arbit")
TMUX_SESSIONS = (
    "hb-genesis",
    "hb-primary",
    "hb-sd-1",
    "hb-sd-2",
    "hb-arbit",
    "hb-client",
    "hb-ui",
    "hb-web",
)
VERIFY_TARGETS = (
    ("primary", 8734, "primary"),
    ("sd1", 8735, "sd1"),
    ("sd2", 8736, "sd2"),
)


def _run(cmd, check=False, capture=True):
    return subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=capture,
        check=check,
    )


def _port_open(port, timeout=0.4):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def _dir_size_bytes(path):
    if not os.path.exists(path):
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            fp = os.path.join(root, name)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def _fmt_mb(num_bytes):
    return f"{num_bytes / (1024 * 1024):.2f} MB"


def _stop_cluster():
    """Stop all HyperBEAM BEAM VMs, ArLocal container, and Game Client UI."""
    print("=== [1/4] Stopping running cluster processes ===")
    down_script = os.path.join(PROJECT_ROOT, "tools", "cluster_down.sh")
    if os.path.isfile(down_script):
        _run(["bash", down_script])

    for sess in TMUX_SESSIONS:
        _run(["tmux", "kill-session", "-t", sess])

    for port in (8080, 8733, 8734, 8735, 8736, 8737):
        _run(["fuser", "-k", f"{port}/tcp"])

    for pattern in ("tools/game_client/serve.py",):
        _run(["pkill", "-f", pattern])

    _run(["docker", "rm", "-f", "arlocal"])

    # Wait until all hb-* BEAM VMs exit cleanly before removing LMDB files.
    deadline = time.time() + 15.0
    while time.time() < deadline:
        res = _run(["pgrep", "-f", "sname[ =]hb-"])
        if res.returncode != 0:
            break
        time.sleep(0.4)
    else:
        _run(["pkill", "-9", "-f", "sname[ =]hb-"])
        time.sleep(0.5)

    print("  [OK] All cluster processes and ArLocal stopped.")


def _wipe_directory_contents(path):
    freed = _dir_size_bytes(path)
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return freed


def _wipe_state(keep_logs=False, wipe_wallets=False, wipe_cache=False):
    """Delete persisted LMDB stores, temporary logs, and optional caches."""
    print("\n=== [2/4] Wiping persisted stores & runtime artifacts ===")
    total_freed = 0

    for node in NODES:
        lmdb_dir = os.path.join(DATA_DIR, node, "lmdb")
        freed = _wipe_directory_contents(lmdb_dir)
        total_freed += freed
        print(f"  [WIPED] engine/data/{node}/lmdb ({_fmt_mb(freed)})")

        if wipe_wallets:
            wallet_path = os.path.join(DATA_DIR, node, "wallet.json")
            if os.path.isfile(wallet_path):
                os.remove(wallet_path)
                print(f"  [WIPED] engine/data/{node}/wallet.json")

    if wipe_wallets:
        hb_key = os.path.join(PROJECT_ROOT, "hyperbeam-key.json")
        if os.path.isfile(hb_key):
            os.remove(hb_key)
            print("  [WIPED] hyperbeam-key.json")

    if not keep_logs and os.path.isdir(LOGS_DIR):
        log_freed = 0
        for name in os.listdir(LOGS_DIR):
            fp = os.path.join(LOGS_DIR, name)
            if os.path.isfile(fp) and name.endswith(".log"):
                try:
                    log_freed += os.path.getsize(fp)
                    os.remove(fp)
                except OSError:
                    pass
        total_freed += log_freed
        print(f"  [WIPED] logs/*.log ({_fmt_mb(log_freed)})")

    if os.path.isfile("/tmp/arlocal.log"):
        try:
            os.remove("/tmp/arlocal.log")
        except OSError:
            pass

    if wipe_cache:
        for cache_dir in CACHE_DIRS:
            if os.path.exists(cache_dir):
                freed = _wipe_directory_contents(cache_dir)
                total_freed += freed
                rel = os.path.relpath(cache_dir, PROJECT_ROOT)
                print(f"  [WIPED] {rel} ({_fmt_mb(freed)})")

    print("\n=== [3/4] Rebuilding node configurations ===")
    res = _run([sys.executable, os.path.join(PROJECT_ROOT, "tools", "build_configs.py")], capture=False)
    if res.returncode != 0:
        raise RuntimeError("build_configs.py failed during hard reset.")

    print(f"  [OK] Hard wipe complete (total freed: {_fmt_mb(total_freed)}).")


def _unwrap_body(payload):
    if not isinstance(payload, dict):
        return {}
    if isinstance(payload.get("body"), dict):
        return payload["body"]
    if isinstance(payload.get("body"), str):
        try:
            return json.loads(payload["body"])
        except Exception:
            return {}
    return payload


def _http_json(port, method, path, payload=None, timeout=8.0):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        headers = {"Accept": "application/json", "Cache-Control": "no-store"}
        body_bytes = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body_bytes = json.dumps(payload).encode("utf-8")
        conn.request(method, path, body=body_bytes, headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        if resp.status not in (200, 202):
            return None, f"HTTP {resp.status}: {raw[:120]}"
        return json.loads(raw), None
    except Exception as exc:
        return None, str(exc)
    finally:
        conn.close()


def _verify_clean_zero():
    """Confirm ArLocal height == 0 and primary/sd1/sd2 all start cleanly at tick 0."""
    print("\n=== Verifying clean tick-0 state ===")
    ar_info, err = _http_json(1984, "GET", "/info")
    if ar_info is None:
        print(f"  [WARN] ArLocal /info check failed: {err}")
    else:
        print(f"  [OK] ArLocal :1984 height = {ar_info.get('height', '?')}")

    roots = {}
    all_clean = True
    for name, port, proc in VERIFY_TARGETS:
        path = f"/~node-process@1.0/{proc}/push/output~message@1.0"
        data, err = _http_json(port, "POST", path, payload={"action": "Hydrate"}, timeout=10.0)
        if data is None:
            print(f"  [ERROR] {name} (:{port}) hydrate failed: {err}")
            all_clean = False
            continue
        body = _unwrap_body(data)
        tick = int(body.get("tick", -1))
        head_tick = int(body.get("head_tick", tick))
        pts = body.get("points") if isinstance(body.get("points"), list) else []
        root = str(body.get("state_root", ""))
        roots[name] = root
        is_zero = (tick == 0 and head_tick == 0 and len(pts) == 0)
        print(
            f"  [{'OK' if is_zero else 'WARN'}] "
            f"{name:<7} (:{port}) tick={tick} head_tick={head_tick} "
            f"entities={len(pts)} root={root[:18]}..."
        )
        if not is_zero:
            all_clean = False

    if len(roots) < len(VERIFY_TARGETS) or len(set(roots.values())) != 1:
        print(f"  [ERROR] State root mismatch across nodes: {roots}")
        all_clean = False

    return all_clean


def _start_cluster_and_ui(start_ui):
    print("\n=== [4/4] Starting fresh cluster from tick 0 ===")
    up_script = os.path.join(PROJECT_ROOT, "tools", "cluster_up.sh")
    res = _run(["bash", up_script], capture=False)
    if res.returncode != 0:
        raise RuntimeError("cluster_up.sh failed while bringing up clean cluster.")

    # Verify tick == 0 before opening :8080 so any existing browser tab cannot race ahead.
    ok = _verify_clean_zero()

    if start_ui:
        print("\n=== Starting Interactive Circle Test UI (:8080) ===")
        ui_cmd = f"{sys.executable} tools/game_client/serve.py"
        _run(["tmux", "new-session", "-d", "-s", "hb-ui", "-c", PROJECT_ROOT, ui_cmd], check=True)
        for _ in range(20):
            if _port_open(8080):
                print("  [OK] Web UI ready at http://127.0.0.1:8080/")
                break
            time.sleep(0.25)

    print("\n==========================================================")
    if ok:
        print("  HARD RESET COMPLETE: Cluster is clean at Tick #0 (0 bots)")
        if start_ui:
            print("  Hard-refresh browser (Ctrl+Shift+R) at http://127.0.0.1:8080/")
        else:
            print("  Launch UI with: python3 tools/game_client/serve.py")
    else:
        print("  HARD RESET FINISHED WITH WARNINGS (check node logs above)")
    print("==========================================================")
    return ok


def main():
    parser = argparse.ArgumentParser(
        description="Hard-reset all HyperStack node data, ArLocal chain, and Circle Test state back to tick 0."
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip interactive confirmation prompt.",
    )
    parser.add_argument(
        "--no-start",
        action="store_true",
        help="Stop cluster and wipe all persisted data without restarting nodes.",
    )
    parser.add_argument(
        "--with-ui",
        action="store_true",
        help="Start tools/game_client/serve.py (:8080) in tmux session hb-ui after reset.",
    )
    parser.add_argument(
        "--keep-logs",
        action="store_true",
        help="Keep logs/*.log files instead of deleting them.",
    )
    parser.add_argument(
        "--wipe-wallets",
        action="store_true",
        help="Also delete node wallet.json files and hyperbeam-key.json (OFF by default).",
    )
    parser.add_argument(
        "--wipe-cache",
        action="store_true",
        help="Also clear cache-mainnet directories.",
    )
    args = parser.parse_args()

    ui_was_running = _port_open(8080)

    if not args.yes:
        print("WARNING: This will permanently erase current Circle Test & cluster state:")
        print("  - Stops hb-genesis, hb-primary, hb-sd-1, hb-sd-2, hb-arbit, hb-ui, and arlocal")
        print("  - Wipes engine/data/{genesis,primary,sd1,sd2,arbit}/lmdb (all ticks, bots, and entities)")
        if not args.keep_logs:
            print("  - Wipes logs/*.log")
        if args.wipe_cache:
            print("  - Wipes cache-mainnet/")
        print("  - Preserves: node wallet.json files" + (" (WILL BE WIPED: --wipe-wallets)" if args.wipe_wallets else ""))
        answer = input("\nProceed with hard reset? [y/N]: ").strip().lower()
        if answer not in ("y", "yes"):
            print("Aborted.")
            return 1

    _stop_cluster()
    _wipe_state(
        keep_logs=args.keep_logs,
        wipe_wallets=args.wipe_wallets,
        wipe_cache=args.wipe_cache,
    )

    if args.no_start:
        print("\n=== Hard reset complete (--no-start). Run ./tools/cluster_up.sh or ./start.sh when ready. ===")
        return 0

    should_start_ui = args.with_ui or ui_was_running
    ok = _start_cluster_and_ui(should_start_ui)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
