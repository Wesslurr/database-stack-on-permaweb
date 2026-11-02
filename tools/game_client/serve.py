#!/usr/bin/env python3
"""
Lightweight HTTP Server + OS Telemetry Endpoint (`/metrics`) for the
Interactive Red Circle WASD Game Client.
Strictly separated from engine/ (located in tools/game_client/).
"""

import http.server
import json
import os
import socketserver
import subprocess

PORT = 8080
WEB_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(WEB_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "engine", "data")

NODES = [
    {"name": "genesis", "sname": "hb-genesis", "port": 8733},
    {"name": "primary", "sname": "hb-primary", "port": 8734},
    {"name": "sd1", "sname": "hb-sd-1", "port": 8735},
    {"name": "sd2", "sname": "hb-sd-2", "port": 8736},
    {"name": "arbit", "sname": "hb-arbit", "port": 8737},
]


def collect_node_metrics():
    """Collects live OS RAM (RSS MB), CPU (%), and LMDB Disk (KB) per HyperBEAM node."""
    metrics = {}
    try:
        ps_out = subprocess.check_output(
            ["ps", "-eo", "pid,rss,vsz,pcpu,args"], text=True, timeout=2
        )
    except Exception:
        ps_out = ""

    for node in NODES:
        name = node["name"]
        sname_token = f"--sname {node['sname']}"
        rss_mb = 0.0
        vsz_mb = 0.0
        cpu_pct = 0.0
        pid = None

        for line in ps_out.splitlines():
            if sname_token in line and "tmux" not in line:
                parts = line.split(None, 4)
                if len(parts) >= 4:
                    pid = int(parts[0])
                    rss_mb = round(int(parts[1]) / 1024.0, 1)
                    vsz_mb = round(int(parts[2]) / 1024.0, 1)
                    cpu_pct = round(float(parts[3]), 1)
                break

        mdb_file = os.path.join(DATA_DIR, name, "lmdb", "data.mdb")
        lmdb_kb = round(os.path.getsize(mdb_file) / 1024.0, 1) if os.path.exists(mdb_file) else 0.0

        metrics[name] = {
            "pid": pid,
            "port": node["port"],
            "rss_mb": rss_mb,
            "vsz_mb": vsz_mb,
            "cpu_pct": cpu_pct,
            "lmdb_kb": lmdb_kb,
            "pure_ram": not os.path.exists(mdb_file),
        }
    return metrics


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        if self.path.startswith("/metrics"):
            payload = json.dumps({"status": "ok", "nodes": collect_node_metrics()}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        return super().do_GET()

    def log_message(self, format, *args):
        # Suppress noisy polling logs
        pass


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

if __name__ == "__main__":
    with ThreadedTCPServer(("0.0.0.0", PORT), Handler) as httpd:
        print(f"[Game-UI] Serving Red Circle WASD Client + Telemetry at http://127.0.0.1:{PORT}")
        httpd.serve_forever()
