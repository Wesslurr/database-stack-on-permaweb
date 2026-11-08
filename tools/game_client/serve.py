#!/usr/bin/env python3
"""
Lightweight HTTP Server + OS Telemetry Endpoint (`/metrics`) for the
Interactive Red Circle WASD Game Client.
Strictly separated from engine/ (located in tools/game_client/).
"""

import http.client
import http.server
import json
import os
import socketserver
import subprocess
import urllib.request

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
    # ArLocal (Docker Container) Metrics
    try:
        doc_out = subprocess.check_output(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}:{{.MemUsage}}:{{.CPUPerc}}", "arlocal"],
            text=True, timeout=2
        ).strip()
        parts = doc_out.split(":")
        if len(parts) >= 3:
            mem_raw = parts[1].split("/")[0].strip()
            rss_val = 0.0
            if "MiB" in mem_raw:
                rss_val = round(float(mem_raw.replace("MiB", "").strip()), 1)
            elif "GiB" in mem_raw:
                rss_val = round(float(mem_raw.replace("GiB", "").strip()) * 1024, 1)
            elif "KiB" in mem_raw:
                rss_val = round(float(mem_raw.replace("KiB", "").strip()) / 1024, 1)
            cpu_val = round(float(parts[2].replace("%", "").strip()), 1)
            metrics["arlocal"] = {
                "container": "arlocal",
                "port": 1984,
                "rss_mb": rss_val,
                "mem_str": mem_raw,
                "cpu_pct": cpu_val,
                "status": "online"
            }
    except Exception:
        metrics["arlocal"] = {
            "container": "arlocal",
            "port": 1984,
            "rss_mb": 0.0,
            "mem_str": "—",
            "cpu_pct": 0.0,
            "status": "offline"
        }

    return metrics


def proxy_request(handler, port, path, method="GET", body=None, headers=None):
    try:
        conn = http.client.HTTPConnection("127.0.0.1", int(port), timeout=5)
        req_headers = {}
        if headers:
            for k, v in headers.items():
                if k.lower() not in ("host", "content-length", "connection"):
                    req_headers[k] = v
        if body is not None and len(body) > 0:
            req_headers["Content-Length"] = str(len(body))
        conn.request(method, path, body=body, headers=req_headers)
        res = conn.getresponse()
        res_data = res.read()

        handler.send_response(res.status)
        for h, v in res.getheaders():
            if h.lower() not in ("transfer-encoding", "content-encoding", "access-control-allow-origin"):
                handler.send_header(h, v)
        handler.send_header("Access-Control-Allow-Origin", "*")
        handler.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        handler.send_header("Access-Control-Allow-Headers", "*")
        handler.end_headers()
        try:
            handler.wfile.write(res_data)
        except (BrokenPipeError, ConnectionResetError):
            pass
        conn.close()
    except Exception as e:
        err_msg = json.dumps({"error": str(e), "target_port": port}).encode("utf-8")
        try:
            handler.send_response(502)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Access-Control-Allow-Origin", "*")
            handler.end_headers()
            handler.wfile.write(err_msg)
        except (BrokenPipeError, ConnectionResetError):
            pass


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/proxy/"):
            raw = self.path[len("/proxy/"):]
            parts = raw.split("/", 1)
            port = parts[0]
            subpath = "/" + parts[1] if len(parts) > 1 else "/"
            return proxy_request(self, port, subpath, method="GET")

        if self.path.startswith("/metrics"):
            payload = json.dumps({"status": "ok", "nodes": collect_node_metrics()}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        if self.path.startswith("/arweave/info") or self.path.startswith("/l1/info"):
            try:
                req = urllib.request.Request("http://127.0.0.1:1984/info", headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=2) as r:
                    data = r.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    try:
                        self.wfile.write(data)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return
            except Exception as e:
                err = json.dumps({"error": str(e), "status": "offline"}).encode("utf-8")
                try:
                    self.send_response(502)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(err)))
                    self.end_headers()
                    self.wfile.write(err)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return
        try:
            return super().do_GET()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        if self.path.startswith("/proxy/"):
            raw = self.path[len("/proxy/"):]
            parts = raw.split("/", 1)
            port = parts[0]
            subpath = "/" + parts[1] if len(parts) > 1 else "/"
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len) if content_len > 0 else b""
            return proxy_request(self, port, subpath, method="POST", body=body, headers=dict(self.headers))
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        # Suppress noisy polling logs
        pass


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        # Suppress harmless BrokenPipeError and ConnectionResetError on client disconnect
        pass

if __name__ == "__main__":
    with ThreadedTCPServer(("0.0.0.0", PORT), Handler) as httpd:
        print(f"[HyperStack-UI] Serving Red Circle WASD Client + Telemetry at http://127.0.0.1:{PORT}")
        httpd.serve_forever()
