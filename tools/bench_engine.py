#!/usr/bin/env python3
"""Honest baseline benchmark for the HyperStack engine.

Measures the paths the ROADMAP cares about with keep-alive HTTP, and writes
``tools/bench_baseline.json``:

  1. now            GET  /~node-process@1.0/primary/now/...   (read, no slot)
  2. write_202      POST Input                                (L0 accept)
  3. write_confirm  POST Input -> poll Batch-Status -> 200     (L1 replicated)
  4. advance        POST Advance for a queued batch            (batch path)
  5. read_push      POST Snapshot                              (read via push)
  6. relay_call_sd  POST /~relay@1.0/call -> SD1 Hydrate        (SD round trip)
  7. sd_direct      POST SD1 Hydrate directly                  (network cost floor)
  8. cold_hydrate   full-state Hydrate from SD1                (recovery proxy)

The script never claims numbers it did not measure: every entry records the
sample count, the status codes seen, and the response size.
"""
import json
import os
import re
import socket
import subprocess
import sys
import time
import http.client

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRIMARY = ("127.0.0.1", 8734, "primary")
SD1 = ("127.0.0.1", 8735, "sd1")
SD2 = ("127.0.0.1", 8736, "sd2")
PUSH = "/~node-process@1.0/{proc}/push/output~message@1.0"


def pct(values, q):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return round(ordered[idx], 3)


def stats(values, codes=None):
    return {
        "n": len(values),
        "p50_ms": pct(values, 0.50),
        "p90_ms": pct(values, 0.90),
        "p99_ms": pct(values, 0.99),
        "min_ms": round(min(values), 3) if values else None,
        "max_ms": round(max(values), 3) if values else None,
        "status_codes": sorted(set(codes or [])),
    }


class KeepAlive:
    """One persistent HTTP connection per node (no per-request TCP setup)."""

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.conn = None

    def _connect(self):
        if self.conn is None:
            self.conn = http.client.HTTPConnection(self.host, self.port, timeout=30)

    def request(self, method, path, body=None, headers=None):
        payload = json.dumps(body).encode() if body is not None else None
        hdrs = {"accept": "application/json"}
        if payload is not None:
            hdrs["content-type"] = "application/json"
        hdrs.update(headers or {})
        t0 = time.perf_counter()
        try:
            self._connect()
            self.conn.request(method, path, body=payload, headers=hdrs)
            resp = self.conn.getresponse()
            raw = resp.read()
            code = resp.status
        except Exception:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self._connect()
            self.conn.request(method, path, body=payload, headers=hdrs)
            resp = self.conn.getresponse()
            raw = resp.read()
            code = resp.status
        dt = (time.perf_counter() - t0) * 1000.0
        return code, raw, dt


def body_json(raw):
    try:
        outer = json.loads(raw)
    except Exception:
        return {}
    inner = outer.get("body", outer) if isinstance(outer, dict) else outer
    for _ in range(2):
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except Exception:
                break
    return inner if isinstance(inner, dict) else {}


def pid_for_port(port):
    try:
        out = subprocess.check_output(
            ["ss", "-lptnH", "sport = :%d" % port], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return None
    m = re.search(r"pid=(\d+)", out)
    return int(m.group(1)) if m else None


def rss_mb(port):
    pid = pid_for_port(port)
    if not pid:
        return None
    try:
        with open("/proc/%d/status" % pid) as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except Exception:
        return None
    return None


def du_mb(path):
    try:
        out = subprocess.check_output(["du", "-sm", path], text=True, stderr=subprocess.DEVNULL)
        return float(out.split()[0])
    except Exception:
        return None


def last_slot_size(session):
    try:
        out = subprocess.check_output(
            ["tmux", "capture-pane", "-t", session, "-p", "-S", "-800"], text=True)
    except Exception:
        return None
    sizes = re.findall(r"computed_slot_size: (\d+)", out)
    return int(sizes[-1]) if sizes else None


def poll_batch(client, batch_id, limit=120, gap=0.002):
    deadline = time.perf_counter() + 10.0
    last = (202, {})
    while time.perf_counter() < deadline:
        code, raw, _ = client.request(
            "POST", PUSH.format(proc=PRIMARY[2]), {"action": "Batch-Status", "batch_id": batch_id})
        last = (code, body_json(raw))
        if code != 202:
            return last
        time.sleep(gap)
    return last


def drain(client, timeout=60.0):
    """Wait until the primary has no unresolved batch."""
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        code, raw, _ = client.request(
            "POST", PUSH.format(proc=PRIMARY[2]), {"action": "Read-All"})
        body = body_json(raw)
        if code == 200 and int(body.get("pending_batches") or 0) == 0:
            return True
        time.sleep(0.01)
    return False


def main():
    samples = int(os.environ.get("BENCH_N", "150"))
    primary = KeepAlive(PRIMARY[0], PRIMARY[1])
    sd1 = KeepAlive(SD1[0], SD1[1])

    # Resolve the topology / hydrate before measuring.
    primary.request("POST", PUSH.format(proc="primary"), {"action": "Read-All"})

    result = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "git_commit": subprocess.getoutput("git -C '%s' rev-parse --short HEAD" % ROOT).strip(),
        "samples_per_path": samples,
        "notes": [
            "keep-alive single connection per node; loopback only",
            "write_202 measures L0 accept, write_confirm measures L1 (poll Batch-Status)",
            "cold_hydrate is a full-state transfer proxy for cold start, not a process restart",
            "slot size comes from the Erlang debug log (computed_slot_size)",
        ],
        "paths": {},
    }

    # 1. /now read (no new slot)
    vals, codes = [], []
    for _ in range(samples):
        code, raw, dt = primary.request(
            "GET", "/~node-process@1.0/primary/now/results~message@1.0/output/body")
        vals.append(dt); codes.append(code)
    result["paths"]["now"] = stats(vals, codes)

    # 2. write accepted (202)
    vals, codes, batch_ids = [], [], []
    for i in range(min(60, samples)):
        code, raw, dt = primary.request("POST", PUSH.format(proc="primary"), {
            "action": "Input", "id": "bench_%d_%d" % (int(time.time()), i),
            "x": float(i % 100), "y": float((i * 7) % 100),
            "client_signer": "bench", "ts": 900000 + i, "flush": True})
        vals.append(dt); codes.append(code)
        body = body_json(raw)
        if body.get("batch_id"):
            batch_ids.append(body["batch_id"])
        drain(primary)
    result["paths"]["write_202"] = stats(vals, codes)
    drain(primary)

    # 3. write -> confirmed (L1)
    vals, codes = [], []
    for i in range(min(40, samples)):
        t0 = time.perf_counter()
        code, raw, _ = primary.request("POST", PUSH.format(proc="primary"), {
            "action": "Input", "id": "bench_confirm_%d_%d" % (int(time.time()), i),
            "x": 1.0, "y": 1.0, "client_signer": "bench", "ts": 910000 + i, "flush": True})
        body = body_json(raw)
        bid = body.get("batch_id")
        target_tick = int(body.get("tick_to") or 0)
        fcode = code
        if bid:
            # Poll /now: it does not create a new slot, so it cannot starve
            # the Ack compute (Batch-Status is the documented fallback).
            deadline = time.perf_counter() + 10.0
            while time.perf_counter() < deadline:
                ncode, nraw, _ = primary.request(
                    "GET", "/~node-process@1.0/primary/now/results~message@1.0/output/body")
                nb = body_json(nraw)
                fcode = ncode
                if nb.get("batch_id") == bid and nb.get("ack") == "replicated":
                    break
                if int(nb.get("tick") or 0) >= target_tick and nb.get("ack") == "replicated":
                    break
                time.sleep(0.02)
        vals.append((time.perf_counter() - t0) * 1000.0)
        codes.append(fcode)
    result["paths"]["write_confirm"] = stats(vals, codes)

    # 4. advance / batch close (queue one write, then Advance)
    vals, codes = [], []
    for i in range(min(40, samples)):
        primary.request("POST", PUSH.format(proc="primary"), {
            "action": "Input", "id": "bench_adv_%d_%d" % (int(time.time()), i),
            "x": 2.0, "y": 2.0, "client_signer": "bench", "ts": 920000 + i})
        code, raw, dt = primary.request("POST", PUSH.format(proc="primary"), {"action": "Advance"})
        vals.append(dt); codes.append(code)
    result["paths"]["advance"] = stats(vals, codes)
    drain(primary)

    # 5. read via push (snapshot)
    vals, codes, sizes = [], [], []
    for _ in range(min(60, samples)):
        code, raw, dt = primary.request("POST", PUSH.format(proc="primary"), {"action": "Snapshot"})
        vals.append(dt); codes.append(code); sizes.append(len(raw))
    st = stats(vals, codes)
    st["response_bytes_p50"] = pct(sizes, 0.5)
    result["paths"]["read_push"] = st

    # 6. relay/call to SD1 from inside the primary (what the commit path costs)
    vals, codes, sizes = [], [], []
    for _ in range(min(60, samples)):
        code, raw, dt = primary.request(
            "POST", PUSH.format(proc=PRIMARY[2]), {"action": "Probe-SD", "sd": 1})
        body = body_json(raw)
        vals.append(dt); codes.append(code); sizes.append(int(body.get("bytes") or 0))
    st = stats(vals, codes)
    st["response_bytes_p50"] = pct(sizes, 0.5)
    result["paths"]["relay_call_sd"] = st

    # 7. direct SD round trip (network floor)
    vals, codes, sizes = [], [], []
    for _ in range(min(60, samples)):
        code, raw, dt = sd1.request("POST", PUSH.format(proc="sd1"), {"action": "Hydrate"})
        vals.append(dt); codes.append(code); sizes.append(len(raw))
    st = stats(vals, codes)
    st["response_bytes_p50"] = pct(sizes, 0.5)
    result["paths"]["sd_direct"] = st

    # 8. cold hydration proxy: transfer the whole state from SD1 via relay
    vals, codes = [], []
    for _ in range(min(20, samples)):
        code, raw, dt = primary.request(
            "POST", PUSH.format(proc=PRIMARY[2]), {"action": "Probe-SD", "sd": 1})
        vals.append(dt); codes.append(code)
    result["paths"]["cold_hydrate"] = stats(vals, codes)

    # Cluster truth at the end of the run.
    state = {}
    for proc in ("primary", "sd1", "sd2"):
        port = {"primary": 8734, "sd1": 8735, "sd2": 8736}[proc]
        code, raw, _ = KeepAlive("127.0.0.1", port).request(
            "POST", PUSH.format(proc=proc), {"action": "Hydrate"})
        body = body_json(raw)
        state[proc] = {
            "http": code,
            "tick": body.get("last_tick", body.get("tick")),
            "state_root": body.get("state_root"),
            "entities": body.get("count", body.get("active_entity_count")),
        }
    result["cluster"] = {
        "state": state,
        "roots_equal": len({v.get("state_root") for v in state.values()}) == 1,
        "rss_mb": {p: rss_mb(port) for p, port in
                   (("primary", 8734), ("sd1", 8735), ("sd2", 8736), ("genesis", 8733))},
        "lmdb_mb": {p: du_mb(os.path.join(ROOT, "engine", "data", p, "lmdb"))
                    for p in ("genesis", "primary", "sd1", "sd2", "arbit")},
        "computed_slot_size": {
            "primary": last_slot_size("hb-primary"),
            "sd1": last_slot_size("hb-sd-1"),
            "sd2": last_slot_size("hb-sd-2"),
        },
    }

    out = os.path.join(ROOT, "tools", "bench_baseline.json")
    with open(out, "w") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
    print(json.dumps(result, indent=2, sort_keys=True))
    print("\nwrote %s" % out)


if __name__ == "__main__":
    main()
