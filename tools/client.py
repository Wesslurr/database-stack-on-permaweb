#!/usr/bin/env python3
"""Client for the HyperStack engine, matching the honest ACK contract.

* ``write_point`` returns the *accepted* response: HTTP 202 with
  ``ack: "accepted"``. That answer never claims replication.
* ``write_point_confirmed`` additionally waits for the verified 200
  (``ack: "replicated"``), which the primary only emits after a replica's
  Ack echoed the batch id, tick and canonical root.
* Quorum failure is reported as HTTP 503 ``quorum_unavailable``; the state
  is unchanged in that case.
"""
import argparse
import hashlib
import http.client
import json
import os
import time
import urllib.parse

DEFAULT_PRIMARY = "http://127.0.0.1:8734"
DEFAULT_GENESIS = "http://127.0.0.1:8733"


class StateEngineClient:
    def __init__(self, genesis_url=DEFAULT_GENESIS, primary_url=None, client_signer=None):
        self.genesis_url = genesis_url.rstrip("/")
        self.primary_url = primary_url.rstrip("/") if primary_url else None
        self.epoch = None
        self.client_signer = client_signer or os.environ.get(
            "CLIENT_SIGNER",
            "client_" + hashlib.sha256(b"local_default_identity").hexdigest()[:16],
        )
        self._conns = {}

    # -- transport -----------------------------------------------------------
    def _conn(self, netloc):
        if netloc not in self._conns:
            self._conns[netloc] = http.client.HTTPConnection(netloc, timeout=15)
        return self._conns[netloc]

    def request(self, method, url, body=None):
        parsed = urllib.parse.urlparse(url)
        path = parsed.path + (("?" + parsed.query) if parsed.query else "")
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Accept": "application/json", "Cache-Control": "no-store"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        last = None
        for attempt in range(2):
            try:
                conn = self._conn(parsed.netloc)
                conn.request(method, path, body=data, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                return resp.status, self._unwrap(raw), raw
            except Exception as exc:  # reconnect once
                last = exc
                try:
                    self._conns.pop(parsed.netloc).close()
                except Exception:
                    pass
        raise RuntimeError("request failed: %s" % last)

    @staticmethod
    def _unwrap(raw):
        try:
            outer = json.loads(raw)
        except Exception:
            return {"raw": raw[:500].decode("utf8", "replace")}
        inner = outer.get("body", outer) if isinstance(outer, dict) else outer
        for _ in range(2):
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    return {"raw": inner}
            else:
                break
        return inner if isinstance(inner, dict) else {"body": inner}

    # -- process helpers -----------------------------------------------------
    def _push(self, base, proc, payload):
        return self.request(
            "POST", "%s/~node-process@1.0/%s/push/output~message@1.0" % (base, proc), payload)

    def _now(self, base, proc):
        return self.request(
            "GET", "%s/~node-process@1.0/%s/now/results~message@1.0/output/body" % (base, proc))

    def discover_primary(self):
        code, body, _ = self._now(self.genesis_url, "genesis")
        primary = body.get("primary_url") if isinstance(body, dict) else None
        if not primary:
            code, body, _ = self._push(self.genesis_url, "genesis", {"action": "Get-Topology"})
            primary = body.get("primary_url")
        if not primary:
            raise RuntimeError("genesis did not report a primary_url: %s" % body)
        self.primary_url = primary.rstrip("/")
        self.epoch = body.get("epoch", 1)
        return self.primary_url

    def _ensure_primary(self):
        if not self.primary_url:
            self.discover_primary()
        return self.primary_url

    # -- writes --------------------------------------------------------------
    def write_point(self, point_id, x, y, client_signer=None, ts=None, flush=True,
                    idempotency_key=None):
        """L0: enqueue a write. Returns the 202 body (never a replication claim)."""
        primary = self._ensure_primary()
        payload = {
            "action": "Input",
            "id": str(point_id),
            "x": float(x),
            "y": float(y),
            "client_signer": client_signer or self.client_signer,
        }
        if ts is not None:
            payload["ts"] = int(ts)
        if flush:
            payload["flush"] = True
        if idempotency_key:
            payload["idempotency_key"] = idempotency_key
        code, body, _ = self._push(primary, "primary", payload)
        return {
            "http_status": code,
            "status": body.get("status"),
            "ack": body.get("ack"),
            "batch_id": body.get("batch_id"),
            "tick": body.get("tick"),
            "tick_to": body.get("tick_to"),
            "state_root": body.get("state_root"),
            "target_root": body.get("target_root"),
            "replicas": body.get("replicas"),
            "raw": body,
        }

    def batch_status(self, batch_id):
        primary = self._ensure_primary()
        code, body, _ = self._push(primary, "primary", {
            "action": "Batch-Status", "batch_id": batch_id})
        body["http_status"] = code
        return body

    def wait_for_batch(self, batch_id, timeout=15.0, poll=0.02, status_every=0.5):
        """Poll /now for the fast path and Batch-Status for the verdict.

        ``/now`` is slot-free and never advances the Primary's request clock,
        so a client that only reads ``/now`` can never trigger the background
        retry pump. Batch-Status is a real request: it advances the clock, lets
        the Primary re-cast a stalled batch and returns the honest verdict
        (200 replicated / 202 pending / 503 quorum unavailable / 404 unknown).
        """
        primary = self._ensure_primary()
        deadline = time.time() + timeout
        next_status = 0.0
        last = None
        while time.time() < deadline:
            now = time.time()
            if now >= next_status:
                last = self.batch_status(batch_id)
                if last.get("http_status") in (200, 404):
                    return last
                next_status = now + status_every
            code, body, _ = self._now(primary, "primary")
            if body.get("batch_id") == batch_id and body.get("ack") == "replicated":
                return self.batch_status(batch_id)
            time.sleep(poll)
        return last if last is not None else self.batch_status(batch_id)

    def write_point_confirmed(self, point_id, x, y, **kwargs):
        """L1: write, then wait for the verified 200 (or an honest 503)."""
        accepted = self.write_point(point_id, x, y, **kwargs)
        if accepted["http_status"] == 200:
            return accepted
        if not accepted.get("batch_id"):
            return accepted
        final = self.wait_for_batch(accepted["batch_id"])
        return {
            "http_status": final.get("http_status"),
            "status": final.get("status"),
            "ack": final.get("ack"),
            "batch_id": accepted["batch_id"],
            "tick": final.get("tick"),
            "state_root": final.get("state_root"),
            "replicas": final.get("replicas"),
            "error": final.get("error"),
            "accepted": accepted,
            "raw": final,
        }

    # -- reads ---------------------------------------------------------------
    def read_point(self, point_id):
        primary = self._ensure_primary()
        code, body, _ = self._push(primary, "primary", {"action": "Read-Point", "id": str(point_id)})
        body["http_status"] = code
        return body

    def read_all_points(self):
        primary = self._ensure_primary()
        code, body, _ = self._push(primary, "primary", {"action": "Read-All"})
        body["http_status"] = code
        return body

    def snapshot(self):
        primary = self._ensure_primary()
        code, body, _ = self._push(primary, "primary", {"action": "Snapshot"})
        body["http_status"] = code
        return body

    def read_replica(self, base, proc="sd1"):
        code, body, _ = self._now(base, proc)
        body["http_status"] = code
        return body


def main():
    parser = argparse.ArgumentParser(description="HyperStack client")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("discover")
    w = sub.add_parser("write")
    w.add_argument("id")
    w.add_argument("x", type=float)
    w.add_argument("y", type=float)
    w.add_argument("--wait", action="store_true", help="wait for verified replication")
    r = sub.add_parser("read")
    r.add_argument("id")
    sub.add_parser("read-all")
    sub.add_parser("snapshot")
    args = parser.parse_args()

    client = StateEngineClient()
    if args.cmd == "discover":
        print(json.dumps({"primary_url": client.discover_primary(), "epoch": client.epoch}, indent=2))
    elif args.cmd == "write":
        fn = client.write_point_confirmed if args.wait else client.write_point
        print(json.dumps(fn(args.id, args.x, args.y), indent=2))
    elif args.cmd == "read":
        print(json.dumps(client.read_point(args.id), indent=2))
    elif args.cmd == "read-all":
        print(json.dumps(client.read_all_points(), indent=2))
    elif args.cmd == "snapshot":
        print(json.dumps(client.snapshot(), indent=2))


if __name__ == "__main__":
    main()
