#!/usr/bin/env python3
"""
=============================================================================
High-Performance State Engine Client Interface
=============================================================================
This client adheres strictly to the architectural boundary:
1. It connects exclusively to the Genesis Process for Service Discovery.
2. It interacts directly ONLY with the discovered Primary Process.
3. It uses HyperBEAM's native single-hop `/push/output~message@1.0/body`
   resolution to schedule, compute, replicate (with AO cryptographic signing),
   and return verified SD feedback in a single HTTP round-trip.
=============================================================================
"""

import sys
import json
import urllib.request
import urllib.error
from typing import Dict, Any, Optional


class StateEngineClient:
    """Client interface for interacting with the HyperBEAM Real-time State Engine."""

    def __init__(self, genesis_url: str = "http://127.0.0.1:8733"):
        self.genesis_url = genesis_url.rstrip("/")
        self.primary_url: Optional[str] = None
        self.epoch: Optional[int] = None

    def _unwrap_ao_body(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, dict) and data.get("ao-result") == "body" and isinstance(data.get("body"), str):
            return json.loads(data["body"])
        if isinstance(data, str):
            return json.loads(data)
        return data

    NO_CACHE_HEADERS = {
        "Cache-Control": "no-cache, no-store",
        "Pragma": "no-cache",
    }

    def _http_get_json(self, url: str) -> Dict[str, Any]:
        headers = {"Accept": "application/json"}
        headers.update(self.NO_CACHE_HEADERS)
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = resp.read().decode("utf-8")
            return json.loads(data)

    def _http_post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        data_bytes = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        headers.update(self.NO_CACHE_HEADERS)
        req = urllib.request.Request(
            url,
            data=data_bytes,
            headers=headers
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = resp.read().decode("utf-8")
            return json.loads(data)

    def _fetch_process_now_body(self, base_url: str, process_name: str) -> Dict[str, Any]:
        """Resolves the HyperBEAM process /now/results~message@1.0/output/body in 1 hop."""
        body_url = f"{base_url}/~node-process@1.0/{process_name}/now/results~message@1.0/output/body"
        data = self._http_get_json(body_url)
        return self._unwrap_ao_body(data)

    def _push_process_body(self, base_url: str, process_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Single-hop schedule + compute + output resolution via HyperBEAM `dev_push`.
        Eliminates the extra client HTTP round-trip.
        """
        push_url = f"{base_url}/~node-process@1.0/{process_name}/push/output~message@1.0/body"
        data = self._http_post_json(push_url, payload)
        return self._unwrap_ao_body(data)

    def discover_primary(self) -> str:
        """Queries Genesis process to discover the active Primary Process URL."""
        body = self._fetch_process_now_body(self.genesis_url, "genesis")
        primary_url = body.get("primary_url") or (body.get("primary", {}) if isinstance(body.get("primary"), dict) else {}).get("url")
        if primary_url:
            self.primary_url = primary_url.rstrip("/")
            self.epoch = body.get("epoch", 1)
            return self.primary_url
        raise RuntimeError(f"Genesis did not report a primary_url: {body}")

    def write_point(self, point_id: str, x: float, y: float) -> Dict[str, Any]:
        """
        Writes a 2D coordinate point to the Primary Process in 1 single HTTP `/push` hop.
        Returns feedback ONLY after Primary + Shared Data (SD1 & SD2) confirm
        cryptographically signed execution + persistence.
        """
        if not self.primary_url:
            self.discover_primary()

        payload = {
            "action": "Write-Point",
            "id": str(point_id),
            "x": float(x),
            "y": float(y)
        }

        now_body = self._push_process_body(self.primary_url, "primary", payload)
        pt = now_body.get("point", payload)
        return {
            "status": now_body.get("status", "error"),
            "assignment_slot": pt.get("tick"),
            "assignment_id": now_body.get("assignment_id", ""),
            "requires_sign": now_body.get("requires_sign", False),
            "primary_signer": now_body.get("primary_signer", ""),
            "sd_signers": now_body.get("sd_signers", []),
            "point": pt,
            "tier": now_body.get("tier", "primary_ram"),
            "sd_ack": now_body.get("sd_ack", False),
            "sd_acked_nodes": now_body.get("sd_acked_nodes", []),
            "sd_confirmed_tick": now_body.get("sd_confirmed_tick", 0),
            "sd_persistence": now_body.get("sd_persistence", ""),
            "active_entity_count": now_body.get("active_entity_count", 0),
            "audit_ring_size": now_body.get("audit_ring_size", 0),
            "max_entities": now_body.get("max_entities", 0),
        }

    def read_point(self, point_id: str) -> Dict[str, Any]:
        """Reads a specific 2D coordinate point from the Primary Process in 1 hop."""
        if not self.primary_url:
            self.discover_primary()

        payload = {
            "action": "Read-Point",
            "id": str(point_id)
        }
        return self._push_process_body(self.primary_url, "primary", payload)

    def read_all_points(self) -> Dict[str, Any]:
        """Reads all coordinate points currently held in Primary RAM in 1 hop."""
        if not self.primary_url:
            self.discover_primary()

        payload = {
            "action": "Read-All"
        }
        return self._push_process_body(self.primary_url, "primary", payload)


def main():
    if len(sys.argv) < 2:
        print("Usage: client.py [discover | write <id> <x> <y> | read <id> | read-all]")
        sys.exit(1)

    cmd = sys.argv[1].lower()
    client = StateEngineClient()

    if cmd == "discover":
        url = client.discover_primary()
        print(f"[OK] Discovered Primary Node: {url} (Epoch: {client.epoch})")
    elif cmd == "write":
        if len(sys.argv) < 5:
            print("Usage: client.py write <id> <x> <y>")
            sys.exit(1)
        pid, x, y = sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
        res = client.write_point(pid, x, y)
        print(f"[OK] Wrote point: {json.dumps(res, indent=2)}")
    elif cmd == "read":
        if len(sys.argv) < 3:
            print("Usage: client.py read <id>")
            sys.exit(1)
        pid = sys.argv[2]
        res = client.read_point(pid)
        print(f"[OK] Point data: {json.dumps(res, indent=2)}")
    elif cmd == "read-all":
        res = client.read_all_points()
        print(f"[OK] All points: {json.dumps(res, indent=2)}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
