"""Shared harness for the honest engine tests.

The suite talks to a *live* local cluster. It never fakes replication and it
fails loudly when a node is missing: an engine test that silently skips the
cluster proves nothing.
"""
import json
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tools.client import StateEngineClient  # noqa: E402

PORTS = {"genesis": 8733, "primary": 8734, "sd1": 8735, "sd2": 8736, "arbit": 8737}
TMUX = {"genesis": "hb-genesis", "primary": "hb-primary", "sd1": "hb-sd-1",
        "sd2": "hb-sd-2", "arbit": "hb-arbit"}
CONFIG = {name: os.path.join(PROJECT_ROOT, "engine", "config", name + ".json")
          for name in PORTS}

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


class Checker:
    def __init__(self, name):
        self.name = name
        self.total = 0
        self.failed = 0

    def check(self, cond, label, detail=""):
        self.total += 1
        if cond:
            print("  %s[PASS]%s %s" % (GREEN, RESET, label))
        else:
            self.failed += 1
            print("  %s[FAIL]%s %s %s" % (RED, RESET, label, detail))
            raise AssertionError(label)

    def note(self, text):
        print("  %s%s%s" % (DIM, text, RESET))

    def summary(self):
        ok = self.total - self.failed
        colour = GREEN if self.failed == 0 else RED
        print("\n%s%s: %d/%d checks passed%s" % (colour, self.name, ok, self.total, RESET))
        return self.failed == 0


def http_ready(port, timeout=2):
    try:
        out = subprocess.run(
            ["curl", "-s", "-m", str(timeout), "-o", "/dev/null", "-w", "%{http_code}",
             "http://127.0.0.1:%d/~meta@1.0/info/address" % port],
            capture_output=True, text=True, timeout=timeout + 2)
        return out.stdout.strip() == "200"
    except Exception:
        return False


def node_up(name):
    return http_ready(PORTS[name])


def wait_ready(name, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node_up(name):
            return True
        time.sleep(1)
    return False


def kill_node(name):
    subprocess.run(["tmux", "kill-session", "-t", TMUX[name]],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 20
    while time.time() < deadline:
        if not node_up(name):
            return True
        time.sleep(0.5)
    return False


def start_node(name):
    cmd = (
        "export PATH=\"/home/weslur/erlang/27.0/bin:/home/weslur/.local/bin:/home/weslur/.cargo/bin:$PATH\" && "
        "export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && "
        "cd \"%s/HyperBEAM\" && HB_PORT=\"%d\" HB_CONFIG=\"%s\" rebar3 shell --sname %s"
        % (PROJECT_ROOT, PORTS[name], CONFIG[name], TMUX[name])
    )
    subprocess.run(["tmux", "new-session", "-d", "-s", TMUX[name], "bash -c '%s'" % cmd],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return wait_ready(name)


def require_cluster(names=("genesis", "primary", "sd1", "sd2", "arbit")):
    missing = [n for n in names if not node_up(n)]
    if missing:
        print("%sCluster is not running; missing: %s%s" % (RED, ", ".join(missing), RESET))
        print("Start it with: ./tools/cluster_up.sh && FRESH=1 ./tools/apply_engine.sh")
        sys.exit(2)


def replica_state(client, port, proc):
    code, body, _ = client.request(
        "POST", "http://127.0.0.1:%d/~node-process@1.0/%s/push/output~message@1.0"
        % (port, proc), {"action": "Hydrate"})
    return code, body


class Contract:
    """The invariants that must never be violated (ROADMAP section 3)."""

    def __init__(self, client, checker):
        self.client = client
        self.checker = checker

    def state(self):
        return self.client.read_all_points()

    def roots(self):
        out = {"primary": self.state().get("state_root")}
        for proc in ("sd1", "sd2"):
            _, body = replica_state(self.client, PORTS[proc], proc)
            out[proc] = body.get("state_root")
        return out

    def wait_roots_equal(self, timeout=20.0, poll=0.25):
        """Wait until every node reports the same root.

        Replication of the hot standby is asynchronous (the Primary casts the
        confirmed delta to SD2), so equality is an *eventual* invariant: a test
        may wait for it, but it may never assume it without observing it.
        """
        deadline = time.time() + timeout
        roots = None
        while time.time() < deadline:
            roots = self.roots()
            if len(set(roots.values())) == 1 and None not in roots.values():
                return roots
            time.sleep(poll)
        return roots

    def assert_roots_equal(self, label="state roots identical on primary, sd1, sd2",
                           timeout=20.0):
        roots = self.wait_roots_equal(timeout=timeout)
        ok = len(set(roots.values())) == 1 and None not in roots.values()
        self.checker.check(ok, label, json.dumps(roots))
        return roots

    def assert_tick_stable(self, before, label):
        after = self.state()
        self.checker.check(after.get("tick") == before,
                           label + " (tick %s -> %s)" % (before, after.get("tick")))

    def wait_confirmed(self, batch_id, timeout=20):
        return self.client.wait_for_batch(batch_id, timeout=timeout)
