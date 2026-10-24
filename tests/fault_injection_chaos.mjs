#!/usr/bin/env node
/**
 * HyDB Phase 05: Real Multi-Process Chaos & Fault-Injection Verification Suite
 * ============================================================================
 * Normative Requirements (Phase 05):
 * 1. Real Separated OS Processes:
 *    - Spawns real standalone Erlang OS processes for Hot Node and SD Worker
 *      via `tests/chaos_node_daemon.escript` (with real OS PIDs).
 * 2. Dual Network Fault Injectors:
 *    - HTTP Chaos Proxy (Client -> :18744 -> Hot Node :18734):
 *      Injects latency jitter, packet drops, dropped HTTP responses after
 *      server commit, and full client-to-hot partitions.
 *      Strictly verifies that all client test traffic traverses the proxy.
 *    - TCP Replication Chaos Proxy (SD Worker -> :18746 -> Hot TCP Hub :18736):
 *      Severs the live replication stream between Hot Node and SD Worker,
 *      proving that no durable ACK is issued when durability conditions fail.
 * 3. Six Mandatory Real Failure Scenarios:
 *    - Scenario 1: Packet loss & jitter (Client -> Hot via HTTP Proxy)
 *    - Scenario 2: Gangguan Hot -> SD (Sever real TCP replication stream)
 *    - Scenario 3: SD mati lalu hidup (Real OS SIGKILL & restart of SD Worker)
 *    - Scenario 4: Hot mati lalu hidup (Real OS SIGKILL & restart of Hot Node + state reconciliation)
 *    - Scenario 5: Retry setelah respons hilang (Drop HTTP response after commit, retry with same request_id)
 *    - Scenario 6: Epoch / fencing (Epoch promotion, stale epoch write rejection & SD fencing)
 * 4. Independent Verification Oracle:
 *    - Audits not only HTTP responses, but also Hot RAM state, SD RAM state,
 *      disk WAL (`wal.log`), gapless sequence continuity, cryptographic event
 *      hash chain, and 64-bit Merkle-XOR `state_root` convergence.
 */

import http from 'node:http';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { readFileSync, existsSync, mkdirSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';
import { tmpdir } from 'node:os';
import crypto from 'node:crypto';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const DAEMON_SCRIPT = join(__dirname, 'chaos_node_daemon.escript');

const TEST_TMP_DIR = join(tmpdir(), `hydb_chaos_${process.pid}_${Date.now()}`);
mkdirSync(TEST_TMP_DIR, { recursive: true });
const WAL_PATH = join(TEST_TMP_DIR, 'wal.log');
const HOT_WAL_PATH = join(TEST_TMP_DIR, 'hot_wal.log');
const HOT_EPOCH_PATH = join(TEST_TMP_DIR, 'hot_epoch.meta');
const MEMBERSHIP_PATH = join(TEST_TMP_DIR, 'cluster_membership.json');

const REAL_HOT_PORT = parseInt(process.env.CHAOS_HOT_PORT || '18734', 10);
const PROXY_PORT = parseInt(process.env.CHAOS_PROXY_PORT || '18744', 10);
const REAL_HOT_TCP_PORT = parseInt(process.env.CHAOS_HOT_TCP_PORT || '18736', 10);
const TCP_PROXY_PORT = parseInt(process.env.CHAOS_TCP_PROXY_PORT || '18746', 10);
const SD_HTTP_PORT = parseInt(process.env.CHAOS_SD_PORT || '18735', 10);

const HOT_DIRECT_URL = `http://127.0.0.1:${REAL_HOT_PORT}/~hymessage@1.0`;
const PROXY_URL = `http://127.0.0.1:${PROXY_PORT}/~hymessage@1.0`;
const SD_URL = `http://127.0.0.1:${SD_HTTP_PORT}/~scheduler@1.0`;

const CLUSTER_SECRET_RAW = 'hydb-phase05-chaos-secret-key-2026';
const ADMIN_TOKEN = crypto.createHash('sha256').update(CLUSTER_SECRET_RAW).digest('hex').toUpperCase();

const isTrue = (v) => v === true || String(v).toLowerCase() === 'true';

// Deterministic Mulberry32 PRNG
function mulberry32(a) {
  return function () {
    let t = (a += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const SEED = parseInt(process.env.CHAOS_SEED || '1337', 10);
const rng = mulberry32(SEED);

// ============================================================================
// 1. Client -> Hot HTTP Chaos Fault Injector (:18744 -> :18734)
// ============================================================================
const HttpChaos = {
  mode: 'pass', // 'pass' | 'jitter' | 'loss' | 'drop_response' | 'partition'
  minJitterMs: 12,
  maxJitterMs: 45,
  lossProbability: 0.45,
  dropNextResponses: 0,
  stats: {
    totalProxied: 0,
    forwardedToHot: 0,
    jitterInjected: 0,
    packetsDropped: 0,
    responsesDropped: 0,
    partitionBlocked: 0
  }
};

function startHttpChaosProxy() {
  const server = http.createServer((clientReq, clientRes) => {
    HttpChaos.stats.totalProxied++;

    if (HttpChaos.mode === 'partition') {
      HttpChaos.stats.partitionBlocked++;
      clientReq.destroy(new Error('HttpChaosProxy: Network Partition Injected'));
      return;
    }

    if (HttpChaos.mode === 'loss' && rng() < HttpChaos.lossProbability) {
      HttpChaos.stats.packetsDropped++;
      clientReq.destroy(new Error('HttpChaosProxy: Packet Loss Injected'));
      return;
    }

    const delayMs =
      HttpChaos.mode === 'jitter'
        ? Math.floor(HttpChaos.minJitterMs + rng() * (HttpChaos.maxJitterMs - HttpChaos.minJitterMs))
        : 0;

    if (delayMs > 0) {
      HttpChaos.stats.jitterInjected++;
    }

    const bodyChunks = [];
    clientReq.on('data', (c) => bodyChunks.push(c));
    clientReq.on('end', () => {
      const reqBody = Buffer.concat(bodyChunks);
      setTimeout(() => {
        HttpChaos.stats.forwardedToHot++;
        const headers = { ...clientReq.headers, host: `127.0.0.1:${REAL_HOT_PORT}` };
        const proxyReq = http.request(
          {
            hostname: '127.0.0.1',
            port: REAL_HOT_PORT,
            path: clientReq.url,
            method: clientReq.method,
            headers
          },
          (realRes) => {
            const resChunks = [];
            realRes.on('data', (c) => resChunks.push(c));
            realRes.on('end', () => {
              const fullRes = Buffer.concat(resChunks);
              // Fault mode: request executed & committed on Hot Node, but HTTP response is lost in transit!
              if (HttpChaos.mode === 'drop_response' && HttpChaos.dropNextResponses > 0) {
                HttpChaos.dropNextResponses--;
                HttpChaos.stats.responsesDropped++;
                clientRes.destroy(new Error('HttpChaosProxy: Response Dropped After Commit'));
                return;
              }
              clientRes.writeHead(realRes.statusCode, realRes.headers);
              clientRes.end(fullRes);
            });
          }
        );

        proxyReq.on('error', (err) => {
          if (!clientRes.headersSent) {
            clientRes.writeHead(502, { 'Content-Type': 'application/json' });
            clientRes.end(JSON.stringify({ ok: false, status: 502, error: 'Upstream Hot Node unreachable', details: err.message }));
          }
        });

        if (reqBody.length > 0) proxyReq.write(reqBody);
        proxyReq.end();
      }, delayMs);
    });
  });

  return new Promise((resolveReady) => {
    server.listen(PROXY_PORT, '127.0.0.1', () => resolveReady(server));
  });
}

// ============================================================================
// 2. Hot <-> SD TCP Replication Chaos Proxy (:18746 -> :18736)
// ============================================================================
const TcpChaos = {
  mode: 'pass', // 'pass' | 'sever'
  activePairs: new Set(),
  stats: {
    totalConnections: 0,
    rejectedConnections: 0,
    severedConnections: 0,
    bytesHotToSd: 0,
    bytesSdToHot: 0
  },
  setMode(newMode) {
    this.mode = newMode;
    if (newMode === 'sever') {
      for (const pair of this.activePairs) {
        this.stats.severedConnections++;
        pair.sdSock.destroy();
        pair.hotSock.destroy();
      }
      this.activePairs.clear();
    }
  }
};

function startTcpChaosProxy() {
  const server = net.createServer((sdSock) => {
    TcpChaos.stats.totalConnections++;
    if (TcpChaos.mode === 'sever') {
      TcpChaos.stats.rejectedConnections++;
      sdSock.destroy();
      return;
    }

    const hotSock = net.connect(REAL_HOT_TCP_PORT, '127.0.0.1');
    const pair = { sdSock, hotSock };
    TcpChaos.activePairs.add(pair);

    sdSock.setNoDelay(true);
    hotSock.setNoDelay(true);

    hotSock.on('data', (chunk) => {
      if (TcpChaos.mode === 'sever') {
        sdSock.destroy();
        hotSock.destroy();
        return;
      }
      TcpChaos.stats.bytesHotToSd += chunk.length;
      sdSock.write(chunk);
    });

    sdSock.on('data', (chunk) => {
      if (TcpChaos.mode === 'sever') {
        sdSock.destroy();
        hotSock.destroy();
        return;
      }
      TcpChaos.stats.bytesSdToHot += chunk.length;
      hotSock.write(chunk);
    });

    const cleanup = () => {
      TcpChaos.activePairs.delete(pair);
      sdSock.destroy();
      hotSock.destroy();
    };

    sdSock.on('error', cleanup);
    sdSock.on('close', cleanup);
    hotSock.on('error', cleanup);
    hotSock.on('close', cleanup);
  });

  return new Promise((resolveReady) => {
    server.listen(TCP_PROXY_PORT, '127.0.0.1', () => resolveReady(server));
  });
}

// ============================================================================
// 3. Real OS Process Manager for Hot Node & SD Worker
// ============================================================================
class NodeProcessManager {
  constructor() {
    this.procs = { hot: null, sd: null };
  }

  async startNode(role, extraEnv = {}) {
    if (this.procs[role]) {
      await this.killNode(role, 'SIGKILL');
    }
    const port = role === 'hot' ? REAL_HOT_PORT : SD_HTTP_PORT;
    const env = {
      ...process.env,
      DB_DIR,
      WALLET_PATH: join(TEST_TMP_DIR, 'nonexistent_wallet.json'),
      HYDB_CLUSTER_SECRET: CLUSTER_SECRET_RAW,
      HYDB_PROFILE: 'test',
      HYDB_WAL_PATH: WAL_PATH,
      ...(role === 'hot'
        ? {
            HYDB_HOT_WAL_PATH: HOT_WAL_PATH,
            HYDB_HOT_EPOCH_PATH: HOT_EPOCH_PATH,
            HYDB_MEMBERSHIP_PATH: MEMBERSHIP_PATH
          }
        : {}),
      HOT_TCP_PORT: String(REAL_HOT_TCP_PORT),
      HOT_NODE_HOST: '127.0.0.1',
      HOT_NODE_PORT: String(TCP_PROXY_PORT),
      HYDB_CLUSTER_SIZE: '1',
      HYDB_CLUSTER_MEMBERS: 'hb_su',
      ...extraEnv
    };

    return new Promise((resolveStart, rejectStart) => {
      const child = spawn('escript', [DAEMON_SCRIPT, role, String(port)], {
        cwd: DB_DIR,
        env,
        stdio: ['ignore', 'pipe', 'pipe']
      });

      this.procs[role] = child;
      let readyResolved = false;
      let stderrBuf = '';

      const timeout = setTimeout(() => {
        if (!readyResolved) {
          rejectStart(new Error(`Timeout waiting for ${role} OS process (pid=${child.pid}) to become READY. Stderr: ${stderrBuf}`));
        }
      }, 10000);

      child.stdout.on('data', (data) => {
        const text = data.toString();
        if (!readyResolved && text.includes(`READY:${role}:${port}`)) {
          readyResolved = true;
          clearTimeout(timeout);
          resolveStart(child);
        }
      });

      child.stderr.on('data', (data) => {
        stderrBuf += data.toString();
        process.stderr.write(`[${role}-stderr] ${data.toString()}`);
      });

      child.on('exit', (code, signal) => {
        if (this.procs[role] === child) {
          this.procs[role] = null;
        }
        if (!readyResolved) {
          clearTimeout(timeout);
          rejectStart(new Error(`Node ${role} exited prematurely (code=${code}, sig=${signal}): ${stderrBuf}`));
        }
      });
    });
  }

  async killNode(role, signal = 'SIGKILL') {
    const child = this.procs[role];
    if (!child) return;
    const pid = child.pid;
    return new Promise((resolveKill) => {
      child.once('exit', () => {
        this.procs[role] = null;
        resolveKill(pid);
      });
      try {
        process.kill(pid, signal);
      } catch {
        this.procs[role] = null;
        resolveKill(pid);
      }
    });
  }

  async stopAll() {
    await this.killNode('sd', 'SIGKILL');
    await this.killNode('hot', 'SIGKILL');
  }
}

// ============================================================================
// 4. Independent Verification Oracle (64-bit Merkle-XOR + Hash Chain + State)
// ============================================================================
function canonicalHash64(keyInt, valInt, verInt) {
  const mask64 = 0xffffffffffffffffn;
  const prime = 0x100000001b3n;
  const ukey = BigInt(keyInt) & mask64;
  const uval = BigInt(valInt) & mask64;
  const uver = BigInt(verInt) & mask64;

  let h = 0xcbf29ce484222325n;
  h = ((h ^ ukey) * prime) & mask64;
  h = ((h ^ uval) * prime) & mask64;
  h = ((h ^ uver) * prime) & mask64;
  h = (h ^ (h >> 30n)) & mask64;
  h = (h * 0xbf58476d1ce4e5b9n) & mask64;
  h = (h ^ (h >> 27n)) & mask64;
  h = (h * 0x94d049bb133111ebn) & mask64;
  h = (h ^ (h >> 31n)) & mask64;
  return BigInt.asIntN(64, h);
}

function computeLegacyEventHash(ev, prevHash) {
  const seqStr = String(ev.seq);
  const op = ev.action || ev.op || 'put';
  const ns = ev.namespace || 'default';
  const valType = ev.val_type || 'int64';
  const keyStr = String(ev.key ?? 1);
  const canonVal = ev.canonical_val !== undefined ? String(ev.canonical_val) : String(ev.val ?? 0);
  const opCode = op === 'register_schema' || op === 'register' ? '24' : op === 'del' ? '22' : '20';

  let payload;
  if (opCode === '24') {
    payload = `${prevHash}${seqStr}24${ns}:${canonVal}`;
  } else if (ns === 'default' && opCode === '20' && valType === 'int64') {
    payload = `${prevHash}${seqStr}20${keyStr}${canonVal}`;
  } else if (ns === 'default' && opCode === '22') {
    payload = `${prevHash}${seqStr}22${keyStr}`;
  } else if (opCode === '22') {
    payload = `${prevHash}${seqStr}22${ns}:${keyStr}`;
  } else {
    payload = `${prevHash}${seqStr}20${ns}:${keyStr}:${canonVal}`;
  }
  return crypto.createHash('sha256').update(payload).digest('hex').toUpperCase();
}

class VerificationOracle {
  constructor() {
    this.kv = new Map(); // keyInt -> { val, ver, active }
    this.durableSeqs = new Set();
    this.acceptedSeqs = new Set();
    this.expectedRoot = 0n;
  }

  recordWrite(key, val, resp) {
    if (!resp) return;
    const seq = Number(resp.seq);
    const k = Number(key);
    const v = Number(val);

    if (resp.idempotent_replay) {
      if (resp.write_status === 'durable') {
        this.durableSeqs.add(seq);
      }
      return;
    }

    if (seq > 0) {
      const prev = this.kv.get(k);
      let newVer = 1;
      let oldHash = 0n;
      if (prev && prev.active) {
        oldHash = canonicalHash64(k, prev.val, prev.ver);
        newVer = prev.ver + 1;
      } else if (prev && !prev.active) {
        newVer = prev.ver + 1;
      }
      const newHash = canonicalHash64(k, v, newVer);
      this.expectedRoot = BigInt.asIntN(64, (this.expectedRoot ^ oldHash ^ newHash) & 0xffffffffffffffffn);
      this.kv.set(k, { val: v, ver: newVer, active: true });

      if (resp.write_status === 'durable') {
        this.durableSeqs.add(seq);
      } else if (resp.write_status === 'accepted') {
        this.acceptedSeqs.add(seq);
      }
    }
  }

  async auditFullCluster(assertFn, label) {
    // 1. Audit disk WAL integrity, sequence continuity, hash chain, and incremental state_root
    assertFn(existsSync(WAL_PATH), `[Oracle:${label}] Disk WAL file exists`);
    const lines = readFileSync(WAL_PATH, 'utf8')
      .split('\n')
      .map((l) => l.trim())
      .filter(Boolean);

    let prevHash = '0000000000000000000000000000000000000000000000000000000000000000';
    let walSeqs = new Set();
    let walRoot = 0n;
    const walKv = new Map();

    for (let idx = 0; idx < lines.length; idx++) {
      const ev = JSON.parse(lines[idx]);
      const expectedSeq = idx + 1;
      assertFn(Number(ev.seq) === expectedSeq, `[Oracle:${label}] WAL gapless sequence continuity at seq=${expectedSeq}`);
      assertFn(
        String(ev.prev_hash).toUpperCase() === prevHash.toUpperCase(),
        `[Oracle:${label}] WAL cryptographic prev_hash chain intact at seq=${expectedSeq}`
      );
      const calcHash = computeLegacyEventHash(ev, prevHash);
      assertFn(
        String(ev.hash).toUpperCase() === calcHash,
        `[Oracle:${label}] WAL event hash verified at seq=${expectedSeq}`
      );

      const k = Number(ev.key);
      const v = Number(ev.val);
      const ver = Number(ev.ver || 1);
      const oldRec = walKv.get(k);
      const oldH = oldRec ? canonicalHash64(k, oldRec.val, oldRec.ver) : 0n;
      const newH = canonicalHash64(k, v, ver);
      walRoot = BigInt.asIntN(64, (walRoot ^ oldH ^ newH) & 0xffffffffffffffffn);
      walKv.set(k, { val: v, ver });

      const evRootStr = ev.state_root_str !== undefined ? String(ev.state_root_str) : String(ev.state_root);
      assertFn(
        BigInt(evRootStr) === walRoot,
        `[Oracle:${label}] WAL event state_root matches recomputed 64-bit Merkle-XOR root at seq=${expectedSeq}`
      );

      prevHash = ev.hash;
      walSeqs.add(expectedSeq);
    }

    // 2. Every write ACKed as durable MUST physically exist in disk WAL
    for (const dSeq of this.durableSeqs) {
      assertFn(walSeqs.has(dSeq), `[Oracle:${label}] Durable ACK seq=${dSeq} physically present in disk WAL`);
    }

    // 3. Compare Hot Node state, SD Worker state, and Oracle state_root
    const hotState = await httpFetch(`${PROXY_URL}/state`);
    const sdState = await httpFetch(`${SD_URL}/state`);
    assertFn(hotState.status === 200 && isTrue(hotState.body.ok), `[Oracle:${label}] Hot /state reachable via proxy`);
    assertFn(sdState.status === 200 && isTrue(sdState.body.ok), `[Oracle:${label}] SD /state reachable`);

    assertFn(
      Number(hotState.body.seq) === lines.length,
      `[Oracle:${label}] Hot seq (${hotState.body.seq}) === WAL length (${lines.length})`
    );
    assertFn(
      Number(sdState.body.tick) === lines.length,
      `[Oracle:${label}] SD tick (${sdState.body.tick}) === WAL length (${lines.length})`
    );
    const hotRootStr = hotState.body.db_root_hash_str || hotState.body.state_root_str || String(hotState.body.db_root_hash);
    const sdRootStr = sdState.body.state_root_str || String(sdState.body.state_root);
    assertFn(
      BigInt(hotRootStr) === this.expectedRoot,
      `[Oracle:${label}] Hot db_root_hash (${hotRootStr}) === Oracle expectedRoot (${this.expectedRoot})`
    );
    assertFn(
      BigInt(sdRootStr) === this.expectedRoot,
      `[Oracle:${label}] SD state_root (${sdRootStr}) === Oracle expectedRoot (${this.expectedRoot})`
    );
    assertFn(
      String(hotState.body.latest_hash).toUpperCase() === prevHash.toUpperCase(),
      `[Oracle:${label}] Hot latest_hash matches WAL tip hash`
    );

    // 4. Verify every active key on both Hot Node and SD Worker
    for (const [k, meta] of this.kv.entries()) {
      if (meta.active) {
        const hotGet = await httpFetch(`${PROXY_URL}/get?namespace=default&key=${k}`);
        const sdGet = await httpFetch(`${SD_URL}/key?key=${k}`);
        assertFn(
          hotGet.status === 200 && Number(hotGet.body.val) === meta.val && Number(hotGet.body.ver) === meta.ver,
          `[Oracle:${label}] Hot key=${k} matches Oracle (val=${meta.val}, ver=${meta.ver})`
        );
        assertFn(
          sdGet.status === 200 && Number(sdGet.body.val) === meta.val,
          `[Oracle:${label}] SD key=${k} matches Oracle (val=${meta.val})`
        );
      }
    }
  }
}

async function httpFetch(url, opts = {}) {
  try {
    const res = await fetch(url, {
      ...opts,
      headers: { Accept: 'application/json', ...(opts.headers || {}) }
    });
    const text = await res.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
    return { status: res.status, body };
  } catch (err) {
    return { status: 0, error: err.message };
  }
}

async function waitForCondition(checkFn, timeoutMs = 6000, intervalMs = 60) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await checkFn()) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}

async function runChaosSuite() {
  console.log('========================================================================');
  console.log(' HyDB Phase 05: Real Multi-Process Chaos & Fault-Injection Suite');
  console.log(` Deterministic PRNG Seed: ${SEED}`);
  console.log('========================================================================\n');

  let passed = 0;
  let failed = 0;
  const assert = (cond, msg) => {
    if (!cond) {
      console.error(`  ❌ FAIL: ${msg}`);
      failed++;
      throw new Error(`Chaos invariant failed: ${msg}`);
    }
    console.log(`  ✅ PASS: ${msg}`);
    passed++;
  };

  const assertProxied = (prevCount, scenarioLabel) => {
    const delta = HttpChaos.stats.totalProxied - prevCount;
    assert(delta > 0, `[FaultInjectorGuard:${scenarioLabel}] ${delta} requests verified traversing HTTP Chaos Proxy`);
  };

  const oracle = new VerificationOracle();
  const procMgr = new NodeProcessManager();
  const httpProxy = await startHttpChaosProxy();
  const tcpProxy = await startTcpChaosProxy();

  console.log(`[HttpChaosProxy] Listening on :${PROXY_PORT} -> Hot HTTP :${REAL_HOT_PORT}`);
  console.log(`[TcpChaosProxy]  Listening on :${TCP_PROXY_PORT} -> Hot TCP Hub :${REAL_HOT_TCP_PORT}\n`);

  try {
    // Boot real separate OS processes for Hot Node and SD Worker
    console.log('[Setup] Spawning real OS processes for Hot Node and SD Worker...');
    const hotChild = await procMgr.startNode('hot');
    const sdChild = await procMgr.startNode('sd');
    console.log(`  -> Hot Node OS PID: ${hotChild.pid}`);
    console.log(`  -> SD Worker OS PID: ${sdChild.pid}`);

    const connected = await waitForCondition(async () => {
      const st = await httpFetch(`${SD_URL}/status`);
      const hst = await httpFetch(`${HOT_DIRECT_URL}/state`);
      return st.status === 200 && isTrue(st.body.connected_to_hot) && hst.status === 200 && Number(hst.body.active_su_subscribers) === 1;
    });
    assert(connected, 'Hot Node & SD Worker authenticated over TCP Replication Proxy');

    // ========================================================================
    // SCENARIO 1: Packet Loss & Latency Jitter (Client -> Hot via HTTP Proxy)
    // ========================================================================
    console.log('\n[Scenario 1] Packet Loss & Latency Jitter (Client -> Hot via HTTP Proxy):');
    let proxyBefore = HttpChaos.stats.totalProxied;
    HttpChaos.mode = 'jitter';

    for (let i = 1; i <= 8; i++) {
      const reqId = `req-jitter-${i}`;
      const k = 7000 + i;
      const v = i * 100;
      const res = await httpFetch(
        `${PROXY_URL}/put?namespace=default&key=${k}&val=${v}&require_durability=true&quorum=majority&request_id=${reqId}`
      );
      if (!(res.status === 200 && isTrue(res.body?.ok) && res.body?.write_status === 'durable')) {
        console.error('DEBUG Scenario 1 res:', JSON.stringify(res));
      }
      assert(
        res.status === 200 && isTrue(res.body.ok) && res.body.write_status === 'durable',
        `Jitter write seq=${res.body?.seq} durable under 12-45ms jitter`
      );
      oracle.recordWrite(k, v, res.body);
    }
    assert(HttpChaos.stats.jitterInjected === 8, `HTTP Fault Injector applied latency jitter to all 8 requests`);
    assertProxied(proxyBefore, 'Scenario1-Jitter');

    proxyBefore = HttpChaos.stats.totalProxied;
    HttpChaos.mode = 'loss';
    HttpChaos.lossProbability = 0.5;
    const dropsBefore = HttpChaos.stats.packetsDropped;

    for (let i = 1; i <= 8; i++) {
      const reqId = `req-loss-${i}`;
      const k = 7100 + i;
      const v = i * 250;
      let committed = false;
      let attempts = 0;
      while (!committed && attempts < 12) {
        attempts++;
        if (attempts > 4) HttpChaos.mode = 'pass'; // Bound max retries deterministically
        const res = await httpFetch(
          `${PROXY_URL}/put?namespace=default&key=${k}&val=${v}&require_durability=true&quorum=majority&request_id=${reqId}`
        );
        if (res.status === 200 && isTrue(res.body.ok)) {
          committed = true;
          oracle.recordWrite(k, v, res.body);
        }
        HttpChaos.mode = 'loss';
      }
      assert(committed, `Key ${k} (request_id=${reqId}) committed cleanly despite packet loss`);
    }
    HttpChaos.mode = 'pass';
    assert(HttpChaos.stats.packetsDropped > dropsBefore, `HTTP Fault Injector dropped packets (${HttpChaos.stats.packetsDropped} dropped)`);
    assertProxied(proxyBefore, 'Scenario1-Loss');
    await oracle.auditFullCluster(assert, 'Scenario1-PostLoss');

    // ========================================================================
    // SCENARIO 2: Gangguan Hot -> SD (Sever Real TCP Replication Stream)
    // ========================================================================
    console.log('\n[Scenario 2] Gangguan Hot -> SD (Sever Real TCP Replication Stream):');
    proxyBefore = HttpChaos.stats.totalProxied;
    const walLinesBeforeSever = readFileSync(WAL_PATH, 'utf8').trim().split('\n').filter(Boolean).length;

    // Sever the TCP link between Hot Node and SD Worker
    TcpChaos.setMode('sever');
    const subDropped = await waitForCondition(async () => {
      const st = await httpFetch(`${SD_URL}/status`);
      return st.status === 200 && st.body.connected_to_hot === false;
    });
    assert(subDropped, 'SD Worker detected severed TCP replication stream (connected_to_hot=false)');

    // Attempt durable write while Hot -> SD TCP stream is severed
    const severReqId = 'req-severed-hot-sd-1';
    const severKey = 7201;
    const severVal = 9999;
    const severRes = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=${severKey}&val=${severVal}&require_durability=true&quorum=majority&timeout_ms=200&request_id=${severReqId}`
    );
    assert(severRes.status === 504, 'Write with require_durability=true&quorum=majority rejected with HTTP 504 while TCP link severed');
    assert(severRes.body.error_code === 'ERR_DURABILITY_TIMEOUT', 'Error code is ERR_DURABILITY_TIMEOUT');
    assert(severRes.body.write_status === 'accepted', 'write_status is strictly "accepted" (not "durable")');
    assert(severRes.body.commit_status === 'provisional', 'commit_status is strictly "provisional"');
    assert(severRes.body.replicated === false, 'replicated flag is strictly false');
    oracle.recordWrite(severKey, severVal, severRes.body);

    const walLinesDuringSever = readFileSync(WAL_PATH, 'utf8').trim().split('\n').filter(Boolean).length;
    assert(
      walLinesDuringSever === walLinesBeforeSever,
      `SD disk WAL did not advance while TCP stream severed (${walLinesDuringSever} === ${walLinesBeforeSever})`
    );
    assertProxied(proxyBefore, 'Scenario2-Severed');

    // Restore TCP Replication Link and verify automatic catch-up + durability restoration
    TcpChaos.setMode('pass');
    const reconnected = await waitForCondition(async () => {
      const st = await httpFetch(`${SD_URL}/status`);
      return st.status === 200 && isTrue(st.body.connected_to_hot) && Number(st.body.latest_tick) === Number(severRes.body.seq);
    });
    assert(reconnected, `SD Worker reconnected and caught up provisional seq=${severRes.body.seq} to disk WAL`);

    // Check /request_status for severReqId: should now be upgraded to durable!
    const statusAfterHeal = await httpFetch(`${PROXY_URL}/request_status?request_id=${severReqId}`);
    assert(
      statusAfterHeal.status === 200 &&
        statusAfterHeal.body.write_status === 'durable' &&
        statusAfterHeal.body.commit_status === 'durable',
      `request_status for ${severReqId} transitioned from "accepted" to "durable" after link heal`
    );
    oracle.durableSeqs.add(Number(severRes.body.seq));
    await oracle.auditFullCluster(assert, 'Scenario2-PostHeal');

    // ========================================================================
    // SCENARIO 3: SD Mati Lalu Hidup (Real OS SIGKILL & Cold WAL Recovery)
    // ========================================================================
    console.log('\n[Scenario 3] SD Mati Lalu Hidup (Real OS SIGKILL & Restart from Disk WAL):');
    proxyBefore = HttpChaos.stats.totalProxied;
    const sdStateBeforeKill = await httpFetch(`${SD_URL}/state`);
    const preKillTick = Number(sdStateBeforeKill.body.tick);
    const preKillRoot = BigInt(sdStateBeforeKill.body.state_root_str || sdStateBeforeKill.body.state_root);
    const oldSdPid = procMgr.procs.sd.pid;

    // Kill real SD OS process with SIGKILL (kill -9)
    await procMgr.killNode('sd', 'SIGKILL');
    const sdDeadFetch = await httpFetch(`${SD_URL}/status`);
    assert(sdDeadFetch.status === 0, `SD Worker OS process (PID ${oldSdPid}) killed with SIGKILL and unreachable`);

    // Spawn brand-new SD Worker OS process pointed at the same WAL_PATH
    const newSdChild = await procMgr.startNode('sd');
    assert(newSdChild.pid !== oldSdPid, `New SD Worker OS process spawned with fresh PID (${newSdChild.pid} !== ${oldSdPid})`);

    const sdRecovered = await waitForCondition(async () => {
      const st = await httpFetch(`${SD_URL}/status`);
      return st.status === 200 && isTrue(st.body.ready) && isTrue(st.body.connected_to_hot);
    });
    assert(sdRecovered, 'Restarted SD Worker recovered from disk WAL and reconnected to Hot Node');

    const sdStatusAfter = await httpFetch(`${SD_URL}/status`);
    assert(sdStatusAfter.body.health === 'healthy', 'Recovered SD health is "healthy"');
    assert(sdStatusAfter.body.wal_corrupted === false, 'Recovered SD wal_corrupted is false');
    assert(sdStatusAfter.body.halt_reason === 'none', 'Recovered SD halt_reason is "none"');
    assert(Number(sdStatusAfter.body.latest_tick) === preKillTick, `Recovered SD tick (${sdStatusAfter.body.latest_tick}) matches pre-crash (${preKillTick})`);
    assert(BigInt(sdStatusAfter.body.state_root_str) === preKillRoot, `Recovered SD state_root (${sdStatusAfter.body.state_root_str}) matches pre-crash (${preKillRoot})`);

    // Execute new durable write after SD restart
    const postSdKillRes = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=7301&val=4242&require_durability=true&quorum=majority&request_id=req-post-sd-kill`
    );
    assert(
      postSdKillRes.status === 200 && postSdKillRes.body.write_status === 'durable',
      'New write after SD OS restart committed durably'
    );
    oracle.recordWrite(7301, 4242, postSdKillRes.body);
    assertProxied(proxyBefore, 'Scenario3-SdRestart');
    await oracle.auditFullCluster(assert, 'Scenario3-PostSdRestart');

    // ========================================================================
    // SCENARIO 4: Hot Mati Lalu Hidup (Real OS SIGKILL & State Reconciliation)
    // ========================================================================
    console.log('\n[Scenario 4] Hot Mati Lalu Hidup (Real OS SIGKILL, Restart & WAL Reconciliation):');
    proxyBefore = HttpChaos.stats.totalProxied;
    const hotStateBeforeKill = await httpFetch(`${PROXY_URL}/state`);
    const preHotKillSeq = Number(hotStateBeforeKill.body.seq);
    const preHotKillRoot = BigInt(hotStateBeforeKill.body.db_root_hash_str || hotStateBeforeKill.body.db_root_hash);
    const preHotKillHash = hotStateBeforeKill.body.latest_hash;
    const oldHotPid = procMgr.procs.hot.pid;

    // Kill real Hot Node OS process with SIGKILL (kill -9)
    await procMgr.killNode('hot', 'SIGKILL');
    const hotDeadViaProxy = await httpFetch(`${PROXY_URL}/state`);
    assert(hotDeadViaProxy.status === 502, `Hot Node OS process (PID ${oldHotPid}) killed with SIGKILL; proxy returns 502`);

    // Spawn brand-new Hot Node OS process reconciling state from production HYDB_HOT_WAL_PATH (matching docker-compose.yml)
    assert(existsSync(HOT_WAL_PATH), 'Production Hot WAL file (HYDB_HOT_WAL_PATH) exists on disk before Hot restart');
    const newHotChild = await procMgr.startNode('hot', {
      HYDB_HOT_WAL_PATH: HOT_WAL_PATH
    });
    assert(newHotChild.pid !== oldHotPid, `New Hot Node OS process spawned with fresh PID (${newHotChild.pid} !== ${oldHotPid}) using HYDB_HOT_WAL_PATH`);

    const hotAndSdReady = await waitForCondition(async () => {
      const hst = await httpFetch(`${PROXY_URL}/state`);
      const sst = await httpFetch(`${SD_URL}/status`);
      return (
        hst.status === 200 &&
        Number(hst.body.active_su_subscribers) === 1 &&
        sst.status === 200 &&
        isTrue(sst.body.connected_to_hot)
      );
    });
    assert(hotAndSdReady, 'Restarted Hot Node reconciled state from HYDB_HOT_WAL_PATH and re-established SD subscriber stream');

    const hotStateAfterRestart = await httpFetch(`${PROXY_URL}/state`);
    assert(
      Number(hotStateAfterRestart.body.seq) === preHotKillSeq,
      `Reconciled Hot seq (${hotStateAfterRestart.body.seq}) matches pre-crash (${preHotKillSeq})`
    );
    const postHotRootStr = hotStateAfterRestart.body.db_root_hash_str || String(hotStateAfterRestart.body.db_root_hash);
    assert(
      BigInt(postHotRootStr) === preHotKillRoot,
      `Reconciled Hot db_root_hash (${postHotRootStr}) matches pre-crash (${preHotKillRoot})`
    );
    assert(
      String(hotStateAfterRestart.body.latest_hash).toUpperCase() === String(preHotKillHash).toUpperCase(),
      'Reconciled Hot latest_hash matches pre-crash hash'
    );

    // Also verify fallback: if local HYDB_HOT_WAL_PATH is empty/missing on restart, Hot Node falls back to SD WAL_PATH
    await procMgr.killNode('hot', 'SIGKILL');
    rmSync(HOT_WAL_PATH, { force: true });
    const fallbackHotChild = await procMgr.startNode('hot', {
      HYDB_HOT_WAL_PATH: HOT_WAL_PATH
    });
    const fallbackReady = await waitForCondition(async () => {
      const hst = await httpFetch(`${PROXY_URL}/state`);
      const sst = await httpFetch(`${SD_URL}/status`);
      return (
        hst.status === 200 &&
        Number(hst.body.seq) === preHotKillSeq &&
        Number(hst.body.active_su_subscribers) === 1 &&
        sst.status === 200 &&
        isTrue(sst.body.connected_to_hot)
      );
    });
    assert(
      fallbackReady && fallbackHotChild.pid !== newHotChild.pid,
      'When local HYDB_HOT_WAL_PATH is missing/empty, Hot Node automatically falls back to validated SD WAL_PATH'
    );

    // Perform a new write on the restarted Hot Node to prove seamless continuation
    const postHotKillRes = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=7401&val=8888&require_durability=true&quorum=majority&request_id=req-post-hot-kill`
    );
    assert(
      postHotKillRes.status === 200 &&
        Number(postHotKillRes.body.seq) === preHotKillSeq + 1 &&
        postHotKillRes.body.write_status === 'durable',
      `Write after Hot OS restart continued seamlessly at seq=${preHotKillSeq + 1}`
    );
    oracle.recordWrite(7401, 8888, postHotKillRes.body);
    assertProxied(proxyBefore, 'Scenario4-HotRestart');
    await oracle.auditFullCluster(assert, 'Scenario4-PostHotRestart');

    // ========================================================================
    // SCENARIO 5: Retry Setelah Respons Hilang (Dropped HTTP Response After Commit)
    // ========================================================================
    console.log('\n[Scenario 5] Retry Setelah Respons Hilang (Idempotent Retry on Lost Response):');
    proxyBefore = HttpChaos.stats.totalProxied;
    const lostRespReqId = 'req-lost-response-7501';
    const lostKey = 7501;
    const lostVal = 13579;

    // Configure HTTP Chaos Proxy to forward request to Hot Node, let it commit, then drop the HTTP response!
    HttpChaos.mode = 'drop_response';
    HttpChaos.dropNextResponses = 1;

    const firstAttempt = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=${lostKey}&val=${lostVal}&require_durability=true&quorum=majority&request_id=${lostRespReqId}`
    );
    assert(firstAttempt.status === 0, 'Client experienced dropped HTTP response after Hot Node committed write');
    assert(HttpChaos.stats.responsesDropped === 1, 'HTTP Chaos Proxy confirmed 1 response dropped after commit');

    HttpChaos.mode = 'pass';

    // Client inspects /request_status to resolve ambiguity
    const statusLookup = await httpFetch(`${PROXY_URL}/request_status?request_id=${lostRespReqId}`);
    assert(statusLookup.status === 200 && isTrue(statusLookup.body.found), 'Client located in-flight request via /request_status');
    assert(statusLookup.body.write_status === 'durable', '/request_status confirms write_status is "durable"');
    const committedSeq = Number(statusLookup.body.seq);

    // Client also retries the exact same PUT request with identical request_id
    const retryRes = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=${lostKey}&val=${lostVal}&require_durability=true&quorum=majority&request_id=${lostRespReqId}`
    );
    assert(retryRes.status === 200 && isTrue(retryRes.body.ok), 'Idempotent retry returned HTTP 200 OK');
    assert(retryRes.body.idempotent_replay === true, 'Retry explicitly flagged idempotent_replay=true');
    assert(Number(retryRes.body.seq) === committedSeq, `Retry returned original seq=${committedSeq} without advancing sequence`);
    assert(Number(retryRes.body.ver) === 1, 'Record version is 1 (no double mutation on retry)');

    oracle.recordWrite(lostKey, lostVal, statusLookup.body.result);
    assertProxied(proxyBefore, 'Scenario5-LostResponseRetry');
    await oracle.auditFullCluster(assert, 'Scenario5-PostLostResponse');

    // ========================================================================
    // SCENARIO 6: Epoch / Fencing (Stale Epoch Rejection on Hot & SD Worker)
    // ========================================================================
    console.log('\n[Scenario 6] Epoch / Fencing (Stale Epoch Rejection on Hot & SD Worker):');
    proxyBefore = HttpChaos.stats.totalProxied;

    // 6A: Promote both Hot Node and SD Worker to Epoch 2
    const promoteHot2 = await httpFetch(`${PROXY_URL}/promote?epoch=2&admin_token=${ADMIN_TOKEN}`);
    const fenceSd2 = await httpFetch(`${SD_URL}/fence_epoch?epoch=2&admin_token=${ADMIN_TOKEN}`);
    assert(promoteHot2.status === 200 && Number(promoteHot2.body.epoch) === 2, 'Hot Node promoted to epoch=2');
    assert(fenceSd2.status === 200 && Number(fenceSd2.body.epoch) === 2, 'SD Worker fenced to epoch=2');

    // Send stale epoch=1 write to Hot Node via Proxy -> must be rejected with 409 FENCED_STALE_EPOCH
    const staleHotRes = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=7601&val=111&epoch=1&request_id=req-stale-hot-ep1`
    );
    assert(staleHotRes.status === 409, 'Hot Node rejected stale epoch=1 write with HTTP 409');
    assert(staleHotRes.body.error_code === 'FENCED_STALE_EPOCH', 'Error code is FENCED_STALE_EPOCH');

    // 6B: Fence SD Worker to Epoch 3 BEFORE promoting Hot Node (simulating split-brain zombie Hot Node at Epoch 2)
    const fenceSd3 = await httpFetch(`${SD_URL}/fence_epoch?epoch=3&admin_token=${ADMIN_TOKEN}`);
    assert(fenceSd3.status === 200 && Number(fenceSd3.body.epoch) === 3, 'SD Worker fenced to epoch=3 ahead of Hot Node (epoch=2)');

    const sdStatusBeforeZombie = await httpFetch(`${SD_URL}/status`);
    const fencedRejBefore = Number(sdStatusBeforeZombie.body.fenced_rejections || 0);
    const walLinesBeforeZombie = readFileSync(WAL_PATH, 'utf8').trim().split('\n').filter(Boolean).length;

    // Zombie Hot Node (at epoch=2) tries to replicate a durable write to SD Worker (at epoch=3)
    const zombieRes = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=7602&val=666&require_durability=true&quorum=majority&timeout_ms=250&request_id=req-zombie-ep2`
    );
    assert(zombieRes.status === 504, 'Zombie Hot Node (epoch=2) failed durability check because SD (epoch=3) fenced the event');

    const sdStatusAfterZombie = await httpFetch(`${SD_URL}/status`);
    assert(
      Number(sdStatusAfterZombie.body.fenced_rejections) > fencedRejBefore,
      `SD Worker incremented fenced_rejections (${fencedRejBefore} -> ${sdStatusAfterZombie.body.fenced_rejections})`
    );
    const walLinesAfterZombie = readFileSync(WAL_PATH, 'utf8').trim().split('\n').filter(Boolean).length;
    assert(
      walLinesAfterZombie === walLinesBeforeZombie,
      'SD Worker disk WAL remained untainted by stale epoch=2 event'
    );

    // Reconcile Hot Node from clean WAL and promote Hot Node to Epoch 3 (discarding fenced zombie's local hot_wal.log)
    rmSync(HOT_WAL_PATH, { force: true });
    await procMgr.startNode('hot', {
      HYDB_RECONCILE_WAL_PATH: WAL_PATH,
      HYDB_RECONCILE_EPOCH: '3'
    });
    const hotEp3Ready = await waitForCondition(async () => {
      const hst = await httpFetch(`${PROXY_URL}/state`);
      return hst.status === 200 && Number(hst.body.epoch) === 3 && Number(hst.body.active_su_subscribers) === 1;
    });
    assert(hotEp3Ready, 'Promoted Hot Node reconciled at epoch=3 and connected to SD Worker');

    // Valid Epoch 3 write succeeds and replicates durably
    const validEp3Res = await httpFetch(
      `${PROXY_URL}/put?namespace=default&key=7603&val=3333&epoch=3&require_durability=true&quorum=majority&request_id=req-valid-ep3`
    );
    assert(
      validEp3Res.status === 200 &&
        Number(validEp3Res.body.epoch) === 3 &&
        validEp3Res.body.write_status === 'durable',
      'Epoch=3 write accepted and replicated durably across Hot Node and SD Worker'
    );
    oracle.recordWrite(7603, 3333, validEp3Res.body);
    assertProxied(proxyBefore, 'Scenario6-EpochFencing');
    await oracle.auditFullCluster(assert, 'Scenario6-FinalAudit');

    console.log('\n========================================================================');
    console.log(` Phase 05 Chaos Suite Results: ${passed} PASSED, ${failed} FAILED`);
    console.log(` Proxy Stats: HTTP Proxied=${HttpChaos.stats.totalProxied}, Jitter=${HttpChaos.stats.jitterInjected}, Drops=${HttpChaos.stats.packetsDropped}, RespDrops=${HttpChaos.stats.responsesDropped}`);
    console.log(` TCP Stats:   Connections=${TcpChaos.stats.totalConnections}, Severed=${TcpChaos.stats.severedConnections}, Bytes=${TcpChaos.stats.bytesHotToSd}`);
    console.log('========================================================================\n');
  } finally {
    await procMgr.stopAll();
    httpProxy.close();
    tcpProxy.close();
    try {
      rmSync(TEST_TMP_DIR, { recursive: true, force: true });
    } catch {}
  }
}

runChaosSuite().catch((err) => {
  console.error('\nFatal Chaos Suite Error:', err);
  process.exit(1);
});
