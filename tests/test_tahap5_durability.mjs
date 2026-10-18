/**
 * HyDB Tahap 5: Durability, Replication, & Resilient Cryptographic Recovery Test Suite
 *
 * Verifies all normative requirements of Tahap 5:
 * 1. Physical Durability Anchor: Physical fsync/datasync prior to emitting sequence ACK.
 * 2. Provisional vs Durable State Separation: Explicit commit_status ('durable' vs 'provisional').
 * 3. Fail-Closed Durability Gate: require_durability=true triggers HTTP 504 ERR_DURABILITY_TIMEOUT on quorum timeout.
 * 4. Quorum Enforcement & Authenticated Node Tracking:
 *    - Modes: 'single', 'majority', 'all', and 'target:NodeId'
 *    - Authenticated identity verification (hb_su).
 * 5. Fail-Closed Node Authentication on Port 8736:
 *    - Zero public secret fallback.
 *    - Forged handshake rejected with AUTH_REJECTED / ERR_NODE_AUTH_FAILED.
 *    - Proper HMAC-SHA256 handshake accepted with AUTH_OK.
 * 6. Strict Cryptographic Recovery (recover_from_wal):
 *    - Clean EOF Truncation Resilience: Detects incomplete line from sudden node crash, recovers all valid preceding data without crash.
 *    - Sequence Gap Detection: Halts recovery on sequence gap with diagnostic reporting.
 *    - Broken Hash Chain Detection: Halts recovery if prev_hash does not chain to prior event.
 *    - Tampered Event Detection: Halts recovery if event payload was altered after hash creation.
 */

import { readFileSync, appendFileSync, writeFileSync } from 'node:fs';
import net from 'node:net';
import crypto from 'node:crypto';

const HOT_URL = 'http://127.0.0.1:8734/~hymessage@1.0';
const SU_SCHEDULER_URL = 'http://127.0.0.1:8735/~scheduler@1.0';
const WAL_PATH = '/home/weslur/database/pool_su/wal.log';
const WALLET_PATH = '/home/weslur/database/secrets/wallet.json';

const isTrue = (v) => v === true || String(v).toLowerCase() === 'true';
const isFalse = (v) => v === false || String(v).toLowerCase() === 'false';

async function httpGet(url) {
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: text };
  }
}

async function httpPost(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body)
  });
  const text = await res.text();
  try {
    return { status: res.status, body: JSON.parse(text) };
  } catch {
    return { status: res.status, body: text };
  }
}

function sendTcpPacket(socket, payloadObj) {
  const jsonBuf = Buffer.from(JSON.stringify(payloadObj), 'utf8');
  const header = Buffer.alloc(4);
  header.writeUInt32BE(jsonBuf.length, 0);
  socket.write(Buffer.concat([header, jsonBuf]));
}

function readTcpPacket(socket, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    let accumulated = Buffer.alloc(0);
    let expectedLen = null;

    const timer = setTimeout(() => {
      cleanup();
      reject(new Error('TCP packet read timeout'));
    }, timeoutMs);

    function onData(chunk) {
      accumulated = Buffer.concat([accumulated, chunk]);
      if (expectedLen === null && accumulated.length >= 4) {
        expectedLen = accumulated.readUInt32BE(0);
      }
      if (expectedLen !== null && accumulated.length >= 4 + expectedLen) {
        clearTimeout(timer);
        cleanup();
        const packetBytes = accumulated.slice(4, 4 + expectedLen);
        try {
          resolve(JSON.parse(packetBytes.toString('utf8')));
        } catch (e) {
          resolve(packetBytes);
        }
      }
    }

    function onClose() {
      clearTimeout(timer);
      cleanup();
      resolve({ closed: true });
    }

    function onError(err) {
      clearTimeout(timer);
      cleanup();
      reject(err);
    }

    function cleanup() {
      socket.removeListener('data', onData);
      socket.removeListener('close', onClose);
      socket.removeListener('error', onError);
    }

    socket.on('data', onData);
    socket.on('close', onClose);
    socket.on('error', onError);
  });
}

async function main() {
  console.log('========================================================================');
  console.log(' HyDB Tahap 5: Durability, Quorum, & Recovery Test Suite');
  console.log('========================================================================\n');

  let passed = 0;
  let failed = 0;

  function assert(condition, message) {
    if (condition) {
      console.log(`  ✅ PASS: ${message}`);
      passed++;
    } else {
      console.error(`  ❌ FAIL: ${message}`);
      failed++;
    }
  }

  // Derive cluster secret
  const walletBytes = readFileSync(WALLET_PATH);
  const clusterSecret = crypto.createHash('sha256').update(walletBytes).digest();
  const validAdminToken = clusterSecret.toString('hex').toUpperCase();

  // Reset both nodes to clean baseline
  console.log('[Setup] Resetting Hot Node and Storage Unit to clean state...');
  await httpGet(`${HOT_URL}/reset?admin_token=${validAdminToken}`);
  await httpGet(`${SU_SCHEDULER_URL}/reset`);
  await new Promise(r => setTimeout(r, 600));

  // --------------------------------------------------------------------------
  console.log('\n[Test 1] Physical Durability & Provisional State Separation:');
  // --------------------------------------------------------------------------
  // 1a: Normal write with active SU connected
  const write1 = await httpGet(`${HOT_URL}/put?namespace=default&key=5001&val=100`);
  assert(isTrue(write1.body.ok), 'Default write accepted (HTTP 200)');
  assert(isTrue(write1.body.replicated), 'Write replicated to SU');
  assert(write1.body.commit_status === 'durable', `commit_status is "durable" on quorum ACK (got ${write1.body.commit_status})`);

  // Verify WAL on disk physically contains Seq 1
  const walLines1 = readFileSync(WAL_PATH, 'utf8').trim().split('\n').filter(Boolean);
  assert(walLines1.length >= 1, `Physical WAL contains written record on disk (lines: ${walLines1.length})`);
  const walEntry1 = JSON.parse(walLines1[walLines1.length - 1]);
  assert(Number(walEntry1.seq) === Number(write1.body.seq), `WAL physical entry seq matches Hot seq (${walEntry1.seq})`);

  // 1b: Write with explicit require_durability=true when quorum is satisfied
  const write2 = await httpGet(`${HOT_URL}/put?namespace=default&key=5002&val=200&require_durability=true`);
  assert(isTrue(write2.body.ok), 'Write with require_durability=true succeeds when quorum satisfied');
  assert(write2.body.commit_status === 'durable', 'commit_status is "durable" with require_durability=true');

  // 1c: Write with require_durability=true when quorum fails (targeting non-existent SU with 50ms timeout)
  const writeUnsatisfied = await httpGet(`${HOT_URL}/put?namespace=default&key=5003&val=300&require_durability=true&quorum=target:nonexistent_su_node&timeout_ms=50`);
  assert(writeUnsatisfied.status === 504 || writeUnsatisfied.body.status === 504, 'Unsatisfied require_durability returns HTTP 504');
  assert(writeUnsatisfied.body.error_code === 'ERR_DURABILITY_TIMEOUT', `Returns ERR_DURABILITY_TIMEOUT (got ${writeUnsatisfied.body.error_code})`);
  assert(writeUnsatisfied.body.commit_status === 'provisional', `commit_status is "provisional" when replication times out (got ${writeUnsatisfied.body.commit_status})`);
  assert(isFalse(writeUnsatisfied.body.replicated), 'replicated flag is false when durability timeout occurs');

  // 1d: Normal write without require_durability when quorum fails -> returns 200 but marked provisional
  const writeProvisional = await httpGet(`${HOT_URL}/put?namespace=default&key=5004&val=400&quorum=target:nonexistent_su_node&timeout_ms=50`);
  assert(isTrue(writeProvisional.body.ok), 'Write without require_durability returns 200 on quorum timeout');
  assert(isFalse(writeProvisional.body.replicated), 'replicated is false for unreachable quorum');
  assert(writeProvisional.body.commit_status === 'provisional', `commit_status is marked "provisional" (got ${writeProvisional.body.commit_status})`);

  // --------------------------------------------------------------------------
  console.log('\n[Test 2] Quorum Modes & Authenticated Node Identification:');
  // --------------------------------------------------------------------------
  // 2a: Quorum "single"
  const qSingle = await httpGet(`${HOT_URL}/put?namespace=default&key=5005&val=500&quorum=single`);
  assert(isTrue(qSingle.body.replicated) && qSingle.body.commit_status === 'durable', 'quorum=single satisfied by connected SU');

  // 2b: Quorum "majority" (with 1 active SU, 1 > 1 div 2 is true)
  const qMajority = await httpGet(`${HOT_URL}/put?namespace=default&key=5006&val=600&quorum=majority`);
  assert(isTrue(qMajority.body.replicated) && qMajority.body.commit_status === 'durable', 'quorum=majority satisfied by active SU majority');

  // 2c: Quorum targeted to authenticated SU node id (hb_su)
  const qTargetOk = await httpGet(`${HOT_URL}/put?namespace=default&key=5007&val=700&quorum=target:hb_su`);
  assert(isTrue(qTargetOk.body.replicated) && qTargetOk.body.commit_status === 'durable', 'quorum=target:hb_su satisfied by authenticated SU hb_su');

  // 2d: Quorum targeted to non-existent node
  const qTargetBad = await httpGet(`${HOT_URL}/put?namespace=default&key=5008&val=800&quorum=target:phantom_node&timeout_ms=50`);
  assert(isFalse(qTargetBad.body.replicated) && qTargetBad.body.commit_status === 'provisional', 'quorum=target:phantom_node correctly rejected');

  // --------------------------------------------------------------------------
  console.log('\n[Test 3] Fail-Closed Cluster Authentication on Port 8736:');
  // --------------------------------------------------------------------------
  // Get initial state to check rejection counter
  const initialState = await httpGet(`${HOT_URL}/state`);
  const initialRejections = Number(initialState.body.unauthorized_tcp_rejections || 0);

  // 3a & 3b: Rogue TCP client connecting with forged signature
  const rogueSock = net.createConnection({ port: 8736, host: '127.0.0.1' });
  await new Promise(r => rogueSock.once('connect', r));

  const challenge = await readTcpPacket(rogueSock);
  assert(challenge.type === 'AUTH_CHALLENGE' && typeof challenge.nonce === 'string', 'Hot Node challenges new TCP peer with cryptographic nonce');

  // Send forged signature
  const forgedSig = crypto.randomBytes(32).toString('hex');
  sendTcpPacket(rogueSock, {
    type: 'AUTH_RESPONSE',
    node_id: 'rogue_attacker',
    sig: forgedSig
  });

  const rogueResp = await readTcpPacket(rogueSock);
  assert(rogueResp.type === 'AUTH_REJECTED', `Hot Node rejects rogue peer with AUTH_REJECTED (got ${rogueResp.type})`);
  assert(rogueResp.error_code === 'ERR_NODE_AUTH_FAILED', `Rejection error_code is ERR_NODE_AUTH_FAILED (got ${rogueResp.error_code})`);

  // Verify rogue socket is terminated by server
  await new Promise(r => setTimeout(r, 100));
  assert(rogueSock.destroyed || !rogueSock.writable, 'Hot Node closed rogue socket immediately');

  // Check state rejection counter incremented
  const stateAfterRej = await httpGet(`${HOT_URL}/state`);
  const newRejections = Number(stateAfterRej.body.unauthorized_tcp_rejections || 0);
  assert(newRejections === initialRejections + 1, `Hot Node tracked unauthorized TCP rejection counter (${newRejections})`);

  // 3c: Valid TCP client connecting with genuine cluster secret
  const validSock = net.createConnection({ port: 8736, host: '127.0.0.1' });
  await new Promise(r => validSock.once('connect', r));

  const validChallenge = await readTcpPacket(validSock);
  assert(validChallenge.type === 'AUTH_CHALLENGE', 'Hot Node sends challenge to valid client');

  const validNodeId = 'test_su_client';
  const hmacData = `${validChallenge.nonce}:${validNodeId}`;
  const validSig = crypto.createHmac('sha256', clusterSecret).update(hmacData).digest('hex');

  sendTcpPacket(validSock, {
    type: 'AUTH_RESPONSE',
    node_id: validNodeId,
    sig: validSig
  });

  const validResp = await readTcpPacket(validSock);
  assert(validResp.type === 'AUTH_OK' && validResp.status === 'AUTHENTICATED', 'Hot Node authenticates peer with genuine HMAC secret');
  validSock.destroy();
  await new Promise(r => setTimeout(r, 100));

  // --------------------------------------------------------------------------
  console.log('\n[Test 4] Resilient Recovery & Clean EOF Truncation (Node Crash Mid-Write):');
  // --------------------------------------------------------------------------
  // Baseline recovery check
  const baseRecover = await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);
  assert(isTrue(baseRecover.body.ok), 'SU simulate_restart succeeds');
  assert(baseRecover.body.halt_reason === 'none', `Baseline recovery halt_reason is none (got ${baseRecover.body.halt_reason})`);
  assert(isFalse(baseRecover.body.truncated_detected), 'Baseline recovery truncated_detected is false');
  const baselineTick = Number(baseRecover.body.tick);
  assert(baselineTick > 0, `Baseline tick is valid (${baselineTick})`);

  // Inject partial / truncated line at EOF (simulating abrupt power cut / OS crash during write)
  const originalWal = readFileSync(WAL_PATH);
  appendFileSync(WAL_PATH, '{"action":"put","backend":"wasm_slot","canonical_val":"999","contract_ver');

  // Trigger simulate_restart on SU
  const truncRecover = await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);
  assert(isTrue(truncRecover.body.ok), 'Recovery succeeds despite truncated line at EOF');
  assert(isTrue(truncRecover.body.truncated_detected), 'truncated_detected is true for truncated EOF line');
  assert(truncRecover.body.halt_reason === 'none', 'Recovery does not abort or fail on EOF truncated line');
  assert(Number(truncRecover.body.tick) === baselineTick, `Tick preserved at last valid record (${truncRecover.body.tick} == ${baselineTick})`);

  // Restore clean WAL
  writeFileSync(WAL_PATH, originalWal);
  await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);

  // --------------------------------------------------------------------------
  console.log('\n[Test 5] Strict Cryptographic Recovery Invariants:');
  // --------------------------------------------------------------------------
  // 5a: Sequence Gap Detection
  const walLines = readFileSync(WAL_PATH, 'utf8').trim().split('\n').filter(Boolean);
  const lastValidEntry = JSON.parse(walLines[walLines.length - 1]);
  const gappedSeq = Number(lastValidEntry.seq) + 5; // Skip 4 sequence numbers!

  const gappedEntry = {
    ...lastValidEntry,
    seq: gappedSeq,
    prev_hash: lastValidEntry.hash,
    val: 999999
  };
  // Compute valid hash for this gapped event so hash is correct, but sequence has a gap
  const gappedPayload = `${gappedEntry.prev_hash}${gappedSeq}20${gappedEntry.key}:${gappedEntry.val}`;
  gappedEntry.hash = crypto.createHash('sha256').update(gappedPayload).digest('hex').toUpperCase();

  appendFileSync(WAL_PATH, JSON.stringify(gappedEntry) + '\n');

  const gapRecover = await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);
  assert(isTrue(gapRecover.body.ok), 'Recovery call returned');
  assert(gapRecover.body.halt_reason.includes('sequence_gap'), `Recovery halts on sequence gap (got ${gapRecover.body.halt_reason})`);
  assert(Number(gapRecover.body.tick) === Number(lastValidEntry.seq), `Recovery state halted at last valid sequence (${gapRecover.body.tick})`);

  // Restore clean WAL
  writeFileSync(WAL_PATH, originalWal);

  // 5b: Broken Hash Chain Detection
  const brokenChainEntry = {
    ...lastValidEntry,
    seq: Number(lastValidEntry.seq) + 1,
    prev_hash: 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
    val: 888888
  };
  appendFileSync(WAL_PATH, JSON.stringify(brokenChainEntry) + '\n');

  const chainRecover = await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);
  assert(chainRecover.body.halt_reason.includes('broken_hash_chain'), `Recovery halts on broken hash chain (got ${chainRecover.body.halt_reason})`);

  // Restore clean WAL
  writeFileSync(WAL_PATH, originalWal);

  // 5c: Tampered Event Detection (altered val without updating hash)
  const tamperedEntry = {
    ...lastValidEntry,
    seq: Number(lastValidEntry.seq) + 1,
    prev_hash: lastValidEntry.hash,
    val: 777777,
    hash: '1111222233334444555566667777888811112222333344445555666677778888' // Forged invalid hash
  };
  appendFileSync(WAL_PATH, JSON.stringify(tamperedEntry) + '\n');

  const tamperRecover = await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);
  assert(tamperRecover.body.halt_reason.includes('hash_mismatch'), `Recovery halts on tampered event hash (got ${tamperRecover.body.halt_reason})`);

  // Final cleanup and recovery
  writeFileSync(WAL_PATH, originalWal);
  const finalRecover = await httpGet(`${SU_SCHEDULER_URL}/simulate_restart`);
  assert(finalRecover.body.halt_reason === 'none', 'Final clean recovery succeeded with zero halt reason');

  console.log('\n========================================================================');
  console.log(` Tahap 5 Verification Summary: ${passed} PASSED, ${failed} FAILED`);
  console.log('========================================================================\n');

  if (failed > 0) {
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Fatal Tahap 5 test error:', err);
  process.exit(1);
});
