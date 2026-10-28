// tests/test_tahap16_genesis_bootstrap.mjs
// Phase 4 Audit: Dynamic Node Registration & Cold-Start Bootstrap against Native Genesis AO Process
//
// Invariants Tested:
// 1. Genesis AO Process spawned on hb-genesis (:8733) via native WAMR/AOS runtime.
// 2. Genesis constitution & AuthorizedNodes configured for 4-node cluster (Hot + 3 SD replicas).
// 3. Arbitrator Process ID bound in Genesis state via SetArbitratorProcess.
// 4. hb-sd-1 bootstraps against Genesis via HTTP control plane and discovers Arbitrator.
// 5. Negative Test 1: Tampered Genesis Hash causes bootstrap rejection (err_genesis_hash_mismatch) and degrades node (fail-closed).
// 6. Negative Test 2: Unauthorized node registration is rejected (REJECTED_UNAUTHORIZED_NODE).
// 7. Negative Test 3: Node impersonation attempt is rejected (REJECTED_NODE_IMPERSONATION).
// 8. Full dynamic registration & heartbeats for hb-hot, hb-sd-1, hb-sd-2, and hb-sd-3.
// 9. Quorum readiness verified: GetStackStatus reaches HEALTHY with quorum satisfied.

import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import * as crypto from 'node:crypto';
import assert from 'node:assert';
import * as arbundles from 'arbundles';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT_DIR = join(__dirname, '..');

const HB_GENESIS_URL = 'http://127.0.0.1:8733';
const HB_HOT_URL = 'http://127.0.0.1:8734';
const HB_SD1_URL = 'http://127.0.0.1:8735';
const HB_SD2_URL = 'http://127.0.0.1:8745';
const HB_SD3_URL = 'http://127.0.0.1:8755';

async function fetchJson(url) {
  const resp = await fetch(url, {
    headers: {
      'Accept': 'application/json',
      'Connection': 'close'
    }
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`GET ${url} failed with HTTP ${resp.status}: ${text}`);
  }
  return await resp.json();
}

async function postAns104(url, dataItem) {
  const rawBytes = dataItem.getRaw();
  const resp = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json',
      'Connection': 'close'
    },
    body: rawBytes
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => '');
    throw new Error(`POST ${url} failed with HTTP ${resp.status}: ${text}`);
  }
  return await resp.json();
}

async function spawnProcess(signer, scheduler, wasmImage) {
  const createData = arbundles.createData || arbundles.default?.createData;
  const spawnTags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Variant', value: 'ao.TN.1' },
    { name: 'Type', value: 'Process' },
    { name: 'Device', value: 'process@1.0' },
    { name: 'Scheduler-Device', value: 'scheduler@1.0' },
    { name: 'Execution-Device', value: 'stack@1.0' },
    { name: 'Scheduler', value: scheduler },
    { name: 'Image', value: wasmImage },
    { name: 'Device-Stack', value: '"wasi@1.0", "json-iface@1.0", "wasm-64@1.0", "multipass@1.0"' },
    { name: 'Output-Prefix', value: 'wasm' },
    { name: 'Patch-From', value: '/results/outbox' },
    { name: 'Passes', value: '2' },
    { name: 'Stack-Keys', value: '"init", "compute", "snapshot", "normalize"' },
    { name: 'Ao-Types', value: 'device-stack="list", passes="integer", stack-keys="list"' }
  ];

  const spawnItem = createData('', signer, { tags: spawnTags });
  await spawnItem.sign(signer);

  const res = await postAns104(`${HB_GENESIS_URL}/~push@1.0/push`, spawnItem);
  assert(res.status === 200, 'Process spawn assignment returned status 200');
  return spawnItem.id;
}

async function scheduleMessage(signer, procId, action, data, extraTags = []) {
  const createData = arbundles.createData || arbundles.default?.createData;
  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: action },
    { name: 'Timestamp', value: Date.now().toString() },
    ...extraTags
  ];
  const item = createData(data, signer, { tags });
  await item.sign(signer);

  const schedRes = await postAns104(`${HB_GENESIS_URL}/${procId}~process@1.0/schedule`, item);
  assert(schedRes.status === 200, `Message scheduled at slot ${schedRes.slot}`);
  return { itemId: item.id, slot: schedRes.slot };
}

async function computeAndGetLatestResult(procId, expectedAction = null, targetSlot = null) {
  for (let attempt = 0; attempt < 30; attempt++) {
    let now;
    try {
      now = await fetchJson(`${HB_GENESIS_URL}/${procId}~process@1.0/now`);
    } catch (e) {
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }
    if (!now || now.status !== 200) {
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }
    const atSlot = parseInt(now['at-slot'], 10);
    if (targetSlot !== null && atSlot < targetSlot) {
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }
    if (!now['results+link']) {
      if (expectedAction || targetSlot !== null) {
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }
      return { now, results: null, outbox: [], targetMsg: null };
    }
    let results;
    try {
      results = await fetchJson(`${HB_GENESIS_URL}/${now['results+link']}`);
    } catch (e) {
      await new Promise(r => setTimeout(r, 2000));
      continue;
    }
    let outbox = [];
    if (results && results['outbox+link']) {
      let outboxMap;
      try {
        outboxMap = await fetchJson(`${HB_GENESIS_URL}/${results['outbox+link']}`);
      } catch {
        await new Promise(r => setTimeout(r, 2000));
        continue;
      }
      const entries = [];
      for (const [k, v] of Object.entries(outboxMap)) {
        if (k.endsWith('+link') || (k !== 'status' && !isNaN(Number(k)))) {
          const slot = parseInt(k.replace('+link', ''), 10) || 0;
          entries.push({ slot, link: v });
        }
      }
      entries.sort((a, b) => b.slot - a.slot);
      for (const entry of entries) {
        try {
          const msg = await fetchJson(`${HB_GENESIS_URL}/${entry.link}`);
          outbox.push(msg);
          if (expectedAction && (msg.Action === expectedAction || (msg.Tags && msg.Tags.some(t => (t.name === 'Action' || t[0] === 'Action') && (t.value === expectedAction || t[1] === expectedAction))))) {
            return { now, results, outbox, targetMsg: msg };
          }
        } catch {}
      }
    }
    if (!expectedAction) {
      return { now, results, outbox, targetMsg: outbox[0] || null };
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  throw new Error(`Timeout waiting for outbox message Action: ${expectedAction} in process ${procId} (targetSlot: ${targetSlot})`);
}

function parsePayload(msg) {
  if (!msg) return {};
  if (msg.Data && typeof msg.Data === 'string' && msg.Data.length > 0) {
    try {
      return JSON.parse(msg.Data);
    } catch {
      return {};
    }
  }
  return msg;
}

function getSignerAddress(signer) {
  return crypto.createHash('sha256').update(signer.publicKey).digest('base64url');
}

async function main() {
  console.log('========================================================================');
  console.log('TAHAP 16: DYNAMIC NODE REGISTRATION & COLD-START BOOTSTRAP AUDIT');
  console.log('========================================================================\n');

  // Check if Genesis (:8733) and SD1 (:8735) are alive
  const isGenAlive = await fetch(`${HB_GENESIS_URL}/~meta@1.0/info`, { headers: { Accept: 'application/json' } }).then(r => r.ok).catch(() => false);
  const isSdAlive = await fetch(`${HB_SD1_URL}/~meta@1.0/info`, { headers: { Accept: 'application/json' } }).then(r => r.ok).catch(() => false);
  if (!isGenAlive || !isSdAlive) {
    console.log('\n  ⚠️ [Live Cluster Inactive] HyperBEAM Genesis (:8733) or SD-1 (:8735) is not running.');
    console.log('  ⚠️ Skipping live cluster bootstrap tests (run npm run cluster:start for live testing).\n');
    console.log('========================================================================');
    console.log(' TAHAP 16 VERIFICATION SUMMARY: SKIPPED (Cluster Offline)');
    console.log('========================================================================');
    return;
  }

  // Load scheduler & wasm image
  const info = await fetchJson(`${HB_GENESIS_URL}/~meta@1.0/info`);
  const scheduler = info.address || 'gw0iqMNNGgXcAj_yejTV0H_152Hwz941gxhnzvB_PY0';
  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;

  // Load Wallets for all cluster nodes
  const genesisJwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_genesis.json'), 'utf8'));
  const hotJwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet.json'), 'utf8'));
  const suJwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_su.json'), 'utf8'));
  const sd2Jwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_sd_2.json'), 'utf8'));
  const sd3Jwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_sd_3.json'), 'utf8'));

  const signerGenesis = new ArweaveSigner(genesisJwk);
  const signerHot = new ArweaveSigner(hotJwk);
  const signerSd1 = new ArweaveSigner(suJwk);
  const signerSd2 = new ArweaveSigner(sd2Jwk);
  const signerSd3 = new ArweaveSigner(sd3Jwk);

  const genesisOwnerAddr = getSignerAddress(signerGenesis);
  const hotAddr = getSignerAddress(signerHot);
  const sd1Addr = getSignerAddress(signerSd1);
  const sd2Addr = getSignerAddress(signerSd2);
  const sd3Addr = getSignerAddress(signerSd3);

  console.log('Cluster Node Identities:');
  console.log('  👑 Genesis Authority :', genesisOwnerAddr);
  console.log('  🔥 Hot Sequencer     :', hotAddr);
  console.log('  💾 SD Replica 1 (SU) :', sd1Addr);
  console.log('  💾 SD Replica 2      :', sd2Addr);
  console.log('  💾 SD Replica 3      :', sd3Addr);

  // -------------------------------------------------------------------------
  // STEP 1: Spawn & Initialize Genesis Process on hb-genesis (:8733)
  // -------------------------------------------------------------------------
  console.log('\n[Step 1] Spawning Genesis AO Process on hb-genesis (:8733)...');
  const genesisId = await spawnProcess(signerGenesis, scheduler, wasmImage);
  console.log('  ✅ Genesis Process spawned with ID:', genesisId);
  writeFileSync(join(ROOT_DIR, 'ao_process/process_genesis_id.txt'), genesisId);

  console.log('  Loading ao_process/genesis.lua into WAMR...');
  const genesisLua = readFileSync(join(ROOT_DIR, 'ao_process/genesis.lua'), 'utf8');
  const s1 = await scheduleMessage(signerGenesis, genesisId, 'Eval', genesisLua);
  await computeAndGetLatestResult(genesisId, null, s1.slot);
  console.log('  ✅ Genesis Process initialized at slot', s1.slot);

  // -------------------------------------------------------------------------
  // STEP 2: Configure Genesis Constitution & Authorize Nodes
  // -------------------------------------------------------------------------
  console.log('\n[Step 2] Configuring Genesis Constitution & AuthorizedNodes...');
  const canonicalGenesisHash = crypto.createHash('sha256')
    .update(`hydb_universal_genesis_epoch_1_${genesisId}`)
    .digest('hex')
    .toUpperCase();
  writeFileSync(join(ROOT_DIR, 'ao_process/genesis_hash.txt'), canonicalGenesisHash);
  console.log('  Canonical Genesis Hash:', canonicalGenesisHash);

  const updateConfigPayload = {
    genesis_hash: canonicalGenesisHash,
    quorum_size: 3,
    quorum_mode: 'majority',
    add_nodes: [hotAddr, sd1Addr, sd2Addr, sd3Addr]
  };

  const s2 = await scheduleMessage(signerGenesis, genesisId, 'UpdateStackConfig', JSON.stringify(updateConfigPayload));
  const updateRes = await computeAndGetLatestResult(genesisId, 'UpdateStackConfigResponse', s2.slot);
  const updateData = parsePayload(updateRes.targetMsg);
  assert(updateData.ok === true, 'UpdateStackConfig returned ok=true');
  assert(updateData.status === 'CONFIG_UPDATED', 'UpdateStackConfig status CONFIG_UPDATED');
  console.log('  ✅ Genesis Constitution updated: QuorumSize = 3, QuorumMode = majority');

  // Verify configuration via GetGenesisConfig
  const s2b = await scheduleMessage(signerGenesis, genesisId, 'GetGenesisConfig', '');
  const configRes = await computeAndGetLatestResult(genesisId, 'GenesisConfigResponse', s2b.slot);
  const configData = parsePayload(configRes.targetMsg);
  assert(configData.ok === true, 'GetGenesisConfig returned ok=true');
  assert.strictEqual(configData.genesis.genesis_hash, canonicalGenesisHash, 'Genesis hash matches canonical hash');
  assert(configData.authorized_nodes[hotAddr], 'Hot Sequencer authorized');
  assert(configData.authorized_nodes[sd1Addr], 'SD-1 authorized');
  assert(configData.authorized_nodes[sd2Addr], 'SD-2 authorized');
  assert(configData.authorized_nodes[sd3Addr], 'SD-3 authorized');
  console.log('  ✅ AuthorizedNodes registry verified with all 4 cluster nodes');

  // -------------------------------------------------------------------------
  // STEP 3: Bind Arbitrator Process ID in Genesis State
  // -------------------------------------------------------------------------
  console.log('\n[Step 3] Binding Arbitrator Process ID via SetArbitratorProcess...');
  let arbitratorId = '';
  const arbFile = join(ROOT_DIR, 'ao_process/process_id.txt');
  if (existsSync(arbFile)) {
    arbitratorId = readFileSync(arbFile, 'utf8').trim();
  }
  if (!arbitratorId) {
    console.log('  Spawning new Arbitrator process...');
    arbitratorId = await spawnProcess(signerGenesis, scheduler, wasmImage);
    const arbitratorLua = readFileSync(join(ROOT_DIR, 'ao_process/arbitrator.lua'), 'utf8');
    const sArbEval = await scheduleMessage(signerGenesis, arbitratorId, 'Eval', arbitratorLua);
    await computeAndGetLatestResult(arbitratorId, null, sArbEval.slot);
    const sArbAuth = await scheduleMessage(signerGenesis, arbitratorId, 'AuthorizeCommitter', JSON.stringify({ committer: sd1Addr }));
    await computeAndGetLatestResult(arbitratorId, null, sArbAuth.slot);
    writeFileSync(arbFile, arbitratorId);
  }
  console.log('  Authoritative Arbitrator ID:', arbitratorId);

  const s3 = await scheduleMessage(signerGenesis, genesisId, 'SetArbitratorProcess', JSON.stringify({
    arbitrator_process_id: arbitratorId
  }));
  const arbBindRes = await computeAndGetLatestResult(genesisId, 'SetArbitratorProcessResponse', s3.slot);
  const arbBindData = parsePayload(arbBindRes.targetMsg);
  assert(arbBindData.ok === true, 'SetArbitratorProcess returned ok=true');
  assert.strictEqual(arbBindData.status, 'ARBITRATOR_CONFIGURED', 'Arbitrator configured successfully');
  console.log('  ✅ Arbitrator bound in Genesis state');

  // -------------------------------------------------------------------------
  // STEP 4: Verify hb-sd-1 Bootstraps against Genesis via HTTP Control Plane
  // -------------------------------------------------------------------------
  console.log('\n[Step 4] Bootstrapping hb-sd-1 against Genesis via HTTP control plane...');
  const bootstrapUrl = `${HB_SD1_URL}/~sd@1.0/status?action=bootstrap_genesis&genesis_id=${genesisId}&genesis_hash=${canonicalGenesisHash}`;
  const bootstrapRes = await fetchJson(bootstrapUrl);
  console.log('  Bootstrap response:', JSON.stringify(bootstrapRes));
  assert(bootstrapRes.ok === true || bootstrapRes.ok === 'true', 'Bootstrap returned ok=true');
  assert.strictEqual(bootstrapRes.result, 'bootstrapped', 'Result is bootstrapped');

  // Verify SD-1 operational state
  const sd1Status = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status`);
  let committerInfo = sd1Status;
  if (sd1Status['committer+link']) {
    try {
      committerInfo = await fetchJson(`${HB_SD1_URL}/${sd1Status['committer+link']}`);
    } catch {
      committerInfo = sd1Status;
    }
  }
  const effectiveArbId = sd1Status.arbitrator_id || committerInfo.arbitrator_id;
  const effectiveGenId = sd1Status.genesis_id || committerInfo.genesis_id;
  const effectiveHealth = sd1Status.committer_health || committerInfo.health || sd1Status.health;

  console.log('  SD-1 committer status:', JSON.stringify({
    health: effectiveHealth,
    arbitrator_id: effectiveArbId,
    genesis_id: effectiveGenId
  }));
  assert.strictEqual(effectiveHealth, 'healthy', 'SD-1 committer health is healthy');
  assert.strictEqual(effectiveArbId, arbitratorId, 'SD-1 discovered correct Arbitrator ID from Genesis');
  assert.strictEqual(effectiveGenId, genesisId, 'SD-1 stored correct Genesis ID');
  console.log('  ✅ hb-sd-1 bootstrapped and healthy');

  // -------------------------------------------------------------------------
  // STEP 5: Negative Test 1 — Tampered Genesis Hash Rejection
  // -------------------------------------------------------------------------
  console.log('\n[Step 5] Negative Test 1: Tampered Genesis Hash Rejection...');
  const tamperedHash = 'DEADBEEF00000000000000000000000000000000000000000000000000000000';
  const tamperedUrl = `${HB_SD1_URL}/~sd@1.0/status?action=bootstrap_genesis&genesis_id=${genesisId}&genesis_hash=${tamperedHash}`;
  const tamperedRes = await fetchJson(tamperedUrl);
  console.log('  Tampered hash response:', JSON.stringify(tamperedRes));
  assert(tamperedRes.ok === false || tamperedRes.ok === 'false', 'Tampered bootstrap rejected');
  assert(tamperedRes.error && tamperedRes.error.includes('err_genesis_hash_mismatch'), 'Error specifies err_genesis_hash_mismatch');

  // Verify node transitions to degraded (fail-closed)
  const degradedStatus = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status`);
  let committerDegraded = degradedStatus;
  if (degradedStatus['committer+link']) {
    try {
      committerDegraded = await fetchJson(`${HB_SD1_URL}/${degradedStatus['committer+link']}`);
    } catch {
      committerDegraded = degradedStatus;
    }
  }
  const degradedHealth = degradedStatus.committer_health || committerDegraded.health || degradedStatus.health;
  assert.strictEqual(degradedHealth, 'degraded', 'Node degraded on hash tampering');
  console.log('  ✅ Fail-closed verified: node degraded on tampered hash');

  // Recover node by re-bootstrapping with authentic hash
  const recoverRes = await fetchJson(bootstrapUrl);
  console.log('  Recovery response:', JSON.stringify(recoverRes));
  assert(recoverRes.ok === true || recoverRes.ok === 'true', `Recovery bootstrap succeeded: ${JSON.stringify(recoverRes)}`);
  const recoveredStatus = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status`);
  let committerRecovered = recoveredStatus;
  if (recoveredStatus['committer+link']) {
    try {
      committerRecovered = await fetchJson(`${HB_SD1_URL}/${recoveredStatus['committer+link']}`);
    } catch {
      committerRecovered = recoveredStatus;
    }
  }
  const recoveredHealth = recoveredStatus.committer_health || committerRecovered.health || recoveredStatus.health;
  assert.strictEqual(recoveredHealth, 'healthy', 'Node recovered to healthy');
  console.log('  ✅ Node cleanly recovered with authentic Genesis Hash');

  // -------------------------------------------------------------------------
  // STEP 6: Negative Test 2 — Unauthorized Node Registration Rejection
  // -------------------------------------------------------------------------
  console.log('\n[Step 6] Negative Test 2: Unauthorized Node Registration Rejection...');
  const { privateKey: attackerJwk } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 4096,
    publicKeyEncoding: { type: 'pkcs1', format: 'jwk' },
    privateKeyEncoding: { type: 'pkcs1', format: 'jwk' }
  });
  const attackerSigner = new ArweaveSigner(attackerJwk);
  const attackerAddr = getSignerAddress(attackerSigner);

  const s6 = await scheduleMessage(attackerSigner, genesisId, 'RegisterNode', JSON.stringify({
    node_id: attackerAddr,
    role: 'storage_unit'
  }));
  const unauthRes = await computeAndGetLatestResult(genesisId, 'RegisterNodeResponse', s6.slot);
  const unauthData = parsePayload(unauthRes.targetMsg);
  console.log('  Unauthorized register response:', JSON.stringify(unauthData));
  assert(unauthData.ok === false, 'Unauthorized registration rejected');
  assert.strictEqual(unauthData.status, 'REJECTED_UNAUTHORIZED_NODE', 'Status REJECTED_UNAUTHORIZED_NODE');
  console.log('  ✅ Unauthorized node registration blocked');

  // -------------------------------------------------------------------------
  // STEP 7: Negative Test 3 — Node Impersonation Protection
  // -------------------------------------------------------------------------
  console.log('\n[Step 7] Negative Test 3: Node Impersonation Protection...');
  // SD-2 attempts to register claiming Hot node's identity
  const s7 = await scheduleMessage(signerSd2, genesisId, 'RegisterNode', JSON.stringify({
    node_id: hotAddr, // Impersonating Hot Sequencer!
    role: 'hot_sequencer'
  }));
  const impRes = await computeAndGetLatestResult(genesisId, 'RegisterNodeResponse', s7.slot);
  const impData = parsePayload(impRes.targetMsg);
  console.log('  Impersonation register response:', JSON.stringify(impData));
  assert(impData.ok === false, 'Impersonation registration rejected');
  assert.strictEqual(impData.status, 'REJECTED_NODE_IMPERSONATION', 'Status REJECTED_NODE_IMPERSONATION');
  console.log('  ✅ Identity impersonation blocked');

  // -------------------------------------------------------------------------
  // STEP 8: Dynamic Registration & Heartbeats for All Remaining Cluster Nodes
  // -------------------------------------------------------------------------
  console.log('\n[Step 8] Registering & reporting heartbeats for all cluster nodes...');

  // 1. Hot Sequencer
  console.log('  Registering hb-hot...');
  const s8_1 = await scheduleMessage(signerHot, genesisId, 'RegisterNode', JSON.stringify({
    node_id: hotAddr,
    role: 'hot_sequencer',
    endpoint: HB_HOT_URL
  }));
  const hotRegRes = await computeAndGetLatestResult(genesisId, 'RegisterNodeResponse', s8_1.slot);
  const hotRegData = parsePayload(hotRegRes.targetMsg);
  assert(hotRegData.ok === true, 'Hot Sequencer registered');

  const s8_2 = await scheduleMessage(signerHot, genesisId, 'ReportHeartbeat', JSON.stringify({
    node_id: hotAddr,
    health: 'ACTIVE',
    last_seq: 0
  }));
  const hotHbRes = await computeAndGetLatestResult(genesisId, 'ReportHeartbeatResponse', s8_2.slot);
  const hotHbData = parsePayload(hotHbRes.targetMsg);
  assert(hotHbData.status === 'ACK', 'Hot Sequencer heartbeat acknowledged');
  console.log('  ✅ hb-hot registered & active');

  // 2. SD Replica 2
  console.log('  Registering hb-sd-2...');
  const s8_3 = await scheduleMessage(signerSd2, genesisId, 'RegisterNode', JSON.stringify({
    node_id: sd2Addr,
    role: 'storage_unit',
    endpoint: HB_SD2_URL
  }));
  const sd2RegRes = await computeAndGetLatestResult(genesisId, 'RegisterNodeResponse', s8_3.slot);
  const sd2RegData = parsePayload(sd2RegRes.targetMsg);
  assert(sd2RegData.ok === true, 'SD-2 registered');

  const s8_4 = await scheduleMessage(signerSd2, genesisId, 'ReportHeartbeat', JSON.stringify({
    node_id: sd2Addr,
    health: 'ACTIVE',
    last_seq: 0
  }));
  const sd2HbRes = await computeAndGetLatestResult(genesisId, 'ReportHeartbeatResponse', s8_4.slot);
  const sd2HbData = parsePayload(sd2HbRes.targetMsg);
  assert(sd2HbData.status === 'ACK', 'SD-2 heartbeat acknowledged');
  console.log('  ✅ hb-sd-2 registered & active');

  // 3. SD Replica 3
  console.log('  Registering hb-sd-3...');
  const s8_5 = await scheduleMessage(signerSd3, genesisId, 'RegisterNode', JSON.stringify({
    node_id: sd3Addr,
    role: 'storage_unit',
    endpoint: HB_SD3_URL
  }));
  const sd3RegRes = await computeAndGetLatestResult(genesisId, 'RegisterNodeResponse', s8_5.slot);
  const sd3RegData = parsePayload(sd3RegRes.targetMsg);
  assert(sd3RegData.ok === true, 'SD-3 registered');

  const s8_6 = await scheduleMessage(signerSd3, genesisId, 'ReportHeartbeat', JSON.stringify({
    node_id: sd3Addr,
    health: 'ACTIVE',
    last_seq: 0
  }));
  const sd3HbRes = await computeAndGetLatestResult(genesisId, 'ReportHeartbeatResponse', s8_6.slot);
  const sd3HbData = parsePayload(sd3HbRes.targetMsg);
  assert(sd3HbData.status === 'ACK', 'SD-3 heartbeat acknowledged');
  console.log('  ✅ hb-sd-3 registered & active');

  // 4. Trigger SD Replica 1 Heartbeat via HTTP
  console.log('  Emitting hb-sd-1 heartbeat via HTTP control plane...');
  const sd1HbHttpRes = await fetchJson(`${HB_SD1_URL}/~sd@1.0/status?action=report_heartbeat`);
  console.log('  SD-1 heartbeat HTTP response:', JSON.stringify(sd1HbHttpRes));
  assert(sd1HbHttpRes.ok === true || sd1HbHttpRes.ok === 'true', 'SD-1 heartbeat emitted');
  console.log('  ✅ hb-sd-1 heartbeat active');

  // -------------------------------------------------------------------------
  // STEP 9: Stack Quorum & Operational Readiness Verification
  // -------------------------------------------------------------------------
  console.log('\n[Step 9] Verifying Cluster Quorum Readiness via GetStackStatus...');
  const s9 = await scheduleMessage(signerGenesis, genesisId, 'GetStackStatus', '');
  const stackStatusRes = await computeAndGetLatestResult(genesisId, 'StackStatusResponse', s9.slot);
  const stackStatus = parsePayload(stackStatusRes.targetMsg);

  console.log('  Cluster Quorum State:');
  console.log('    Stack Status           :', stackStatus.status);
  console.log('    Healthy Nodes Count    :', stackStatus.healthy_nodes_count);
  console.log('    Registered Nodes Count :', stackStatus.registered_nodes_count);
  console.log('    Quorum Size            :', stackStatus.quorum_size);
  console.log('    Quorum Mode            :', stackStatus.quorum_mode);
  console.log('    Arbitrator Process ID  :', stackStatus.arbitrator_process_id);

  assert.strictEqual(stackStatus.ok, true, 'GetStackStatus returned ok=true');
  assert.strictEqual(stackStatus.status, 'HEALTHY', 'Stack reached HEALTHY quorum readiness');
  assert(stackStatus.healthy_nodes_count >= 3, 'At least 3 healthy nodes active');
  assert.strictEqual(stackStatus.registered_nodes_count, 4, 'All 4 cluster nodes registered');
  assert.strictEqual(stackStatus.quorum_size, 3, 'Quorum size is 3');
  assert.strictEqual(stackStatus.arbitrator_process_id, arbitratorId, 'Arbitrator ID bound in stack status');

  // Assert each registered node has active status
  assert(stackStatus.nodes[hotAddr] && stackStatus.nodes[hotAddr].status === 'ACTIVE', 'hb-hot is ACTIVE');
  assert(stackStatus.nodes[sd1Addr] && (stackStatus.nodes[sd1Addr].status === 'ACTIVE' || stackStatus.nodes[sd1Addr].status === 'REGISTERED'), 'hb-sd-1 is registered/active');
  assert(stackStatus.nodes[sd2Addr] && stackStatus.nodes[sd2Addr].status === 'ACTIVE', 'hb-sd-2 is ACTIVE');
  assert(stackStatus.nodes[sd3Addr] && stackStatus.nodes[sd3Addr].status === 'ACTIVE', 'hb-sd-3 is ACTIVE');

  console.log('\n========================================================================');
  console.log('🎉 TAHAP 16: GENESIS BOOTSTRAP & DYNAMIC REGISTRATION PASSED 100%');
  console.log('========================================================================');
}

main().catch(err => {
  console.error('\n❌ FATAL TEST FAILURE in Tahap 16:');
  console.error(err);
  process.exit(1);
});
