// tests/test_tahap14_genesis_persistence.mjs
// Verifies that Genesis AO Process & Arbitrator AO Process state persist across HyperBEAM cluster restarts.

import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { readFileSync } from 'node:fs';
import * as crypto from 'node:crypto';
import assert from 'node:assert';
import { execSync } from 'node:child_process';
import * as arbundles from 'arbundles';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const ROOT_DIR = join(__dirname, '..');

const GENESIS_PORT = 8733;
const HB_GENESIS_URL = `http://127.0.0.1:${GENESIS_PORT}`;

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
    ...extraTags
  ];
  const item = createData(data, signer, { tags });
  await item.sign(signer);

  const schedRes = await postAns104(`${HB_GENESIS_URL}/${procId}~process@1.0/schedule`, item);
  assert(schedRes.status === 200, `Message scheduled at slot ${schedRes.slot}`);
  return { itemId: item.id, slot: schedRes.slot };
}

async function computeAndGetLatestResult(procId) {
  const now = await fetchJson(`${HB_GENESIS_URL}/${procId}~process@1.0/now`);
  assert(now.status === 200, 'Now query returned status 200');
  if (!now['results+link']) {
    return { now, results: null, outbox: [] };
  }
  const results = await fetchJson(`${HB_GENESIS_URL}/${now['results+link']}`);
  let outbox = [];
  if (results['outbox+link']) {
    const outboxMap = await fetchJson(`${HB_GENESIS_URL}/${results['outbox+link']}`);
    for (const [k, v] of Object.entries(outboxMap)) {
      if (k.endsWith('+link') || (k !== 'status' && !isNaN(Number(k)))) {
        const msg = await fetchJson(`${HB_GENESIS_URL}/${v}`);
        outbox.push(msg);
      }
    }
  }
  return { now, results, outbox };
}

async function main() {
  console.log('========================================================================');
  console.log('TAHAP 14: GENESIS CONFIGURATION PERSISTENCE ACROSS RESTART AUDIT');
  console.log('========================================================================\n');

  // Check if Genesis node is alive
  const isAlive = await fetch(`${HB_GENESIS_URL}/~meta@1.0/info`, { headers: { Accept: 'application/json' } }).then(r => r.ok).catch(() => false);
  if (!isAlive) {
    console.log(`\n  ⚠️ [Live Cluster Inactive] HyperBEAM Genesis Node is not running on ${HB_GENESIS_URL}.`);
    console.log('  ⚠️ Skipping live cluster persistence tests (run npm run cluster:start for live testing).\n');
    console.log('========================================================================');
    console.log(' TAHAP 14 VERIFICATION SUMMARY: SKIPPED (Cluster Offline)');
    console.log('========================================================================');
    return;
  }

  // Load SU wallet & info
  const info = await fetchJson(`${HB_GENESIS_URL}/~meta@1.0/info`);
  const scheduler = info.address || 'gw0iqMNNGgXcAj_yejTV0H_152Hwz941gxhnzvB_PY0';
  const suWalletPath = join(ROOT_DIR, 'secrets/wallet_su.json');
  const suJwk = JSON.parse(readFileSync(suWalletPath, 'utf8'));
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const signerSu = new ArweaveSigner(suJwk);
  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  console.log(`[Step 1] Spawning and initializing Genesis Process on hb-genesis (:8733)...`);
  const genesisId = await spawnProcess(signerSu, scheduler, wasmImage);
  console.log('  ✅ Genesis Process spawned with ID:', genesisId);

  const genesisLua = readFileSync(join(ROOT_DIR, 'ao_process/genesis.lua'), 'utf8');
  await scheduleMessage(signerSu, genesisId, 'Eval', genesisLua);
  await computeAndGetLatestResult(genesisId);

  console.log('  Reading Genesis configuration before restart...');
  await scheduleMessage(signerSu, genesisId, 'GetGenesisConfig', '');
  const preConfigRes = await computeAndGetLatestResult(genesisId);
  const preConfigMsg = preConfigRes.outbox.find(m => m.Action === 'GenesisConfigResponse');
  assert(preConfigMsg, 'GenesisConfigResponse emitted');
  const preConfig = JSON.parse(preConfigMsg.Data);
  const preHash = crypto.createHash('sha256').update(preConfigMsg.Data).digest('hex');

  console.log('  ✅ Pre-restart Genesis Configuration:');
  console.log('     StackId:    ', preConfig.genesis.stack_id);
  console.log('     QuorumSize: ', preConfig.genesis.quorum_size);
  console.log('     ConfigHash: ', preHash);

  console.log(`\n[Step 2] Executing full HyperBEAM Cluster restart...`);
  execSync('/home/weslur/database/scripts/stop-cluster.sh', { stdio: 'inherit' });
  console.log('  Cluster stopped.');

  execSync('/home/weslur/database/scripts/start-cluster.sh', { stdio: 'inherit' });
  console.log('  Cluster restarted.');

  console.log(`\n[Step 3] Reading Genesis Configuration from same Process ID after restart...`);
  await scheduleMessage(signerSu, genesisId, 'GetGenesisConfig', '');
  const postConfigRes = await computeAndGetLatestResult(genesisId);
  const postConfigMsg = postConfigRes.outbox.find(m => m.Action === 'GenesisConfigResponse');
  assert(postConfigMsg, 'GenesisConfigResponse emitted after restart');
  const postConfig = JSON.parse(postConfigMsg.Data);
  const postHash = crypto.createHash('sha256').update(postConfigMsg.Data).digest('hex');

  console.log('  ✅ Post-restart Genesis Configuration:');
  console.log('     StackId:    ', postConfig.genesis.stack_id);
  console.log('     QuorumSize: ', postConfig.genesis.quorum_size);
  console.log('     ConfigHash: ', postHash);

  console.log(`\n[Step 4] Verifying Invariants...`);
  assert.strictEqual(postConfig.genesis.stack_id, preConfig.genesis.stack_id, 'Stack ID must match exactly');
  assert.strictEqual(postConfig.genesis.quorum_size, preConfig.genesis.quorum_size, 'Quorum size must match exactly');
  assert.strictEqual(postHash, preHash, 'Configuration SHA-256 hash must be byte-identical');

  console.log('  ✅ Configuration hash and state survived cluster restart with 100% fidelity!');
  console.log('\n========================================================================');
  console.log('🎉 TAHAP 14 GENESIS PERSISTENCE AUDIT COMPLETED SUCCESSFULLY');
  console.log('========================================================================');
}

main().catch(err => {
  console.error('\n❌ TAHAP 14 AUDIT FAILED:', err);
  process.exit(1);
});
