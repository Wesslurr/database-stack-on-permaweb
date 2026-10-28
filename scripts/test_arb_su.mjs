import arbundles from 'arbundles';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import assert from 'assert';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = join(__dirname, '..');

const createData = arbundles.createData || arbundles.default?.createData;
const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
const suJwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_su.json'), 'utf8'));
const signerSu = new ArweaveSigner(suJwk);
const suAddress = readFileSync(join(ROOT_DIR, 'secrets/address_su.txt'), 'utf8').trim();

const scheduler = 'gw0iqMNNGgXcAj_yejTV0H_152Hwz941gxhnzvB_PY0';
const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

async function fetchJson(url) {
  const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}: ${await res.text()}`);
  return res.json();
}

async function postAns104(url, item) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
    body: item.getRaw()
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} on ${url}: ${await res.text()}`);
  return res.json();
}

async function spawnProcess() {
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
  const item = createData('', signerSu, { tags: spawnTags });
  await item.sign(signerSu);
  await postAns104('http://127.0.0.1:8733/~push@1.0/push', item);
  return item.id;
}

async function scheduleMessage(procId, action, data) {
  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: action }
  ];
  const item = createData(data, signerSu, { tags });
  await item.sign(signerSu);
  return await postAns104(`http://127.0.0.1:8733/${procId}~process@1.0/schedule`, item);
}

async function computeResult(procId) {
  const comp = await fetchJson(`http://127.0.0.1:8733/${procId}~process@1.0/compute`);
  assert(comp['results+link'], 'compute returned results+link');
  const results = await fetchJson(`http://127.0.0.1:8733/${comp['results+link']}`);
  let outbox = [];
  if (results['outbox+link']) {
    const outboxMap = await fetchJson(`http://127.0.0.1:8733/${results['outbox+link']}`);
    for (const [k, v] of Object.entries(outboxMap)) {
      if (k !== 'status' && !isNaN(Number(k))) {
        const msg = await fetchJson(`http://127.0.0.1:8733/${v}`);
        outbox.push(msg);
      }
    }
  }
  return { comp, results, outbox };
}

async function test() {
  console.log('Spawning Arbitrator with signerSu...');
  const procId = await spawnProcess();
  console.log('Spawned procId:', procId);

  console.log('Loading arbitrator.lua...');
  const lua = readFileSync(join(ROOT_DIR, 'ao_process/arbitrator.lua'), 'utf8');
  await scheduleMessage(procId, 'Eval', lua);
  const r1 = await computeResult(procId);
  console.log('Slot 0 result data:', r1.results.data ? r1.results.data.slice(0, 100) : 'none');

  console.log('Authorizing SU...');
  await scheduleMessage(procId, 'Eval', `AuthorizedNodes["${suAddress}"] = true; return "AUTHORIZED"`);
  const r2 = await computeResult(procId);
  console.log('Slot 1 result data:', r2.results.data);

  console.log('Querying GetOfficialState...');
  await scheduleMessage(procId, 'GetOfficialState', '');
  const r3 = await computeResult(procId);
  console.log('Slot 2 outbox messages count:', r3.outbox.length);
  if (r3.outbox.length > 0) {
    console.log('OfficialStateResponse:', JSON.stringify(r3.outbox[0]));
  }
}
test().catch(console.error);
