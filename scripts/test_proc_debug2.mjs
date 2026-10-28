import { readFileSync } from 'node:fs';
import * as arbundles from 'arbundles';
import * as crypto from 'node:crypto';

const HB_URL = 'http://127.0.0.1:8733';
const genesisJwk = JSON.parse(readFileSync('secrets/wallet_genesis.json', 'utf8'));
const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
const signer = new ArweaveSigner(genesisJwk);
const createData = arbundles.createData || arbundles.default?.createData;

const info = await fetch(`${HB_URL}/~meta@1.0/info`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());

const spawnTags = [
  { name: 'Data-Protocol', value: 'ao' },
  { name: 'Variant', value: 'ao.TN.1' },
  { name: 'Type', value: 'Process' },
  { name: 'Device', value: 'process@1.0' },
  { name: 'Scheduler-Device', value: 'scheduler@1.0' },
  { name: 'Execution-Device', value: 'stack@1.0' },
  { name: 'Scheduler', value: info.address },
  { name: 'Image', value: 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI' },
  { name: 'Device-Stack', value: '"wasi@1.0", "json-iface@1.0", "wasm-64@1.0", "multipass@1.0"' },
  { name: 'Output-Prefix', value: 'wasm' },
  { name: 'Patch-From', value: '/results/outbox' },
  { name: 'Passes', value: '2' },
  { name: 'Stack-Keys', value: '"init", "compute", "snapshot", "normalize"' },
  { name: 'Ao-Types', value: 'device-stack="list", passes="integer", stack-keys="list"' }
];

const spawnItem = createData('', signer, { tags: spawnTags });
await spawnItem.sign(signer);

const spawnRes = await fetch(`${HB_URL}/~push@1.0/push`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
  body: spawnItem.getRaw()
}).then(r => r.json());
const procId = spawnItem.id;
console.log('Process ID:', procId);

// 1. Eval genesis.lua
console.log('Sending Eval genesis.lua...');
const genesisLua = readFileSync('ao_process/genesis.lua', 'utf8');
const evalItem = createData(genesisLua, signer, {
  tags: [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: 'Eval' }
  ]
});
await evalItem.sign(signer);
await fetch(`${HB_URL}/${procId}~process@1.0/schedule`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
  body: evalItem.getRaw()
}).then(r => r.json());

// Wait for slot 1
while (true) {
  const now = await fetch(`${HB_URL}/${procId}~process@1.0/now`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
  if (parseInt(now['at-slot'], 10) >= 1) {
    console.log('Eval completed at slot:', now['at-slot']);
    break;
  }
  await new Promise(r => setTimeout(r, 500));
}

// 2. Send UpdateStackConfig
console.log('Sending UpdateStackConfig...');
const payload = JSON.stringify({
  genesis_hash: '521CA8AAB700FDE9CFD0B5AD8FA95DC7145F97A5D2C7CA090D083FBB2F01EFFA',
  quorum_size: 3,
  quorum_mode: 'majority'
});

const cfgItem = createData(payload, signer, {
  tags: [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: 'UpdateStackConfig' },
    { name: 'Timestamp', value: Date.now().toString() }
  ]
});
await cfgItem.sign(signer);
await fetch(`${HB_URL}/${procId}~process@1.0/schedule`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
  body: cfgItem.getRaw()
}).then(r => r.json());

// Wait for slot 2
for (let attempt = 0; attempt < 10; attempt++) {
  const now = await fetch(`${HB_URL}/${procId}~process@1.0/now`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
  console.log(`Slot query [${attempt}]: at-slot: ${now['at-slot']}`);
  if (parseInt(now['at-slot'], 10) >= 2 && now['results+link']) {
    const res = await fetch(`${HB_URL}/${now['results+link']}`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
    console.log('Slot 2 results:', res);
    if (res['outbox+link']) {
      const outbox = await fetch(`${HB_URL}/${res['outbox+link']}`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
      console.log('Slot 2 outbox map:', outbox);
      for (const [k, v] of Object.entries(outbox)) {
        if (k !== 'status') {
          const msg = await fetch(`${HB_URL}/${v}`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
          console.log(`Outbox [${k}]:`, msg);
        }
      }
    }
    break;
  }
  await new Promise(r => setTimeout(r, 1000));
}
