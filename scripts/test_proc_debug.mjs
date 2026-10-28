import { readFileSync } from 'node:fs';
import * as arbundles from 'arbundles';

const HB_URL = 'http://127.0.0.1:8733';
const genesisJwk = JSON.parse(readFileSync('secrets/wallet_genesis.json', 'utf8'));
const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
const signer = new ArweaveSigner(genesisJwk);

const createData = arbundles.createData || arbundles.default?.createData;

const info = await fetch(`${HB_URL}/~meta@1.0/info`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
console.log('Scheduler address:', info.address);

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

console.log('Pushing spawn item...');
const spawnRes = await fetch(`${HB_URL}/~push@1.0/push`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
  body: spawnItem.getRaw()
}).then(r => r.json());
console.log('Spawn response:', spawnRes);
const procId = spawnItem.id;
console.log('Process ID:', procId);

console.log('Scheduling message...');
const msgItem = createData('Handlers.add("ping", Handlers.utils.hasMatchingTag("Action", "Ping"), function(m) ao.send({Target=m.From, Action="Pong"}) end)', signer, {
  tags: [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: 'Eval' }
  ]
});
await msgItem.sign(signer);

const schedRes = await fetch(`${HB_URL}/${procId}~process@1.0/schedule`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
  body: msgItem.getRaw()
}).then(r => r.json());
console.log('Schedule response:', schedRes);

console.log('Checking /now...');
for (let i = 0; i < 5; i++) {
  const now = await fetch(`${HB_URL}/${procId}~process@1.0/now`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
  console.log(`Now [${i}]: at-slot:`, now['at-slot'], 'results+link:', now['results+link']);
  if (now['results+link']) {
    const res = await fetch(`${HB_URL}/${now['results+link']}`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
    console.log('Results:', res);
    break;
  }
  await new Promise(r => setTimeout(r, 1000));
}
