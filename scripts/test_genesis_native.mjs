import { readFileSync } from 'node:fs';
import * as arbundles from 'arbundles';

async function main() {
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const createData = arbundles.createData || arbundles.default?.createData;

  const wallet = JSON.parse(readFileSync('secrets/wallet_su.json', 'utf8'));
  const signer = new ArweaveSigner(wallet);

  const scheduler = 'XpOEPX668w7VNicVDHK81unzeeH8HcD83fKZRZKx9pk';
  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  console.log('=== Step 1: Spawning Genesis AO Process in HyperBEAM ===');
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

  const procId = spawnItem.id;
  console.log('Genesis Process ID:', procId);

  const spawnRes = await fetch('http://127.0.0.1:8734/~push@1.0/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: spawnItem.getRaw()
  });

  console.log('Spawn Status:', spawnRes.status);

  console.log('\n=== Step 2: Loading genesis.lua via Action: Eval ===');
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

  const evalRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/schedule`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: evalItem.getRaw()
  });
  console.log('Eval Schedule Status:', evalRes.status);

  console.log('\n=== Step 3: Triggering Compute (Now) ===');
  const nowRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/now`, {
    headers: { 'Accept': 'application/json' }
  });
  console.log('Now Status:', nowRes.status);
  const nowJson = await nowRes.json();
  console.log('Now Slot:', nowJson['at-slot']);

  if (nowJson['results+link']) {
    const resRes = await fetch(`http://127.0.0.1:8734/${nowJson['results+link']}`, {
      headers: { 'Accept': 'application/json' }
    });
    console.log('Eval Results Data:', (await resRes.json()).data);
  }

  console.log('\n=== Step 4: Dispatching Action: GetGenesisConfig ===');
  const queryItem = createData('', signer, {
    tags: [
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Type', value: 'Message' },
      { name: 'Target', value: procId },
      { name: 'Action', value: 'GetGenesisConfig' }
    ]
  });
  await queryItem.sign(signer);

  const queryRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/schedule`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: queryItem.getRaw()
  });
  console.log('Query Schedule Status:', queryRes.status);

  console.log('\n=== Step 5: Triggering Compute for GetGenesisConfig ===');
  const nowQueryRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/now`, {
    headers: { 'Accept': 'application/json' }
  });
  console.log('Now Query Status:', nowQueryRes.status);
  const nowQueryJson = await nowQueryRes.json();
  console.log('Now Slot after Query:', nowQueryJson['at-slot']);

  if (nowQueryJson['results+link']) {
    const resRes = await fetch(`http://127.0.0.1:8734/${nowQueryJson['results+link']}`, {
      headers: { 'Accept': 'application/json' }
    });
    const resData = await resRes.json();
    console.log('Query Outbox Link:', resData['outbox+link']);
    if (resData['outbox+link']) {
      const outboxRes = await fetch(`http://127.0.0.1:8734/${resData['outbox+link']}`, {
        headers: { 'Accept': 'application/json' }
      });
      console.log('Outbox Content:', await outboxRes.text());
    }
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
