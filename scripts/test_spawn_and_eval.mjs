import { readFileSync } from 'node:fs';
import * as arbundles from 'arbundles';

async function main() {
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const createData = arbundles.createData || arbundles.default?.createData;

  const wallet = JSON.parse(readFileSync('secrets/wallet_su.json', 'utf8'));
  const signer = new ArweaveSigner(wallet);

  const scheduler = 'XpOEPX668w7VNicVDHK81unzeeH8HcD83fKZRZKx9pk';
  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  console.log('=== Step 1: Spawning AOS Process ===');
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
  console.log('Spawning Process ID:', procId);

  const spawnRes = await fetch('http://127.0.0.1:8734/~push@1.0/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: spawnItem.getRaw()
  });

  console.log('Spawn HTTP Status:', spawnRes.status);
  const spawnBody = await spawnRes.text();
  console.log('Spawn Response:', spawnBody);

  console.log('\n=== Step 2: Scheduling Eval Message via /schedule ===');
  const luaCode = 'print("Hello from Native HyperBEAM AOS!")\nreturn 40+2';
  const msgTags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: 'Eval' }
  ];

  const msgItem = createData(luaCode, signer, { tags: msgTags });
  await msgItem.sign(signer);

  console.log('Eval Msg ID:', msgItem.id);

  // POST directly to the process's /schedule endpoint:
  const schedRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/schedule`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: msgItem.getRaw()
  });

  console.log('Sched HTTP Status:', schedRes.status);
  const schedBody = await schedRes.text();
  console.log('Sched Response:', schedBody);

  console.log('\n=== Step 3: Triggering Compute / Querying State (Now) ===');
  const nowRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/now`, {
    headers: { 'Accept': 'application/json' }
  });

  console.log('Now HTTP Status:', nowRes.status);
  const nowBody = await nowRes.text();
  console.log('Now Response:', nowBody);

  const parsedNow = JSON.parse(nowBody);
  if (parsedNow['results+link']) {
    const resRes = await fetch(`http://127.0.0.1:8734/${parsedNow['results+link']}`, {
      headers: { 'Accept': 'application/json' }
    });
    console.log('Results Body:', await resRes.text());
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
