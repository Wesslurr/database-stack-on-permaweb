import { readFileSync } from 'node:fs';
import * as crypto from 'node:crypto';
import * as arbundles from 'arbundles';

const mask64 = 0xffffffffffffffffn;
function canonicalHash64(keyInt, valInt, verInt) {
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

function buildValidEvents(startSeq, count, prevHashInit, prevRootInit) {
  const events = [];
  let prevHash = prevHashInit;
  let root = BigInt(prevRootInit);
  for (let i = 0; i < count; i++) {
    const seq = startSeq + i;
    const key = 100 + seq;
    const val = seq * 1000;
    const ver = 1;
    const payload = `${prevHash}${seq}20${key}${val}`;
    const hash = crypto.createHash('sha256').update(payload).digest('hex').toUpperCase();
    const recHash = canonicalHash64(key, val, ver);
    root = BigInt.asIntN(64, (root ^ recHash) & mask64);
    events.push({
      contract_version: 'hydb.core@1.0',
      epoch: 1,
      seq,
      namespace: 'default',
      action: 'put',
      key,
      val,
      val_type: 'int64',
      canonical_val: String(val),
      ver,
      prev_hash: prevHash,
      hash,
      state_root: root.toString(),
      state_root_str: root.toString()
    });
    prevHash = hash;
  }
  return events;
}

async function main() {
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const createData = arbundles.createData || arbundles.default?.createData;

  const wallet = JSON.parse(readFileSync('secrets/wallet_su.json', 'utf8'));
  const signer = new ArweaveSigner(wallet);

  // Compute SU address from RSA wallet n
  const suAddress = crypto.createHash('sha256').update(Buffer.from(wallet.n, 'base64url')).digest('base64url');
  console.log('SU Wallet Address:', suAddress);

  const scheduler = 'XpOEPX668w7VNicVDHK81unzeeH8HcD83fKZRZKx9pk';
  const wasmImage = 'FSq0wtCYfPUVRHprQEB_4hAvQBpMCf_HYnJLn5FuqNI';

  console.log('=== Step 1: Spawning Arbitrator AO Process in HyperBEAM ===');
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
  console.log('Arbitrator Process ID:', procId);

  const spawnRes = await fetch('http://127.0.0.1:8734/~push@1.0/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: spawnItem.getRaw()
  });
  console.log('Spawn Status:', spawnRes.status);

  console.log('\n=== Step 2: Loading arbitrator.lua via Action: Eval ===');
  const arbLua = readFileSync('ao_process/arbitrator.lua', 'utf8');
  const evalItem = createData(arbLua, signer, {
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

  console.log('\n=== Step 3: Triggering Compute (Now) to initialize Arbitrator ===');
  const nowRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/now`, {
    headers: { 'Accept': 'application/json' }
  });
  console.log('Now Status:', nowRes.status);
  const nowJson = await nowRes.json();
  console.log('Now Slot:', nowJson['at-slot']);

  console.log('\n=== Step 4: Authorizing SU Wallet in Arbitrator ===');
  const authCode = `AuthorizedNodes["${suAddress}"] = true; return "AUTHORIZED"`;
  const authItem = createData(authCode, signer, {
    tags: [
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Type', value: 'Message' },
      { name: 'Target', value: procId },
      { name: 'Action', value: 'Eval' }
    ]
  });
  await authItem.sign(signer);

  const authRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/schedule`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: authItem.getRaw()
  });
  console.log('Auth Schedule Status:', authRes.status);

  // Compute Auth
  await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/now`, { headers: { 'Accept': 'application/json' } });

  console.log('\n=== Step 5: Preparing and Committing Batch 1 (seq 1..3) ===');
  const b1Events = buildValidEvents(1, 3, '0000000000000000000000000000000000000000000000000000000000000000', '0');
  const b1PropHash = b1Events[b1Events.length - 1].hash;
  const b1PropRoot = b1Events[b1Events.length - 1].state_root;

  const batch1 = {
    contract_version: 'hydb.core@1.0',
    epoch: 1,
    from_seq: 1,
    to_seq: 3,
    prev_hash: '0000000000000000000000000000000000000000000000000000000000000000',
    proposed_hash: b1PropHash,
    proposed_state_root: b1PropRoot,
    events: b1Events
  };

  const commitItem = createData(JSON.stringify(batch1), signer, {
    tags: [
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Type', value: 'Message' },
      { name: 'Target', value: procId },
      { name: 'Action', value: 'CommitCheckpoint' }
    ]
  });
  await commitItem.sign(signer);

  const commitRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/schedule`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: commitItem.getRaw()
  });
  console.log('Commit Schedule Status:', commitRes.status);

  console.log('\n=== Step 6: Computing Checkpoint Verdict (Now) ===');
  const nowCommitRes = await fetch(`http://127.0.0.1:8734/${procId}~process@1.0/now`, {
    headers: { 'Accept': 'application/json' }
  });
  console.log('Now Commit Status:', nowCommitRes.status);
  const nowCommitJson = await nowCommitRes.json();
  console.log('Now Commit Slot:', nowCommitJson['at-slot']);

  if (nowCommitJson['results+link']) {
    const resRes = await fetch(`http://127.0.0.1:8734/${nowCommitJson['results+link']}`, {
      headers: { 'Accept': 'application/json' }
    });
    const resData = await resRes.json();
    console.log('Commit Outbox Link:', resData['outbox+link']);
    if (resData['outbox+link']) {
      const outboxRes = await fetch(`http://127.0.0.1:8734/${resData['outbox+link']}`, {
        headers: { 'Accept': 'application/json' }
      });
      const outboxJson = await outboxRes.json();
      console.log('Outbox Message Keys:', Object.keys(outboxJson));
      const firstKey = Object.keys(outboxJson).find(k => k.includes('+link') || k === '1');
      if (firstKey && outboxJson[firstKey]) {
        const msgLink = outboxJson[firstKey];
        const verdictRes = await fetch(`http://127.0.0.1:8734/${msgLink}`, {
          headers: { 'Accept': 'application/json' }
        });
        const verdict = await verdictRes.json();
        console.log('\n🎉 VERDICT RECEIVED FROM ARBITRATOR AO PROCESS IN HYPERBEAM:');
        console.log(JSON.stringify(verdict, null, 2));

        if (verdict.Status === 'COMMITTED' && String(verdict.OfficialSeq) === '3') {
          console.log('\n✅ GATE 1 VERIFICATION PASSED: Arbitrator committed Batch 1 at sequence 3!');
        } else {
          console.error('\n❌ Gate 1 verification failed: unexpected verdict:', verdict);
          process.exit(1);
        }
      }
    }
  }
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
