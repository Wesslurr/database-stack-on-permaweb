#!/usr/bin/env node
/**
 * AO Process Committer & ArLocal Bridge — Lapisan 4
 * ==================================================
 * Menjembatani Hot Node / Pool SU ke Arweave (ArLocal).
 * Meng-commit batch event checkpoint secara asinkron ke blockchain.
 */

import Arweave from 'arweave';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');
const WALLET_PATH = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');
const PROCESS_ID_FILE = join(DB_DIR, 'ao_process/process_id.txt');
const LUA_PATH = join(DB_DIR, 'ao_process/arbitrator.lua');

const arweave = Arweave.init({
  host: '127.0.0.1',
  port: 1984,
  protocol: 'http',
});

async function getWallet() {
  const jwk = JSON.parse(readFileSync(WALLET_PATH, 'utf-8'));
  const address = await arweave.wallets.jwkToAddress(jwk);
  return { jwk, address };
}

async function mineBlock() {
  const res = await fetch('http://127.0.0.1:1984/mine').then(r => r.json());
  return res;
}

export async function getOrCreateProcessId() {
  if (existsSync(PROCESS_ID_FILE)) {
    const id = readFileSync(PROCESS_ID_FILE, 'utf-8').trim();
    if (id) return id;
  }

  const { jwk, address } = await getWallet();
  const luaCode = existsSync(LUA_PATH) ? readFileSync(LUA_PATH, 'utf-8') : '-- AO Arbitrator';

  const tx = await arweave.createTransaction({
    data: Buffer.from(luaCode),
  }, jwk);

  tx.addTag('App-Name', 'AO');
  tx.addTag('Type', 'Process');
  tx.addTag('Data-Protocol', 'ao');
  tx.addTag('Variant', 'ao.TN.1');
  tx.addTag('Name', 'HyDB-Official-Arbitrator');
  tx.addTag('Owner', address);
  tx.addTag('Timestamp', Date.now().toString());

  await arweave.transactions.sign(tx, jwk);
  const resp = await arweave.transactions.post(tx);
  if (resp.status !== 200 && resp.status !== 202) {
    throw new Error(`Failed to post process creation tx: ${resp.status}`);
  }

  await mineBlock();
  writeFileSync(PROCESS_ID_FILE, tx.id);
  console.log(`[AO Process] Created new 'Satu Kepala' Process ID: ${tx.id}`);
  return tx.id;
}

export async function commitCheckpointBatch(batchData) {
  const processId = await getOrCreateProcessId();
  const { jwk } = await getWallet();

  const payload = Buffer.from(JSON.stringify(batchData));
  const tx = await arweave.createTransaction({ data: payload }, jwk);

  tx.addTag('App-Name', 'AO');
  tx.addTag('Type', 'Message');
  tx.addTag('Data-Protocol', 'ao');
  tx.addTag('Variant', 'ao.TN.1');
  tx.addTag('Recipient', processId);
  tx.addTag('Action', 'CommitCheckpoint');
  tx.addTag('From-Seq', String(batchData.from_seq));
  tx.addTag('To-Seq', String(batchData.to_seq));
  tx.addTag('State-Root', String(batchData.proposed_state_root));
  tx.addTag('Hash', String(batchData.proposed_hash));
  tx.addTag('Events-Count', String((batchData.events || []).length));
  tx.addTag('Timestamp', Date.now().toString());

  await arweave.transactions.sign(tx, jwk);
  const resp = await arweave.transactions.post(tx);
  if (resp.status !== 200 && resp.status !== 202) {
    throw new Error(`Failed to post checkpoint tx: ${resp.status}`);
  }

  const mineRes = await mineBlock();
  const txStatus = await arweave.transactions.getStatus(tx.id);

  return {
    ok: true,
    tx_id: tx.id,
    process_id: processId,
    from_seq: batchData.from_seq,
    to_seq: batchData.to_seq,
    state_root: batchData.proposed_state_root,
    final_hash: batchData.proposed_hash,
    block_height: mineRes.height,
    block_hash: mineRes.current,
    confirmed: txStatus.status === 200,
  };
}

async function cli() {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'init';

  if (cmd === 'init') {
    const id = await getOrCreateProcessId();
    console.log(JSON.stringify({ ok: true, process_id: id }));
  } else if (cmd === 'commit') {
    const filePath = args[1];
    if (!filePath) {
      console.error('Usage: committer.mjs commit <batch_json_path>');
      process.exit(1);
    }
    const batchData = JSON.parse(readFileSync(filePath, 'utf-8'));
    const res = await commitCheckpointBatch(batchData);
    console.log(JSON.stringify(res, null, 2));
  } else {
    console.error(`Unknown command: ${cmd}`);
    process.exit(1);
  }
}

// Run CLI if invoked directly
if (process.argv[1]?.endsWith('committer.mjs')) {
  cli().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
