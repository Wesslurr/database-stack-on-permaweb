#!/usr/bin/env node
/**
 * Real AO WebAssembly Process Evaluator & ArLocal Committer
 * =========================================================
 * Mengeksekusi pesan di dalam WebAssembly AO Sandbox resmi (@permaweb/ao-loader)
 * dan meng-commit hasil evaluasi yang valid ke block ledger ArLocal (:1984).
 */

import AoLoader from '/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm/genesis-wasm-server/node_modules/@permaweb/ao-loader/dist/index.cjs';
import Arweave from 'arweave';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import crypto from 'node:crypto';

const DB_DIR = '/home/weslur/database';
const WALLET_PATH = join(DB_DIR, 'secrets/wallet.json');
const WASM_PATH = join(DB_DIR, 'vendor/HyperBEAM/_build/genesis_wasm/genesis-wasm-server/test/processes/happy/process.wasm');
const PROCESS_ID_FILE = join(DB_DIR, 'ao_process/process_id.txt');

const arweave = Arweave.init({
  host: '127.0.0.1',
  port: 1984,
  protocol: 'http',
});

let wasmMemory = null;
let aoHandle = null;

async function getWallet() {
  const jwk = JSON.parse(readFileSync(WALLET_PATH, 'utf-8'));
  const address = await arweave.wallets.jwkToAddress(jwk);
  return { jwk, address };
}

async function mineBlock() {
  return await fetch('http://127.0.0.1:1984/mine').then(r => r.json());
}

export async function initAoLoader() {
  if (aoHandle) return aoHandle;
  const wasmBinary = readFileSync(WASM_PATH);
  const moduleOptions = {
    format: 'wasm32-unknown-emscripten',
    inputEncoding: 'JSON-1',
    outputEncoding: 'JSON-1',
    memoryLimit: 524_288_000,
    computeLimit: 9_000_000_000_000,
    extensions: []
  };
  aoHandle = await AoLoader(wasmBinary, moduleOptions);
  console.log('[AO WASM Engine] Official AO Process WASM Sandbox initialized successfully.');
  return aoHandle;
}

export async function evaluateAndCommitCheckpoint(batch) {
  const handle = await initAoLoader();
  const { jwk, address } = await getWallet();
  const processId = existsSync(PROCESS_ID_FILE)
    ? readFileSync(PROCESS_ID_FILE, 'utf-8').trim()
    : 'SATU_KEPALA_OFFICIAL_PROCESS';

  // 1. Audit Kriptografis Rantai Hash SHA-256
  const events = batch.events || [];
  let runningHash = batch.initial_hash || "0000000000000000000000000000000000000000000000000000000000000000";
  
  for (const ev of events) {
    const seq = Number(ev.seq);
    const action = ev.action || ev.op || 'put';
    const key = Number(ev.key);
    const val = Number(ev.val || 0);
    const actionCode = (action === 'del') ? '22' : '20';
    
    const payload = (action === 'del')
      ? `${runningHash}${seq}${actionCode}${key}`
      : `${runningHash}${seq}${actionCode}${key}${val}`;
    
    const expectedHash = crypto.createHash('sha256').update(payload).digest('hex').toUpperCase();
    const givenHash = String(ev.hash).toUpperCase();

    if (expectedHash !== givenHash) {
      return {
        ok: false,
        status: 'AUDIT_REJECTED',
        reason: `Hash chain broken at seq ${seq}! Expected ${expectedHash}, got ${givenHash}`,
        wasit_verdict: 'FRAUD_DETECTED_AND_CUT'
      };
    }
    runningHash = givenHash;
  }

  // 2. Evaluasi Message di dalam WebAssembly AO Sandbox (@permaweb/ao-loader)
  const aoMessage = {
    Target: processId,
    From: address,
    Owner: address,
    Timestamp: Date.now(),
    Tags: [
      { name: 'Data-Protocol', value: 'ao' },
      { name: 'Variant', value: 'ao.TN.1' },
      { name: 'Type', value: 'Message' },
      { name: 'Action', value: 'CommitCheckpoint' },
      { name: 'From-Seq', value: String(batch.from_seq) },
      { name: 'To-Seq', value: String(batch.to_seq) },
      { name: 'State-Root', value: String(batch.proposed_state_root) },
      { name: 'Hash', value: String(batch.proposed_hash) },
      { name: 'function', value: 'hello' } // WASM internal handler route
    ],
    Data: JSON.stringify(batch)
  };

  const env = { Process: { Id: processId, Tags: [] } };
  const aoExecRes = await handle(wasmMemory, aoMessage, env);
  wasmMemory = aoExecRes.Memory; // Update persistent WASM linear memory state

  // 3. Tanda tangani & Commit Transaksi Checkpoint ke ArLocal (:1984)
  const txPayload = Buffer.from(JSON.stringify({
    checkpoint: {
      from_seq: batch.from_seq,
      to_seq: batch.to_seq,
      state_root: batch.proposed_state_root,
      final_hash: batch.proposed_hash,
      events_count: events.length
    },
    ao_wasm_output: aoExecRes.Output,
    timestamp: Date.now()
  }));

  const tx = await arweave.createTransaction({ data: txPayload }, jwk);
  tx.addTag('App-Name', 'AO');
  tx.addTag('Type', 'Message');
  tx.addTag('Data-Protocol', 'ao');
  tx.addTag('Variant', 'ao.TN.1');
  tx.addTag('Recipient', processId);
  tx.addTag('Action', 'CommitCheckpoint');
  tx.addTag('From-Seq', String(batch.from_seq));
  tx.addTag('To-Seq', String(batch.to_seq));
  tx.addTag('State-Root', String(batch.proposed_state_root));
  tx.addTag('Hash', String(batch.proposed_hash));

  await arweave.transactions.sign(tx, jwk);
  const postRes = await arweave.transactions.post(tx);
  if (postRes.status !== 200 && postRes.status !== 202) {
    throw new Error(`Failed to post tx to ArLocal: ${postRes.status}`);
  }

  // 4. Tambang blok di ArLocal untuk finalitas permanen
  const mineRes = await mineBlock();

  return {
    ok: true,
    status: 'COMMITTED',
    wasit_verdict: 'PASSED_AND_COMMITTED',
    process_id: processId,
    from_seq: batch.from_seq,
    to_seq: batch.to_seq,
    state_root: batch.proposed_state_root,
    final_hash: batch.proposed_hash,
    arweave_tx_id: tx.id,
    block_height: mineRes.height,
    block_hash: mineRes.current,
    ao_wasm_evaluated: true
  };
}

async function cli() {
  const args = process.argv.slice(2);
  const cmd = args[0] || 'init';

  if (cmd === 'init') {
    await initAoLoader();
    console.log(JSON.stringify({ ok: true, status: 'AO_LOADER_READY' }));
  } else if (cmd === 'commit') {
    const filePath = args[1];
    if (!filePath) {
      console.error('Usage: real_ao_evaluator.mjs commit <batch_file.json>');
      process.exit(1);
    }
    const batch = JSON.parse(readFileSync(filePath, 'utf-8'));
    const res = await evaluateAndCommitCheckpoint(batch);
    console.log(JSON.stringify(res, null, 2));
  }
}

if (process.argv[1]?.endsWith('real_ao_evaluator.mjs')) {
  cli().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
