#!/usr/bin/env node
/**
 * Official AO Process Deployment & Bootstrapping Script
 * ====================================================
 * Spawns an official AO Process on the Permaweb using @permaweb/aoconnect,
 * loads the Arbitrator Lua state machine (arbitrator.lua), and sets up the
 * 'Satu Kepala' decentralized truth arbiter for HyDB.
 *
 * Usage:
 *   node ao_process/deploy_ao_process.mjs [--dry-run] [--force]
 */

import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { connect, createDataItemSigner } from '@permaweb/aoconnect';
import Arweave from 'arweave';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');
const DEFAULT_WALLET = join(ROOT_DIR, 'secrets/wallet.json');
const LUA_PATH = join(ROOT_DIR, 'ao_process/arbitrator.lua');
const PROCESS_ID_FILE = join(ROOT_DIR, 'ao_process/process_id.txt');

// Standard AO Testnet Module & Scheduler (AOS Lua WASM Engine)
const DEFAULT_AOS_MODULE = process.env.AO_MODULE || 'Pq2gahioPTspHGvLjQip-B_r4q5LldMZWUC-20CDohM';
const DEFAULT_SCHEDULER = process.env.AO_SCHEDULER || '_GQ33BkPtZrqxA84vM82zscllkQufHyqDistribution_';

const MU_URL = process.env.AO_MU_URL || 'https://mu.ao-testnet.xyz';
const CU_URL = process.env.AO_CU_URL || 'https://cu.ao-testnet.xyz';
const GATEWAY_URL = process.env.ARWEAVE_GATEWAY || 'https://arweave.net';

export async function deployAoProcess(options = {}) {
  const isDryRun = options.dryRun || process.argv.includes('--dry-run');
  const isForce = options.force || process.argv.includes('--force');
  const walletPath = options.walletPath || process.env.WALLET_PATH || DEFAULT_WALLET;

  if (!existsSync(walletPath)) {
    throw new Error(`Wallet file not found at: ${walletPath}. Run 'npm run wallet' first.`);
  }

  const jwk = JSON.parse(readFileSync(walletPath, 'utf8'));
  const arweave = Arweave.init({ host: 'arweave.net', port: 443, protocol: 'https' });
  const address = await arweave.wallets.jwkToAddress(jwk);

  console.log('================================================================');
  console.log('  HyDB Official AO Arbitrator Process Deployment');
  console.log('================================================================');
  console.log(`Operator Address: ${address}`);
  console.log(`Module ID:        ${options.moduleId || DEFAULT_AOS_MODULE}`);
  console.log(`Scheduler:        ${options.scheduler || DEFAULT_SCHEDULER}`);
  console.log(`MU URL:           ${MU_URL}`);
  console.log(`CU URL:           ${CU_URL}`);
  console.log(`Gateway:          ${GATEWAY_URL}`);

  if (existsSync(PROCESS_ID_FILE) && !isForce) {
    const existing = readFileSync(PROCESS_ID_FILE, 'utf8').trim();
    if (existing && existing.length > 20 && !existing.startsWith('MOCK_')) {
      console.log(`\nExisting AO Process ID found: ${existing}`);
      console.log('Use --force to overwrite and deploy a new process.');
      return { ok: true, processId: existing, deployed: false };
    }
  }

  if (isDryRun) {
    console.log('\n[Dry-Run] Pre-flight validation passed. Process not spawned.');
    return { ok: true, processId: 'DRY_RUN_PROCESS_ID', deployed: false };
  }

  const ao = connect({
    MU_URL,
    CU_URL,
    GATEWAY_URL
  });

  const signer = createDataItemSigner(jwk);

  console.log('\n[1/3] Spawning AO Process...');
  const processId = await ao.spawn({
    module: options.moduleId || DEFAULT_AOS_MODULE,
    scheduler: options.scheduler || DEFAULT_SCHEDULER,
    signer,
    tags: [
      { name: 'App-Name', value: 'aos' },
      { name: 'Variant', value: 'ao.TN.1' },
      { name: 'Type', value: 'Process' },
      { name: 'Name', value: 'HyDB-Arbitrator' },
      { name: 'Authority', value: address }
    ]
  });

  console.log(`✓ Process Spawned with ID: ${processId}`);

  console.log('\n[2/3] Loading Arbitrator State Machine (arbitrator.lua)...');
  const luaCode = readFileSync(LUA_PATH, 'utf8');

  const evalMsgId = await ao.message({
    process: processId,
    signer,
    tags: [
      { name: 'Action', value: 'Eval' }
    ],
    data: luaCode
  });

  console.log(`✓ Eval message dispatched: ${evalMsgId}`);
  console.log('\n[3/3] Awaiting Compute Unit Confirmation...');

  let evalResult = null;
  try {
    evalResult = await ao.result({
      process: processId,
      message: evalMsgId
    });
    console.log('✓ Arbitrator successfully initialized on AO Compute Unit.');
  } catch (err) {
    console.warn(`! Note: Could not immediately read CU result (${err.message}). Process will evaluate on first query.`);
  }

  writeFileSync(PROCESS_ID_FILE, processId, 'utf8');
  console.log(`\n✓ Process ID saved to: ${PROCESS_ID_FILE}`);
  console.log('================================================================');

  return {
    ok: true,
    processId,
    evalMsgId,
    evalResult,
    deployed: true
  };
}

if (process.argv[1] && process.argv[1].endsWith('deploy_ao_process.mjs')) {
  deployAoProcess().catch((err) => {
    console.error('\nDeployment Failed:', err.message);
    process.exit(1);
  });
}
