#!/usr/bin/env node
/**
 * HyDB Stack Genesis Spawner CLI
 * ==============================
 * Spawns an official HyDB stack:
 * 1. Computes canonical Genesis Hash
 * 2. Spawns dedicated Genesis Process (Stack Constitution & Control Plane)
 * 3. Spawns dedicated Arbitrator Process (Data Plane Arbitrator)
 * 4. Links ArbitratorProcessId into Genesis Process
 * 5. Persists process_genesis_id.txt, process_id.txt, and genesis_hash.txt
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { HyDB } from '../sdk/hydb_sdk.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');
const DEFAULT_WALLET_PATH = join(ROOT_DIR, 'secrets/wallet.json');

async function main() {
  const args = process.argv.slice(2);
  const isDryRun = args.includes('--dry-run') || process.env.HYDB_DRY_RUN === 'true';

  let network = process.env.AO_NETWORK || 'testnet';
  const netIdx = args.indexOf('--network');
  if (netIdx !== -1 && args[netIdx + 1]) {
    network = args[netIdx + 1];
  }

  let stackId = 'hydb-universal-stack';
  const stackIdx = args.indexOf('--stack-id');
  if (stackIdx !== -1 && args[stackIdx + 1]) {
    stackId = args[stackIdx + 1];
  }

  let walletPath = DEFAULT_WALLET_PATH;
  const walletIdx = args.indexOf('--wallet');
  if (walletIdx !== -1 && args[walletIdx + 1]) {
    walletPath = args[walletIdx + 1];
  }

  let quorumSize = 3;
  const quorumIdx = args.indexOf('--quorum');
  if (quorumIdx !== -1 && args[quorumIdx + 1]) {
    quorumSize = parseInt(args[quorumIdx + 1], 10);
  }

  console.log('========================================================================');
  console.log(' HyDB Stack Genesis Spawner CLI');
  console.log('========================================================================');
  console.log(` Stack ID:       ${stackId}`);
  console.log(` Target Network: ${network}`);
  console.log(` Mode:           ${isDryRun ? 'DRY-RUN (Simulation)' : 'LIVE DEPLOYMENT'}`);
  console.log(` Quorum Size:    ${quorumSize}`);
  console.log('========================================================================\n');

  const spawnOptions = {
    stackId,
    network,
    quorumSize,
    dryRun: isDryRun,
    walletPath: existsSync(walletPath) ? walletPath : undefined,
    nodes: [
      { id: 'sd-node-1', address: 'SU_NODE_1' },
      { id: 'sd-node-2', address: 'SU_NODE_2' },
      { id: 'sd-node-3', address: 'SU_NODE_3' }
    ],
    admins: ['ADMIN_PRIMARY'],
    schemas: [
      { namespace: 'default' },
      { namespace: 'finance.balances' },
      { namespace: 'app.docs' }
    ]
  };

  const result = await HyDB.spawnStack(spawnOptions);

  if (!result.ok) {
    console.error('❌ Failed to spawn HyDB Stack:', result.error || 'Unknown error');
    process.exit(1);
  }

  console.log('✅ HyDB Stack Spawned Successfully!\n');
  console.log(` • Genesis Process ID:    ${result.genesisProcessId || result.processId}`);
  console.log(` • Arbitrator Process ID: ${result.arbitratorProcessId}`);
  console.log(` • Genesis Hash:          ${result.genesisHash}`);
  console.log(` • Network:               ${result.network}`);

  if (!isDryRun) {
    const genesisFile = join(ROOT_DIR, 'ao_process/process_genesis_id.txt');
    const arbitFile = join(ROOT_DIR, 'ao_process/process_id.txt');
    const hashFile = join(ROOT_DIR, 'ao_process/genesis_hash.txt');

    writeFileSync(genesisFile, result.genesisProcessId || result.processId);
    writeFileSync(arbitFile, result.arbitratorProcessId);
    writeFileSync(hashFile, result.genesisHash);

    console.log('\nSaved process identifiers:');
    console.log(` -> ${genesisFile}`);
    console.log(` -> ${arbitFile}`);
    console.log(` -> ${hashFile}`);
  }

  console.log('\n========================================================================');
}

main().catch((err) => {
  console.error('\nFatal Spawner Error:', err.message || err);
  process.exit(1);
});
