# @hydb/sdk — Universal Database SDK for AO & Arweave

The official client SDK for HyDB: an optimistic, sub-15ms real-time distributed database stack backed by physical WAL durability and verified autonomously by independent AO processes on Arweave.

## Key Capabilities

- **Zero-Config Declarative Contracts**: Define namespaces, backends (`wasm_slot`, `ets_doc`), schema constraints (`min_value`, `max_value`, `requires`), and security policies in clean JavaScript (`defineContract`).
- **Autonomous Stack Spawner & Genesis Anchoring**: Orchestrate and deploy both the **Genesis Process** and the **Arbitrator Process** with canonical SHA-256 genesis hashes (`HyDB.spawnStack`).
- **Cluster Health & Discovery**: Real-time stack observability, dynamic node registration, and heartbeat reporting (`getStackStatus`, `registerNode`, `reportHeartbeat`).
- **Arweave-Native Immutable Publishing**: One-line publishing of contract definitions and WASM bytecode directly to Arweave (`publishToArweave`).
- **Autonomous Node Mounting**: Hot Nodes autonomously load and verify contracts by Arweave Transaction ID (`mountOnNode`).
- **High-Throughput Optimistic Client**: Simple CRUD operations (`get`, `put`, `del`, `exists`, `scan`) with realistic P95 latencies under 25ms.

## Quick Start

### 1. Defining and Using a Contract

```javascript
import { defineContract } from '@hydb/sdk';

// 1. Define a contract
const contract = defineContract({
  namespace: 'finance.treasury',
  backend: 'wasm_slot',
  key_type: 'int64',
  value_type: 'int64',
  rules: {
    min_value: 0,
    max_value: 1000000000
  }
});

// 2. Publish to Arweave
const { contractTxId } = await contract.publishToArweave({
  gatewayUrl: process.env.ARWEAVE_GATEWAY || 'https://arweave.net',
  walletPath: './wallet.json'
});

// 3. Mount on any Hot Node
const mount = await contract.mountOnNode('http://hot-node:8734', {
  adminToken: process.env.ADMIN_TOKEN
});

// 4. Bind client and execute data operations
const client = contract.bindClient('http://hot-node:8734');
await client.put(1001, 5000);
const record = await client.get(1001);
console.log(record.val); // 5000
```

### 2. Spawning and Managing an AO Genesis Stack

```javascript
import { HyDB } from '@hydb/sdk';

// 1. Calculate canonical Genesis Hash
const genesisHash = HyDB.computeGenesisHash({
  stackId: 'hydb-production-mesh',
  quorumSize: 3,
  authorizedNodes: ['NODE_HOT_1', 'NODE_SU_1', 'NODE_SU_2'],
  authorizedAdmins: ['WALLET_ADMIN_ADDR']
});

// 2. Spawn Dual-Process Stack (Genesis + Arbitrator)
const stack = await HyDB.spawnStack({
  stackId: 'hydb-production-mesh',
  quorumSize: 3,
  wallet: jwkWallet,
  network: 'testnet'
});

console.log('Genesis Process ID:', stack.genesisProcessId);
console.log('Arbitrator Process ID:', stack.arbitratorProcessId);

// 3. Query Stack Health Status
const status = await HyDB.getStackStatus(stack.genesisProcessId);
console.log('Cluster Status:', status.status); // HEALTHY
```
