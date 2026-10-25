#!/usr/bin/env node
/**
 * HyDB Tahap 10: SDK Spawner & Stack Genesis Integration Test Suite
 * ====================================================================
 * Tests and verifies:
 * 1. Deterministic canonical Genesis Hash computation and ordering invariance
 * 2. Unified Master Contract ("1 Lembar Kontrak") Lua rendering
 * 3. SDK HyDB.spawnStack() dryRun simulation and parameter verification
 * 4. SDK HyDB.spawnStack() live/simulated spawn with tag binding and contract evaluation
 * 5. Production safety gates (mainnet module/scheduler enforcement, wallet requirement)
 * 6. HyDB.getGenesisConfig() state query via dryrun / evaluator
 * 7. HyDB.verifyStackGenesis() cryptographic divergence detection
 * 8. End-to-End node cold-start bootstrap & authorized node membership verification
 */

import crypto from 'node:crypto';
import {
  HyDB,
  computeGenesisHash,
  buildMasterContractLua,
  spawnStack,
  getGenesisConfig,
  verifyStackGenesis
} from '../sdk/hydb_sdk.mjs';
import { getOrCreateProcessId } from '../ao_process/committer.mjs';

let passed = 0;
let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    failed++;
    console.error(`  ❌ FAIL: ${msg}`);
    throw new Error(`Assertion failed: ${msg}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${msg}`);
}

function assertEq(act, exp, msg) {
  if (act !== exp) {
    failed++;
    console.error(`  ❌ FAIL: ${msg}`);
    console.error(`     Expected: ${exp}`);
    console.error(`     Actual:   ${act}`);
    throw new Error(`Equality assertion failed: ${msg}`);
  }
  passed++;
  console.log(`  ✅ PASS: ${msg}`);
}

console.log('========================================================================');
console.log(' HyDB Tahap 10: SDK Spawner & Stack Genesis Integration Test Suite');
console.log('========================================================================');

// ============================================================================
// [1] Canonical Genesis Hash Computation & Order Invariance
// ============================================================================
console.log('\n[1] Canonical Genesis Hash Computation & Order Invariance:');

const manifestA = {
  stackId: 'hydb-production-cluster',
  genesisVersion: '1.0.0',
  quorumSize: 3,
  quorumMode: 'majority',
  nodes: ['NODE_CHARLIE', 'NODE_ALPHA', 'NODE_BRAVO'],
  admins: ['WALLET_BOB', 'WALLET_ALICE'],
  schemas: [
    { namespace: 'users.profile', schemaVersion: '1.0.0' },
    { namespace: 'finance.balances', schemaVersion: '1.0.0' }
  ]
};

const manifestB = {
  stackId: 'hydb-production-cluster',
  genesisVersion: '1.0.0',
  quorumSize: 3,
  quorumMode: 'majority',
  nodes: ['NODE_ALPHA', 'NODE_BRAVO', 'NODE_CHARLIE'], // Different order
  admins: ['WALLET_ALICE', 'WALLET_BOB'],             // Different order
  schemas: [
    { namespace: 'finance.balances', schemaVersion: '1.0.0' }, // Different order
    { namespace: 'users.profile', schemaVersion: '1.0.0' }
  ]
};

const hashA = HyDB.computeGenesisHash(manifestA);
const hashB = HyDB.computeGenesisHash(manifestB);

assertEq(typeof hashA, 'string', 'computeGenesisHash returns a string');
assertEq(hashA.length, 64, 'Genesis Hash is 64 hex characters (SHA-256)');
assertEq(hashA, hashB, 'Genesis Hash is strictly order-invariant over nodes, admins, and schemas');

// Perturbing manifest produces a different hash
const manifestC = { ...manifestA, quorumSize: 5 };
const hashC = HyDB.computeGenesisHash(manifestC);
assert(hashA !== hashC, 'Changing quorumSize produces distinct Genesis Hash');

// ============================================================================
// [2] Master Contract Lua Rendering ("1 Lembar Kontrak")
// ============================================================================
console.log('\n[2] Master Contract Lua Rendering:');

const renderedLua = HyDB.buildMasterContractLua({
  stackId: 'hydb-custom-cluster',
  quorumSize: 3,
  nodes: ['NODE_PRIMARY', 'NODE_SECONDARY'],
  admins: ['ADMIN_WALLET_X']
});

assert(renderedLua.includes('stack_id = "hydb-custom-cluster"'), 'Rendered Lua embeds stack_id');
assert(renderedLua.includes('["NODE_PRIMARY"] = true'), 'Rendered Lua embeds NODE_PRIMARY in AuthorizedNodes');
assert(renderedLua.includes('["NODE_SECONDARY"] = true'), 'Rendered Lua embeds NODE_SECONDARY in AuthorizedNodes');
assert(renderedLua.includes('["ADMIN_WALLET_X"] = true'), 'Rendered Lua embeds ADMIN_WALLET_X in AuthorizedAdmins');
assert(renderedLua.includes('"CommitCheckpoint"'), 'Rendered Lua contains Arbitrator CommitCheckpoint logic');
assert(renderedLua.includes('"GetGenesisConfig"'), 'Rendered Lua contains GetGenesisConfig handler');

// ============================================================================
// [3] SDK HyDB.spawnStack Dry-Run Simulation
// ============================================================================
console.log('\n[3] SDK HyDB.spawnStack Dry-Run Simulation:');

const dryRunResult = await HyDB.spawnStack({
  stackId: 'hydb-dryrun-stack',
  dryRun: true,
  nodes: ['NODE_1', 'NODE_2', 'NODE_3']
});

assertEq(dryRunResult.ok, true, 'Dry-run spawn succeeds');
assertEq(dryRunResult.dryRun, true, 'Dry-run flag set to true');
assert(dryRunResult.genesisHash.length === 64, 'Dry-run returns valid 64-char genesisHash');
assert(dryRunResult.processId.startsWith('STACK_'), 'Dry-run returns mock processId with STACK_ prefix');
assert(dryRunResult.contractLua.includes('hydb-dryrun-stack'), 'Dry-run returns compiled contract Lua');

// ============================================================================
// [4] Production Safety Gates & Parameter Validation
// ============================================================================
console.log('\n[4] Production Safety Gates & Parameter Validation:');

// Mainnet without explicit module and scheduler must throw
let caughtMainnetError = null;
try {
  await HyDB.spawnStack({
    network: 'mainnet'
  });
} catch (e) {
  caughtMainnetError = e;
}
assert(caughtMainnetError !== null, 'Mainnet spawn without explicit module/scheduler is rejected');
assert(
  caughtMainnetError.message.includes('Mainnet AO stack spawn requires explicit moduleId and scheduler'),
  'Rejection error message is descriptive and explicit'
);

// Spawn without wallet/signer must throw in non-dryRun
let caughtWalletError = null;
try {
  await HyDB.spawnStack({
    network: 'testnet',
    dryRun: false
  });
} catch (e) {
  caughtWalletError = e;
}
assert(caughtWalletError !== null, 'Spawn without wallet or signer is rejected');
assert(
  caughtWalletError.message.includes('A valid wallet (JWK) or signer is required'),
  'Rejection error specifies wallet requirement'
);

// getGenesisConfig without processId must throw
let caughtProcIdError = null;
try {
  await HyDB.getGenesisConfig('');
} catch (e) {
  caughtProcIdError = e;
}
assert(caughtProcIdError !== null, 'getGenesisConfig rejects empty processId');

// ============================================================================
// [5] Simulated Live AO Spawner via Mock Transport
// ============================================================================
console.log('\n[5] Simulated Live AO Spawner via Mock Transport:');

const mockSpawnEvents = [];
const mockMessageEvents = [];
let spawnCounter = 0;

const mockAoTransport = {
  async spawn(args) {
    mockSpawnEvents.push(args);
    spawnCounter++;
    return spawnCounter === 1 ? 'AO_MOCK_GENESIS_ID' : 'AO_MOCK_ARBITRATOR_ID';
  },
  async message(args) {
    mockMessageEvents.push(args);
    return `AO_MSG_ID_${mockMessageEvents.length}`;
  },
  async dryrun(args) {
    const action = args.tags?.find((t) => t.name === 'Action')?.value;
    if (action === 'GetStackStatus') {
      return {
        Messages: [
          {
            Data: JSON.stringify({
              ok: true,
              stack_id: 'hydb-mock-mesh',
              status: 'HEALTHY',
              quorum_size: 3,
              healthy_nodes_count: 2,
              registered_nodes_count: 2,
              arbitrator_process_id: 'AO_MOCK_ARBITRATOR_ID',
              nodes: {
                SU_NODE_ALPHA: { status: 'READY', role: 'storage_unit' },
                SU_NODE_BETA: { status: 'READY', role: 'storage_unit' }
              }
            })
          }
        ]
      };
    }
    return {
      Messages: [
        {
          Data: JSON.stringify({
            genesis: {
              stack_id: 'hydb-mock-mesh',
              genesis_hash: args.expectedHash || '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF',
              quorum_size: 3,
              quorum_mode: 'majority'
            },
            arbitrator_process_id: 'AO_MOCK_ARBITRATOR_ID',
            authorized_nodes: {
              SU_NODE_ALPHA: true,
              SU_NODE_BETA: true
            },
            authorized_admins: {
              OPERATOR_PRIMARY: true
            },
            official_seq: 10,
            current_epoch: 1
          })
        }
      ]
    };
  }
};

const fakeSigner = async (data) => ({ id: 'mock_sig', ...data });

const liveResult = await HyDB.spawnStack({
  stackId: 'hydb-mock-mesh',
  signer: fakeSigner,
  ao: mockAoTransport,
  nodes: ['SU_NODE_ALPHA', 'SU_NODE_BETA'],
  admins: ['OPERATOR_PRIMARY']
});

assertEq(liveResult.ok, true, 'Live spawnStack succeeds with mock transport');
assertEq(liveResult.deployed, true, 'Result indicates deployed=true');
assertEq(liveResult.genesisProcessId, 'AO_MOCK_GENESIS_ID', 'Returns created Genesis Process ID');
assertEq(liveResult.arbitratorProcessId, 'AO_MOCK_ARBITRATOR_ID', 'Returns created Arbitrator Process ID');
assertEq(liveResult.processId, 'AO_MOCK_GENESIS_ID', 'Default processId aliases genesisProcessId');

// Verify dual-process spawn calls
assertEq(mockSpawnEvents.length, 2, 'Exactly 2 spawn calls dispatched (Genesis + Arbitrator)');
const getTag = (tags, name) => tags.find((t) => t.name === name)?.value;

// 1. Genesis Process Tags
assertEq(getTag(mockSpawnEvents[0].tags, 'App-Name'), 'aos', 'Genesis spawn tag App-Name is "aos"');
assertEq(getTag(mockSpawnEvents[0].tags, 'Type'), 'Process', 'Genesis spawn tag Type is "Process"');
assertEq(getTag(mockSpawnEvents[0].tags, 'Name'), 'hydb-mock-mesh', 'Genesis spawn tag Name matches stackId');
assertEq(getTag(mockSpawnEvents[0].tags, 'Genesis-Hash'), liveResult.genesisHash, 'Genesis spawn tag Genesis-Hash matches calculated hash');

// 2. Arbitrator Process Tags
assertEq(getTag(mockSpawnEvents[1].tags, 'Name'), 'hydb-mock-mesh-Arbitrator', 'Arbitrator spawn tag Name is <stackId>-Arbitrator');
assertEq(getTag(mockSpawnEvents[1].tags, 'Genesis-Process-Id'), 'AO_MOCK_GENESIS_ID', 'Arbitrator bound to Genesis-Process-Id');

// Verify messages: Genesis Eval, Arbitrator Eval, SetArbitratorProcess
assertEq(mockMessageEvents.length, 3, 'Exactly 3 messages dispatched (Genesis Eval, Arbitrator Eval, SetArbitratorProcess)');
assertEq(mockMessageEvents[0].process, 'AO_MOCK_GENESIS_ID', '1st msg evaluates Genesis contract');
assertEq(mockMessageEvents[1].process, 'AO_MOCK_ARBITRATOR_ID', '2nd msg evaluates Arbitrator contract');
assertEq(mockMessageEvents[2].process, 'AO_MOCK_GENESIS_ID', '3rd msg registers ArbitratorProcessId in Genesis');
assertEq(getTag(mockMessageEvents[2].tags, 'Action'), 'SetArbitratorProcess', '3rd msg Action is SetArbitratorProcess');

// ============================================================================
// [6] HyDB.getGenesisConfig State Retrieval
// ============================================================================
console.log('\n[6] HyDB.getGenesisConfig State Retrieval:');

const genesisConfig = await HyDB.getGenesisConfig('AO_MOCK_PROCESS_ID_789XYZ', {
  ao: mockAoTransport
});

assertEq(genesisConfig.ok, true, 'getGenesisConfig successfully retrieves state');
assertEq(genesisConfig.processId, 'AO_MOCK_PROCESS_ID_789XYZ', 'Config binds processId');
assertEq(genesisConfig.genesis.stack_id, 'hydb-mock-mesh', 'Genesis stack_id parsed');
assertEq(genesisConfig.genesis.quorum_size, 3, 'Genesis quorum_size parsed');
assertEq(genesisConfig.authorized_nodes.SU_NODE_ALPHA, true, 'SU_NODE_ALPHA is authorized');
assertEq(genesisConfig.authorized_nodes.SU_NODE_BETA, true, 'SU_NODE_BETA is authorized');
assertEq(genesisConfig.official_seq, 10, 'official_seq parsed from contract state');

// Test with custom evaluatorFn
const customEvaluatorConfig = await HyDB.getGenesisConfig('PROC_123', {
  evaluatorFn: async () => ({
    Messages: [
      {
        Data: JSON.stringify({
          genesis: { stack_id: 'custom-eval-stack', genesis_hash: 'HASH_AAA' },
          authorized_nodes: { NODE_A: true }
        })
      }
    ]
  })
});
assertEq(customEvaluatorConfig.ok, true, 'getGenesisConfig works with evaluatorFn');
assertEq(customEvaluatorConfig.genesis.stack_id, 'custom-eval-stack', 'Stack ID parsed from evaluatorFn');

// ============================================================================
// [7] HyDB.verifyStackGenesis Cryptographic Divergence Gate
// ============================================================================
console.log('\n[7] HyDB.verifyStackGenesis Cryptographic Divergence Gate:');

// Test 1: Matching Hash passes verification
const expectedHash = '0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF';
const verifyPass = await HyDB.verifyStackGenesis('AO_MOCK_PROCESS_ID_789XYZ', expectedHash, {
  ao: mockAoTransport
});

assertEq(verifyPass.ok, true, 'verifyStackGenesis succeeds when genesis_hash matches');
assertEq(verifyPass.divergence, false, 'divergence flag is false');
assertEq(verifyPass.config.genesis.stack_id, 'hydb-mock-mesh', 'Returns valid config on verification');

// Test 2: Diverging Hash fails verification
const wrongHash = 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF';
const verifyFail = await HyDB.verifyStackGenesis('AO_MOCK_PROCESS_ID_789XYZ', wrongHash, {
  ao: mockAoTransport
});

assertEq(verifyFail.ok, false, 'verifyStackGenesis fails when genesis_hash diverges');
assertEq(verifyFail.divergence, true, 'divergence flag is true');
assert(verifyFail.reason.includes('Genesis hash mismatch'), 'Failure reason explains mismatch');

// Test 3: Unreachable Process fails verification
const unreachableTransport = {
  async dryrun() {
    return { Messages: [] };
  }
};
const verifyUnreachable = await HyDB.verifyStackGenesis('UNREACHABLE_PROC', expectedHash, {
  ao: unreachableTransport
});
assertEq(verifyUnreachable.ok, false, 'verifyStackGenesis fails when process is unreachable');
assertEq(verifyUnreachable.divergence, true, 'divergence flag is true on unreachable');

// ============================================================================
// [8] HyDB.getStackStatus, registerNode, and reportHeartbeat Operations
// ============================================================================
console.log('\n[8] HyDB.getStackStatus, registerNode, and reportHeartbeat Operations:');

// Test 1: getStackStatus
const stackStatus = await HyDB.getStackStatus('AO_MOCK_GENESIS_ID', {
  ao: mockAoTransport
});
assertEq(stackStatus.ok, true, 'getStackStatus succeeds');
assertEq(stackStatus.status, 'HEALTHY', 'Stack status parsed as HEALTHY');
assertEq(stackStatus.arbitrator_process_id, 'AO_MOCK_ARBITRATOR_ID', 'Stack reports bound ArbitratorProcessId');
assertEq(stackStatus.registered_nodes_count, 2, 'Stack reports 2 registered nodes');

// Test 2: registerNode
const regResult = await HyDB.registerNode('AO_MOCK_GENESIS_ID', {
  node_id: 'SU_NODE_ALPHA',
  role: 'storage_unit',
  endpoint: 'http://127.0.0.1:8735'
}, {
  ao: mockAoTransport,
  signer: fakeSigner
});
assertEq(regResult.ok, true, 'registerNode returns ok=true');
assert(regResult.msgId !== undefined, 'registerNode returns msgId');

// Test 3: reportHeartbeat
const hbResult = await HyDB.reportHeartbeat('AO_MOCK_GENESIS_ID', {
  node_id: 'SU_NODE_ALPHA',
  throughput: 2500,
  last_seq: 42
}, {
  ao: mockAoTransport,
  signer: fakeSigner
});
assertEq(hbResult.ok, true, 'reportHeartbeat returns ok=true');
assert(hbResult.msgId !== undefined, 'reportHeartbeat returns msgId');

// Test 4: Dynamic Arbitrator resolution from Genesis in committer via real queryOfficialAoState
const resolvedArbId = await getOrCreateProcessId({
  processIdFile: '/nonexistent/proc.txt',
  genesisProcessId: 'GENESIS_STACK_999',
  evaluatorFn: async (msg) => {
    assert(msg.Action === 'GetGenesisConfig', 'evaluatorFn receives Action: GetGenesisConfig');
    return {
      Messages: [{
        Data: JSON.stringify({
          ok: true,
          arbitrator_process_id: 'RESOLVED_ARBITRATOR_VIA_EVALUATOR_777'
        })
      }]
    };
  }
});
assertEq(resolvedArbId, 'RESOLVED_ARBITRATOR_VIA_EVALUATOR_777', 'getOrCreateProcessId dynamically resolves ArbitratorProcessId from Genesis via queryOfficialAoState');

// Test 5: Resolution failure does NOT return Genesis ID
let resolutionFailedClosed = false;
try {
  await getOrCreateProcessId({
    processIdFile: '/nonexistent/proc.txt',
    genesisProcessId: 'GENESIS_FAIL_STACK',
    allowAutoCreate: false,
    queryGenesisFn: async () => ({})
  });
} catch (err) {
  resolutionFailedClosed = (err.code === 'ERR_ARBITRATOR_NOT_RESOLVED_FROM_GENESIS' || err.code === 'ERR_AO_PROCESS_ID_REQUIRED');
}
assertEq(resolutionFailedClosed, true, 'Resolution failure fails-closed without returning Genesis ID as Arbitrator ID');

// ============================================================================
// [9] End-to-End Cold-Start Bootstrap & Node Authorization
// ============================================================================
console.log('\n[9] End-to-End Cold-Start Bootstrap & Node Authorization:');

// Simulate a node booting with an intended process ID
async function simulateNodeBootstrap(nodeId, processId, expectedHash, transport) {
  // Step 1: Verify Stack Genesis Identity
  const genesisCheck = await HyDB.verifyStackGenesis(processId, expectedHash, { ao: transport });
  if (!genesisCheck.ok) {
    return { ok: false, error: 'ERR_GENESIS_DIVERGENCE', reason: genesisCheck.reason };
  }

  // Step 2: Check Node Authorization in AuthorizedNodes
  const authorizedNodes = genesisCheck.config.authorized_nodes || {};
  if (!authorizedNodes[nodeId]) {
    return { ok: false, error: 'ERR_UNAUTHORIZED_NODE', nodeId };
  }

  return {
    ok: true,
    status: 'READY',
    stackId: genesisCheck.config.genesis.stack_id,
    quorumSize: genesisCheck.config.genesis.quorum_size,
    officialSeq: genesisCheck.config.official_seq
  };
}

// Authorized node boots successfully
const authNodeBoot = await simulateNodeBootstrap('SU_NODE_ALPHA', 'AO_MOCK_PROCESS_ID_789XYZ', expectedHash, mockAoTransport);
assertEq(authNodeBoot.ok, true, 'Authorized node (SU_NODE_ALPHA) boots cleanly');
assertEq(authNodeBoot.status, 'READY', 'Authorized node enters READY state');
assertEq(authNodeBoot.officialSeq, 10, 'Authorized node synchronizes starting officialSeq');

// Unauthorized node is blocked fail-closed
const rogueNodeBoot = await simulateNodeBootstrap('ROGUE_NODE_99', 'AO_MOCK_PROCESS_ID_789XYZ', expectedHash, mockAoTransport);
assertEq(rogueNodeBoot.ok, false, 'Rogue node (ROGUE_NODE_99) is rejected at cold-start');
assertEq(rogueNodeBoot.error, 'ERR_UNAUTHORIZED_NODE', 'Error code is ERR_UNAUTHORIZED_NODE');

// ============================================================================
// Summary
// ============================================================================
console.log('\n========================================================================');
console.log(` Tahap 10 Spawner Results: ${passed} PASSED, ${failed} FAILED`);
console.log('========================================================================\n');

if (failed > 0) {
  process.exit(1);
}
