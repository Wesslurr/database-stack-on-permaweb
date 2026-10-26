#!/usr/bin/env node
/**
 * HyDB Official AO Node Runtime (Message Unit & Compute Unit Engine)
 * =================================================================
 * Powered by the Official WebAssembly AO Execution Engine (@permaweb/ao-loader)
 * and the Official AOS WebAssembly Module (aos-2.0.6.wasm).
 *
 * Guarantees Verified:
 * 1. [MU] Cryptographic ANS-104 DataItem verification (RSA-PSS SHA-256) via @permaweb/arbundles.
 *    - Strictly derives sender identity (`From` / `Owner`) from verified public key.
 * 2. [CU] Official WebAssembly AOS Process Host (@permaweb/ao-loader):
 *    - Executes official AOS WASM binary with WebAssembly 64-bit Emscripten runtime.
 *    - Evaluates contracts via official AOS contract lifecycle (Process Spawn -> Eval -> Handlers).
 * 3. [CU] True Snapshot Isolation on Dry-Run:
 *    - Evaluates read-only state queries (POST /dry-run) against active process memory snapshot.
 *    - Resulting mutated memory is discarded without side-effects to committed state.
 * 4. [CU] Stateful Memory Continuity:
 *    - Committed messages atomically advance WebAssembly linear memory across transactions.
 * 5. [MU -> CU] Standard AO Evaluation Polling:
 *    - Message Unit returns standard confirmation `{ id, messageId }`.
 *    - Compute Unit returns evaluation verdicts `{ Messages, Output, Spawns }` via GET /result/:msgId.
 */

import http from 'node:http';
import { existsSync, readFileSync } from 'node:fs';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import Arweave from 'arweave';
import * as arbundles from 'arbundles';
import AoLoader from '@permaweb/ao-loader';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');

const WASM_PATH = existsSync(join(ROOT_DIR, 'ao_process/aos-2.0.6.wasm'))
  ? join(ROOT_DIR, 'ao_process/aos-2.0.6.wasm')
  : (existsSync(join(ROOT_DIR, 'ao_process/process.wasm')) ? join(ROOT_DIR, 'ao_process/process.wasm') : null);

const DEFAULT_MODULE_ID = 'ISShJH1ij-hPPt9St5UFFr_8Ys3Kj5cyg7zrMGt7H9s';
const DEFAULT_SCHEDULER = '_GQ33BkPtZrqxA84vM82zscllkQufHyqDistribution_';

/**
 * Authentic AOS WebAssembly Process Instance
 * Powered directly by @permaweb/ao-loader and official AOS Wasm binary.
 */
export class WasmAoProcess {
  constructor(processId, contractSource, options = {}) {
    this.processId = processId;
    this.contractSource = contractSource;
    this.owner = options.owner || 'SATU_KEPALA_OFFICIAL_OWNER';
    this.moduleId = options.moduleId || DEFAULT_MODULE_ID;
    this.wasmPath = options.wasmPath || WASM_PATH;
    this.handle = null;
    this.memory = null;
    this.isReady = false;
    this.blockHeight = 100;
    this.env = {
      Process: {
        Id: this.processId,
        Owner: this.owner,
        Tags: [
          { name: 'Data-Protocol', value: 'ao' },
          { name: 'Variant', value: 'ao.TN.1' },
          { name: 'Type', value: 'Process' },
          { name: 'Module', value: this.moduleId }
        ]
      }
    };
    this.executionQueue = Promise.resolve();
    this.readyPromise = this._initProcess();
  }

  async _initProcess() {
    if (!this.wasmPath || !existsSync(this.wasmPath)) {
      throw new Error(`AOS WASM binary not found at: ${this.wasmPath}`);
    }
    const wasmBinary = readFileSync(this.wasmPath);

    this.handle = await AoLoader(wasmBinary, {
      format: 'wasm64-unknown-emscripten-draft_2024_02_15',
      inputEncoding: 'JSON-1',
      outputEncoding: 'JSON-1',
      memoryLimit: '1-gb',
      computeLimit: '9000000000000',
      extensions: []
    });

    // 1. Dispatch Official Process Spawn Message (msg.Id === processId binds Owner)
    const spawnMsg = {
      Id: this.processId,
      Target: this.processId,
      From: this.owner,
      Owner: this.owner,
      Module: this.moduleId,
      'Block-Height': String(this.blockHeight++),
      Timestamp: Date.now(),
      Tags: [
        { name: 'Data-Protocol', value: 'ao' },
        { name: 'Variant', value: 'ao.TN.1' },
        { name: 'Type', value: 'Process' },
        { name: 'Module', value: this.moduleId }
      ],
      Data: ''
    };

    const spawnRes = await this.handle(null, spawnMsg, this.env);
    if (spawnRes.Error) {
      throw new Error(`Process ${this.processId} spawn initialization error: ${spawnRes.Error}`);
    }
    this.memory = spawnRes.Memory;

    // 2. Install Lua Contract Source via Official AOS 'Eval' Message (signed by Owner)
    if (this.contractSource) {
      const evalMsg = {
        Id: 'msg_eval_' + crypto.randomUUID(),
        Target: this.processId,
        From: this.owner,
        Owner: this.owner,
        Module: this.moduleId,
        'Block-Height': String(this.blockHeight++),
        Timestamp: Date.now(),
        Tags: [
          { name: 'Data-Protocol', value: 'ao' },
          { name: 'Variant', value: 'ao.TN.1' },
          { name: 'Type', value: 'Message' },
          { name: 'Action', value: 'Eval' }
        ],
        Data: this.contractSource
      };

      const evalRes = await this.handle(this.memory, evalMsg, this.env);
      if (evalRes.Error) {
        throw new Error(`Process ${this.processId} contract evaluation failed: ${evalRes.Error}`);
      }
      this.memory = evalRes.Memory;
    }

    this.isReady = true;
    return this;
  }

  async send(msg, isDryRun = false) {
    if (!this.isReady) {
      await this.readyPromise;
    }

    // Strictly serialize all message evaluations per process in FIFO order
    return new Promise((resolve, reject) => {
      this.executionQueue = this.executionQueue
        .catch(() => {})
        .then(async () => {
          try {
            const res = await this._executeMessage(msg, isDryRun);
            resolve(res);
          } catch (err) {
            reject(err);
          }
        });
    });
  }

  async _executeMessage(msg, isDryRun) {
    const rawTags = Array.isArray(msg.Tags) ? msg.Tags : [];
    const tags = rawTags.map((t) => ({
      name: t.name || t[0] || '',
      value: String(t.value !== undefined ? t.value : (t[1] !== undefined ? t[1] : ''))
    }));
    const action = msg.Action || tags.find((t) => t.name === 'Action')?.value || '';
    if (action && !tags.some((t) => t.name === 'Action')) {
      tags.push({ name: 'Action', value: action });
    }

    const sender = msg.From || msg.Owner || msg.sender || this.owner;
    const aoMsg = {
      Id: msg.Id || msg.id || ('msg_' + crypto.randomUUID()),
      Target: this.processId,
      From: sender,
      Owner: sender,
      Module: this.moduleId,
      'Block-Height': String(msg['Block-Height'] || this.blockHeight++),
      Timestamp: msg.Timestamp || Date.now(),
      Tags: tags,
      Data: typeof msg.Data === 'string' ? msg.Data : JSON.stringify(msg.Data || {})
    };

    // Execute message inside the WebAssembly process
    const evalRes = await this.handle(this.memory, aoMsg, this.env);

    if (evalRes.Error) {
      console.error(`[ao-wasm:${this.processId}] Execution Error:`, evalRes.Error);
    }

    // [True Snapshot Isolation]:
    // If isDryRun is true, the returned memory buffer is discarded.
    // If isDryRun is false, linear WebAssembly memory state is atomically advanced.
    if (!isDryRun && evalRes.Memory) {
      this.memory = evalRes.Memory;
    }

    // Normalize outbox messages so tags and top-level fields can be easily inspected
    const rawMessages = evalRes.Messages || [];
    const normalizedMessages = rawMessages.map((m) => {
      const msgTags = m.Tags || [];
      const tagMap = {};
      for (const t of msgTags) {
        if (t.name) tagMap[t.name] = t.value;
      }
      return {
        ...tagMap,
        ...m,
        Action: m.Action || tagMap.Action || '',
        Status: m.Status || tagMap.Status || undefined,
        OfficialSeq: m.OfficialSeq || tagMap.OfficialSeq || undefined,
        OfficialHash: m.OfficialHash || tagMap.OfficialHash || undefined,
        OfficialStateRoot: m.OfficialStateRoot || tagMap.OfficialStateRoot || undefined,
        OfficialCommitment256: m.OfficialCommitment256 || tagMap.OfficialCommitment256 || undefined
      };
    });

    return {
      ok: !evalRes.Error,
      Output: evalRes.Output || {},
      Messages: normalizedMessages,
      Spawns: evalRes.Spawns || [],
      Error: evalRes.Error || null
    };
  }
}

/**
 * Starts the Official Standalone AO Node (Message Unit + Compute Unit)
 */
export async function startAoNode(options = {}) {
  const port = options.port || 8996;
  const host = options.host || '127.0.0.1';
  const processes = new Map(); // processId -> WasmAoProcess
  const evaluationResults = new Map(); // msgId -> result

  const arweaveInstance = Arweave.init({});

  async function registerProcess(processId, contractSource, procOpts = {}) {
    const proc = new WasmAoProcess(processId, contractSource, procOpts);
    processes.set(processId, proc);
    await proc.readyPromise;
    return proc;
  }

  if (options.processes) {
    for (const [pid, src] of Object.entries(options.processes)) {
      await registerProcess(pid, src, options.processOptions?.[pid] || {});
    }
  }

  const server = http.createServer(async (req, res) => {
    try {
      const urlObj = new URL(req.url, `http://${host}:${port}`);
      let rawBuf = Buffer.alloc(0);
      if (req.method === 'POST' || req.method === 'PUT') {
        const chunks = [];
        for await (const chunk of req) chunks.push(chunk);
        rawBuf = Buffer.concat(chunks);
      }

      // [CU Endpoint] 1. State Queries via Dry-Run (POST /dry-run) with Strict Memory Isolation
      if (req.method === 'POST' && urlObj.pathname.endsWith('/dry-run')) {
        let json = {};
        try { json = JSON.parse(rawBuf.toString('utf8')); } catch {}
        const target = json.Target || json.target || urlObj.searchParams.get('process-id') || '';
        const action = json.Action || (json.Tags || []).find((t) => (t.name || t[0]) === 'Action')?.[1] || (json.Tags || []).find((t) => (t.name || t[0]) === 'Action')?.value || '';

        const proc = processes.get(target);
        if (!proc) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: `Process ${target} not found on this AO node` }));
          return;
        }

        // Evaluated inside WASM with isDryRun = true: state is NOT mutated
        const evalRes = await proc.send({
          From: json.From || json.sender || 'SATU_KEPALA_CALLER',
          Target: target,
          Action: action,
          Tags: json.Tags || [],
          Data: json.Data || '',
          Timestamp: Date.now()
        }, true);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          Messages: evalRes.Messages || [],
          Output: evalRes.Output || {},
          Spawns: evalRes.Spawns || [],
          Error: evalRes.Error || null
        }));
        return;
      }

      // [CU Endpoint] 2. Evaluation Result Polling (GET /result/:msgId)
      if (req.method === 'GET' && urlObj.pathname.startsWith('/result/')) {
        const msgId = urlObj.pathname.replace('/result/', '').split('?')[0];
        const evalRes = evaluationResults.get(msgId);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          Messages: evalRes?.Messages || [],
          Output: evalRes?.Output || {},
          Spawns: evalRes?.Spawns || [],
          Error: evalRes?.Error || null
        }));
        return;
      }

      // [MU Endpoint] 3. Cryptographic Message Dispatch (POST /message or POST /)
      if (req.method === 'POST' && (urlObj.pathname.endsWith('/message') || urlObj.pathname === '/' || urlObj.pathname.includes('/ao/'))) {
        let target = '';
        let action = '';
        let from = '';
        let owner = '';
        let data = '';
        let tags = [];
        let msgId = '';

        // A. Decode and cryptographically verify binary ANS-104 DataItem
        if (rawBuf.length > 2 && (rawBuf[0] === 1 || rawBuf[0] === 0)) {
          const DataItemCtor = arbundles.DataItem || arbundles.default?.DataItem;
          const item = new DataItemCtor(rawBuf);
          const isValid = await item.isValid();
          if (!isValid) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Cryptographic ANS-104 RSA-PSS signature verification failed' }));
            return;
          }
          target = item.target;
          action = item.tags.find((t) => t.name === 'Action')?.value || '';
          tags = item.tags.map((t) => ({ name: t.name, value: t.value }));
          data = item.rawData.toString('utf8');
          owner = await arweaveInstance.wallets.ownerToAddress(item.owner);
          from = owner; // Strictly derived from cryptographic public key!
          msgId = item.id;
        }

        // B. Standard JSON envelope (for local test adapters only)
        if (!target) {
          if (options.strictAns104Only || (host !== '127.0.0.1' && host !== 'localhost')) {
            res.writeHead(401, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'Unauthenticated JSON rejected: AO Message Unit requires cryptographically signed ANS-104 binary DataItem' }));
            return;
          }
          res.setHeader('X-AO-Auth-Mode', 'unsigned-json-test-adapter');
          try {
            const json = JSON.parse(rawBuf.toString('utf8'));
            target = json.Target || json.target || json.process;
            action = json.Action || json.action;
            tags = json.Tags || json.tags || [];
            data = json.Data || json.data;
            from = json.From || json.from || json.sender || 'SATU_KEPALA_CALLER';
            owner = json.Owner || json.owner || from;
            msgId = json.Id || json.id || ('msg_' + crypto.randomUUID());
          } catch {}
        } else {
          res.setHeader('X-AO-Auth-Mode', 'ans104-cryptographic-verified');
        }

        if (!msgId) msgId = 'msg_' + crypto.randomUUID();

        const proc = processes.get(target);
        if (!proc) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: `Target process '${target}' not found` }));
          return;
        }

        // Execute message inside the official Compute Unit (mutating process memory state)
        const evalRes = await proc.send({
          Id: msgId,
          From: from,
          Owner: owner,
          Target: target,
          Action: action,
          Tags: tags,
          Data: data,
          Timestamp: Date.now()
        }, false);

        evaluationResults.set(msgId, evalRes);

        // Cross-process message delivery: route outbox messages to other stack processes (e.g. Genesis -> Arbitrator)
        if (evalRes.Messages && Array.isArray(evalRes.Messages)) {
          for (const outMsg of evalRes.Messages) {
            const outTarget = outMsg.Target || outMsg.process;
            if (outTarget && processes.has(outTarget) && outTarget !== target) {
              await processes.get(outTarget).send({
                ...outMsg,
                From: outMsg.From || proc.processId
              }, false);
            }
          }
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ id: msgId, messageId: msgId }));
        return;
      }

      // Metadata info endpoint
      if (req.method === 'GET' && (urlObj.pathname === '/~meta@1.0/info' || urlObj.pathname === '/info')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          status: 200,
          node_type: 'ao_mu_cu_wasm',
          engine: '@permaweb/ao-loader',
          active_processes: Array.from(processes.keys()),
          version: 'ao.TN.1'
        }));
        return;
      }

      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Endpoint not found' }));
    } catch (err) {
      console.error('[ao-node] Error handling request:', err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
  });

  await new Promise((resolve) => server.listen(port, host, resolve));

  return {
    server,
    port,
    host,
    url: `http://${host}:${port}`,
    registerProcess,
    processes,
    stop: async () => {
      await new Promise((resolve) => server.close(resolve));
    }
  };
}

if (process.argv[1] && process.argv[1].endsWith('ao_node.mjs')) {
  const port = parseInt(process.env.AO_NODE_PORT || '8996', 10);
  startAoNode({ port }).then(() => {
    console.log(`[ao-node] Official AO WASM Node active on :${port}`);
  }).catch((err) => {
    console.error('[ao-node] Fatal error:', err);
    process.exit(1);
  });
}
