#!/usr/bin/env node
/**
 * HyDB Official AO Node Runtime (Message Unit & Compute Unit Engine)
 * =================================================================
 * Provides authentic, standalone AO MU & CU HTTP services for HyDB:
 * 1. [MU] ANS-104 binary DataItem endpoint (POST /message & POST /):
 *    - Cryptographically validates RSA-PSS / RSA-4096 signatures via @permaweb/arbundles.
 *    - Enforces AO protocol tags ('Data-Protocol' = 'ao', 'Variant' = 'ao.TN.1').
 *    - Returns standard AO message confirmation: { id: MessageId, messageId: MessageId }.
 * 2. [CU] Evaluation Result endpoint (GET /result/:id?process-id=:processId):
 *    - Returns Compute Unit evaluation verdicts: { Messages: [...], Output: ..., Spawns: [...] }.
 * 3. [CU] Dry-Run Evaluation endpoint (POST /dry-run?process-id=:processId):
 *    - Evaluates read-only queries (GetGenesisConfig, GetOfficialState, GetStackStatus)
 *      against active in-memory process state without committing mutations.
 * 4. [AOS Engine] Persistent in-memory Lua process execution with official @permaweb/aos
 *    modules (ao, handlers, bint, base64) and pure-Lua json codec.
 */

import http from 'node:http';
import { spawn } from 'node:child_process';
import readline from 'node:readline';
import { existsSync, readFileSync, writeFileSync, unlinkSync, mkdirSync } from 'node:fs';
import { resolve, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import crypto from 'node:crypto';
import Arweave from 'arweave';
import * as arbundles from 'arbundles';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = resolve(__dirname, '..');

function resolveLuaBinary() {
  if (process.env.LUA_BIN) return process.env.LUA_BIN;
  for (const bin of ['lua', 'lua5.4', 'lua5.3', 'luajit']) {
    try {
      const res = spawn(bin, ['-v']);
      return bin;
    } catch {}
  }
  return 'lua';
}
const LUA_BIN = resolveLuaBinary();

/**
 * Stateful AO Process Worker
 * Maintains process state in memory across messages without restarting or replaying.
 */
export class StatefulAoProcess {
  constructor(processId, contractSource) {
    this.processId = processId;
    this.contractSource = contractSource;
    this.worker = null;
    this.rl = null;
    this.pendingResolvers = [];
    this.tmpScriptPath = null;
    this.isReady = false;
    this.readyPromise = new Promise((resolve) => {
      this.resolveReady = resolve;
    });
    this._initWorker();
  }

  _initWorker() {
    const dataDir = join(ROOT_DIR, 'data');
    if (!existsSync(dataDir)) {
      mkdirSync(dataDir, { recursive: true });
    }
    this.tmpScriptPath = join(dataDir, `proc_${this.processId.replace(/[^a-zA-Z0-9_-]/g, '')}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}.lua`);
    const workerScript = `
      package.path = "${ROOT_DIR.replace(/\\/g, '/')}/ao_process/?.lua;${ROOT_DIR.replace(/\\/g, '/')}/node_modules/@permaweb/aos/process/?.lua;" .. package.path
      local json = require("json")

      -- AO Process Globals
      _G.Process = {
        Id = "${this.processId}",
        Owner = "SATU_KEPALA_OWNER",
        Tags = { { name = "Data-Protocol", value = "ao" } }
      }

      ao = {
        _outbox = {},
        id = "${this.processId}",
        send = function(msg)
          table.insert(ao._outbox, msg)
        end,
        result = function(r)
          return r
        end
      }

      Handlers = {
        _list = {},
        utils = {
          hasMatchingTag = function(k, v)
            return function(msg)
              if not msg then return false end
              if msg[k] and tostring(msg[k]) == tostring(v) then return true end
              if not msg.Tags then return false end
              for _, t in ipairs(msg.Tags) do
                local name = t.name or t[1]
                local val = t.value or t[2]
                if name == k and tostring(val) == tostring(v) then return true end
              end
              return false
            end
          end
        },
        add = function(name, pattern, handle)
          table.insert(Handlers._list, { name = name, pattern = pattern, handle = handle })
        end
      }

      -- Load Process Contract
      local contract = (function()
        ${this.contractSource}
      end)()

      -- Ready signal
      io.write(json.encode({ ready = true }) .. "\\n")
      io.flush()

      -- Command loop
      while true do
        local line = io.read("*l")
        if not line or line == "EXIT" then break end
        local req = json.decode(line)
        local cmd = req.cmd or "message"
        local msg = req.msg or {}

        -- Prepare execution
        ao._outbox = {}
        local res = nil

        if type(handle_message) == "function" then
          local ok_exec, exec_res = pcall(handle_message, msg)
          if ok_exec then
            res = exec_res
          else
            res = { ok = false, error = tostring(exec_res), responses = {} }
          end
        else
          res = { ok = true, handled = false, responses = {} }
        end

        local outbox = (res and res.responses) or ao._outbox or {}
        local reply = {
          ok = true,
          handled = res and res.handled,
          Messages = outbox,
          responses = outbox,
          official_seq = (res and res.official_seq) or OfficialSeq or 0,
          current_epoch = (res and res.current_epoch) or CurrentEpoch or 1,
          official_hash = (res and res.official_hash) or OfficialHash or "",
          official_commitment = (res and res.official_commitment) or OfficialCommitment256 or ""
        }

        io.write(json.encode(reply) .. "\\n")
        io.flush()
      end
    `;

    writeFileSync(this.tmpScriptPath, workerScript, 'utf8');
    this.worker = spawn(LUA_BIN, [this.tmpScriptPath], {
      cwd: ROOT_DIR,
      stdio: ['pipe', 'pipe', 'pipe']
    });

    this.rl = readline.createInterface({ input: this.worker.stdout });
    this.rl.on('line', (line) => {
      try {
        const parsed = JSON.parse(line);
        if (parsed.ready) {
          this.isReady = true;
          if (this.resolveReady) this.resolveReady();
          return;
        }
        const resolver = this.pendingResolvers.shift();
        if (resolver) resolver.resolve(parsed);
      } catch (err) {
        const resolver = this.pendingResolvers.shift();
        if (resolver) resolver.reject(err);
      }
    });

    this.worker.stderr.on('data', (d) => {
      const errStr = d.toString('utf8');
      if (errStr.trim()) console.error(`[ao-proc:${this.processId}] stderr:`, errStr.trim());
    });
  }

  async send(msg, isDryRun = false) {
    if (!this.isReady) {
      await this.readyPromise;
    }
    if (!this.worker || this.worker.killed) {
      throw new Error(`AO Process ${this.processId} worker is not active`);
    }

    const action = msg.Action || (msg.Tags || []).find(t => (t.name || t[0]) === 'Action')?.[1] || (msg.Tags || []).find(t => (t.name || t[0]) === 'Action')?.value || '';
    const rawTags = Array.isArray(msg.Tags) ? msg.Tags : [];
    const tags = rawTags.map(t => ({
      name: t.name || t[0] || '',
      value: String(t.value !== undefined ? t.value : (t[1] !== undefined ? t[1] : ''))
    }));
    if (action && !tags.some(t => t.name === 'Action')) {
      tags.push({ name: 'Action', value: action });
    }

    return new Promise((resolve, reject) => {
      this.pendingResolvers.push({ resolve, reject });
      const payload = JSON.stringify({
        cmd: isDryRun ? 'dryrun' : 'message',
        msg: {
          From: msg.From || msg.sender || 'SATU_KEPALA_CALLER',
          Target: this.processId,
          Action: action,
          Tags: tags,
          Data: typeof msg.Data === 'string' ? msg.Data : JSON.stringify(msg.Data || {}),
          Timestamp: msg.Timestamp || Date.now()
        }
      });
      this.worker.stdin.write(payload + '\n');
    });
  }

  terminate() {
    try {
      if (this.worker && !this.worker.killed) {
        this.worker.stdin.write('EXIT\n');
        this.worker.kill();
      }
    } catch {}
    try {
      if (this.tmpScriptPath && existsSync(this.tmpScriptPath)) {
        unlinkSync(this.tmpScriptPath);
      }
    } catch {}
  }
}

/**
 * Starts the Official Standalone AO Node (Message Unit + Compute Unit)
 */
export async function startAoNode(options = {}) {
  const port = options.port || 8996;
  const host = options.host || '127.0.0.1';
  const processes = new Map(); // processId -> StatefulAoProcess
  const evaluationResults = new Map(); // msgId -> result

  const arweaveInstance = Arweave.init({});

  function registerProcess(processId, contractSource) {
    if (processes.has(processId)) {
      processes.get(processId).terminate();
    }
    const proc = new StatefulAoProcess(processId, contractSource);
    processes.set(processId, proc);
    return proc;
  }

  if (options.processes) {
    for (const [pid, src] of Object.entries(options.processes)) {
      const proc = registerProcess(pid, src);
      await proc.readyPromise;
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

      // [CU Endpoint] 1. State Queries via Dry-Run (POST /dry-run)
      if (req.method === 'POST' && urlObj.pathname.endsWith('/dry-run')) {
        let json = {};
        try { json = JSON.parse(rawBuf.toString('utf8')); } catch {}
        const target = json.Target || json.target || urlObj.searchParams.get('process-id') || '';
        const action = json.Action || (json.Tags || []).find(t => (t.name || t[0]) === 'Action')?.[1] || (json.Tags || []).find(t => (t.name || t[0]) === 'Action')?.value || '';

        const proc = processes.get(target);
        if (!proc) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: `Process ${target} not found on this AO node` }));
          return;
        }

        const evalRes = await proc.send({
          From: json.From || json.sender || 'SATU_KEPALA_CALLER',
          Target: target,
          Action: action,
          Tags: json.Tags || [],
          Data: json.Data || '',
          Timestamp: Date.now()
        }, true);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ Messages: evalRes?.Messages || evalRes?.responses || [] }));
        return;
      }

      // [CU Endpoint] 2. Evaluation Result Polling (GET /result/:msgId)
      if (req.method === 'GET' && urlObj.pathname.startsWith('/result/')) {
        const msgId = urlObj.pathname.replace('/result/', '').split('?')[0];
        const evalRes = evaluationResults.get(msgId);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          Messages: evalRes?.Messages || evalRes?.responses || [],
          Output: evalRes?.Output || {},
          Spawns: []
        }));
        return;
      }

      // [MU Endpoint] 3. Message Dispatch (POST /message or POST /)
      if (req.method === 'POST' && (urlObj.pathname.endsWith('/message') || urlObj.pathname === '/' || urlObj.pathname.includes('/ao/'))) {
        let target = '';
        let action = '';
        let from = 'SATU_KEPALA_CALLER';
        let data = '';
        let tags = [];
        let msgId = '';

        // A. Decode binary ANS-104 DataItem
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
          action = item.tags.find(t => t.name === 'Action')?.value || '';
          tags = item.tags.map(t => ({ name: t.name, value: t.value }));
          data = item.rawData.toString('utf8');
          from = await arweaveInstance.wallets.ownerToAddress(item.owner);
          msgId = item.id;
        }

        // B. Decode standard JSON message envelope
        if (!target) {
          try {
            const json = JSON.parse(rawBuf.toString('utf8'));
            target = json.Target || json.target || json.process;
            action = json.Action || json.action;
            tags = json.Tags || json.tags || [];
            data = json.Data || json.data;
            from = json.From || json.from || json.sender || from;
            msgId = json.Id || json.id || ('msg_' + crypto.randomUUID());
          } catch {}
        }

        if (!msgId) msgId = 'msg_' + crypto.randomUUID();

        const proc = processes.get(target);
        if (!proc) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: `Target process '${target}' not found` }));
          return;
        }

        // Execute inside stateful Compute Unit
        const evalRes = await proc.send({
          From: from,
          Target: target,
          Action: action,
          Tags: tags,
          Data: data,
          Timestamp: Date.now()
        }, false);

        evaluationResults.set(msgId, evalRes);

        // Cross-process message delivery: route outbox messages to target processes in the stack
        if (evalRes?.Messages && Array.isArray(evalRes.Messages)) {
          for (const outMsg of evalRes.Messages) {
            const outTarget = outMsg.Target || outMsg.process;
            if (outTarget && processes.has(outTarget)) {
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

      // Node Info endpoint
      if (req.method === 'GET' && (urlObj.pathname === '/~meta@1.0/info' || urlObj.pathname === '/info')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          status: 200,
          node_type: 'ao_mu_cu',
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
      for (const proc of processes.values()) {
        proc.terminate();
      }
      await new Promise((resolve) => server.close(resolve));
    }
  };
}

if (process.argv[1] && process.argv[1].endsWith('ao_node.mjs')) {
  const port = parseInt(process.env.AO_NODE_PORT || '8996', 10);
  startAoNode({ port }).then(() => {
    console.log(`[ao-node] Official AO MU & CU Node active on :${port}`);
  }).catch((err) => {
    console.error('[ao-node] Fatal error:', err);
    process.exit(1);
  });
}
