import { readFileSync, existsSync } from 'node:fs';
import * as crypto from 'node:crypto';

const relDir = 'vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/secrets';
const files = ['wallet_genesis.json', 'wallet.json', 'wallet_su.json', 'wallet_sd_2.json', 'wallet_sd_3.json'];

for (const f of files) {
  const p = `${relDir}/${f}`;
  if (existsSync(p)) {
    const jwk = JSON.parse(readFileSync(p, 'utf8'));
    const hash = crypto.createHash('sha256').update(Buffer.from(jwk.n, 'base64url')).digest('base64url');
    console.log(f, '->', hash);
  }
}
