import { readFileSync, existsSync } from 'node:fs';
import * as crypto from 'node:crypto';

const files = [
  'hyperbeam-key.json',
  'secrets/wallet_genesis.json',
  'secrets/wallet.json',
  'secrets/wallet_su.json',
  'secrets/wallet_sd_2.json',
  'secrets/wallet_sd_3.json'
];

for (const f of files) {
  if (existsSync(f)) {
    const jwk = JSON.parse(readFileSync(f, 'utf8'));
    const hash = crypto.createHash('sha256').update(Buffer.from(jwk.n, 'base64url')).digest('base64url');
    console.log(f, '->', hash);
  }
}
