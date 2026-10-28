import * as crypto from 'crypto';
import * as fs from 'fs';

const files = [
  ['secrets/wallet_genesis.json', 'secrets/address_genesis.txt'],
  ['secrets/wallet.json', 'secrets/address.txt'],
  ['secrets/wallet_su.json', 'secrets/address_su.txt'],
  ['secrets/wallet_sd_2.json', 'secrets/address_sd_2.txt'],
  ['secrets/wallet_sd_3.json', 'secrets/address_sd_3.txt']
];

for (const [wFile, aFile] of files) {
  const jwk = JSON.parse(fs.readFileSync(wFile, 'utf8'));
  const nBuf = Buffer.from(jwk.n, 'base64url');
  const addr = crypto.createHash('sha256').update(nBuf).digest('base64url');
  console.log(wFile, '->', addr);
}
