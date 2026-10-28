import { readFileSync } from 'node:fs';
import * as arbundles from 'arbundles';

const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;

const files = [
  'secrets/wallet_genesis.json',
  'secrets/wallet.json',
  'secrets/wallet_su.json',
  'secrets/wallet_sd_2.json',
  'secrets/wallet_sd_3.json'
];

for (const f of files) {
  const jwk = JSON.parse(readFileSync(f, 'utf8'));
  const signer = new ArweaveSigner(jwk);
  const rawOwner = signer.publicKey;
  const hash = arbundles.crypto ? arbundles.crypto.hash(rawOwner) : null;
  // arbundles base64url
  const addr = arbundles.utils?.byteArrayToB64 ? arbundles.utils.byteArrayToB64(hash) : 'n/a';
  console.log(f, '->', signer.owner ? signer.owner.length : 'none');
}
