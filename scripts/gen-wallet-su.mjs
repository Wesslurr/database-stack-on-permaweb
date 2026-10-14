import Arweave from 'arweave';
import { mkdirSync, writeFileSync } from 'node:fs';

const arweave = Arweave.init({});
const jwk = await arweave.wallets.generate();
const address = await arweave.wallets.jwkToAddress(jwk);

mkdirSync('secrets', { recursive: true });
writeFileSync('secrets/wallet_su.json', JSON.stringify(jwk));
writeFileSync('secrets/address_su.txt', address + '\n');
console.log('SU Wallet:', address);
