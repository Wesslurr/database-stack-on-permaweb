import Arweave from 'arweave';
import { mkdirSync, writeFileSync } from 'node:fs';

const arweave = Arweave.init({});
const jwk = await arweave.wallets.generate();
const address = await arweave.wallets.jwkToAddress(jwk);

mkdirSync('secrets', { recursive: true });
writeFileSync('secrets/wallet.json', JSON.stringify(jwk));
writeFileSync('secrets/address.txt', address + '\n');
console.log(address);
