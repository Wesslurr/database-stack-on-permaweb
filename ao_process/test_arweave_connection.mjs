import Arweave from 'arweave';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_DIR = process.env.DB_DIR || resolve(__dirname, '..');

const arweave = Arweave.init({
  host: '127.0.0.1',
  port: 1984,
  protocol: 'http',
});

async function main() {
  const walletPath = process.env.WALLET_PATH || join(DB_DIR, 'secrets/wallet.json');
  const jwk = JSON.parse(readFileSync(walletPath, 'utf-8'));
  const address = await arweave.wallets.jwkToAddress(jwk);
  const balance = await arweave.wallets.getBalance(address);
  console.log(`Connected to ArLocal. Address: ${address}, Balance: ${balance} winston`);

  // Create a test transaction
  const tx = await arweave.createTransaction({
    data: Buffer.from(JSON.stringify({ test: "hello AO", timestamp: Date.now() })),
  }, jwk);

  tx.addTag('App-Name', 'AO');
  tx.addTag('Type', 'Process-Init');
  tx.addTag('Data-Protocol', 'ao');

  await arweave.transactions.sign(tx, jwk);
  const response = await arweave.transactions.post(tx);
  console.log(`Transaction posted. Status: ${response.status}, ID: ${tx.id}`);

  // Mine the block
  const mineRes = await fetch('http://127.0.0.1:1984/mine').then(r => r.json());
  console.log(`Block mined. Height: ${mineRes.height}, Current: ${mineRes.current}`);

  // Check tx status
  const txStatus = await arweave.transactions.getStatus(tx.id);
  console.log(`TX Status:`, txStatus);
}

main().catch(console.error);
