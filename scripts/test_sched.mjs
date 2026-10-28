import arbundles from 'arbundles';
import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = join(__dirname, '..');

const createData = arbundles.createData || arbundles.default?.createData;
const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
const jwk = JSON.parse(readFileSync(join(ROOT_DIR, 'secrets/wallet_genesis.json'), 'utf8'));
const signer = new ArweaveSigner(jwk);
const procId = 'n6emO_eAOZJo4qI1j11T-dX_pmZpaGCGR5SO4tJuW-k';

async function test() {
  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Type', value: 'Message' },
    { name: 'Target', value: procId },
    { name: 'Action', value: 'Eval' }
  ];
  const item = createData('return 42', signer, { tags });
  await item.sign(signer);
  const raw = item.getRaw();
  console.log('Sending item raw length:', raw.length);
  const res = await fetch('http://127.0.0.1:8733/' + procId + '~process@1.0/schedule', {
    method: 'POST',
    headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
    body: raw
  });
  console.log('Status:', res.status);
  const text = await res.text();
  console.log('Body:', text);

  // Now query now
  const nowRes = await fetch('http://127.0.0.1:8733/' + procId + '~process@1.0/now', {
    headers: { 'Accept': 'application/json' }
  });
  console.log('Now status:', nowRes.status);
  const nowData = await nowRes.json();
  console.log('Now at-slot:', nowData['at-slot'], 'results+link:', nowData['results+link']);
}
test().catch(console.error);
