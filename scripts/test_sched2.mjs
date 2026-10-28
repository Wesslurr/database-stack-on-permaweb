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
  const item = createData('return 999', signer, { tags });
  await item.sign(signer);
  const raw = item.getRaw();
  const schedRes = await fetch('http://127.0.0.1:8733/' + procId + '~process@1.0/schedule', {
    method: 'POST',
    headers: { 'Content-Type': 'application/ans104', 'Accept': 'application/json' },
    body: raw
  }).then(r => r.json());
  console.log('Scheduled slot:', schedRes.slot);

  console.log('Fetching /compute:');
  const compRes = await fetch('http://127.0.0.1:8733/' + procId + '~process@1.0/compute', {
    headers: { 'Accept': 'application/json' }
  }).then(r => r.json());
  console.log('Compute at-slot:', compRes['at-slot'], 'results+link:', compRes['results+link']);
}
test().catch(console.error);
