import { readFileSync } from 'node:fs';
import * as arbundles from 'arbundles';

async function main() {
  const ArweaveSigner = arbundles.signers?.ArweaveSigner || arbundles.default?.signers?.ArweaveSigner;
  const createData = arbundles.createData || arbundles.default?.createData;

  const wallet = JSON.parse(readFileSync('secrets/wallet_su.json', 'utf8'));
  const signer = new ArweaveSigner(wallet);

  const tags = [
    { name: 'Data-Protocol', value: 'ao' },
    { name: 'Variant', value: 'ao.TN.1' },
    { name: 'Type', value: 'Process' },
    { name: 'Device', value: 'process@1.0' },
    { name: 'Scheduler-Device', value: 'scheduler@1.0' },
    { name: 'Execution-Device', value: 'stack@1.0' },
    { name: 'Scheduler', value: 'XpOEPX668w7VNicVDHK81unzeeH8HcD83fKZRZKx9pk' }
  ];

  const dataItem = createData('', signer, { tags });
  await dataItem.sign(signer);

  console.log('Created DataItem:', {
    id: dataItem.id,
    rawLength: dataItem.getRaw().length,
    tags
  });

  const res = await fetch('http://127.0.0.1:8734/~push@1.0/push', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/ans104',
      'Accept': 'application/json'
    },
    body: dataItem.getRaw()
  });

  console.log('HTTP Status:', res.status);
  console.log('HTTP Headers:', Object.fromEntries(res.headers.entries()));
  const text = await res.text();
  console.log('HTTP Body:', text);
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
