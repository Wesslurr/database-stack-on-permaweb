const procId = 'n6emO_eAOZJo4qI1j11T-dX_pmZpaGCGR5SO4tJuW-k';

async function test() {
  console.log('Testing /compute:');
  const res = await fetch('http://127.0.0.1:8733/' + procId + '~process@1.0/compute', {
    headers: { 'Accept': 'application/json' }
  });
  console.log('compute status:', res.status);
  const data = await res.json();
  console.log('at-slot:', data['at-slot']);
  console.log('results+link:', data['results+link']);
}
test().catch(console.error);
