const nodes = [
  { name: 'Genesis', port: 8733 },
  { name: 'Hot', port: 8734 },
  { name: 'SD-1', port: 8735 },
  { name: 'SD-2', port: 8745 },
  { name: 'SD-3', port: 8755 }
];

async function check() {
  console.log('=== CLUSTER STATUS ===');
  for (const n of nodes) {
    try {
      const meta = await fetch(`http://127.0.0.1:${n.port}/~meta@1.0/info`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
      console.log(`[${n.name}:${n.port}] OK - Address: ${meta.address}`);
    } catch (e) {
      console.log(`[${n.name}:${n.port}] ERROR: ${e.message}`);
    }
  }

  try {
    const arl = await fetch('http://127.0.0.1:1984/info').then(r => r.json());
    console.log(`[ArLocal:1984] OK - Network: ${arl.network}, Blocks: ${arl.blocks}, Height: ${arl.height}`);
  } catch (e) {
    console.log(`[ArLocal:1984] ERROR: ${e.message}`);
  }

  for (const p of [8735, 8745, 8755]) {
    try {
      const status = await fetch(`http://127.0.0.1:${p}/~sd@1.0/status`, { headers: { 'Accept': 'application/json' } }).then(r => r.json());
      console.log(`[SD on :${p}] Tick: ${status.latest_tick}, StateRoot: ${status.state_root_str}, Connected: ${status.connected_to_hot}, Health: ${status.health}`);
    } catch (e) {
      console.log(`[SD on :${p}] ERROR: ${e.message}`);
    }
  }
}

check();
