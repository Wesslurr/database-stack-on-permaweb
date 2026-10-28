const ports = [8733, 8734, 8735, 8745, 8755];
for (const p of ports) {
  try {
    const res = await fetch(`http://127.0.0.1:${p}/~meta@1.0/info`, {
      headers: { 'Accept': 'application/json' }
    });
    const data = await res.json();
    console.log(p, '->', data.address);
  } catch (e) {
    console.log(p, '-> ERROR', e.message);
  }
}
