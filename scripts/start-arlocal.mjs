import mod from 'arlocal';

const ArLocal = typeof mod === 'function' ? mod : mod.default;
const port = parseInt(process.env.ARLOCAL_PORT || '1984', 10);
const arLocal = new ArLocal(port, true, '.arlocal-db', true);
await arLocal.start();
console.log(`[arlocal] UP on :${port} (persist .arlocal-db)`);
