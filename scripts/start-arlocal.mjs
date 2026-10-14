import mod from 'arlocal';

const ArLocal = typeof mod === 'function' ? mod : mod.default;
const arLocal = new ArLocal(1984, true, '.arlocal-db', true);
await arLocal.start();
console.log('[arlocal] UP on :1984 (persist .arlocal-db)');
