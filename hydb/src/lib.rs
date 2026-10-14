#![no_std]
use core::panic::PanicInfo;

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

pub const TABLE_SIZE: usize = 1024;
pub const MAX_PLAYERS: usize = 32;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Record {
    pub active: u8,
    pub key: u64,
    pub val: u64,
    pub ver: u32,
    pub last_seq: u64,
}

impl Record {
    const fn empty() -> Self {
        Self {
            active: 0,
            key: 0,
            val: 0,
            ver: 0,
            last_seq: 0,
        }
    }
}

pub struct HyDatabase {
    pub count: u32,
    pub total_mutations: u64,
    pub state_root: u64,
    pub records: [Record; TABLE_SIZE],
}

static mut DB: HyDatabase = HyDatabase {
    count: 0,
    total_mutations: 0,
    state_root: 0,
    records: [Record::empty(); TABLE_SIZE],
};

// Fixed memory buffer for Snapshot Export / Import (36KB)
#[allow(dead_code)]
static mut SNAPSHOT_BUFFER: [u8; 36864] = [0u8; 36864];

#[inline]
pub fn record_hash(key: u64, val: u64, ver: u32) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    h ^= key;
    h = h.wrapping_mul(0x100000001b3);
    h ^= val;
    h = h.wrapping_mul(0x100000001b3);
    h ^= ver as u64;
    h = h.wrapping_mul(0x100000001b3);
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

fn hash_slot(key: u64) -> usize {
    (key as usize) % TABLE_SIZE
}

/// O(1) In-Memory Put
#[no_mangle]
pub extern "C" fn db_put(key: i64, val: i64, seq: i64) -> i64 {
    unsafe {
        let ukey = key as u64;
        let uval = val as u64;
        let useq = seq as u64;
        let start_slot = hash_slot(ukey);

        // Linear probing
        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &mut DB.records[slot];

            if rec.active == 1 && rec.key == ukey {
                // Update existing record: subtract old hash, add new hash
                let old_h = record_hash(rec.key, rec.val, rec.ver);
                rec.val = uval;
                rec.ver = rec.ver.wrapping_add(1);
                rec.last_seq = useq;
                let new_h = record_hash(rec.key, rec.val, rec.ver);

                DB.state_root ^= old_h ^ new_h;
                DB.total_mutations += 1;
                return DB.state_root as i64;
            }

            if rec.active == 0 {
                // Insert into empty slot
                rec.active = 1;
                rec.key = ukey;
                rec.val = uval;
                rec.ver = 1;
                rec.last_seq = useq;

                let h = record_hash(ukey, uval, 1);
                DB.state_root ^= h;
                DB.count += 1;
                DB.total_mutations += 1;
                return DB.state_root as i64;
            }
        }
        -1 // Table full
    }
}

/// O(1) In-Memory Get
#[no_mangle]
pub extern "C" fn db_get(key: i64) -> i64 {
    unsafe {
        let ukey = key as u64;
        let start_slot = hash_slot(ukey);

        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &DB.records[slot];
            if rec.active == 1 && rec.key == ukey {
                return rec.val as i64;
            }
            if rec.active == 0 {
                break;
            }
        }
        -1 // Not found
    }
}

/// O(1) In-Memory Delete
#[no_mangle]
pub extern "C" fn db_del(key: i64, _seq: i64) -> i64 {
    unsafe {
        let ukey = key as u64;
        let start_slot = hash_slot(ukey);

        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &mut DB.records[slot];
            if rec.active == 1 && rec.key == ukey {
                let h = record_hash(rec.key, rec.val, rec.ver);
                DB.state_root ^= h;
                rec.active = 0;
                if DB.count > 0 {
                    DB.count -= 1;
                }
                DB.total_mutations += 1;
                return DB.state_root as i64;
            }
            if rec.active == 0 {
                break;
            }
        }
        DB.state_root as i64
    }
}

#[no_mangle]
pub extern "C" fn db_root_hash() -> i64 {
    unsafe { DB.state_root as i64 }
}

#[no_mangle]
pub extern "C" fn db_count() -> i64 {
    unsafe { DB.count as i64 }
}

#[no_mangle]
pub extern "C" fn db_version(key: i64) -> i64 {
    unsafe {
        let ukey = key as u64;
        let start_slot = hash_slot(ukey);
        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &DB.records[slot];
            if rec.active == 1 && rec.key == ukey {
                return rec.ver as i64;
            }
            if rec.active == 0 {
                break;
            }
        }
        0
    }
}

#[no_mangle]
pub extern "C" fn db_reset() -> i64 {
    unsafe {
        DB.count = 0;
        DB.total_mutations = 0;
        DB.state_root = 0;
        for i in 0..TABLE_SIZE {
            DB.records[i] = Record::empty();
        }
        0
    }
}

/// Snapshot Export: serializes all active records into binary format
/// Returns total bytes written
#[no_mangle]
pub extern "C" fn db_snapshot_export() -> i64 {
    unsafe {
        let mut offset = 0;
        // Header: [Magic:4] [Count:4] [StateRoot:8]
        SNAPSHOT_BUFFER[0..4].copy_from_slice(b"HYDB");
        offset += 4;

        let count_bytes = DB.count.to_le_bytes();
        SNAPSHOT_BUFFER[offset..offset+4].copy_from_slice(&count_bytes);
        offset += 4;

        let root_bytes = DB.state_root.to_le_bytes();
        SNAPSHOT_BUFFER[offset..offset+8].copy_from_slice(&root_bytes);
        offset += 8;

        for i in 0..TABLE_SIZE {
            let rec = &DB.records[i];
            if rec.active == 1 {
                // Record format: Key:8, Val:8, Ver:4, Seq:8 (28 bytes)
                SNAPSHOT_BUFFER[offset..offset+8].copy_from_slice(&rec.key.to_le_bytes());
                offset += 8;
                SNAPSHOT_BUFFER[offset..offset+8].copy_from_slice(&rec.val.to_le_bytes());
                offset += 8;
                SNAPSHOT_BUFFER[offset..offset+4].copy_from_slice(&rec.ver.to_le_bytes());
                offset += 4;
                SNAPSHOT_BUFFER[offset..offset+8].copy_from_slice(&rec.last_seq.to_le_bytes());
                offset += 8;
            }
        }
        offset as i64
    }
}

/// Snapshot Restore: imports active records from binary format and verifies root hash
#[no_mangle]
pub extern "C" fn db_snapshot_import(total_bytes: i64) -> i64 {
    unsafe {
        let len = total_bytes as usize;
        if len < 16 || &SNAPSHOT_BUFFER[0..4] != b"HYDB" {
            return -1; // Invalid header
        }

        let mut offset = 4;
        let mut count_arr = [0u8; 4];
        count_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset+4]);
        let expected_count = u32::from_le_bytes(count_arr);
        offset += 4;

        let mut root_arr = [0u8; 8];
        root_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset+8]);
        let expected_root = u64::from_le_bytes(root_arr);
        offset += 8;

        // Reset current DB
        db_reset();

        for _ in 0..expected_count {
            if offset + 28 > len {
                break;
            }
            let mut k_arr = [0u8; 8];
            k_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset+8]);
            let key = u64::from_le_bytes(k_arr);
            offset += 8;

            let mut v_arr = [0u8; 8];
            v_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset+8]);
            let val = u64::from_le_bytes(v_arr);
            offset += 8;

            let mut ver_arr = [0u8; 4];
            ver_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset+4]);
            let ver = u32::from_le_bytes(ver_arr);
            offset += 4;

            let mut seq_arr = [0u8; 8];
            seq_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset+8]);
            let seq = u64::from_le_bytes(seq_arr);
            offset += 8;

            // Direct restore into slot
            let start_slot = hash_slot(key);
            for i in 0..TABLE_SIZE {
                let slot = (start_slot + i) % TABLE_SIZE;
                let rec = &mut DB.records[slot];
                if rec.active == 0 {
                    rec.active = 1;
                    rec.key = key;
                    rec.val = val;
                    rec.ver = ver;
                    rec.last_seq = seq;
                    DB.state_root ^= record_hash(key, val, ver);
                    DB.count += 1;
                    break;
                }
            }
        }

        // Verify state root matches exactly
        if DB.state_root == expected_root {
            1 // Verified success
        } else {
            -2 // Root mismatch
        }
    }
}

/// Unified compute dispatcher: handles both backward-compat and HyDB operations
#[no_mangle]
pub extern "C" fn compute(action: i64, value: i64) -> i64 {
    match action {
        // Legacy add / multiply
        1 => {
            let cur = db_get(1);
            let n = if cur < 0 { value } else { cur + value };
            db_put(1, n, 1);
            n
        }
        2 => {
            let cur = db_get(1);
            let n = if cur < 0 { value } else { cur * value };
            db_put(1, n, 1);
            n
        }
        // HyDB Action 20: Put (packed: key in high 32, val in low 32)
        20 => {
            let k = (value >> 32) & 0xFFFFFFFF;
            let v = value & 0xFFFFFFFF;
            db_put(k, v, 1)
        }
        // HyDB Action 21: Get key
        21 => db_get(value),
        // HyDB Action 22: Del key
        22 => db_del(value, 1),
        // HyDB Action 23: Root hash
        23 => db_root_hash(),
        // HyDB Action 24: Count
        24 => db_count(),
        // HyDB Action 99: Reset
        99 => db_reset(),
        _ => 0,
    }
}

#[no_mangle]
pub extern "C" fn get_state() -> i64 {
    db_root_hash()
}

#[no_mangle]
pub extern "C" fn increment(delta: i64) -> i64 {
    compute(1, delta)
}

#[no_mangle]
pub extern "C" fn fac(n: f64) -> f64 {
    if n <= 1.0 { 1.0 } else { n * fac(n - 1.0) }
}
