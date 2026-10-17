#![no_std]
use core::panic::PanicInfo;

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    loop {}
}

pub const TABLE_SIZE: usize = 1024;
pub const MAX_CONTRACTS: usize = 64;

pub const SLOT_EMPTY: u8 = 0;
pub const SLOT_ACTIVE: u8 = 1;
pub const SLOT_TOMBSTONE: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Record {
    pub active: u8,
    pub ns: u64,
    pub key: u64,
    pub val: u64,
    pub ver: u32,
    pub last_seq: u64,
}

impl Record {
    const fn empty() -> Self {
        Self {
            active: SLOT_EMPTY,
            ns: 0,
            key: 0,
            val: 0,
            ver: 0,
            last_seq: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ContractRule {
    pub active: u8,
    pub has_min: u8,
    pub has_max: u8,
    pub ns: u64,
    pub min_val: i64,
    pub max_val: i64,
    pub max_records: u32,
    pub violation_count: u32,
}

impl ContractRule {
    const fn empty() -> Self {
        Self {
            active: 0,
            has_min: 0,
            has_max: 0,
            ns: 0,
            min_val: i64::MIN,
            max_val: i64::MAX,
            max_records: TABLE_SIZE as u32,
            violation_count: 0,
        }
    }
}

pub struct HyDatabase {
    pub count: u32,
    pub total_mutations: u64,
    pub state_root: u64,
    pub records: [Record; TABLE_SIZE],
    pub contracts: [ContractRule; MAX_CONTRACTS],
}

static mut DB: HyDatabase = HyDatabase {
    count: 0,
    total_mutations: 0,
    state_root: 0,
    records: [Record::empty(); TABLE_SIZE],
    contracts: [ContractRule::empty(); MAX_CONTRACTS],
};

// Fixed memory buffer for Snapshot Export / Import (44KB: 16B header + 1024 * 36B records)
#[allow(dead_code)]
static mut SNAPSHOT_BUFFER: [u8; 45056] = [0u8; 45056];

#[inline]
pub fn composite_key(ns: u64, key: u64) -> u64 {
    if ns == 0 {
        key
    } else {
        ns ^ key
    }
}

/// Fast 64-bit FNV-1a + SplitMix64 record hash for incremental Merkle-XOR fingerprint
#[inline]
pub fn record_hash(comp_key: u64, val: u64, ver: u32) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    h ^= comp_key;
    h = h.wrapping_mul(0x100000001b3);
    h ^= val;
    h = h.wrapping_mul(0x100000001b3);
    h ^= ver as u32 as u64;
    h = h.wrapping_mul(0x100000001b3);
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

#[inline]
fn hash_slot(ns: u64, key: u64) -> usize {
    (composite_key(ns, key) as usize) % TABLE_SIZE
}

/// Register or update contract invariants directly inside WASM linear memory
#[no_mangle]
pub extern "C" fn db_set_contract_ns(
    ns: i64,
    has_min: i64,
    min_val: i64,
    has_max: i64,
    max_val: i64,
    max_records: i64,
) -> i64 {
    unsafe {
        let uns = ns as u64;
        let max_rec = if max_records > 0 && (max_records as usize) <= TABLE_SIZE {
            max_records as u32
        } else {
            TABLE_SIZE as u32
        };
        let mut empty_idx: Option<usize> = None;
        for i in 0..MAX_CONTRACTS {
            if DB.contracts[i].active == 1 && DB.contracts[i].ns == uns {
                DB.contracts[i].has_min = if has_min != 0 { 1 } else { 0 };
                DB.contracts[i].min_val = min_val;
                DB.contracts[i].has_max = if has_max != 0 { 1 } else { 0 };
                DB.contracts[i].max_val = max_val;
                DB.contracts[i].max_records = max_rec;
                return 1;
            }
            if DB.contracts[i].active == 0 && empty_idx.is_none() {
                empty_idx = Some(i);
            }
        }
        if let Some(idx) = empty_idx {
            DB.contracts[idx] = ContractRule {
                active: 1,
                has_min: if has_min != 0 { 1 } else { 0 },
                has_max: if has_max != 0 { 1 } else { 0 },
                ns: uns,
                min_val,
                max_val,
                max_records: max_rec,
                violation_count: 0,
            };
            return 1;
        }
        0
    }
}

/// Count active records belonging to a specific namespace inside WASM
#[no_mangle]
pub extern "C" fn db_count_ns(ns: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        let mut c: u32 = 0;
        for i in 0..TABLE_SIZE {
            let rec = &DB.records[i];
            if rec.active == SLOT_ACTIVE && rec.ns == uns {
                c += 1;
            }
        }
        c as i64
    }
}

/// Return total contract violations caught inside WASM for a namespace
#[no_mangle]
pub extern "C" fn db_contract_violations_ns(ns: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        for i in 0..MAX_CONTRACTS {
            if DB.contracts[i].active == 1 && DB.contracts[i].ns == uns {
                return DB.contracts[i].violation_count as i64;
            }
        }
        0
    }
}

/// Validate a candidate Put against the in-WASM Contract Rule Table.
/// Returns:
///   1    => Valid
///  -422  => Schema min/max invariant violated inside WASM
///  -507  => WASM namespace/table capacity exceeded
#[no_mangle]
pub extern "C" fn db_check_put_ns(ns: i64, key: i64, val: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        let exists = db_has_ns(ns, key) == 1;
        for i in 0..MAX_CONTRACTS {
            if DB.contracts[i].active == 1 && DB.contracts[i].ns == uns {
                if DB.contracts[i].has_min == 1 && val < DB.contracts[i].min_val {
                    DB.contracts[i].violation_count = DB.contracts[i].violation_count.wrapping_add(1);
                    return -422;
                }
                if DB.contracts[i].has_max == 1 && val > DB.contracts[i].max_val {
                    DB.contracts[i].violation_count = DB.contracts[i].violation_count.wrapping_add(1);
                    return -422;
                }
                if !exists {
                    let ns_count = db_count_ns(ns) as u32;
                    if ns_count >= DB.contracts[i].max_records || DB.count >= (TABLE_SIZE as u32) {
                        DB.contracts[i].violation_count = DB.contracts[i].violation_count.wrapping_add(1);
                        return -507;
                    }
                }
                return 1;
            }
        }
        if !exists && DB.count >= (TABLE_SIZE as u32) {
            return -507;
        }
        1
    }
}

/// O(1) Namespace-Isolated In-Memory Put with Tombstone-aware Linear Probing
/// Enforces in-WASM Contract Rules directly at the WebAssembly instruction level!
#[no_mangle]
pub extern "C" fn db_put_ns(ns: i64, key: i64, val: i64, seq: i64) -> i64 {
    unsafe {
        if db_check_put_ns(ns, key, val) < 0 {
            return DB.state_root as i64;
        }
        let uns = ns as u64;
        let ukey = key as u64;
        let uval = val as u64;
        let useq = seq as u64;
        let ckey = composite_key(uns, ukey);
        let start_slot = hash_slot(uns, ukey);
        let mut first_tombstone: Option<usize> = None;

        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &mut DB.records[slot];

            if rec.active == SLOT_ACTIVE && rec.ns == uns && rec.key == ukey {
                // Update existing active record in this namespace: XOR out old hash, XOR in new hash
                let old_h = record_hash(ckey, rec.val, rec.ver);
                rec.val = uval;
                rec.ver = rec.ver.wrapping_add(1);
                rec.last_seq = useq;
                let new_h = record_hash(ckey, rec.val, rec.ver);

                DB.state_root ^= old_h ^ new_h;
                DB.total_mutations += 1;
                return DB.state_root as i64;
            }

            if rec.active == SLOT_TOMBSTONE {
                if rec.ns == uns && rec.key == ukey {
                    // Re-activating previously deleted key in the same namespace
                    // Preserve monotonic version continuity (ver + 1)
                    rec.active = SLOT_ACTIVE;
                    rec.val = uval;
                    rec.ver = rec.ver.wrapping_add(1);
                    rec.last_seq = useq;

                    let new_h = record_hash(ckey, uval, rec.ver);
                    DB.state_root ^= new_h;
                    DB.count += 1;
                    DB.total_mutations += 1;
                    return DB.state_root as i64;
                }
                if first_tombstone.is_none() {
                    first_tombstone = Some(slot);
                }
                // Continue probing to ensure (uns, ukey) does not exist later in the cluster
                continue;
            }

            if rec.active == SLOT_EMPTY {
                let target_slot = match first_tombstone {
                    Some(t_slot) => t_slot,
                    None => slot,
                };
                let target_rec = &mut DB.records[target_slot];
                target_rec.active = SLOT_ACTIVE;
                target_rec.ns = uns;
                target_rec.key = ukey;
                target_rec.val = uval;
                target_rec.ver = 1;
                target_rec.last_seq = useq;

                let h = record_hash(ckey, uval, 1);
                DB.state_root ^= h;
                DB.count += 1;
                DB.total_mutations += 1;
                return DB.state_root as i64;
            }
        }

        // If table has no SLOT_EMPTY left but had a tombstone, reuse it
        if let Some(t_slot) = first_tombstone {
            let target_rec = &mut DB.records[t_slot];
            target_rec.active = SLOT_ACTIVE;
            target_rec.ns = uns;
            target_rec.key = ukey;
            target_rec.val = uval;
            target_rec.ver = 1;
            target_rec.last_seq = useq;

            let h = record_hash(ckey, uval, 1);
            DB.state_root ^= h;
            DB.count += 1;
            DB.total_mutations += 1;
            return DB.state_root as i64;
        }

        DB.state_root as i64
    }
}

#[no_mangle]
pub extern "C" fn db_put(key: i64, val: i64, seq: i64) -> i64 {
    db_put_ns(0, key, val, seq)
}

/// O(1) Namespace-Isolated Existence Check (1 = found, 0 = not found)
#[no_mangle]
pub extern "C" fn db_has_ns(ns: i64, key: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        let ukey = key as u64;
        let start_slot = hash_slot(uns, ukey);

        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &DB.records[slot];
            if rec.active == SLOT_ACTIVE && rec.ns == uns && rec.key == ukey {
                return 1;
            }
            if rec.active == SLOT_EMPTY {
                break;
            }
        }
        0
    }
}

#[no_mangle]
pub extern "C" fn db_has(key: i64) -> i64 {
    db_has_ns(0, key)
}

/// O(1) Namespace-Isolated In-Memory Get (Tombstone-aware)
#[no_mangle]
pub extern "C" fn db_get_ns(ns: i64, key: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        let ukey = key as u64;
        let start_slot = hash_slot(uns, ukey);

        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &DB.records[slot];
            if rec.active == SLOT_ACTIVE && rec.ns == uns && rec.key == ukey {
                return rec.val as i64;
            }
            if rec.active == SLOT_EMPTY {
                break;
            }
        }
        -1
    }
}

#[no_mangle]
pub extern "C" fn db_get(key: i64) -> i64 {
    db_get_ns(0, key)
}

/// O(1) Namespace-Isolated In-Memory Delete (Marks slot as SLOT_TOMBSTONE)
#[no_mangle]
pub extern "C" fn db_del_ns(ns: i64, key: i64, seq: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        let ukey = key as u64;
        let useq = seq as u64;
        let ckey = composite_key(uns, ukey);
        let start_slot = hash_slot(uns, ukey);

        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &mut DB.records[slot];
            if rec.active == SLOT_ACTIVE && rec.ns == uns && rec.key == ukey {
                let h = record_hash(ckey, rec.val, rec.ver);
                DB.state_root ^= h;
                rec.active = SLOT_TOMBSTONE;
                rec.last_seq = useq;
                if DB.count > 0 {
                    DB.count -= 1;
                }
                DB.total_mutations += 1;
                return DB.state_root as i64;
            }
            if rec.active == SLOT_EMPTY {
                break;
            }
        }
        DB.state_root as i64
    }
}

#[no_mangle]
pub extern "C" fn db_del(key: i64, seq: i64) -> i64 {
    db_del_ns(0, key, seq)
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
pub extern "C" fn db_version_ns(ns: i64, key: i64) -> i64 {
    unsafe {
        let uns = ns as u64;
        let ukey = key as u64;
        let start_slot = hash_slot(uns, ukey);
        for i in 0..TABLE_SIZE {
            let slot = (start_slot + i) % TABLE_SIZE;
            let rec = &DB.records[slot];
            if rec.active == SLOT_ACTIVE && rec.ns == uns && rec.key == ukey {
                return rec.ver as i64;
            }
            if rec.active == SLOT_EMPTY {
                break;
            }
        }
        0
    }
}

#[no_mangle]
pub extern "C" fn db_version(key: i64) -> i64 {
    db_version_ns(0, key)
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
        for i in 0..MAX_CONTRACTS {
            DB.contracts[i] = ContractRule::empty();
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
        SNAPSHOT_BUFFER[offset..offset + 4].copy_from_slice(&count_bytes);
        offset += 4;

        let root_bytes = DB.state_root.to_le_bytes();
        SNAPSHOT_BUFFER[offset..offset + 8].copy_from_slice(&root_bytes);
        offset += 8;

        for i in 0..TABLE_SIZE {
            let rec = &DB.records[i];
            if rec.active == SLOT_ACTIVE {
                // Record format: Ns:8, Key:8, Val:8, Ver:4, Seq:8 (36 bytes)
                SNAPSHOT_BUFFER[offset..offset + 8].copy_from_slice(&rec.ns.to_le_bytes());
                offset += 8;
                SNAPSHOT_BUFFER[offset..offset + 8].copy_from_slice(&rec.key.to_le_bytes());
                offset += 8;
                SNAPSHOT_BUFFER[offset..offset + 8].copy_from_slice(&rec.val.to_le_bytes());
                offset += 8;
                SNAPSHOT_BUFFER[offset..offset + 4].copy_from_slice(&rec.ver.to_le_bytes());
                offset += 4;
                SNAPSHOT_BUFFER[offset..offset + 8].copy_from_slice(&rec.last_seq.to_le_bytes());
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
        count_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 4]);
        let expected_count = u32::from_le_bytes(count_arr);
        offset += 4;

        let mut root_arr = [0u8; 8];
        root_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 8]);
        let expected_root = u64::from_le_bytes(root_arr);
        offset += 8;

        db_reset();

        for _ in 0..expected_count {
            if offset + 36 > len {
                break;
            }
            let mut ns_arr = [0u8; 8];
            ns_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 8]);
            let ns = u64::from_le_bytes(ns_arr);
            offset += 8;

            let mut k_arr = [0u8; 8];
            k_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 8]);
            let key = u64::from_le_bytes(k_arr);
            offset += 8;

            let mut v_arr = [0u8; 8];
            v_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 8]);
            let val = u64::from_le_bytes(v_arr);
            offset += 8;

            let mut ver_arr = [0u8; 4];
            ver_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 4]);
            let ver = u32::from_le_bytes(ver_arr);
            offset += 4;

            let mut seq_arr = [0u8; 8];
            seq_arr.copy_from_slice(&SNAPSHOT_BUFFER[offset..offset + 8]);
            let seq = u64::from_le_bytes(seq_arr);
            offset += 8;

            let ckey = composite_key(ns, key);
            let start_slot = hash_slot(ns, key);
            for i in 0..TABLE_SIZE {
                let slot = (start_slot + i) % TABLE_SIZE;
                let rec = &mut DB.records[slot];
                if rec.active == SLOT_EMPTY {
                    rec.active = SLOT_ACTIVE;
                    rec.ns = ns;
                    rec.key = key;
                    rec.val = val;
                    rec.ver = ver;
                    rec.last_seq = seq;
                    DB.state_root ^= record_hash(ckey, val, ver);
                    DB.count += 1;
                    break;
                }
            }
        }

        if DB.state_root == expected_root {
            1
        } else {
            -2
        }
    }
}

/// Unified compute dispatcher for HyperBEAM wasm-64@1.0 calls
#[no_mangle]
pub extern "C" fn compute(action: i64, value: i64) -> i64 {
    match action {
        20 => {
            let k = (value >> 32) & 0xFFFFFFFF;
            let v = value & 0xFFFFFFFF;
            db_put(k, v, 1)
        }
        21 => db_get(value),
        22 => db_del(value, 1),
        23 => db_root_hash(),
        24 => db_count(),
        25 => db_has(value),
        99 => db_reset(),
        _ => 0,
    }
}

#[no_mangle]
pub extern "C" fn get_state() -> i64 {
    db_root_hash()
}
