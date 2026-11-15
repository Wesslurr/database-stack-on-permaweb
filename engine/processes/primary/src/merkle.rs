//! Canonical 32-shard state root (`HS2`), deterministic across Primary and
//! Shared Data (`id_shard` / `shard_digest` / `shard_root` / `candidate_root` /
//! `commit_shards`).
//!
//! THIS MODULE IS THE SHARED CRYPTOGRAPHIC CONTRACT. The Shared Data crate
//! must carry an identical copy; any change here is a consensus change.
use crate::prelude::*;
use sha2::{Digest, Sha256};

pub const SHARD_COUNT: usize = 32;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// `ao.sha256` / `sha256_hex_lua`: `"0x"` + lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in digest.iter() {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// FNV-1a 32-bit over the id bytes. Returns the 0-based shard index; the Lua
/// shard label is `index + 1`.
pub fn id_shard(id: &str) -> usize {
    let mut h: u32 = 2_166_136_261;
    for b in id.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    (h % SHARD_COUNT as u32) as usize
}

/// sha256 over `id=packed` lines (ids byte-sorted), joined by `\n`.
pub fn shard_digest(map: &BTreeMap<String, String>) -> String {
    let mut buf = String::new();
    for (i, (id, packed)) in map.iter().enumerate() {
        if i > 0 {
            buf.push('\n');
        }
        buf.push_str(id);
        buf.push('=');
        buf.push_str(packed);
    }
    sha256_hex(buf.as_bytes())
}

/// sha256 over `HS2|tick=<tick>|shards\n1:<h1>\n2:<h2>...\n32:<h32>`.
pub fn shard_root(hashes: &[String], tick: i64) -> String {
    let mut buf = format!("HS2|tick={}|shards\n", tick);
    for (i, h) in hashes.iter().enumerate().take(SHARD_COUNT) {
        if i > 0 {
            buf.push('\n');
        }
        buf.push_str(&format!("{}:{}", i + 1, h));
    }
    sha256_hex(buf.as_bytes())
}

#[derive(Clone, Debug)]
pub struct Shards {
    maps: Vec<BTreeMap<String, String>>,
    hashes: Vec<String>,
}

impl Default for Shards {
    fn default() -> Self {
        Self::new()
    }
}

impl Shards {
    pub fn new() -> Self {
        let empty = sha256_hex(b"");
        Shards {
            maps: (0..SHARD_COUNT).map(|_| BTreeMap::new()).collect(),
            hashes: vec![empty; SHARD_COUNT],
        }
    }

    /// Lua `build_shards`.
    pub fn from_points(points: &BTreeMap<String, String>) -> Self {
        let mut s = Shards::new();
        for (id, packed) in points {
            s.maps[id_shard(id)].insert(id.clone(), packed.clone());
        }
        for i in 0..SHARD_COUNT {
            s.hashes[i] = shard_digest(&s.maps[i]);
        }
        s
    }

    pub fn root(&self, tick: i64) -> String {
        shard_root(&self.hashes, tick)
    }

    /// Lua `candidate_root`: root of (shards + delta) without mutating.
    pub fn candidate_root(&self, delta: &BTreeMap<String, String>, tick: i64) -> String {
        let touched: BTreeSet<usize> = delta.keys().map(|id| id_shard(id)).collect();
        let mut next = self.hashes.clone();
        for si in touched {
            let mut tmp = self.maps[si].clone();
            for (id, packed) in delta {
                if id_shard(id) == si {
                    tmp.insert(id.clone(), packed.clone());
                }
            }
            next[si] = shard_digest(&tmp);
        }
        shard_root(&next, tick)
    }

    /// Lua `commit_shards`: apply an accepted delta and return the new root.
    pub fn commit(&mut self, delta: &BTreeMap<String, String>, tick: i64) -> String {
        let mut touched = BTreeSet::new();
        for (id, packed) in delta {
            let si = id_shard(id);
            self.maps[si].insert(id.clone(), packed.clone());
            touched.insert(si);
        }
        for si in touched {
            self.hashes[si] = shard_digest(&self.maps[si]);
        }
        shard_root(&self.hashes, tick)
    }
}
