//! Compact entity encoding (`json_safe`, `pack_entity`, `unpack_entity`)
//! for canonical HyperStack Shared Data state.

use crate::num::{fmt6, parse_num};
use crate::prelude::*;

/// Escape JSON string values: `\` -> `\\`, `"` -> `\"`, `[\r\n\t\b\f]` -> space.
pub fn json_safe(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for b in s.bytes() {
        match b {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\r' | b'\n' | b'\t' | 0x08 | 0x0C => out.push(' '),
            _ => out.push(b as char),
        }
    }
    out
}

#[derive(Clone, Debug, PartialEq)]
pub struct Entity {
    pub x: f64,
    pub y: f64,
    pub tick: u64,
    pub owner: String,
}

/// Lua `pack_entity`: `"%.6f|%.6f|%d|%s"` with a `json_safe` owner.
pub fn pack_entity(x: f64, y: f64, tick: u64, owner: &str) -> String {
    format!("{}|{}|{}|{}", fmt6(x), fmt6(y), tick, json_safe(owner))
}

/// Lua `unpack_entity`: `^([%d%.%-]+)|([%d%.%-]+)|(%d+)|(.*)$`.
pub fn unpack_entity(packed: &str) -> Option<Entity> {
    let mut parts = packed.splitn(4, '|');
    let x_str = parts.next()?;
    let y_str = parts.next()?;
    let tick_str = parts.next()?;
    let owner_str = parts.next()?;

    let x = parse_num(x_str)?;
    let y = parse_num(y_str)?;
    let tick = tick_str.parse::<u64>().ok()?;
    Some(Entity {
        x,
        y,
        tick,
        owner: owner_str.to_owned(),
    })
}
