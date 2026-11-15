//! Compact entity encoding (`json_safe`, `pack_entity`, `unpack_entity`)
//! for canonical HyperStack 2D points state.
use crate::num::{is_numeric_field, parse_num};
use crate::prelude::*;

pub const ANONYMOUS: &str = "anonymous";

#[derive(Clone, Debug, PartialEq)]
pub struct Entity {
    pub x: f64,
    pub y: f64,
    pub tick: i64,
    pub owner: String,
}

/// Lua `json_safe`: `\` -> `\\`, `"` -> `\"`, `[\r\n\t\b\f]` -> space.
pub fn json_safe(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\r' | '\n' | '\t' | '\u{8}' | '\u{c}' => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}

/// Lua `pack_entity`: `"%.6f|%.6f|%d|%s"` with a `json_safe` owner.
pub fn pack_entity(x: f64, y: f64, tick: i64, owner: &str) -> String {
    format!("{:.6}|{:.6}|{}|{}", x, y, tick, json_safe(owner))
}

/// Lua `unpack_entity`: `^([%d%.%-]+)|([%d%.%-]+)|(%d+)|(.*)$`.
pub fn unpack_entity(packed: &str) -> Option<Entity> {
    let mut it = packed.splitn(4, '|');
    let xs = it.next()?;
    let ys = it.next()?;
    let ts = it.next()?;
    let owner = it.next()?;
    if !is_numeric_field(xs) || !is_numeric_field(ys) {
        return None;
    }
    if ts.is_empty() || !ts.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(Entity {
        x: parse_num(xs).unwrap_or(0.0),
        y: parse_num(ys).unwrap_or(0.0),
        tick: ts.parse().unwrap_or(0),
        owner: owner.to_string(),
    })
}
