//! Deterministic `no_std`-friendly numeric helpers without depending on
//! `std` float intrinsics.
use crate::prelude::*;

pub fn fabs(v: f64) -> f64 {
    if v < 0.0 {
        -v
    } else {
        v
    }
}

/// `math.floor` for finite values; NaN maps to 0 (Lua `tonumber` never yields NaN).
pub fn floor_i64(v: f64) -> i64 {
    if v != v {
        return 0;
    }
    let t = v as i64;
    if (t as f64) > v {
        t - 1
    } else {
        t
    }
}

/// Lua `int_str`: `string.format("%d", math.floor(tonumber(value) or 0))`.
pub fn int_str(v: f64) -> String {
    format!("{}", floor_i64(v))
}

/// Lua `string.format("%.6f", v)`.
pub fn fmt6(v: f64) -> String {
    format!("{:.6}", v)
}

/// Lua 5.3 `tostring` of a float value (used only by the audit ring).
pub fn lua_tostring(v: f64) -> String {
    if v == v && fabs(v) < 1e15 && (floor_i64(v) as f64) == v {
        format!("{}.0", floor_i64(v))
    } else {
        format!("{}", v)
    }
}

/// Decimal `tonumber`: accepts `[+-]digits[.digits][e[+-]digits]`, rejects
/// `inf`/`nan`/garbage (Lua returns nil for those).
pub fn parse_num(s: &str) -> Option<f64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    if !t
        .bytes()
        .all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E'))
    {
        return None;
    }
    let v: f64 = t.parse().ok()?;
    if v != v {
        None
    } else {
        Some(v)
    }
}

/// Lua pattern class `[%d%.%-]+`.
pub fn is_numeric_field(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit() || b == b'.' || b == b'-')
}
