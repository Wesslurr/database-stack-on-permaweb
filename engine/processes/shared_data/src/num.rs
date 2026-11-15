//! Deterministic `no_std`-friendly numeric helpers without depending on
//! `std` float intrinsics.

use crate::prelude::*;

#[inline]
pub fn fabs(x: f64) -> f64 {
    if x < 0.0 { -x } else { x }
}

/// `math.floor` for finite values; NaN maps to 0 (Lua `tonumber` never yields NaN).
pub fn floor_i64(v: f64) -> i64 {
    if v.is_nan() || v.is_infinite() {
        return 0;
    }
    let i = v as i64;
    let diff = v - (i as f64);
    if diff < 0.0 {
        i - 1
    } else {
        i
    }
}

/// Lua `int_str`: `string.format("%d", math.floor(tonumber(value) or 0))`.
pub fn int_str(v: f64) -> String {
    format!("{}", floor_i64(v))
}

/// Lua `string.format("%.6f", v)`.
pub fn fmt6(v: f64) -> String {
    let sign = if v < 0.0 { "-" } else { "" };
    let abs_v = fabs(v);
    let int_part = floor_i64(abs_v);
    let frac_part = floor_i64((abs_v - (int_part as f64)) * 1_000_000.0 + 0.5);
    format!("{}{}.{:06}", sign, int_part, frac_part)
}

/// Decimal `tonumber`: accepts `[+-]digits[.digits][e[+-]digits]`, rejects
/// `inf`/`nan`/garbage (Lua returns nil for those).
pub fn parse_num(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    // Reject non-numeric keywords
    let lower = s.as_bytes();
    if lower.len() >= 3 {
        let b0 = lower[0].to_ascii_lowercase();
        let b1 = lower[1].to_ascii_lowercase();
        let b2 = lower[2].to_ascii_lowercase();
        if (b0 == b'i' && b1 == b'n' && b2 == b'f') || (b0 == b'n' && b1 == b'a' && b2 == b'n') {
            return None;
        }
    }
    s.parse::<f64>().ok().filter(|v| v.is_finite())
}
