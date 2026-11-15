//! Shared `alloc` imports so every module works identically under `no_std`
//! (wasm32) and `std` (native tests).

extern crate alloc;

pub use alloc::borrow::ToOwned;
pub use alloc::collections::{BTreeMap, BTreeSet, VecDeque};
pub use alloc::format;
pub use alloc::string::{String, ToString};
pub use alloc::vec;
pub use alloc::vec::Vec;
