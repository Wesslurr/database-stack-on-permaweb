//! Input and security guard rails for HyperStack Primary WASM ingress.
use crate::entity::{unpack_entity, Entity, ANONYMOUS};
use crate::num::fabs;
use crate::prelude::*;

pub const MAX_ENTITIES: usize = 1000;
pub const MAX_PAYLOAD_BYTES: usize = 65536;
pub const MAX_ID_LENGTH: usize = 128;
pub const MAX_QUEUE: usize = 4096;
pub const MAX_COORD: f64 = 1000.0;

#[derive(Clone, Debug, PartialEq)]
pub enum GuardError {
    PayloadTooLarge,
    InvalidEntityId,
    InvalidCoordinates,
    UnauthorizedOwner,
    EntityLimitReached,
    QueueFull,
}

impl GuardError {
    pub fn http_status(&self) -> u16 {
        match self {
            GuardError::PayloadTooLarge => 413,
            GuardError::InvalidEntityId => 400,
            GuardError::InvalidCoordinates => 400,
            GuardError::UnauthorizedOwner => 403,
            GuardError::EntityLimitReached => 409,
            GuardError::QueueFull => 503,
        }
    }

    pub fn status_str(&self) -> &'static str {
        match self {
            GuardError::PayloadTooLarge => "error_payload_too_large",
            GuardError::InvalidEntityId => "error_invalid_entity_id",
            GuardError::InvalidCoordinates => "error_invalid_coordinates",
            GuardError::UnauthorizedOwner => "error_unauthorized_owner",
            GuardError::EntityLimitReached => "error_entity_limit_reached",
            GuardError::QueueFull => "error_queue_full",
        }
    }
}

pub fn contains_newlines(s: &str) -> bool {
    s.as_bytes().iter().any(|&b| b == b'\r' || b == b'\n')
}

pub fn validate_point_input(
    id: Option<&str>,
    x: Option<f64>,
    y: Option<f64>,
    client_signer: Option<&str>,
    existing_packed: Option<&str>,
    active_entity_count: usize,
    queue_len: usize,
    payload_len: usize,
    max_entities: usize,
) -> Result<(String, f64, f64, String), GuardError> {
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(GuardError::PayloadTooLarge);
    }

    let id_str = match id {
        Some(s) => {
            if s.is_empty() || s.len() > MAX_ID_LENGTH || contains_newlines(s) {
                return Err(GuardError::InvalidEntityId);
            }
            s.to_string()
        }
        None => return Err(GuardError::InvalidEntityId),
    };

    let x_val = match x {
        Some(v) => {
            if v != v || fabs(v) > MAX_COORD {
                return Err(GuardError::InvalidCoordinates);
            }
            v
        }
        None => return Err(GuardError::InvalidCoordinates),
    };

    let y_val = match y {
        Some(v) => {
            if v != v || fabs(v) > MAX_COORD {
                return Err(GuardError::InvalidCoordinates);
            }
            v
        }
        None => return Err(GuardError::InvalidCoordinates),
    };

    let existing_entity: Option<Entity> = existing_packed.and_then(unpack_entity);
    let candidate_owner = client_signer
        .map(|s| s.to_string())
        .or_else(|| existing_entity.as_ref().map(|e| e.owner.clone()))
        .unwrap_or_else(|| ANONYMOUS.to_string());

    if let Some(ref existing) = existing_entity {
        if existing.owner != ANONYMOUS && Some(existing.owner.as_str()) != client_signer {
            return Err(GuardError::UnauthorizedOwner);
        }
    }

    if existing_entity.is_none() && active_entity_count >= max_entities {
        return Err(GuardError::EntityLimitReached);
    }

    if queue_len >= MAX_QUEUE {
        return Err(GuardError::QueueFull);
    }

    Ok((id_str, x_val, y_val, candidate_owner))
}
