use crate::CacheDigest;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const AGENT_PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentRequest {
    Hello {
        protocol: u8,
        client_version: String,
    },
    FindBlob {
        digest: CacheDigest,
    },
    StoreBlob {
        digest: CacheDigest,
        source: PathBuf,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentResponse {
    Hello { protocol: u8, agent_version: String },
    Blob { path: Option<PathBuf> },
    Stored { path: PathBuf },
    Error { message: String },
}
