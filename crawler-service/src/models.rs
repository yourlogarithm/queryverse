use chrono::{serde::ts_seconds, DateTime, Utc};
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/vector.rs"));

#[derive(Serialize)]
#[serde(tag = "status", content = "data")]
pub enum ApiResponse<T> {
    Ok(T),
    Err(String),
}

#[derive(Serialize, Deserialize)]
pub struct QdrantDocument {
    pub url: String,
    pub content: String,
    #[serde(with = "ts_seconds")]
    pub date: DateTime<Utc>,
    pub sha256: String,
}
