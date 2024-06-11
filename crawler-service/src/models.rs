use serde::Serialize;

include!(concat!(env!("OUT_DIR"), "/vector.rs"));

#[derive(Serialize)]
#[serde(tag = "status", content = "data")]
pub enum ApiResponse<T> {
    Ok(T),
    Err(String),
}
