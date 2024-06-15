use bitcode::Encode;
use serde::Serialize;

include!(concat!(env!("OUT_DIR"), "/vector.rs"));

#[derive(Serialize)]
#[serde(tag = "status", content = "data")]
pub enum ApiResponse<T> {
    Ok(T),
    Err(String),
}

#[derive(Serialize, Encode)]
#[serde(rename_all = "camelCase")]
pub enum CrawlerResponse {
    Crawled(Vec<String>),
    NotAllowed,
}
