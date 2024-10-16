use std::collections::HashMap;

use axum::{response::IntoResponse, Json};
use qdrant_client::qdrant::{value::Kind, Value};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct SearchRequest {
    pub query: String,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Serialize)]
pub struct MatchResult {
    pub title: String,
    pub url: String,
}

impl TryFrom<HashMap<String, Value>> for MatchResult {
    type Error = anyhow::Error;

    fn try_from(mut value: HashMap<String, Value>) -> Result<Self, Self::Error> {
        macro_rules! get {
            ($key:expr) => {
                match value.remove($key) {
                    Some(Value {
                        kind: Some(Kind::StringValue(v)),
                    }) => v,
                    Some(other) => anyhow::bail!("Unexpected `{}` value: {:?}", $key, other),
                    None => anyhow::bail!("Missing `{}` value", $key),
                }
            };
        }
        let title = get!("title");
        let url = get!("url");

        Ok(MatchResult { title, url })
    }
}

#[derive(Serialize)]
pub struct SearchResponse {
    pub matches: Vec<MatchResult>,
}

impl IntoResponse for SearchResponse {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}

pub mod error {
    use axum::{http::StatusCode, response::IntoResponse};
    use serde::Serialize;

    #[derive(Serialize)]
    pub struct ApiError {
        pub message: String,
        pub error: ErrorType,
    }

    #[derive(Serialize)]
    pub enum ErrorType {
        NotFound,
        InternalServerError,
    }

    impl ErrorType {
        pub fn status_code(&self) -> StatusCode {
            match self {
                ErrorType::NotFound => StatusCode::NOT_FOUND,
                ErrorType::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
            }
        }
    }

    impl IntoResponse for ApiError {
        fn into_response(self) -> axum::response::Response {
            (self.error.status_code(), self).into_response()
        }
    }
}
