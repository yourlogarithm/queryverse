mod models;
mod state;

use std::net::SocketAddr;

use axum::{extract::State, routing::get, Json, Router};
use models::{error::ApiError, SearchRequest, SearchResponse};
use proto::{EmbedRequest, EmbedResponse};
use qdrant_client::qdrant::{SearchPointsBuilder, SearchResponse as QdrantSearchResponse};
use state::{AppConfig, AppState};
use utils::database::COLLNAME;

mod proto {
    tonic::include_proto!("tei.v1");
}

#[axum::debug_handler]
async fn fallback() -> ApiError {
    ApiError {
        message: "Endpoint not Found".to_string(),
        error: models::error::ErrorType::NotFound,
    }
}

#[axum::debug_handler]
#[tracing::instrument(skip(state, request), fields(query = request.query.len(), limit = request.limit, offset = request.offset))]
async fn search(
    State(state): State<AppState>,
    Json(request): Json<SearchRequest>,
) -> Result<SearchResponse, ApiError> {
    tracing::debug!("Search request");
    let EmbedResponse { embeddings, .. } = state
        .tei_client
        .clone()
        .embed(EmbedRequest {
            inputs: request.query,
            truncate: true,
            truncation_direction: 0,
            prompt_name: None,
            normalize: true,
        })
        .await
        .map_err(|e| {
            tracing::error!("Failed to embed query: {e:#}");
            return ApiError {
                message: "Failed to process query".to_string(),
                error: models::error::ErrorType::InternalServerError,
            };
        })?
        .into_inner();
    let search_points =
        SearchPointsBuilder::new(COLLNAME, embeddings, request.limit.unwrap_or(10).min(50))
            .offset(request.offset.unwrap_or(0))
            .with_payload(true);
    let QdrantSearchResponse { result, .. } = state
        .qdrant_client
        .search_points(search_points)
        .await
        .map_err(|e| {
            tracing::error!("Failed to search points: {e:#}");
            return ApiError {
                message: "Failed to find results".to_string(),
                error: models::error::ErrorType::InternalServerError,
            };
        })?;

    let matches: Vec<_> = result
        .into_iter()
        .map(|s| s.payload.try_into())
        .filter_map(|result| match result {
            Ok(result) => Some(result),
            Err(e) => {
                tracing::error!("Failed to parse search result: {e:#}");
                None
            }
        })
        .collect();

    Ok(SearchResponse { matches })
}

async fn serve() {
    let app_config = AppConfig::new();
    let state = state::AppState::new(app_config).await;

    let socket_address: SocketAddr = "0.0.0.0:8000".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(socket_address).await.unwrap();

    let app = Router::new()
        .nest(
            "/api",
            Router::new().nest(
                "/v1",
                Router::new()
                    .route("/search", get(search))
                    .with_state(state),
            ),
        )
        .fallback(fallback);

    tracing::debug!("Listening on {socket_address}");

    axum::serve(listener, app.into_make_service())
        .await
        .unwrap()
}

#[tokio::main]
async fn main() {
    utils::start(env!("CARGO_PKG_NAME"), Box::pin(serve())).await;
}
