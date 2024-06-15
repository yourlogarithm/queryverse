use axum::extract::{Json, Path, State};
use reqwest::StatusCode;
use tracing::{debug, error, info};

use crate::{
    core::process,
    models::{ApiResponse, CrawlerResponse},
    robots::is_robots_allowed,
    state::AppState,
};

#[axum::debug_handler]
#[tracing::instrument(skip(app_state), fields(url = %url.as_str()))]
pub async fn crawl(
    Path(url): Path<url::Url>,
    State(app_state): State<AppState>,
) -> (StatusCode, Json<ApiResponse<Vec<u8>>>) {
    debug!("Crawl request");
    match is_robots_allowed(&url, &app_state).await {
        Ok(true) => {
            info!("Allowed to crawl");
            match process(url.clone(), &app_state).await {
                Ok(urls) => {
                    debug!("{} edges found", urls.len());
                    let response = CrawlerResponse::Crawled(urls);
                    let encoded = bitcode::encode(&response);
                    info!("Crawled");
                    (StatusCode::OK, Json(ApiResponse::Ok(encoded)))
                }
                Err(e) => {
                    error!("Failed to process - {e:#}");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiResponse::Err(e.to_string())),
                    );
                }
            }
        }
        Ok(false) => {
            info!("Not allowed to crawl");
            let response = CrawlerResponse::NotAllowed;
            let encoded = bitcode::encode(&response);
            (StatusCode::OK, Json(ApiResponse::Ok(encoded)))
        }
        Err(e) => {
            let msg = format!("Failed to check robots.txt - {e:#}");
            error!(msg);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::Err(msg)),
            )
        }
    }
}
