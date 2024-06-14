use axum::extract::{Json, Path, State};
use prost::Message;
use reqwest::StatusCode;
use tracing::{debug, error, info};

use crate::{
    core::process,
    models::{ApiResponse, EdgesMessage},
    robots::is_robots_allowed,
    state::{AppState, QUEUE},
};

#[axum::debug_handler]
#[tracing::instrument(skip(app_state))]
pub async fn crawl(
    Path(url): Path<url::Url>,
    State(app_state): State<AppState>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    debug!("Crawl request");
    match is_robots_allowed(&url, &app_state).await {
        Ok(true) => {
            info!("Allowed to crawl");
            match process(url.clone(), &app_state).await {
                Ok(urls) => {
                    debug!("{} edges found", urls.len());
                    let message = EdgesMessage { urls };
                    if let Err(e) = app_state
                        .amqp_channel
                        .basic_publish(
                            "",
                            QUEUE,
                            Default::default(),
                            &message.encode_to_vec(),
                            Default::default(),
                        )
                        .await
                    {
                        error!("Failed to publish edges - {e}");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiResponse::Err(e.to_string())),
                        );
                    }
                    info!("Crawled");
                    (StatusCode::OK, Json(ApiResponse::Ok(())))
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
            (StatusCode::OK, Json(ApiResponse::Ok(())))
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
