use anyhow::{Context, Result};
use axum::{extract::Path, http::StatusCode, Json};
use tracing::{debug, error, warn};

use crate::{robots::is_robots_allowed, state};

async fn inner_crawl(url: &url::Url, state: state::State) -> Result<()> {
    todo!()
}

#[axum::debug_handler]
pub async fn crawl(
    Path(url): Path<url::Url>,
    axum::extract::State(state): axum::extract::State<state::State>,
) -> (StatusCode, Json<Option<String>>) {
    match is_robots_allowed(&url, &state).await {
        Ok(true) => {
            tokio::spawn(async move {
                match inner_crawl(&url, state).await {
                    Ok(_) => debug!("Crawled {url}"),
                    Err(e) => error!("Failed to crawl {url} - {e}: {:?}", e.source()),
                }
            });
            (StatusCode::ACCEPTED, Json(None))
        }
        Ok(false) => {
            warn!("Not allowed to crawl {url}");
            (StatusCode::OK, Json(None))
        }
        Err(e) => {
            error!(
                "Failed to check robots.txt for {url} - {e}: {:?}",
                e.source()
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Json(Some(e.to_string())))
        }
    }
}
