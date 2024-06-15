use axum::extract::{Path, State};
use bson::doc;
use mongodm::{f, operator::GreaterThan, ToRepository};
use reqwest::StatusCode;
use tracing::{debug, error, info};

use crate::{
    core::process,
    database::{Document, DATABASE},
    robots::is_robots_allowed,
    state::AppState,
};

#[axum::debug_handler]
#[tracing::instrument(skip(app_state), fields(url = %url.as_str()))]
pub async fn crawl(Path(url): Path<url::Url>, State(app_state): State<AppState>) -> StatusCode {
    debug!("Crawl request");
    match is_robots_allowed(&url, &app_state).await {
        Ok(true) => {
            info!("Allowed to crawl");
            let repo = app_state
                .mongo_client
                .database(DATABASE)
                .repository::<Document>();
            let count_options = mongodm::mongo::options::CountOptions::builder()
                .hint(mongodm::mongo::options::Hint::Keys(
                    doc! {f!(url in Document): 1},
                ))
                .limit(1)
                .build();
            let past = chrono::Utc::now() - chrono::Duration::hours(1);
            let filter = doc! {
                f!(url in Document): url.as_str(),
                f!(last in Document): { GreaterThan: past }
            };
            match repo.count_documents(filter, count_options).await {
                Ok(0) => (),
                Ok(_) => {
                    info!("Already crawled");
                    return StatusCode::OK;
                }
                Err(e) => {
                    error!("Failed to check if already crawled - {e:#}");
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            match process(url.clone(), &app_state).await {
                Ok(_) => {
                    info!("Crawled");
                    StatusCode::ACCEPTED
                }
                Err(e) => {
                    error!("Failed to process - {e:#}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Ok(false) => {
            info!("Not allowed to crawl");
            StatusCode::OK
        }
        Err(e) => {
            error!("Failed to check robots.txt - {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
