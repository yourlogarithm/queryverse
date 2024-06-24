use axum::extract::State;
use bson::doc;
use mongodm::{f, operator::GreaterThan, ToRepository};
use reqwest::StatusCode;
use tracing::{debug, error, info};

use crate::{
    core::{cooldown, process},
    database::{Document, DATABASE},
    robots::is_robots_allowed,
    state::AppState,
};

const EXPIRATION: i64 = 5;

#[axum::debug_handler]
#[tracing::instrument(skip(app_state))]
pub async fn crawl(State(app_state): State<AppState>, url: String) -> StatusCode {
    let url = match reqwest::Url::parse(&url) {
        Err(e) => {
            error!("Failed to parse URL - {e:#}");
            return StatusCode::BAD_REQUEST;
        }
        Ok(url) => url,
    };
    debug!("Crawl request");

    macro_rules! cooldown {
        ($seconds:expr) => {
            if let Some(domain) = url.domain() {
                debug!("Cooldown");
                if let Err(e) = cooldown(domain, $seconds, &app_state.redis_client).await {
                    error!("Failed to cooldown - {e:#}");
                }
            }
        };
    }

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
                    cooldown!(0);
                    return StatusCode::OK;
                }
                Err(e) => {
                    error!("Failed to check if already crawled - {e:#}");
                    cooldown!(0);
                    return StatusCode::INTERNAL_SERVER_ERROR;
                }
            }
            let process_result = process(url.clone(), &app_state).await;
            cooldown!(EXPIRATION);
            match process_result {
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
            cooldown!(0);
            StatusCode::OK
        }
        Err(e) => {
            error!("Failed to check robots.txt - {e:#}");
            cooldown!(0);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
