use std::collections::HashSet;

use anyhow::Result;
use axum::{extract::Path, http::StatusCode, response::IntoResponse, Json};
use scraper::{Html, Selector};
use tracing::{error, info, warn};

use crate::{robots::is_robots_allowed, state};

lazy_static! {
    static ref SELECTOR: Selector = Selector::parse("a[href]").unwrap();
}

async fn inner_crawl(url: url::Url, state: state::State) -> Result<()> {
    let response = state
        .reqwest_client
        .head(url.clone())
        .send()
        .await?
        .error_for_status()?;
    if let Some(content_type) = response.headers().get("content-type") {
        if !content_type.to_str()?.contains("text/html") {
            info!("Skipping {url} as it is not HTML");
            return Ok(());
        }
    }
    let document = Html::parse_document(
        &state
            .reqwest_client
            .get(url.clone())
            .send()
            .await?
            .text()
            .await?,
    );
    let urls = document
        .select(&SELECTOR)
        .flat_map(|e| e.value().attr("href").map(|relative| url.join(relative)))
        .flatten()
        .filter(|u| u != &url)
        .collect::<HashSet<_>>();
    let futures: Vec<_> = urls
        .iter()
        .map(|url| async {
            state
                .amqp_channel
                .basic_publish(
                    "",
                    "crawled_urls",
                    Default::default(),
                    url.as_str().as_bytes(),
                    Default::default(),
                )
                .await?
                .await
        }).collect();
    for result in futures::future::join_all(futures).await {
        if let Err(e) = result {
            error!("Failed to publish url - {e}");
        }
    }
    Ok(())
}

#[axum::debug_handler]
pub async fn crawl(
    Path(url): Path<url::Url>,
    axum::extract::State(state): axum::extract::State<state::State>,
) -> impl IntoResponse {
    match is_robots_allowed(&url, &state).await {
        Ok(true) => {
            tokio::spawn(async move {
                match inner_crawl(url.clone(), state).await {
                    Ok(_) => info!("Crawled {url}"),
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
