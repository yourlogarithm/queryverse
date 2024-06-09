use std::collections::HashSet;

use anyhow::Context;
use axum::{extract::Path, http::StatusCode, Json};
use prost::Message;
use scraper::{Html, Selector};
use tracing::{debug, error, info};

use crate::{
    models::{ApiResponse, VectorResponse},
    robots::is_robots_allowed,
    state,
};

lazy_static! {
    static ref BODY_SELECTOR: Selector = Selector::parse("body").unwrap();
    static ref HREF_SELECTOR: Selector = Selector::parse("a[href]").unwrap();
}

#[tracing::instrument(skip(client))]
async fn get_content(url: url::Url, client: &reqwest::Client) -> anyhow::Result<Option<String>> {
    let response = client
        .head(url.clone())
        .send()
        .await
        .context("HEAD Request")?
        .error_for_status()
        .context("HEAD Response")?;
    if let Some(content_type) = response.headers().get("content-type") {
        if !content_type.to_str()?.contains("text/html") {
            debug!("Skipping not HTML Content-Type: {content_type:?}");
            return Ok(None);
        }
    }
    let content = client
        .get(url)
        .send()
        .await
        .context("GET Request")?
        .text()
        .await
        .context("GET Response")?;
    Ok(Some(content))
}

#[tracing::instrument(skip(state))]
async fn inner_crawl(url: url::Url, state: &state::AppState) -> anyhow::Result<HashSet<url::Url>> {
    let content = if let Some(content) = get_content(url.clone(), &state.reqwest_client).await? {
        content
    } else {
        return Ok(HashSet::new());
    };
    let hash = sha256::digest(&content);
    let document = Html::parse_document(&content);
    let body_maybe = document
        .select(&BODY_SELECTOR)
        .next()
        .map(|e| e.text().collect::<Vec<_>>().join(" "));
    if let Some(body) = body_maybe {
        let response = state
            .reqwest_client
            .post(concat!(env!("LANGUAGE_PROCESSOR_API"), "/embed"))
            .body(body)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .context("Embeddings request")?;
        let status = response.status();
        if !status.is_success() {
            if let Ok(text) = response.text().await {
                error!("Failed to get embeddings - {status} {text}");
            } else {
                error!("Failed to get embeddings - {status}");
            }
        } else {
            let body = response.bytes().await.context("Embeddings response")?;
            let embeddings = VectorResponse::decode(body)
                .context("Embeddings deserialization")?
                .value;
        }
    }
    let links = document
        .select(&HREF_SELECTOR)
        .flat_map(|e| e.value().attr("href").map(|relative| url.join(relative)))
        .flatten()
        .collect();
    Ok(links)
}

#[axum::debug_handler]
#[tracing::instrument(skip(state))]
pub async fn crawl(
    Path(url): Path<url::Url>,
    axum::extract::State(state): axum::extract::State<state::AppState>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    match is_robots_allowed(&url, &state).await {
        Ok(true) => {
            tokio::spawn(async move {
                match inner_crawl(url.clone(), &state).await {
                    Ok(links) => {
                        info!("{} links found", links.len());
                        let futures: Vec<_> = links
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
                            })
                            .collect();
                        for result in futures::future::join_all(futures).await {
                            if let Err(e) = result {
                                error!("Failed to publish url - {e}");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to crawl - {e:#}");
                    }
                }
            });
            (StatusCode::ACCEPTED, Json(ApiResponse::Ok(())))
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
