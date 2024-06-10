use std::collections::HashSet;

use anyhow::Context;
use axum::{extract::Path, http::StatusCode, Json};
use prost::Message;
use qdrant_client::qdrant::PointStruct;
use scraper::{Html, Selector};
use tracing::{debug, error, info};

use crate::{
    models::{ApiResponse, QdrantDocument, VectorResponse},
    robots::is_robots_allowed,
    state,
};

lazy_static! {
    static ref WHITESPACES: regex::Regex = regex::Regex::new(r"(\s)\s+").unwrap();
    static ref PARAGRAPH_SELECTOR: Selector = Selector::parse("p").unwrap();
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

#[tracing::instrument(skip(url, state), fields(url = %url.as_str()))]
async fn inner_crawl(url: url::Url, state: &state::AppState) -> anyhow::Result<HashSet<url::Url>> {
    let content = if let Some(content) = get_content(url.clone(), &state.reqwest_client).await? {
        content
    } else {
        return Ok(HashSet::new());
    };
    debug!("Content length: {}", content.len());
    let document = Html::parse_document(&content);
    let body = document
        .select(&PARAGRAPH_SELECTOR)
        .map(|e| e.text().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    let body = WHITESPACES.replace_all(&body, "$1").to_string();
    debug!("Body length: {}", body.len());
    let response = state
        .reqwest_client
        .get(concat!(env!("LANGUAGE_PROCESSOR_API"), "/embedding"))
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
        let embedding = VectorResponse::decode(body)
            .context("Embeddings deserialization")?
            .value;
        let hash = sha256::digest(&content);
        let entry = QdrantDocument {
            url: url.as_str().to_string(),
            content,
            date: chrono::Utc::now(),
            sha256: hash,
        };
        let point = PointStruct::new(
            0,
            embedding,
            serde_json::to_value(&entry).unwrap().try_into().unwrap(),
        );
        match state
            .qdrant_client
            .upsert_points("documents", None, vec![point], None)
            .await
        {
            Ok(info) => debug!("Upserted embeddings - {info:?}"),
            Err(e) => error!("Failed to upsert embeddings - {e}"),
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
#[tracing::instrument(skip(url, state), fields(url = %url.as_str()))]
pub async fn crawl(
    Path(url): Path<url::Url>,
    axum::extract::State(state): axum::extract::State<state::AppState>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    match is_robots_allowed(&url, &state).await {
        Ok(true) => {
            tokio::spawn(async move {
                info!("Crawling");
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
