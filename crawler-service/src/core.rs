use std::collections::HashSet;

use anyhow::Context;
use bson::{doc, Uuid};
use mongodm::{
    f,
    mongo::options::{Hint, ReturnDocument},
    operator::{Set, SetOnInsert},
    ToRepository,
};
use prost::Message;
use qdrant_client::qdrant::PointStruct;
use scraper::{Html, Selector};
use tracing::{debug, error};

use crate::{
    database::{Document, UuidProjection},
    models::VectorResponse,
    state::AppState,
};

lazy_static! {
    static ref WHITESPACES: regex::Regex = regex::Regex::new(r"(\s)\s+").unwrap();
    static ref PARAGRAPH_SELECTOR: Selector = Selector::parse("p").unwrap();
    static ref HREF_SELECTOR: Selector = Selector::parse("a[href]").unwrap();
    static ref TITLE_SELECTOR: Selector = Selector::parse("title").unwrap();
}

#[tracing::instrument(skip(state), fields(url = %url.as_str()))]
pub async fn process(url: url::Url, state: &AppState) -> anyhow::Result<Vec<String>> {
    let content = if let Some(content) = get_content(url.clone(), &state.reqwest_client).await? {
        content
    } else {
        return Ok(Vec::new());
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
    // TODO: Remove page refs
    let links: HashSet<_> = document
        .select(&HREF_SELECTOR)
        .flat_map(|e| e.value().attr("href").map(|relative| url.join(relative)))
        .flatten()
        .filter(|edge| edge != &url)
        .collect();
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
        let options: mongodm::prelude::MongoFindOneAndUpdateOptions =
            mongodm::mongo::options::FindOneAndUpdateOptions::builder()
                .hint(Hint::Keys(doc! { f!(url in Document): 1 }))
                .return_document(ReturnDocument::After)
                .projection(doc! { f!(uuid in Document): 1 })
                .upsert(true)
                .build();
        let t = chrono::Utc::now();
        let uuid = Uuid::new();
        let uuid = match state
            .mongo_client
            .database("crawler")
            .repository::<UuidProjection>()
            .find_one_and_update(
                doc! { f!(url in Document): url.as_str() },
                doc! {
                    SetOnInsert: {
                        f!(first in Document): mongodm::bson::Bson::DateTime(t.into()),
                        f!(uuid in Document): uuid,
                    },
                    Set: {
                        f!(sha256 in Document): &hash,
                        f!(last in Document): mongodm::bson::Bson::DateTime(t.into())
                    }
                },
                options,
            )
            .await
            .context("Failed to update or insert document")?
        {
            Some(document) => {
                debug!("Updated document");
                document.uuid
            }
            None => {
                debug!("Inserted document");
                uuid
            }
        };
        let title = document
            .select(&TITLE_SELECTOR)
            .next()
            .and_then(|element| Some(element.inner_html()));
        let payload = if let Some(title) = title {
            serde_json::json!({"url": url, "title": title})
        } else {
            serde_json::json!({"url": url})
        };
        let point = PointStruct::new(
            uuid.to_string(),
            embedding,
            payload.try_into().unwrap(),
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
    Ok(links.into_iter().map(|url| url.into()).collect())
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
