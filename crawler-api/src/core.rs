use std::collections::{HashMap, HashSet};

use anyhow::Context;
use bson::{doc, Uuid};
use mongodm::{
    f,
    mongo::options::{Hint, ReturnDocument},
    operator::{Set, SetOnInsert},
    ToRepository,
};
use qdrant_client::qdrant::{value::Kind, PointStruct, Value};
use scraper::{Html, Selector};

use crate::{
    database::{Document, UuidProjection, DATABASE},
    proto::{
        messaging_client::MessagingClient, EmbedRequest, EmbedResponse, Payload, PublishRequest,
    },
    state::AppState,
};

lazy_static::lazy_static! {
    static ref WHITESPACES: regex::Regex = regex::Regex::new(r"(\s)\s+").unwrap();
    static ref PARAGRAPH_SELECTOR: Selector = Selector::parse("p").unwrap();
    static ref HREF_SELECTOR: Selector = Selector::parse("a[href]").unwrap();
    static ref TITLE_SELECTOR: Selector = Selector::parse("title").unwrap();
}

#[tracing::instrument(skip(state), fields(url = %url.as_str()))]
pub async fn process(url: url::Url, state: &AppState) -> anyhow::Result<()> {
    let content = if let Some(content) = get_content(url.clone(), &state.reqwest_client).await? {
        content
    } else {
        tracing::info!("Skipping URL due to empty content");
        return Ok(());
    };
    tracing::info!(content_length = content.len(), "Retrieved content");

    let document = Html::parse_document(&content);
    let body = document
        .select(&PARAGRAPH_SELECTOR)
        .map(|e| e.text().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    let body = WHITESPACES.replace_all(&body, "$1").to_string();
    tracing::info!(body_length = body.len(), "Extracted and processed body");

    let hash = sha256::digest(&content);
    let mut uuid = Uuid::new();
    let t = chrono::Utc::now();
    let filter = doc! { f!(url in Document): url.as_str() };
    let update = doc! {
        SetOnInsert: {
            f!(first in Document): mongodm::bson::Bson::DateTime(t.into()),
            f!(uuid in Document): uuid,
        },
        Set: {
            f!(sha256 in Document): &hash,
            f!(last in Document): mongodm::bson::Bson::DateTime(t.into())
        }
    };
    if body.is_empty() {
        let options = mongodm::mongo::options::UpdateOptions::builder()
            .hint(Hint::Keys(doc! { f!(url in Document): 1 }))
            .upsert(true)
            .build();
        let result = state
            .mongo_client
            .database(DATABASE)
            .repository::<Document>()
            .update_one(filter, update)
            .with_options(options)
            .await
            .context("Failed to update or insert document")?;
        tracing::info!(matched_count = result.matched_count, modified_count = result.modified_count, "Upserted document with empty body");
    } else {
        let options = mongodm::mongo::options::FindOneAndUpdateOptions::builder()
            .hint(Hint::Keys(doc! { f!(url in Document): 1 }))
            .return_document(ReturnDocument::After)
            .projection(doc! { f!(uuid in Document): 1 })
            .upsert(true)
            .build();

        if let Some(document) = state
            .mongo_client
            .database(DATABASE)
            .repository::<UuidProjection>()
            .find_one_and_update(filter, update)
            .with_options(options)
            .await
            .context("Failed to update or insert document")?
        {
            tracing::info!(uuid = ?document.uuid, "Updated document");
            uuid = document.uuid
        } else {
            tracing::info!(uuid = ?uuid, "Inserted document");
        }

        let request = EmbedRequest {
            inputs: body,
            truncate: true,
            normalize: true,
            truncation_direction: 0,
            prompt_name: None,
        };
        match state
            .tei_client
            .clone()
            .embed(request)
            .await
            .map(|r| r.into_inner())
        {
            Ok(EmbedResponse { embeddings, .. }) => {
                let title = document
                    .select(&TITLE_SELECTOR)
                    .next()
                    .map(|element| element.inner_html());
                let mut payload = HashMap::new();

                macro_rules! value {
                    ($value:expr) => {
                        Value {
                            kind: Some(Kind::StringValue($value)),
                        }
                    };
                }

                if let Some(title) = title {
                    payload.insert("title", value!(title));
                }
                payload.insert("url", value!(url.to_string()));
                let point = PointStruct::new(uuid.to_string(), embeddings, payload.into());
                match state
                    .qdrant_client
                    .upsert_points("documents", None, vec![point], None)
                    .await
                {
                    Ok(info) => tracing::info!(operation_id = ?info.result.map(|r| r.operation_id), "Upserted embeddings"),
                    Err(e) => tracing::error!(error = %e, "Failed to upsert embeddings"),
                }
            }
            Err(e) => tracing::error!(error = %e, "Failed to embed content"),
        }
    }

    let links: HashSet<_> = document
        .select(&HREF_SELECTOR)
        .flat_map(|e| e.value().attr("href").map(|relative| url.join(relative)))
        .flatten()
        .map(|mut url| {
            url.set_fragment(None);
            url
        })
        .filter(|edge| edge != &url)
        .collect();
    tracing::info!(edge_count = links.len(), "Extracted links");

    tracing::info!("Publishing links");
    publish(links, &state.messaging_client).await
}

#[tracing::instrument(skip(client), fields(url = %url.as_str()))]
async fn get_content(url: url::Url, client: &reqwest::Client) -> anyhow::Result<Option<String>> {
    tracing::info!("Sending HEAD request");
    let response = client
        .head(url.clone())
        .send()
        .await
        .context("HEAD Request")?
        .error_for_status()
        .context("HEAD Response")?;
    if let Some(content_type) = response.headers().get("content-type") {
        if !content_type.to_str()?.contains("text/html") {
            tracing::info!(content_type = ?content_type, "Skipping non-HTML content");
            return Ok(None);
        }
    }
    tracing::info!("Sending GET request");
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

async fn publish(
    urls: HashSet<url::Url>,
    client: &MessagingClient<tonic::transport::Channel>,
) -> anyhow::Result<()> {
    let payloads: Vec<_> = urls
        .iter()
        .filter_map(|url| {
            if let Some(domain) = url.domain() {
                Some(Payload {
                    queue: domain.to_owned(),
                    message: url.to_string(),
                })
            } else {
                None
            }
        })
        .collect();
    if payloads.is_empty() {
        tracing::info!("No valid payloads to publish");
        return Ok(());
    }
    tracing::info!(payload_count = payloads.len(), "Publishing payloads");
    client
        .clone()
        .publish_urls(PublishRequest { payloads })
        .await
        .context("messaging publish")
        .map(|r| r.into_inner())
}
