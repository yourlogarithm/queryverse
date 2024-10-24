use std::collections::{HashMap, HashSet};

use crate::{
    log::{Content, Log},
    proto::{
        messaging_client::MessagingClient, EmbedRequest, EmbedResponse, Payload, PublishRequest,
    },
    state::AppState,
};
use anyhow::Context;
use mongodb::bson::{doc, Uuid};
use mongodm::{
    f,
    mongo::options::{Hint, ReturnDocument},
    operator::{Set, SetOnInsert},
    ToRepository,
};
use qdrant_client::qdrant::{value::Kind, PointStruct, UpsertPointsBuilder, Value};
use scraper::{Html, Selector};
use url::Url;
use utils::database::{Page, UuidProjection, COLLNAME, DATABASE};

lazy_static::lazy_static! {
    static ref WHITESPACES: regex::Regex = regex::Regex::new(r"(\s)\s+").unwrap();
    static ref PARAGRAPH_SELECTOR: Selector = Selector::parse("p").unwrap();
    static ref HREF_SELECTOR: Selector = Selector::parse("a[href]").unwrap();
    static ref TITLE_SELECTOR: Selector = Selector::parse("title").unwrap();
}

#[tracing::instrument(skip(log, state), fields(url = %url))]
pub async fn process(url: &Url, log: &mut Log<'_>, state: &AppState) -> anyhow::Result<()> {
    let content = if let Some(content) = get_content(url, &state.reqwest_client).await? {
        content
    } else {
        tracing::debug!(url = %url, "Skipping URL due to empty content");
        return Ok(());
    };
    tracing::debug!(content_length = content.len(), url = %url, "Retrieved content");

    let document = Html::parse_document(&content);
    let body = document
        .select(&PARAGRAPH_SELECTOR)
        .map(|e| e.text().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    let body = WHITESPACES.replace_all(&body, "$1").to_string();
    tracing::debug!(body_length = body.len(), url = %url, "Extracted and processed body");

    let links: HashSet<_> = document
        .select(&HREF_SELECTOR)
        .flat_map(|e| {
            e.value()
                .attr("href")
                .map(|relative| url.join(relative))
        })
        .flatten()
        .map(|mut url| {
            url.set_fragment(None);
            url
        })
        .filter(|edge| edge != url)
        .collect();
    tracing::debug!(edge_count = links.len(), url = %url, "Extracted links");

    log.data = Some(Content {
        content_length: content.len(),
        body_length: body.len(),
        edges: links.len(),
    });

    let hash = sha256::digest(&content);
    let mut uuid = Uuid::new();
    let t = chrono::Utc::now();
    let filter = doc! { f!(url in Page): url.as_str() };
    let update = doc! {
        SetOnInsert: {
            f!(first in Page): mongodm::bson::Bson::DateTime(t.into()),
            f!(uuid in Page): uuid,
        },
        Set: {
            f!(sha256 in Page): &hash,
            f!(last in Page): mongodm::bson::Bson::DateTime(t.into())
        }
    };
    if body.is_empty() {
        let options = mongodm::mongo::options::UpdateOptions::builder()
            .hint(Hint::Keys(doc! { f!(url in Page): 1 }))
            .upsert(true)
            .build();
        let result = state
            .mongo_client
            .database(DATABASE)
            .repository::<Page>()
            .update_one(filter, update)
            .with_options(options)
            .await
            .context("Failed to update or insert document")?;
        tracing::debug!(
            matched_count = result.matched_count,
            modified_count = result.modified_count,
            url = %url,
            "Upserted document with empty body"
        );
    } else {
        let options = mongodm::mongo::options::FindOneAndUpdateOptions::builder()
            .hint(Hint::Keys(doc! { f!(url in Page): 1 }))
            .return_document(ReturnDocument::After)
            .projection(doc! { f!(uuid in Page): 1 })
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
            tracing::debug!(uuid = ?document.uuid, url = %url, "Updated document");
            uuid = document.uuid
        } else {
            tracing::debug!(uuid = ?uuid, url = %url, "Inserted document");
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
                let point = PointStruct::new(uuid.to_string(), embeddings, payload);
                let request = UpsertPointsBuilder::new(COLLNAME, vec![point]);
                match state.qdrant_client.upsert_points(request).await {
                    Ok(info) => {
                        tracing::debug!(operation_id = ?info.result.map(|r| r.operation_id), url = %url, "Upserted embeddings")
                    }
                    Err(e) => {
                        tracing::error!(error = %e, url = %url, "Failed to upsert embeddings")
                    }
                }
            }
            Err(e) => tracing::error!(error = %e, url = %url, "Failed to embed content"),
        }
    }

    tracing::debug!(url = %url, "Publishing links");
    publish(links, &state.messaging_client).await
}

#[tracing::instrument(skip(client), fields(url = %url.as_str()))]
async fn get_content(url: &url::Url, client: &reqwest::Client) -> anyhow::Result<Option<String>> {
    tracing::debug!(url = %url, "Sending HEAD request");
    let response = client
        .head(url.clone())
        .send()
        .await
        .context("HEAD Request")?
        .error_for_status()
        .context("HEAD Response")?;
    if let Some(content_type) = response.headers().get("content-type") {
        if !content_type.to_str()?.contains("text/html") {
            tracing::debug!(content_type = ?content_type, "Skipping non-HTML content");
            return Ok(None);
        }
    }
    tracing::debug!(url = %url, "Sending GET request");
    let content = client
        .get(url.clone())
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
        tracing::debug!("No valid payloads to publish");
        return Ok(());
    }
    tracing::debug!(payload_count = payloads.len(), "Publishing payloads");
    client
        .clone()
        .publish_urls(PublishRequest { payloads })
        .await
        .context("messaging publish")
        .map(|r| r.into_inner())
}
