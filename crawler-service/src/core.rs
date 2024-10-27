use crate::{
    log::{Content, Log},
    proto::{EmbedRequest, EmbedResponse},
    state::AppState,
    traverse::HtmlTraverse,
};
use anyhow::Context;
use ego_tree::iter::Edge;
use lapin::{options::QueueDeclareOptions, types::FieldTable, BasicProperties};
use mongodb::bson::{doc, Uuid};
use mongodm::{
    f,
    mongo::options::ReturnDocument,
    operator::{Set, SetOnInsert},
    ToRepository,
};
use qdrant_client::qdrant::{value::Kind, PointStruct, UpsertPointsBuilder, Value};
use scraper::{Html, Node, Selector};
use std::collections::{HashMap, HashSet};
use url::Url;
use utils::database::{Page, UuidProjection, COLLNAME, DATABASE};

lazy_static::lazy_static! {
    static ref WHITESPACES: regex::Regex = regex::Regex::new(r"(\s)\s+").unwrap();
    static ref HREF_SELECTOR: Selector = Selector::parse("a[href]").unwrap();
    static ref TITLE_SELECTOR: Selector = Selector::parse("title").unwrap();
}

#[tracing::instrument(skip(log, state), fields(url = %url))]
pub async fn process(url: &Url, log: &mut Log<'_>, state: &AppState) -> anyhow::Result<()> {
    let content = if let Some(content) = get_content(url, state).await? {
        content
    } else {
        tracing::debug!(url = %url, "Skipping URL due to empty content");
        return Ok(());
    };
    tracing::debug!(content_length = content.len(), url = %url, "Retrieved content");

    let document = Html::parse_document(&content);

    let mut body = String::new();
    let traverse = HtmlTraverse::new(*document.root_element());
    for edge in traverse {
        if let Edge::Open(node) = edge {
            if let Node::Text(ref text) = node.value() {
                body.push_str(text);
                body.push_str(" ");
            }
        }
    }

    let body = WHITESPACES.replace_all(&body, " ").to_string();
    tracing::debug!(body_length = body.len(), url = %url, "Extracted and processed body");

    let links: HashSet<_> = document
        .select(&HREF_SELECTOR)
        .flat_map(|e| e.value().attr("href").map(|relative| url.join(relative)))
        .flatten()
        .map(|mut url| {
            url.set_fragment(None);
            url
        })
        .filter(|edge| edge != url)
        .collect();
    tracing::debug!(links = links.len(), url = %url, "Extracted links");

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
                        tracing::debug!(operation_id = info.result.map(|r| r.operation_id), url = %url, "Upserted embeddings")
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
    let tasks = links.iter().filter_map(|link| {
        if let Some(domain) = link.domain() {
            Some(publish(domain, link, &state.amqp_channel))
        } else {
            None
        }
    });
    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<(), _>>()
        .context("Link publishing")
}

#[tracing::instrument(skip(state), fields(url = %url.as_str()))]
async fn get_content(url: &url::Url, state: &AppState) -> anyhow::Result<Option<String>> {
    tracing::debug!(url = %url, "Sending GET request");
    let response = state
        .reqwest_client
        .get(url.clone())
        .send()
        .await
        .context("GET send")?
        .error_for_status()
        .context("GET response")?;
    if let Some(value) = response.headers().get(reqwest::header::CONTENT_TYPE) {
        let str_mime = value
            .to_str()
            .with_context(|| format!("{} to_str", reqwest::header::CONTENT_TYPE))?;
        let mime: mime::Mime = str_mime.parse().with_context(|| {
            format!(
                "{} parse from {str_mime} failed",
                reqwest::header::CONTENT_TYPE
            )
        })?;
        if mime != mime::TEXT_HTML_UTF_8 {
            tracing::debug!(mime = ?mime, "Skipping non-HTML content");
            return Ok(None);
        }
    } else {
        tracing::debug!(url = %url, "{} missing", reqwest::header::CONTENT_TYPE)
    }
    response.text().await.map(Some).context("content")
}

async fn publish(domain: &str, url: &url::Url, channel: &lapin::Channel) -> anyhow::Result<()> {
    channel
        .queue_declare(
            domain,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .context("queue declare")?;
    let props = BasicProperties::default().with_delivery_mode(2);
    channel
        .basic_publish(
            "",
            domain,
            Default::default(),
            url.as_str().as_bytes(),
            props,
        )
        .await
        .context("basic publish")?
        .await
        .context("publish confirm")?;
    Ok(())
}
