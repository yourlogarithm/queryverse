use std::collections::HashSet;

use anyhow::Context;
use bson::{doc, Uuid};
use lapin::{options::QueueDeclareOptions, types::FieldTable};
use mongodm::{
    f,
    mongo::options::{Hint, ReturnDocument},
    operator::{Set, SetOnInsert},
    ToRepository,
};
use qdrant_client::qdrant::PointStruct;
use scraper::{Html, Selector};
use serde_json::json;
use tracing::{debug, error};

use crate::{
    database::{Document, UuidProjection, DATABASE},
    state::AppState,
};

use utils::redis::Key;

lazy_static! {
    static ref WHITESPACES: regex::Regex = regex::Regex::new(r"(\s)\s+").unwrap();
    static ref PARAGRAPH_SELECTOR: Selector = Selector::parse("p").unwrap();
    static ref HREF_SELECTOR: Selector = Selector::parse("a[href]").unwrap();
    static ref TITLE_SELECTOR: Selector = Selector::parse("title").unwrap();
}

#[tracing::instrument(skip(app_state), fields(url = %url.as_str()))]
pub async fn process(url: url::Url, app_state: &AppState) -> anyhow::Result<()> {
    let content = if let Some(content) = get_content(url.clone(), &app_state.reqwest_client).await?
    {
        content
    } else {
        return Ok(());
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
    let hash = sha256::digest(&content);
    let uuid = Uuid::new();
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
        let result = app_state
            .mongo_client
            .database(DATABASE)
            .repository::<Document>()
            .update_one(filter, update, options)
            .await
            .context("Failed to update or insert document")?;
        debug!("Upserted document - {result:?}");
    } else {
        let options = mongodm::mongo::options::FindOneAndUpdateOptions::builder()
            .hint(Hint::Keys(doc! { f!(url in Document): 1 }))
            .return_document(ReturnDocument::After)
            .projection(doc! { f!(uuid in Document): 1 })
            .upsert(true)
            .build();

        let uuid = match app_state
            .mongo_client
            .database(DATABASE)
            .repository::<UuidProjection>()
            .find_one_and_update(filter, update, options)
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
        
        let request = json!({"inputs": body, "truncate": true});
        let response = app_state
            .reqwest_client
            .post(concat!(env!("NLP_API"), "/embed"))
            .json(&request)
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
            let embeddings: Vec<f32> = response
                .json::<Vec<Vec<f32>>>()
                .await
                .context("Embeddings response")?
                .into_iter()
                .flatten()
                .collect();
            debug!("Embeddings length: {}", embeddings.len());

            let title = document
                .select(&TITLE_SELECTOR)
                .next()
                .map(|element| element.inner_html());
            let payload = if let Some(title) = title {
                serde_json::json!({"url": url, "title": title})
            } else {
                serde_json::json!({"url": url})
            };
            let point = PointStruct::new(uuid.to_string(), embeddings, payload.try_into().unwrap());
            match app_state
                .qdrant_client
                .upsert_points("documents", None, vec![point], None)
                .await
            {
                Ok(info) => debug!("Upserted embeddings - {info:?}"),
                Err(e) => error!("Failed to upsert embeddings - {e}"),
            }
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
    debug!("Edges: {}", links.len());

    debug!("Publishing links");
    let tasks = links.iter().filter_map(|link| {
        if let Some(domain) = link.domain() {
            Some(publish(domain, link, &app_state.amqp_channel))
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

#[tracing::instrument(skip(client), fields(url = %url.as_str()))]
async fn get_content(url: url::Url, client: &reqwest::Client) -> anyhow::Result<Option<String>> {
    debug!("Sending HEAD request");
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
    debug!("Sending GET request");
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
    channel
        .basic_publish(
            "",
            domain,
            Default::default(),
            url.as_str().as_bytes(),
            Default::default(),
        )
        .await
        .context("basic publish")?
        .await
        .context("publish confirm")?;
    Ok(())
}

pub async fn cooldown(domain: &str, seconds: i64, client: &redis::Client) -> anyhow::Result<()> {
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("redis connection")
        .unwrap();
    let key = Key::Cooldown(domain);
    redis::pipe()
        .atomic()
        .set(&key, 1)
        .expire(&key, seconds)
        .query_async(&mut conn)
        .await
        .context("Redis SET & EXPIRE")
}
