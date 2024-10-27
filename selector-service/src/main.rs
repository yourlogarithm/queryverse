use std::sync::Arc;

use anyhow::Context;
use config::{Config, Environment};
use proto::CrawlRequest;
use rabbitmq_management_client::api::queue::QueueApi;
use rand::seq::IteratorRandom;
use redis::AsyncCommands;
use serde::Deserialize;
use state::{AppConfig, AppState};
use tokio::sync::Semaphore;
use utils::redis::Key;

mod state;
mod proto {
    tonic::include_proto!("crawler");
}

#[derive(Debug, Deserialize)]
pub struct QueueInfo {
    pub name: String,
    #[serde(default)]
    pub messages: u32,
}

async fn step(state: &AppState) -> anyhow::Result<()> {
    let queues = state
        .management_client
        .list_queues(None)
        .await
        .context("list_queues")?;
    tracing::debug!(queues = queues.len(), "Listed queues");
    let (empty, full): (Vec<_>, Vec<_>) = queues.into_iter().partition(|q| q.messages == 0);
    let mc = state.management_client.clone();
    tokio::spawn(async move {
        let tasks = empty
            .into_iter()
            .map(|q| mc.delete_queue(String::from("/"), q.name));
        futures::future::join_all(tasks).await;
    });
    if full.is_empty() {
        tracing::debug!("No full queues");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        return Ok(());
    }
    let reset_keys: Vec<_> = full
        .iter()
        .map(|queue| Key::Cooldown(&queue.name))
        .collect();
    let mut conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .context("get_multiplexed_async_connection")?;
    let results: Vec<Option<u8>> = conn.mget(&reset_keys).await.context("mget")?;
    let maybe_domain = {
        let mut rng = rand::thread_rng();
        full.into_iter()
            .zip(results.into_iter())
            .filter_map(
                |(queue, result)| {
                    if result.is_none() {
                        Some(queue)
                    } else {
                        None
                    }
                },
            )
            .choose(&mut rng)
    };
    if let Some(domain) = maybe_domain {
        tracing::debug!("Selected domain - \"{}\"", domain.name);
        if let Some(msg) = state
            .amqp_channel
            .basic_get(&domain.name, Default::default())
            .await
            .context("basic get")?
        {
            let url = match String::from_utf8(msg.delivery.data.clone()) {
                Ok(url) => url,
                Err(e) => {
                    tracing::error!("Bad payload - {e:#}");
                    msg.delivery
                        .reject(Default::default())
                        .await
                        .context("basic reject")?;
                    return Ok(());
                }
            };
            tracing::info!(domain = domain.name, url = url, "Got url");
            if let Err(e) = state
                .crawler_client
                .clone()
                .crawl(CrawlRequest { url })
                .await
            {
                tracing::error!(error = %e, "Failed to crawl");
            }
        }
    } else {
        tracing::info!("No eligible domains");
    }
    Ok(())
}

async fn listen() {
    let env = Environment::default();

    let config = Config::builder()
        .add_source(env)
        .build()
        .expect("Failed to build configuration");

    let config: AppConfig = config
        .try_deserialize()
        .expect("Failed to deserialize configuration");

    let semaphore = Arc::new(Semaphore::new(config.selector_concurrent));
    let state = state::AppState::new(config).await;

    tracing::info!("Starting");
    // TODO: Reopen channel if closed
    loop {
        let semaclone = semaphore.clone();
        let state = state.clone();
        tokio::spawn(async move {
            match semaclone.acquire_owned().await {
                Ok(_guard) => {
                    if let Err(e) = step(&state).await {
                        tracing::error!("Failed to run step: {e:#}");
                    }
                }
                Err(e) => tracing::error!(error = %e, "Failed to aquire semaphore"),
            }
        });
    }
}

#[tokio::main]
async fn main() {
    utils::start(env!("CARGO_PKG_NAME"), Box::pin(listen())).await;
}
