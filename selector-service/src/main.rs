extern crate redis;

use rand::seq::IteratorRandom;

use anyhow::Context;
use futures::TryStreamExt;
use models::QueueInfo;
use redis::AsyncCommands;
use state::AppState;
use tracing::{debug, error, info, warn};
use utils::redis::Key;

mod models;
mod state;

const MANAGEMENT_URL: &str = concat!(env!("RABBITMQ_MANAGEMENT_HOST"));

#[tracing::instrument(skip(app_state))]
async fn consume(domain: String, app_state: AppState) -> anyhow::Result<()> {
    debug!("Creating consumer");
    let mut consumer = app_state
        .amqp_channel
        .basic_consume(
            &domain,
            "",
            Default::default(),
            Default::default(),
        )
        .await
        .context("basic consume")?;
    debug!("Consuming message");
    if let Some(delivery) = consumer.try_next().await.context("consumer next")? {
        match std::str::from_utf8(&delivery.data).context("data decoding") {
            Ok(url) => {
                let encoded_url = urlencoding::encode(url);
                let endpoint = format!("{}/v1/crawl/{}", env!("CRAWLER_API"), encoded_url);
                debug!("Sending POST request to {endpoint}");
                match app_state
                    .reqwest_client
                    .post(endpoint)
                    .send()
                    .await
                    .and_then(|r| r.error_for_status())
                {
                    Ok(response) => {
                        debug!("Received response - {response:?}");
                        info!("Sent to crawler - {url}");
                        delivery.ack(Default::default()).await.context("basic ack")
                    }
                    Err(e) => {
                        error!("Failed to send POST request - {e:#}");
                        delivery
                            .reject(Default::default())
                            .await
                            .context("basic reject")
                    }
                }
            }
            Err(e) => {
                error!("Failed to decode data - {e:#}");
                delivery
                    .reject(Default::default())
                    .await
                    .context("basic reject")
            }
        }
    } else {
        anyhow::bail!("No delivery");
    }
}

#[tokio::main]
async fn main() {
    let app_state = state::AppState::new().await;
    let mut conn = app_state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let mut rng = rand::thread_rng();
    let endpoint = format!("{MANAGEMENT_URL}/queues");
    info!("Starting selector service");
    loop {
        let response = match app_state
            .reqwest_client
            .get(&endpoint)
            .basic_auth(env!("AMQP_USR"), option_env!("AMQP_PWD"))
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                error!("Failed to send GET request - {e:#}");
                continue;
            }
        };
        if response.status().is_success() {
            let queues: Vec<QueueInfo> = response.json().await.unwrap();
            let non_empty = queues
                .into_iter()
                .filter_map(|queue| {
                    if queue.messages > 0 {
                        Some(queue.name)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if non_empty.is_empty() {
                debug!("No non-empty queues");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            let reset_keys: Vec<_> = non_empty
                .iter()
                .map(|queue| Key::Cooldown(&queue))
                .collect();
            let results: Vec<Option<bool>> = match conn.mget(&reset_keys).await {
                Ok(results) => results,
                Err(e) => {
                    error!("Failed to get keys - {e:#}");
                    continue;
                }
            };
            if let Some(domain) = non_empty
                .into_iter()
                .zip(results.into_iter())
                .filter_map(
                    |(queue, result)| {
                        if let None = result {
                            Some(queue)
                        } else {
                            None
                        }
                    },
                )
                .choose(&mut rng)
            {
                info!("Selected domain - \"{domain}\"");
                let app_state = app_state.clone();
                tokio::spawn(async move {
                    if let Err(e) = consume(domain, app_state).await {
                        error!("Failed to consume - {e:#}")
                    }
                });
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            } else {
                warn!("No eligible domains");
            }
        } else {
            error!("Failed to get queues - {}", response.status());
        }
    }
}
