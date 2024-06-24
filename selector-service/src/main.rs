extern crate redis;

use rand::seq::IteratorRandom;

use models::QueueInfo;
use redis::AsyncCommands;
use tracing::{debug, error, info, warn};
use utils::redis::Key;

mod models;
mod core;
mod state;

const MANAGEMENT_URL: &str = concat!(env!("RABBITMQ_MANAGEMENT_HOST"));


async fn listen() {
    let app_state = state::AppState::new().await;
    debug!("get_multiplexed_async_connection");
    let mut conn = app_state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    debug!("rng");
    let mut rng = rand::thread_rng();
    let endpoint = format!("{MANAGEMENT_URL}/queues");
    info!("Starting selector service");
    // TODO: Reopen channel if closed
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
            let results: Vec<Option<u8>> = match conn.mget(&reset_keys).await {
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
                    if let Err(e) = core::consume(domain, app_state).await {
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

#[tokio::main]
async fn main() {
    utils::start(env!("CARGO_PKG_NAME"), Box::pin(listen())).await;
}