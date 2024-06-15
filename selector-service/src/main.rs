extern crate redis;

use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use state::AppState;
use tracing::{debug, error, info, warn};

mod state;

const CONSUMER_TAG: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[tracing::instrument(skip(app_state))]
async fn consume(domain: &str, app_state: AppState) -> anyhow::Result<()> {
    debug!("Creating consumer");
    let mut consumer = app_state
        .amqp_channel
        .basic_consume(domain, CONSUMER_TAG, Default::default(), Default::default())
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
                    .context("crawler post")
                {
                    Ok(response) => {
                        debug!("Received response - {response:?}");
                        info!("Sent to crawler - {url}");
                        delivery.ack(Default::default()).await.context("basic ack")
                    },
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
    let con = app_state.redis_client.get_async_connection().await.unwrap();
    let mut pubsub = con.into_pubsub();
    pubsub.psubscribe("__keyspace@*__:c:*").await.unwrap();
    let mut stream = pubsub.on_message();
    info!("Listening for messages");
    while let Some(msg) = stream.next().await {
        debug!("Received message - {msg:?}");
        let app_state = app_state.clone();
        tokio::spawn(async move {
            match msg.get_payload::<String>().as_deref() {
                Ok("expired") => match msg.get_channel::<String>() {
                    Ok(channel) => {
                        let Some(domain) = channel.splitn(3, ":").nth(2) else {
                            warn!("Invalid channel - {channel}");
                            return;
                        };
                        info!("\"{domain}\" cooldown expired");
                        if let Err(e) = consume(domain, app_state).await {
                            error!("Failed to consume - {e:#}");
                        }
                    }
                    Err(e) => {
                        error!("Channel error - {e:#}");
                    }
                },
                Ok(_) => (),
                Err(e) => {
                    error!("Payload error - {e:#}");
                }
            }
        });
    }
}
